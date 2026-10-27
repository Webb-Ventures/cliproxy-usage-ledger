package main

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Model prices come from LiteLLM's model_prices_and_context_window.json. The
// plugin serves them per token in USD and never multiplies them itself: the
// frontend prices token counts at read time.
//
// Fallback chain: the last good remote copy (written next to the database),
// then the snapshot embedded at build time, then an empty table.

const (
	priceCacheFileName   = "usage-ledger-prices.json"
	priceFetchTimeout    = 30 * time.Second
	priceMaxBodyBytes    = 20 << 20
	priceMinModels       = 100
	priceCacheFileSchema = 1

	priceSourceRemote   = "remote"
	priceSourceCache    = "cache"
	priceSourceEmbedded = "embedded"
	priceSourceNone     = "none"
)

var priceUserAgent = "cliproxy-usage-ledger/" + pluginVersion + " (+" + pluginRepository + ")"

//go:embed prices-snapshot.json
var embeddedPriceSnapshot []byte

//go:embed plan-prices.json
var embeddedPlanPrices []byte

// ---- Served shapes ----

// tokenPrices are USD per token. Absent fields were not in the source.
type tokenPrices struct {
	Input      *float64 `json:"input,omitempty"`
	Output     *float64 `json:"output,omitempty"`
	CacheRead  *float64 `json:"cache_read,omitempty"`
	CacheWrite *float64 `json:"cache_write,omitempty"`
}

func (t tokenPrices) empty() bool {
	return t.Input == nil && t.Output == nil && t.CacheRead == nil && t.CacheWrite == nil
}

// priceTier applies when the prompt is larger than AboveTokens (LiteLLM's
// *_above_<N>k_tokens fields).
type priceTier struct {
	AboveTokens int64 `json:"above_tokens"`
	tokenPrices
}

type modelPrice struct {
	Provider string `json:"provider,omitempty"`
	Mode     string `json:"mode,omitempty"`
	tokenPrices
	// CacheWrite1h is the 1-hour cache write price (Anthropic).
	CacheWrite1h *float64 `json:"cache_write_1h,omitempty"`
	// Reasoning is set only where it differs from output in the source.
	Reasoning    *float64               `json:"reasoning,omitempty"`
	Tiers        []priceTier            `json:"tiers,omitempty"`
	ServiceTiers map[string]tokenPrices `json:"service_tiers,omitempty"`
}

// ---- LiteLLM parsing ----

var (
	liteLLMBaseFields = map[string]string{
		"input_cost_per_token":            "input",
		"output_cost_per_token":           "output",
		"cache_read_input_token_cost":     "cache_read",
		"cache_creation_input_token_cost": "cache_write",
	}
	liteLLMTierPattern = regexp.MustCompile(
		`^(input_cost_per_token|output_cost_per_token|cache_read_input_token_cost|cache_creation_input_token_cost)_above_(\d+)k_tokens$`)
	liteLLMServiceTierPattern = regexp.MustCompile(
		`^(input_cost_per_token|output_cost_per_token|cache_read_input_token_cost|cache_creation_input_token_cost)_(priority|flex)$`)
	// Token-priced modes. Image, audio, embedding, and video entries are
	// skipped: the ledger only counts text tokens.
	liteLLMModes = map[string]bool{"": true, "chat": true, "completion": true, "responses": true}
)

func setTokenPrice(t *tokenPrices, field string, v float64) {
	value := v
	switch field {
	case "input":
		t.Input = &value
	case "output":
		t.Output = &value
	case "cache_read":
		t.CacheRead = &value
	case "cache_write":
		t.CacheWrite = &value
	}
}

// parseLiteLLM turns a LiteLLM price document into the served table. Entries
// without a token price, non-text modes, and malformed values are skipped.
func parseLiteLLM(raw []byte) (map[string]modelPrice, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("price document is not a JSON object")
	}
	var doc map[string]json.RawMessage
	if errUnmarshal := json.Unmarshal(trimmed, &doc); errUnmarshal != nil {
		return nil, fmt.Errorf("price document is not a JSON object: %w", errUnmarshal)
	}
	models := make(map[string]modelPrice, len(doc))
	for name, entryRaw := range doc {
		if name == "sample_spec" || strings.TrimSpace(name) == "" {
			continue
		}
		var entry map[string]json.RawMessage
		if json.Unmarshal(entryRaw, &entry) != nil {
			continue
		}
		var mode, provider string
		_ = json.Unmarshal(entry["mode"], &mode)
		_ = json.Unmarshal(entry["litellm_provider"], &provider)
		if !liteLLMModes[mode] {
			continue
		}
		price := modelPrice{Provider: provider, Mode: mode}
		tiers := map[int64]*tokenPrices{}
		for field, value := range entry {
			cost, ok := parseCost(value)
			if !ok {
				continue
			}
			if target, base := liteLLMBaseFields[field]; base {
				setTokenPrice(&price.tokenPrices, target, cost)
				continue
			}
			switch field {
			case "cache_creation_input_token_cost_above_1hr":
				price.CacheWrite1h = &cost
				continue
			case "output_cost_per_reasoning_token":
				price.Reasoning = &cost
				continue
			}
			if m := liteLLMTierPattern.FindStringSubmatch(field); m != nil {
				thousands, errAtoi := strconv.ParseInt(m[2], 10, 64)
				if errAtoi != nil || thousands <= 0 {
					continue
				}
				above := thousands * 1000
				if tiers[above] == nil {
					tiers[above] = &tokenPrices{}
				}
				setTokenPrice(tiers[above], liteLLMBaseFields[m[1]], cost)
				continue
			}
			if m := liteLLMServiceTierPattern.FindStringSubmatch(field); m != nil {
				if price.ServiceTiers == nil {
					price.ServiceTiers = map[string]tokenPrices{}
				}
				tier := price.ServiceTiers[m[2]]
				setTokenPrice(&tier, liteLLMBaseFields[m[1]], cost)
				price.ServiceTiers[m[2]] = tier
			}
		}
		if price.Input == nil && price.Output == nil {
			continue
		}
		for above, prices := range tiers {
			price.Tiers = append(price.Tiers, priceTier{AboveTokens: above, tokenPrices: *prices})
		}
		sort.Slice(price.Tiers, func(i, j int) bool { return price.Tiers[i].AboveTokens < price.Tiers[j].AboveTokens })
		models[name] = price
	}
	return models, nil
}

// parseCost accepts a finite, non-negative JSON number.
func parseCost(raw json.RawMessage) (float64, bool) {
	var v float64
	if len(raw) == 0 || raw[0] == '"' || json.Unmarshal(raw, &v) != nil {
		return 0, false
	}
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, false
	}
	return v, true
}

// validateRemotePrices applies the acceptance rules for a fetched document.
func validateRemotePrices(raw []byte) (map[string]modelPrice, error) {
	if len(raw) > priceMaxBodyBytes {
		return nil, fmt.Errorf("price document is larger than %d bytes", priceMaxBodyBytes)
	}
	models, errParse := parseLiteLLM(raw)
	if errParse != nil {
		return nil, errParse
	}
	if len(models) < priceMinModels {
		return nil, fmt.Errorf("price document has %d priced models, want at least %d", len(models), priceMinModels)
	}
	return models, nil
}

// ---- Embedded data ----

type priceSnapshotFile struct {
	Source    string          `json:"source"`
	SourceURL string          `json:"source_url"`
	Commit    string          `json:"commit"`
	AsOf      string          `json:"as_of"`
	Models    json.RawMessage `json:"models"`
}

type planPrice struct {
	Name         string  `json:"name"`
	MonthlyPrice float64 `json:"monthly_price"`
	Currency     string  `json:"currency"`
	Billing      string  `json:"billing"`
	AsOf         string  `json:"as_of"`
	SourceURL    string  `json:"source_url"`
	Verified     bool    `json:"verified"`
	Note         string  `json:"note,omitempty"`
}

type planPricesFile struct {
	AsOf  string                          `json:"as_of"`
	Note  string                          `json:"note"`
	Plans map[string]map[string]planPrice `json:"plans"`
}

// priceTable is one immutable generation of the model price map.
type priceTable struct {
	models    map[string]modelPrice
	source    string
	fetchedAt time.Time // zero for embedded and empty tables
	sourceURL string
}

func loadEmbeddedTable() (*priceTable, string) {
	var snap priceSnapshotFile
	if json.Unmarshal(embeddedPriceSnapshot, &snap) != nil {
		return &priceTable{models: map[string]modelPrice{}, source: priceSourceNone}, ""
	}
	models, errParse := parseLiteLLM(snap.Models)
	if errParse != nil {
		return &priceTable{models: map[string]modelPrice{}, source: priceSourceNone}, snap.AsOf
	}
	return &priceTable{models: models, source: priceSourceEmbedded, sourceURL: snap.SourceURL}, snap.AsOf
}

func loadPlanPrices() planPricesFile {
	var plans planPricesFile
	if json.Unmarshal(embeddedPlanPrices, &plans) != nil || plans.Plans == nil {
		plans.Plans = map[string]map[string]planPrice{}
	}
	return plans
}

// ---- Cache file ----

// priceCacheFile wraps the last good remote document with its provenance.
type priceCacheFile struct {
	Schema    int             `json:"schema"`
	FetchedAt time.Time       `json:"fetched_at"`
	SourceURL string          `json:"source_url"`
	Data      json.RawMessage `json:"data"`
}

func readPriceCache(path string) (*priceTable, error) {
	raw, errRead := os.ReadFile(path)
	if errRead != nil {
		return nil, errRead
	}
	var file priceCacheFile
	if errUnmarshal := json.Unmarshal(raw, &file); errUnmarshal != nil {
		return nil, fmt.Errorf("price cache %s: %w", path, errUnmarshal)
	}
	if file.Schema != priceCacheFileSchema {
		return nil, fmt.Errorf("price cache %s: unsupported schema %d", path, file.Schema)
	}
	models, errValidate := validateRemotePrices(file.Data)
	if errValidate != nil {
		return nil, fmt.Errorf("price cache %s: %w", path, errValidate)
	}
	return &priceTable{models: models, source: priceSourceCache, fetchedAt: file.FetchedAt.UTC(), sourceURL: file.SourceURL}, nil
}

// writePriceCache replaces the cache atomically: a temp file in the same
// directory, fsync, then rename over the old copy.
func writePriceCache(path string, fetchedAt time.Time, sourceURL string, data []byte) error {
	raw, errMarshal := json.Marshal(priceCacheFile{Schema: priceCacheFileSchema, FetchedAt: fetchedAt.UTC(), SourceURL: sourceURL, Data: data})
	if errMarshal != nil {
		return errMarshal
	}
	dir := filepath.Dir(path)
	tmp, errCreate := os.CreateTemp(dir, ".usage-ledger-prices-*.tmp")
	if errCreate != nil {
		return errCreate
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }
	if _, errWrite := tmp.Write(raw); errWrite != nil {
		_ = tmp.Close()
		cleanup()
		return errWrite
	}
	if errSync := tmp.Sync(); errSync != nil {
		_ = tmp.Close()
		cleanup()
		return errSync
	}
	if errClose := tmp.Close(); errClose != nil {
		cleanup()
		return errClose
	}
	if errRename := os.Rename(tmpPath, path); errRename != nil {
		cleanup()
		return errRename
	}
	return nil
}

// ---- Manager ----

type priceConfig struct {
	url       string
	refresh   time.Duration
	cachePath string
}

// priceManager owns the current price table and the refresh goroutine. The
// refresher never touches the database, so it runs independently of the
// ingest worker.
type priceManager struct {
	now    func() time.Time
	client *http.Client

	embedded     *priceTable
	embeddedAsOf string
	plans        planPricesFile

	mu          sync.RWMutex
	table       *priceTable
	lastAttempt time.Time
	lastError   *string

	loopMu   sync.Mutex
	cfg      priceConfig
	running  bool
	stopLoop chan struct{}
	loopDone chan struct{}

	fetchedLocal atomic.Bool // a remote fetch succeeded in this process
}

func newPriceManager(now func() time.Time) *priceManager {
	embedded, asOf := loadEmbeddedTable()
	return &priceManager{
		now:          now,
		client:       newPriceHTTPClient(),
		embedded:     embedded,
		embeddedAsOf: asOf,
		plans:        loadPlanPrices(),
		table:        embedded,
	}
}

// newPriceHTTPClient makes exactly one request per refresh: no retries, and
// redirects are not followed (a redirect is reported as a failed fetch).
func newPriceHTTPClient() *http.Client {
	return &http.Client{
		Timeout: priceFetchTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

type priceSnapshot struct {
	table        *priceTable
	lastAttempt  time.Time
	lastError    *string
	embeddedAsOf string
	plans        map[string]map[string]planPrice
	plansAsOf    string
}

func (m *priceManager) snapshot() priceSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return priceSnapshot{
		table:        m.table,
		lastAttempt:  m.lastAttempt,
		lastError:    m.lastError,
		embeddedAsOf: m.embeddedAsOf,
		plans:        m.plans.Plans,
		plansAsOf:    m.plans.AsOf,
	}
}

func (m *priceManager) setTable(t *priceTable) {
	m.mu.Lock()
	m.table = t
	m.mu.Unlock()
}

// configure applies url, refresh interval, and cache path. It never blocks on
// the network: the first fetch runs on the refresh goroutine. An empty url
// disables fetching; the cache (else the embedded snapshot) is served.
func (m *priceManager) configure(cfg priceConfig) {
	m.loopMu.Lock()
	defer m.loopMu.Unlock()
	if m.running && m.cfg == cfg {
		return
	}
	cachePathChanged := m.cfg.cachePath != cfg.cachePath
	urlChanged := m.cfg.url != cfg.url
	m.stopLocked()
	m.cfg = cfg

	current := m.snapshot().table
	if cachePathChanged || current == nil || current.source == priceSourceEmbedded || current.source == priceSourceNone {
		if cached, errCache := readPriceCache(cfg.cachePath); errCache == nil {
			m.setTable(cached)
		} else if current == nil || current.source != priceSourceRemote {
			m.setTable(m.embedded)
		}
	}
	if cfg.url == "" {
		return
	}
	// The first configure in a process fetches at once. A later restart (new
	// interval, or reconfigure after quiesce) keeps the schedule of the last
	// successful fetch from the same URL.
	delay := time.Duration(0)
	if t := m.snapshot().table; m.fetchedLocal.Load() && !urlChanged && t.source == priceSourceRemote {
		if next := t.fetchedAt.Add(cfg.refresh); next.After(m.now()) {
			delay = next.Sub(m.now())
		}
	}
	m.stopLoop = make(chan struct{})
	m.loopDone = make(chan struct{})
	m.running = true
	go m.loop(cfg, delay, m.stopLoop, m.loopDone)
}

// stop ends the refresh goroutine and waits for it.
func (m *priceManager) stop() {
	m.loopMu.Lock()
	defer m.loopMu.Unlock()
	m.stopLocked()
}

func (m *priceManager) stopLocked() {
	if !m.running {
		return
	}
	close(m.stopLoop)
	<-m.loopDone
	m.running = false
}

func (m *priceManager) loop(cfg priceConfig, delay time.Duration, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	timer := time.NewTimer(delay)
	defer timer.Stop()
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
			_ = m.refresh(ctx, cfg)
			timer.Reset(cfg.refresh)
		}
	}
}

// refresh performs one fetch. On success the table is replaced and the cache
// rewritten; on failure the current table (cache or embedded) stays.
func (m *priceManager) refresh(ctx context.Context, cfg priceConfig) error {
	started := m.now().UTC()
	data, errFetch := m.fetch(ctx, cfg.url)
	var models map[string]modelPrice
	if errFetch == nil {
		models, errFetch = validateRemotePrices(data)
	}
	if errFetch != nil {
		message := errFetch.Error()
		m.mu.Lock()
		m.lastAttempt, m.lastError = started, &message
		m.mu.Unlock()
		return errFetch
	}
	table := &priceTable{models: models, source: priceSourceRemote, fetchedAt: started, sourceURL: cfg.url}
	var lastError *string
	if errCache := writePriceCache(cfg.cachePath, started, cfg.url, data); errCache != nil {
		message := "write price cache: " + errCache.Error()
		lastError = &message
	}
	m.mu.Lock()
	m.table, m.lastAttempt, m.lastError = table, started, lastError
	m.mu.Unlock()
	m.fetchedLocal.Store(true)
	return nil
}

func (m *priceManager) fetch(ctx context.Context, rawURL string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, priceFetchTimeout)
	defer cancel()
	req, errReq := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if errReq != nil {
		return nil, errReq
	}
	req.Header.Set("User-Agent", priceUserAgent)
	req.Header.Set("Accept", "application/json")
	resp, errDo := m.client.Do(req)
	if errDo != nil {
		return nil, fmt.Errorf("fetch prices: %w", errDo)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil, fmt.Errorf("fetch prices: HTTP %d", resp.StatusCode)
	}
	if resp.ContentLength > priceMaxBodyBytes {
		return nil, fmt.Errorf("price document is larger than %d bytes", priceMaxBodyBytes)
	}
	data, errRead := io.ReadAll(io.LimitReader(resp.Body, priceMaxBodyBytes+1))
	if errRead != nil {
		return nil, fmt.Errorf("fetch prices: %w", errRead)
	}
	if len(data) > priceMaxBodyBytes {
		return nil, fmt.Errorf("price document is larger than %d bytes", priceMaxBodyBytes)
	}
	return data, nil
}

// ---- Route ----

type pricesResponse struct {
	Schema        int                             `json:"schema"`
	Source        string                          `json:"source"`
	FetchedAt     *string                         `json:"fetched_at"`
	SourceURL     string                          `json:"source_url"`
	EmbeddedAsOf  string                          `json:"embedded_as_of"`
	LastAttemptAt *string                         `json:"last_attempt_at"`
	LastError     *string                         `json:"last_error"`
	Models        map[string]modelPrice           `json:"models"`
	Plans         map[string]map[string]planPrice `json:"plans"`
	PlansAsOf     string                          `json:"plans_as_of"`
	Overrides     *ledgerSettings                 `json:"overrides"`
}

func (p *Plugin) handlePrices(managementRequest) managementResponse {
	snap := p.prices.snapshot()
	resp := pricesResponse{
		Schema:       routeSchema,
		Source:       snap.table.source,
		SourceURL:    snap.table.sourceURL,
		EmbeddedAsOf: snap.embeddedAsOf,
		LastError:    snap.lastError,
		Models:       snap.table.models,
		Plans:        snap.plans,
		PlansAsOf:    snap.plansAsOf,
	}
	resp.FetchedAt = optionalTime(snap.table.fetchedAt)
	resp.LastAttemptAt = optionalTime(snap.lastAttempt)
	// Overrides are served separately and never merged into models or plans.
	if settings, errSettings := p.loadSettings(); errSettings == nil {
		resp.Overrides = &settings
	}
	return jsonResponse(http.StatusOK, resp)
}

func optionalTime(t time.Time) *string {
	if t.IsZero() {
		return nil
	}
	formatted := t.UTC().Format(time.RFC3339)
	return &formatted
}
