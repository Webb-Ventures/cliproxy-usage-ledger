package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// liteLLMDoc builds a LiteLLM-format document with n priced chat models plus
// the entries the parser must skip.
func liteLLMDoc(n int) []byte {
	doc := map[string]any{
		"sample_spec":    map[string]any{"input_cost_per_token": 0.0, "mode": "one of: chat, embedding"},
		"dall-e-3":       map[string]any{"mode": "image_generation", "output_cost_per_image": 0.04},
		"text-embedding": map[string]any{"mode": "embedding", "input_cost_per_token": 1e-8},
		"claude-opus-5-5": map[string]any{
			"litellm_provider": "anthropic", "mode": "chat",
			"input_cost_per_token": 4e-6, "output_cost_per_token": 2e-5,
			"cache_read_input_token_cost": 2e-7, "cache_creation_input_token_cost": 5e-6,
			"cache_creation_input_token_cost_above_1hr": 8e-6,
			"input_cost_per_token_above_200k_tokens":    8e-6, "output_cost_per_token_above_200k_tokens": 3e-5,
			"cache_read_input_token_cost_above_200k_tokens": 4e-7,
			"input_cost_per_token_priority":                 1e-5,
			"output_cost_per_token_flex":                    1e-5,
			"output_cost_per_reasoning_token":               2e-5,
			"input_cost_per_token_batches":                  2e-6,
			"max_tokens":                                    "not a cost",
		},
		"bad-cost": map[string]any{"mode": "chat", "input_cost_per_token": "0.1", "output_cost_per_token": -1},
	}
	for i := 0; i < n; i++ {
		doc[fmt.Sprintf("model-%03d", i)] = map[string]any{"litellm_provider": "openai", "mode": "chat", "input_cost_per_token": 1e-6, "output_cost_per_token": 2e-6}
	}
	raw, _ := json.Marshal(doc)
	return raw
}

func TestParseLiteLLMFields(t *testing.T) {
	models, err := parseLiteLLM(liteLLMDoc(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 {
		t.Fatalf("models = %v, want only claude-opus-5-5", models)
	}
	m := models["claude-opus-5-5"]
	if m.Provider != "anthropic" || *m.Input != 4e-6 || *m.Output != 2e-5 || *m.CacheRead != 2e-7 ||
		*m.CacheWrite != 5e-6 || *m.CacheWrite1h != 8e-6 || *m.Reasoning != 2e-5 {
		t.Fatalf("price = %+v", m)
	}
	if len(m.Tiers) != 1 || m.Tiers[0].AboveTokens != 200000 || *m.Tiers[0].Input != 8e-6 ||
		*m.Tiers[0].CacheRead != 4e-7 || m.Tiers[0].CacheWrite != nil {
		t.Fatalf("tiers = %+v", m.Tiers)
	}
	if *m.ServiceTiers["priority"].Input != 1e-5 || *m.ServiceTiers["flex"].Output != 1e-5 || len(m.ServiceTiers) != 2 {
		t.Fatalf("service tiers = %+v", m.ServiceTiers)
	}
	raw, _ := json.Marshal(m)
	if strings.Contains(string(raw), "batches") || !strings.Contains(string(raw), `"tiers":[{"above_tokens":200000,`) {
		t.Fatalf("served JSON = %s", raw)
	}
}

func TestValidateRemotePrices(t *testing.T) {
	if _, err := validateRemotePrices(liteLLMDoc(priceMinModels)); err != nil {
		t.Fatalf("valid document rejected: %v", err)
	}
	for name, raw := range map[string][]byte{
		"array":      []byte(`[{"input_cost_per_token": 1}]`),
		"string":     []byte(`"prices"`),
		"html":       []byte(`<html>rate limited</html>`),
		"truncated":  liteLLMDoc(priceMinModels)[:500],
		"too few":    liteLLMDoc(priceMinModels - 2), // 98 + claude = 99
		"empty":      nil,
		"over 20 MB": append(liteLLMDoc(priceMinModels), make([]byte, priceMaxBodyBytes)...),
	} {
		if _, err := validateRemotePrices(raw); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

// priceServer serves body (or status) and counts requests.
type priceServer struct {
	*httptest.Server
	hits   atomic.Int64
	body   atomic.Value // []byte
	status atomic.Int64
	agent  atomic.Value // string
}

func newPriceServer(t *testing.T, body []byte) *priceServer {
	s := &priceServer{}
	s.body.Store(body)
	s.status.Store(http.StatusOK)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		s.agent.Store(r.UserAgent())
		w.WriteHeader(int(s.status.Load()))
		_, _ = w.Write(s.body.Load().([]byte))
	}))
	t.Cleanup(s.Close)
	return s
}

func testPriceManager(t *testing.T) (*priceManager, string) {
	m := newPriceManager(time.Now)
	m.client.Transport = loopbackOnlyTransport{}
	t.Cleanup(m.stop)
	return m, filepath.Join(t.TempDir(), priceCacheFileName)
}

func TestPriceRefreshSuccessWritesCache(t *testing.T) {
	m, cachePath := testPriceManager(t)
	srv := newPriceServer(t, liteLLMDoc(priceMinModels))
	cfg := priceConfig{url: srv.URL + "/prices.json", refresh: time.Hour, cachePath: cachePath}
	if err := m.refresh(context.Background(), cfg); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if srv.hits.Load() != 1 {
		t.Fatalf("requests = %d, want exactly 1", srv.hits.Load())
	}
	if ua := srv.agent.Load().(string); !strings.HasPrefix(ua, "cliproxy-usage-ledger/"+pluginVersion+" ") {
		t.Fatalf("User-Agent = %q", ua)
	}
	snap := m.snapshot()
	if snap.table.source != priceSourceRemote || snap.table.sourceURL != cfg.url || snap.table.fetchedAt.IsZero() ||
		len(snap.table.models) != priceMinModels+1 || snap.lastError != nil {
		t.Fatalf("table = %s %s %v %d err=%v", snap.table.source, snap.table.sourceURL, snap.table.fetchedAt, len(snap.table.models), snap.lastError)
	}
	cached, err := readPriceCache(cachePath)
	if err != nil || cached.source != priceSourceCache || cached.sourceURL != cfg.url || !cached.fetchedAt.Equal(snap.table.fetchedAt) {
		t.Fatalf("cache = %+v err %v", cached, err)
	}
	entries, _ := os.ReadDir(filepath.Dir(cachePath))
	if len(entries) != 1 || entries[0].Name() != priceCacheFileName {
		t.Fatalf("cache dir = %v, want only the cache file (no temp files)", entries)
	}
}

func TestPriceRefreshBadPayloadKeepsLastGood(t *testing.T) {
	m, cachePath := testPriceManager(t)
	srv := newPriceServer(t, liteLLMDoc(priceMinModels))
	cfg := priceConfig{url: srv.URL, refresh: time.Hour, cachePath: cachePath}
	if err := m.refresh(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	good, _ := os.ReadFile(cachePath)
	goodTable := m.snapshot().table
	for name, set := range map[string]func(){
		"not an object": func() { srv.body.Store([]byte(`["nope"]`)) },
		"too few":       func() { srv.body.Store(liteLLMDoc(10)) },
		"HTTP 500":      func() { srv.status.Store(http.StatusInternalServerError); srv.body.Store(liteLLMDoc(priceMinModels)) },
		"oversized":     func() { srv.status.Store(http.StatusOK); srv.body.Store(make([]byte, priceMaxBodyBytes+10)) },
	} {
		set()
		if err := m.refresh(context.Background(), cfg); err == nil {
			t.Fatalf("%s accepted", name)
		}
		snap := m.snapshot()
		if snap.table != goodTable || snap.lastError == nil || snap.lastAttempt.IsZero() {
			t.Fatalf("%s: table replaced or error not recorded (err %v)", name, snap.lastError)
		}
		if now, _ := os.ReadFile(cachePath); string(now) != string(good) {
			t.Fatalf("%s: cache overwritten", name)
		}
	}
}

func TestPriceRefreshDoesNotFollowRedirects(t *testing.T) {
	m, cachePath := testPriceManager(t)
	target := newPriceServer(t, liteLLMDoc(priceMinModels))
	redirect := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusFound))
	defer redirect.Close()
	if err := m.refresh(context.Background(), priceConfig{url: redirect.URL, refresh: time.Hour, cachePath: cachePath}); err == nil {
		t.Fatal("redirect followed")
	}
	if target.hits.Load() != 0 {
		t.Fatal("a second request was made")
	}
}

func TestPriceFallbackOfflineUsesCacheThenEmbedded(t *testing.T) {
	// Seed a cache from a server, then take the server away.
	srv := newPriceServer(t, liteLLMDoc(priceMinModels))
	seeder, cachePath := testPriceManager(t)
	if err := seeder.refresh(context.Background(), priceConfig{url: srv.URL, refresh: time.Hour, cachePath: cachePath}); err != nil {
		t.Fatal(err)
	}
	offlineURL := srv.URL
	srv.Close()

	m, _ := testPriceManager(t)
	m.configure(priceConfig{url: offlineURL, refresh: time.Hour, cachePath: cachePath})
	waitFor(t, "failed fetch", func() bool { return m.snapshot().lastError != nil })
	if snap := m.snapshot(); snap.table.source != priceSourceCache || snap.table.sourceURL != offlineURL || len(snap.table.models) != priceMinModels+1 {
		t.Fatalf("offline with cache: source=%s models=%d", snap.table.source, len(snap.table.models))
	}

	// No cache (or an unreadable one): the embedded snapshot.
	for name, setup := range map[string]func(path string){
		"missing": func(string) {},
		"corrupt": func(path string) { _ = os.WriteFile(path, []byte(`{"schema":1,"data":[1]}`), 0o600) },
	} {
		path := filepath.Join(t.TempDir(), priceCacheFileName)
		setup(path)
		fresh, _ := testPriceManager(t)
		fresh.configure(priceConfig{url: offlineURL, refresh: time.Hour, cachePath: path})
		waitFor(t, "failed fetch", func() bool { return fresh.snapshot().lastError != nil })
		if snap := fresh.snapshot(); snap.table.source != priceSourceEmbedded || snap.table.fetchedAt != (time.Time{}) || len(snap.table.models) < 100 {
			t.Fatalf("%s cache: source=%s", name, snap.table.source)
		}
	}
}

func TestPriceFetchDisabledByEmptyURL(t *testing.T) {
	srv := newPriceServer(t, liteLLMDoc(priceMinModels)) // must never be called
	m, cachePath := testPriceManager(t)
	m.configure(priceConfig{url: "", refresh: time.Millisecond, cachePath: cachePath})
	time.Sleep(30 * time.Millisecond)
	snap := m.snapshot()
	if snap.table.source != priceSourceEmbedded || !snap.lastAttempt.IsZero() || srv.hits.Load() != 0 {
		t.Fatalf("disabled fetch: source=%s attempt=%v hits=%d", snap.table.source, snap.lastAttempt, srv.hits.Load())
	}
	m.loopMu.Lock()
	running := m.running
	m.loopMu.Unlock()
	if running {
		t.Fatal("refresher running with fetching disabled")
	}
}

func TestPriceRefreshLoopRunsOnSchedule(t *testing.T) {
	srv := newPriceServer(t, liteLLMDoc(priceMinModels))
	m, cachePath := testPriceManager(t)
	m.configure(priceConfig{url: srv.URL, refresh: 40 * time.Millisecond, cachePath: cachePath})
	waitFor(t, "startup fetch and two refreshes", func() bool { return srv.hits.Load() >= 3 })
	m.stop()
	hits := srv.hits.Load()
	time.Sleep(100 * time.Millisecond)
	if srv.hits.Load() != hits {
		t.Fatal("refresher kept running after stop")
	}
}

func TestRegisterDoesNotWaitForPriceFetch(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int64
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(liteLLMDoc(priceMinModels))
	}))
	defer slow.Close()
	defer close(release)

	p := newTestPlugin(t, 8, nil)
	start := time.Now()
	dbPath := registerTestPlugin(t, p, "price-source-url: "+slow.URL+"\n")
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("register took %v", elapsed)
	}
	waitFor(t, "fetch started", func() bool { return hits.Load() == 1 })
	if src := p.Status().Prices.Source; src != priceSourceEmbedded {
		t.Fatalf("source while fetching = %s", src)
	}
	release <- struct{}{}
	waitFor(t, "remote prices", func() bool { return p.Status().Prices.Source == priceSourceRemote })
	if st := p.Status(); st.Prices.FetchedAt == nil {
		t.Fatal("status prices.fetched_at is null after a fetch")
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dbPath), priceCacheFileName)); err != nil {
		t.Fatalf("cache not written next to the database: %v", err)
	}
}

func TestPricesRouteShape(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	resp := callRoute(t, p, http.MethodGet, routePrices, nil, nil)
	var body map[string]any
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"schema", "models", "plans", "source", "fetched_at", "source_url", "embedded_as_of", "overrides", "last_error", "last_attempt_at", "plans_as_of"} {
		if _, ok := body[key]; !ok {
			t.Fatalf("prices response lacks %q", key)
		}
	}
	var prices pricesResponse
	_ = json.Unmarshal(resp.Body, &prices)
	if prices.Source != priceSourceEmbedded || prices.FetchedAt != nil || prices.EmbeddedAsOf != "2026-10-27" ||
		!strings.Contains(prices.SourceURL, "BerriAI/litellm/") || prices.Overrides == nil {
		t.Fatalf("prices = source %s fetched %v as_of %s url %s", prices.Source, prices.FetchedAt, prices.EmbeddedAsOf, prices.SourceURL)
	}
	if opus := prices.Models["claude-opus-5-5"]; opus.Input == nil || opus.CacheWrite1h == nil {
		t.Fatalf("claude-opus-5-5 = %+v", opus)
	}
	if prices.Plans["claude"]["max_20x"].MonthlyPrice != 200 {
		t.Fatalf("plans = %+v", prices.Plans["claude"])
	}
}

func TestEmbeddedSnapshotIsSmallAndCoversProviders(t *testing.T) {
	if len(embeddedPriceSnapshot) > 300<<10 {
		t.Fatalf("embedded snapshot is %d bytes, want < 300 KB", len(embeddedPriceSnapshot))
	}
	table, asOf := loadEmbeddedTable()
	if table.source != priceSourceEmbedded || asOf == "" || len(table.models) < priceMinModels {
		t.Fatalf("embedded: source=%s as_of=%q models=%d", table.source, asOf, len(table.models))
	}
	providers := map[string]bool{}
	for _, m := range table.models {
		providers[m.Provider] = true
	}
	for _, want := range []string{"anthropic", "openai", "gemini", "vertex_ai-language-models", "xai", "moonshot", "deepseek"} {
		if !providers[want] {
			t.Fatalf("embedded snapshot has no %s models", want)
		}
	}
}

func TestPlanPricesFile(t *testing.T) {
	plans := loadPlanPrices()
	want := map[string][]string{
		"claude":      {"pro", "max_5x", "max_20x"},
		"codex":       {"plus", "pro", "team", "business"},
		"xai":         {"supergrok", "supergrok_heavy"},
		"antigravity": {"pro", "ultra"},
		"devin":       {"pro"},
	}
	for provider, keys := range want {
		for _, key := range keys {
			plan, ok := plans.Plans[provider][key]
			if !ok {
				t.Fatalf("plan-prices.json lacks %s/%s", provider, key)
			}
			if plan.MonthlyPrice <= 0 || plan.Currency != "USD" || plan.AsOf == "" || plan.Name == "" ||
				!strings.HasPrefix(plan.SourceURL, "https://") || (plan.Billing != "per_account" && plan.Billing != "per_seat") {
				t.Fatalf("%s/%s = %+v", provider, key, plan)
			}
		}
	}
}
