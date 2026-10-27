package main

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseConfigDefaults(t *testing.T) {
	for _, input := range []string{"", "   \n", "enabled: true\npriority: 0\n"} {
		cfg, err := ParseConfig([]byte(input))
		if err != nil {
			t.Fatalf("ParseConfig(%q) error: %v", input, err)
		}
		if !reflect.DeepEqual(cfg, DefaultConfig()) {
			t.Fatalf("ParseConfig(%q) = %+v, want defaults %+v", input, cfg, DefaultConfig())
		}
	}
	def := DefaultConfig()
	if def.MinuteRetentionDays != 14 || def.HourRetentionDays != 400 || def.PriceRefreshHours != 24 {
		t.Fatalf("unexpected integer defaults: %+v", def)
	}
	if def.PriceSourceURL != "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json" {
		t.Fatalf("unexpected price source default: %q", def.PriceSourceURL)
	}
	if def.DatabasePath != "" {
		t.Fatalf("database-path default should be empty, got %q", def.DatabasePath)
	}
}

func TestParseConfigOverrides(t *testing.T) {
	// Shape mirrors what the host sends: the plugins.configs.<id> mapping with
	// enabled/priority injected and unrelated keys passed through.
	input := `enabled: true
priority: 3
store:
  version: 0.1.0
database-path: /var/lib/cpa/ledger.db
minute-retention-days: 7
hour-retention-days: 90
price-source-url: https://prices.example.com/p.json
price-refresh-hours: 6
`
	cfg, err := ParseConfig([]byte(input))
	if err != nil {
		t.Fatalf("ParseConfig error: %v", err)
	}
	want := Config{
		DatabasePath:        "/var/lib/cpa/ledger.db",
		MinuteRetentionDays: 7,
		HourRetentionDays:   90,
		PriceSourceURL:      "https://prices.example.com/p.json",
		PriceRefreshHours:   6,
	}
	if cfg != want {
		t.Fatalf("ParseConfig = %+v, want %+v", cfg, want)
	}
}

func TestParseConfigEmptyPriceURLUsesDefault(t *testing.T) {
	cfg, err := ParseConfig([]byte("price-source-url: \"\"\n"))
	if err != nil {
		t.Fatalf("ParseConfig error: %v", err)
	}
	if cfg.PriceSourceURL != defaultPriceSourceURL {
		t.Fatalf("PriceSourceURL = %q, want default", cfg.PriceSourceURL)
	}
}

func TestParseConfigRejectsInvalid(t *testing.T) {
	cases := map[string]string{
		"zero minute retention":  "minute-retention-days: 0\n",
		"negative hour":          "hour-retention-days: -1\n",
		"zero refresh":           "price-refresh-hours: 0\n",
		"non-integer retention":  "minute-retention-days: soon\n",
		"relative price url":     "price-source-url: prices.json\n",
		"unsupported url scheme": "price-source-url: file:///tmp/prices.json\n",
		"malformed yaml":         "minute-retention-days: [\n",
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseConfig([]byte(input)); err == nil {
				t.Fatalf("ParseConfig(%q) succeeded, want error", input)
			}
		})
	}
}

func TestResolveDatabasePath(t *testing.T) {
	wd := filepath.FromSlash("/srv/cpa")
	cases := []struct {
		configured string
		want       string
	}{
		{"", filepath.Join(wd, "usage-ledger.db")},
		{"data/ledger.db", filepath.Join(wd, "data", "ledger.db")},
		{filepath.FromSlash("/abs/ledger.db"), filepath.FromSlash("/abs/ledger.db")},
	}
	for _, tc := range cases {
		got, err := Config{DatabasePath: tc.configured}.ResolveDatabasePath(wd)
		if err != nil {
			t.Fatalf("ResolveDatabasePath(%q) error: %v", tc.configured, err)
		}
		if got != tc.want {
			t.Fatalf("ResolveDatabasePath(%q) = %q, want %q", tc.configured, got, tc.want)
		}
	}
	if _, err := (Config{}).ResolveDatabasePath(""); err == nil {
		t.Fatal("expected error resolving relative path without working directory")
	}
}

func TestHandleUsageNeverBlocksAndCountsDrops(t *testing.T) {
	// Not started: nothing drains, so the buffer fills deterministically.
	p := NewPlugin(4, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			p.HandleUsage(usageRecord{RequestID: "r"})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("HandleUsage blocked with a full buffer")
	}
	if got := p.droppedTotal.Load(); got != 6 {
		t.Fatalf("dropped_total = %d, want 6", got)
	}

	// Starting the worker drains the 4 accepted records.
	p.Start()
	p.Shutdown()
	if got := p.recordsTotal.Load(); got != 4 {
		t.Fatalf("records_total = %d, want 4", got)
	}

	// After shutdown, ingest still returns immediately and counts as dropped.
	if p.HandleUsage(usageRecord{}) {
		t.Fatal("HandleUsage accepted a record after shutdown")
	}
	if got := p.droppedTotal.Load(); got != 7 {
		t.Fatalf("dropped_total after shutdown = %d, want 7", got)
	}
	p.Shutdown() // idempotent
}

func TestHandleUsageConcurrentAccounting(t *testing.T) {
	p := NewPlugin(usageBufferSize, nil)
	p.Start()
	const workers, perWorker = 8, 2000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				p.HandleUsage(usageRecord{})
			}
		}()
	}
	wg.Wait()
	p.Shutdown()
	total := p.recordsTotal.Load() + p.droppedTotal.Load()
	if total != workers*perWorker {
		t.Fatalf("records_total+dropped_total = %d, want %d", total, workers*perWorker)
	}
}

func TestShutdownWithoutStart(t *testing.T) {
	p := NewPlugin(1, nil)
	finished := make(chan struct{})
	go func() {
		p.Shutdown()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown hung when Start was never called")
	}
}

func TestStatusJSONShape(t *testing.T) {
	fixed := time.Date(2026, 10, 27, 12, 30, 45, 123456789, time.FixedZone("x", 3600))
	p := NewPlugin(8, func() time.Time { return fixed })

	// Before any record: last_record_at and recording_since are null.
	assertStatus(t, p, map[string]any{
		"schema":          float64(1),
		"plugin_version":  "0.1.0",
		"records_total":   float64(0),
		"dropped_total":   float64(0),
		"recording_since": nil,
		"last_record_at":  nil,
		"retention":       map[string]any{"minute_days": float64(14), "hour_days": float64(400)},
	})

	p.Start()
	p.HandleUsage(usageRecord{})
	p.Shutdown()
	assertStatus(t, p, map[string]any{
		"schema":          float64(1),
		"plugin_version":  "0.1.0",
		"records_total":   float64(1),
		"dropped_total":   float64(0),
		"recording_since": nil,
		"last_record_at":  "2026-10-27T11:30:45Z",
		"retention":       map[string]any{"minute_days": float64(14), "hour_days": float64(400)},
	})
}

func assertStatus(t *testing.T, p *Plugin, want map[string]any) {
	t.Helper()
	raw, err := p.HandleMethod(methodManagementHandle, mustJSON(t, managementRequest{
		Method: http.MethodGet,
		Path:   "/v0/management/usage-ledger/status",
	}))
	if err != nil {
		t.Fatalf("management.handle error: %v", err)
	}
	var env struct {
		OK     bool
		Result managementResponse
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if !env.OK || env.Result.StatusCode != http.StatusOK {
		t.Fatalf("unexpected response: ok=%v status=%d", env.OK, env.Result.StatusCode)
	}
	if ct := env.Result.Headers.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("Content-Type = %q", ct)
	}
	var got map[string]any
	if err := json.Unmarshal(env.Result.Body, &got); err != nil {
		t.Fatalf("decode status body %s: %v", env.Result.Body, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("status = %#v\nwant   %#v", got, want)
	}
}

func TestStatusReflectsReconfiguredRetention(t *testing.T) {
	p := NewPlugin(1, nil)
	defer p.Shutdown()
	cfgYAML := []byte("minute-retention-days: 3\nhour-retention-days: 30\n")
	if _, err := p.HandleMethod(methodPluginReconfigure, mustJSON(t, lifecycleRequest{ConfigYAML: cfgYAML})); err != nil {
		t.Fatalf("reconfigure error: %v", err)
	}
	st := p.Status()
	if st.Retention.MinuteDays != 3 || st.Retention.HourDays != 30 {
		t.Fatalf("retention = %+v", st.Retention)
	}
}

func TestManagementHandleRejectsOtherRoutes(t *testing.T) {
	p := NewPlugin(1, nil)
	defer p.Shutdown()
	if resp := p.handleManagement(managementRequest{Method: "POST", Path: statusRouteFullPath}); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", resp.StatusCode)
	}
	if resp := p.handleManagement(managementRequest{Method: "GET", Path: "/v0/management/other"}); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("other path status = %d, want 404", resp.StatusCode)
	}
}

func TestRegistrationPayload(t *testing.T) {
	p := NewPlugin(1, nil)
	defer p.Shutdown()
	raw, err := p.HandleMethod(methodPluginRegister, mustJSON(t, lifecycleRequest{SchemaVersion: 7}))
	if err != nil {
		t.Fatalf("register error: %v", err)
	}
	var env struct {
		OK     bool `json:"ok"`
		Result struct {
			SchemaVersion uint32 `json:"schema_version"`
			Metadata      struct {
				Name, Version, Author, GitHubRepository string
				ConfigFields                            []struct {
					Name, Type string
					Default    any
				}
			} `json:"metadata"`
			Capabilities map[string]bool `json:"capabilities"`
		} `json:"result"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	r := env.Result
	if !env.OK || r.SchemaVersion != pluginSchemaVersion {
		t.Fatalf("ok=%v schema=%d", env.OK, r.SchemaVersion)
	}
	if r.Metadata.Name != "usage-ledger" || r.Metadata.Version != "0.1.0" ||
		r.Metadata.Author != "Webb Ventures" ||
		r.Metadata.GitHubRepository != "https://github.com/Webb-Ventures/cliproxy-usage-ledger" {
		t.Fatalf("metadata = %+v", r.Metadata)
	}
	if !r.Capabilities["usage_plugin"] || !r.Capabilities["management_api"] {
		t.Fatalf("capabilities = %v", r.Capabilities)
	}
	names := make([]string, 0, len(r.Metadata.ConfigFields))
	for _, f := range r.Metadata.ConfigFields {
		names = append(names, f.Name+":"+f.Type)
	}
	sort.Strings(names)
	wantNames := []string{
		"database-path:string", "hour-retention-days:integer", "minute-retention-days:integer",
		"price-refresh-hours:integer", "price-source-url:string",
	}
	if !reflect.DeepEqual(names, wantNames) {
		t.Fatalf("config fields = %v", names)
	}

	// An older host caps the declared schema version.
	raw, err = p.HandleMethod(methodPluginRegister, mustJSON(t, lifecycleRequest{SchemaVersion: 2}))
	if err != nil {
		t.Fatalf("register error: %v", err)
	}
	if err := json.Unmarshal(raw, &env); err != nil || env.Result.SchemaVersion != 2 {
		t.Fatalf("schema with older host = %d (err %v), want 2", env.Result.SchemaVersion, err)
	}
}

func TestRegisterRejectsInvalidConfig(t *testing.T) {
	p := NewPlugin(1, nil)
	defer p.Shutdown()
	req := mustJSON(t, lifecycleRequest{ConfigYAML: []byte("hour-retention-days: 0\n")})
	if _, err := p.HandleMethod(methodPluginRegister, req); err == nil {
		t.Fatal("expected register to fail for invalid config")
	}
	if p.Config() != DefaultConfig() {
		t.Fatal("invalid config must not replace the current config")
	}
}

func TestManagementRegisterDeclaresStatusRoute(t *testing.T) {
	p := NewPlugin(1, nil)
	defer p.Shutdown()
	raw, err := p.HandleMethod(methodManagementRegister, nil)
	if err != nil {
		t.Fatalf("management.register error: %v", err)
	}
	var env struct {
		Result managementRegistrationResponse `json:"result"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(env.Result.Routes) != 1 || env.Result.Routes[0].Method != "GET" ||
		env.Result.Routes[0].Path != "usage-ledger/status" {
		t.Fatalf("routes = %+v", env.Result.Routes)
	}
}

func TestUsageHandleDecodesHostRecord(t *testing.T) {
	p := NewPlugin(1, nil)
	// Field names and encodings as produced by json.Marshal(pluginapi.UsageRecord).
	body := []byte(`{"RequestID":"abc","Provider":"claude","Model":"claude-sonnet",` +
		`"RequestedAt":"2026-10-27T10:00:00Z","Latency":1500000000,"Detail":{"InputTokens":10,"OutputTokens":5,"TotalTokens":15}}`)
	raw, err := p.HandleMethod(methodUsageHandle, body)
	if err != nil {
		t.Fatalf("usage.handle error: %v", err)
	}
	if string(raw) != `{"ok":true,"result":{}}` {
		t.Fatalf("usage.handle response = %s", raw)
	}
	rec := <-p.records
	if rec.RequestID != "abc" || rec.Latency != 1500*time.Millisecond || rec.Detail.TotalTokens != 15 {
		t.Fatalf("decoded record = %+v", rec)
	}
	p.Shutdown()

	if _, err := NewPlugin(1, nil).HandleMethod(methodUsageHandle, []byte("not json")); err == nil {
		t.Fatal("expected decode error for malformed usage record")
	}
}

func TestUnknownMethodEnvelope(t *testing.T) {
	raw, err := NewPlugin(1, nil).HandleMethod("plugin.quiesce", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil || env.OK || env.Error == nil || env.Error.Code != "unknown_method" {
		t.Fatalf("envelope = %s (err %v)", raw, err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}
