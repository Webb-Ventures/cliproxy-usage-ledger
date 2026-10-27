package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// main is required by -buildmode=c-shared. It lives here rather than in the
// cgo glue so the package still builds (and tests run) with CGO_ENABLED=0.
func main() {}

const (
	pluginName       = "usage-ledger"
	pluginVersion    = "0.1.0"
	pluginAuthor     = "Webb Ventures"
	pluginRepository = "https://github.com/Webb-Ventures/cliproxy-usage-ledger"

	// abiVersion is the native C ABI version (pluginabi.ABIVersion).
	abiVersion uint32 = 1
	// pluginSchemaVersion is the RPC JSON contract version this plugin speaks.
	// 6 is the first version where management JSON bodies are passed through
	// without HTML escaping (pluginabi.SchemaVersionRawManagementResponse).
	pluginSchemaVersion uint32 = 6

	// statusSchema versions the JSON document served by the status route.
	statusSchema = 1

	// usageBufferSize bounds the ingest queue between the host and the worker.
	usageBufferSize = 4096

	// statusRoutePath is declared relative; the host resolves it under
	// /v0/management (internal/pluginhost/management.go normalizeManagementRoute).
	statusRoutePath     = "usage-ledger/status"
	managementBasePath  = "/v0/management"
	statusRouteFullPath = managementBasePath + "/" + statusRoutePath
)

// RPC method names, mirrored from sdk/pluginabi/types.go.
const (
	methodPluginRegister     = "plugin.register"
	methodPluginReconfigure  = "plugin.reconfigure"
	methodUsageHandle        = "usage.handle"
	methodManagementRegister = "management.register"
	methodManagementHandle   = "management.handle"
)

// ---- Wire types (JSON shapes mirrored from sdk/pluginabi and sdk/pluginapi) ----

type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	HTTPStatus int    `json:"http_status,omitempty"`
}

// lifecycleRequest is the plugin.register / plugin.reconfigure payload
// (internal/pluginhost/rpc_schema.go rpcLifecycleRequest).
type lifecycleRequest struct {
	ConfigYAML    []byte `json:"config_yaml"`
	SchemaVersion uint32 `json:"schema_version"`
}

type registration struct {
	SchemaVersion uint32       `json:"schema_version"`
	Metadata      metadata     `json:"metadata"`
	Capabilities  capabilities `json:"capabilities"`
}

// metadata mirrors pluginapi.Metadata, which has no JSON tags.
type metadata struct {
	Name             string
	Version          string
	Author           string
	GitHubRepository string
	Logo             string
	ConfigFields     []configField
}

// configField mirrors pluginapi.ConfigField, which has no JSON tags.
type configField struct {
	Name        string
	Type        string
	Default     any
	EnumValues  []string `json:",omitempty"`
	Description string
}

type capabilities struct {
	UsagePlugin   bool `json:"usage_plugin"`
	ManagementAPI bool `json:"management_api"`
}

// managementRoute mirrors pluginapi.ManagementRoute minus the host-side Handler.
type managementRoute struct {
	Method      string
	Path        string
	Description string `json:",omitempty"`
}

type managementRegistrationResponse struct {
	Routes []managementRoute `json:"routes,omitempty"`
}

// managementRequest mirrors pluginapi.ManagementRequest (+ host_callback_id).
type managementRequest struct {
	Method         string
	Path           string
	Headers        http.Header
	Query          map[string][]string
	Body           []byte
	HostCallbackID string `json:"host_callback_id,omitempty"`
}

// managementResponse mirrors pluginapi.ManagementResponse.
type managementResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

// usageRecord mirrors pluginapi.UsageRecord. Durations arrive as nanoseconds.
type usageRecord struct {
	RequestID            string
	TraceID              string
	SelectionID          string
	Provider             string
	BaseURL              string
	ExecutorType         string
	Model                string
	Alias                string
	APIKey               string
	IsNativeKey          bool
	AccessProvider       string
	SessionID            string
	ParentSessionID      string
	AuthID               string
	AuthIndex            string
	AuthType             string
	Source               string
	ReasoningEffort      string
	ServiceTier          string
	EffectiveServiceTier string
	ResponseServiceTier  string
	ResponseModel        string
	Generate             bool
	Stream               bool
	RequestedAt          time.Time
	Latency              time.Duration
	TTFT                 time.Duration
	Failed               bool
	Failure              usageFailure
	Detail               usageDetail
	ResponseHeaders      http.Header
}

type usageFailure struct {
	StatusCode int
	Body       string
}

type usageDetail struct {
	InputTokens         int64
	OutputTokens        int64
	ReasoningTokens     int64
	CachedTokens        int64
	CacheReadTokens     int64
	CacheCreationTokens int64
	TotalTokens         int64
}

// ---- Status document ----

type statusResponse struct {
	Schema         int             `json:"schema"`
	PluginVersion  string          `json:"plugin_version"`
	RecordsTotal   uint64          `json:"records_total"`
	DroppedTotal   uint64          `json:"dropped_total"`
	RecordingSince *string         `json:"recording_since"`
	LastRecordAt   *string         `json:"last_record_at"`
	Retention      statusRetention `json:"retention"`
}

type statusRetention struct {
	MinuteDays int `json:"minute_days"`
	HourDays   int `json:"hour_days"`
}

// ---- Plugin ----

// Plugin holds runtime state. All exported behaviour is safe for concurrent
// use; the host may invoke RPC methods from multiple goroutines.
type Plugin struct {
	now func() time.Time

	config atomic.Pointer[Config]

	// mu guards closed against the ingest path so no record is enqueued after
	// the worker has finished draining. Ingest takes only the read lock.
	mu      sync.RWMutex
	closed  bool
	records chan usageRecord

	recordsTotal atomic.Uint64
	droppedTotal atomic.Uint64
	lastRecordAt atomic.Int64 // UnixNano; 0 means no record yet.

	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	done      chan struct{}
}

// NewPlugin creates a plugin with default config and a bounded ingest queue.
func NewPlugin(bufferSize int, now func() time.Time) *Plugin {
	if bufferSize <= 0 {
		bufferSize = usageBufferSize
	}
	if now == nil {
		now = time.Now
	}
	p := &Plugin{
		now:     now,
		records: make(chan usageRecord, bufferSize),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	cfg := DefaultConfig()
	p.config.Store(&cfg)
	return p
}

// Start launches the drain goroutine. It is idempotent.
func (p *Plugin) Start() {
	p.startOnce.Do(func() { go p.run() })
}

// Shutdown stops accepting records, drains what is already queued, and waits
// for the worker to exit. It is idempotent and safe if Start was never called.
func (p *Plugin) Shutdown() {
	p.stopOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		close(p.stop)
		// Ensure the worker exists so done is always closed.
		p.Start()
	})
	<-p.done
}

// Config returns the current effective configuration.
func (p *Plugin) Config() Config {
	return *p.config.Load()
}

// HandleUsage enqueues a record without ever blocking. It reports whether the
// record was accepted; rejected records increment dropped_total.
func (p *Plugin) HandleUsage(rec usageRecord) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		p.droppedTotal.Add(1)
		return false
	}
	select {
	case p.records <- rec:
		return true
	default:
		p.droppedTotal.Add(1)
		return false
	}
}

func (p *Plugin) run() {
	defer close(p.done)
	for {
		select {
		case rec := <-p.records:
			p.process(rec)
		case <-p.stop:
			for {
				select {
				case rec := <-p.records:
					p.process(rec)
				default:
					return
				}
			}
		}
	}
}

// process is the storage hook. Step 1 only counts records.
func (p *Plugin) process(_ usageRecord) {
	p.recordsTotal.Add(1)
	p.lastRecordAt.Store(p.now().UTC().UnixNano())
}

// Status builds the status document served by the management route.
func (p *Plugin) Status() statusResponse {
	cfg := p.Config()
	status := statusResponse{
		Schema:         statusSchema,
		PluginVersion:  pluginVersion,
		RecordsTotal:   p.recordsTotal.Load(),
		DroppedTotal:   p.droppedTotal.Load(),
		RecordingSince: nil, // Populated once persistent storage exists (step 2).
		Retention: statusRetention{
			MinuteDays: cfg.MinuteRetentionDays,
			HourDays:   cfg.HourRetentionDays,
		},
	}
	if last := p.lastRecordAt.Load(); last != 0 {
		formatted := time.Unix(0, last).UTC().Format(time.RFC3339)
		status.LastRecordAt = &formatted
	}
	return status
}

// HandleMethod dispatches one C ABI call and returns a JSON envelope. A non-nil
// error is converted into a plugin_error envelope by the cgo glue.
func (p *Plugin) HandleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case methodPluginRegister, methodPluginReconfigure:
		return p.handleLifecycle(request)
	case methodUsageHandle:
		var rec usageRecord
		if errUnmarshal := json.Unmarshal(request, &rec); errUnmarshal != nil {
			return nil, fmt.Errorf("decode usage record: %w", errUnmarshal)
		}
		p.HandleUsage(rec)
		return okEnvelope(struct{}{})
	case methodManagementRegister:
		return okEnvelope(managementRegistrationResponse{Routes: []managementRoute{{
			Method:      http.MethodGet,
			Path:        statusRoutePath,
			Description: "Usage ledger ingest status.",
		}}})
	case methodManagementHandle:
		var req managementRequest
		if len(request) > 0 {
			if errUnmarshal := json.Unmarshal(request, &req); errUnmarshal != nil {
				return nil, fmt.Errorf("decode management request: %w", errUnmarshal)
			}
		}
		return okEnvelope(p.handleManagement(req))
	default:
		// The host treats unknown_method as "unsupported" (e.g. plugin.quiesce).
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

func (p *Plugin) handleLifecycle(request []byte) ([]byte, error) {
	var req lifecycleRequest
	if len(request) > 0 {
		if errUnmarshal := json.Unmarshal(request, &req); errUnmarshal != nil {
			return nil, fmt.Errorf("decode lifecycle request: %w", errUnmarshal)
		}
	}
	cfg, errParse := ParseConfig(req.ConfigYAML)
	if errParse != nil {
		return nil, errParse
	}
	p.config.Store(&cfg)
	p.Start()
	return okEnvelope(buildRegistration(req.SchemaVersion))
}

func buildRegistration(hostSchemaVersion uint32) registration {
	schema := pluginSchemaVersion
	if hostSchemaVersion != 0 && hostSchemaVersion < schema {
		schema = hostSchemaVersion
	}
	return registration{
		SchemaVersion: schema,
		Metadata: metadata{
			Name:             pluginName,
			Version:          pluginVersion,
			Author:           pluginAuthor,
			GitHubRepository: pluginRepository,
			ConfigFields:     configFields(),
		},
		Capabilities: capabilities{UsagePlugin: true, ManagementAPI: true},
	}
}

func configFields() []configField {
	return []configField{
		{
			Name:    "database-path",
			Type:    "string",
			Default: "",
			Description: "SQLite database file. Empty means usage-ledger.db in the CLIProxyAPI " +
				"working directory; relative paths resolve against that directory.",
		},
		{
			Name:        "minute-retention-days",
			Type:        "integer",
			Default:     defaultMinuteRetentionDays,
			Description: "Days to keep per-minute usage buckets.",
		},
		{
			Name:        "hour-retention-days",
			Type:        "integer",
			Default:     defaultHourRetentionDays,
			Description: "Days to keep per-hour usage buckets.",
		},
		{
			Name:        "price-source-url",
			Type:        "string",
			Default:     defaultPriceSourceURL,
			Description: "LiteLLM-format model price JSON used for cost estimates.",
		},
		{
			Name:        "price-refresh-hours",
			Type:        "integer",
			Default:     defaultPriceRefreshHours,
			Description: "Hours between price table refreshes.",
		},
	}
}

func (p *Plugin) handleManagement(req managementRequest) managementResponse {
	path := strings.TrimRight(strings.TrimSpace(req.Path), "/")
	if path != statusRouteFullPath {
		return jsonResponse(http.StatusNotFound, map[string]string{"error": "not found"})
	}
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method != "" && method != http.MethodGet {
		resp := jsonResponse(http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		resp.Headers.Set("Allow", http.MethodGet)
		return resp
	}
	return jsonResponse(http.StatusOK, p.Status())
}

func jsonResponse(status int, v any) managementResponse {
	body, errMarshal := json.Marshal(v)
	if errMarshal != nil {
		status = http.StatusInternalServerError
		body = []byte(`{"error":"failed to encode response"}`)
	}
	return managementResponse{
		StatusCode: status,
		Headers: http.Header{
			"Content-Type":  []string{"application/json; charset=utf-8"},
			"Cache-Control": []string{"no-store"},
		},
		Body: body,
	}
}

func okEnvelope(v any) ([]byte, error) {
	raw, errMarshal := json.Marshal(v)
	if errMarshal != nil {
		return nil, errMarshal
	}
	return json.Marshal(envelope{OK: true, Result: raw})
}

func errorEnvelope(code, message string) []byte {
	raw, errMarshal := json.Marshal(envelope{OK: false, Error: &envelopeError{Code: code, Message: message}})
	if errMarshal != nil {
		return []byte(`{"ok":false,"error":{"code":"plugin_error","message":"failed to encode error"}}`)
	}
	return raw
}

// ---- Process-wide instance used by the cgo glue ----

var (
	instanceMu sync.Mutex
	instance   *Plugin
)

// currentPlugin returns the live instance, creating one if needed (first init,
// or a re-init after shutdown).
func currentPlugin() *Plugin {
	instanceMu.Lock()
	defer instanceMu.Unlock()
	if instance == nil {
		instance = NewPlugin(usageBufferSize, time.Now)
		instance.Start()
	}
	return instance
}

// shutdownPlugin stops and discards the live instance, if any.
func shutdownPlugin() {
	instanceMu.Lock()
	p := instance
	instance = nil
	instanceMu.Unlock()
	if p != nil {
		p.Shutdown()
	}
}
