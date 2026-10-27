package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// routeSchema versions every JSON document served under usage-ledger/.
const routeSchema = 1

const (
	routeStatus   = "usage-ledger/status"
	routeSummary  = "usage-ledger/summary"
	routeSeries   = "usage-ledger/series"
	routeWindows  = "usage-ledger/windows"
	routeBlocks   = "usage-ledger/blocks"
	routePrices   = "usage-ledger/prices"
	routeSettings = "usage-ledger/settings"
	routeExport   = "usage-ledger/export"

	maxWindows       = 500
	maxBodyBytes     = 256 << 10
	blockSessionTime = 5 * time.Hour
)

type routeHandler func(p *Plugin, req managementRequest) managementResponse

type routeSpec struct {
	method      string
	path        string // relative, as declared to the host
	description string
	handler     routeHandler
}

// routeTable lists every management route. The host matches method and exact
// path (internal/pluginhost/management.go RegisterManagementRoutes), so each
// method is declared separately.
var routeTable = []routeSpec{
	{http.MethodGet, routeStatus, "Usage ledger ingest status.", (*Plugin).handleStatus},
	{http.MethodGet, routeSummary, "Token totals for a range, grouped by model, credential, provider, or client key.", (*Plugin).handleSummary},
	{http.MethodGet, routeSeries, "Token totals per hour, day, or week.", (*Plugin).handleSeries},
	{http.MethodPost, routeWindows, "Tokens per credential since each limit window started.", (*Plugin).handleWindows},
	{http.MethodGet, routeBlocks, "5-hour usage blocks.", (*Plugin).handleBlocks},
	{http.MethodGet, routePrices, "Model and plan prices.", (*Plugin).handlePrices},
	{http.MethodGet, routeSettings, "Plan and price overrides.", (*Plugin).handleGetSettings},
	{http.MethodPut, routeSettings, "Replace plan and price overrides.", (*Plugin).handlePutSettings},
	{http.MethodGet, routeExport, "CSV export of the series rows.", (*Plugin).handleExport},
}

func managementRoutes() []managementRoute {
	routes := make([]managementRoute, 0, len(routeTable))
	for _, route := range routeTable {
		routes = append(routes, managementRoute{Method: route.method, Path: route.path, Description: route.description})
	}
	return routes
}

func (p *Plugin) handleManagement(req managementRequest) managementResponse {
	path := strings.TrimRight(strings.TrimSpace(req.Path), "/")
	method := strings.ToUpper(strings.TrimSpace(req.Method))
	if method == "" {
		method = http.MethodGet
	}
	var allowed []string
	for _, route := range routeTable {
		if managementBasePath+"/"+route.path != path {
			continue
		}
		if route.method == method {
			return route.handler(p, req)
		}
		allowed = append(allowed, route.method)
	}
	if len(allowed) == 0 {
		return errorResponse(&apiError{status: http.StatusNotFound, code: "not_found", message: "not found"})
	}
	resp := errorResponse(&apiError{status: http.StatusMethodNotAllowed, code: "method_not_allowed", message: "method not allowed"})
	resp.Headers.Set("Allow", strings.Join(allowed, ", "))
	return resp
}

func (p *Plugin) handleStatus(managementRequest) managementResponse {
	return jsonResponse(http.StatusOK, p.Status())
}

type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func errorResponse(err *apiError) managementResponse {
	return jsonResponse(err.status, errorBody{Error: err.message, Code: err.code})
}

// readError maps a read-side failure to a response.
func readError(err error) managementResponse {
	var apiErr *apiError
	switch {
	case errors.As(err, &apiErr):
		return errorResponse(apiErr)
	case errors.Is(err, errNoDatabase):
		return errorResponse(&apiError{status: http.StatusServiceUnavailable, code: "unavailable", message: "no database is open"})
	default:
		return errorResponse(&apiError{status: http.StatusInternalServerError, code: "internal", message: err.Error()})
	}
}

// ---- Accumulation ----

type modelKey struct{ Model, ServiceTier string }

type modelUsage struct {
	Model       string      `json:"model"`
	ServiceTier string      `json:"service_tier"`
	Requests    int64       `json:"requests"`
	Failed      int64       `json:"failed"`
	Tokens      tokenTotals `json:"tokens"`
}

// usageTotals is the shared {requests, failed, tokens, by_model} block.
type usageTotals struct {
	Requests int64        `json:"requests"`
	Failed   int64        `json:"failed"`
	Tokens   tokenTotals  `json:"tokens"`
	ByModel  []modelUsage `json:"by_model"`
}

type usageAcc struct {
	requests, failed int64
	tokens           tokenTotals
	models           map[modelKey]*modelUsage
}

func (a *usageAcc) add(r aggRow) {
	a.requests += r.Requests
	a.failed += r.Failed
	a.tokens.add(r.Tokens)
	if a.models == nil {
		a.models = make(map[modelKey]*modelUsage)
	}
	key := modelKey{r.Model, r.ServiceTier}
	m := a.models[key]
	if m == nil {
		m = &modelUsage{Model: r.Model, ServiceTier: r.ServiceTier}
		a.models[key] = m
	}
	m.Requests += r.Requests
	m.Failed += r.Failed
	m.Tokens.add(r.Tokens)
}

// totals renders the accumulator; by_model is sorted by total tokens
// (descending), then model and tier.
func (a *usageAcc) totals() usageTotals {
	out := usageTotals{Requests: a.requests, Failed: a.failed, Tokens: a.tokens, ByModel: make([]modelUsage, 0, len(a.models))}
	for _, m := range a.models {
		out.ByModel = append(out.ByModel, *m)
	}
	sort.Slice(out.ByModel, func(i, j int) bool {
		x, y := out.ByModel[i], out.ByModel[j]
		if x.Tokens.Total != y.Tokens.Total {
			return x.Tokens.Total > y.Tokens.Total
		}
		if x.Model != y.Model {
			return x.Model < y.Model
		}
		return x.ServiceTier < y.ServiceTier
	})
	return out
}

const (
	groupModel      = "model"
	groupCredential = "credential"
	groupProvider   = "provider"
	groupClientKey  = "client_key"
)

func parseGroup(raw string, required bool) (string, *apiError) {
	switch g := strings.TrimSpace(raw); g {
	case groupModel, groupCredential, groupProvider, groupClientKey:
		return g, nil
	case "":
		if required {
			return groupModel, nil
		}
		return "", nil
	default:
		return "", badRequest("invalid_group", "group must be model, credential, provider, or client_key")
	}
}

func groupKey(group string, r aggRow) string {
	switch group {
	case groupCredential:
		return "auth:" + r.AuthIndex
	case groupProvider:
		return "provider:" + r.Provider
	case groupClientKey:
		return "key:" + r.ClientKey
	default:
		return "model:" + r.Model
	}
}

// ---- Range block ----

type rangeInfo struct {
	From           string  `json:"from"`
	To             string  `json:"to"`
	TZ             string  `json:"tz"`
	RecordingSince *string `json:"recording_since"`
	Resolution     string  `json:"resolution"`
	Approximate    bool    `json:"approximate"`
}

func newRangeInfo(tr timeRange, recordingSince time.Time, plan readPlan) rangeInfo {
	info := rangeInfo{
		From:        formatTime(tr.From, tr.Loc),
		To:          formatTime(tr.To, tr.Loc),
		TZ:          tr.TZ,
		Resolution:  plan.resolution,
		Approximate: plan.approximate,
	}
	if !recordingSince.IsZero() {
		formatted := recordingSince.UTC().Format(time.RFC3339)
		info.RecordingSince = &formatted
	}
	return info
}

// ---- Summary ----

type summaryGroup struct {
	Key string `json:"key"`
	usageTotals
}

type summaryResponse struct {
	Schema int            `json:"schema"`
	Range  rangeInfo      `json:"range"`
	Group  string         `json:"group"`
	Groups []summaryGroup `json:"groups"`
	Totals usageTotals    `json:"totals"`
}

func (p *Plugin) handleSummary(req managementRequest) managementResponse {
	q := url.Values(req.Query)
	tr, errRange := parseRange(q)
	if errRange != nil {
		return errorResponse(errRange)
	}
	group, errGroup := parseGroup(q.Get("group"), true)
	if errGroup != nil {
		return errorResponse(errGroup)
	}
	cfg := p.Config()
	plan := planRead(tr.From, tr.To, minuteDataSince(p.now(), cfg.MinuteRetentionDays), edgesAligned(tr.From, tr.To))
	var resp summaryResponse
	errRead := p.withReader(func(ctx context.Context, r *storeReader) error {
		since, errSince := r.recordingSince(ctx)
		if errSince != nil {
			return errSince
		}
		rows, errRows := r.scanRows(ctx, plan.segments, false, nil)
		if errRows != nil {
			return errRows
		}
		var total usageAcc
		groups := map[string]*usageAcc{}
		for _, row := range rows {
			total.add(row)
			key := groupKey(group, row)
			acc := groups[key]
			if acc == nil {
				acc = &usageAcc{}
				groups[key] = acc
			}
			acc.add(row)
		}
		resp = summaryResponse{
			Schema: routeSchema,
			Range:  newRangeInfo(tr, since, plan),
			Group:  group,
			Groups: sortedGroups(groups),
			Totals: total.totals(),
		}
		return nil
	})
	if errRead != nil {
		return readError(errRead)
	}
	return jsonResponse(http.StatusOK, resp)
}

// sortedGroups orders groups by total tokens (descending), then key.
func sortedGroups(groups map[string]*usageAcc) []summaryGroup {
	out := make([]summaryGroup, 0, len(groups))
	for key, acc := range groups {
		out = append(out, summaryGroup{Key: key, usageTotals: acc.totals()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tokens.Total != out[j].Tokens.Total {
			return out[i].Tokens.Total > out[j].Tokens.Total
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// ---- Series ----

type seriesBucket struct {
	Start           string `json:"start"`
	End             string `json:"end"`
	BeforeRecording bool   `json:"before_recording"`
	usageTotals
}

type seriesGroup struct {
	Key     string         `json:"key"`
	Buckets []seriesBucket `json:"buckets"`
}

type seriesResponse struct {
	Schema  int            `json:"schema"`
	Range   rangeInfo      `json:"range"`
	Bucket  string         `json:"bucket"`
	Group   *string        `json:"group"`
	Buckets []seriesBucket `json:"buckets"`
	Groups  []seriesGroup  `json:"groups,omitempty"`
}

// seriesData is the computed series, shared by the series and export routes.
type seriesData struct {
	tr             timeRange
	bucket         string
	group          string
	edges          []time.Time
	plan           readPlan
	recordingSince time.Time
	total          []usageAcc
	groups         map[string][]usageAcc
}

func (p *Plugin) computeSeries(q url.Values) (*seriesData, error) {
	tr, errRange := parseRange(q)
	if errRange != nil {
		return nil, errRange
	}
	bucket := strings.TrimSpace(q.Get("bucket"))
	if bucket == "" {
		bucket = bucketDay
	}
	group, errGroup := parseGroup(q.Get("group"), false)
	if errGroup != nil {
		return nil, errGroup
	}
	edges, errEdges := bucketEdges(tr.From, tr.To, tr.Loc, bucket)
	if errEdges != nil {
		return nil, errEdges
	}
	cfg := p.Config()
	aligned := edgesAligned(append([]time.Time{tr.From, tr.To}, edges...)...)
	plan := planRead(tr.From, tr.To, minuteDataSince(p.now(), cfg.MinuteRetentionDays), aligned)
	data := &seriesData{tr: tr, bucket: bucket, group: group, edges: edges, plan: plan,
		total: make([]usageAcc, len(edges)-1), groups: map[string][]usageAcc{}}
	errRead := p.withReader(func(ctx context.Context, r *storeReader) error {
		since, errSince := r.recordingSince(ctx)
		if errSince != nil {
			return errSince
		}
		data.recordingSince = since
		rows, errRows := r.scanRows(ctx, plan.segments, true, nil)
		if errRows != nil {
			return errRows
		}
		for _, row := range rows {
			i := bucketIndex(edges, row.Pos)
			if i < 0 {
				continue
			}
			data.total[i].add(row)
			if group != "" {
				key := groupKey(group, row)
				accs := data.groups[key]
				if accs == nil {
					accs = make([]usageAcc, len(edges)-1)
					data.groups[key] = accs
				}
				accs[i].add(row)
			}
		}
		return nil
	})
	if errRead != nil {
		return nil, errRead
	}
	return data, nil
}

func (d *seriesData) buckets(accs []usageAcc) []seriesBucket {
	out := make([]seriesBucket, len(accs))
	for i := range accs {
		out[i] = seriesBucket{
			Start:           formatTime(d.edges[i], d.tr.Loc),
			End:             formatTime(d.edges[i+1], d.tr.Loc),
			BeforeRecording: !d.recordingSince.IsZero() && !d.edges[i+1].After(d.recordingSince),
			usageTotals:     accs[i].totals(),
		}
	}
	return out
}

func (d *seriesData) groupKeys() []string {
	keys := make([]string, 0, len(d.groups))
	for key := range d.groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (p *Plugin) handleSeries(req managementRequest) managementResponse {
	data, errSeries := p.computeSeries(url.Values(req.Query))
	if errSeries != nil {
		return readError(errSeries)
	}
	resp := seriesResponse{
		Schema:  routeSchema,
		Range:   newRangeInfo(data.tr, data.recordingSince, data.plan),
		Bucket:  data.bucket,
		Buckets: data.buckets(data.total),
	}
	if data.group != "" {
		group := data.group
		resp.Group = &group
		resp.Groups = []seriesGroup{}
		for _, key := range data.groupKeys() {
			resp.Groups = append(resp.Groups, seriesGroup{Key: key, Buckets: data.buckets(data.groups[key])})
		}
	}
	return jsonResponse(http.StatusOK, resp)
}

// ---- Export ----

var exportHeader = []string{
	"bucket_start", "bucket_end", "group", "model", "service_tier", "requests", "failed",
	"input_tokens", "cache_read_tokens", "cache_write_tokens", "output_tokens",
	"reasoning_tokens", "unclassified_tokens", "total_tokens",
}

func (p *Plugin) handleExport(req managementRequest) managementResponse {
	data, errSeries := p.computeSeries(url.Values(req.Query))
	if errSeries != nil {
		return readError(errSeries)
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write(exportHeader)
	writeRows := func(groupKey string, accs []usageAcc) {
		for _, b := range data.buckets(accs) {
			for _, m := range b.ByModel {
				_ = w.Write([]string{
					b.Start, b.End, csvText(groupKey), csvText(m.Model), csvText(m.ServiceTier),
					itoa(m.Requests), itoa(m.Failed), itoa(m.Tokens.Input), itoa(m.Tokens.CacheRead),
					itoa(m.Tokens.CacheWrite), itoa(m.Tokens.Output), itoa(m.Tokens.Reasoning),
					itoa(m.Tokens.Unclassified), itoa(m.Tokens.Total),
				})
			}
		}
	}
	if data.group == "" {
		writeRows("", data.total)
	} else {
		for _, key := range data.groupKeys() {
			writeRows(key, data.groups[key])
		}
	}
	w.Flush()
	lastDay := data.tr.To.Add(-time.Nanosecond)
	filename := "usage-ledger_" + data.tr.From.In(data.tr.Loc).Format(localDateForm) + "_" +
		lastDay.In(data.tr.Loc).Format(localDateForm) + "_" + data.bucket + ".csv"
	return managementResponse{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type":        []string{"text/csv; charset=utf-8"},
			"Content-Disposition": []string{`attachment; filename="` + filename + `"`},
			"Cache-Control":       []string{"no-store"},
		},
		Body: buf.Bytes(),
	}
}

// csvText neutralizes spreadsheet formulas: model names and keys come from
// clients, so a value starting with = + - @ or a control character is
// prefixed with a single quote. encoding/csv handles quoting and escaping.
func csvText(s string) string {
	if s != "" && strings.ContainsRune("=+-@\t\r", rune(s[0])) {
		return "'" + s
	}
	return s
}

func itoa(v int64) string { return strconv.FormatInt(v, 10) }

// ---- Windows ----

type windowsRequest struct {
	Windows []windowRequest `json:"windows"`
}

type windowRequest struct {
	AuthIndex string `json:"auth_index"`
	Since     string `json:"since"`
}

type windowResult struct {
	AuthIndex   string `json:"auth_index"`
	Since       string `json:"since"`
	Resolution  string `json:"resolution"`
	Approximate bool   `json:"approximate"`
	usageTotals
}

type windowsResponse struct {
	Schema          int            `json:"schema"`
	Now             string         `json:"now"`
	MinuteDataSince string         `json:"minute_data_since"`
	Windows         []windowResult `json:"windows"`
}

func (p *Plugin) handleWindows(req managementRequest) managementResponse {
	if len(req.Body) > maxBodyBytes {
		return errorResponse(badRequest("body_too_large", "the request body is limited to 256 KB"))
	}
	var body windowsRequest
	if errDecode := decodeStrict(req.Body, &body); errDecode != nil {
		return errorResponse(badRequest("invalid_body", "invalid JSON body: "+errDecode.Error()))
	}
	if len(body.Windows) > maxWindows {
		return errorResponse(badRequest("too_many_windows", "at most 500 windows per request"))
	}
	now := p.now().UTC()
	oldest := now.AddDate(0, 0, -maxRangeDays)
	sinces := make([]time.Time, len(body.Windows))
	for i, w := range body.Windows {
		since, errParse := time.Parse(time.RFC3339Nano, strings.TrimSpace(w.Since))
		if errParse != nil {
			return errorResponse(badRequest("invalid_window", "windows["+strconv.Itoa(i)+"].since must be RFC 3339"))
		}
		if since.Before(oldest) {
			return errorResponse(badRequest("invalid_window", "windows["+strconv.Itoa(i)+"].since is more than 400 days ago"))
		}
		sinces[i] = since
	}
	minuteSince := minuteDataSince(now, p.Config().MinuteRetentionDays)
	// "until now" includes the current minute.
	end := time.Unix(ceilUnix(now.Unix()+1, 60), 0).UTC()
	resp := windowsResponse{
		Schema:          routeSchema,
		Now:             now.Format(time.RFC3339),
		MinuteDataSince: minuteSince.Format(time.RFC3339),
		Windows:         make([]windowResult, len(body.Windows)),
	}
	errRead := p.withReader(func(ctx context.Context, r *storeReader) error {
		for i, w := range body.Windows {
			since := sinces[i]
			result := windowResult{AuthIndex: w.AuthIndex, Since: since.UTC().Format(time.RFC3339Nano), Resolution: resolutionMinute}
			var acc usageAcc
			if since.Before(end) {
				// Minute rows are exact; the hour fallback is never exact
				// because a window starts at an arbitrary instant.
				plan := planRead(since, end, minuteSince, false)
				result.Resolution, result.Approximate = plan.resolution, plan.approximate
				authIndex := w.AuthIndex
				rows, errRows := r.scanRows(ctx, plan.segments, false, &authIndex)
				if errRows != nil {
					return errRows
				}
				for _, row := range rows {
					acc.add(row)
				}
			}
			result.usageTotals = acc.totals()
			resp.Windows[i] = result
		}
		return nil
	})
	if errRead != nil {
		return readError(errRead)
	}
	return jsonResponse(http.StatusOK, resp)
}

// decodeStrict decodes one JSON value and rejects unknown fields and trailing
// data.
func decodeStrict(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if errDecode := dec.Decode(v); errDecode != nil {
		return errDecode
	}
	if dec.More() {
		return errors.New("unexpected data after the JSON value")
	}
	return nil
}

// ---- Blocks ----

type blocksRange struct {
	From            string  `json:"from"`
	To              string  `json:"to"`
	RequestedFrom   string  `json:"requested_from"`
	TZ              string  `json:"tz"`
	Clamped         bool    `json:"clamped"`
	MinuteDataSince string  `json:"minute_data_since"`
	RecordingSince  *string `json:"recording_since"`
	Resolution      string  `json:"resolution"`
}

type blocksResponse struct {
	Schema       int          `json:"schema"`
	Range        blocksRange  `json:"range"`
	SessionHours int          `json:"session_hours"`
	Blocks       []usageBlock `json:"blocks"`
}

func (p *Plugin) handleBlocks(req managementRequest) managementResponse {
	tr, errRange := parseRange(url.Values(req.Query))
	if errRange != nil {
		return errorResponse(errRange)
	}
	now := p.now().UTC()
	minuteSince := minuteDataSince(now, p.Config().MinuteRetentionDays)
	from := tr.From
	clamped := from.Before(minuteSince)
	if clamped {
		from = minuteSince
	}
	resp := blocksResponse{
		Schema: routeSchema,
		Range: blocksRange{
			From:            formatTime(from, tr.Loc),
			To:              formatTime(tr.To, tr.Loc),
			RequestedFrom:   formatTime(tr.From, tr.Loc),
			TZ:              tr.TZ,
			Clamped:         clamped,
			MinuteDataSince: formatTime(minuteSince, tr.Loc),
			Resolution:      resolutionMinute,
		},
		SessionHours: int(blockSessionTime / time.Hour),
		Blocks:       []usageBlock{},
	}
	errRead := p.withReader(func(ctx context.Context, r *storeReader) error {
		since, errSince := r.recordingSince(ctx)
		if errSince != nil {
			return errSince
		}
		if !since.IsZero() {
			formatted := since.Format(time.RFC3339)
			resp.Range.RecordingSince = &formatted
		}
		if !from.Before(tr.To) {
			return nil
		}
		// Block boundaries depend on all earlier activity, so detection always
		// starts at the oldest minute row; only the blocks that overlap the
		// requested range are returned.
		rows, errRows := r.scanRows(ctx, []segment{{table: tableUsageMinute, lo: minuteSince.Unix(), hi: tr.To.Unix()}}, true, nil)
		if errRows != nil {
			return errRows
		}
		for _, block := range detectBlocks(rows, now) {
			if block.end.After(from) && block.start.Before(tr.To) {
				resp.Blocks = append(resp.Blocks, block.render(tr.Loc))
			}
		}
		return nil
	})
	if errRead != nil {
		return readError(errRead)
	}
	return jsonResponse(http.StatusOK, resp)
}
