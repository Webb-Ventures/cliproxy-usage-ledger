package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---- Helpers ----

// newRoutePlugin registers a plugin with a fixed clock and price fetching
// disabled.
func newRoutePlugin(t *testing.T, now time.Time, extraYAML string) *Plugin {
	t.Helper()
	clock := &fakeClock{t: now}
	p := newTestPlugin(t, usageBufferSize, clock.Now)
	p.flushInterval = 10 * time.Millisecond // set before the worker starts
	registerTestPlugin(t, p, "price-source-url: \"\"\n"+extraYAML)
	return p
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	at, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return at
}

// ev is a successful claude request with input tokens only.
func ev(t *testing.T, at, model, auth string, tokens int64) ledgerEvent {
	return ledgerEvent{
		At: mustTime(t, at), Provider: "claude", Model: model, AuthIndex: auth, ClientKey: "k",
		Tokens: tokenBuckets{Input: tokens, Total: tokens},
	}
}

func seed(t *testing.T, p *Plugin, events ...ledgerEvent) {
	t.Helper()
	queryStore(t, p, func(s *Store) {
		if err := s.WriteBatch(events); err != nil {
			t.Fatalf("seed: %v", err)
		}
	})
}

func setRecordingSince(t *testing.T, p *Plugin, at string) {
	t.Helper()
	queryStore(t, p, func(s *Store) {
		if err := s.SetMeta(metaKeyRecordingSince, at); err != nil {
			t.Fatal(err)
		}
	})
}

// callRoute sends a management.handle request through the JSON envelope, as
// the host does.
func callRoute(t *testing.T, p *Plugin, method, route string, query url.Values, body []byte) managementResponse {
	t.Helper()
	raw, err := p.HandleMethod(methodManagementHandle, mustJSON(t, managementRequest{
		Method: method, Path: managementBasePath + "/" + route, Query: query, Body: body,
	}))
	if err != nil {
		t.Fatalf("management.handle: %v", err)
	}
	var env struct {
		OK     bool
		Result managementResponse
	}
	if err := json.Unmarshal(raw, &env); err != nil || !env.OK {
		t.Fatalf("envelope %s (err %v)", raw, err)
	}
	return env.Result
}

func q(pairs ...string) url.Values {
	v := url.Values{}
	for i := 0; i+1 < len(pairs); i += 2 {
		v.Set(pairs[i], pairs[i+1])
	}
	return v
}

// getJSON calls a GET route, requires 200, and decodes the body into out.
func getJSON(t *testing.T, p *Plugin, route string, query url.Values, out any) {
	t.Helper()
	resp := callRoute(t, p, http.MethodGet, route, query, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s?%s = %d %s", route, query.Encode(), resp.StatusCode, resp.Body)
	}
	if err := json.Unmarshal(resp.Body, out); err != nil {
		t.Fatalf("decode %s: %v", resp.Body, err)
	}
}

func expectError(t *testing.T, resp managementResponse, status int, code string) {
	t.Helper()
	var body errorBody
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		t.Fatalf("decode error body %s: %v", resp.Body, err)
	}
	if resp.StatusCode != status || body.Code != code || body.Error == "" {
		t.Fatalf("got %d %s, want %d code %q", resp.StatusCode, resp.Body, status, code)
	}
}

func bucketRequests(buckets []seriesBucket) []int64 {
	out := make([]int64, len(buckets))
	for i, b := range buckets {
		out[i] = b.Requests
	}
	return out
}

func bucketStarts(buckets []seriesBucket) []string {
	out := make([]string, len(buckets))
	for i, b := range buckets {
		out[i] = b.Start
	}
	return out
}

var routeNow = time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)

// ---- Timezone bucketing ----

func TestSeriesEuropeLondonAcrossOctoberDSTChange(t *testing.T) {
	// One day of minute retention forces hour rows for these dates.
	p := newRoutePlugin(t, routeNow, "minute-retention-days: 1\n")
	setRecordingSince(t, p, "2026-01-01T00:00:00Z")
	seed(t, p,
		ev(t, "2026-10-23T22:00:00Z", "m", "a", 1), // Oct 23 23:00 BST: before the range
		ev(t, "2026-10-24T22:59:00Z", "m", "a", 1), // Oct 24 23:59 BST
		ev(t, "2026-10-24T23:00:00Z", "m", "a", 1), // Oct 25 00:00 BST
		ev(t, "2026-10-25T23:30:00Z", "m", "a", 1), // Oct 25 23:30 GMT
		ev(t, "2026-10-26T00:00:00Z", "m", "a", 1), // Oct 26 00:00 GMT
	)
	var series seriesResponse
	getJSON(t, p, routeSeries, q("from", "2026-10-24", "to", "2026-10-26", "tz", "Europe/London", "bucket", "day"), &series)
	wantStarts := []string{"2026-10-24T00:00:00+01:00", "2026-10-25T00:00:00+01:00", "2026-10-26T00:00:00Z"}
	if got := bucketStarts(series.Buckets); fmt.Sprint(got) != fmt.Sprint(wantStarts) {
		t.Fatalf("starts = %v, want %v", got, wantStarts)
	}
	if got := bucketRequests(series.Buckets); fmt.Sprint(got) != "[1 2 1]" {
		t.Fatalf("requests = %v, want [1 2 1]", got)
	}
	// The fall-back day is 25 hours long.
	if start, end := mustTime(t, series.Buckets[1].Start), mustTime(t, series.Buckets[1].End); end.Sub(start) != 25*time.Hour {
		t.Fatalf("Oct 25 lasts %v", end.Sub(start))
	}
	if series.Range.Resolution != resolutionHour || series.Range.Approximate {
		t.Fatalf("range = %+v, want exact hour resolution", series.Range)
	}
	if series.Range.From != "2026-10-24T00:00:00+01:00" || series.Range.To != "2026-10-27T00:00:00Z" || series.Range.TZ != "Europe/London" {
		t.Fatalf("range = %+v", series.Range)
	}

	// Hour buckets on the fall-back day: 25 of them, 01:00 appears twice.
	var hourly seriesResponse
	getJSON(t, p, routeSeries, q("from", "2026-10-25", "to", "2026-10-25", "tz", "Europe/London", "bucket", "hour"), &hourly)
	if len(hourly.Buckets) != 25 {
		t.Fatalf("hour buckets = %d, want 25", len(hourly.Buckets))
	}
	if hourly.Buckets[1].Start != "2026-10-25T01:00:00+01:00" || hourly.Buckets[2].Start != "2026-10-25T01:00:00Z" {
		t.Fatalf("repeated hour = %s, %s", hourly.Buckets[1].Start, hourly.Buckets[2].Start)
	}
	if hourly.Buckets[0].Requests != 1 || hourly.Buckets[24].Requests != 1 {
		t.Fatalf("hour buckets 0 and 24 = %d, %d", hourly.Buckets[0].Requests, hourly.Buckets[24].Requests)
	}

	var summary summaryResponse
	getJSON(t, p, routeSummary, q("from", "2026-10-24", "to", "2026-10-26", "tz", "Europe/London"), &summary)
	if summary.Totals.Requests != 4 || summary.Range.Resolution != resolutionHour {
		t.Fatalf("summary totals = %+v range = %+v", summary.Totals, summary.Range)
	}
}

func TestBucketEdgesSpringForwardDayIs23Hours(t *testing.T) {
	london, _ := time.LoadLocation("Europe/London")
	from := startOfLocalDay(2027, 3, 28, london)
	edges, err := bucketEdges(from, from.Add(time.Hour), london, bucketDay)
	if err != nil {
		t.Fatal(err)
	}
	if len(edges) != 2 || edges[1].Sub(edges[0]) != 23*time.Hour {
		t.Fatalf("edges = %v", edges)
	}
	if got := formatTime(edges[1], london); got != "2027-03-29T00:00:00+01:00" {
		t.Fatalf("end = %s", got)
	}
}

func TestStartOfLocalDayWhenMidnightIsSkipped(t *testing.T) {
	// Chile springs forward at 24:00 on 2026-09-05: 2026-09-06 starts at 01:00.
	santiago, err := time.LoadLocation("America/Santiago")
	if err != nil {
		t.Skip("zone unavailable")
	}
	start := startOfLocalDay(2026, 9, 6, santiago)
	if y, m, d := start.Date(); y != 2026 || m != 9 || d != 6 {
		t.Fatalf("start = %s", start)
	}
	if before := start.Add(-time.Minute).In(santiago); before.Day() != 5 {
		t.Fatalf("a minute before %s is %s, want Sept 5", start, before)
	}
}

func TestKolkataForcesMinuteResolution(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "") // 14 days of minute rows
	setRecordingSince(t, p, "2026-09-01T00:00:00Z")
	seed(t, p,
		ev(t, "2026-10-19T18:15:00Z", "m", "a", 1), // Oct 19 23:45 IST
		ev(t, "2026-10-19T18:45:00Z", "m", "a", 1), // Oct 20 00:15 IST
		ev(t, "2026-10-21T18:29:00Z", "m", "a", 1), // Oct 21 23:59 IST
		ev(t, "2026-10-21T18:30:00Z", "m", "a", 1), // Oct 22 00:00 IST
		ev(t, "2026-10-01T10:00:00Z", "m", "a", 1), // outside minute retention
	)
	var series seriesResponse
	getJSON(t, p, routeSeries, q("from", "2026-10-20", "to", "2026-10-21", "tz", "Asia/Kolkata", "bucket", "day"), &series)
	if series.Range.Resolution != resolutionMinute || series.Range.Approximate {
		t.Fatalf("range = %+v, want exact minute resolution", series.Range)
	}
	if got := bucketStarts(series.Buckets); fmt.Sprint(got) != "[2026-10-20T00:00:00+05:30 2026-10-21T00:00:00+05:30]" {
		t.Fatalf("starts = %v", got)
	}
	// Hour rows would have put the 18:15Z request in Oct 20 as well.
	if got := bucketRequests(series.Buckets); fmt.Sprint(got) != "[1 1]" {
		t.Fatalf("requests = %v, want [1 1]", got)
	}

	// Past minute retention a half-hour zone mixes hour rows (approximate)
	// with minute rows.
	var summary summaryResponse
	getJSON(t, p, routeSummary, q("from", "2026-09-30", "to", "2026-10-21", "tz", "Asia/Kolkata"), &summary)
	if summary.Range.Resolution != resolutionMixed || !summary.Range.Approximate {
		t.Fatalf("range = %+v, want approximate mixed resolution", summary.Range)
	}
	if summary.Totals.Requests != 4 {
		t.Fatalf("requests = %d, want 4", summary.Totals.Requests)
	}
}

func TestNewYorkWeeksStartMonday(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	setRecordingSince(t, p, "2026-09-01T00:00:00Z")
	seed(t, p,
		ev(t, "2026-10-27T12:00:00Z", "m", "a", 1), // Tue Oct 27: before from
		ev(t, "2026-11-01T04:30:00Z", "m", "a", 1), // Sun Nov 1 00:30 EDT
		ev(t, "2026-11-02T04:30:00Z", "m", "a", 1), // Sun Nov 1 23:30 EST
		ev(t, "2026-11-02T05:00:00Z", "m", "a", 1), // Mon Nov 2 00:00 EST
	)
	var series seriesResponse
	getJSON(t, p, routeSeries, q("from", "2026-10-28", "to", "2026-11-03", "tz", "America/New_York", "bucket", "week"), &series)
	want := []string{"2026-10-26T00:00:00-04:00", "2026-11-02T00:00:00-05:00"}
	if got := bucketStarts(series.Buckets); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("starts = %v, want %v", got, want)
	}
	if got := bucketRequests(series.Buckets); fmt.Sprint(got) != "[2 1]" {
		t.Fatalf("requests = %v, want [2 1]", got)
	}
	if mustTime(t, series.Buckets[0].Start).Weekday() != time.Monday {
		t.Fatal("week does not start on Monday")
	}
	if d := mustTime(t, series.Buckets[0].End).Sub(mustTime(t, series.Buckets[0].Start)); d != 7*24*time.Hour+time.Hour {
		t.Fatalf("DST week lasts %v", d)
	}

	var days seriesResponse
	getJSON(t, p, routeSeries, q("from", "2026-11-01", "to", "2026-11-01", "tz", "America/New_York"), &days)
	if len(days.Buckets) != 1 || days.Buckets[0].Requests != 2 || days.Bucket != bucketDay {
		t.Fatalf("Nov 1 = %+v", days.Buckets)
	}
}

// ---- Range edges ----

func TestRangeToIsExclusive(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	seed(t, p, ev(t, "2026-10-27T10:00:00Z", "m", "a", 1), ev(t, "2026-10-27T11:00:00Z", "m", "a", 1))
	var summary summaryResponse
	getJSON(t, p, routeSummary, q("from", "2026-10-27T10:00:00Z", "to", "2026-10-27T11:00:00Z"), &summary)
	if summary.Totals.Requests != 1 {
		t.Fatalf("RFC 3339 to included the request at to: %d", summary.Totals.Requests)
	}
	getJSON(t, p, routeSummary, q("from", "2026-10-27", "to", "2026-10-27"), &summary)
	if summary.Totals.Requests != 2 || summary.Range.To != "2026-10-28T00:00:00Z" {
		t.Fatalf("date to should cover the whole day: %d, to=%s", summary.Totals.Requests, summary.Range.To)
	}
}

func TestRangeBeforeRecordingIsMarked(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	setRecordingSince(t, p, "2026-10-20T12:00:00Z")
	var series seriesResponse
	getJSON(t, p, routeSeries, q("from", "2026-10-18", "to", "2026-10-21"), &series)
	var flags []bool
	for _, b := range series.Buckets {
		flags = append(flags, b.BeforeRecording)
		if b.Requests != 0 || b.ByModel == nil {
			t.Fatalf("empty bucket = %+v", b)
		}
	}
	// Oct 20 contains recording_since, so it is not entirely before it.
	if fmt.Sprint(flags) != "[true true false false]" {
		t.Fatalf("before_recording = %v", flags)
	}
	if series.Range.RecordingSince == nil || *series.Range.RecordingSince != "2026-10-20T12:00:00Z" {
		t.Fatalf("recording_since = %v", series.Range.RecordingSince)
	}
	var summary summaryResponse
	getJSON(t, p, routeSummary, q("from", "2026-09-01", "to", "2026-09-30"), &summary)
	if summary.Totals.Requests != 0 || len(summary.Groups) != 0 || summary.Groups == nil {
		t.Fatalf("summary before recording = %+v", summary)
	}
}

func TestRangePastNowReturnsEmptyFutureBuckets(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	seed(t, p, ev(t, "2026-10-28T11:00:00Z", "m", "a", 7))
	var series seriesResponse
	getJSON(t, p, routeSeries, q("from", "2026-10-28", "to", "2026-10-30"), &series)
	if got := bucketRequests(series.Buckets); fmt.Sprint(got) != "[1 0 0]" {
		t.Fatalf("requests = %v", got)
	}
	// Far in the future (and across the spring DST change) still works.
	getJSON(t, p, routeSeries, q("from", "2027-03-27", "to", "2027-03-29", "tz", "Europe/London"), &series)
	if len(series.Buckets) != 3 || series.Buckets[1].End != "2027-03-29T00:00:00+01:00" {
		t.Fatalf("future buckets = %v", bucketStarts(series.Buckets))
	}
}

func TestRangeLimitIs400Days(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	var summary summaryResponse
	getJSON(t, p, routeSummary, q("from", "2025-09-23", "to", "2026-10-27"), &summary) // exactly 400 days
	expectError(t, callRoute(t, p, http.MethodGet, routeSummary, q("from", "2025-09-22", "to", "2026-10-27"), nil),
		http.StatusBadRequest, "range_too_large")
}

func TestRangeRejectsBadInput(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	cases := []struct {
		route string
		query url.Values
		code  string
	}{
		{routeSummary, q("to", "2026-10-27"), "missing_param"},
		{routeSummary, q("from", "2026-10-27"), "missing_param"},
		{routeSummary, q("from", "2026-13-01", "to", "2026-10-27"), "invalid_from"},
		{routeSummary, q("from", "yesterday", "to", "2026-10-27"), "invalid_from"},
		{routeSummary, q("from", "2026-10-01", "to", "2026-10-27T25:00:00Z"), "invalid_to"},
		{routeSummary, q("from", "2026-10-27", "to", "2026-10-26"), "invalid_range"},
		{routeSummary, q("from", "2026-10-27T10:00:00Z", "to", "2026-10-27T10:00:00Z"), "invalid_range"},
		{routeSummary, q("from", "2026-10-01", "to", "2026-10-27", "tz", "Mars/Olympus"), "invalid_tz"},
		{routeSummary, q("from", "2026-10-01", "to", "2026-10-27", "tz", "Local"), "invalid_tz"},
		{routeSummary, q("from", "2026-10-01", "to", "2026-10-27", "tz", "../../etc/passwd"), "invalid_tz"},
		{routeSummary, q("from", "2026-10-01", "to", "2026-10-27", "group", "user"), "invalid_group"},
		{routeSeries, q("from", "2026-10-01", "to", "2026-10-27", "bucket", "month"), "invalid_bucket"},
		{routeSeries, q("from", "2026-01-01", "to", "2026-10-27", "bucket", "hour"), "too_many_buckets"},
		{routeExport, q("from", "2026-10-27", "to", "2026-10-26"), "invalid_range"},
		{routeBlocks, q("from", "2026-10-27"), "missing_param"},
	}
	for _, tc := range cases {
		expectError(t, callRoute(t, p, http.MethodGet, tc.route, tc.query, nil), http.StatusBadRequest, tc.code)
	}
}

// ---- Summary ----

func TestSummaryGroupsAndModelBreakdown(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	premium := ev(t, "2026-10-27T10:00:00Z", "gpt-5.5", "a2", 50)
	premium.Provider, premium.Tier, premium.ClientKey = "codex", "priority", "key2"
	failed := ev(t, "2026-10-27T10:01:00Z", "claude-opus-5-5", "a1", 0)
	failed.Failed = true
	seed(t, p,
		ev(t, "2026-10-27T09:00:00Z", "claude-opus-5-5", "a1", 100),
		ev(t, "2026-10-27T09:30:00Z", "claude-sonnet-5-5", "a1", 30),
		premium, failed,
	)
	var summary summaryResponse
	getJSON(t, p, routeSummary, q("from", "2026-10-27", "to", "2026-10-27", "group", "credential"), &summary)
	if summary.Schema != 1 || summary.Group != groupCredential || len(summary.Groups) != 2 {
		t.Fatalf("summary = %+v", summary)
	}
	a1, a2 := summary.Groups[0], summary.Groups[1]
	if a1.Key != "auth:a1" || a1.Requests != 3 || a1.Failed != 1 || a1.Tokens.Total != 130 || a1.Tokens.Input != 130 {
		t.Fatalf("auth:a1 = %+v", a1)
	}
	if len(a1.ByModel) != 2 || a1.ByModel[0].Model != "claude-opus-5-5" || a1.ByModel[0].Requests != 2 || a1.ByModel[0].Failed != 1 {
		t.Fatalf("auth:a1 by_model = %+v", a1.ByModel)
	}
	if a2.Key != "auth:a2" || len(a2.ByModel) != 1 || a2.ByModel[0].ServiceTier != "priority" {
		t.Fatalf("auth:a2 = %+v", a2)
	}
	if summary.Totals.Requests != 4 || summary.Totals.Failed != 1 || summary.Totals.Tokens.Total != 180 || len(summary.Totals.ByModel) != 3 {
		t.Fatalf("totals = %+v", summary.Totals)
	}
	for group, wantKey := range map[string]string{"model": "model:claude-opus-5-5", "provider": "provider:claude", "client_key": "key:k"} {
		getJSON(t, p, routeSummary, q("from", "2026-10-27", "to", "2026-10-27", "group", group), &summary)
		if summary.Groups[0].Key != wantKey {
			t.Fatalf("group=%s first key = %s, want %s", group, summary.Groups[0].Key, wantKey)
		}
	}
	// group defaults to model.
	getJSON(t, p, routeSummary, q("from", "2026-10-27", "to", "2026-10-27"), &summary)
	if summary.Group != groupModel {
		t.Fatalf("default group = %q", summary.Group)
	}
}

func TestSeriesPerGroupIncludesEmptyBuckets(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	seed(t, p, ev(t, "2026-10-26T10:00:00Z", "m", "a1", 5), ev(t, "2026-10-27T10:00:00Z", "m", "a2", 7))
	var series seriesResponse
	getJSON(t, p, routeSeries, q("from", "2026-10-26", "to", "2026-10-27", "group", "credential"), &series)
	if series.Group == nil || *series.Group != groupCredential || len(series.Groups) != 2 {
		t.Fatalf("series = %+v", series)
	}
	if series.Groups[0].Key != "auth:a1" || fmt.Sprint(bucketRequests(series.Groups[0].Buckets)) != "[1 0]" ||
		fmt.Sprint(bucketRequests(series.Groups[1].Buckets)) != "[0 1]" {
		t.Fatalf("groups = %+v", series.Groups)
	}
	if fmt.Sprint(bucketRequests(series.Buckets)) != "[1 1]" || series.Buckets[1].ByModel[0].Tokens.Total != 7 {
		t.Fatalf("total buckets = %+v", series.Buckets)
	}
	// Without group there is no groups field.
	resp := callRoute(t, p, http.MethodGet, routeSeries, q("from", "2026-10-26", "to", "2026-10-27"), nil)
	if bytes.Contains(resp.Body, []byte(`"groups"`)) || !bytes.Contains(resp.Body, []byte(`"group":null`)) {
		t.Fatalf("ungrouped series body = %s", resp.Body)
	}
}

// ---- Windows ----

func postWindows(t *testing.T, p *Plugin, windows []windowRequest) (managementResponse, windowsResponse) {
	t.Helper()
	resp := callRoute(t, p, http.MethodPost, routeWindows, nil, mustJSON(t, windowsRequest{Windows: windows}))
	var out windowsResponse
	if resp.StatusCode == http.StatusOK {
		if err := json.Unmarshal(resp.Body, &out); err != nil {
			t.Fatal(err)
		}
	}
	return resp, out
}

func TestWindowsMinutePath(t *testing.T) {
	now := mustTime(t, "2026-10-28T12:00:30Z")
	p := newRoutePlugin(t, now, "")
	seed(t, p,
		ev(t, "2026-10-28T09:59:00Z", "m", "a1", 1000), // before since
		ev(t, "2026-10-28T10:00:00Z", "m", "a1", 10),
		ev(t, "2026-10-28T11:30:00Z", "m2", "a1", 20),
		ev(t, "2026-10-28T12:00:00Z", "m", "a1", 30), // the current minute counts
		ev(t, "2026-10-28T11:00:00Z", "m", "a2", 5),
	)
	resp, out := postWindows(t, p, []windowRequest{
		{AuthIndex: "a1", Since: "2026-10-28T10:00:00Z"},
		{AuthIndex: "a2", Since: "2026-10-28T10:00:00+00:00"},
		{AuthIndex: "unknown", Since: "2026-10-28T10:00:00Z"},
		{AuthIndex: "a1", Since: "2026-10-29T00:00:00Z"}, // in the future
	})
	if resp.StatusCode != http.StatusOK || len(out.Windows) != 4 || out.Now != "2026-10-28T12:00:30Z" {
		t.Fatalf("windows = %d %s", resp.StatusCode, resp.Body)
	}
	w := out.Windows[0]
	if w.AuthIndex != "a1" || w.Requests != 3 || w.Tokens.Total != 60 || len(w.ByModel) != 2 ||
		w.Resolution != resolutionMinute || w.Approximate {
		t.Fatalf("a1 window = %+v", w)
	}
	if out.Windows[1].Tokens.Total != 5 || out.Windows[2].Requests != 0 || out.Windows[2].ByModel == nil || out.Windows[3].Requests != 0 {
		t.Fatalf("windows = %+v", out.Windows)
	}
}

func TestWindowsApproximatePath(t *testing.T) {
	now := mustTime(t, "2026-10-28T12:00:30Z")
	p := newRoutePlugin(t, now, "minute-retention-days: 1\n")
	seed(t, p,
		ev(t, "2026-10-26T09:10:00Z", "m", "a1", 1), // before since, but its hour's midpoint is not
		ev(t, "2026-10-27T12:30:00Z", "m", "a1", 2), // hour rows (before the minute edge)
		ev(t, "2026-10-27T13:00:00Z", "m", "a1", 4), // minute rows
	)
	_, out := postWindows(t, p, []windowRequest{{AuthIndex: "a1", Since: "2026-10-26T09:30:00Z"}})
	w := out.Windows[0]
	if !w.Approximate || w.Resolution != resolutionMixed || w.Tokens.Total != 7 {
		t.Fatalf("window = %+v", w)
	}
	if out.MinuteDataSince != "2026-10-27T12:01:00Z" {
		t.Fatalf("minute_data_since = %s", out.MinuteDataSince)
	}
}

func TestWindowsEntryLimitAndValidation(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	windows := make([]windowRequest, maxWindows)
	for i := range windows {
		windows[i] = windowRequest{AuthIndex: fmt.Sprint(i), Since: "2026-10-28T00:00:00Z"}
	}
	if resp, out := postWindows(t, p, windows); resp.StatusCode != http.StatusOK || len(out.Windows) != maxWindows {
		t.Fatalf("500 windows = %d", resp.StatusCode)
	}
	resp, _ := postWindows(t, p, append(windows, windowRequest{AuthIndex: "x", Since: "2026-10-28T00:00:00Z"}))
	expectError(t, resp, http.StatusBadRequest, "too_many_windows")
	resp, _ = postWindows(t, p, []windowRequest{{AuthIndex: "a", Since: "last tuesday"}})
	expectError(t, resp, http.StatusBadRequest, "invalid_window")
	resp, _ = postWindows(t, p, []windowRequest{{AuthIndex: "a", Since: "2024-01-01T00:00:00Z"}})
	expectError(t, resp, http.StatusBadRequest, "invalid_window")
	expectError(t, callRoute(t, p, http.MethodPost, routeWindows, nil, []byte(`{"windows":[{"auth":"a"}]}`)), http.StatusBadRequest, "invalid_body")
	expectError(t, callRoute(t, p, http.MethodPost, routeWindows, nil, []byte(`not json`)), http.StatusBadRequest, "invalid_body")
	expectError(t, callRoute(t, p, http.MethodPost, routeWindows, nil, bytes.Repeat([]byte(" "), maxBodyBytes+1)), http.StatusBadRequest, "body_too_large")
}

// ---- Settings ----

func TestSettingsRoundTrip(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "minute-retention-days: 7\n")
	var got settingsResponse
	getJSON(t, p, routeSettings, nil, &got)
	if got.DefaultCurrency != "USD" || len(got.PlanOverrides) != 0 || got.PlanOverrides == nil ||
		got.UpdatedAt != nil || got.Retention.MinuteDays != 7 || got.Retention.HourDays != 400 {
		t.Fatalf("default settings = %+v", got)
	}
	input := 0.000003
	body := []byte(`{
		"plan_overrides": {"auth-1": {"provider": "claude", "plan": "max_20x", "monthly_price": 180, "currency": "EUR"},
		                   "auth-2": {"provider": "kimi", "plan": "moderato", "monthly_price": 19}},
		"model_price_overrides": {"kimi-k3": {"input": 0.000003, "output": 0.000012}},
		"default_currency": "GBP",
		"retention": {"minute_days": 1, "hour_days": 1}
	}`)
	resp := callRoute(t, p, http.MethodPut, routeSettings, nil, body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT = %d %s", resp.StatusCode, resp.Body)
	}
	getJSON(t, p, routeSettings, nil, &got)
	if got.DefaultCurrency != "GBP" || got.PlanOverrides["auth-1"].MonthlyPrice != 180 ||
		got.PlanOverrides["auth-2"].Currency != "GBP" || *got.ModelPriceOverrides["kimi-k3"].Input != input ||
		got.ModelPriceOverrides["kimi-k3"].CacheRead != nil || got.UpdatedAt == nil || *got.UpdatedAt != "2026-10-28T12:00:00Z" {
		t.Fatalf("settings after PUT = %+v", got)
	}
	// Retention stays read-only.
	if got.Retention.MinuteDays != 7 {
		t.Fatalf("retention changed: %+v", got.Retention)
	}
	// A GET document can be sent back unchanged.
	if resp := callRoute(t, p, http.MethodPut, routeSettings, nil, mustJSON(t, got)); resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT of a GET document = %d %s", resp.StatusCode, resp.Body)
	}
	// Overrides are served next to prices, not merged into them.
	var prices pricesResponse
	getJSON(t, p, routePrices, nil, &prices)
	if prices.Overrides == nil || prices.Overrides.PlanOverrides["auth-1"].Plan != "max_20x" {
		t.Fatalf("prices.overrides = %+v", prices.Overrides)
	}
	if _, merged := prices.Models["kimi-k3"]; merged {
		t.Fatal("model override merged into models")
	}
	// PUT replaces: an empty document clears everything.
	callRoute(t, p, http.MethodPut, routeSettings, nil, []byte(`{}`))
	var cleared settingsResponse
	getJSON(t, p, routeSettings, nil, &cleared)
	if len(cleared.PlanOverrides) != 0 || len(cleared.ModelPriceOverrides) != 0 || cleared.DefaultCurrency != "USD" {
		t.Fatalf("settings after empty PUT = %+v", cleared)
	}
}

func TestSettingsValidation(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	cases := map[string]string{
		"bad currency":     `{"default_currency": "usd"}`,
		"plan currency":    `{"plan_overrides": {"a": {"provider": "claude", "plan": "pro", "monthly_price": 20, "currency": "DOLLARS"}}}`,
		"missing provider": `{"plan_overrides": {"a": {"plan": "pro", "monthly_price": 20}}}`,
		"missing plan":     `{"plan_overrides": {"a": {"provider": "claude", "monthly_price": 20}}}`,
		"negative price":   `{"plan_overrides": {"a": {"provider": "claude", "plan": "pro", "monthly_price": -1}}}`,
		"empty key":        `{"plan_overrides": {" ": {"provider": "claude", "plan": "pro", "monthly_price": 1}}}`,
		"per-MTok price":   `{"model_price_overrides": {"m": {"input": 3}}}`,
		"negative token":   `{"model_price_overrides": {"m": {"output": -0.1}}}`,
		"empty override":   `{"model_price_overrides": {"m": {}}}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			expectError(t, callRoute(t, p, http.MethodPut, routeSettings, nil, []byte(body)), http.StatusBadRequest, "invalid_settings")
		})
	}
	for _, body := range []string{``, `[]`, `{"plan_overrides": 3}`, `{"unknown": 1}`, `{} {}`,
		`{"model_price_overrides": {"m": {"input": 0.1, "reasoning": 0.1}}}`} {
		expectError(t, callRoute(t, p, http.MethodPut, routeSettings, nil, []byte(body)), http.StatusBadRequest, "invalid_body")
	}
	var got settingsResponse
	getJSON(t, p, routeSettings, nil, &got)
	if got.UpdatedAt != nil {
		t.Fatal("a rejected PUT was stored")
	}
}

func TestSettingsBodyLimit(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	overrides := map[string]planOverride{}
	for i := 0; len(mustJSON(t, overrides)) <= maxBodyBytes; i++ {
		overrides[fmt.Sprintf("auth-%06d-%s", i, strings.Repeat("x", 200))] = planOverride{Provider: "claude", Plan: "pro", MonthlyPrice: 20}
	}
	body := mustJSON(t, map[string]any{"plan_overrides": overrides})
	expectError(t, callRoute(t, p, http.MethodPut, routeSettings, nil, body), http.StatusBadRequest, "body_too_large")
}

// ---- Export ----

func TestExportCSVEscaping(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	tricky := ev(t, "2026-10-27T10:00:00Z", `gpt,"5"`, "a1", 3)
	formula := ev(t, "2026-10-27T11:00:00Z", `=HYPERLINK("https://evil")`, "a1", 4)
	newline := ev(t, "2026-10-26T11:00:00Z", "line\nbreak", "a2", 5)
	seed(t, p, tricky, formula, newline)
	resp := callRoute(t, p, http.MethodGet, routeExport, q("from", "2026-10-26", "to", "2026-10-27", "group", "credential"), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("export = %d %s", resp.StatusCode, resp.Body)
	}
	if ct := resp.Headers.Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if cd := resp.Headers.Get("Content-Disposition"); cd != `attachment; filename="usage-ledger_2026-10-26_2026-10-27_day.csv"` {
		t.Fatalf("Content-Disposition = %q", cd)
	}
	if !bytes.Contains(resp.Body, []byte(`"gpt,""5"""`)) {
		t.Fatalf("quote/comma not escaped:\n%s", resp.Body)
	}
	records, err := csv.NewReader(bytes.NewReader(resp.Body)).ReadAll()
	if err != nil {
		t.Fatalf("CSV does not parse: %v\n%s", err, resp.Body)
	}
	if fmt.Sprint(records[0]) != fmt.Sprint(exportHeader) || len(records) != 4 {
		t.Fatalf("records = %q", records)
	}
	byModel := map[string][]string{}
	for _, r := range records[1:] {
		byModel[r[3]] = r
	}
	if r := byModel[`gpt,"5"`]; r == nil || r[0] != "2026-10-27T00:00:00Z" || r[1] != "2026-10-28T00:00:00Z" || r[2] != "auth:a1" || r[7] != "3" || r[13] != "3" {
		t.Fatalf("gpt row = %q", r)
	}
	if byModel[`'=HYPERLINK("https://evil")`] == nil {
		t.Fatalf("formula not neutralized: %q", records)
	}
	if byModel["line\nbreak"] == nil {
		t.Fatalf("newline field lost: %q", records)
	}
}

func TestCSVTextNeutralizesFormulas(t *testing.T) {
	for in, want := range map[string]string{"=1+1": "'=1+1", "+x": "'+x", "-x": "'-x", "@x": "'@x", "\tx": "'\tx", "gpt-5": "gpt-5", "": ""} {
		if got := csvText(in); got != want {
			t.Fatalf("csvText(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---- Routing and availability ----

func TestRoutesNotFoundAndMethodNotAllowed(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	expectError(t, callRoute(t, p, http.MethodGet, "usage-ledger/nope", nil, nil), http.StatusNotFound, "not_found")
	resp := callRoute(t, p, http.MethodDelete, routeSettings, nil, nil)
	expectError(t, resp, http.StatusMethodNotAllowed, "method_not_allowed")
	if allow := resp.Headers.Get("Allow"); allow != "GET, PUT" {
		t.Fatalf("Allow = %q", allow)
	}
	expectError(t, callRoute(t, p, http.MethodGet, routeWindows, nil, nil), http.StatusMethodNotAllowed, "method_not_allowed")
}

func TestRoutesUnavailableWithoutDatabase(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	if err := p.Quiesce(); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{routeSummary, routeSeries, routeBlocks, routeExport, routeSettings} {
		resp := callRoute(t, p, http.MethodGet, route, q("from", "2026-10-27", "to", "2026-10-27"), nil)
		expectError(t, resp, http.StatusServiceUnavailable, "unavailable")
	}
	expectError(t, callRoute(t, p, http.MethodPut, routeSettings, nil, []byte(`{}`)), http.StatusServiceUnavailable, "unavailable")
	// Prices need no database; overrides are null.
	var prices pricesResponse
	getJSON(t, p, routePrices, nil, &prices)
	if prices.Overrides != nil || len(prices.Models) == 0 {
		t.Fatalf("prices while quiesced: overrides=%v models=%d", prices.Overrides, len(prices.Models))
	}
}

// ---- Ingest rules visible through the routes ----

func TestGenerateFalseRecordsAreSkipped(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	no, yes := false, true
	at := routeNow.Add(-time.Hour)
	p.HandleUsage(usageRecord{Provider: "claude", Model: "m", RequestedAt: at, Generate: &no})
	p.HandleUsage(usageRecord{Provider: "claude", Model: "m", RequestedAt: at, Generate: &yes})
	p.HandleUsage(usageRecord{Provider: "claude", Model: "m", RequestedAt: at}) // omitted: generation
	if _, err := p.HandleMethod(methodUsageHandle, []byte(`{"Provider":"claude","Model":"m","Generate":false,"RequestedAt":"2026-10-28T11:00:00Z"}`)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "records", func() bool { return p.recordsTotal.Load() == 2 })
	if st := p.Status(); st.SkippedTotal != 2 || st.DroppedTotal != 0 {
		t.Fatalf("skipped=%d dropped=%d", st.SkippedTotal, st.DroppedTotal)
	}
	var summary summaryResponse
	getJSON(t, p, routeSummary, q("from", "2026-10-28", "to", "2026-10-28"), &summary)
	if summary.Totals.Requests != 2 {
		t.Fatalf("requests = %d, want 2", summary.Totals.Requests)
	}
}

func TestResponseModelAndServiceTierReachTheRoutes(t *testing.T) {
	p := newRoutePlugin(t, routeNow, "")
	at := routeNow.Add(-time.Hour)
	p.HandleUsage(usageRecord{Provider: "codex", Model: "gpt-5.5", ResponseModel: "gpt-5.5-2026-09-01",
		ServiceTier: "auto", ResponseServiceTier: "default", RequestedAt: at, Detail: usageDetail{InputTokens: 10}})
	p.HandleUsage(usageRecord{Provider: "codex", Model: "gpt-5.5", ServiceTier: "priority", RequestedAt: at})
	waitFor(t, "records", func() bool { return p.recordsTotal.Load() == 2 })
	var summary summaryResponse
	getJSON(t, p, routeSummary, q("from", "2026-10-28", "to", "2026-10-28"), &summary)
	got := map[string]string{}
	for _, m := range summary.Totals.ByModel {
		got[m.Model] = m.ServiceTier
	}
	if got["gpt-5.5-2026-09-01"] != "default" || got["gpt-5.5"] != "priority" || len(got) != 2 {
		t.Fatalf("models = %v", got)
	}
}

// ---- Concurrency: reads never race the worker's open/swap/close ----

func TestReadsDuringIngestSwapAndQuiesce(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	p.flushInterval = 5 * time.Millisecond
	registerTestPlugin(t, p, "price-source-url: \"\"\n")
	wd, _ := p.getwd()
	stop := make(chan struct{})
	var unexpected atomic.Value
	var reads atomic.Int64
	var wg sync.WaitGroup
	today := time.Now().UTC().Format(localDateForm)
	routes := []struct {
		method, route string
		query         url.Values
		body          []byte
	}{
		{http.MethodGet, routeSummary, q("from", today, "to", today, "group", "credential"), nil},
		{http.MethodGet, routeSeries, q("from", today, "to", today, "bucket", "hour"), nil},
		{http.MethodGet, routeBlocks, q("from", today, "to", today), nil},
		{http.MethodGet, routeExport, q("from", today, "to", today), nil},
		{http.MethodGet, routeSettings, nil, nil},
		{http.MethodGet, routePrices, nil, nil},
		{http.MethodGet, routeStatus, nil, nil},
		{http.MethodPost, routeWindows, nil, []byte(`{"windows":[{"auth_index":"idx","since":"` + time.Now().UTC().Add(-time.Hour).Format(time.RFC3339) + `"}]}`)},
		{http.MethodPut, routeSettings, nil, []byte(`{"default_currency":"EUR"}`)},
	}
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				r := routes[(i+n)%len(routes)]
				resp := p.handleManagement(managementRequest{Method: r.method, Path: managementBasePath + "/" + r.route, Query: r.query, Body: r.body})
				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusServiceUnavailable {
					unexpected.Store(fmt.Sprintf("%s %s: %d %s", r.method, r.route, resp.StatusCode, resp.Body))
				}
				reads.Add(1)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				p.HandleUsage(testRecord(time.Now()))
				time.Sleep(100 * time.Microsecond)
			}
		}
	}()
	paths := []string{"ledger.db", wd + "/moved/ledger2.db"}
	for i := 0; i < 6; i++ {
		cfg := []byte("price-source-url: \"\"\ndatabase-path: " + paths[i%2] + "\n")
		if _, err := p.HandleMethod(methodPluginReconfigure, mustJSON(t, lifecycleRequest{ConfigYAML: cfg})); err != nil {
			t.Fatalf("reconfigure: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
		if err := p.Quiesce(); err != nil {
			t.Fatalf("quiesce: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := p.HandleMethod(methodPluginReconfigure, mustJSON(t, lifecycleRequest{ConfigYAML: []byte("price-source-url: \"\"\ndatabase-path: ledger.db\n")})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
	if msg := unexpected.Load(); msg != nil {
		t.Fatalf("unexpected response: %v", msg)
	}
	if reads.Load() < 50 {
		t.Fatalf("only %d reads ran", reads.Load())
	}
	if p.writeErrorsTotal.Load() != 0 {
		t.Fatalf("write errors: %v", *p.lastWriteError.Load())
	}
}
