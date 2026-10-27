package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func openTestStore(t *testing.T, now time.Time) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ledger.db")
	s, err := OpenStore(path, now)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, path
}

type rollupRow struct {
	BucketStart                                                   time.Time
	Provider, Model, AuthIndex, ClientKey                         string
	Failed                                                        bool
	Requests, Failures                                            int64
	Input, Output, CacheRead, CacheCreation, Reasoning, Unclassif int64
	Total, LatencyMs                                              int64
}

func readRollups(t *testing.T, s *Store, table string) []rollupRow {
	t.Helper()
	rows, err := s.db.Query(`SELECT ` + strings.Join(rollupColumns, ", ") + ` FROM ` + table +
		` ORDER BY bucket_start, provider, model, auth_index, client_key, failed`)
	if err != nil {
		t.Fatalf("query %s: %v", table, err)
	}
	defer rows.Close()
	var out []rollupRow
	for rows.Next() {
		var r rollupRow
		var bucket int64
		if err := rows.Scan(&bucket, &r.Provider, &r.Model, &r.AuthIndex, &r.ClientKey, &r.Failed,
			&r.Requests, &r.Failures, &r.Input, &r.Output, &r.CacheRead, &r.CacheCreation,
			&r.Reasoning, &r.Unclassif, &r.Total, &r.LatencyMs); err != nil {
			t.Fatalf("scan: %v", err)
		}
		r.BucketStart = time.Unix(bucket, 0).UTC()
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

// countRequests sums request_count in table of the database at path, opening
// its own connection.
func countRequests(t *testing.T, path, table string) int64 {
	t.Helper()
	s, err := OpenStore(path, time.Now())
	if err != nil {
		t.Fatalf("OpenStore(%s): %v", path, err)
	}
	defer s.Close()
	var n int64
	if err := s.db.QueryRow(`SELECT COALESCE(SUM(request_count), 0) FROM ` + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func TestOpenStorePragmasAndSchemaVersion(t *testing.T) {
	created := time.Date(2026, 10, 27, 9, 15, 0, 0, time.FixedZone("x", -5*3600))
	s, path := openTestStore(t, created)

	var mode string
	var busy int
	if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal_mode = %q (err %v), want wal", mode, err)
	}
	if err := s.db.QueryRow("PRAGMA busy_timeout").Scan(&busy); err != nil || busy != storeBusyTimeoutMillis {
		t.Fatalf("busy_timeout = %d (err %v), want %d", busy, err, storeBusyTimeoutMillis)
	}
	var version int
	if err := s.db.QueryRow("SELECT MAX(version) FROM schema_version").Scan(&version); err != nil || version != 1 {
		t.Fatalf("schema_version = %d (err %v), want 1", version, err)
	}
	stats, err := s.Stats()
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.RecordingSince == nil || *stats.RecordingSince != "2026-10-27T14:15:00Z" {
		t.Fatalf("recording_since = %v", stats.RecordingSince)
	}

	// Reopening keeps recording_since and does not re-run migrations.
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	reopened, err := OpenStore(path, created.Add(48*time.Hour))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	stats, err = reopened.Stats()
	if err != nil || *stats.RecordingSince != "2026-10-27T14:15:00Z" || stats.RowCounts[tableSchemaVersion] != 1 {
		t.Fatalf("after reopen: %+v (err %v)", stats, err)
	}
}

func TestOpenStoreRejectsNewerSchema(t *testing.T) {
	s, path := openTestStore(t, time.Now())
	if _, err := s.db.Exec(`INSERT INTO schema_version (version, applied_at) VALUES (99, 'x')`); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if _, err := OpenStore(path, time.Now()); err == nil || !strings.Contains(err.Error(), "newer") {
		t.Fatalf("OpenStore on newer schema: err = %v", err)
	}
}

func TestOpenStoreRejectsUnusablePaths(t *testing.T) {
	for _, path := range []string{"relative.db", filepath.Join(t.TempDir(), "a?b.db")} {
		if _, err := OpenStore(path, time.Now()); err == nil {
			t.Fatalf("OpenStore(%q) succeeded", path)
		}
	}
}

func TestOpenStoreCreatesParentDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "dir", "ledger.db")
	s, err := OpenStore(path, time.Now())
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	_ = s.Close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("database file not created: %v", err)
	}
}

func TestWriteBatchAccumulatesUpserts(t *testing.T) {
	s, _ := openTestStore(t, time.Now())
	at := time.Date(2026, 10, 27, 10, 5, 30, 0, time.UTC)
	ev := func(failed bool, input, output int64, latency int64) ledgerEvent {
		return ledgerEvent{
			At: at, Provider: "claude", Model: "claude-sonnet", AuthIndex: "abcd", ClientKey: "k",
			Failed: failed, LatencyMs: latency,
			Tokens: tokenBuckets{Input: input, Output: output, CacheRead: 1, CacheCreation: 2, Reasoning: 3, Unclassified: 4, Total: input + output + 10},
		}
	}
	if err := s.WriteBatch([]ledgerEvent{ev(false, 10, 5, 100), ev(false, 20, 6, 200)}); err != nil {
		t.Fatalf("WriteBatch 1: %v", err)
	}
	if err := s.WriteBatch([]ledgerEvent{ev(false, 30, 7, 300), ev(true, 1, 0, 50)}); err != nil {
		t.Fatalf("WriteBatch 2: %v", err)
	}
	for _, table := range []string{tableUsageMinute, tableUsageHour} {
		rows := readRollups(t, s, table)
		if len(rows) != 2 {
			t.Fatalf("%s rows = %+v, want success and failure rows", table, rows)
		}
		ok, failed := rows[0], rows[1]
		if ok.Failed || ok.Requests != 3 || ok.Failures != 0 || ok.Input != 60 || ok.Output != 18 ||
			ok.CacheRead != 3 || ok.CacheCreation != 6 || ok.Reasoning != 9 || ok.Unclassif != 12 ||
			ok.Total != 108 || ok.LatencyMs != 600 {
			t.Fatalf("%s success row = %+v", table, ok)
		}
		if !failed.Failed || failed.Requests != 1 || failed.Failures != 1 || failed.Input != 1 || failed.LatencyMs != 50 {
			t.Fatalf("%s failure row = %+v", table, failed)
		}
	}
}

func TestWriteBatchSeparatesDimensions(t *testing.T) {
	s, _ := openTestStore(t, time.Now())
	at := time.Date(2026, 10, 27, 10, 0, 0, 0, time.UTC)
	base := ledgerEvent{At: at, Provider: "p", Model: "m", AuthIndex: "a", ClientKey: "k"}
	variants := []ledgerEvent{base, base, base, base, base}
	variants[1].Provider = "p2"
	variants[2].Model = "m2"
	variants[3].AuthIndex = "a2"
	variants[4].ClientKey = "k2"
	if err := s.WriteBatch(variants); err != nil {
		t.Fatal(err)
	}
	if rows := readRollups(t, s, tableUsageMinute); len(rows) != 5 {
		t.Fatalf("rows = %d, want 5 distinct keys", len(rows))
	}
}

func TestMinuteAndHourBucketingAcrossBoundaries(t *testing.T) {
	s, _ := openTestStore(t, time.Now())
	plus2 := time.FixedZone("UTC+2", 2*3600)
	times := []time.Time{
		time.Date(2026, 12, 31, 23, 59, 30, 0, time.UTC),
		time.Date(2026, 12, 31, 23, 59, 59, 999999999, time.UTC),
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 1, 2, 0, 59, 0, plus2), // 00:00:59Z, given in UTC+2
		time.Date(2027, 1, 1, 0, 59, 59, 0, time.UTC),
		time.Date(2027, 1, 1, 1, 0, 0, 0, time.UTC),
	}
	events := make([]ledgerEvent, len(times))
	for i, at := range times {
		events[i] = eventFromRecord(usageRecord{Provider: "p", Model: "m", RequestedAt: at}, time.Time{})
	}
	if err := s.WriteBatch(events); err != nil {
		t.Fatal(err)
	}
	bucketCounts := func(table string) map[string]int64 {
		out := map[string]int64{}
		for _, r := range readRollups(t, s, table) {
			out[r.BucketStart.Format(time.RFC3339)] = r.Requests
		}
		return out
	}
	wantMinute := map[string]int64{
		"2026-12-31T23:59:00Z": 2,
		"2027-01-01T00:00:00Z": 2,
		"2027-01-01T00:59:00Z": 1,
		"2027-01-01T01:00:00Z": 1,
	}
	wantHour := map[string]int64{
		"2026-12-31T23:00:00Z": 2,
		"2027-01-01T00:00:00Z": 3,
		"2027-01-01T01:00:00Z": 1,
	}
	if got := bucketCounts(tableUsageMinute); !reflect.DeepEqual(got, wantMinute) {
		t.Fatalf("minute buckets = %v, want %v", got, wantMinute)
	}
	if got := bucketCounts(tableUsageHour); !reflect.DeepEqual(got, wantHour) {
		t.Fatalf("hour buckets = %v, want %v", got, wantHour)
	}
}

func TestBucketStartFloorsBeforeEpoch(t *testing.T) {
	at := time.Date(1969, 12, 31, 23, 59, 30, 0, time.UTC)
	if got := bucketStart(at, time.Minute); got != -60 {
		t.Fatalf("bucketStart = %d, want -60", got)
	}
}

func TestApplyRetentionDeletesExactlyPastCutoff(t *testing.T) {
	s, _ := openTestStore(t, time.Now())
	now := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	minuteCutoff := now.Add(-2 * 24 * time.Hour) // minute-retention-days: 2
	hourCutoff := now.Add(-10 * 24 * time.Hour)  // hour-retention-days: 10
	at := []time.Time{
		minuteCutoff.Add(-time.Minute), // minute: deleted; hour: kept
		minuteCutoff,                   // kept in both
		minuteCutoff.Add(time.Minute),  // kept in both
		hourCutoff.Add(-time.Hour),     // deleted in both
		hourCutoff,                     // minute: deleted; hour: kept
		now,                            // kept in both
	}
	events := make([]ledgerEvent, len(at))
	for i, a := range at {
		events[i] = ledgerEvent{At: a, Provider: "p"}
	}
	if err := s.WriteBatch(events); err != nil {
		t.Fatal(err)
	}
	minuteDeleted, hourDeleted, err := s.ApplyRetention(now.In(time.FixedZone("x", 7*3600)), 2, 10)
	if err != nil {
		t.Fatalf("ApplyRetention: %v", err)
	}
	starts := func(table string) []string {
		var out []string
		for _, r := range readRollups(t, s, table) {
			out = append(out, r.BucketStart.Format(time.RFC3339))
		}
		return out
	}
	wantMinute := []string{"2026-10-25T12:00:00Z", "2026-10-25T12:01:00Z", "2026-10-27T12:00:00Z"}
	wantHour := []string{"2026-10-17T12:00:00Z", "2026-10-25T11:00:00Z", "2026-10-25T12:00:00Z", "2026-10-27T12:00:00Z"}
	if got := starts(tableUsageMinute); !reflect.DeepEqual(got, wantMinute) || minuteDeleted != 3 {
		t.Fatalf("minute rows = %v (deleted %d), want %v (deleted 3)", got, minuteDeleted, wantMinute)
	}
	if got := starts(tableUsageHour); !reflect.DeepEqual(got, wantHour) || hourDeleted != 1 {
		t.Fatalf("hour rows = %v (deleted %d), want %v (deleted 1)", got, hourDeleted, wantHour)
	}
}

func TestMaskClientKey(t *testing.T) {
	hash8 := func(s string) string {
		sum := sha256.Sum256([]byte(s))
		return hex.EncodeToString(sum[:])[:8]
	}
	key := "sk-live-0123456789abcdefa1b2"
	if got, want := maskClientKey(key), hash8(key)+"…a1b2"; got != want {
		t.Fatalf("maskClientKey = %q, want %q", got, want)
	}
	if got := maskClientKey("  " + key + "\n"); got != maskClientKey(key) {
		t.Fatalf("surrounding whitespace changed the mask: %q", got)
	}
	if maskClientKey("") != "" || maskClientKey("   ") != "" {
		t.Fatal("empty key must stay empty")
	}
	// Keys shorter than 12 characters keep only the hash prefix.
	if got := maskClientKey("short-key"); got != hash8("short-key") {
		t.Fatalf("short key mask = %q", got)
	}
	// Multi-byte keys keep the last 4 characters, not bytes.
	if got := maskClientKey("ключ-доступа-эюя€"); !strings.HasSuffix(got, "…эюя€") {
		t.Fatalf("multi-byte mask = %q", got)
	}
	if strings.Contains(maskClientKey(key), "sk-live") {
		t.Fatal("mask leaks the key prefix")
	}
}

func TestSchemaHasOnlyWhitelistedColumns(t *testing.T) {
	s, _ := openTestStore(t, time.Now())
	want := map[string][]string{
		tableSchemaVersion: {"applied_at", "version"},
		tableMeta:          {"key", "value"},
		tableUsageMinute:   sortedCopy(rollupColumns),
		tableUsageHour:     sortedCopy(rollupColumns),
	}
	rows, err := s.db.Query(`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, name)
	}
	_ = rows.Close()
	if !reflect.DeepEqual(tables, []string{"meta", "schema_version", "usage_hour", "usage_minute"}) {
		t.Fatalf("tables = %v", tables)
	}
	disallowed := []string{
		"header", "body", "prompt", "message", "failure_body", "status_code", "request_id",
		"trace", "session", "api_key", "apikey", "base_url", "auth_id", "user", "ip",
	}
	for _, table := range tables {
		columns := tableColumns(t, s, table)
		if !reflect.DeepEqual(columns, want[table]) {
			t.Fatalf("%s columns = %v, want %v", table, columns, want[table])
		}
		for _, column := range columns {
			for _, bad := range disallowed {
				if strings.Contains(column, bad) {
					t.Fatalf("%s has disallowed column %q", table, column)
				}
			}
		}
	}
}

func tableColumns(t *testing.T, s *Store, table string) []string {
	t.Helper()
	rows, err := s.db.Query(`SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func sortedCopy(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// TestSensitiveFieldsNeverReachDisk fills every non-persisted field with a
// sentinel, writes the record, and scans the raw database bytes.
func TestSensitiveFieldsNeverReachDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.db")
	s, err := OpenStore(path, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	const apiKey = "SENTINEL-APIKEY-0123456789-wxyz"
	rec := usageRecord{
		RequestID: "SENTINEL-REQUEST-ID", TraceID: "SENTINEL-TRACE", SelectionID: "SENTINEL-SELECTION",
		Provider: "claude", BaseURL: "https://SENTINEL-BASEURL.example", ExecutorType: "ClaudeExecutor",
		Model: "claude-sonnet", Alias: "SENTINEL-ALIAS", APIKey: apiKey, AccessProvider: "SENTINEL-ACCESS",
		SessionID: "SENTINEL-SESSION", ParentSessionID: "SENTINEL-PARENT", AuthID: "SENTINEL-AUTHID@example.com.json",
		AuthIndex: "0123456789abcdef", AuthType: "SENTINEL-AUTHTYPE", Source: "SENTINEL-SOURCE",
		ReasoningEffort: "SENTINEL-EFFORT", ServiceTier: "SENTINEL-TIER", ResponseModel: "SENTINEL-RESPMODEL",
		RequestedAt: time.Now(), Latency: time.Second, Failed: true,
		Failure: usageFailure{StatusCode: 529, Body: "SENTINEL-FAILURE-BODY prompt: tell me a secret"},
		Detail:  usageDetail{InputTokens: 10, OutputTokens: 5},
		ResponseHeaders: http.Header{
			"X-Sentinel-Header": []string{"SENTINEL-HEADER-VALUE"},
		},
	}
	if err := s.WriteBatch([]ledgerEvent{eventFromRecord(rec, time.Now())}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	for _, file := range []string{path, path + "-wal"} {
		if b, err := os.ReadFile(file); err == nil {
			raw = append(raw, b...)
		}
	}
	if !bytes.Contains(raw, []byte("0123456789abcdef")) || !bytes.Contains(raw, []byte("claude-sonnet")) {
		t.Fatal("expected whitelisted values in the database file")
	}
	for _, sentinel := range []string{"SENTINEL", "sentinel", apiKey[:20], "secret"} {
		if bytes.Contains(raw, []byte(sentinel)) {
			t.Fatalf("database file contains %q", sentinel)
		}
	}
	if !bytes.Contains(raw, []byte(maskClientKey(apiKey))) {
		t.Fatal("expected the masked client key in the database file")
	}
}

func TestEventFromRecordNormalizesTokensAndFallbacks(t *testing.T) {
	now := time.Date(2026, 10, 27, 8, 0, 0, 0, time.UTC)
	e := eventFromRecord(usageRecord{
		Provider: " claude ", Model: "claude-sonnet ", AuthIndex: " idx ",
		Latency: -5 * time.Millisecond,
		Detail:  usageDetail{InputTokens: 100, OutputTokens: 30, ReasoningTokens: 12, CacheReadTokens: 40, CacheCreationTokens: 10},
	}, now)
	if !e.At.Equal(now) || e.Provider != "claude" || e.Model != "claude-sonnet" || e.AuthIndex != "idx" || e.LatencyMs != 0 {
		t.Fatalf("event = %+v", e)
	}
	// Claude: cache and reasoning are independent of input/output.
	want := tokenBuckets{Input: 100, Output: 30, CacheRead: 40, CacheCreation: 10, Reasoning: 12, Total: 192}
	if e.Tokens != want {
		t.Fatalf("tokens = %+v, want %+v", e.Tokens, want)
	}
}
