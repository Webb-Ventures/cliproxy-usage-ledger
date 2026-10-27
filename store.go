package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // Pure-Go SQLite driver, registered as "sqlite".
)

const (
	// storeSchemaVersion is the latest migration in storeMigrations.
	storeSchemaVersion = 2

	// storeBusyTimeoutMillis bounds how long a statement waits on a lock held
	// by another connection (for example an operator running sqlite3 on the file).
	storeBusyTimeoutMillis = 5000

	metaKeyRecordingSince = "recording_since"
	metaKeySettings       = "settings"

	tableSchemaVersion = "schema_version"
	tableMeta          = "meta"
	tableUsageMinute   = "usage_minute"
	tableUsageHour     = "usage_hour"
)

// storeTables lists every table the store creates, in status order.
var storeTables = []string{tableMeta, tableSchemaVersion, tableUsageHour, tableUsageMinute}

// rollupColumns is the complete whitelist of persisted rollup columns. The
// schema test asserts both rollup tables have exactly these columns, so adding
// a field means editing this list on purpose. The first rollupKeyColumnCount
// columns form the primary key; the rest are counters.
var rollupColumns = []string{
	"bucket_start",
	"provider",
	"model",
	"service_tier",
	"auth_index",
	"client_key",
	"failed",
	"request_count",
	"failure_count",
	"input_tokens",
	"output_tokens",
	"cache_read_tokens",
	"cache_creation_tokens",
	"reasoning_tokens",
	"unclassified_tokens",
	"total_tokens",
	"latency_ms_sum",
}

const rollupKeyColumnCount = 7

// rollupTableDDL is the current (schema v2) rollup table definition.
func rollupTableDDL(table string) string {
	return `CREATE TABLE ` + table + ` (
	bucket_start          INTEGER NOT NULL, -- UTC bucket start, Unix seconds
	provider              TEXT    NOT NULL,
	model                 TEXT    NOT NULL, -- response model, else requested model
	service_tier          TEXT    NOT NULL DEFAULT '',
	auth_index            TEXT    NOT NULL, -- CPA's hashed credential index
	client_key            TEXT    NOT NULL, -- masked client API key, see maskClientKey
	failed                INTEGER NOT NULL CHECK (failed IN (0, 1)),
	request_count         INTEGER NOT NULL DEFAULT 0,
	failure_count         INTEGER NOT NULL DEFAULT 0,
	input_tokens          INTEGER NOT NULL DEFAULT 0, -- uncached input
	output_tokens         INTEGER NOT NULL DEFAULT 0, -- non-reasoning output
	cache_read_tokens     INTEGER NOT NULL DEFAULT 0,
	cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
	reasoning_tokens      INTEGER NOT NULL DEFAULT 0,
	unclassified_tokens   INTEGER NOT NULL DEFAULT 0,
	total_tokens          INTEGER NOT NULL DEFAULT 0,
	latency_ms_sum        INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (bucket_start, provider, model, service_tier, auth_index, client_key, failed)
) WITHOUT ROWID`
}

// rollupTableDDLv1 is the schema v1 definition. Migration 1 must keep creating
// exactly this table so that upgrades replay the same history.
func rollupTableDDLv1(table string) string {
	return `CREATE TABLE ` + table + ` (
	bucket_start          INTEGER NOT NULL, -- UTC bucket start, Unix seconds
	provider              TEXT    NOT NULL,
	model                 TEXT    NOT NULL,
	auth_index            TEXT    NOT NULL, -- CPA's hashed credential index
	client_key            TEXT    NOT NULL, -- masked client API key, see maskClientKey
	failed                INTEGER NOT NULL CHECK (failed IN (0, 1)),
	request_count         INTEGER NOT NULL DEFAULT 0,
	failure_count         INTEGER NOT NULL DEFAULT 0,
	input_tokens          INTEGER NOT NULL DEFAULT 0, -- uncached input
	output_tokens         INTEGER NOT NULL DEFAULT 0, -- non-reasoning output
	cache_read_tokens     INTEGER NOT NULL DEFAULT 0,
	cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
	reasoning_tokens      INTEGER NOT NULL DEFAULT 0,
	unclassified_tokens   INTEGER NOT NULL DEFAULT 0,
	total_tokens          INTEGER NOT NULL DEFAULT 0,
	latency_ms_sum        INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (bucket_start, provider, model, auth_index, client_key, failed)
) WITHOUT ROWID`
}

// rollupAuthIndexDDL indexes rollups by credential for the windows route.
func rollupAuthIndexDDL(table string) string {
	return `CREATE INDEX ` + table + `_auth_bucket ON ` + table + ` (auth_index, bucket_start)`
}

// migrateRollupToV2 rebuilds a v1 rollup table with service_tier in its
// primary key. SQLite cannot alter a primary key, so the rows are copied into a
// new table that then takes the old name. Existing rows get service_tier ”.
func migrateRollupToV2(table string) []string {
	selectColumns := make([]string, len(rollupColumns))
	for i, column := range rollupColumns {
		selectColumns[i] = column
		if column == "service_tier" {
			selectColumns[i] = "''"
		}
	}
	return []string{
		rollupTableDDL(table + "_v2"),
		`INSERT INTO ` + table + `_v2 (` + strings.Join(rollupColumns, ", ") + `)
SELECT ` + strings.Join(selectColumns, ", ") + ` FROM ` + table,
		`DROP TABLE ` + table,
		`ALTER TABLE ` + table + `_v2 RENAME TO ` + table,
		rollupAuthIndexDDL(table),
	}
}

// storeMigrations[i] upgrades the schema from version i to i+1. Each runs in
// one transaction together with its schema_version row.
var storeMigrations = [][]string{
	{
		`CREATE TABLE meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
) WITHOUT ROWID`,
		rollupTableDDLv1(tableUsageMinute),
		rollupTableDDLv1(tableUsageHour),
	},
	append(migrateRollupToV2(tableUsageMinute), migrateRollupToV2(tableUsageHour)...),
}

// Store is the SQLite-backed rollup ledger. It is not safe for concurrent use;
// the plugin serializes access with Plugin.storeMu.
type Store struct {
	db   *sql.DB
	path string
}

// OpenStore opens (creating if needed) the ledger at path, applies pending
// migrations, and records recording_since on first creation.
func OpenStore(path string, now time.Time) (*Store, error) {
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("usage-ledger: database path %q is not absolute", path)
	}
	if strings.ContainsAny(path, "?#") {
		// The driver splits its DSN at the first '?'.
		return nil, fmt.Errorf("usage-ledger: database path %q must not contain '?' or '#'", path)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o750); errMkdir != nil {
		return nil, fmt.Errorf("usage-ledger: create database directory: %w", errMkdir)
	}
	params := url.Values{}
	params.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", storeBusyTimeoutMillis))
	params.Add("_pragma", "journal_mode(WAL)")
	params.Set("_txlock", "immediate")
	db, errOpen := sql.Open("sqlite", path+"?"+params.Encode())
	if errOpen != nil {
		return nil, fmt.Errorf("usage-ledger: open database: %w", errOpen)
	}
	// One connection: writes are serialized anyway, and DSN pragmas are applied
	// per connection, so this also keeps them in force for every statement.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	s := &Store{db: db, path: path}
	if errInit := s.init(now); errInit != nil {
		_ = db.Close()
		return nil, errInit
	}
	return s, nil
}

func (s *Store) init(now time.Time) error {
	ctx := context.Background()
	var mode string
	if errMode := s.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); errMode != nil {
		return fmt.Errorf("usage-ledger: open database: %w", errMode)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("usage-ledger: database journal_mode is %q, want wal", mode)
	}
	return s.migrate(ctx, now)
}

func (s *Store) migrate(ctx context.Context, now time.Time) error {
	tx, errBegin := s.db.BeginTx(ctx, nil)
	if errBegin != nil {
		return fmt.Errorf("usage-ledger: migrate: %w", errBegin)
	}
	defer func() { _ = tx.Rollback() }()

	if _, errCreate := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_version (
	version    INTEGER PRIMARY KEY,
	applied_at TEXT    NOT NULL
)`); errCreate != nil {
		return fmt.Errorf("usage-ledger: migrate: %w", errCreate)
	}
	var current int
	if errScan := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_version`).Scan(&current); errScan != nil {
		return fmt.Errorf("usage-ledger: migrate: %w", errScan)
	}
	if current > storeSchemaVersion {
		return fmt.Errorf("usage-ledger: database schema version %d is newer than this plugin supports (%d)", current, storeSchemaVersion)
	}
	appliedAt := now.UTC().Format(time.RFC3339)
	for version := current; version < storeSchemaVersion; version++ {
		for _, statement := range storeMigrations[version] {
			if _, errExec := tx.ExecContext(ctx, statement); errExec != nil {
				return fmt.Errorf("usage-ledger: migrate to schema %d: %w", version+1, errExec)
			}
		}
		if _, errExec := tx.ExecContext(ctx, `INSERT INTO schema_version (version, applied_at) VALUES (?, ?)`, version+1, appliedAt); errExec != nil {
			return fmt.Errorf("usage-ledger: migrate to schema %d: %w", version+1, errExec)
		}
	}
	if _, errExec := tx.ExecContext(ctx, `INSERT OR IGNORE INTO meta (key, value) VALUES (?, ?)`, metaKeyRecordingSince, appliedAt); errExec != nil {
		return fmt.Errorf("usage-ledger: migrate: %w", errExec)
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return fmt.Errorf("usage-ledger: migrate: %w", errCommit)
	}
	return nil
}

// Path returns the absolute database path.
func (s *Store) Path() string { return s.path }

// Close closes the database and releases the file. SQLite checkpoints the WAL
// and removes the -wal and -shm files when the last connection closes.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	errClose := s.db.Close()
	s.db = nil
	return errClose
}

// ---- Events and rollups ----

// ledgerEvent is the whitelisted projection of a usage record. It is the only
// thing the worker keeps from a record; nothing else reaches the database.
type ledgerEvent struct {
	At        time.Time
	Provider  string
	Model     string
	Tier      string
	AuthIndex string
	ClientKey string // already masked
	Failed    bool
	Tokens    tokenBuckets
	LatencyMs int64
}

// eventFromRecord copies the persisted fields out of a delivered record. Request
// and response headers, failure bodies, prompts, session and request IDs, and
// the raw client API key are intentionally not copied.
//
// model is the model the upstream response reported (ResponseModel), falling
// back to the model CPA requested (Model); see
// internal/pluginhost/adapters_usage_translation.go:197 and :212.
func eventFromRecord(rec usageRecord, now time.Time) ledgerEvent {
	at := rec.RequestedAt
	if at.IsZero() {
		at = now
	}
	latency := rec.Latency.Milliseconds()
	if latency < 0 {
		latency = 0
	}
	return ledgerEvent{
		At:        at.UTC(),
		Provider:  strings.TrimSpace(rec.Provider),
		Model:     firstNonEmpty(rec.ResponseModel, rec.Model),
		Tier:      recordServiceTier(rec),
		AuthIndex: strings.TrimSpace(rec.AuthIndex),
		ClientKey: maskClientKey(rec.APIKey),
		Failed:    rec.Failed,
		Tokens:    bucketsFromUsage(rec.Detail, rec.Provider, rec.ExecutorType),
		LatencyMs: latency,
	}
}

// recordServiceTier is the tier the request was billed at, as far as CPA knows:
// the tier the upstream response reported, else the tier in the final outbound
// request, else the tier the client asked for (CPA fills "auto" when the client
// sent none). Lowercased so "Priority" and "priority" share a row.
func recordServiceTier(rec usageRecord) string {
	return strings.ToLower(firstNonEmpty(rec.ResponseServiceTier, rec.EffectiveServiceTier, rec.ServiceTier))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// clientKeyMinSuffixLength is the shortest key whose last 4 characters are
// kept. Shorter keys are stored as the hash prefix only, so a short key is
// never stored half (or wholly) in clear.
const clientKeyMinSuffixLength = 12

// maskClientKey reduces a client API key to the first 8 hex characters of its
// SHA-256 hash plus "…" and its last 4 characters, e.g. 3f9a0c12…a1b2. Empty
// keys stay empty.
func maskClientKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(key))
	prefix := hex.EncodeToString(sum[:4])
	runes := []rune(key)
	if len(runes) < clientKeyMinSuffixLength {
		return prefix
	}
	return prefix + "…" + string(runes[len(runes)-4:])
}

type rollupKey struct {
	BucketStart int64
	Provider    string
	Model       string
	Tier        string
	AuthIndex   string
	ClientKey   string
	Failed      bool
}

type rollupValues struct {
	Requests      int64
	Failures      int64
	Input         int64
	Output        int64
	CacheRead     int64
	CacheCreation int64
	Reasoning     int64
	Unclassified  int64
	Total         int64
	LatencyMs     int64
}

func (v *rollupValues) add(e ledgerEvent) {
	v.Requests++
	if e.Failed {
		v.Failures++
	}
	v.Input += e.Tokens.Input
	v.Output += e.Tokens.Output
	v.CacheRead += e.Tokens.CacheRead
	v.CacheCreation += e.Tokens.CacheCreation
	v.Reasoning += e.Tokens.Reasoning
	v.Unclassified += e.Tokens.Unclassified
	v.Total += e.Tokens.Total
	v.LatencyMs += e.LatencyMs
}

// bucketStart floors t (in UTC) to a multiple of size and returns Unix seconds.
func bucketStart(t time.Time, size time.Duration) int64 {
	seconds := t.Unix()
	step := int64(size / time.Second)
	bucket := seconds - seconds%step
	if seconds < 0 && seconds%step != 0 {
		bucket -= step
	}
	return bucket
}

func aggregate(events []ledgerEvent, size time.Duration) map[rollupKey]*rollupValues {
	out := make(map[rollupKey]*rollupValues)
	for _, e := range events {
		key := rollupKey{
			BucketStart: bucketStart(e.At, size),
			Provider:    e.Provider,
			Model:       e.Model,
			Tier:        e.Tier,
			AuthIndex:   e.AuthIndex,
			ClientKey:   e.ClientKey,
			Failed:      e.Failed,
		}
		values := out[key]
		if values == nil {
			values = &rollupValues{}
			out[key] = values
		}
		values.add(e)
	}
	return out
}

func upsertSQL(table string) string {
	updates := make([]string, 0, len(rollupColumns))
	for _, column := range rollupColumns[rollupKeyColumnCount:] {
		updates = append(updates, column+" = "+column+" + excluded."+column)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(rollupColumns)), ", ")
	return `INSERT INTO ` + table + ` (` + strings.Join(rollupColumns, ", ") + `)
VALUES (` + placeholders + `)
ON CONFLICT (` + strings.Join(rollupColumns[:rollupKeyColumnCount], ", ") + `) DO UPDATE SET
	` + strings.Join(updates, ",\n\t")
}

// WriteBatch rolls events into both the minute and hour tables in one
// transaction. Either every event is counted or none is.
func (s *Store) WriteBatch(events []ledgerEvent) error {
	if len(events) == 0 {
		return nil
	}
	ctx := context.Background()
	tx, errBegin := s.db.BeginTx(ctx, nil)
	if errBegin != nil {
		return fmt.Errorf("begin: %w", errBegin)
	}
	defer func() { _ = tx.Rollback() }()
	for _, rollup := range []struct {
		table string
		size  time.Duration
	}{{tableUsageMinute, time.Minute}, {tableUsageHour, time.Hour}} {
		stmt, errPrepare := tx.PrepareContext(ctx, upsertSQL(rollup.table))
		if errPrepare != nil {
			return fmt.Errorf("prepare %s upsert: %w", rollup.table, errPrepare)
		}
		for key, v := range aggregate(events, rollup.size) {
			failed := 0
			if key.Failed {
				failed = 1
			}
			if _, errExec := stmt.ExecContext(ctx,
				key.BucketStart, key.Provider, key.Model, key.Tier, key.AuthIndex, key.ClientKey, failed,
				v.Requests, v.Failures, v.Input, v.Output, v.CacheRead, v.CacheCreation,
				v.Reasoning, v.Unclassified, v.Total, v.LatencyMs,
			); errExec != nil {
				_ = stmt.Close()
				return fmt.Errorf("upsert %s: %w", rollup.table, errExec)
			}
		}
		if errClose := stmt.Close(); errClose != nil {
			return fmt.Errorf("upsert %s: %w", rollup.table, errClose)
		}
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return fmt.Errorf("commit: %w", errCommit)
	}
	return nil
}

// ApplyRetention deletes minute rows whose bucket starts more than minuteDays
// before now, and hour rows more than hourDays before now. A row exactly at the
// cutoff is kept.
func (s *Store) ApplyRetention(now time.Time, minuteDays, hourDays int) (minuteDeleted, hourDeleted int64, err error) {
	ctx := context.Background()
	tx, errBegin := s.db.BeginTx(ctx, nil)
	if errBegin != nil {
		return 0, 0, fmt.Errorf("retention: begin: %w", errBegin)
	}
	defer func() { _ = tx.Rollback() }()
	deleteBefore := func(table string, days int) (int64, error) {
		cutoff := now.UTC().Add(-time.Duration(days) * 24 * time.Hour).Unix()
		res, errExec := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE bucket_start < ?`, cutoff)
		if errExec != nil {
			return 0, fmt.Errorf("retention: %s: %w", table, errExec)
		}
		return res.RowsAffected()
	}
	if minuteDeleted, err = deleteBefore(tableUsageMinute, minuteDays); err != nil {
		return 0, 0, err
	}
	if hourDeleted, err = deleteBefore(tableUsageHour, hourDays); err != nil {
		return 0, 0, err
	}
	if errCommit := tx.Commit(); errCommit != nil {
		return 0, 0, fmt.Errorf("retention: commit: %w", errCommit)
	}
	return minuteDeleted, hourDeleted, nil
}

// ---- Status ----

type storeStats struct {
	RecordingSince *string
	SizeBytes      int64
	RowCounts      map[string]int64
}

// Stats reports recording_since, on-disk size (database plus WAL), and row
// counts for every table.
func (s *Store) Stats() (storeStats, error) {
	ctx := context.Background()
	stats := storeStats{RowCounts: make(map[string]int64, len(storeTables))}
	var since string
	errSince := s.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, metaKeyRecordingSince).Scan(&since)
	switch {
	case errSince == nil:
		stats.RecordingSince = &since
	case errors.Is(errSince, sql.ErrNoRows):
	default:
		return storeStats{}, errSince
	}
	for _, table := range storeTables {
		var count int64
		if errCount := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); errCount != nil {
			return storeStats{}, errCount
		}
		stats.RowCounts[table] = count
	}
	for _, file := range []string{s.path, s.path + "-wal"} {
		if info, errStat := os.Stat(file); errStat == nil {
			stats.SizeBytes += info.Size()
		}
	}
	return stats, nil
}
