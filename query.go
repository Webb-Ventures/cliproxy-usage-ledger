package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Read side of the ledger.
//
// Route handlers never touch the worker's write connection. Next to every
// Store the worker opens a storeReader: a second, query_only connection pool on
// the same file. In WAL mode readers see the last committed state and never
// block the writer (or each other), so a 400-day summary does not stall ingest.
//
// The worker is still the only goroutine that opens, swaps, or closes either
// handle. Handlers borrow the reader under Plugin.readMu (shared), and the
// worker takes readMu exclusively before replacing or closing it, so a query
// never runs on a closed pool and quiesce releases the file only after the
// in-flight queries finish. Each query is bounded by readQueryTimeout.

const (
	readerMaxConns   = 4
	readQueryTimeout = 15 * time.Second
)

var errNoDatabase = errors.New("usage-ledger: no database is open")

type storeReader struct {
	db   *sql.DB
	path string
}

// openReader opens a read-only pool on an existing ledger. Call it after
// OpenStore so the schema is already migrated.
func openReader(path string) (*storeReader, error) {
	params := url.Values{}
	params.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", storeBusyTimeoutMillis))
	params.Add("_pragma", "query_only(1)")
	db, errOpen := sql.Open("sqlite", path+"?"+params.Encode())
	if errOpen != nil {
		return nil, fmt.Errorf("usage-ledger: open reader: %w", errOpen)
	}
	db.SetMaxOpenConns(readerMaxConns)
	db.SetMaxIdleConns(readerMaxConns)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if errPing := db.PingContext(context.Background()); errPing != nil {
		_ = db.Close()
		return nil, fmt.Errorf("usage-ledger: open reader: %w", errPing)
	}
	return &storeReader{db: db, path: path}, nil
}

func (r *storeReader) Close() error {
	if r == nil || r.db == nil {
		return nil
	}
	errClose := r.db.Close()
	r.db = nil
	return errClose
}

// withReader runs fn with the current reader, or returns errNoDatabase when no
// database is open (before register, after quiesce or shutdown).
func (p *Plugin) withReader(fn func(ctx context.Context, r *storeReader) error) error {
	p.readMu.RLock()
	defer p.readMu.RUnlock()
	if p.reader == nil {
		return errNoDatabase
	}
	ctx, cancel := context.WithTimeout(context.Background(), readQueryTimeout)
	defer cancel()
	return fn(ctx, p.reader)
}

// meta returns a meta value, or ok=false if the key is absent.
func (r *storeReader) meta(ctx context.Context, key string) (value string, ok bool, err error) {
	errScan := r.db.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	switch {
	case errScan == nil:
		return value, true, nil
	case errors.Is(errScan, sql.ErrNoRows):
		return "", false, nil
	default:
		return "", false, errScan
	}
}

// recordingSince parses meta.recording_since; zero if absent or unparsable.
func (r *storeReader) recordingSince(ctx context.Context) (time.Time, error) {
	value, ok, err := r.meta(ctx, metaKeyRecordingSince)
	if err != nil || !ok {
		return time.Time{}, err
	}
	parsed, errParse := time.Parse(time.RFC3339, value)
	if errParse != nil {
		return time.Time{}, nil
	}
	return parsed.UTC(), nil
}

// SetMeta upserts a meta value on the write connection. Callers hold storeMu.
func (s *Store) SetMeta(key, value string) error {
	_, errExec := s.db.ExecContext(context.Background(),
		`INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value)
	return errExec
}

// ---- Aggregated rows ----

// tokenTotals is the JSON token breakdown used by every route. The buckets
// never overlap: total = input + cache_read + cache_write + output + reasoning
// + unclassified.
type tokenTotals struct {
	Input        int64 `json:"input"`
	CacheRead    int64 `json:"cache_read"`
	CacheWrite   int64 `json:"cache_write"`
	Output       int64 `json:"output"`
	Reasoning    int64 `json:"reasoning"`
	Unclassified int64 `json:"unclassified"`
	Total        int64 `json:"total"`
}

func (t *tokenTotals) add(o tokenTotals) {
	t.Input += o.Input
	t.CacheRead += o.CacheRead
	t.CacheWrite += o.CacheWrite
	t.Output += o.Output
	t.Reasoning += o.Reasoning
	t.Unclassified += o.Unclassified
	t.Total += o.Total
}

// aggRow is one rollup row summed over the failed flag (and over buckets when
// the scan is not bucketed).
type aggRow struct {
	// Pos is the instant used to place the row in a time bucket: the start of
	// a minute row, or the midpoint of an hour row (see segment.offset).
	Pos         int64
	Provider    string
	Model       string
	ServiceTier string
	AuthIndex   string
	ClientKey   string
	Requests    int64
	Failed      int64
	Tokens      tokenTotals
}

// segment selects rows of one rollup table with bucket_start in [lo, hi).
// offset is added to bucket_start to get aggRow.Pos.
type segment struct {
	table  string
	lo, hi int64
	offset int64
}

const hourMidpointOffset = 1800

// scanRows reads every row in segs. When bucketed is false the rows are summed
// over time (Pos is 0). authIndex, when non-nil, restricts the scan to one
// credential.
func (r *storeReader) scanRows(ctx context.Context, segs []segment, bucketed bool, authIndex *string) ([]aggRow, error) {
	var out []aggRow
	for _, seg := range segs {
		if seg.hi <= seg.lo {
			continue
		}
		dims := "provider, model, service_tier, auth_index, client_key"
		selectBucket := "0"
		groupBy := dims
		if bucketed {
			selectBucket = "bucket_start"
			groupBy = "bucket_start, " + dims
		}
		var where strings.Builder
		where.WriteString("bucket_start >= ? AND bucket_start < ?")
		args := []any{seg.lo, seg.hi}
		if authIndex != nil {
			where.WriteString(" AND auth_index = ?")
			args = append(args, *authIndex)
		}
		query := `SELECT ` + selectBucket + `, ` + dims + `,
	SUM(request_count), SUM(failure_count), SUM(input_tokens), SUM(cache_read_tokens),
	SUM(cache_creation_tokens), SUM(output_tokens), SUM(reasoning_tokens),
	SUM(unclassified_tokens), SUM(total_tokens)
FROM ` + seg.table + ` WHERE ` + where.String() + ` GROUP BY ` + groupBy
		rows, errQuery := r.db.QueryContext(ctx, query, args...)
		if errQuery != nil {
			return nil, fmt.Errorf("query %s: %w", seg.table, errQuery)
		}
		for rows.Next() {
			var row aggRow
			var bucket int64
			if errScan := rows.Scan(&bucket, &row.Provider, &row.Model, &row.ServiceTier, &row.AuthIndex, &row.ClientKey,
				&row.Requests, &row.Failed, &row.Tokens.Input, &row.Tokens.CacheRead, &row.Tokens.CacheWrite,
				&row.Tokens.Output, &row.Tokens.Reasoning, &row.Tokens.Unclassified, &row.Tokens.Total); errScan != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan %s: %w", seg.table, errScan)
			}
			if bucketed {
				row.Pos = bucket + seg.offset
			}
			out = append(out, row)
		}
		errRows := rows.Err()
		_ = rows.Close()
		if errRows != nil {
			return nil, fmt.Errorf("query %s: %w", seg.table, errRows)
		}
	}
	return out, nil
}

// ---- Resolution planning ----

const (
	resolutionMinute = "minute"
	resolutionHour   = "hour"
	resolutionMixed  = "mixed"
)

// readPlan says which rollup rows answer a query over [from, to).
type readPlan struct {
	segments    []segment
	resolution  string
	approximate bool
}

// planRead picks the rollup table(s) for [from, to).
//
//   - Inside minute retention: minute rows, exact.
//   - Otherwise, if every boundary the query needs (aligned) is a whole UTC
//     hour: hour rows, exact.
//   - Otherwise (a zone such as +05:30, or an RFC 3339 bound off the hour):
//     hour rows before the minute-retention edge, minute rows after it. Hour
//     rows are placed by their midpoint, so a boundary in the middle of an hour
//     is off by up to 30 minutes and the result is flagged approximate.
//
// minuteSince is the first instant for which minute rows are complete.
func planRead(from, to, minuteSince time.Time, aligned bool) readPlan {
	lo, hi := from.Unix(), to.Unix()
	minuteSeg := func(a, b int64) segment { return segment{table: tableUsageMinute, lo: a, hi: b} }
	// An hour row belongs to [a, b) when its midpoint does.
	hourSeg := func(a, b int64) segment {
		return segment{table: tableUsageHour, lo: a - hourMidpointOffset, hi: b - hourMidpointOffset, offset: hourMidpointOffset}
	}
	if !from.Before(minuteSince) {
		return readPlan{segments: []segment{minuteSeg(lo, hi)}, resolution: resolutionMinute}
	}
	if aligned {
		return readPlan{segments: []segment{hourSeg(lo, hi)}, resolution: resolutionHour}
	}
	split := ceilUnix(minuteSince.Unix(), 3600)
	if split >= hi {
		return readPlan{segments: []segment{hourSeg(lo, hi)}, resolution: resolutionHour, approximate: true}
	}
	return readPlan{
		segments:    []segment{hourSeg(lo, split), minuteSeg(split, hi)},
		resolution:  resolutionMixed,
		approximate: true,
	}
}

// minuteDataSince is the first whole minute that minute retention keeps.
func minuteDataSince(now time.Time, minuteRetentionDays int) time.Time {
	cutoff := now.UTC().Add(-time.Duration(minuteRetentionDays) * 24 * time.Hour)
	return time.Unix(ceilUnix(cutoff.Unix(), 60), 0).UTC()
}

func ceilUnix(seconds, step int64) int64 {
	floor := seconds - ((seconds%step)+step)%step
	if floor == seconds {
		return seconds
	}
	return floor + step
}

func isWholeHour(t time.Time) bool {
	return t.Unix()%3600 == 0 && t.Nanosecond() == 0
}
