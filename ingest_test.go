package main

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock is a settable clock shared between the test and the worker.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	c.t = t
	c.mu.Unlock()
}

func testRecord(at time.Time) usageRecord {
	return usageRecord{
		Provider: "claude", Model: "claude-sonnet", AuthIndex: "idx", APIKey: "sk-test-0123456789",
		RequestedAt: at, Latency: 250 * time.Millisecond,
		Detail: usageDetail{InputTokens: 10, OutputTokens: 5},
	}
}

// queryStore runs fn against the plugin's open store under storeMu.
func queryStore(t *testing.T, p *Plugin, fn func(s *Store)) {
	t.Helper()
	p.storeMu.Lock()
	defer p.storeMu.Unlock()
	if p.store == nil {
		t.Fatal("no database open")
	}
	fn(p.store)
}

func TestWorkerDefaults(t *testing.T) {
	p := NewPlugin(0, nil)
	if p.batchSize != 256 || p.flushInterval != time.Second || p.retentionInterval != time.Hour || cap(p.records) != 4096 {
		t.Fatalf("defaults: batch=%d flush=%v retention=%v buffer=%d", p.batchSize, p.flushInterval, p.retentionInterval, cap(p.records))
	}
}

func TestBatchFlushesOnSize(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	p.flushInterval = time.Hour // only the size trigger can fire
	registerTestPlugin(t, p, "")
	now := time.Now()
	for i := 0; i < p.batchSize-1; i++ {
		p.HandleUsage(testRecord(now))
	}
	time.Sleep(150 * time.Millisecond)
	if got := p.recordsTotal.Load(); got != 0 {
		t.Fatalf("flushed %d records before the batch was full", got)
	}
	p.HandleUsage(testRecord(now))
	waitFor(t, "size-triggered flush", func() bool { return p.recordsTotal.Load() == uint64(p.batchSize) })
	queryStore(t, p, func(s *Store) {
		rows := readRollups(t, s, tableUsageMinute)
		if len(rows) != 1 || rows[0].Requests != 256 || rows[0].Total != 256*15 || rows[0].LatencyMs != 256*250 {
			t.Fatalf("rows = %+v", rows)
		}
	})
}

func TestBatchFlushesOnTime(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	p.flushInterval = 200 * time.Millisecond
	registerTestPlugin(t, p, "")
	start := time.Now()
	for i := 0; i < 3; i++ {
		p.HandleUsage(testRecord(start))
	}
	waitFor(t, "time-triggered flush", func() bool { return p.recordsTotal.Load() == 3 })
	if elapsed := time.Since(start); elapsed < p.flushInterval {
		t.Fatalf("flushed after %v, before the %v interval", elapsed, p.flushInterval)
	}
	queryStore(t, p, func(s *Store) {
		if rows := readRollups(t, s, tableUsageHour); len(rows) != 1 || rows[0].Requests != 3 {
			t.Fatalf("rows = %+v", rows)
		}
	})
}

func TestDropsCountedWhenChannelFull(t *testing.T) {
	p := newTestPlugin(t, 8, nil)
	// Not registered: no database, so the worker does not consume.
	p.Start()
	accepted := 0
	for i := 0; i < 20; i++ {
		if p.HandleUsage(testRecord(time.Now())) {
			accepted++
		}
	}
	if accepted != 8 || p.droppedTotal.Load() != 12 {
		t.Fatalf("accepted=%d dropped=%d, want 8 and 12", accepted, p.droppedTotal.Load())
	}
	if st := p.Status(); st.DroppedTotal != 12 {
		t.Fatalf("status dropped_total = %d", st.DroppedTotal)
	}
}

func TestQuiesceThenShutdownLosesNothing(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	p.flushInterval = time.Hour // nothing flushes unless quiesce does it
	dbPath := registerTestPlugin(t, p, "")
	const n = 1000
	for i := 0; i < n; i++ {
		if !p.HandleUsage(testRecord(time.Now())) {
			t.Fatal("record dropped")
		}
	}

	raw, err := p.HandleMethod(methodPluginQuiesce, []byte(`{}`))
	if err != nil {
		t.Fatalf("quiesce: %v", err)
	}
	if string(raw) != `{"ok":true,"result":{}}` {
		t.Fatalf("quiesce response = %s", raw)
	}
	// Quiesce wrote everything already queued and released the file.
	if p.recordsTotal.Load() != n || len(p.records) != 0 {
		t.Fatalf("records_total=%d queued=%d, want %d and 0", p.recordsTotal.Load(), len(p.records), n)
	}
	if got := countRequests(t, dbPath, tableUsageHour); got != n {
		t.Fatalf("persisted %d of %d records after quiesce", got, n)
	}
	if _, err := os.Stat(dbPath + "-wal"); !os.IsNotExist(err) {
		t.Fatalf("WAL still present after quiesce (err %v)", err)
	}
	if st := p.Status(); st.DatabasePath != nil || st.RowCounts != nil {
		t.Fatalf("status after quiesce still reports a database: %+v", st)
	}

	p.Shutdown()
	if got := countRequests(t, dbPath, tableUsageMinute); got != n {
		t.Fatalf("persisted %d of %d records", got, n)
	}
	if p.droppedTotal.Load() != 0 || p.writeErrorsTotal.Load() != 0 {
		t.Fatalf("dropped=%d write_errors=%d", p.droppedTotal.Load(), p.writeErrorsTotal.Load())
	}

	// Quiesce after shutdown is a harmless no-op.
	if _, err := p.HandleMethod(methodPluginQuiesce, []byte(`{}`)); err != nil {
		t.Fatalf("quiesce after shutdown: %v", err)
	}
}

func TestRecordsAfterQuiesceWaitForReconfigure(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	p.flushInterval = time.Hour
	dbPath := registerTestPlugin(t, p, "")
	if err := p.Quiesce(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		p.HandleUsage(testRecord(time.Now()))
	}
	time.Sleep(50 * time.Millisecond)
	if p.recordsTotal.Load() != 0 || len(p.records) != 5 {
		t.Fatalf("records consumed while quiesced: total=%d queued=%d", p.recordsTotal.Load(), len(p.records))
	}
	// The host rolls a failed replacement back with plugin.reconfigure.
	cfg := []byte("database-path: ledger.db\n")
	if _, err := p.HandleMethod(methodPluginReconfigure, mustJSON(t, lifecycleRequest{ConfigYAML: cfg})); err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	p.Shutdown()
	if got := countRequests(t, dbPath, tableUsageMinute); got != 5 || p.droppedTotal.Load() != 0 {
		t.Fatalf("persisted=%d dropped=%d, want 5 and 0", got, p.droppedTotal.Load())
	}
}

func TestShutdownWhileQuiescedLeavesFileAlone(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	dbPath := registerTestPlugin(t, p, "")
	if err := p.Quiesce(); err != nil {
		t.Fatal(err)
	}
	// Simulate the replacement copy owning the path: the old copy must not
	// recreate or write it.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		_ = os.Remove(dbPath + suffix)
	}
	p.HandleUsage(testRecord(time.Now()))
	p.HandleUsage(testRecord(time.Now()))
	p.Shutdown()
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Fatalf("quiesced instance touched the database (stat err %v)", err)
	}
	if p.droppedTotal.Load() != 2 {
		t.Fatalf("dropped_total = %d, want 2", p.droppedTotal.Load())
	}
}

func TestWriteErrorDoesNotStopWorker(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	p.flushInterval = 10 * time.Millisecond
	registerTestPlugin(t, p, "")
	queryStore(t, p, func(s *Store) {
		if _, err := s.db.Exec(`DROP TABLE usage_hour`); err != nil {
			t.Fatal(err)
		}
	})
	p.HandleUsage(testRecord(time.Now()))
	waitFor(t, "write error", func() bool { return p.writeErrorsTotal.Load() == 1 })
	st := p.Status()
	if st.LastWriteError == nil || !strings.Contains(*st.LastWriteError, "usage_hour") || st.RecordsTotal != 0 {
		t.Fatalf("status = %+v (last error %v)", st, st.LastWriteError)
	}
	// The failed batch was one transaction: its minute rows were rolled back.
	queryStore(t, p, func(s *Store) {
		if rows := readRollups(t, s, tableUsageMinute); len(rows) != 0 {
			t.Fatalf("partial write: %+v", rows)
		}
		if _, err := s.db.Exec(rollupTableDDL(tableUsageHour)); err != nil {
			t.Fatal(err)
		}
	})
	p.HandleUsage(testRecord(time.Now()))
	waitFor(t, "recovery", func() bool { return p.recordsTotal.Load() == 1 })
	if p.writeErrorsTotal.Load() != 1 {
		t.Fatalf("write_errors_total = %d", p.writeErrorsTotal.Load())
	}
}

func TestReconfigureSwitchesDatabasePath(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	p.flushInterval = time.Hour
	first := registerTestPlugin(t, p, "")
	p.HandleUsage(testRecord(time.Now()))

	wd, _ := p.getwd()
	second := filepath.Join(wd, "moved", "ledger2.db")
	cfg := []byte("database-path: " + second + "\n")
	if _, err := p.HandleMethod(methodPluginReconfigure, mustJSON(t, lifecycleRequest{ConfigYAML: cfg})); err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	if st := p.Status(); st.DatabasePath == nil || *st.DatabasePath != second {
		t.Fatalf("database_path = %v, want %s", st.DatabasePath, second)
	}
	p.HandleUsage(testRecord(time.Now()))
	p.Shutdown()
	if a, b := countRequests(t, first, tableUsageMinute), countRequests(t, second, tableUsageMinute); a != 1 || b != 1 {
		t.Fatalf("first=%d second=%d, want 1 and 1 (pending batch flushed to the old file)", a, b)
	}
}

func TestReconfigureKeepsDatabaseWhenNewPathFails(t *testing.T) {
	p := newTestPlugin(t, usageBufferSize, nil)
	first := registerTestPlugin(t, p, "minute-retention-days: 5\n")
	bad := []byte("database-path: " + filepath.Join(t.TempDir(), "bad?.db") + "\nminute-retention-days: 1\n")
	if _, err := p.HandleMethod(methodPluginReconfigure, mustJSON(t, lifecycleRequest{ConfigYAML: bad})); err == nil {
		t.Fatal("reconfigure to an unusable path succeeded")
	}
	st := p.Status()
	if st.DatabasePath == nil || *st.DatabasePath != first || st.Retention.MinuteDays != 5 {
		t.Fatalf("status after failed reconfigure = path %v retention %+v", st.DatabasePath, st.Retention)
	}
}

func TestRetentionRunsAtStartupAndHourly(t *testing.T) {
	start := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	clock := &fakeClock{t: start}
	p := newTestPlugin(t, usageBufferSize, clock.Now)
	p.retentionInterval = 20 * time.Millisecond
	wd, _ := p.getwd()
	dbPath := filepath.Join(wd, "ledger.db")

	// Seed rows: one past the 2-day minute cutoff, one inside it.
	seed, err := OpenStore(dbPath, start)
	if err != nil {
		t.Fatal(err)
	}
	old := ledgerEvent{At: start.Add(-49 * time.Hour), Provider: "old"}
	recent := ledgerEvent{At: start.Add(-time.Hour), Provider: "recent"}
	if err := seed.WriteBatch([]ledgerEvent{old, recent}); err != nil {
		t.Fatal(err)
	}
	_ = seed.Close()

	registerTestPlugin(t, p, "minute-retention-days: 2\nhour-retention-days: 3\n")
	providers := func(table string) []string {
		var out []string
		queryStore(t, p, func(s *Store) {
			for _, r := range readRollups(t, s, table) {
				out = append(out, r.Provider)
			}
		})
		return out
	}
	// Startup run (inside register): minute "old" gone, hour rows kept.
	if got := providers(tableUsageMinute); len(got) != 1 || got[0] != "recent" {
		t.Fatalf("minute rows after startup retention = %v", got)
	}
	if got := providers(tableUsageHour); len(got) != 2 {
		t.Fatalf("hour rows after startup retention = %v", got)
	}

	// Advance the injected clock one day: the hourly job drops the old hour row.
	clock.Set(start.Add(24 * time.Hour))
	waitFor(t, "hourly retention", func() bool { return len(providers(tableUsageHour)) == 1 })
	if got := providers(tableUsageHour); got[0] != "recent" {
		t.Fatalf("hour rows = %v", got)
	}
}
