package main

import (
	"errors"
	"fmt"
	"time"
)

const (
	// defaultFlushBatchSize and defaultFlushInterval bound how long a record
	// waits before its rollups are committed: whichever comes first.
	defaultFlushBatchSize = 256
	defaultFlushInterval  = time.Second
	// defaultRetentionInterval is how often retention runs after the run that
	// happens whenever the database is opened.
	defaultRetentionInterval = time.Hour
)

var errPluginStopped = errors.New("usage-ledger: plugin is shut down")

type controlOp int

const (
	// controlOpen opens the database at path, or keeps it if already open.
	controlOpen controlOp = iota
	// controlQuiesce flushes and closes the database.
	controlQuiesce
)

// controlRequest is executed on the worker goroutine after it flushes its
// pending batch, so store changes never race with a write.
type controlRequest struct {
	op     controlOp
	path   string
	config Config
	reply  chan error
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

// openStore makes the worker write to the database at path, opening it, or
// switching to it from a different path. If opening fails the current database
// (if any) stays in use.
func (p *Plugin) openStore(path string, cfg Config) error {
	return p.sendControl(controlRequest{op: controlOpen, path: path, config: cfg})
}

// Quiesce writes every record already queued, flushes, and closes the
// database so that a replacement copy of the plugin can open the same file.
// Records delivered afterwards wait in the queue until a later register/reconfigure reopens the
// database; if the instance is shut down first they are counted as dropped.
func (p *Plugin) Quiesce() error {
	errQuiesce := p.sendControl(controlRequest{op: controlQuiesce})
	if errors.Is(errQuiesce, errPluginStopped) {
		return nil // Shutdown already flushed and closed.
	}
	return errQuiesce
}

func (p *Plugin) sendControl(req controlRequest) error {
	p.Start()
	req.reply = make(chan error, 1)
	select {
	case p.control <- req:
		return <-req.reply
	case <-p.done:
		return errPluginStopped
	}
}

// run is the single worker goroutine. It owns the pending batch and is the
// only goroutine that opens, replaces, or closes p.store.
func (p *Plugin) run() {
	defer close(p.done)
	b := &batcher{p: p}
	retention := time.NewTicker(p.retentionInterval)
	defer retention.Stop()
	for {
		// With no database open (before register, or after quiesce) the worker
		// stops consuming: records wait in the bounded queue, and overflow is
		// counted in dropped_total by HandleUsage.
		var in <-chan usageRecord
		if p.store != nil {
			in = p.records
		}
		select {
		case rec := <-in:
			b.add(rec)
		case <-b.timerC:
			b.flush()
		case <-retention.C:
			p.runRetention(p.Config())
		case req := <-p.control:
			if req.op == controlQuiesce && p.store != nil {
				b.drainQueue()
			}
			b.flush()
			req.reply <- p.applyControl(req)
		case <-p.stop:
			b.flush()
			p.drainAndClose(b)
			return
		}
	}
}

// drainAndClose runs at shutdown. HandleUsage no longer enqueues (p.closed is
// set before stop is closed), so the queue only shrinks.
func (p *Plugin) drainAndClose(b *batcher) {
	if p.store == nil {
		// Not registered, or quiesced: another copy may own the file now, so
		// these records cannot be written.
		for {
			select {
			case <-p.records:
				p.droppedTotal.Add(1)
			default:
				return
			}
		}
	}
	b.drainQueue()
	b.flush()
	if errClose := p.closeStore(); errClose != nil {
		p.recordWriteError(errClose)
	}
}

func (p *Plugin) applyControl(req controlRequest) error {
	switch req.op {
	case controlOpen:
		if current := p.store; current != nil && current.Path() == req.path {
			p.runRetention(req.config)
			return nil
		}
		next, errOpen := OpenStore(req.path, p.now())
		if errOpen != nil {
			return errOpen
		}
		p.storeMu.Lock()
		previous := p.store
		p.store = next
		p.storeMu.Unlock()
		if previous != nil {
			if errClose := previous.Close(); errClose != nil {
				p.recordWriteError(fmt.Errorf("close %s: %w", previous.Path(), errClose))
			}
		}
		p.runRetention(req.config)
		return nil
	case controlQuiesce:
		if errClose := p.closeStore(); errClose != nil {
			return fmt.Errorf("usage-ledger: quiesce: %w", errClose)
		}
		return nil
	default:
		return fmt.Errorf("usage-ledger: unknown control op %d", req.op)
	}
}

func (p *Plugin) closeStore() error {
	p.storeMu.Lock()
	current := p.store
	p.store = nil
	p.storeMu.Unlock()
	if current == nil {
		return nil
	}
	return current.Close()
}

// runRetention deletes rows past the configured retention, using the injected
// clock. Failures are counted like write errors.
func (p *Plugin) runRetention(cfg Config) {
	p.storeMu.Lock()
	defer p.storeMu.Unlock()
	if p.store == nil {
		return
	}
	if _, _, errRetention := p.store.ApplyRetention(p.now(), cfg.MinuteRetentionDays, cfg.HourRetentionDays); errRetention != nil {
		p.recordWriteError(errRetention)
	}
}

// writeBatch commits one batch. A failure is counted and the batch is
// discarded; the worker keeps running.
func (p *Plugin) writeBatch(events []ledgerEvent) {
	p.storeMu.Lock()
	errWrite := errors.New("no database open")
	if p.store != nil {
		errWrite = p.store.WriteBatch(events)
	}
	p.storeMu.Unlock()
	if errWrite != nil {
		p.recordWriteError(fmt.Errorf("write %d records: %w", len(events), errWrite))
		return
	}
	p.recordsTotal.Add(uint64(len(events)))
	p.lastRecordAt.Store(p.now().UTC().UnixNano())
}

func (p *Plugin) recordWriteError(err error) {
	p.writeErrorsTotal.Add(1)
	message := err.Error()
	p.lastWriteError.Store(&message)
}

// batcher accumulates whitelisted events until the batch is full or the
// flush interval has elapsed since its first event.
type batcher struct {
	p      *Plugin
	events []ledgerEvent
	timer  *time.Timer
	timerC <-chan time.Time
}

func (b *batcher) add(rec usageRecord) {
	b.events = append(b.events, eventFromRecord(rec, b.p.now()))
	if len(b.events) >= b.p.batchSize {
		b.flush()
		return
	}
	if len(b.events) == 1 {
		b.timer = time.NewTimer(b.p.flushInterval)
		b.timerC = b.timer.C
	}
}

// drainQueue moves everything currently queued into the batch, flushing each
// time it fills. It does not wait for more records.
func (b *batcher) drainQueue() {
	for {
		select {
		case rec := <-b.p.records:
			b.add(rec)
		default:
			return
		}
	}
}

func (b *batcher) flush() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
		b.timerC = nil
	}
	if len(b.events) == 0 {
		return
	}
	b.p.writeBatch(b.events)
	b.events = b.events[:0]
}
