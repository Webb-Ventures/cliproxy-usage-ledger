package main

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// minuteRow is one minute of activity for detectBlocks.
func minuteRow(t *testing.T, at, model, auth string, tokens int64) aggRow {
	return aggRow{Pos: mustTime(t, at).Unix(), Model: model, AuthIndex: auth, Requests: 1, Tokens: tokenTotals{Input: tokens, Total: tokens}}
}

func renderBlocks(blocks []*detectedBlock) []usageBlock {
	out := make([]usageBlock, len(blocks))
	for i, b := range blocks {
		out[i] = b.render(time.UTC)
	}
	return out
}

func blockSpans(blocks []usageBlock) []string {
	out := make([]string, len(blocks))
	for i, b := range blocks {
		out[i] = b.Start + ".." + b.End + " last " + b.ActualEnd
	}
	return out
}

func TestBlocksSingleBurst(t *testing.T) {
	now := mustTime(t, "2026-10-28T12:00:00Z")
	rows := []aggRow{
		minuteRow(t, "2026-10-28T10:30:00Z", "opus", "a2", 200),
		minuteRow(t, "2026-10-28T10:07:00Z", "opus", "a1", 100),
		minuteRow(t, "2026-10-28T10:07:00Z", "sonnet", "a1", 50), // same minute, other model
		minuteRow(t, "2026-10-28T10:08:00Z", "opus", "a1", 300),
	}
	blocks := renderBlocks(detectBlocks(rows, now))
	if len(blocks) != 1 {
		t.Fatalf("blocks = %v", blockSpans(blocks))
	}
	b := blocks[0]
	if b.Start != "2026-10-28T10:00:00Z" || b.End != "2026-10-28T15:00:00Z" || b.ActualEnd != "2026-10-28T10:30:00Z" || !b.Active {
		t.Fatalf("block = %+v", b)
	}
	if b.Requests != 4 || b.Tokens.Total != 650 || b.ActiveMinutes != 3 || len(b.ByModel) != 2 {
		t.Fatalf("block totals = %+v", b)
	}
	if b.PeakTokensPerMinute != 300 {
		t.Fatalf("peak = %d, want 300", b.PeakTokensPerMinute)
	}
	// Burn rate is over active minutes, not wall time: 650 / 3.
	if b.BurnRateTokensPerMinute != 216.67 {
		t.Fatalf("burn rate = %v, want 216.67", b.BurnRateTokensPerMinute)
	}
	if fmt.Sprint(b.AuthIndexes) != "[a1 a2]" {
		t.Fatalf("auth_indexes = %v", b.AuthIndexes)
	}
}

func TestBlocksGapOfExactlyFiveHoursVersusMore(t *testing.T) {
	now := mustTime(t, "2026-10-29T00:00:00Z")
	exact := renderBlocks(detectBlocks([]aggRow{
		minuteRow(t, "2026-10-28T10:00:00Z", "m", "a", 1),
		minuteRow(t, "2026-10-28T15:00:00Z", "m", "a", 1), // exactly 5 h later: same block, as in ccusage
	}, now))
	if len(exact) != 1 || exact[0].ActualEnd != "2026-10-28T15:00:00Z" || exact[0].Requests != 2 {
		t.Fatalf("exact gap blocks = %v", blockSpans(exact))
	}
	more := renderBlocks(detectBlocks([]aggRow{
		minuteRow(t, "2026-10-28T10:00:00Z", "m", "a", 1),
		minuteRow(t, "2026-10-28T15:01:00Z", "m", "a", 1),
	}, now))
	want := "[2026-10-28T10:00:00Z..2026-10-28T15:00:00Z last 2026-10-28T10:00:00Z 2026-10-28T15:00:00Z..2026-10-28T20:00:00Z last 2026-10-28T15:01:00Z]"
	if fmt.Sprint(blockSpans(more)) != want {
		t.Fatalf("gap > 5 h blocks = %v", blockSpans(more))
	}
	if more[0].Active || more[1].Active {
		t.Fatal("past blocks marked active")
	}
}

func TestBlocksActivitySpanningBlockEnd(t *testing.T) {
	now := mustTime(t, "2026-10-28T15:30:00Z")
	blocks := renderBlocks(detectBlocks([]aggRow{
		minuteRow(t, "2026-10-28T10:30:00Z", "m", "a", 10),
		minuteRow(t, "2026-10-28T12:00:00Z", "m", "a", 10),
		minuteRow(t, "2026-10-28T14:59:00Z", "m", "a", 10),
		minuteRow(t, "2026-10-28T15:10:00Z", "m", "a", 10), // continuous use, but past the end
	}, now))
	want := "[2026-10-28T10:00:00Z..2026-10-28T15:00:00Z last 2026-10-28T14:59:00Z 2026-10-28T15:00:00Z..2026-10-28T20:00:00Z last 2026-10-28T15:10:00Z]"
	if fmt.Sprint(blockSpans(blocks)) != want {
		t.Fatalf("blocks = %v", blockSpans(blocks))
	}
	if blocks[0].Requests != 3 || blocks[0].Active || !blocks[1].Active || blocks[1].Requests != 1 {
		t.Fatalf("blocks = %+v", blocks)
	}
}

func TestBlocksRouteClampsToMinuteRetention(t *testing.T) {
	now := mustTime(t, "2026-10-28T12:00:00Z")
	p := newRoutePlugin(t, now, "minute-retention-days: 2\n")
	setRecordingSince(t, p, "2026-10-01T00:00:00Z")
	seed(t, p,
		ev(t, "2026-10-20T10:00:00Z", "m", "old", 99), // minute row past retention
		ev(t, "2026-10-27T08:10:00Z", "m", "a1", 10),
		ev(t, "2026-10-27T09:00:00Z", "m", "a2", 20),
		ev(t, "2026-10-28T11:45:00Z", "m", "a1", 30),
	)
	var resp blocksResponse
	getJSON(t, p, routeBlocks, q("from", "2026-10-01", "to", "2026-10-28", "tz", "Europe/London"), &resp)
	if !resp.Range.Clamped || resp.Range.RequestedFrom != "2026-10-01T00:00:00+01:00" ||
		resp.Range.From != "2026-10-26T12:00:00Z" || resp.Range.MinuteDataSince != resp.Range.From ||
		resp.Range.Resolution != resolutionMinute || resp.SessionHours != 5 {
		t.Fatalf("range = %+v", resp.Range)
	}
	if len(resp.Blocks) != 2 {
		t.Fatalf("blocks = %+v", resp.Blocks)
	}
	first, second := resp.Blocks[0], resp.Blocks[1]
	// Times are rendered in tz (London is on GMT after Oct 25).
	if first.Start != "2026-10-27T08:00:00Z" || first.Requests != 2 || fmt.Sprint(first.AuthIndexes) != "[a1 a2]" || first.Active {
		t.Fatalf("first = %+v", first)
	}
	if second.Start != "2026-10-28T11:00:00Z" || !second.Active || second.Tokens.Total != 30 {
		t.Fatalf("second = %+v", second)
	}

	// A range that starts inside a block still returns the whole block.
	getJSON(t, p, routeBlocks, q("from", "2026-10-27T10:00:00Z", "to", "2026-10-27T11:00:00Z"), &resp)
	if resp.Range.Clamped || len(resp.Blocks) != 1 || resp.Blocks[0].Start != "2026-10-27T08:00:00Z" {
		t.Fatalf("mid-block range = %+v", resp)
	}
	// Entirely before minute retention: no blocks, and the response says so.
	getJSON(t, p, routeBlocks, q("from", "2026-10-01", "to", "2026-10-02"), &resp)
	if !resp.Range.Clamped || len(resp.Blocks) != 0 || resp.Blocks == nil {
		t.Fatalf("old range = %+v", resp)
	}
	expectError(t, callRoute(t, p, http.MethodGet, routeBlocks, q("from", "2026-10-02", "to", "2026-10-01"), nil), http.StatusBadRequest, "invalid_range")
}
