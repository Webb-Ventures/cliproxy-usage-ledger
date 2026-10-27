package main

import (
	"math"
	"sort"
	"time"
)

// 5-hour usage blocks, following ccusage (src/_session-blocks.ts
// identifySessionBlocks):
//
//   - A block starts at the first activity, floored to the UTC hour, and lasts
//     5 hours.
//   - Activity more than 5 hours after the block start, or more than 5 hours
//     after the previous activity, starts a new block. As in ccusage the
//     comparison is strict, so a minute exactly 5 hours after the start still
//     belongs to the block. (The gap rule never fires on its own: the previous
//     activity is never before the block start. It is kept to match ccusage.)
//   - Gaps between blocks are not reported as blocks.
//
// Activity is read from minute rows, so times have minute resolution: a
// minute bucket counts as activity at its start.

type usageBlock struct {
	Start                   string   `json:"start"`
	End                     string   `json:"end"`
	ActualEnd               string   `json:"actual_end"`
	Active                  bool     `json:"active"`
	PeakTokensPerMinute     int64    `json:"peak_tokens_per_minute"`
	BurnRateTokensPerMinute float64  `json:"burn_rate_tokens_per_minute"`
	ActiveMinutes           int64    `json:"active_minutes"`
	AuthIndexes             []string `json:"auth_indexes"`
	usageTotals
}

type detectedBlock struct {
	start, end, lastActivity time.Time
	active                   bool
	acc                      usageAcc
	activeMinutes            int64
	peak                     int64
	auths                    map[string]struct{}
}

func (b *detectedBlock) render(loc *time.Location) usageBlock {
	out := usageBlock{
		Start:               formatTime(b.start, loc),
		End:                 formatTime(b.end, loc),
		ActualEnd:           formatTime(b.lastActivity, loc),
		Active:              b.active,
		PeakTokensPerMinute: b.peak,
		ActiveMinutes:       b.activeMinutes,
		AuthIndexes:         make([]string, 0, len(b.auths)),
		usageTotals:         b.acc.totals(),
	}
	if b.activeMinutes > 0 {
		rate := float64(out.Tokens.Total) / float64(b.activeMinutes)
		out.BurnRateTokensPerMinute = math.Round(rate*100) / 100
	}
	for auth := range b.auths {
		out.AuthIndexes = append(out.AuthIndexes, auth)
	}
	sort.Strings(out.AuthIndexes)
	return out
}

// detectBlocks groups minute rows (Pos = minute start) into blocks. now marks
// the block that contains it as active.
func detectBlocks(rows []aggRow, now time.Time) []*detectedBlock {
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].Pos < rows[j].Pos })
	var blocks []*detectedBlock
	var current *detectedBlock
	for i := 0; i < len(rows); {
		minute := rows[i].Pos
		j := i
		var minuteTokens, minuteRequests int64
		for j < len(rows) && rows[j].Pos == minute {
			minuteTokens += rows[j].Tokens.Total
			minuteRequests += rows[j].Requests
			j++
		}
		if minuteRequests == 0 {
			i = j
			continue
		}
		at := time.Unix(minute, 0).UTC()
		if current == nil || at.Sub(current.start) > blockSessionTime || at.Sub(current.lastActivity) > blockSessionTime {
			start := at.Truncate(time.Hour)
			current = &detectedBlock{start: start, end: start.Add(blockSessionTime), auths: map[string]struct{}{}}
			blocks = append(blocks, current)
		}
		for _, row := range rows[i:j] {
			current.acc.add(row)
			current.auths[row.AuthIndex] = struct{}{}
		}
		current.lastActivity = at
		current.activeMinutes++
		if minuteTokens > current.peak {
			current.peak = minuteTokens
		}
		i = j
	}
	for _, b := range blocks {
		b.active = !now.Before(b.start) && now.Before(b.end)
	}
	return blocks
}
