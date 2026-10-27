package main

import (
	"net/url"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata" // IANA zones on hosts without a zoneinfo database (Windows, slim containers).
)

const (
	maxRangeDays  = 400
	maxBuckets    = 2000
	localDateForm = "2006-01-02"
)

var localDatePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// apiError is a 400-class error returned as {"error": message, "code": code}.
type apiError struct {
	status  int
	code    string
	message string
}

func (e *apiError) Error() string { return e.message }

func badRequest(code, message string) *apiError {
	return &apiError{status: 400, code: code, message: message}
}

// timeRange is a validated [From, To) query range in Loc.
type timeRange struct {
	From time.Time
	To   time.Time
	Loc  *time.Location
	TZ   string
}

// loadZone resolves the tz parameter. Empty means UTC. "Local" is refused so
// answers never depend on the host's zone.
func loadZone(name string) (*time.Location, string, *apiError) {
	name = strings.TrimSpace(name)
	if name == "" {
		return time.UTC, "UTC", nil
	}
	if strings.EqualFold(name, "local") {
		return nil, "", badRequest("invalid_tz", "tz must be an IANA zone name such as Europe/London")
	}
	loc, errLoad := time.LoadLocation(name)
	if errLoad != nil {
		return nil, "", badRequest("invalid_tz", "unknown time zone "+quote(name))
	}
	return loc, name, nil
}

// parseRange reads from, to, and tz. Dates (YYYY-MM-DD) are whole local days:
// from=D starts at local midnight of D, to=D ends at local midnight after D
// (to is exclusive, so the day D is included). RFC 3339 values are instants.
func parseRange(q url.Values) (timeRange, *apiError) {
	loc, tzName, errTZ := loadZone(q.Get("tz"))
	if errTZ != nil {
		return timeRange{}, errTZ
	}
	from, errFrom := parseBound(q.Get("from"), "from", loc, false)
	if errFrom != nil {
		return timeRange{}, errFrom
	}
	to, errTo := parseBound(q.Get("to"), "to", loc, true)
	if errTo != nil {
		return timeRange{}, errTo
	}
	if !from.Before(to) {
		return timeRange{}, badRequest("invalid_range", "from must be before to")
	}
	if to.After(from.In(loc).AddDate(0, 0, maxRangeDays)) {
		return timeRange{}, badRequest("range_too_large", "the range is limited to 400 days")
	}
	return timeRange{From: from, To: to, Loc: loc, TZ: tzName}, nil
}

func parseBound(raw, name string, loc *time.Location, isTo bool) (time.Time, *apiError) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, badRequest("missing_param", name+" is required")
	}
	if localDatePattern.MatchString(raw) {
		day, errParse := time.ParseInLocation(localDateForm, raw, time.UTC)
		if errParse != nil {
			return time.Time{}, badRequest("invalid_"+name, name+" is not a valid date: "+quote(raw))
		}
		if isTo {
			day = day.AddDate(0, 0, 1)
		}
		return startOfLocalDay(day.Year(), day.Month(), day.Day(), loc), nil
	}
	parsed, errParse := time.Parse(time.RFC3339Nano, raw)
	if errParse != nil {
		return time.Time{}, badRequest("invalid_"+name, name+" must be YYYY-MM-DD or RFC 3339: "+quote(raw))
	}
	return parsed, nil
}

// startOfLocalDay returns the first instant of the local date. Where midnight
// does not exist (a DST gap at 00:00) it returns the first instant that does.
// Out-of-range days (day 0, day 32) are normalized like time.Date does.
func startOfLocalDay(year int, month time.Month, day int, loc *time.Location) time.Time {
	year, month, day = time.Date(year, month, day, 12, 0, 0, 0, time.UTC).Date()
	t := time.Date(year, month, day, 0, 0, 0, 0, loc)
	y, m, d := t.Date()
	if y == year && m == month && d == day && t.Hour() == 0 && t.Minute() == 0 {
		return t
	}
	// Walk forward in 15-minute steps from a few hours earlier; real zone
	// transitions happen on quarter hours.
	probe := t.Add(-4 * time.Hour).Truncate(15 * time.Minute)
	for i := 0; i < 64; i++ {
		y, m, d = probe.In(loc).Date()
		if y == year && m == month && d == day {
			return probe
		}
		probe = probe.Add(15 * time.Minute)
	}
	return t
}

// ---- Buckets ----

const (
	bucketHour = "hour"
	bucketDay  = "day"
	bucketWeek = "week"
)

// bucketEdges returns the boundaries of the local-calendar buckets covering
// [from, to): edges[0] <= from, edges[len-1] >= to, and bucket i is
// [edges[i], edges[i+1]). Days follow local midnights, so DST days are 23 or
// 25 hours long; weeks start on Monday.
func bucketEdges(from, to time.Time, loc *time.Location, bucket string) ([]time.Time, *apiError) {
	local := from.In(loc)
	var first time.Time
	var next func(time.Time) time.Time
	switch bucket {
	case bucketHour:
		first = local.Add(-time.Duration(local.Minute())*time.Minute -
			time.Duration(local.Second())*time.Second - time.Duration(local.Nanosecond()))
		next = func(t time.Time) time.Time { return t.Add(time.Hour) }
	case bucketDay, bucketWeek:
		y, m, d := local.Date()
		step := 1
		if bucket == bucketWeek {
			d -= (int(local.Weekday()) + 6) % 7 // back to Monday
			step = 7
		}
		first = startOfLocalDay(y, m, d, loc)
		next = func(t time.Time) time.Time {
			// Noon is never inside a DST transition, so its date is safe to step.
			ny, nm, nd := t.In(loc).Add(12 * time.Hour).Date()
			return startOfLocalDay(ny, nm, nd+step, loc)
		}
	default:
		return nil, badRequest("invalid_bucket", "bucket must be hour, day, or week")
	}
	edges := []time.Time{first.In(loc)}
	for edges[len(edges)-1].Before(to) {
		if len(edges) > maxBuckets {
			return nil, badRequest("too_many_buckets", "the range has more than 2000 buckets; use a larger bucket")
		}
		edges = append(edges, next(edges[len(edges)-1]).In(loc))
	}
	return edges, nil
}

// bucketIndex returns i such that edges[i] <= at < edges[i+1], or -1.
func bucketIndex(edges []time.Time, at int64) int {
	lo, hi := 0, len(edges)-1
	if hi < 1 || at < edges[0].Unix() || at >= edges[hi].Unix() {
		return -1
	}
	for hi-lo > 1 {
		mid := (lo + hi) / 2
		if edges[mid].Unix() <= at {
			lo = mid
		} else {
			hi = mid
		}
	}
	return lo
}

// edgesAligned reports whether every time the query depends on is a whole UTC
// hour, so hour rows answer it exactly.
func edgesAligned(times ...time.Time) bool {
	for _, t := range times {
		if !isWholeHour(t) {
			return false
		}
	}
	return true
}

func formatTime(t time.Time, loc *time.Location) string {
	return t.In(loc).Format(time.RFC3339)
}

func quote(s string) string {
	if len(s) > 64 {
		s = s[:64] + "…"
	}
	return `"` + s + `"`
}
