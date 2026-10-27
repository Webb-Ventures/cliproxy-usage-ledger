# cliproxy-usage-ledger

`usage-ledger` is a native backend plugin for
[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) (CPA). It receives
the usage record CPA emits after every model request and keeps a local,
retention-bounded ledger of token usage that the management UI can query. It
also serves model and plan prices so the UI can estimate cost. The plugin
returns token counts and per-token prices, never dollars: the frontend prices
usage at read time, so old ranges are re-priced the same way ccusage does.

> **Status: prices and routes (step 3).** Records are rolled up into
> per-minute and per-hour buckets in SQLite (schema v2), and the summary,
> series, windows, blocks, prices, settings, and export routes are served.
> Tagged releases publish prebuilt libraries (see [Releases](#releases)).

## How it plugs into CPA

CPA loads plugins as C-ABI shared libraries (ABI v1) built with
`go build -buildmode=c-shared`. It calls `cliproxy_plugin_init`, then talks to
the plugin through JSON envelopes (`{"ok":true,"result":...}`). This plugin
declares two capabilities:

| Capability | RPC methods | What it does now |
| --- | --- | --- |
| `usage_plugin` | `usage.handle` | Pushes the record into a 4096-slot channel without blocking. When the channel is full, the record is dropped and `dropped_total` goes up. A worker goroutine drains the channel and writes rollups to SQLite in batches. |
| `management_api` | `management.register`, `management.handle` | Serves the routes under `/v0/management/usage-ledger/` (see [Management routes](#management-routes)). |

The JSON shapes are copied into `plugin.go` instead of importing the CPA SDK,
the same way the single-capability `usage` and `management-api` examples in
CPA do. CPA's token-accounting normalization (`EnsureTokenBreakdownForProvider`)
is copied into `tokens.go` for the same reason; the file header names the
upstream file and commit. The only dependencies are `gopkg.in/yaml.v3` and the
pure-Go SQLite driver `modernc.org/sqlite`, which needs no cgo of its own. The
IANA zone database is compiled in (`time/tzdata`), so `tz` works on hosts
without one.
`modernc.org/sqlite` is pinned to v1.59.0 because v1.60+ requires `go 1.26.0`
in `go.mod`.

## Storage

The ledger is one SQLite file (see `database-path`) opened in WAL mode with a
5 second `busy_timeout`. While the plugin runs you will also see `-wal` and
`-shm` files next to it; they are folded back in and removed when the database
is closed.

| Table | Contents |
| --- | --- |
| `schema_version` | One row per applied migration (`version`, `applied_at`). The current schema is version 2. A database from a newer plugin is refused rather than modified. |
| `meta` | Key/value pairs. `recording_since` is the UTC time the database was created; `settings` holds the [settings](#get-and-put-usage-ledgersettings) document. |
| `usage_minute` | Rollups per UTC minute. |
| `usage_hour` | Rollups per UTC hour. |

Both rollup tables have the same columns. A row is keyed by:

| Column | Meaning |
| --- | --- |
| `bucket_start` | Start of the UTC minute or hour, as Unix seconds. Taken from the record's `RequestedAt` (the plugin's clock if that is missing). |
| `provider` | `Provider`, for example `claude`, `gemini`, `codex`. |
| `model` | `ResponseModel`, the model the upstream response reported, falling back to `Model`, the model CPA requested upstream (internal/pluginhost/adapters_usage_translation.go:212 and :197). |
| `service_tier` | The tier the request ran at, lowercased: `ResponseServiceTier` (reported by the upstream), else `EffectiveServiceTier` (the tier in the final outbound request), else `ServiceTier` (what the client asked for; CPA fills `auto` when the client sent none) (adapters_usage_translation.go:209-211). Empty for rows written before schema v2. |
| `auth_index` | `AuthIndex`, CPA's stable hashed credential index (the `auth_index` the Management API reports for auth files). `AuthID` is not stored because it is often a file name containing an e-mail address. |
| `client_key` | The client API key, masked: the first 8 hex characters of its SHA-256 hash, `…`, and its last 4 characters, e.g. `3f9a0c12…a1b2`. Keys shorter than 12 characters keep only the hash part. Empty when the request had no client key. |
| `failed` | `1` if the request failed, else `0`. |

Both tables also have an index on `(auth_index, bucket_start)` for the windows
route. AuthType is not stored: the frontend resolves credentials from
`auth_index`.

and accumulates:

| Column | Meaning |
| --- | --- |
| `request_count`, `failure_count` | Requests, and how many of them failed. |
| `input_tokens` | Uncached input tokens. |
| `cache_read_tokens`, `cache_creation_tokens` | Cache reads and cache writes. |
| `output_tokens` | Non-reasoning output tokens. |
| `reasoning_tokens` | Reasoning tokens. |
| `unclassified_tokens` | Tokens the provider reported without saying which bucket they belong to (unknown providers, or contradictory counters). |
| `total_tokens` | Always the sum of the six token columns above. |
| `latency_ms_sum` | Sum of request latency in milliseconds. Divide by `request_count` for the mean. |

The token columns never overlap. Providers report cache and reasoning tokens
differently (OpenAI-style counters include them in input/output, Claude
reports them separately, Gemini reports reasoning separately), so each record
is normalized with CPA's own provider rules before it is added.

### What is stored and what isn't

Only the fields above are persisted. The worker copies them out of each
record (`eventFromRecord` in `store.go`) and drops the rest, and a test checks
that the schema has exactly these columns. The plugin never stores:

- the client API key itself (only the mask above);
- request or response headers, request or response bodies, prompts, or failure
  messages and status codes;
- request, trace, selection, or session IDs, base URLs, `AuthID`, `AuthType`,
  the model alias, the reasoning effort, or the access provider.

### Schema v2 migration

Step 2 databases (schema v1) are upgraded in place the first time this
version opens them, in one transaction. SQLite cannot change a primary key, so
each rollup table is rebuilt: a new table with `service_tier` in its key is
created, the rows are copied with `service_tier = ''`, the old table is
dropped, the new one is renamed, and the `(auth_index, bucket_start)` index is
added. Rows written before the upgrade keep the requested model in `model`;
the response model is only known for new records.

### Ingest rules

- Records with `Generate=false` are skipped: they are not queued or stored,
  and they count in `skipped_total`. `pluginapi.UsageRecord` has no separate
  count-only flag; `Generate` is the only one (sdk/pluginapi/types.go:1593,
  filled at adapters_usage_translation.go:213 from
  `coreusage.GenerateEnabled`, which treats a missing value as `true`). CPA's
  count-tokens executors never publish usage records, so token-count calls
  never reach the plugin. `Generate=false` comes from a client body with
  `"generate": false` (sdk/api/handlers/handlers.go:416-427) and from the
  zero-token record CPA publishes for an upstream 401 outside an executor
  (sdk/cliproxy/auth/home_result.go:74), so those 401s are not counted as
  failures either. A record without the field counts as a generation request.

### Writes and retention

The worker writes in batches: a batch is committed when it reaches 256
records or 1 second after its first record, whichever comes first. Each
commit is one transaction that adds the batch to both rollup tables with
`INSERT … ON CONFLICT … DO UPDATE`. If a commit fails, the batch is discarded,
`write_errors_total` goes up, `last_write_error` keeps the message, and the
worker carries on.

Retention runs whenever the database is opened (register, reconfigure) and
then every hour. It deletes `usage_minute` rows whose bucket starts more than
`minute-retention-days` ago and `usage_hour` rows more than
`hour-retention-days` ago.

## Lifecycle

- **`plugin.register` / `plugin.reconfigure`** open the database. If
  `database-path` changed, the pending batch is flushed to the old file, the
  new file is opened, and the old one is closed. If the new path cannot be
  opened, the call fails and the old database stays in use.
- **`plugin.quiesce`** is sent by CPA before it hot-replaces the plugin with a
  new version. The plugin writes every record already queued, commits, closes
  the database, and answers `{}`. This releases the file before the new copy
  opens it: two copies of SQLite in one process must not hold the same file.
  Records that arrive after quiesce wait in the queue. If CPA rolls the
  replacement back, it sends `plugin.reconfigure`, the database is reopened,
  and they are written. If this copy is shut down instead, they are counted
  in `dropped_total` and the file is left alone.
- **Shutdown** (the C-ABI `shutdown` call) stops accepting records, writes
  everything queued, commits, and closes the database.
- **Prices.** Register and reconfigure (re)start the price refresher without
  waiting for it; quiesce and shutdown stop it, so a hot-replaced copy never
  fetches alongside its replacement.

Before the first register, and after quiesce, the worker does not take
records off the queue, so a full queue drops new records as usual.

### Reading the ledger

Route handlers never use the worker's write connection. Whenever the worker
opens a database it also opens a second connection pool on the same file
with `PRAGMA query_only` (`openReader` in `query.go`), and every query runs
there:

- **Why a read-only connection rather than requests to the worker.** In WAL
  mode readers see the last committed state without blocking the writer, so a
  400-day summary or a 500-entry windows request does not hold up batch
  commits. Sending reads to the worker would serialize them behind writes and
  delay ingest while they ran. `query_only` makes the pool unable to write.
- **The worker is still the only goroutine that opens, swaps, or closes
  either handle.** Handlers borrow the reader under `Plugin.readMu` (shared).
  Before it replaces or closes the reader (reconfigure to a new path,
  quiesce, shutdown), the worker takes `readMu` exclusively. That waits for
  in-flight queries, so no query runs on a closed pool, and quiesce releases
  the file only after they finish. Each query has a 15 second timeout, which
  bounds that wait.
- **Settings writes** (`PUT settings`) use the worker's connection under
  `storeMu`, the lock the batch writes, retention, and status already use.
- While no database is open, the reader is `nil` and data routes answer 503.

`TestReadsDuringIngestSwapAndQuiesce` runs every route from several
goroutines while records are ingested and the database is swapped between two
paths and quiesced repeatedly, under `-race`.

## Management routes

All routes live under `/v0/management/usage-ledger/` and use CPA's normal
Management API authentication (the management key as a bearer token). CPA
mounts plugin routes under `/v0/management` only
(internal/pluginhost/management.go), not under `/v8/management`. Each route is
declared as an exact method and path, so `PUT settings` and `GET settings` are
two declarations.

| Method | Path | What it returns |
| --- | --- | --- |
| GET | `usage-ledger/status` | Ingest counters, database, retention, price source. |
| GET | `usage-ledger/summary` | Totals for a range, grouped, each with a per-model breakdown. |
| GET | `usage-ledger/series` | The same per hour, day, or week bucket, optionally per group. |
| POST | `usage-ledger/windows` | Tokens per credential since each limit window started. |
| GET | `usage-ledger/blocks` | ccusage-style 5-hour blocks. |
| GET | `usage-ledger/prices` | Model prices per token, plan prices, and the stored overrides. |
| GET, PUT | `usage-ledger/settings` | Plan and model price overrides. |
| GET | `usage-ledger/export` | CSV of the series rows. |

### Conventions

- Every JSON document has `"schema": 1`. JSON is UTF-8 and is never
  HTML-escaped by the host (RPC schema version 6).
- Errors are `{"error": "<message>", "code": "<code>"}`. Client errors are
  always HTTP 400, including an oversized body. Codes: `missing_param`,
  `invalid_from`, `invalid_to`, `invalid_range` (from is not before to),
  `range_too_large` (over 400 days), `invalid_tz`, `invalid_group`,
  `invalid_bucket`, `too_many_buckets` (over 2000), `invalid_body`,
  `body_too_large` (over 256 KB), `too_many_windows` (over 500),
  `invalid_window`, `invalid_settings`. Other statuses: 404 `not_found`,
  405 `method_not_allowed` (with an `Allow` header), 503 `unavailable` (no
  database open: before register, or between quiesce and the next
  reconfigure), 500 `internal`.
- Times in responses are RFC 3339 with the offset of `tz` (UTC renders as `Z`).
  `recording_since`, `now`, and price times are always UTC.
- Token objects are always the same seven non-overlapping counters:

  ```json
  { "input": 0, "cache_read": 0, "cache_write": 0, "output": 0, "reasoning": 0, "unclassified": 0, "total": 0 }
  ```

  `input` is uncached input, `output` is non-reasoning output, and `total` is
  the sum of the other six.
- A usage block (`usage`) is `{requests, failed, tokens, by_model[]}`, where
  `by_model[]` entries are
  `{model, service_tier, requests, failed, tokens}`. `requests` includes failed
  requests. `by_model` is sorted by `tokens.total` (descending), then model and
  tier, and is `[]` when empty.

#### Range parameters

`summary`, `series`, `export`, and `blocks` take:

| Parameter | Meaning |
| --- | --- |
| `from` | Required. A local date `YYYY-MM-DD` (local midnight in `tz`) or an RFC 3339 instant. Inclusive. |
| `to` | Required. A local date or RFC 3339 instant. Exclusive: a date means the end of that day, so `to=2026-10-27` covers through 23:59:59 on the 27th. An RFC 3339 `to` excludes that instant. |
| `tz` | IANA zone, default `UTC`. `Local` and unknown zones are rejected (`invalid_tz`). The zone database is compiled in. |

`from` must be before `to`, and `to` at most 400 calendar days after `from`.
Ranges before recording began or after now are allowed; they return zeros.

#### Resolution

Summary and series pick their rows like this, and report it in
`range.resolution` and `range.approximate`:

| `resolution` | When | Exact? |
| --- | --- | --- |
| `minute` | `from` is inside minute retention. | Yes. |
| `hour` | Older ranges whose every boundary (from, to, and for series each bucket edge) is a whole UTC hour. | Yes. |
| `hour` | The range ends before minute retention but a boundary is not a whole hour. | No, `approximate: true`. |
| `mixed` | The range starts before minute retention and a boundary is not a whole hour, such as any day in a +05:30 zone. Hour rows answer the part before the minute-retention edge, minute rows the rest. | No, `approximate: true`. |

Hour rows are placed by their midpoint, so an off-hour boundary is wrong by at
most 30 minutes, and only before the minute-retention edge. This keeps a 30-day
range in Asia/Kolkata complete instead of dropping the days that only exist as
hour rows.

### GET usage-ledger/status

```sh
curl -H "Authorization: Bearer $MANAGEMENT_KEY" \
  http://127.0.0.1:8317/v0/management/usage-ledger/status
```

```json
{
  "schema": 1,
  "plugin_version": "0.1.0",
  "records_total": 1520,
  "dropped_total": 0,
  "skipped_total": 3,
  "write_errors_total": 0,
  "last_write_error": null,
  "recording_since": "2026-10-27T02:18:51Z",
  "last_record_at": "2026-10-27T09:41:07Z",
  "retention": { "minute_days": 14, "hour_days": 400 },
  "database_path": "/var/lib/cliproxy/usage-ledger.db",
  "database_size_bytes": 49448,
  "row_counts": { "meta": 2, "schema_version": 2, "usage_hour": 12, "usage_minute": 85 },
  "prices": { "source": "remote", "fetched_at": "2026-10-27T02:18:52Z" }
}
```

| Field | Meaning |
| --- | --- |
| `records_total` | Records committed to the database by this plugin instance. |
| `dropped_total` | Records lost because the queue was full, or that were still queued when a quiesced instance shut down. |
| `skipped_total` | Records with `Generate=false`, which are not stored (see [Ingest rules](#ingest-rules)). |
| `write_errors_total`, `last_write_error` | Failed commits, retention runs, or closes, and the most recent error message (`null` if none). |
| `recording_since` | When the database was created, from its `meta` table. |
| `last_record_at` | When this instance last committed records (RFC 3339 UTC), or `null`. |
| `database_path`, `database_size_bytes`, `row_counts` | The open database, its size (main file plus WAL), and the row count of each table. |
| `prices.source`, `prices.fetched_at` | The price table in use (`remote`, `cache`, `embedded`, or `none`) and when it was fetched (`null` for the embedded snapshot). |

The database fields (`recording_since`, `database_path`, `database_size_bytes`,
`row_counts`) are `null` while no database is open. Status never returns 503.

### GET usage-ledger/summary

Parameters: the range parameters, plus `group` = `model` (default),
`credential`, `provider`, or `client_key`.

```sh
curl -H "Authorization: Bearer $MANAGEMENT_KEY" \
  "http://127.0.0.1:8317/v0/management/usage-ledger/summary?from=2026-09-28&to=2026-10-27&group=credential&tz=Europe/London"
```

```json
{
  "schema": 1,
  "range": {
    "from": "2026-09-28T00:00:00+01:00",
    "to": "2026-10-28T00:00:00Z",
    "tz": "Europe/London",
    "recording_since": "2026-06-14T09:12:00Z",
    "resolution": "hour",
    "approximate": false
  },
  "group": "credential",
  "groups": [
    {
      "key": "auth:7f3a9c0d12e4b5a6",
      "requests": 9880,
      "failed": 41,
      "tokens": { "input": 14200000, "cache_read": 362900000, "cache_write": 18600000, "output": 4100000, "reasoning": 1400000, "unclassified": 0, "total": 401200000 },
      "by_model": [
        {
          "model": "claude-opus-5-5",
          "service_tier": "auto",
          "requests": 9100,
          "failed": 40,
          "tokens": { "input": 13000000, "cache_read": 350000000, "cache_write": 18000000, "output": 3900000, "reasoning": 1300000, "unclassified": 0, "total": 386200000 }
        }
      ]
    }
  ],
  "totals": {
    "requests": 39598,
    "failed": 120,
    "tokens": { "input": 50000000, "cache_read": 900000000, "cache_write": 40000000, "output": 12000000, "reasoning": 5000000, "unclassified": 0, "total": 1007000000 },
    "by_model": []
  }
}
```

- `groups[]` is `{key, requests, failed, tokens, by_model}`, sorted by
  `tokens.total` (descending), then `key`. It is `[]` when there is no data.
- Keys are `model:<model>`, `auth:<auth_index>`, `provider:<provider>`, or
  `key:<masked client key>`. A record without a credential or client key gives
  `auth:` or `key:`. There are no labels; the frontend maps `auth_index` to
  credential names itself.
- `totals` has the same shape as a group without `key`; its `by_model` covers
  every group (abbreviated above).
- `range.to` is the exclusive end. `recording_since` is `null` if unknown.

### GET usage-ledger/series

Parameters: the range parameters, plus `bucket` = `hour`, `day` (default), or
`week`, and an optional `group` (as in summary).

```sh
curl -H "Authorization: Bearer $MANAGEMENT_KEY" \
  "http://127.0.0.1:8317/v0/management/usage-ledger/series?from=2026-10-24&to=2026-10-26&bucket=day&tz=Europe/London&group=credential"
```

```json
{
  "schema": 1,
  "range": { "from": "2026-10-24T00:00:00+01:00", "to": "2026-10-27T00:00:00Z", "tz": "Europe/London", "recording_since": "2026-10-24T15:00:00Z", "resolution": "minute", "approximate": false },
  "bucket": "day",
  "group": "credential",
  "buckets": [
    { "start": "2026-10-24T00:00:00+01:00", "end": "2026-10-25T00:00:00+01:00", "before_recording": false, "requests": 12, "failed": 0, "tokens": { "input": 900, "cache_read": 40000, "cache_write": 1200, "output": 3000, "reasoning": 0, "unclassified": 0, "total": 45100 }, "by_model": [ { "model": "claude-opus-5-5", "service_tier": "auto", "requests": 12, "failed": 0, "tokens": { "input": 900, "cache_read": 40000, "cache_write": 1200, "output": 3000, "reasoning": 0, "unclassified": 0, "total": 45100 } } ] },
    { "start": "2026-10-25T00:00:00+01:00", "end": "2026-10-26T00:00:00Z", "before_recording": false, "requests": 0, "failed": 0, "tokens": { "input": 0, "cache_read": 0, "cache_write": 0, "output": 0, "reasoning": 0, "unclassified": 0, "total": 0 }, "by_model": [] },
    { "start": "2026-10-26T00:00:00Z", "end": "2026-10-27T00:00:00Z", "before_recording": false, "requests": 0, "failed": 0, "tokens": { "input": 0, "cache_read": 0, "cache_write": 0, "output": 0, "reasoning": 0, "unclassified": 0, "total": 0 }, "by_model": [] }
  ],
  "groups": [
    { "key": "auth:7f3a9c0d12e4b5a6", "buckets": [ "… same bucket shape, one per bucket …" ] }
  ]
}
```

- Buckets follow the local calendar of `tz`: the 2026-10-25 bucket above is 25
  hours long, a spring-forward day is 23, an `hour` series on a fall-back day
  has 25 buckets (01:00 appears twice, with different offsets), and weeks start
  on Monday.
- Every bucket between `from` and `to` is present, with zeros and
  `by_model: []` when empty. If `from` or `to` is not on a bucket boundary,
  the first or last bucket keeps its calendar `start`/`end` but only counts
  data inside the range.
- `before_recording` is `true` when the whole bucket ends at or before
  `recording_since`.
- `group` is `null` and `groups` is absent without the `group` parameter.
  With it, `groups[]` is `{key, buckets[]}` sorted by key, each with every
  bucket; `buckets` still holds the totals.
- At most 2000 buckets (`too_many_buckets`): about 83 days of `hour`.

### POST usage-ledger/windows

Body (at most 500 entries, 256 KB): each credential and the time its limit
window last reset. Returns tokens from `since` until now, including the
current minute.

```sh
curl -X POST -H "Authorization: Bearer $MANAGEMENT_KEY" -H "Content-Type: application/json" \
  -d '{"windows":[{"auth_index":"7f3a9c0d12e4b5a6","since":"2026-10-27T05:00:00Z"}]}' \
  http://127.0.0.1:8317/v0/management/usage-ledger/windows
```

```json
{
  "schema": 1,
  "now": "2026-10-27T09:41:30Z",
  "minute_data_since": "2026-10-13T09:42:00Z",
  "windows": [
    {
      "auth_index": "7f3a9c0d12e4b5a6",
      "since": "2026-10-27T05:00:00Z",
      "resolution": "minute",
      "approximate": false,
      "requests": 3,
      "failed": 0,
      "tokens": { "input": 300, "cache_read": 120, "cache_write": 30, "output": 90, "reasoning": 36, "unclassified": 0, "total": 576 },
      "by_model": [ { "model": "claude-opus-5-5", "service_tier": "auto", "requests": 3, "failed": 0, "tokens": { "input": 300, "cache_read": 120, "cache_write": 30, "output": 90, "reasoning": 36, "unclassified": 0, "total": 576 } } ]
    }
  ]
}
```

- `windows[]` is in request order, one result per entry (duplicates allowed).
  `since` is echoed in UTC.
- Minute rows are used when `since` is inside minute retention
  (`minute_data_since`). Otherwise hour rows answer the part before that edge
  and `approximate` is `true` (`resolution` `mixed`, or `hour`).
- A `since` in the future returns zeros. A `since` that is not RFC 3339 or is
  more than 400 days ago is `invalid_window`. Unknown fields are
  `invalid_body`.

### GET usage-ledger/blocks

Parameters: `from`, `to`, and `tz` (as above; `tz` only affects date parsing
and how times are rendered).

```sh
curl -H "Authorization: Bearer $MANAGEMENT_KEY" \
  "http://127.0.0.1:8317/v0/management/usage-ledger/blocks?from=2026-10-01&to=2026-10-27"
```

```json
{
  "schema": 1,
  "range": {
    "from": "2026-10-13T09:42:00Z",
    "to": "2026-10-28T00:00:00Z",
    "requested_from": "2026-10-01T00:00:00Z",
    "tz": "UTC",
    "clamped": true,
    "minute_data_since": "2026-10-13T09:42:00Z",
    "recording_since": "2026-06-14T09:12:00Z",
    "resolution": "minute"
  },
  "session_hours": 5,
  "blocks": [
    {
      "start": "2026-10-27T09:00:00Z",
      "end": "2026-10-27T14:00:00Z",
      "actual_end": "2026-10-27T09:40:00Z",
      "active": true,
      "peak_tokens_per_minute": 960,
      "burn_rate_tokens_per_minute": 480,
      "active_minutes": 2,
      "auth_indexes": ["7f3a9c0d12e4b5a6", "c2d4e6f8a0b1c3d5"],
      "requests": 5,
      "failed": 1,
      "tokens": { "input": 500, "cache_read": 200, "cache_write": 50, "output": 150, "reasoning": 60, "unclassified": 0, "total": 960 },
      "by_model": [ { "model": "claude-opus-5-5", "service_tier": "auto", "requests": 5, "failed": 1, "tokens": { "input": 500, "cache_read": 200, "cache_write": 50, "output": 150, "reasoning": 60, "unclassified": 0, "total": 960 } } ]
    }
  ]
}
```

- Block rules follow ccusage (`identifySessionBlocks`): a block starts at the
  first activity, floored to the UTC hour, and lasts 5 hours. Activity more
  than 5 hours after the block start, or more than 5 hours after the previous
  activity, starts a new block. The comparison is strict, as in ccusage, so a
  minute exactly 5 hours after the start still belongs to the block. Gaps are
  not reported as blocks.
- Activity comes from minute rows, so times have minute resolution.
  `actual_end` is the start of the last active minute. `active_minutes` counts
  minutes with at least one request, `peak_tokens_per_minute` is the busiest
  of them, and `burn_rate_tokens_per_minute` is `tokens.total / active_minutes`
  (two decimals). `active` is true when now is in `[start, end)`.
- Minute rows only: `from` is clamped to `minute_data_since`, and
  `clamped`/`requested_from` say so. A range entirely before it returns
  `blocks: []`.
- Blocks are detected over all minute rows, so their boundaries don't depend
  on `from`. Every block that overlaps `[from, to)` is returned in full,
  oldest first.

### GET usage-ledger/prices

```sh
curl -H "Authorization: Bearer $MANAGEMENT_KEY" \
  http://127.0.0.1:8317/v0/management/usage-ledger/prices
```

```json
{
  "schema": 1,
  "source": "remote",
  "fetched_at": "2026-10-27T02:18:52Z",
  "source_url": "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json",
  "embedded_as_of": "2026-10-27",
  "last_attempt_at": "2026-10-27T02:18:52Z",
  "last_error": null,
  "models": {
    "claude-opus-5-5": {
      "provider": "anthropic", "mode": "chat",
      "input": 0.000004, "output": 0.00002, "cache_read": 2e-7, "cache_write": 0.000005,
      "cache_write_1h": 0.000008
    },
    "gpt-5.5": {
      "provider": "openai", "mode": "chat",
      "input": 0.000005, "output": 0.00003, "cache_read": 5e-7,
      "tiers": [ { "above_tokens": 272000, "input": 0.00001, "output": 0.000045, "cache_read": 0.000001 } ],
      "service_tiers": {
        "flex": { "input": 0.0000025, "output": 0.000015, "cache_read": 2.5e-7 },
        "priority": { "input": 0.0000125, "output": 0.000075, "cache_read": 0.00000125 }
      }
    }
  },
  "plans": {
    "claude": {
      "max_5x": { "name": "Claude Max 5x", "monthly_price": 100, "currency": "USD", "billing": "per_account", "as_of": "2026-10-27", "source_url": "https://claude.com/pricing", "verified": true, "note": "…" }
    }
  },
  "plans_as_of": "2026-10-27",
  "overrides": { "plan_overrides": {}, "model_price_overrides": {}, "default_currency": "USD", "updated_at": null }
}
```

- `source` is `remote` (fetched by this process), `cache` (the last good copy
  on disk), `embedded` (the build-time snapshot), or `none` (nothing usable;
  `models` is `{}`). `fetched_at` is `null` for `embedded` and `none`.
  `source_url` is where the current table came from. `last_attempt_at` and
  `last_error` describe the most recent fetch (`null` before the first one or
  when fetching is disabled); a failed fetch keeps the previous table.
- `models` is keyed exactly as LiteLLM keys it. All prices are USD per token.
  Every field except `provider`/`mode` may be absent:

  | Field | LiteLLM field |
  | --- | --- |
  | `input`, `output` | `input_cost_per_token`, `output_cost_per_token` |
  | `cache_read`, `cache_write` | `cache_read_input_token_cost`, `cache_creation_input_token_cost` |
  | `cache_write_1h` | `cache_creation_input_token_cost_above_1hr` |
  | `reasoning` | `output_cost_per_reasoning_token` (only where the source sets it; otherwise price reasoning as `output`) |
  | `tiers[]` `{above_tokens, input, output, cache_read, cache_write}` | `*_above_<N>k_tokens`, sorted by `above_tokens` |
  | `service_tiers.priority`, `service_tiers.flex` | `*_priority`, `*_flex` |

  Only `chat`, `completion`, and `responses` models (and entries without a
  mode) with an input or output price are served.
- `plans` comes from the embedded `plan-prices.json`, keyed by provider
  (`claude`, `codex`, `xai`, `antigravity`, `devin`) and then plan. Entries are
  `{name, monthly_price, currency, billing, as_of, source_url, verified, note?}`;
  `billing` is `per_account` or `per_seat`, and `verified: false` marks prices
  that could not be read on the provider's own page.
- `overrides` is the settings document (see below) without `schema` and
  `retention`, and `null` while no database is open. It is never merged into
  `models` or `plans`.

**Model name matching is left to the frontend.** Usage rows carry the model
the upstream reported, which may differ from LiteLLM's key. Some providers
need a prefix (`moonshot/kimi-k3`, `xai/grok-…`, `vertex_ai/…`,
`chatgpt/gpt-…`), some responses add a date suffix
(`claude-opus-5-5-20261001`, `gpt-5.5-2026-09-01`), and the same model can
appear under several providers. A sensible lookup: try the exact name, then
with the row's provider prefix, then without a trailing `-YYYYMMDD` or
`-YYYY-MM-DD`, then a `model_price_overrides` entry.

### GET and PUT usage-ledger/settings

Shared overrides, stored as JSON in the database's `meta` table (key
`settings`), so every browser sees the same values. Retention comes from the
CPA plugin config and is read-only here.

```sh
curl -H "Authorization: Bearer $MANAGEMENT_KEY" \
  http://127.0.0.1:8317/v0/management/usage-ledger/settings
```

```json
{
  "schema": 1,
  "plan_overrides": {
    "7f3a9c0d12e4b5a6": { "provider": "claude", "plan": "max_20x", "monthly_price": 180, "currency": "EUR" }
  },
  "model_price_overrides": {
    "kimi-k3": { "input": 0.000003, "output": 0.000012 }
  },
  "default_currency": "EUR",
  "updated_at": "2026-10-27T09:45:00Z",
  "retention": { "minute_days": 14, "hour_days": 400 }
}
```

`PUT` takes the same document and replaces the stored settings entirely
(an empty `{}` clears them). It answers with the stored document.

```sh
curl -X PUT -H "Authorization: Bearer $MANAGEMENT_KEY" -H "Content-Type: application/json" \
  -d '{"plan_overrides":{"7f3a9c0d12e4b5a6":{"provider":"claude","plan":"max_20x","monthly_price":180,"currency":"EUR"}},"default_currency":"EUR"}' \
  http://127.0.0.1:8317/v0/management/usage-ledger/settings
```

Validation (`invalid_settings` unless noted):

- The body is at most 256 KB (`body_too_large`) and one JSON object. Unknown
  fields are `invalid_body`. `schema`, `retention`, and `updated_at` are
  accepted and ignored, so a GET document can be sent back as is.
- `plan_overrides` is keyed by `auth_index` (1-256 characters). `provider`
  and `plan` are 1-64 characters, `monthly_price` is 0 to 1,000,000, and
  `currency` is an ISO 4217 code (three capital letters) that defaults to
  `default_currency`.
- `model_price_overrides` is keyed by model name (1-256 characters). Each
  entry sets at least one of `input`, `output`, `cache_read`, or
  `cache_write`, in USD per token between 0 and 1.
- `default_currency` is an ISO 4217 code; it defaults to `USD`.
- At most 2000 overrides in total.

### GET usage-ledger/export

The same parameters as series. Returns `text/csv; charset=utf-8` with
`Content-Disposition: attachment; filename="usage-ledger_<from>_<last day>_<bucket>.csv"`,
one row per bucket × group × model (and tier). Empty buckets have no rows.

```sh
curl -OJ -H "Authorization: Bearer $MANAGEMENT_KEY" \
  "http://127.0.0.1:8317/v0/management/usage-ledger/export?from=2026-10-27&to=2026-10-27&bucket=day&group=credential"
```

```csv
bucket_start,bucket_end,group,model,service_tier,requests,failed,input_tokens,cache_read_tokens,cache_write_tokens,output_tokens,reasoning_tokens,unclassified_tokens,total_tokens
2026-10-27T00:00:00Z,2026-10-28T00:00:00Z,auth:7f3a9c0d12e4b5a6,claude-opus-5-5,auto,4,1,400,160,40,120,48,0,768
2026-10-27T00:00:00Z,2026-10-28T00:00:00Z,auth:7f3a9c0d12e4b5a6,claude-opus-5-5-20261001,priority,1,0,100,40,10,30,12,0,192
```

Rows are ordered by group, then bucket, then the by_model order. `group` is
empty without the `group` parameter. Fields are quoted per RFC 4180 when they
contain a comma, quote, or line break. Text fields (`group`, `model`,
`service_tier`) that start with `=`, `+`, `-`, `@`, a tab, or a carriage
return get a leading `'`, so a model name chosen by a client can't run as a
spreadsheet formula.

## Prices

**Model prices** come from LiteLLM's `model_prices_and_context_window.json`
at `price-source-url`. The plugin fetches it on the first register (on the
refresher goroutine, so register never waits on the network), then every
`price-refresh-hours`. Each refresh makes exactly one request: a 30 second
timeout, `User-Agent: cliproxy-usage-ledger/<version> (+<repository>)`, no
retries, and no redirects (a redirect counts as a failed fetch). A response is
accepted only if it is HTTP 200, at most 20 MB, a JSON object, and has at
least 100 priced models.

The served table falls back in this order:

1. **remote**: the last successful fetch in this process.
2. **cache**: the last good copy on disk, `usage-ledger-prices.json` next to
   the database. Every accepted fetch rewrites it atomically (a temp file in
   the same directory, fsync, rename). It wraps the document as
   `{"schema":1,"fetched_at":"…","source_url":"…","data":{…}}` and is
   validated again when it is read.
3. **embedded**: `prices-snapshot.json`, compiled in with `go:embed`.
4. **none**: an empty table, only if the embedded snapshot is unusable.

A failed fetch keeps whatever is being served and records `last_error`. An
empty `price-source-url` disables fetching; the cache is used if there is one,
otherwise the embedded snapshot.

**The embedded snapshot** is LiteLLM's file at commit
`c905cd065dfcc64440420e26fa9a085bdc991ba1` (2026-10-27), trimmed to the
`chat`/`completion`/`responses` models of Anthropic, OpenAI, ChatGPT, Gemini,
Vertex AI (Gemini and Anthropic), xAI, Moonshot/Kimi, and DeepSeek, and to the
fields above: 322 models, about 92 KB. To refresh it:

```sh
mkdir -p /tmp/litellm && curl -sSfo /tmp/litellm/prices.json \
  https://raw.githubusercontent.com/BerriAI/litellm/<commit>/model_prices_and_context_window.json
go run tools/trimprices/main.go /tmp/litellm/prices.json <commit> <YYYY-MM-DD> > prices-snapshot.json
```

`tools/trimprices` has a `//go:build ignore` tag, so it is not part of the
plugin build.

**Plan prices** are in `plan-prices.json`, also embedded. No provider API
reports what a plan costs, so these are public US list prices for monthly
billing, each with an `as_of` date and source link. `verified: false` marks
prices that could not be read on the provider's own pricing page (it blocked
automated fetches or rendered prices client-side) and came from secondary
sources. Override them per credential in settings.

## Build

Requires Go 1.26+ and a C toolchain, because cgo is required.

| Platform | Command |
| --- | --- |
| macOS (arm64 / amd64) | `go build -buildmode=c-shared -o dist/usage-ledger.dylib .` |
| Linux | `go build -buildmode=c-shared -o dist/usage-ledger.so .` |
| Windows (MinGW-w64 gcc on `PATH`) | `go build -buildmode=c-shared -o dist/usage-ledger.dll .` |

The build also writes a `usage-ledger.h` header next to the library. CPA does
not use it, so you can delete it. Build on the target platform, or set up a
cross-compiling C toolchain (`CC`, `GOOS`, `GOARCH`, `CGO_ENABLED=1`).

Tests and vet don't need cgo:

```sh
go test -race ./...   # -race itself needs cgo; drop it for CGO_ENABLED=0
go vet ./...
```

**macOS troubleshooting.** If the build fails in `runtime/cgo` with
``KernelKit requires `-fdefine_target_os_macros` ``, a non-Apple or older
`clang` is ahead of Xcode's on your `PATH`. Toolchain managers such as
`swiftly` can cause this. Point cgo at the system compiler:

```sh
CC=/usr/bin/clang go build -buildmode=c-shared -o dist/usage-ledger.dylib .
```

`scripts/build-release.sh <version> [outdir]` builds the same way the release
workflow does: it stamps the version, strips the binary, deletes the header,
and writes `usage-ledger-v<version>.<so|dylib|dll>`.

## Releases

Pushing a tag `vX.Y.Z` runs `.github/workflows/release.yml`. It runs the CI
checks (`gofmt -l`, `go vet`, `go test -race`, and a c-shared build, the same
as `ci.yml` on pushes to `main` and on pull requests), builds the library on
a native runner per platform, and publishes a GitHub Release. A tag with a
suffix, such as `v0.2.0-rc.1`, is published as a prerelease.

```sh
git tag v0.2.0 && git push origin v0.2.0
```

The tag minus its `v` becomes the plugin version. It is set with
`-ldflags "-X main.pluginVersion=<version>"` and reported in registration
metadata, the status route, and the price fetcher's User-Agent. Local builds
report the default in `plugin.go`.

Release assets:

| Asset | Contents |
| --- | --- |
| `usage-ledger_<version>_linux_amd64.zip` | `usage-ledger-v<version>.so` |
| `usage-ledger_<version>_linux_arm64.zip` | `usage-ledger-v<version>.so` |
| `usage-ledger_<version>_darwin_arm64.zip` | `usage-ledger-v<version>.dylib` |
| `usage-ledger_<version>_darwin_amd64.zip` | `usage-ledger-v<version>.dylib` |
| `usage-ledger_<version>_windows_amd64.zip` | `usage-ledger-v<version>.dll` |
| `checksums.txt` | SHA-256 of each zip, in `sha256sum` format |

These names follow CPA's plugin store, which downloads
`<id>_<version>_<goos>_<goarch>.zip`, verifies it against `checksums.txt`, and
installs the library from the zip root. The library inside is already named
`<id>-v<version>`, so CPA reads the plugin ID as `usage-ledger`. Do not rename
it to the zip's name: the ID would no longer match the
`plugins.configs.usage-ledger` entry. Linux builds link against glibc 2.17
(manylinux2014), the same floor as CPA's own Linux binaries. macOS builds
target macOS 12, the oldest version Go 1.26 supports.

To install by hand, check the zip and unpack it into the platform directory:

```sh
sha256sum --check --ignore-missing checksums.txt
unzip usage-ledger_0.2.0_linux_amd64.zip -d plugins/linux/amd64/
```

## Install into CLIProxyAPI

1. Copy the library into CPA's plugins directory. That directory is
   `plugins.dir`, which defaults to `plugins`. A relative path resolves against
   CPA's working directory. CPA looks in `<dir>/<goos>/<goarch>/` first, then in
   `<dir>/`. The plugin ID is the file name without its extension. You can add
   a version suffix such as `-v0.1.0`:

   ```text
   plugins/usage-ledger.dylib                   # or
   plugins/darwin/arm64/usage-ledger-v0.1.0.dylib
   ```

   Use `.so` on Linux and `.dll` on Windows. Release zips already contain the
   versioned file (see [Releases](#releases)).

2. Enable plugins and this plugin in CPA's `config.yaml`. A plugin without a
   `plugins.configs.<id>` entry is disabled.

   ```yaml
   plugins:
     enabled: true
     dir: "plugins"
     configs:
       usage-ledger:
         enabled: true
         priority: 0
         # database-path: "/var/lib/cliproxy/usage-ledger.db"
         # minute-retention-days: 14
         # hour-retention-days: 400
         # price-source-url: "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"
         # price-refresh-hours: 24
   ```

3. Restart CPA, or let it reload plugins. Then check the route:

   ```sh
   curl -H "Authorization: Bearer $MANAGEMENT_KEY" \
     http://127.0.0.1:8317/v0/management/usage-ledger/status
   ```

## Configuration

CPA sends the plugin its `plugins.configs.usage-ledger` mapping as YAML in
`config_yaml` on `plugin.register` and again on `plugin.reconfigure`. CPA adds
`enabled` and `priority` to that mapping, and the plugin ignores keys it does
not know. If a value is invalid, registration fails with an error that CPA
logs.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `database-path` | string | `""` | SQLite file for the ledger. Empty means `usage-ledger.db` in CPA's working directory. Relative paths resolve against that directory, and `~` expands to the home directory. |
| `minute-retention-days` | integer | `14` | Days to keep per-minute buckets. Must be greater than 0. |
| `hour-retention-days` | integer | `400` | Days to keep per-hour buckets. Must be greater than 0. |
| `price-source-url` | string | LiteLLM `model_prices_and_context_window.json` on raw.githubusercontent.com | Source of model prices, in LiteLLM format. Must be an absolute http(s) URL. An empty value (`""`) disables fetching; omit the key to use the default. |
| `price-refresh-hours` | integer | `24` | Hours between price refreshes. Must be greater than 0. |

The default for `database-path` uses the working directory because CPA does
not tell a plugin where its plugins directory is. The only lifecycle payload is
`config_yaml` plus `schema_version`. CPA also loads each library from a
content-addressed shadow copy in the OS temp directory, so the library's own
path doesn't point to the plugins directory either. For a predictable location,
set an absolute `database-path`.

## Development notes

- `main.go` contains only the cgo C-ABI glue. `plugin.go` holds the RPC
  dispatch, the JSON shapes, and the status document. `ingest.go` holds the
  queue and the worker (batching, lifecycle, retention scheduling). `store.go`
  holds the SQLite schema, migrations, rollups, retention, and key masking.
  `query.go` holds the read-only pool and the aggregate queries. `calendar.go`
  parses ranges and computes local-calendar buckets. `routes.go` holds the
  route table and the summary, series, windows, blocks, and export handlers,
  `blocks.go` the block detection, `prices.go` the price fetcher and the prices
  route, and `settings.go` the settings route. `tokens.go` is the copied token
  normalization. `config.go` parses and validates config. All of it is plain
  Go except `main.go`, so it can be tested without cgo.
- `usage.handle` must never block. CPA delivers usage to plugins one at a time
  from a single dispatcher goroutine, so a slow plugin delays every other usage
  plugin.
- Only the worker goroutine opens, swaps, or closes the database (both the
  write connection and the read-only pool). Register, reconfigure, and quiesce
  send it a request and wait for the answer, so a store change never races
  with a write or a query.
- Tests never use the network: `newTestPlugin` installs an HTTP transport
  that only reaches loopback, and the price tests use `httptest` servers.
- CPA never unloads Go plugin images, so a later `cliproxy_plugin_init` in the
  same image starts a fresh instance after shutdown.
- The plugin declares RPC schema version 6, so management JSON is returned
  without HTML escaping. If CPA reports a lower version, the plugin uses that.
