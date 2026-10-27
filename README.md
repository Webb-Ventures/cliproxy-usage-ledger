# cliproxy-usage-ledger

`usage-ledger` is a native backend plugin for
[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) (CPA). It receives
the usage record CPA emits after every model request and keeps a local,
retention-bounded ledger of token usage (and, later, estimated cost) that the
management UI can query.

> **Status: ingest and storage (step 2).** Every usage record is rolled up
> into per-minute and per-hour buckets in a local SQLite database, with
> retention. The only route so far is the status route. Query routes and
> price-table fetching come next.

## How it plugs into CPA

CPA loads plugins as C-ABI shared libraries (ABI v1) built with
`go build -buildmode=c-shared`. It calls `cliproxy_plugin_init`, then talks to
the plugin through JSON envelopes (`{"ok":true,"result":...}`). This plugin
declares two capabilities:

| Capability | RPC methods | What it does now |
| --- | --- | --- |
| `usage_plugin` | `usage.handle` | Pushes the record into a 4096-slot channel without blocking. When the channel is full, the record is dropped and `dropped_total` goes up. A worker goroutine drains the channel and writes rollups to SQLite in batches. |
| `management_api` | `management.register`, `management.handle` | Serves `GET /v0/management/usage-ledger/status`. |

The JSON shapes are copied into `plugin.go` instead of importing the CPA SDK,
the same way the single-capability `usage` and `management-api` examples in
CPA do. CPA's token-accounting normalization (`EnsureTokenBreakdownForProvider`)
is copied into `tokens.go` for the same reason; the file header names the
upstream file and commit. The only dependencies are `gopkg.in/yaml.v3` and the
pure-Go SQLite driver `modernc.org/sqlite`, which needs no cgo of its own.
`modernc.org/sqlite` is pinned to v1.59.0 because v1.60+ requires `go 1.26.0`
in `go.mod`.

## Storage

The ledger is one SQLite file (see `database-path`) opened in WAL mode with a
5 second `busy_timeout`. While the plugin runs you will also see `-wal` and
`-shm` files next to it; they are folded back in and removed when the database
is closed.

| Table | Contents |
| --- | --- |
| `schema_version` | One row per applied migration (`version`, `applied_at`). The current schema is version 1. A database from a newer plugin is refused rather than modified. |
| `meta` | Key/value pairs. `recording_since` is the UTC time the database was created. |
| `usage_minute` | Rollups per UTC minute. |
| `usage_hour` | Rollups per UTC hour. |

Both rollup tables have the same columns. A row is keyed by:

| Column | Meaning |
| --- | --- |
| `bucket_start` | Start of the UTC minute or hour, as Unix seconds. Taken from the record's `RequestedAt` (the plugin's clock if that is missing). |
| `provider` | `Provider`, for example `claude`, `gemini`, `codex`. |
| `model` | `Model`, the upstream model used. |
| `auth_index` | `AuthIndex`, CPA's stable hashed credential index (the `auth_index` the Management API reports for auth files). `AuthID` is not stored because it is often a file name containing an e-mail address. |
| `client_key` | The client API key, masked: the first 8 hex characters of its SHA-256 hash, `…`, and its last 4 characters, e.g. `3f9a0c12…a1b2`. Keys shorter than 12 characters keep only the hash part. Empty when the request had no client key. |
| `failed` | `1` if the request failed, else `0`. |

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
- request, trace, selection, or session IDs, base URLs, `AuthID`, the model
  alias, or the access provider.

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

Before the first register, and after quiesce, the worker does not take
records off the queue, so a full queue drops new records as usual.

### Status route

`GET /v0/management/usage-ledger/status` uses CPA's normal Management API
authentication (management key as a bearer token). It returns:

```json
{
  "schema": 1,
  "plugin_version": "0.1.0",
  "records_total": 1520,
  "dropped_total": 0,
  "write_errors_total": 0,
  "last_write_error": null,
  "recording_since": "2026-10-27T02:18:51Z",
  "last_record_at": "2026-10-27T09:41:07Z",
  "retention": { "minute_days": 14, "hour_days": 400 },
  "database_path": "/var/lib/cliproxy/usage-ledger.db",
  "database_size_bytes": 32968,
  "row_counts": { "meta": 1, "schema_version": 1, "usage_hour": 12, "usage_minute": 85 }
}
```

| Field | Meaning |
| --- | --- |
| `records_total` | Records committed to the database by this plugin instance. |
| `dropped_total` | Records lost because the queue was full, or that were still queued when a quiesced instance shut down. |
| `write_errors_total`, `last_write_error` | Failed commits, retention runs, or closes, and the most recent error message (`null` if none). |
| `recording_since` | When the database was created, from its `meta` table. |
| `last_record_at` | When this instance last committed records (RFC 3339 UTC), or `null`. |
| `database_path`, `database_size_bytes`, `row_counts` | The open database, its size (main file plus WAL), and the row count of each table. |

The database fields (`recording_since`, `database_path`, `database_size_bytes`,
`row_counts`) are `null` while no database is open: before the first register,
or after quiesce or shutdown.

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

   Use `.so` on Linux and `.dll` on Windows.

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
| `price-source-url` | string | LiteLLM `model_prices_and_context_window.json` on raw.githubusercontent.com | Source of model prices, in LiteLLM format. Must be an absolute http(s) URL. An empty value uses the default. |
| `price-refresh-hours` | integer | `24` | Hours between price refreshes. Must be greater than 0. |

The default for `database-path` uses the working directory because CPA does
not tell a plugin where its plugins directory is. The only lifecycle payload is
`config_yaml` plus `schema_version`. CPA also loads each library from a
content-addressed shadow copy in the OS temp directory, so the library's own
path doesn't point to the plugins directory either. For a predictable location,
set an absolute `database-path`.

## Development notes

- `main.go` contains only the cgo C-ABI glue. `plugin.go` holds the RPC
  dispatch, the JSON shapes, and the status route. `ingest.go` holds the
  queue and the worker (batching, lifecycle, retention scheduling). `store.go`
  holds the SQLite schema, rollups, retention, and key masking. `tokens.go` is
  the copied token normalization. `config.go` parses and validates config. All
  of it is plain Go except `main.go`, so it can be tested without cgo.
- `usage.handle` must never block. CPA delivers usage to plugins one at a time
  from a single dispatcher goroutine, so a slow plugin delays every other usage
  plugin.
- Only the worker goroutine opens, swaps, or closes the database. Register,
  reconfigure, and quiesce send it a request and wait for the answer, so a
  store change never races with a write.
- CPA never unloads Go plugin images, so a later `cliproxy_plugin_init` in the
  same image starts a fresh instance after shutdown.
- The plugin declares RPC schema version 6, so management JSON is returned
  without HTML escaping. If CPA reports a lower version, the plugin uses that.
