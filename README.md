# cliproxy-usage-ledger

`usage-ledger` is a native backend plugin for
[CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) (CPA). It receives
the usage record CPA emits after every model request and will keep a local,
retention-bounded ledger of token usage and estimated cost that the
management UI can query.

> **Status: scaffold (step 1).** The plugin loads, registers its
> capabilities and config fields, accepts usage records through a bounded
> non-blocking queue, and serves an ingest status route. Records are only
> counted. Persistent storage (SQLite, minute/hour buckets, retention),
> query routes, and price-table fetching come next.

## How it plugs into CPA

CPA loads plugins as C-ABI shared libraries (ABI v1) built with
`go build -buildmode=c-shared`. It calls `cliproxy_plugin_init`, then talks to
the plugin through JSON envelopes (`{"ok":true,"result":...}`). This plugin
declares two capabilities:

| Capability | RPC methods | What it does now |
| --- | --- | --- |
| `usage_plugin` | `usage.handle` | Pushes the record into a 4096-slot channel without blocking. When the channel is full, the record is dropped and `dropped_total` goes up. A worker goroutine drains the channel and counts records. |
| `management_api` | `management.register`, `management.handle` | Serves `GET /v0/management/usage-ledger/status`. |

The JSON shapes are copied into `plugin.go` instead of importing the CPA SDK,
the same way the single-capability `usage` and `management-api` examples in
CPA do. That keeps the plugin dependency-free apart from `gopkg.in/yaml.v3`.

### Status route

`GET /v0/management/usage-ledger/status` uses CPA's normal Management API
authentication (management key as a bearer token). It returns:

```json
{
  "schema": 1,
  "plugin_version": "0.1.0",
  "records_total": 0,
  "dropped_total": 0,
  "recording_since": null,
  "last_record_at": null,
  "retention": { "minute_days": 14, "hour_days": 400 }
}
```

`last_record_at` is an RFC 3339 UTC timestamp, or `null` before the first
record. `recording_since` stays `null` until storage exists.

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
go test ./...
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
  dispatch, the JSON shapes, and the ingest queue. `config.go` parses and
  validates config. All of it is plain Go except `main.go`, so it can be tested
  without cgo.
- `usage.handle` must never block. CPA delivers usage to plugins one at a time
  from a single dispatcher goroutine, so a slow plugin delays every other usage
  plugin.
- On the C-ABI `shutdown` call, the plugin stops taking records, drains
  what is already queued, and stops the worker. CPA never unloads Go plugin
  images, so a later `cliproxy_plugin_init` in the same image starts a fresh
  instance.
- `plugin.quiesce` returns `unknown_method`, which CPA treats as "unsupported".
- The plugin declares RPC schema version 6, so management JSON is returned
  without HTML escaping. If CPA reports a lower version, the plugin uses that.
