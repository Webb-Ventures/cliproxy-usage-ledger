#!/usr/bin/env bash
# Builds the plugin as a C shared library for the current GOOS/GOARCH and
# writes <outdir>/usage-ledger-v<version>.<so|dylib|dll>, the versioned file
# name CPA's plugin loader and plugin store both accept.
#
#   scripts/build-release.sh <version> [outdir]
#
# <version> has no leading "v" (CPA rejects it). The release workflow runs this
# on each target platform; it also works locally, e.g.
#   CC=/usr/bin/clang GOTOOLCHAIN=local scripts/build-release.sh 0.1.0 dist/local
set -euo pipefail

version="${1:?usage: build-release.sh <version> [outdir]}"
outdir="${2:-dist}"

# Same rule as CPA's validPluginVersion (internal/pluginhost/platform.go).
if [[ ! "$version" =~ ^[0-9][0-9A-Za-z.+-]*$ ]]; then
  echo "invalid plugin version: $version" >&2
  exit 1
fi

goos="$(go env GOOS)"
case "$goos" in
  darwin) ext=dylib ;;
  windows) ext=dll ;;
  *) ext=so ;;
esac

if [[ "$goos" == darwin ]]; then
  # Go 1.26 supports macOS 12+. Without this, clang stamps the runner's SDK
  # version as the minimum OS of the cgo objects.
  export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-12.0}"
fi

mkdir -p "$outdir"
lib="$outdir/usage-ledger-v${version}.${ext}"

CGO_ENABLED=1 go build \
  -buildmode=c-shared \
  -trimpath \
  -buildvcs=false \
  -ldflags="-s -w -X main.pluginVersion=${version}" \
  -o "$lib" .

# CPA does not use the generated C header.
rm -f "${lib%."$ext"}.h"

# The host resolves this symbol with dlsym/GetProcAddress.
case "$goos" in
  linux) nm -D --defined-only "$lib" | grep -qw cliproxy_plugin_init ;;
  darwin) nm -gU "$lib" | grep -qw _cliproxy_plugin_init ;;
  windows) objdump -p "$lib" | grep -qw cliproxy_plugin_init ;;
esac || {
  echo "$lib does not export cliproxy_plugin_init" >&2
  exit 1
}

echo "$lib"
