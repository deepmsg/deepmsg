#!/usr/bin/env bash
# Fetch and verify the release binaries the L2 end-to-end scripts run.
#
# One cache serves every run: an asset that is present and matches its pinned
# SHA256 is not downloaded again. A mismatch is a hard failure, never a warning.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
# shellcheck source=versions.env
. "$HERE/versions.env"

CACHE=${DEEPMSG_OBS_TOOLS:-$REPO/target/observability-tools}
DOWNLOADS=$CACHE/downloads
UNPACKED=$CACHE/unpacked
BIN=$CACHE/bin
mkdir -p "$DOWNLOADS" "$UNPACKED" "$BIN"

fail() { echo "fetch-tools: $*" >&2; exit 1; }

for tool in curl tar sha256sum; do
  command -v "$tool" >/dev/null || fail "missing $tool"
done

fetch() {
  local asset=$1 url=$2 sha256=$3
  local file=$DOWNLOADS/$asset

  [[ $sha256 != PENDING ]] || fail "$asset has no pinned SHA256 in versions.env"

  if [[ -f $file ]] && printf '%s  %s\n' "$sha256" "$file" | sha256sum --check --status; then
    echo "fetch-tools: $asset is cached and verified"
    return
  fi

  echo "fetch-tools: fetching $asset"
  # The release CDN is slow and drops connections; resume a partial transfer
  # instead of starting the 100 MB over.
  curl --fail --location --retry 5 --retry-delay 2 --retry-all-errors \
    --continue-at - --output "$file.part" "$url" ||
    { rm -f "$file.part"; fail "curl failed for $url"; }
  if ! printf '%s  %s\n' "$sha256" "$file.part" | sha256sum --check --status; then
    rm -f "$file.part"
    fail "$asset does not match the pinned SHA256"
  fi
  mv "$file.part" "$file"
}

extract() {
  local asset=$1 directory=$UNPACKED/${asset%.tar.gz}
  [[ -d $directory ]] || {
    mkdir -p "$directory"
    tar --extract --gzip --file "$DOWNLOADS/$asset" --directory "$directory"
  }
}

# The extracted names come from the release assets; the stable names are what
# the scripts use, so a rename in a release is caught here and nowhere else.
link() {
  local stable=$1 extracted=$2
  local found
  found=$(find "$UNPACKED" -maxdepth 2 -type f -name "$extracted" | head -n 1)
  [[ -n $found ]] || fail "no $extracted in the unpacked releases"
  chmod +x "$found"
  ln --force --symbolic "$found" "$BIN/$stable"
}

BASE_OTELCOL=https://github.com/open-telemetry/opentelemetry-collector-releases/releases/download/v$OTELCOL_VERSION
BASE_VL=https://github.com/VictoriaMetrics/VictoriaLogs/releases/download/v$VICTORIA_LOGS_VERSION
BASE_VM=https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/v$VICTORIA_METRICS_VERSION

fetch "$OTELCOL_ASSET" "$BASE_OTELCOL/$OTELCOL_ASSET" "$OTELCOL_SHA256"
fetch "$VICTORIA_LOGS_ASSET" "$BASE_VL/$VICTORIA_LOGS_ASSET" "$VICTORIA_LOGS_SHA256"
fetch "$VICTORIA_METRICS_ASSET" "$BASE_VM/$VICTORIA_METRICS_ASSET" "$VICTORIA_METRICS_SHA256"
fetch "$VMUTILS_ASSET" "$BASE_VM/$VMUTILS_ASSET" "$VMUTILS_SHA256"

for asset in "$OTELCOL_ASSET" "$VICTORIA_LOGS_ASSET" "$VICTORIA_METRICS_ASSET" "$VMUTILS_ASSET"; do
  extract "$asset"
done

link otelcol-contrib "$OTELCOL_BINARY"
link victoria-logs "$VICTORIA_LOGS_BINARY"
link victoria-metrics "$VICTORIA_METRICS_BINARY"
link vmauth "$VMUTILS_BINARY"

for binary in otelcol-contrib victoria-logs victoria-metrics vmauth; do
  printf 'fetch-tools: %s: ' "$binary"
  "$BIN/$binary" --version 2>&1 | head -n 1
done
