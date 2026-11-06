#!/usr/bin/env bash
# Fetch and verify Grafana and the VictoriaLogs datasource plugin.
#
# Both are big (Grafana is about 482 MB) and the link this runs on is slow, so
# an asset that is present and matches its pinned SHA256 is never fetched
# again. A mismatch is a hard failure, never a warning.
set -euo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO=$(cd "$HERE/../../../.." && pwd)
# shellcheck source=versions.env
. "$HERE/versions.env"

CACHE=${DEEPMSG_OBS_TOOLS:-$REPO/target/observability-tools}
DOWNLOADS=$CACHE/downloads
UNPACKED=$CACHE/unpacked
GRAFANA_HOME=$CACHE/grafana
PLUGINS=$CACHE/grafana-plugins
PLUGIN_NAME=victoriametrics-logs-datasource
mkdir -p "$DOWNLOADS" "$UNPACKED" "$PLUGINS"

fail() { echo "fetch-grafana: $*" >&2; exit 1; }

for tool in curl tar unzip sha256sum; do
  command -v "$tool" >/dev/null || fail "missing $tool"
done

fetch() { # asset url sha256
  local asset=$1 url=$2 sha256=$3
  local file=$DOWNLOADS/$asset

  [[ $sha256 != PENDING ]] || fail "$asset has no pinned SHA256 in versions.env"

  if [[ -f $file ]] && printf '%s  %s\n' "$sha256" "$file" | sha256sum --check --status; then
    echo "fetch-grafana: $asset is cached and verified"
    return
  fi

  echo "fetch-grafana: fetching $asset"
  curl --fail --location --retry 5 --retry-delay 2 --retry-all-errors \
    --continue-at - --output "$file.part" "$url" ||
    { rm -f "$file.part"; fail "curl failed for $url"; }
  if ! printf '%s  %s\n' "$sha256" "$file.part" | sha256sum --check --status; then
    rm -f "$file.part"
    fail "$asset does not match the pinned SHA256"
  fi
  mv "$file.part" "$file"
}

BASE_GRAFANA=https://dl.grafana.com/oss/release
BASE_PLUGIN=https://github.com/VictoriaMetrics/victorialogs-datasource/releases/download/v$PLUGIN_VERSION

fetch "$GRAFANA_ASSET" "$BASE_GRAFANA/$GRAFANA_ASSET" "$GRAFANA_SHA256"
fetch "$PLUGIN_ASSET" "$BASE_PLUGIN/$PLUGIN_ASSET" "$PLUGIN_SHA256"

# Grafana's tarball extracts to a directory whose name carries a `v`; find the
# home by its binary rather than assuming the spelling.
if [[ ! -e $GRAFANA_HOME ]]; then
  tar --extract --gzip --file "$DOWNLOADS/$GRAFANA_ASSET" --directory "$UNPACKED"
  found=$(find "$UNPACKED" -maxdepth 3 -type f -path '*/bin/grafana' | head -n 1)
  [[ -n $found ]] || fail "no bin/grafana in the unpacked release"
  ln --force --symbolic "$(dirname "$(dirname "$found")")" "$GRAFANA_HOME"
fi

# The plugin zip's top directory is the plugin itself.
unzip -q -o "$DOWNLOADS/$PLUGIN_ASSET" -d "$PLUGINS"
[[ -f $PLUGINS/$PLUGIN_NAME/plugin.json ]] || fail "no $PLUGIN_NAME/plugin.json in the zip"

printf 'fetch-grafana: grafana: '
"$GRAFANA_HOME/bin/grafana" --version 2>&1 | head -n 1
printf 'fetch-grafana: plugin: '
python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["info"]["version"])' \
  "$PLUGINS/$PLUGIN_NAME/plugin.json"
