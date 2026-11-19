#!/usr/bin/env bash
#
# What the two drivers did with the CPU, from the cells run with `E1_PERF=1`.
#
# For each cell: the counters `perf stat` took over the measurement window, and
# the symbols `perf record` found. The per-message numbers divide by the rig's
# own count, which is exact; the window also holds the warmup and the idle
# before and after it, and both arms are given the same window.
#
#     ./perf-report.sh <results-dir>
set -euo pipefail

out=${1:?usage: perf-report.sh <results-dir>}

count() { # $1 = number with commas
    echo "$1" | tr -d ','
}

for cell in "$out"/*/*; do
    [ -f "$cell/perf-stat.txt" ] || continue
    [ -f "$cell/run-1/client.log" ] || continue
    n=$(grep -o 'Total count  = *[0-9]*' "$cell/run-1/client.log" | tail -1 | grep -o '[0-9]*$' || true)
    [ -n "${n:-}" ] || n=1
    echo "== $cell   (rig counted $n messages)"
    awk -v n="$n" '
        $1 ~ /^[0-9,]+$/ && NF >= 2 {
            value = $1; gsub(/,/, "", value)
            name = $2
            if (name == "cycles" || name == "instructions" || name == "cache-references" || name == "cache-misses") {
                printf "   %-18s %18.0f   %8.3f per message\n", name, value, value / n
            }
        }
        /seconds time elapsed/ { printf "   %-18s %18s\n", "window", $1 " s" }
    ' "$cell/perf-stat.txt"
    echo "   top symbols (rig-side driver):"
    sudo perf report --stdio -i "$cell/perf.data" --sort symbol --percent-limit 0.8 2>/dev/null \
        | awk '/^#/ {next} /^$/ {next} {printf "     %-6s %s\n", $1, $3}' | head -14 | sed 's/^/   /'
    echo
done
