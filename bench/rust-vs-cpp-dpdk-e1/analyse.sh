#!/usr/bin/env bash
#
# The E1 table: Rust media (arm R, rounds 1 and 4) against C++ media (arm C,
# rounds 2 and 3), per tier and statistic.
#
# The floor is each arm against **itself** — its two rounds of the same cell —
# and a ratio is only read when it is outside both floors. That is the D6 rule
# (`deepmsg-dpdk-d6-plan.md` §2) and the reason the rounds are R C C R.
#
#     ./analyse.sh [results-dir]
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=${E1_RUST_DIR:-/home/gavin/data/trader/deepmsg-wt-e1}
out=${1:-$(ls -dt /home/gavin/data/trader/analysis/dpdk/e1-out/full-* 2>/dev/null | head -1)}
if [ -z "$out" ] || [ ! -d "$out" ]; then
    echo "no results directory (give one, or run the batch first)" >&2
    exit 2
fi

args=()
for tag in 1-R 4-R 2-C 3-C; do
    arm=${tag##*-}
    dir="$out/$tag-$arm"
    for cell in "$dir"/*_length=*; do
        [ -d "$cell" ] || continue
        rate=$(basename "$cell")
        rate=${rate%%_length=*}
        file=$(ls "$cell/run-1/"*.hdr 2>/dev/null | head -1 || true)
        [ -n "$file" ] && args+=("$tag:$rate=$file")
    done
done

if [ "${#args[@]}" -eq 0 ]; then
    echo "no histograms under $out" >&2
    exit 1
fi

"$ROOT/bench/tools/quantiles.sh" "${args[@]}" >"$out/quantiles.tsv"
echo "wrote $out/quantiles.tsv"
echo

python3 - "$out/quantiles.tsv" <<'PY'
import sys

rows = {}
with open(sys.argv[1]) as handle:
    header = handle.readline().split()
    for line in handle:
        parts = line.rstrip("\n").split()
        if len(parts) != len(header):
            continue
        label, values = parts[0], dict(zip(header[1:], parts[1:]))
        tag, rate = label.split(":")
        rows[(tag, rate)] = values

rates = sorted({rate for (_, rate) in rows}, key=lambda r: (len(r), r))
stats = [("p50", "p50"), ("p99", "p99"), ("e_lt_p99", "E[X|X<p99]")]
rounds = {"R": ["1-R", "4-R"], "C": ["2-C", "3-C"]}


def us(value):
    return float(value) / 1000.0


def floor_pct(first, second):
    a, b = float(first), float(second)
    low = min(a, b)
    return 0.0 if low == 0 else abs(a - b) / low * 100.0


print(f"{'tier':<12} {'stat':<12} {'R (µs)':>16} {'C (µs)':>16} {'R/C':>7} {'floor':>7}  verdict")
for rate in rates:
    for key, name in stats:
        values = {}
        for arm, tags in rounds.items():
            got = [rows.get((tag, rate), {}).get(key) for tag in tags]
            if any(value is None for value in got):
                values[arm] = None
            else:
                values[arm] = got
        if values["R"] is None or values["C"] is None:
            print(f"{rate:<12} {name:<12} {'(a round is missing)':>40}")
            continue
        r = sum(float(v) for v in values["R"]) / 2.0
        c = sum(float(v) for v in values["C"]) / 2.0
        ratio = r / c if c else float("inf")
        floor = max(floor_pct(*values["R"]), floor_pct(*values["C"]))
        delta = abs(ratio - 1.0) * 100.0
        if delta > floor:
            verdict = "R better" if ratio < 1.0 else "C better"
        else:
            verdict = "not measured"
        print(f"{rate:<12} {name:<12} {us(values['R'][0]):7.2f}/{us(values['R'][1]):7.2f} "
              f"{us(values['C'][0]):7.2f}/{us(values['C'][1]):7.2f} {ratio:7.3f} {floor:6.2f}%  {verdict}")
PY
