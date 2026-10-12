#!/usr/bin/env python3
"""Read a `rust-vs-java/run-batch.sh` result tree and print the pair.

Usage:  read.py <results-dir> [--stat {e_lt_p99,p50,p40}] [--runs N]

The tree is one directory per (round, arm, pair, run):

    <out>/<round>/<arm>/<rate>_length=<L>/run-<N>/<prefix>_rate=<R>_batch=1_length=<L>.hdr

Both arms' files are HdrHistogram interval logs and both are read by the
harness's own decoder (`bench/tools/quantiles.sh`), which is what makes the
ratio mean anything.

Read it the way the rest of this work is read:

- **The ratio is `rust/java`**, below 1.00 meaning this build's stack is faster.
- **Two rounds, forward and reverse**, and the sign has to survive both. One
  round on its own was the session as often as it was the stack.
- **`E[X | X < p99]` and p40/p50, never the mean.** The reference's node stops for
  ~8 ms in every cell; that is a whole microsecond of mean and nothing of a p50.
- **The floor is printed with it.** An arm against itself across the two rounds
  is the drift, and a ratio inside it is not a difference.
"""

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUANTILES = HERE.parent / "tools" / "quantiles.sh"

WANTED = ("p40", "p50", "p90", "p99", "e_lt_p99")

CELL = re.compile(r"^(?P<rate>[0-9A-Za-z]+)_length=(?P<length>\d+)$")


def parse_cell(path: Path, root: Path):
    rel = path.relative_to(root).parts
    if len(rel) < 5:
        return None
    round_name, arm = rel[0], rel[1]
    m = CELL.match(rel[2])
    run_m = re.search(r"run-(\d+)", str(path))
    if m is None or run_m is None:
        return None
    return round_name, arm, m.group("rate"), int(m.group("length")), int(run_m.group(1))


def read_histograms(pairs):
    args, keys = [], {}
    for key, path in pairs:
        label = ":".join(str(x) for x in key)
        args.append(f"{label}={path}")
        keys[label] = key
    out = subprocess.run([str(QUANTILES), *args], capture_output=True, text=True, check=True)
    lines = out.stdout.splitlines()
    if not lines:
        return {}
    # Columns come from the reader's own header rather than from a hand-counted
    # index: the reader emits fourteen of them, and a guard written for sixteen
    # silently drops every row (which is what the first version of this did).
    index = {name: i for i, name in enumerate(lines[0].split("\t"))}
    rows = {}
    for line in lines[1:]:
        f = line.split("\t")
        if len(f) < len(index):
            continue
        rows[keys[f[0]]] = {name: float(f[index[name]]) for name in WANTED if name in index}
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--stat", default="e_lt_p99", choices=["e_lt_p99", "p50", "p40"])
    args = ap.parse_args()

    root = Path(args.results)
    found = []
    for path in sorted(root.glob("round*/**/*.hdr")):
        parsed = parse_cell(path, root)
        if parsed:
            found.append((parsed, path))
    if not found:
        print(f"no histograms under {root}", file=sys.stderr)
        return 1
    rows = read_histograms(found)

    rounds = sorted({k[0] for k in rows})
    arms = sorted({k[1] for k in rows})
    pairs = sorted({(k[2], k[3]) for k in rows}, key=lambda p: -int(p[1]))

    cell = defaultdict(list)
    for (rnd, arm, rate, length, _run), v in rows.items():
        cell[(rnd, arm, (rate, length))].append(v[args.stat])

    def mean_of(key):
        vals = cell.get(key, [])
        return sum(vals) / len(vals) if vals else None

    print(f"results   {root}")
    print(f"reading   {args.stat} (ns, mean over the runs in each cell)")
    print()

    for rnd in rounds:
        print(f"=== {rnd} ===")
        print(f"{'pair':>14} " + "".join(f"{a:>14}" for a in arms) + f"{'rust/java':>12}")
        for pair in pairs:
            name = f"{pair[0]}:{pair[1]}"
            line = f"{name:>14} "
            vals = {}
            for arm in arms:
                v = mean_of((rnd, arm, pair))
                vals[arm] = v
                line += f"{v:>14.0f}" if v is not None else f"{'-':>14}"
            if vals.get("rust") and vals.get("java"):
                line += f"{vals['rust'] / vals['java']:>12.4f}"
            else:
                line += f"{'-':>12}"
            print(line)
        print()

    print(f"=== rust/java, both rounds ({args.stat}) ===")
    print(f"{'pair':>14} {'round 1':>10} {'round 2':>10}   verdict")
    for pair in pairs:
        name = f"{pair[0]}:{pair[1]}"
        ratios = []
        for rnd in rounds:
            r, j = mean_of((rnd, "rust", pair)), mean_of((rnd, "java", pair))
            ratios.append(r / j if r and j else None)
        if len(ratios) == 2 and all(ratios):
            same = (ratios[0] < 1.0) == (ratios[1] < 1.0)
            verdict = "same sign" if same else "SIGN FLIPS"
            print(f"{name:>14} {ratios[0]:>10.4f} {ratios[1]:>10.4f}   {verdict}")
        else:
            print(f"{name:>14} " + " ".join(f"{v:>10.4f}" if v else f"{'-':>10}" for v in ratios))
    print()

    print("=== floor: each arm against itself, forward vs reverse ===")
    if len(rounds) > 1:
        print(f"{'pair':>14} " + "".join(f"{a:>14}" for a in arms))
        for pair in pairs:
            name = f"{pair[0]}:{pair[1]}"
            line = f"{name:>14} "
            for arm in arms:
                a, b = mean_of((rounds[0], arm, pair)), mean_of((rounds[1], arm, pair))
                line += f"{b / a:>14.4f}" if a and b else f"{'-':>14}"
            print(line)
    print()
    print("A ratio is read only when both rounds agree in sign, and only when it is")
    print("outside the floor. This batch swaps whole stacks: it cannot say which layer")
    print("a difference came from, and no number here may be read as one layer's.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
