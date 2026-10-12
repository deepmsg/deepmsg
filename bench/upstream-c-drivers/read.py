#!/usr/bin/env python3
"""Read a `run-batch.sh` result tree and print the comparison.

Usage:  read.py <results-dir> [--runs N]

The tree is what the harness writes: one directory per (arm, pair, run) named
`<test>_<arm>_<context>_mtu=<mtu>_length=<L>_rate=<R>/run-<N>/<prefix>.hdr`,
copied out of the harness's own `scripts/results` by the batch.

What is printed, and why in this shape:

- **Two rounds, and the round is a column.** The batch runs the arms forward and
  then reversed; a reading whose sign survives the reversal is the driver, and
  one that does not was the session. Ratios are printed per round and then
  judged together.
- **`E[X | X < p99]`, not the mean.** The mean of one of these cells is a place a
  single stalled iteration can drag anywhere: the reference's node stops for
  ~8 ms in every cell, which is a whole microsecond of mean and nothing of a
  p50. The percentile columns are the reading; `p99` is printed for shape only.
- **The control is the point.** `c-patched` against `c-control` is a paired
  reading of the commits — same source, same build script, same flags — so it is
  the ratio a claim should rest on. `c-patched` against `java` says whether the
  result is interesting to anyone else.

Nothing here decides anything on its own: a ratio below 1.00 means the arm on
the left is faster, and the two rounds have to agree in sign before it is read
at all.
"""

import argparse
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUANTILES = HERE.parent / "tools" / "quantiles.sh"

WANTED = ("p40", "p50", "p90", "p99", "e_lt_p99")

CELL = __import__("re").compile(r"_length=(?P<length>\d+)_rate=(?P<rate>[0-9A-Za-z]+)/run-(?P<run>\d+)/")


def parse_cell(path: Path):
    """(round, arm, rate, length, run) out of a harness result path, or None."""
    parts = path.parts
    round_name = next((p for p in parts if p.startswith("round")), None)
    if round_name is None:
        return None
    m = CELL.search(str(path))
    if m is None:
        return None
    # `<test>_<arm>_<context>_mtu=...`: the arm is the field after the test type.
    for part in parts:
        if part.endswith(f"_mtu=1408_length={m.group('length')}_rate={m.group('rate')}"):
            head = part.split("_mtu=")[0]
            fields = head.split("_")
            if len(fields) < 3:
                return None
            arm = fields[-2]          # <test>_<arm>_<context>
            return round_name, arm, m.group("rate"), int(m.group("length")), int(m.group("run"))
    return None


def read_histograms(paths):
    """One quantiles.sh call for all of them; rows keyed by the label we built."""
    args = []
    keys = {}
    for key, path in paths:
        label = ":".join(str(x) for x in key)
        args.append(f"{label}={path}")
        keys[label] = key
    proc = subprocess.run(
        [str(QUANTILES), *args], capture_output=True, text=True, check=True
    )
    lines = proc.stdout.splitlines()
    if not lines:
        return {}
    # Columns are taken by name from the reader's own header. Counting them by
    # hand is how the first version of this dropped every row on the floor: the
    # reader emits fourteen columns and the guard asked for sixteen.
    index = {name: i for i, name in enumerate(lines[0].split("\t"))}
    rows = {}
    for line in lines[1:]:
        fields = line.split("\t")
        if len(fields) < len(index):
            continue
        key = keys[fields[0]]
        rows[key] = {name: float(fields[index[name]]) for name in WANTED if name in index}
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--stat", default="e_lt_p99", choices=["e_lt_p99", "p50", "p40"])
    args = ap.parse_args()

    root = Path(args.results)
    found = []
    for path in sorted(root.glob("round*/**/*.hdr")):
        parsed = parse_cell(path)
        if parsed:
            found.append((parsed, path))
    if not found:
        print(f"no harness histograms under {root}", file=sys.stderr)
        return 1

    rows = read_histograms(found)

    rounds = sorted({k[0] for k in rows})
    arms = sorted({k[1] for k in rows})
    pairs = sorted({(k[2], k[3]) for k in rows}, key=lambda p: -int(p[1]))

    # (round, arm, pair) -> the per-run values, so a run that went missing shows.
    cell = defaultdict(list)
    for (round_name, arm, rate, length, run), v in rows.items():
        cell[(round_name, arm, (rate, length))].append(v[args.stat])

    print(f"results   {root}")
    print(f"reading   {args.stat} (ns, mean over the runs in each cell)")
    print()

    for round_name in rounds:
        print(f"=== {round_name} ===")
        head = f"{'pair':>12} " + "".join(f"{a:>14}" for a in arms)
        print(head)
        for pair in pairs:
            name = f"{pair[0]}:{pair[1]}"
            cells = []
            for arm in arms:
                vals = cell.get((round_name, arm, pair), [])
                cells.append(f"{sum(vals) / len(vals):>14.0f}" if vals else f"{'-':>14}")
            print(f"{name:>12} " + "".join(cells))
        print()

    # --- the ratios, per round, and whether the two rounds agree --------------

    def ratio(round_name, left, right, pair):
        l = cell.get((round_name, left, pair), [])
        r = cell.get((round_name, right, pair), [])
        if not l or not r:
            return None
        return (sum(l) / len(l)) / (sum(r) / len(r))

    comparisons = []
    if "c-patched" in arms and "c-control" in arms:
        comparisons.append(("c-patched/c-control", "c-patched", "c-control"))
    if "c-patched" in arms and "java" in arms:
        comparisons.append(("c-patched/java", "c-patched", "java"))
    if "c-control" in arms and "java" in arms:
        comparisons.append(("c-control/java", "c-control", "java"))

    if comparisons:
        print(f"=== ratios, {args.stat} ===")
        print(f"{'pair':>12} " + "".join(f"{n:>30}" for n, _, _ in comparisons))
        verdicts = defaultdict(dict)
        for pair in pairs:
            name = f"{pair[0]}:{pair[1]}"
            line = f"{name:>12} "
            for label, left, right in comparisons:
                vals = [ratio(r, left, right, pair) for r in rounds]
                shown = " / ".join(f"{v:.4f}" if v else "-" for v in vals)
                agree = len(vals) == 2 and all(v is not None for v in vals) and (
                    (vals[0] < 1.0) == (vals[1] < 1.0)
                )
                verdicts[label][pair] = agree
                line += f"{shown + ('  same sign' if agree else '  SIGN FLIPS'):>30}"
            print(line)
        print()

        # --- the floor: each arm against itself across the two rounds ---------
        print("=== floor: each arm against itself, forward vs reverse ===")
        print(f"{'pair':>12} " + "".join(f"{a:>14}" for a in arms))
        for pair in pairs:
            name = f"{pair[0]}:{pair[1]}"
            line = f"{name:>12} "
            for arm in arms:
                r = ratio(rounds[1], arm, arm, pair) if len(rounds) > 1 else None
                # the same arm twice, so this is the drift between the rounds
                fwd = cell.get((rounds[0], arm, pair), [])
                rev = cell.get((rounds[1], arm, pair), [])
                if fwd and rev:
                    v = (sum(rev) / len(rev)) / (sum(fwd) / len(fwd))
                    line += f"{v:>14.4f}"
                else:
                    line += f"{'-':>14}"
            print(line)
        print()
        print("A ratio is read only when both rounds agree in sign; anything inside")
        print("the floor above is not a difference. The `c-patched/c-control` column")
        print("is the paired reading of the commits.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
