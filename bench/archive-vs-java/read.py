#!/usr/bin/env python3
"""Read an `archive-vs-java/run-batch.sh` result tree and print the comparison.

Usage:  read.py <results-dir> [--stat e_lt_p99|p50|p40]

Same shape as its two siblings (`rust-vs-java/read.py`, `upstream-c-drivers/read.py`):
two rounds, the round is a column, and a ratio is read only when both rounds agree
in sign. What is different here is what the arms *are*, and the file below is
where that difference is written down.

# The arms, and the one rule about them

    j           the reference archive, on the reference driver, with the
                reference's own clients
    r-attach    our archive, on the reference driver
    r-java      our archive, on our driver
    r-rust      our archive, on our driver, with our Rust rig as the far end

★ **Only `r-attach / j` may be read as "the archive".** The three of them differ
by one thing each — `r-attach` and `r-java` differ by the driver, `r-java` and
`r-rust` by the instrument — and a ratio between arms that differ by more than
one is a number about four things rather than a measurement of one. The table
prints all three ratios because all three are wanted; it prints them with the
question each one answers beside it, and `r-rust / j` says plainly that it
attributes nothing.

# The tree, which is the contract with `run-batch.sh`

    <results>/round-<name>/
        <scenario>_<arm>_fsync=<0|2>_mtu=<M>_length=<L>_rate=<R>/
            run-<N>/
                <anything>.hdr      the client's latency histogram
                counters.txt        the archive's counters at the end of the cell
                archive-du.txt      bytes in the archive directory
                iostat.txt          optional; the device's own line

`counters.txt` is `<label>\t<value>` per line, and **the labels are the
reference's own** — `archive-recorder total write bytes` and its five siblings,
`archive-max-cycle-time`, `Archive Control Sessions` — which is what makes them
comparable across arms at all: both archives allocate the same type ids under the
same names (`AeronCounters`), so a column read from the `j` arm and the same
column read from `r-attach` are the same counter. The batch is what normalises
whichever stat tool an arm has into that file.

★ **A cell without `counters.txt` is still a cell**: the latency table is the
reading and the counters are what a difference is explained with. What a cell may
not be is unfinished — `run-batch.sh` gates on the client's own `WARNING:` line
and on the histogram holding what the rate and the iterations asked for, exactly
as `lib/cell.sh` does, and a cell that did not finish is `.FAIL` rather than a
row here.
"""

import argparse
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUANTILES = HERE.parent / "tools" / "quantiles.sh"

WANTED = ("p40", "p50", "p90", "p99", "e_lt_p99")

CELL = __import__("re").compile(
    r"(?P<scenario>[A-Za-z0-9]+)_(?P<arm>j|r-attach|r-java|r-rust)"
    r"_fsync=(?P<fsync>[02])_mtu=\d+_length=(?P<length>\d+)_rate=(?P<rate>[0-9A-Za-z]+)"
    r"/run-(?P<run>\d+)(?:/|$)"
)

# What each ratio is allowed to say. The key is `<left>/<right>`, and the value is
# the question it answers — printed beside the number so that nobody has to hold
# the rule in their head to read the table.
QUESTIONS = {
    "r-attach/j": "the archive, one driver under both",
    "r-java/r-attach": "the driver, one archive under both",
    "r-rust/j": "the whole stack (attributes nothing)",
}

# The counters a difference gets explained with, by the label the archive
# allocates them under. Identical strings on both sides is the whole reason a
# column can be compared at all.
COUNTERS = (
    "archive-recorder total write bytes",
    "archive-recorder total write time in ns",
    "archive-recorder max write time in ns",
    "archive-replayer total read bytes",
    "archive-replayer total read time in ns",
    "archive-replayer max read time in ns",
)

# Printed when the cell has one, because it is what says *which* agent was slow
# rather than that one was.
CYCLE_PREFIXES = ("archive-conductor max cycle time", "archive-recorder max cycle time",
                  "archive-replayer max cycle time")


def parse_cell(path: Path):
    """(round, scenario, arm, fsync, rate, length, run) out of a result path."""
    round_name = next((p for p in path.parts if p.startswith("round-")), None)
    if round_name is None:
        return None

    m = CELL.search(str(path))
    if m is None:
        return None

    return (
        round_name,
        m.group("scenario"),
        m.group("arm"),
        int(m.group("fsync")),
        m.group("rate"),
        int(m.group("length")),
        int(m.group("run")),
    )


def read_histograms(paths):
    """One `quantiles.sh` call for all of them, rows keyed by the key we built."""
    args = []
    keys = {}
    for key, path in paths:
        label = ":".join(str(x) for x in key)
        args.append(f"{label}={path}")
        keys[label] = key

    proc = subprocess.run([str(QUANTILES), *args], capture_output=True, text=True, check=True)

    lines = proc.stdout.splitlines()
    if not lines:
        return {}

    # Columns are taken by name from the reader's own header: counting them by
    # hand is how the first version of the sibling reader dropped every row on
    # the floor (it emits fourteen and the guard asked for sixteen).
    index = {name: i for i, name in enumerate(lines[0].split("\t"))}

    rows = {}
    for line in lines[1:]:
        fields = line.split("\t")
        if len(fields) < len(index):
            continue

        key = keys[fields[0]]
        rows[key] = {name: float(fields[index[name]]) for name in WANTED if name in index}

    return rows


def read_counters(cell_dir: Path):
    """`<cell>/run-<N>/counters.txt`, as a dict; `{}` when it is not there."""
    values = {}
    for path in sorted(cell_dir.glob("run-*/counters.txt")):
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            name, _, value = line.partition("\t")
            try:
                values.setdefault(name.strip(), []).append(float(value))
            except ValueError:
                continue
    return values


def read_one_number(cell_dir: Path, name: str):
    """The single number in `<cell>/run-*/<name>`, meaned over the runs."""
    numbers = []
    for path in sorted(cell_dir.glob(f"run-*/{name}")):
        for line in path.read_text().split():
            try:
                numbers.append(float(line))
            except ValueError:
                continue
    return sum(numbers) / len(numbers) if numbers else None


def cell_directories(root: Path):
    """Every cell directory, keyed by the parsed cell it is."""
    found = {}
    for path in sorted(root.glob("round-*/**/run-*/")):
        parsed = parse_cell(path)
        if parsed is None:
            continue
        found[parsed[:6]] = path.parent
    return found


def mean(values):
    return sum(values) / len(values) if values else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--stat", default="e_lt_p99", choices=["e_lt_p99", "p50", "p40"])
    args = ap.parse_args()

    root = Path(args.results)

    found = []
    for path in sorted(root.glob("round-*/**/*.hdr")):
        parsed = parse_cell(path)
        if parsed:
            found.append((parsed, path))

    if not found:
        print(f"no harness histograms under {root}", file=sys.stderr)
        return 1

    rows = read_histograms(found)

    rounds = sorted({k[0] for k in rows})
    arms = [a for a in ("j", "r-attach", "r-java", "r-rust") if any(k[2] == a for k in rows)]
    cells = sorted({(k[1], k[3], k[4], k[5]) for k in rows}, key=lambda c: (c[0], c[1], -c[3]))

    # The full key -> the per-run values, so that a run which went missing shows
    # as one fewer run rather than as a smaller mean.
    def key(round_name, scenario, arm, fsync, rate, length):
        return (round_name, scenario, arm, fsync, rate, length)

    cell = defaultdict(list)
    for (round_name, scenario, arm, fsync, rate, length, _run), value in rows.items():
        cell[key(round_name, scenario, arm, fsync, rate, length)].append(value[args.stat])

    print(f"results   {root}")
    print(f"reading   {args.stat} (ns, mean over the runs in each cell)")
    print()

    for round_name in rounds:
        print(f"=== {round_name} ===")
        for scenario, fsync, rate, length in cells:
            print(f"  {scenario}  fsync={fsync}  length={length}  rate={rate}")
            print(f"    {'arm':>10} {'runs':>5} {'ns':>12}")
            for arm in arms:
                values = cell.get(key(round_name, scenario, arm, fsync, rate, length), [])
                shown = f"{mean(values):>12.0f}" if values else f"{'-':>12}"
                print(f"    {arm:>10} {len(values):>5} {shown}")
            print()

    # --- the ratios, per round, and whether the two rounds agree --------------

    def ratio(round_name, left, right, scenario, fsync, rate, length):
        l = cell.get(key(round_name, scenario, left, fsync, rate, length), [])
        r = cell.get(key(round_name, scenario, right, fsync, rate, length), [])
        if not l or not r:
            return None
        return mean(l) / mean(r)

    print(f"=== ratios, {args.stat} ===")
    print(f"  {'cell':>28} " + "".join(f"{name:>34}" for name in QUESTIONS))
    for scenario, fsync, rate, length in cells:
        line = f"  {scenario + ' fsync=' + str(fsync):>28} "
        for name in QUESTIONS:
            left, _, right = name.partition("/")
            values = [ratio(r, left, right, scenario, fsync, rate, length) for r in rounds]
            shown = " / ".join(f"{v:.4f}" if v else "-" for v in values)
            if len(values) == 2 and all(v is not None for v in values):
                verdict = "  same sign" if (values[0] < 1.0) == (values[1] < 1.0) else "  SIGN FLIPS"
            else:
                # One side missing is not a disagreement: there is nothing to
                # disagree with. Printing SIGN FLIPS here was the first version's
                # bug, and it would have made an incomplete batch look like a
                # rounding.
                verdict = ""
            line += f"{shown + verdict:>34}"
        print(line)
    print()

    # --- the floor: each arm against itself across the two rounds ------------
    print("=== floor: each arm against itself, forward vs reverse ===")
    print(f"  {'cell':>28} " + "".join(f"{a:>12}" for a in arms))
    for scenario, fsync, rate, length in cells:
        line = f"  {scenario + ' fsync=' + str(fsync):>28} "
        for arm in arms:
            forward = cell.get(key(rounds[0], scenario, arm, fsync, rate, length), [])
            reverse = (
                cell.get(key(rounds[1], scenario, arm, fsync, rate, length), [])
                if len(rounds) > 1
                else []
            )
            if forward and reverse:
                line += f"{mean(reverse) / mean(forward):>12.4f}"
            else:
                line += f"{'-':>12}"
        print(line)
    print()

    # --- the archive's own time, and the disk --------------------------------
    #
    # Cell by cell, because these are what a difference in the table above is
    # *explained* with: how much the archive wrote and how long it took, how much
    # it read back, which agent took the longest cycle, and how many bytes
    # reached the disk. The counter labels are the reference's own on both sides
    # (see the module note), which is what makes the two columns comparable.

    directories = cell_directories(root)
    groups = sorted({k[:4] for k in directories})

    if groups:
        print("=== the archive's own counters, and the disk (mean over the runs) ===")
        head = f"  {'round':>12} {'cell':>16} {'arm':>10}"
        for name in COUNTERS:
            head += f"{name.replace('archive-recorder ', 'w ').replace('archive-replayer ', 'r '):>26}"
        print(head + f"{'du bytes':>12}")

        for round_name, scenario, arm, fsync in groups:
            merged = defaultdict(list)
            for (r, s, a, f, _rate, _length), directory in directories.items():
                if (r, s, a, f) != (round_name, scenario, arm, fsync):
                    continue
                for name, values in read_counters(directory).items():
                    merged[name].extend(values)

                du = read_one_number(directory, "archive-du.txt")
                if du is not None:
                    merged["archive directory bytes"].append(du)

            line = f"  {round_name:>12} {scenario + ':' + str(fsync):>16} {arm:>10}"
            for name in COUNTERS:
                value = mean(merged.get(name, []))
                line += f"{value:>26.0f}" if value is not None else f"{'-':>26}"

            du = mean(merged.get("archive directory bytes", []))
            line += f"{du:>12.0f}" if du is not None else f"{'-':>12}"
            print(line)

        print()
        print("=== which agent took the longest cycle (ns, mean over the runs) ===")
        print(f"  {'round':>12} {'cell':>16} {'arm':>10} {'conductor':>14} {'recorder':>14} {'replayer':>14}")
        for round_name, scenario, arm, fsync in groups:
            by_actor = defaultdict(list)
            for (r, s, a, f, _rate, _length), directory in directories.items():
                if (r, s, a, f) != (round_name, scenario, arm, fsync):
                    continue
                for name, values in read_counters(directory).items():
                    for actor in ("conductor", "recorder", "replayer"):
                        if name.startswith(f"archive-{actor} max cycle time"):
                            by_actor[actor].extend(values)

            line = f"  {round_name:>12} {scenario + ':' + str(fsync):>16} {arm:>10}"
            for actor in ("conductor", "recorder", "replayer"):
                value = mean(by_actor.get(actor, []))
                line += f"{value:>14.0f}" if value is not None else f"{'-':>14}"
            print(line)
        print()

    print("A ratio is read only when both rounds agree in sign, and only from the")
    print("column whose question it is: `r-attach/j` is the archive, `r-java/r-attach`")
    print("is the driver, and `r-rust/j` differs by all three and attributes nothing.")
    print("Anything inside the floor is not a difference.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
