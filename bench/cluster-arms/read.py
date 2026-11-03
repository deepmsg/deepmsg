#!/usr/bin/env python3
"""Read a `cluster-arms/run-batch.sh` result tree and print the comparison.

Usage:  read.py <results-dir> [--stat e_lt_p99|p50|p99|p99p9]

Same shape as its three siblings (`rust-vs-java/`, `archive-vs-java/`,
`upstream-c-drivers/`): two rounds, the round is a column, and a ratio is read
only when both rounds agree in sign. What is different here is what the arms
*are*, and this file is where that difference is written down.

# The arms, and the one rule about them

    j     the reference's Java driver + the reference's node (Java archive,
          Java consensus module and Java echo service in one JVM)
    j-d   this build's driver + the same reference node
    r-a   this build's driver + the reference's `Archive` in a JVM of its own,
          this build's consensus module and container
    r     this build's driver + this build's archive, consensus module and
          container, each a process of its own

    r-a / j-d   the consensus module and the container — one archive under both
    j-d / j     the driver — one node under both
    r-a / r     the archive — one consensus module under both
    r / j       the whole stack, minus the client: it attributes nothing

★ **The client is the reference's in every arm**, so no ratio here is a ratio
between instruments. `rust-vs-java` is the batch that measures instruments.

# The tree, which is the contract with `run-batch.sh`

    <results>/round-<name>/
        <arm>_sync=<A>:<C>_length=<L>_rate=<R>/
            run-<N>/
                cluster-<arm>_rate=..._batch=..._length=....hdr
                client.log      the client's own output
                client.log.FAIL present when the cell did not count
                driver.log, node.log | archive.log, module.log, container.log

★ **A cell that did not finish is not a reading.** `run-batch.sh` gates on the
client's own `WARNING:` line, on the histogram being present rather than
`.FAIL`-suffixed, and on the histogram holding at least `rate x iterations`
samples (`lib/cell.sh`'s `cell_validate`, unchanged). A failed cell leaves a
`client.log.FAIL` and is printed here as `FAILED` rather than left out: a cell
nobody ran and a cell that failed must not look the same.

# Reading rules (docs/benchmarks.md, and they are not negotiable)

- `E[X | X < p99]` and p40/p50 decide; **the mean does not**. The reference's
  node stops for ~8 ms in every cell — a whole microsecond of mean and nothing of
  a p50.
- **Two rounds, the second reverse order.** A ratio that keeps its sign is the
  thing being measured; one that flips was the session.
- **The floor is printed with every table.** An arm against itself across the
  two rounds is the drift, and a difference inside it is not a difference.

The quantiles come out of `bench/tools/quantiles.sh` — the harness's own
HdrHistogram decoder — so the reference client's files are read with the same
code that reads this build's.
"""

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
QUANTILES = HERE.parent / "tools" / "quantiles.sh"

ARMS = ("j", "j-d", "r-a", "r")

# `<arm>_sync=<A>:<C>_length=<L>_rate=<R>/run-<N>`
CELL = re.compile(
    r"(?P<arm>j-d|r-a|j|r)_sync=(?P<sync_a>\d+):(?P<sync_c>\d+)"
    r"_length=(?P<length>\d+)_rate=(?P<rate>[0-9A-Za-z]+)"
    r"/run-(?P<run>\d+)(?:/|$)"
)

# What each ratio is allowed to say.
QUESTIONS = {
    "r-a/j-d": "the consensus module and the container, one archive under both",
    "j-d/j": "the driver, one node under both",
    "r-a/r": "the archive, one consensus module under both",
    "r/j": "the whole stack (attributes nothing)",
}

# The statistics this batch reads, in the order the tables print them.
STATS = {
    "e_lt_p99": "E[X | X < p99]",
    "p50": "p50",
    "p99": "p99",
    "p99p9": "p99.9",
}


def quantile_column(stat: str) -> str:
    """The reader's own column name for one of this batch's statistics."""
    return "p99.9" if stat == "p99p9" else stat


def collect(root: Path):
    """Every run directory in the tree, keyed by (round, arm, sync, rate:length)."""
    cells = defaultdict(dict)  # key -> {run: (run_dir, hdr_or_None, failed)}
    for run_dir in sorted(root.glob("round*/*/run-*")):
        rel = run_dir.relative_to(root)
        m = CELL.search(str(rel))
        if not m:
            continue
        round_name = rel.parts[0]
        key = (
            round_name,
            m["arm"],
            f"{m['sync_a']}:{m['sync_c']}",
            f"{m['rate']}:{m['length']}",
        )
        failed = (run_dir / "client.log.FAIL").exists() or bool(
            list(run_dir.glob("*.FAIL"))
        )
        hdrs = [p for p in run_dir.glob("*.hdr") if not p.name.endswith(".FAIL")]
        cells[key][int(m["run"])] = (run_dir, hdrs[0] if len(hdrs) == 1 else None, failed)
    return cells


def read_quantiles(paths):
    """Run the harness's own decoder over every histogram in one call."""
    labels = []
    for label, (path, _) in paths.items():
        labels.append(f"{label}={path}")
    if not labels:
        return {}
    proc = subprocess.run(
        ["bash", str(QUANTILES), "--quantiles", "99.9", *labels],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
        raise SystemExit("the quantile reader failed")
    lines = proc.stdout.splitlines()
    if not lines:
        return {}
    # Columns are taken by name from the reader's own header: counting them by
    # position would silently mis-read the day a column is added.
    index = {name: i for i, name in enumerate(lines[0].split("\t"))}
    out = {}
    for line in lines[1:]:
        fields = line.split("\t")
        out[fields[index["label"]]] = {
            name: float(fields[i]) for name, i in index.items() if name != "label"
        }
    return out


def fmt_ns(value):
    if value is None:
        return "-"
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:,.1f}"


# --- A3: what the members themselves said -----------------------------------
#
# `run-batch.sh` runs `cnc-dump --all` against every member's own aeron
# directory **before** it stops anything, and lands the text as
# `run-<N>/member<i>/counters.txt`. Three things in it decide whether a row is
# a row: the driver's heartbeat (was the member still alive at the end?), the
# election count (did it re-elect under the load?), and the node role (is there
# exactly one leader?). None of that is visible from the client's histogram,
# and a three-member cluster runs on a quorum of two -- so a member that dies
# mid-cell produces a cell that still sends every message and still reads well.

ROLE_NAMES = {0: "follower", 1: "candidate", 2: "leader"}


def read_member_counters(run_dir: Path) -> dict:
    """{member directory name: {active, elections, role}} for one run."""
    out = {}
    for path in sorted(run_dir.glob("member*/counters.txt")):
        active, elections, role = None, None, None
        for line in path.read_text(errors="replace").splitlines():
            if line.startswith("status"):
                # `status  ACTIVE (...)` or `status  INACTIVE (...)`; the second
                # word, because "ACTIVE" is a substring of "INACTIVE".
                words = line.split()
                active = len(words) > 1 and words[1] == "ACTIVE"
                continue
            # `  id  type  value  registration  label`
            parts = line.split(None, 4)
            if len(parts) < 5:
                continue
            label = parts[4]
            try:
                value = int(parts[2])
            except ValueError:
                continue
            if label.startswith("Cluster election count"):
                elections = value
            elif label.startswith("Cluster node role"):
                role = value
        out[path.parent.name] = {"active": active, "elections": elections, "role": role}
    return out


def cell_flags(run_dir: Path) -> list:
    """Every reason this cell is not a clean three-member cell, in words."""
    members = read_member_counters(run_dir)
    if not members:
        return ["no member counters"]
    flags = []
    leaders = 0
    for name, m in sorted(members.items()):
        role = ROLE_NAMES.get(m["role"], f"role={m['role']}")
        if m["role"] == 2:
            leaders += 1
        if m["elections"] is None:
            flags.append(f"{name}: no election counter ({role})")
        elif m["elections"] > 1:
            flags.append(f"{name}: {m['elections']} elections ({role})")
        if m["active"] is False:
            flags.append(f"{name}: driver not active")
        elif m["active"] is None:
            flags.append(f"{name}: no driver status")
    if leaders != 1:
        flags.append(f"{leaders} leaders")
    return flags


# --- A1: the client's own time series ---------------------------------------
#
# With `track.history=true` the harness writes, beside each `.hdr`, a
# `.hdr.csv` holding one row per logging interval (default 1 s):
#
#   timestamp (ms),50.0,99.0,99.9,99.99,99.999,100.0
#
# in nanoseconds. That is where "the knee" is a fact rather than something
# reverse-engineered from the aggregate histogram: the first interval whose
# p50 leaves the steady state by more than `factor`.


def read_history(hdr_csv: Path):
    """[(offset seconds, p50 ns, p99 ns)] from the client's interval log."""
    rows = []
    lines = hdr_csv.read_text(errors="replace").splitlines()
    if not lines:
        return rows
    start = None
    for line in lines[1:]:
        fields = line.split(",")
        if len(fields) < 7:
            continue
        try:
            ts, p50, p99 = int(fields[0]), float(fields[1]), float(fields[2])
        except ValueError:
            continue
        if start is None:
            start = ts
        rows.append(((ts - start) / 1000.0, p50, p99))
    return rows


def knee(rows, factor=5.0, baseline_rows=10):
    """(seconds, p50 before, p50 at the knee) or None."""
    if len(rows) < baseline_rows + 1:
        return None
    base = sorted(r[1] for r in rows[:baseline_rows])[baseline_rows // 2]
    if base <= 0:
        return None
    for seconds, p50, _ in rows[baseline_rows:]:
        if p50 > factor * base:
            return seconds, base, p50
    return None


# --- A4: where the interrupts went ------------------------------------------
#
# `run-batch.sh` snapshots `/proc/interrupts` before and after the client runs;
# the difference is the per-queue receive count. RSS hashes the 4-tuple, so a
# single client flow lands on **one** queue no matter how the IRQs are spread --
# which is why the question "did spreading the queues help?" is answered by the
# share of the top queue, not by the number of queues that moved.

IRQ_LINE = re.compile(r"^\s*(\d+):\s*((?:\d+\s+)+)(.*)$")


def read_irq_queues(path: Path) -> dict:
    """{label: count} for the NIC's receive queues in a /proc/interrupts file."""
    out = {}
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        m = IRQ_LINE.match(line)
        if not m:
            continue
        label = m.group(3).strip()
        if "-input" not in label:
            continue
        out[f"{m.group(1)} {label}"] = sum(int(v) for v in m.group(2).split())
    return out


def irq_delta(before: Path, after: Path):
    """(queues moved, top share, top label, total packets) or None."""
    b, a = read_irq_queues(before), read_irq_queues(after)
    if not a:
        return None
    delta = {k: a[k] - b.get(k, 0) for k in a}
    moved = {k: v for k, v in delta.items() if v > 0}
    total = sum(moved.values())
    if not total:
        return None
    top_label, top = max(moved.items(), key=lambda kv: kv[1])
    return len(moved), top / total, top_label.split()[-1], total


def ratio(left, right):
    if left is None or right is None or right == 0:
        return None
    return left / right


def sign(x):
    if x is None:
        return " "
    return "+" if x > 1 else ("-" if x < 1 else "=")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("results")
    ap.add_argument("--stat", default="e_lt_p99", choices=sorted(STATS))
    ap.add_argument(
        "--history",
        action="store_true",
        help="read each cell's .hdr.csv and print where its distribution turns (A1)",
    )
    ap.add_argument(
        "--irq",
        action="store_true",
        help="read each cell's /proc/interrupts pair and print where the packets went (A4)",
    )
    args = ap.parse_args()

    root = Path(args.results)
    if not root.is_dir():
        raise SystemExit(f"no such results directory: {root}")

    cells = collect(root)
    if not cells:
        raise SystemExit(f"no cells under {root} — is this a cluster-arms tree?")

    # One decoder run for the whole tree.
    wanted = {}
    for (round_name, arm, sync, pair), runs in cells.items():
        for run, (run_dir, hdr, failed) in runs.items():
            if hdr is None or failed:
                continue
            wanted[f"{round_name}|{arm}|{sync}|{pair}|{run}"] = (hdr, run_dir)
    stats = read_quantiles(wanted)
    column = quantile_column(args.stat)

    def value(round_name, arm, sync, pair):
        out = {}
        for run in cells[(round_name, arm, sync, pair)]:
            key = f"{round_name}|{arm}|{sync}|{pair}|{run}"
            got = stats.get(key)
            out[run] = got[column] if got else None
        return out

    rounds = sorted({k[0] for k in cells})
    syncs = sorted({k[2] for k in cells})
    pairs = sorted({k[3] for k in cells}, key=lambda p: (len(p.split(":")[0]), p))

    print(f"\n### {STATS[args.stat]} (ns), per round, per member's single node\n")

    per_round_ratio = defaultdict(dict)  # (round, ratio, sync, pair) -> number
    for round_name in rounds:
        print(f"== {round_name} ==")
        for sync in syncs:
            print(f"   sync archive:cluster = {sync}")
            print(f"   {'pair':10s} " + " ".join(f"{a:>10s}" for a in ARMS) + "   note")
            for pair in pairs:
                row, note, flags = [], "", []
                for arm in ARMS:
                    if (round_name, arm, sync, pair) not in cells:
                        row.append("-")
                        continue
                    runs = value(round_name, arm, sync, pair)
                    if not runs or any(v is None for v in runs.values()):
                        row.append("FAILED")
                        note = " (a cell did not finish)"
                        continue
                    row.append(fmt_ns(sum(runs.values()) / len(runs)))
                    for run, (run_dir, _hdr, failed) in cells[(round_name, arm, sync, pair)].items():
                        if failed:
                            continue
                        flags.extend(
                            f"{arm} run-{run}: {flag}" for flag in cell_flags(run_dir)
                        )
                if flags:
                    note = f" ! {len(flags)} member flag(s), see below"
                print(f"   {pair:10s} " + " ".join(f"{c:>10s}" for c in row) + note)

            for name in QUESTIONS:
                left, right = name.split("/")
                print(f"   ratio {name:8s} ({QUESTIONS[name]})")
                for pair in pairs:
                    got = ""
                    lr = value(round_name, left, sync, pair)
                    rr = value(round_name, right, sync, pair)
                    if lr and rr and None not in lr.values() and None not in rr.values():
                        l = sum(lr.values()) / len(lr)
                        r = sum(rr.values()) / len(rr)
                        q = ratio(l, r)
                        per_round_ratio[(round_name, name, sync, pair)] = q
                        got = f"{q:.4f} {sign(q)}"
                    print(f"      {pair:10s} {got}")
            print()

    print("### the members (A3): election count, role and driver heartbeat per cell\n")
    print("   silence below means every cell ended with one leader, one election per")
    print("   member, and every driver still active. Anything else is printed in full:")
    print("   a quorum of two lets a three-member cell survive a dead member.\n")
    any_flag = False
    missing = 0
    cells_seen = 0
    for round_name in rounds:
        for sync in syncs:
            for pair in pairs:
                for arm in ARMS:
                    key = (round_name, arm, sync, pair)
                    if key not in cells:
                        continue
                    for run, (run_dir, _hdr, failed) in sorted(cells[key].items()):
                        if failed:
                            continue
                        cells_seen += 1
                        flags = cell_flags(run_dir)
                        if flags == ["no member counters"]:
                            missing += 1
                            continue
                        if flags:
                            any_flag = True
                            print(
                                f"   {round_name} {arm:4s} sync {sync} {pair:10s} run-{run}: "
                                + "; ".join(flags)
                            )
    if cells_seen and missing == cells_seen:
        print("   (no member counters anywhere: this tree predates A2, or --no-counters)")
    elif missing:
        print(f"   ({missing} of {cells_seen} cells have no member counters)")
    if not any_flag and missing < cells_seen:
        print("   (none)")
    print()

    print("### ratios, both rounds (a ratio is read only when the sign survives)\n")
    for sync in syncs:
        print(f"   sync {sync}")
        for name in QUESTIONS:
            print(f"   {name:8s} {QUESTIONS[name]}")
            for pair in pairs:
                keys = [(r, name, sync, pair) for r in rounds]
                vals = [per_round_ratio.get(k) for k in keys]
                cells_txt = " ".join("-" if v is None else f"{v:.4f}" for v in vals)
                if len(vals) == 2 and None not in vals:
                    agree = sign(vals[0]) == sign(vals[1]) and sign(vals[0]) in "+-"
                    verdict = "read" if agree else "flips - not a reading"
                else:
                    verdict = "incomplete"
                print(f"      {pair:10s} {cells_txt:24s} {verdict}")
        print()

    print("### the floor: each arm against itself, forward vs reverse\n")
    print("   (a difference smaller than this is the session, not the arm)")
    for sync in syncs:
        for arm in ARMS:
            for pair in pairs:
                vals = []
                for round_name in rounds:
                    runs = value(round_name, arm, sync, pair) if (round_name, arm, sync, pair) in cells else {}
                    if runs and None not in runs.values():
                        vals.append(sum(runs.values()) / len(runs))
                    else:
                        vals.append(None)
                if len(vals) == 2 and None not in vals and min(vals) > 0:
                    drift = abs(vals[0] - vals[1]) / min(vals)
                    print(f"   sync {sync} {arm:4s} {pair:10s} {drift:.2%}")
    print()

    if args.history:
        print("### time series (A1): where each cell's distribution turns\n")
        print("   one row per second from the client's own .hdr.csv; the knee is the")
        print("   first interval whose p50 is 5x the median of the first ten. '-') means")
        print("   the cell never left its steady state (or the csv is not there.\n")
        for round_name in rounds:
            for sync in syncs:
                for pair in pairs:
                    for arm in ARMS:
                        key = (round_name, arm, sync, pair)
                        if key not in cells:
                            continue
                        for run, (run_dir, _hdr, failed) in sorted(cells[key].items()):
                            if failed:
                                continue
                            csvs = sorted(run_dir.glob("*.hdr.csv"))
                            label = f"   {round_name} {arm:4s} sync {sync} {pair:10s} run-{run}:"
                            if not csvs:
                                print(f"{label} no .hdr.csv (track.history off?)")
                                continue
                            rows = read_history(csvs[0])
                            got = knee(rows)
                            if got is None:
                                base = rows[-1][1] if rows else 0
                                print(f"{label} {len(rows):3d} intervals, no knee (last p50 {base / 1000:,.0f} us)")
                            else:
                                seconds, base, at = got
                                print(
                                    f"{label} {len(rows):3d} intervals, knee at +{seconds:,.0f} s"
                                    f" (p50 {base / 1000:,.0f} -> {at / 1000:,.0f} us)"
                                )
        print()
    if args.irq:
        print("### interrupts (A4): which queue took the traffic\n")
        print("   per member, the change in its NIC receive queues between the two")
        print("   snapshots. RSS hashes the 4-tuple: one client flow is one queue, so")
        print("   a top share near 100% is expected and spreading the IRQs cannot split it.\n")
        for round_name in rounds:
            for sync in syncs:
                for pair in pairs:
                    for arm in ARMS:
                        key = (round_name, arm, sync, pair)
                        if key not in cells:
                            continue
                        for run, (run_dir, _hdr, failed) in sorted(cells[key].items()):
                            if failed:
                                continue
                            for before in sorted(run_dir.glob("member*/irq-before.txt")):
                                after = before.with_name("irq-after.txt")
                                member = before.parent.name
                                got = irq_delta(before, after)
                                label = (
                                    f"   {round_name} {arm:4s} sync {sync} {pair:10s}"
                                    f" run-{run} {member}:"
                                )
                                if got is None:
                                    print(f"{label} no delta (snapshot missing or no rx queues)")
                                    continue
                                moved, share, top, total = got
                                print(
                                    f"{label} {moved} queue(s) moved, top {share:.0%}"
                                    f" ({top}), {total:,} packets"
                                )
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
