#!/usr/bin/env bash
#
# Read a result tree that was made on purpose, and check the table `read.py`
# prints from it.
#
#   self-check.sh
#
# `read.py` is a reader, and a reader is the one part of a batch that can be
# wrong without anything failing: it finds fewer cells than the batch wrote and
# prints a table that looks like a result. So it is exercised here against a tree
# whose every number is known — same shape, same file names, same counter labels
# as a real batch writes (see the contract in `read.py`'s module note).
#
# What the tree is made to contain, beyond the happy path:
#
#   * **an arm with one run instead of two**, which has to show as one run rather
#     than as a mean over a shorter list;
#   * **a cell with no `counters.txt`**, which is still a cell — the latency
#     table is the reading — and has to show `-` in the counters table only;
#   * **both values of the `fsync` axis**, which is what tells the two cells
#     apart in every table;
#   * and counters whose values **differ by arm**, so that a table which paired
#     the wrong cell with the wrong arm is a mismatch rather than a coincidence.
#
# The histograms are copies of one committed capture
# (`crates/bench/tests/fixtures/reference-interval-log.hdr`), so the ratios in
# this run are all exactly 1.0000 by construction. **The ratios are not what this
# checks** — the pairing, the run counts, the cells and the counters table are.
#
# Needs a HdrHistogram to read the files with, the same one `tools/quantiles.sh`
# uses: the harness's jar or the Gradle cache. Without it this prints SKIPPED.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
bench=$(dirname "$here")
histogram="$bench/../crates/bench/tests/fixtures/reference-interval-log.hdr"
tree=$(mktemp -d)
trap 'rm -rf "$tree"' EXIT

failures=0
check() {
    local what=$1 haystack=$2
    if grep -qF -- "$what" <<<"$haystack"; then
        echo "  ok   $what"
    else
        echo "  FAIL $what"
        failures=$((failures + 1))
    fi
}

# The same, for the checks whose answer is a shape rather than a string: column
# widths in a table are not something to pin with a literal.
check_re() {
    local what=$1 haystack=$2
    if grep -qE -- "$what" <<<"$haystack"; then
        echo "  ok   $what"
    else
        echo "  FAIL $what"
        failures=$((failures + 1))
    fi
}

# --- the tree ----------------------------------------------------------------

# One cell: `<scenario>_<arm>_fsync=<0|2>_mtu=.._length=.._rate=..`.
#
# `runs` is how many run directories it gets, `write_bytes` what the recorder's
# total-write-bytes counter holds in each of them, and `with_counters` whether it
# gets that file at all.
cell() {
    local round=$1 scenario=$2 arm=$3 fsync=$4 runs=$5 write_bytes=$6 with_counters=$7
    local name="${scenario}_${arm}_fsync=${fsync}_mtu=1408_length=32_rate=100K"
    local run

    for run in $(seq 1 "$runs"); do
        local dir="$tree/$round/$name/run-$run"
        mkdir -p "$dir"
        cp "$histogram" "$dir/cell.hdr"
        echo "$((write_bytes * 10))" >"$dir/archive-du.txt"

        [ "$with_counters" = no ] && continue

        {
            printf 'archive-recorder total write bytes\t%s\n' "$write_bytes"
            printf 'archive-recorder total write time in ns\t%s\n' "$((write_bytes / 4))"
            printf 'archive-recorder max write time in ns\t%s\n' 1000
            printf 'archive-replayer total read bytes\t%s\n' "$((write_bytes * 2))"
            printf 'archive-replayer total read time in ns\t%s\n' "$((write_bytes / 2))"
            printf 'archive-replayer max read time in ns\t%s\n' 1000
            printf 'archive-conductor max cycle time in ns\t%s\n' 1000
            printf 'archive-recorder max cycle time in ns\t%s\n' 2000
            printf 'archive-replayer max cycle time in ns\t%s\n' 3000
        } >"$dir/counters.txt"
    done
}

round() {
    local name=$1
    cell "$name" s2 j        0 2 524288 yes
    cell "$name" s2 r-attach 0 2 1048576 yes
    cell "$name" s2 r-java   0 2 1048576 yes
    # One run, and the two rounds disagree about the arm's presence is the other
    # thing that must not be hidden: it is here with one run in both.
    cell "$name" s2 r-rust   0 1 2097152 yes
    cell "$name" s2 j        2 2 524288 yes
    cell "$name" s2 r-attach 2 2 1048576 no
}

round round-forward
round round-reverse

# --- the reading -------------------------------------------------------------

if ! "$bench/tools/quantiles.sh" "$histogram" >/dev/null 2>&1; then
    echo "SKIPPED: no HdrHistogram to read histograms with -- build the harness"
    echo "         (\`bench/build.sh\`) or let \`bench/lib/checkouts.sh\` find it."
    exit 0
fi

table=$("$here/read.py" "$tree")

echo "the tree:  $tree (5 cells x 2 rounds, one arm with a single run)"
echo
check "results   $tree" "$table"
check "r-attach/j" "$table"
check "r-java/r-attach" "$table"
check "r-rust/j" "$table"

# The arm's write-bytes column, per arm: 512 KiB for `j`, 1 MiB for the two arms
# that are ours, 2 MiB for the one with a single run. A reader that paired a cell
# with the wrong arm prints one of these where another belongs.
check "524288" "$table"
check "1048576" "$table"
check "2097152" "$table"

# The single-run arm shows one run, not two.
check_re "r-rust +1 +122099782" "$table"

# The cell whose `counters.txt` was never written still has its latency rows --
# it is the counters table that has to say `-` for it. Both are checked by their
# shape rather than by a number, because the histogram is the same everywhere in
# this tree and so are the latencies.
check_re "r-attach +2 +122099782" "$table"
check_re "s2:2 +r-attach +-" "$table"

# Every ratio here is 1.0000 by construction, and both rounds have to agree.
check "1.0000 / 1.0000  same sign" "$table"

if [ "$failures" -ne 0 ]; then
    echo
    echo "$failures check(s) failed; the table was:"
    sed 's/^/  | /' <<<"$table"
    exit 1
fi

echo
echo "all checks passed"
