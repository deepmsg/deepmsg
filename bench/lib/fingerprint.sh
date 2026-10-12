#!/usr/bin/env bash
#
# What a result set was taken on.
#
# `docs/benchmarks.md` says it first and this is the same rule: a number without
# its fingerprint is not a baseline, it is an anecdote. Both batches write one
# of these beside their results, and the tables in `docs/benchmarks.md` name the
# command that produced them.
#
# Sourced. `bench_fingerprint [output-file]` (default: stdout).
#
# Everything below is read, not restated — where a digest is derivable it is
# taken from the file itself, because a copy of a setting written here is a
# second place to keep in step.

bench_fingerprint() {
    local out=${1:-/dev/stdout}

    {
        echo "# machine"
        echo "host            $(hostname)"
        echo "date            $(date --rfc-3339=seconds)"
        echo "cpu             $(awk -F': ' '/^model name/{print $2; exit}' /proc/cpuinfo)"
        echo "cores           $(nproc) logical, $(lscpu | awk -F: '/^Core\(s\) per socket/{gsub(/ /,"",$2); print $2}') per socket, $(lscpu | awk -F: '/^Socket\(s\)/{gsub(/ /,"",$2); print $2}') socket(s)"
        echo "numa nodes      $(ls -d /sys/devices/system/node/node* 2>/dev/null | wc -l)"
        echo "memory          $(awk '/MemTotal/{printf "%.0f GiB\n", $2/1048576}' /proc/meminfo)"
        echo "kernel          $(uname -r)"
        echo "distro          $(. /etc/os-release && echo "$PRETTY_NAME")"
        echo "governor        $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo unknown)"
        echo "turbo           $(cat /sys/devices/system/cpu/intel_pstate/no_turbo 2>/dev/null || echo unknown) (0 = enabled)"
        # The directory itself only exists during a cell, so the size is asked of
        # whatever mount it would be created on.
        local mount=$BENCH_AERON_DIR
        while [ ! -d "$mount" ] && [ "$mount" != / ]; do mount=$(dirname "$mount"); done
        echo "aeron dir       $BENCH_AERON_DIR (on $mount, $(df -h "$mount" 2>/dev/null | awk 'NR==2 { print $2 }') total)"

        echo
        echo "# toolchain"
        echo "java            $(java -version 2>&1 | head -1)"
        echo "cargo           $(cargo --version 2>/dev/null)"
        echo "rustc           $(rustc --version 2>/dev/null)"
        echo "gcc             $(gcc --version 2>/dev/null | head -1)"
        echo "clang           $(clang --version 2>/dev/null | head -1)"

        echo
        echo "# the checkouts"
        _rev "$DEEPMSG_DIR" "this build"
        _rev "$BENCHMARKS_DIR" "harness"
        _rev "$AERON_REF_DIR" "reference"
        _rev "$AERON_FORK_DIR" "upstream"
        # A checkout with uncommitted work in it is not the revision it names.
        local dirty
        dirty=$(git -C "$BENCHMARKS_DIR" status --porcelain 2>/dev/null | wc -l)
        echo "harness dirty   $dirty path(s) modified"
        echo "harness aeron   $(grep -E '^aeron *=' "$BENCHMARKS_DIR/gradle/libs.versions.toml" 2>/dev/null)"

        echo
        echo "# binaries, by digest"
        _digest "$BENCH_DRIVER" "driver"
        _digest "$BENCH_RIG" "rig"
        _digest "$BENCH_NODE" "node"
        echo "driver profile  $(awk '/^\[profile\.release\]$/{f=1;next} /^\[/{f=0} f && /^(lto|codegen-units|panic)/{gsub(/ /,"");printf "%s ",$0}' "$DEEPMSG_DIR/Cargo.toml" | sed 's/ $//')"
        echo "rig build       CARGO_TARGET_DIR=target/rig CARGO_PROFILE_RELEASE_LTO=fat CARGO_PROFILE_RELEASE_PANIC=abort RUSTFLAGS=-C target-cpu=${RIG_TARGET_CPU:-skylake} cargo build --release -p deepmsg-bench --bin loadtest-rig --bin echo-node"

        echo
        echo "# the settings both drivers are given, by digest"
        _digest "$BENCHMARKS_SCRIPTS_DIR/low-latency-driver.properties" "low-latency"
        _digest "$BENCHMARKS_SCRIPTS_DIR/benchmark.properties" "harness props"
        for f in "$BENCH_DIR"/lib/conf/*.properties; do
            [ -f "$f" ] && _digest "$f" "$(basename "$f")"
        done

        echo
        echo "# pinning"
        echo "node cpu        $BENCH_NODE_CPU"
        echo "rig cpu         $BENCH_RIG_CPU"
    } >"$out"

    [ "$out" != /dev/stdout ] && echo "fingerprint written to $out"
}

_rev() {
    local dir=$1 label=$2 rev branch
    rev=$(git -C "$dir" rev-parse HEAD 2>/dev/null) || { echo "$label  (no git checkout at $dir)"; return; }
    branch=$(git -C "$dir" rev-parse --abbrev-ref HEAD 2>/dev/null)
    printf '%-15s %s (%s)\n' "$label" "$rev" "$branch"
}

_digest() {
    local path=$1 label=$2
    printf '%-15s %s\n' "$label" "$(sha256sum "$path" 2>/dev/null | cut -d' ' -f1 || echo '(missing)')"
}
