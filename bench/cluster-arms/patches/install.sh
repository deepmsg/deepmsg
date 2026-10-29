#!/usr/bin/env bash
#
# Put "this build can be the media driver of a cluster" into the harness, and
# take it out again.
#
#   install.sh --check     say what is and is not in place (default)
#   install.sh --apply     apply the patch
#   install.sh --revert    undo it
#
# What goes in, and why:
#
# - **`scripts/aeron/deepmsg-media-driver`** — the launcher the harness's own
#   `media-driver` and `c-media-driver` have. It reads the two property files
#   those two hand their drivers and turns them into `-Dname=value`, which is
#   what this build's driver takes; the process keeps the name `deepmsg-driver`.
# - **`drivers['deepmsg']`** in `scripts/aeron/remote-cluster-benchmarks` — so
#   the reference's own cluster orchestration can be told `--server-drivers
#   deepmsg` and start this build's driver on a node.
# - **two branches in `scripts/aeron/remote-benchmarks-helper`** — the launcher
#   is resolved by `pgrep -x` when the harness waits for a driver, and killed by
#   name when it takes one down; both lists name `aeronmd` and the Java class,
#   and neither has a name for this build's binary.
#
# ★ **No jar is rebuilt.** This patch adds a shell launcher and two table rows;
# nothing in it is compiled, which is the difference between it and
# `archive-vs-java/patches/` (that one carries a Java change and has to rebuild
# the shadow jar). A `--check` that says `in source: in place` is therefore the
# whole answer.
#
#   BENCHMARKS_DIR    the harness checkout (see bench/lib/checkouts.sh)
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$(dirname "$here")")
# shellcheck source=../../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"

action=${1:---check}
patch=$here/cluster-arms.patch
launcher=$BENCHMARKS_DIR/scripts/aeron/deepmsg-media-driver
cluster_script=$BENCHMARKS_DIR/scripts/aeron/remote-cluster-benchmarks
helper=$BENCHMARKS_DIR/scripts/aeron/remote-benchmarks-helper

_in_source() {
    [ -x "$launcher" ] &&
        grep -q "deepmsg-media-driver" "$cluster_script" 2>/dev/null &&
        grep -q "deepmsg-media-driver" "$helper" 2>/dev/null
}

_check() {
    need_dir "harness checkout" "$BENCHMARKS_DIR" || return 1
    printf 'harness          %s\n' "$BENCHMARKS_DIR"
    printf 'patch            %s\n' "$patch"
    if _in_source; then
        printf 'in source        %s\n' "in place"
        printf 'launcher         %s\n' "$launcher"
    else
        printf 'in source        %s\n' "MISSING"
        printf 'launcher         %s\n' "$([ -x "$launcher" ] && echo 'present' || echo 'MISSING')"
    fi
}

_apply() {
    if _in_source; then
        echo "the patch is already in the source"
    else
        git -C "$BENCHMARKS_DIR" apply --check "$patch"
        git -C "$BENCHMARKS_DIR" apply "$patch"
        echo "applied $patch"
    fi
    _check
}

_revert() {
    if _in_source; then
        git -C "$BENCHMARKS_DIR" apply -R "$patch"
        echo "reverted $patch"
    else
        echo "the patch is not in the source"
    fi
    _check
}

case "$action" in
    --check) _check ;;
    --apply) _apply ;;
    --revert) _revert ;;
    -h|--help) sed -n '3,40p' "${BASH_SOURCE[0]}" ;;
    *) echo "usage: install.sh [--check|--apply|--revert]" >&2; exit 2 ;;
esac
