#!/usr/bin/env bash
#
# Put "the archive is not mine" into the harness's `ArchiveNode`, and take it out
# again.
#
#   install.sh --check     say what is and is not in place (default)
#   install.sh --apply     apply the patch and rebuild the jar it lives in
#   install.sh --revert    undo the patch (the jar is left as it is; rebuild)
#
# What goes in, and why:
#
# - **`external-archive.patch`** — `ArchiveNode` launches an `ArchivingMediaDriver`
#   in its own constructor, so the reference's node can only ever talk to the
#   reference's archive. The arm that says anything about **this build's archive**
#   needs a node that connects to an archive somebody else runs; the patch adds
#   `io.aeron.benchmarks.aeron.archive.external=true`, which skips the launch and
#   connects through `aeron.archive.control.channel` exactly as a client does.
#
#   ★ The measurement side is untouched: the forwarding loop, the subscription on
#   the destination channel, and the fact that this node answers nothing are the
#   reference's own lines. A cell may swap the archive and nothing else, and this
#   patch is written so that is true.
#
#   The three dumps at the end read their directories from the driver the node is
#   a client of rather than from an `ArchivingMediaDriver` it may not own — that
#   is the whole of the rest of the change.
#
# ★ **The jar has to be rebuilt**: the harness's classes come out of one shadow
# jar, and a patched source with the old jar runs the old code. `--apply` does the
# rebuild; `--check` reads the property name out of the compiled class, so a stale
# jar is visible rather than silently used.
#
#   BENCHMARKS_DIR    the harness checkout (see bench/lib/checkouts.sh)
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$(dirname "$here")")
# shellcheck source=../../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"

action=${1:---check}
patch=$here/external-archive.patch
source_file=$BENCHMARKS_DIR/benchmarks-aeron/src/main/java/io/aeron/benchmarks/aeron/ArchiveNode.java
jar=$BENCHMARKS_DIR/benchmarks-all/build/libs/benchmarks.jar
marker=ARCHIVE_EXTERNAL_PROP_NAME

_in_source() { grep -q "$marker" "$source_file" 2>/dev/null; }

# The property name is a string constant in the compiled class, so the jar can be
# asked the same question the source is: is this switch in there?
_in_jar() {
    [ -f "$jar" ] || return 1
    unzip -p "$jar" io/aeron/benchmarks/aeron/ArchiveNode.class 2>/dev/null |
        grep -a -q 'io.aeron.benchmarks.aeron.archive.external'
}

_check() {
    need_dir "harness checkout" "$BENCHMARKS_DIR" || return 1
    printf 'harness          %s\n' "$BENCHMARKS_DIR"
    printf 'patch            %s\n' "$patch"
    printf 'in source        %s\n' "$(_in_source && echo 'in place' || echo 'MISSING')"
    printf 'in jar           %s\n' "$(_in_jar && echo 'in place' || echo 'MISSING — rebuild with --apply')"
}

_apply() {
    if _in_source; then
        echo "the patch is already in the source"
    else
        git -C "$BENCHMARKS_DIR" apply --check "$patch"
        git -C "$BENCHMARKS_DIR" apply "$patch"
        echo "applied $patch"
    fi
    if _in_jar; then
        echo "the jar already carries the switch"
    else
        echo "rebuilding the harness (one shadow jar holds its clients and nodes)…"
        ( cd "$BENCHMARKS_DIR" && ./gradlew :benchmarks-all:shadowJar -q )
    fi
    _check
}

_revert() {
    if _in_source; then
        git -C "$BENCHMARKS_DIR" apply -R "$patch"
        echo "reverted $patch"
        echo "the jar still carries the switch: rebuild it (./gradlew :benchmarks-all:shadowJar)"
    else
        echo "the patch is not in the source"
    fi
    _check
}

case "$action" in
    --check) _check ;;
    --apply) _apply ;;
    --revert) _revert ;;
    -h|--help) sed -n '3,30p' "${BASH_SOURCE[0]}" ;;
    *) echo "usage: install.sh [--check|--apply|--revert]" >&2; exit 2 ;;
esac
