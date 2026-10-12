#!/usr/bin/env bash
#
# Build one C driver arm out of a revision of the upstream work area.
#
#   build-c-driver.sh <arm-name> <git-ref> [--repo DIR] [--jobs N]
#
#   bench/upstream-c-drivers/build-c-driver.sh c-control upstream/master
#   bench/upstream-c-drivers/build-c-driver.sh c-patched driver/c-driver-receive-path
#
# The arm lands in `$AERON_C_ARM_DIR/<arm>/` as `aeronmd` beside the two shared
# libraries it needs, which is the shape the launcher in `arms/` expects (and
# the shape `scripts/aeron` itself has: the harness's own `c-media-driver` execs
# the `aeronmd` next to it).
#
# **The control is built here too, on purpose.** The harness ships prebuilt C
# binaries in `scripts/aeron`, but those are from a build of some other source
# at some other time, and comparing a patch against them compares the patch
# against a compiler, an era and a set of flags as well. Both arms of this batch
# come out of this script, from the same repository, with the same cmake and the
# same compiler; the difference between them is the commits and nothing else.
# The shipped `c` arm stays usable and is worth running as a witness.
#
# The source is taken as a `git worktree` at the revision, so the work area's
# own checkout is never moved. Worktrees and build trees live under
# `bench/out/`, which is ignored.
#
#   AERON_FORK_DIR    the upstream work area (default: ../aeron-deepmsg)
#   AERON_C_ARM_DIR   where arms are collected (default: bench/out/arms)
#   CMAKE             a cmake to use, if the search below finds the wrong one
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$here")
# shellcheck source=../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"


arm=${1:-}
ref=${2:-}
shift 2 2>/dev/null || true

jobs=$(nproc)
while [ $# -gt 0 ]; do
    case "$1" in
        --repo) AERON_FORK_DIR=$2; shift 2 ;;
        --jobs) jobs=$2; shift 2 ;;
        *) echo "unknown option $1" >&2; exit 2 ;;
    esac
done

if [ -z "$arm" ] || [ -z "$ref" ]; then
    sed -n '3,12p' "${BASH_SOURCE[0]}" >&2
    exit 2
fi

need_dir "upstream work area" "$AERON_FORK_DIR" || exit 1

# `cmake_minimum_required(VERSION 3.30)` is above what a distribution package
# tends to carry (Ubuntu 24.04 has 3.28), so the one the harness's own build
# downloads is looked for first, then `tools/`, then whatever is on PATH.
find_cmake() {
    local candidate version
    for candidate in "${CMAKE:-}" \
        "$AERON_FORK_DIR/cppbuild/cmake/bin/cmake" \
        "$(dirname "$BENCH_DIR")"/tools/cmake-*/bin/cmake \
        "$(command -v cmake 2>/dev/null || true)"; do
        [ -n "$candidate" ] && [ -x "$candidate" ] || continue
        version=$("$candidate" --version | head -1 | awk '{print $3}')
        if [ "$(printf '%s\n3.30\n' "$version" | sort -V | head -1)" = "3.30" ]; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

cmake=$(find_cmake) || {
    echo "no cmake >= 3.30 found" >&2
    echo "  the harness's own build downloads one into \$AERON_FORK_DIR/cppbuild/cmake/bin;" >&2
    echo "  or put one in tools/ and it will be found, or set CMAKE=/path/to/cmake" >&2
    exit 1
}

src="$BENCH_OUT_DIR/arms-src/$arm"
build="$BENCH_OUT_DIR/arms-build/$arm"
dest="$AERON_C_ARM_DIR/$arm"

revision=$(git -C "$AERON_FORK_DIR" rev-parse "$ref")

echo "== $arm =="
echo "revision        $revision ($ref)"
echo "cmake           $cmake ($("$cmake" --version | head -1))"
echo "compiler        $(cc --version | head -1)"
echo

rm -rf "$src"
mkdir -p "$BENCH_OUT_DIR/arms-src"
git -C "$AERON_FORK_DIR" worktree add --detach --force "$src" "$revision" >/dev/null

"$cmake" -S "$src" -B "$build" -DCMAKE_BUILD_TYPE=Release -DCMAKE_EXPORT_COMPILE_COMMANDS=OFF >/dev/null
"$cmake" --build "$build" --target aeronmd -j "$jobs" >"$BENCH_OUT_DIR/arms-build/$arm.log" 2>&1

mkdir -p "$dest"
for name in aeronmd libaeron_driver.so libaeron.so; do
    found=$(find "$build" -name "$name" -type f | head -1)
    if [ -z "$found" ]; then
        echo "the build produced no $name — see $BENCH_OUT_DIR/arms-build/$arm.log" >&2
        exit 1
    fi
    install -m 0755 "$found" "$dest/$name"
done

git -C "$AERON_FORK_DIR" worktree remove --force "$src" >/dev/null 2>&1 || true

echo "== $dest =="
ls -l "$dest"
echo
echo "== digests (these go in the record) =="
sha256sum "$dest/aeronmd" "$dest/libaeron_driver.so"
