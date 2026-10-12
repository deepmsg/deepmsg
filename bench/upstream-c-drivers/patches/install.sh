#!/usr/bin/env bash
#
# Put the two driver arms into the harness checkout, and take them out again.
#
#   install.sh --check            say what is and is not in place (default)
#   install.sh --apply            apply what is missing
#   install.sh --revert           undo what this applied
#
# What goes in, and why:
#
# - **`local-launch-shim.patch`** — the harness's single-host script still
#   launches every process through `execute_remote_command`, which is `ssh -i
#   <key> user@node` even when the node is this one. The patch makes an empty
#   node mean "this host" (`bash -c`, and `cp` instead of `scp`), and lets the
#   three throughput knobs be set independently. Without it a local run needs
#   passwordless ssh to localhost purely to re-enter the same machine — which is
#   also a legitimate way to run this, and the reason the patch is skipped when
#   the checkout already has it.
# - **`supported-drivers.patch`** — two more entries in the single-host script's
#   `supported_drivers`, `c-control` and `c-patched`. The harness picks a driver
#   by *name*, and the name is a launcher; there is no environment variable that
#   says which binary to run, so a third and fourth C driver have to be two more
#   names.
#
# `c-control` is the unmodified build of the same revision the patches are
# against; `c-patched` is that revision plus this tree's three driver commits.
# Both are built by `build-c-driver.sh` from the same source, with the same
# flags, so that what a run measures between them is the patches and not a
# compiler or an era. The harness's own `c` arm stays available and uses the
# binaries checked into the harness — a witness, not the control.
#
#   BENCHMARKS_DIR    the harness checkout (see bench/lib/checkouts.sh)
#   AERON_C_ARM_DIR   where the built arms are (default: bench/out/arms)
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BENCH_DIR=$(dirname "$(dirname "$here")")
# shellcheck source=../../lib/checkouts.sh
. "$BENCH_DIR/lib/checkouts.sh"

action=${1:---check}
arms=(c-control c-patched)

shim_marker='Local launch shim'
arm_marker="supported_drivers['c-patched']"

_shim_in_place() { grep -q "$shim_marker" "$BENCHMARKS_DIR/scripts/remote-benchmarks-runner" 2>/dev/null; }
_arms_in_place() { grep -q "$arm_marker" "$BENCHMARKS_DIR/scripts/aeron/remote-echo-single-host-benchmarks" 2>/dev/null; }

_check() {
    need_dir "harness checkout" "$BENCHMARKS_DIR" || return 1
    printf 'harness          %s\n' "$BENCHMARKS_DIR"
    printf 'local launch     %s\n' "$(_shim_in_place && echo 'in place' || echo 'MISSING')"
    printf 'driver arms      %s\n' "$(_arms_in_place && echo 'in place' || echo 'MISSING')"
    for arm in "${arms[@]}"; do
        printf '  %-10s binary   %s\n' "$arm" \
            "$([ -x "$AERON_C_ARM_DIR/$arm/aeronmd" ] && echo 'built' || echo 'not built')"
        printf '  %-10s launcher %s\n' "$arm" \
            "$([ -x "$BENCHMARKS_DIR/scripts/aeron/c-media-driver-$arm" ] && echo 'installed' || echo 'not installed')"
    done
}

_apply() {
    _check || return 1

    if _shim_in_place; then
        echo "local launch shim: already there, left alone"
    else
        echo "local launch shim: applying"
        git -C "$BENCHMARKS_DIR" apply "$here/local-launch-shim.patch"
    fi

    if _arms_in_place; then
        echo "driver arms: already there, left alone"
    else
        echo "driver arms: applying"
        git -C "$BENCHMARKS_DIR" apply "$here/supported-drivers.patch"
    fi

    for arm in "${arms[@]}"; do
        install -m 0755 "$BENCH_DIR/upstream-c-drivers/arms/c-media-driver-arm" \
            "$BENCHMARKS_DIR/scripts/aeron/c-media-driver-$arm"
        echo "launcher installed: scripts/aeron/c-media-driver-$arm"
    done

    echo
    echo "the harness now has four C driver names: c (its own binaries), c-control,"
    echo "c-patched, and the c-* variants it shipped with. A run names the arms it"
    echo "wants with --drivers, and the arms have to be built first:"
    echo "  bench/upstream-c-drivers/build-c-driver.sh c-control <the base revision>"
    echo "  bench/upstream-c-drivers/build-c-driver.sh c-patched <the patched revision>"
}

_revert() {
    for arm in "${arms[@]}"; do
        rm -f "$BENCHMARKS_DIR/scripts/aeron/c-media-driver-$arm"
        echo "launcher removed: scripts/aeron/c-media-driver-$arm"
    done
    if _arms_in_place; then
        git -C "$BENCHMARKS_DIR" apply -R "$here/supported-drivers.patch"
        echo "driver arms: reverted"
    fi
    if _shim_in_place; then
        git -C "$BENCHMARKS_DIR" apply -R "$here/local-launch-shim.patch"
        echo "local launch shim: reverted"
    fi
    echo
    echo "the built arms under ${AERON_C_ARM_DIR:-$BENCH_OUT_DIR/arms} are left where they are."
}

case "$action" in
    --check) _check ;;
    --apply) _apply ;;
    --revert) _revert ;;
    *)
        echo "usage: $0 [--check|--apply|--revert]" >&2
        exit 2
        ;;
esac
