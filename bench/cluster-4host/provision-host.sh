#!/usr/bin/env bash
#
# Make one more machine able to be a cluster member: the same four things the
# existing pair has, copied from the controller so that nothing depends on the
# cloud having a package mirror.
#
#   provision-host.sh [--check] <user@host>
#
# What goes on the target, and why:
#
#   sysctl        `net.core.rmem_max/wmem_max` 16 MiB and 2 MiB defaults. The
#                 image ships ~416 KiB; the Java driver only warns about it and
#                 the C driver exits, so a machine that skipped this fails in a
#                 way that looks like a broken driver.
#   JDK           an **offline copy** of `java-21-openjdk-amd64`, and with it
#                 `/etc/java-21-openjdk` and `/etc/ssl/certs/java`: the JDK's
#                 `conf/security/java.security`, `lib/security/default.policy`,
#                 `lib/jvm.cfg` and `cacerts` are absolute symlinks into `/etc`,
#                 so a JDK copied without them dies in
#                 `SecureRandom.getDefaultPRNG` and no CnC file ever appears.
#   harness       `benchmarks-deepmsg`, `.git` and the built `benchmarks.jar`
#                 included: `scripts/aeron/cluster-node`, `media-driver` and
#                 `deepmsg-media-driver` all live there, and a member without
#                 them is a member that cannot start.
#   deepmsg       `target/release/deepmsg-driver` (the only binary the cluster
#                 run needs), plus `cnc-dump` for the counters afterwards.
#
# ★ Nothing here installs packages, so it works on a host with no egress. If a
# package is genuinely missing (`numactl`, `sudo`), the check says so by name
# rather than letting the first cell fail.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=hosts.env
. "$here/hosts.env"

check_only=0
if [ "${1:-}" = "--check" ]; then check_only=1; shift; fi
target=${1:?usage: provision-host.sh [--check] <user@host>}
target_ip=$(_cluster_host_of "$target")

_on() { ssh -n -o BatchMode=yes -o ConnectTimeout=8 "$target" "$@"; }

# ★ Without `-n`: `-n` sends stdin from /dev/null, which silently turns a piped
# `tar` into "This does not look like a tar archive".
_on_stream() { ssh -o BatchMode=yes -o ConnectTimeout=8 "$target" "$@"; }

echo "=== $target ($target_ip) ==="
_on true || { echo "provision: $target is not reachable over ssh" >&2; exit 1; }

verify() {
    local bad=0
    echo "-- what is there"
    _on "echo \"host      \$(hostname -s)  \$(nproc) cores\"; \
         echo \"kernel    \$(uname -sr)\"; \
         echo \"cpu       \$(awk -F': ' '/model name/{print \$2; exit}' /proc/cpuinfo)\"; \
         echo \"rmem_max  \$(cat /proc/sys/net/core/rmem_max)\"; \
         echo \"java      \$($CLUSTER_JAVA_HOME/bin/java -version 2>&1 | head -1 || echo missing)\"; \
         echo \"harness   \$(test -x $CLUSTER_BENCHMARKS_PATH/scripts/aeron/cluster-node && echo ok || echo missing)\"; \
         echo \"jar       \$(test -f $CLUSTER_BENCHMARKS_PATH/benchmarks-all/build/libs/benchmarks.jar && echo ok || echo missing)\"; \
         echo \"driver    \$(sha256sum $CLUSTER_DEEPMSG_DIR/target/release/deepmsg-driver 2>/dev/null | cut -d' ' -f1 || echo missing)\""

    echo "-- what a run needs and the image may not have"
    for tool in numactl sudo fstrim; do
        if ! _on "command -v $tool >/dev/null"; then
            echo "  MISSING $tool" >&2
            bad=1
        fi
    done
    if [ "$(_on "cat /proc/sys/net/core/rmem_max")" != "16777216" ]; then
        echo "  rmem_max is not 16777216" >&2
        bad=1
    fi
    local want got
    want=$(sha256sum "$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-driver" 2>/dev/null | cut -d' ' -f1)
    got=$(_on "sha256sum $CLUSTER_DEEPMSG_DIR/target/release/deepmsg-driver 2>/dev/null | cut -d' ' -f1" || true)
    if [ -n "$want" ] && [ "$want" != "$got" ]; then
        echo "  the driver differs from this machine's: want $want got ${got:-missing}" >&2
        bad=1
    fi
    return "$bad"
}

if [ "$check_only" = 1 ]; then
    verify && echo "check: ok"
    exit $?
fi

echo "-- sysctl"
_on "printf 'net.core.rmem_max = 16777216\nnet.core.wmem_max = 16777216\nnet.core.rmem_default = 2097152\nnet.core.wmem_default = 2097152\n' > /etc/sysctl.d/99-aeron.conf && sysctl --system >/dev/null && sysctl -n net.core.rmem_max"

echo "-- JDK (with the /etc symlinks it points at)"
_on "mkdir -p /usr/lib/jvm /etc/java-21-openjdk /etc/ssl/certs"
if ! _on "test -x $CLUSTER_JAVA_HOME/bin/java"; then
    tar -C /usr/lib/jvm -cf - "$(basename "$CLUSTER_JAVA_HOME")" | _on_stream "tar -C /usr/lib/jvm -xf -"
    tar -C /etc -cf - java-21-openjdk | _on_stream "tar -C /etc -xf -"
    tar -C /etc/ssl/certs -cf - java | _on_stream "tar -C /etc/ssl/certs -xf -"
fi

echo "-- harness"
_on "mkdir -p '$CLUSTER_BENCHMARKS_PATH' '$CLUSTER_DEEPMSG_DIR/target/release' '$CLUSTER_DATA_DIR'"
rsync -a --delete-after \
    "$CLUSTER_BENCHMARKS_PATH/" "$target:$CLUSTER_BENCHMARKS_PATH/"

echo "-- this build's driver"
rsync -a "$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-driver" \
    "$target:$CLUSTER_DEEPMSG_DIR/target/release/deepmsg-driver"
rsync -a "$CLUSTER_DEEPMSG_DIR/target/release/cnc-dump" \
    "$target:$CLUSTER_DEEPMSG_DIR/target/release/cnc-dump" 2>/dev/null || true

echo
verify && echo "provision: ok"
