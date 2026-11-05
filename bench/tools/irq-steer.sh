#!/usr/bin/env bash
#
# irq-steer.sh apply [CPUS] | spread [CPUS] | restore | show
#
# The NIC's receive queues: where their interrupts land, and whether that core
# is allowed to sleep. Two jobs, because the two things we have measured on
# these machines fight each other:
#
#   * an interrupt that lands on a core in HLT costs a C-state wakeup (~200 us
#     of path floor on the 2026-11-01 measurements), so the cores that take
#     interrupts must not idle; and
#   * one core taking every queue's interrupts caps the packet rate, and a
#     cluster has several flows (client ingress, log replication, catchup,
#     archive control/replication) even though each *flow* stays on one queue
#     (RSS hashes the 4-tuple; the client's ingress is a single 4-tuple, so
#     spreading queues does **not** split it).
#
#   apply [CPUS]    every queue's interrupt on CPUS[0] (default 12), C1 off there.
#                   This is the layout every reading before 2026-11-04 used.
#   spread [CPUS]   queues round-robin over a core list (default 8-15), C1 off
#                   on every core in it. Same "never idle" guarantee, but the
#                   other flows stop queueing behind the client's.
#   restore         put every saved affinity and C1 flag back, and drop the save.
#   show            the current mapping, the NIC's queue count, and the C1 flags.
#
# The save file is written once, before the first change, and restore is the
# only thing that consumes it: a machine must not be left steered.
set -eu

save=${IRQ_STEER_SAVE:-${TMPDIR:-/tmp}/irq-steer.saved}
nic=${IRQ_STEER_NIC:-virtio2}
iface=${IRQ_STEER_IFACE:-eth0}

_irqs() {
    awk -F: "/${nic}-input/ { gsub(/ /, \"\", \$1); print \$1 }" /proc/interrupts
}

_expand() { # "8-15" or "8,9,12" -> one cpu per line
    local spec=$1 part lo hi i
    local IFS=,
    for part in $spec; do
        case "$part" in
            *-*) lo=${part%%-*}; hi=${part##*-}
                 for ((i = lo; i <= hi; i++)); do echo "$i"; done ;;
            *)   echo "$part" ;;
        esac
    done
}

_save_once() {
    [ -e "$save" ] && return 0
    : >"$save"
    local n
    for n in $(_irqs); do
        echo "irq $n $(cat /proc/irq/$n/smp_affinity_list)" >>"$save"
    done
}

_save_cpus() { # <cpu>...
    local c
    for c in "$@"; do
        grep -q "^cpu $c " "$save" 2>/dev/null && continue
        echo "cpu $c $(cat /sys/devices/system/cpu/cpu$c/cpuidle/state1/disable 2>/dev/null || echo -)" >>"$save"
    done
}

_wake() { # <cpu>...
    local c
    for c in "$@"; do
        [ -e "/sys/devices/system/cpu/cpu$c/cpuidle/state1/disable" ] || continue
        echo 1 >"/sys/devices/system/cpu/cpu$c/cpuidle/state1/disable"
    done
}

_show() {
    echo "$(hostname -s) nic=$nic iface=$iface"
    if command -v ethtool >/dev/null 2>&1; then
        ethtool -l "$iface" 2>/dev/null | sed 's/^/  /' || true
    fi
    local n
    for n in $(_irqs); do
        printf '  irq %-4s cpu %s\n' "$n" "$(cat /proc/irq/$n/smp_affinity_list)"
    done
    local c
    for c in $(seq 0 $(($(nproc) - 1))); do
        [ -e "/sys/devices/system/cpu/cpu$c/cpuidle/state1/disable" ] || continue
        printf '  cpu %-2s c1-disabled=%s\n' "$c" \
            "$(cat /sys/devices/system/cpu/cpu$c/cpuidle/state1/disable)"
    done
}

case "${1:-}" in
    apply)
        first=$(_expand "${2:-12}" | head -1)
        _save_once
        _save_cpus "$first"
        for n in $(_irqs); do echo "$first" >"/proc/irq/$n/smp_affinity_list"; done
        _wake "$first"
        _show
        ;;
    spread)
        list=($(_expand "${2:-8-15}"))
        [ ${#list[@]} -gt 0 ] || { echo "irq-steer.sh: empty core list" >&2; exit 2; }
        _save_once
        _save_cpus "${list[@]}"
        i=0
        for n in $(_irqs); do
            echo "${list[$((i % ${#list[@]}))]}" >"/proc/irq/$n/smp_affinity_list"
            i=$((i + 1))
        done
        _wake "${list[@]}"
        echo "spread ${#list[@]} cores over $i queues"
        _show
        ;;
    restore)
        [ -e "$save" ] || { echo "irq-steer.sh: nothing saved" >&2; exit 0; }
        while read -r kind a b; do
            case "$kind" in
                irq) echo "$b" >"/proc/irq/$a/smp_affinity_list" ;;
                cpu) [ "$b" = "-" ] || echo "$b" >"/sys/devices/system/cpu/cpu$a/cpuidle/state1/disable" ;;
            esac
        done <"$save"
        rm -f "$save"
        _show
        ;;
    show|"")
        _show
        ;;
    *)
        echo "usage: irq-steer.sh apply [CPUS] | spread [CPUS] | restore | show" >&2
        exit 2
        ;;
esac
