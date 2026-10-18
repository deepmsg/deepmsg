#!/usr/bin/env bash
#
# The two machines a batch measures across, and the one thing that has to be
# true before it measures anything: they can hear each other.
#
# The archive batch is the first one here that runs halves of a cell on two
# hosts (`archive-vs-java/`: the archive on the bench machine, the far end on
# the other). That makes the network a **precondition, not a variable**: a
# batch that starts with the two halves unable to reach each other does not
# fail cleanly — Aeron reports it as a connection that never formed, which
# reads exactly like an archive that is broken.
#
# So this file answers one question, before the first cell:
#
#     can these two machines actually exchange UDP datagrams?
#
# and if not, says which machine, which direction and which port.
#
# Sourced, never executed. Reads:
#
#   BENCH_PEER_SSH    how to reach the other machine, as `ssh` takes it
#                     (default: gavin@192.168.1.127, the WSL2 machine)
#   BENCH_PEER_IP     the address to probe, when the ssh target is a name
#                     (default: the host part of BENCH_PEER_SSH)
#   BENCH_LAN_PORT    the UDP port the probe echoes on (default: 47811)
#
# ★ Three checks, and the third is the only one that proves anything:
#
#   1. **the same segment** — the peer is reached directly, not through a
#      gateway (`ip route get`). Two addresses in the same /24 on two different
#      links would pass a prefix comparison and fail this;
#   2. **a control path** — `ssh` gets there, because the batch starts and stops
#      processes on both hosts and has to carry the histograms home;
#   3. ★ **the data path** — a UDP datagram goes out and comes back, **in both
#      directions**. This is the one that matters: Windows' firewall and WSL2's
#      default NAT let ICMP and even TCP be blocked while the machine is up, and
#      they are exactly what a listener on the other side has to survive.
#
# ★ Why not `ping`: ICMP is blocked by default on Windows. A failed ping does
# not mean UDP is blocked, and a successful one does not mean it is not. The
# criterion has to travel the way the measurement will.

if [ -z "${BASH_VERSION:-}" ]; then
    echo "lan.sh needs bash" >&2
    exit 2
fi

BENCH_PEER_SSH="${BENCH_PEER_SSH:-gavin@192.168.1.127}"
BENCH_PEER_IP="${BENCH_PEER_IP:-${BENCH_PEER_SSH#*@}}"
BENCH_LAN_PORT="${BENCH_LAN_PORT:-47811}"

_lan_fail() {
    # One shape for every failure, so that a reader of the batch's log knows
    # this was the network and not the thing being measured.
    echo "lan_require: $*" >&2
    echo "  the two machines are not usable as one measurement; nothing was run." >&2
    return 1
}

# The local address this machine would use to reach the peer. Not "the first
# address on the box": a machine with a virtual adapter has several, and the
# one that reaches the peer is the only one a cell may advertise.
_lan_local_ip() {
    ip -4 route get "$BENCH_PEER_IP" 2>/dev/null |
        awk '{ for (i = 1; i <= NF; i++) if ($i == "src") { print $(i + 1); exit } }'
}

# The peer must be on this machine's own segment: `ip route get` prints `via`
# only when a gateway is involved. A routed peer is still "the same network" in
# a loose sense and is **not** what a two-machine cell is allowed to run on —
# the whole point of two machines here is the wire, and a routed path makes the
# topology a third variable.
_lan_same_segment() {
    local route
    route=$(ip -4 route get "$BENCH_PEER_IP" 2>/dev/null) || return 1
    case " $route " in
        *" via "*) return 1 ;;
        *) return 0 ;;
    esac
}

# The UDP round trip, written as a one-liner on each side so that nothing has
# to be installed on the peer. Python3 is already a prerequisite of the readers.
_LAN_UDP_ECHO='
import socket, sys
port = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(("0.0.0.0", port))
s.settimeout(10.0)
data, addr = s.recvfrom(64)
s.sendto(b"pong:" + data, addr)
s.close()
'

_LAN_UDP_SEND='
import socket, sys
host, port, token = sys.argv[1], int(sys.argv[2]), sys.argv[3].encode()
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.settimeout(5.0)
s.sendto(b"ping:" + token, (host, port))
try:
    data, _ = s.recvfrom(64)
except socket.timeout:
    sys.exit("no reply within 5 s")
sys.exit(0 if data == b"pong:ping:" + token else "unexpected reply %r" % data)
'

# One direction: the echo runs on `$host_kind` (local or peer), the sender on
# the other. Returns 0 only when the token came back.
_lan_udp_round_trip() {
    local direction=$1 token="lan-$RANDOM$RANDOM"
    local -a echo_cmd send_cmd

    if [ "$direction" = "to-peer" ]; then
        echo_cmd=(ssh -o BatchMode=yes -o ConnectTimeout=5 "$BENCH_PEER_SSH"
            "python3 -c '$_LAN_UDP_ECHO' $BENCH_LAN_PORT")
        send_cmd=(python3 -c "$_LAN_UDP_SEND" "$BENCH_PEER_IP" "$BENCH_LAN_PORT" "$token")
    else
        echo_cmd=(python3 -c "$_LAN_UDP_ECHO" "$BENCH_LAN_PORT")
        send_cmd=(ssh -o BatchMode=yes -o ConnectTimeout=5 "$BENCH_PEER_SSH"
            "python3 -c '$_LAN_UDP_SEND' $(_lan_local_ip) $BENCH_LAN_PORT $token")
    fi

    "${echo_cmd[@]}" >/dev/null 2>&1 &
    local echo_pid=$!
    sleep 1

    local status=0
    "${send_cmd[@]}" >/dev/null 2>&1 || status=$?
    wait "$echo_pid" 2>/dev/null || true
    return "$status"
}

# What a batch calls before its first cell.
lan_require() {
    local problem=0

    if ! command -v python3 >/dev/null 2>&1; then
        _lan_fail "no python3 here; the UDP probe needs it"
        return 1
    fi

    if ! _lan_same_segment; then
        _lan_fail "the peer ($BENCH_PEER_SSH, $BENCH_PEER_IP) is not reached directly:" \
            "the route to it goes through a gateway, so the two machines are not on" \
            "one segment. Put them on one switch (or one Wi-Fi network) and re-run."
        problem=1
    fi

    if ! ssh -o BatchMode=yes -o ConnectTimeout=5 "$BENCH_PEER_SSH" true 2>/dev/null; then
        _lan_fail "no ssh to $BENCH_PEER_SSH." \
            "The batch starts and stops processes on both hosts, so a control path" \
            "is part of the measurement. Check the address, the key and the peer's" \
            "firewall before running a cell."
        problem=1
    fi

    [ "$problem" -eq 0 ] || return 1

    if ! _lan_udp_round_trip to-peer; then
        _lan_fail "UDP $BENCH_LAN_PORT does not get from here to $BENCH_PEER_IP and back." \
            "This is the data path a cell will use. On a WSL2 peer the usual causes" \
            "are the Windows firewall and WSL2's NAT: a port has to be forwarded" \
            "from Windows to the WSL2 instance (netsh interface portproxy) and the" \
            "firewall rule has to allow it, or WSL2 has to run in mirrored" \
            "networking mode."
        problem=1
    fi

    if ! _lan_udp_round_trip to-here; then
        _lan_fail "UDP $BENCH_LAN_PORT does not get from $BENCH_PEER_IP to here and back." \
            "Aeron's setup handshake needs the peer to be able to open the path in" \
            "this direction too; a one-way probe would pass and the cell would not."
        problem=1
    fi

    [ "$problem" -eq 0 ]
}
