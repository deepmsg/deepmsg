# Test fixtures

## The programs that make these are elsewhere

Four of the directories below are made by **Java programs**, and those programs are
not in this repository: they are C and Java, and this is a Rust workspace that
compiles neither. They live in the fork of the reference,
**`deepmsg/aeron`, branch `deepmsg-tests`**, under `deepmsg-tests/generators/`, and
`tests/src/aeron_tests.rs` is where this build looks for a checkout of it
(`DEEPMSG_AERON_TESTS`).

**What is here is what the tests read.** The `.bin`, `.catalog`, `.dat` and `.tsv`
files are committed, the tests take them by path or by `include_bytes!`, and moving a
generator changed no criterion — only the answer to "which program do I run to make
this again", which each directory's own README now gives.

The three C probes and three Java probes that *are* run by the interop suite
(`destination_probe.c`, `spy_probe.c`, `ipc_response_probe.c`, `ArchiveConnectProbe.java`,
`RecordingEventsProbe.java`, `ReferenceArchiveClientProbe.java`) live on that same
branch, under `deepmsg-tests/probes/` and `deepmsg-tests/java/`.

## `cnc-header.bin`

The first 512 bytes of a `cnc.dat` written by a **default-configured
`aeronmd` 1.53.2** (commit `664f58e705`), captured on 2026-09-26.

The file it came from was **48,238,592 bytes**. That number is asserted in
`tests/integration/cnc_fixture.rs` as the length the region arithmetic has to
reproduce from the metadata alone, so a regenerated fixture may need that
constant updated — which is the point of writing it down.

Only a prefix is committed, because a default driver's CnC file is 46 MB and
40 MB of that is the counter regions. The prefix carries the entire metadata
block (128 bytes) plus the first 384 bytes of the to-driver region, which is
enough to pin every field's offset and width and the packed version encoding.
The contents of the regions are covered by the synthetic buffers in
`deepmsg-cnc` and by the interop suite.

### Regenerating

    export AERON_DIR=/tmp/deepmsg-fixture-$$
    /path/to/aeron/cppbuild/Release/binaries/aeronmd &
    PID=$!
    sleep 4                       # until cnc.dat exists and is non-zero
    stat -c %s "$AERON_DIR/cnc.dat"          # note this; update the test if it changed
    head -c 512 "$AERON_DIR/cnc.dat" > tests/fixtures/cnc-header.bin
    kill -TERM "$PID"; wait "$PID"
    rm -rf "$AERON_DIR"

Signal the pid you spawned, never `pkill aeronmd`: a developer may have their
own driver running, and killing it would take their work with it. The harness
in `tests/src/driver.rs` exists partly so that no test has to remember this.

## `counters-metadata.bin`

The first **46 counter metadata records** — 46 × 512 = 23,552 bytes — of the
same captured `cnc.dat`: the counters a default-configured `aeronmd` 1.53.2
allocates before it publishes anything.

Captured from the same run as `cnc-header.bin` (2026-09-27), from the start of
the counters metadata region, which `docs/protocol/cnc-layout.md` places at
128 + to_driver + to_clients = 2,098,176 for the default layout.

The records are the contract `AeronStat` reads: state, type id, a four-byte
little-endian index as the key, the label, and — in the values region — the
index as the registration id and `-1` as the owner. What a *golden test* may
compare directly is everything except three things that are runtime or build
identity rather than contract:

- the counter values (byte counts, cycle times, the pid of the run),
- the two labels that name a build (`Errors: …`, `Aeron software: …`), and
- the labels' runtime suffixes: the threading mode, the resolver name and the
  duty-cycle thresholds, which a driver appends from its own configuration.

`crates/driver/tests/system_counters.rs` compares up to the first `:` of each
label for exactly that reason, and says so where it does it.

### Regenerating

Same as `cnc-header.bin`, with one extra step before the kill:

    python3 -c "
    import sys
    data = open(sys.argv[1], 'rb').read()[2098176:2098176 + 46 * 512]
    open('tests/fixtures/counters-metadata.bin', 'wb').write(data)
    " "$AERON_DIR/cnc.dat"

If a future reference build changes a label, the diff will be in this file —
which is the point of committing it whole rather than as a list of strings.

## `sbe/`

Sixty-two SBE messages encoded by the reference's own `sbe-tool` output, with
the reference's own decoder's reading of each one recorded in `sbe/golden.tsv`.
They are the golden for `deepmsg-codec`, and they are a directory of their own
because they are a set rather than a capture: `sbe/README.md` says how the set
is built, what the value rule is, and what it deliberately leaves uncovered.

## `frame/`

Two frames a Linux kernel built, captured on the wire, for the userspace UDP
stack (`crates/driver/src/media/userspace/frame.rs`) to reproduce byte for
byte:

- `ipv4-udp.bin` — 72 bytes: Ethernet II, IPv4, UDP, 30 bytes of payload.
- `arp-request.bin` — 42 bytes: the ARP request that resolves a next hop on the
  same subnet.

Captured 2026-10-14 on a `veth` pair inside a network namespace, with the
sending side's **TX checksum offload turned off**, so the UDP checksum in the
file is the one the kernel computed in software.

### Why not the loopback

Because `lo` cannot be made to compute it: `ethtool -k lo` reports
`tx-checksum-ip-generic: on [fixed]`, and `[fixed]` means it cannot be turned
off. What such a capture holds is the pseudo-header sum alone — tcpdump says
`bad udp cksum` — which is the opposite of what a golden for a checksum is for.

### Regenerating

    sudo sh capture.sh        # g13-capture/capture.sh, the recipe below

which is, with nothing left out:

    ip netns add g13; X="ip netns exec g13"
    $X ip link add va type veth peer name vb
    $X ip link set va address 02:00:00:00:00:01
    $X ip link set vb address 02:00:00:00:00:02
    $X ip link set va up; $X ip link set vb up
    $X ip addr add 10.77.0.1/24 dev va; $X ip addr add 10.77.0.2/24 dev vb
    $X ip addr add 10.77.0.3/24 dev vb
    $X ethtool -K va tx off
    $X ip neigh replace 10.77.0.2 lladdr 02:00:00:00:00:02 dev va
    $X tcpdump -i vb -c 1 -w udp.pcap 'udp port 40999' &
    $X python3 -c 'import socket; s = socket.socket(2, 2); s.bind(("10.77.0.1", 42001));
                   s.setsockopt(1, 25, b"va"); s.sendto(b"deepmsg golden ipv4/udp frame!", ("10.77.0.2", 40999))'
    $X tcpdump -i vb -c 1 -w arp.pcap 'arp' &
    $X python3 -c 'import socket; s = socket.socket(2, 2); s.bind(("10.77.0.1", 42002));
                   s.setsockopt(1, 25, b"va"); s.sendto(b"arp me", ("10.77.0.3", 40999))'
    ip netns del g13

then cut the frame out of each pcap (24-byte file header, 16-byte record
header, the bytes) and check it with `tcpdump -r udp.pcap -vv -e`, which is
where `[udp sum ok]` has to appear.

### What `ipv4-udp.bin` pins

| field | value |
|---|---|
| destination MAC | `02:00:00:00:00:02` |
| source MAC | `02:00:00:00:00:01` |
| ethertype | `0x0800` |
| version / IHL | 4 / 5 |
| TOS | 0 |
| total length | 58 |
| identification | 20175 (`0x4ecf`) — Linux's, and an **input** to the encoder |
| flags / fragment offset | `DF`, 0 |
| TTL | 64 |
| protocol | 17 |
| header checksum | `0xd747` |
| source / destination | `10.77.0.1` / `10.77.0.2` |
| source / destination port | 42001 / 40999 |
| UDP length / checksum | 38 / `0xf78b` |
| payload | `deepmsg golden ipv4/udp frame!` |

The identification is the one field the encoder takes rather than derives, and
the golden is why: Linux fills it per datagram from its own counter pool, so a
test that wants these bytes reproduces them by feeding the captured value back
in. TTL 64, TOS 0 and DF are the reference's own defaults for a UDP datagram on
a Linux host, which is what the userspace stack writes.

### What `arp-request.bin` pins

A request for `10.77.0.3` from `10.77.0.1`/`02:00:00:00:00:01`: the
broadcast destination, `0x0806`, hardware type 1, protocol type `0x0800`,
hardware length 6, protocol length 4, operator 1, the sender's MAC and
address, and a zero target hardware address.

### What it does not pin

**The ARP reply.** This host's kernel does not answer that request — measured,
not assumed: three requests a second apart, and the far side's `ip neigh`
ends `FAILED` — so there is no reply to capture here, and the reply's encoder
is tested against a frame built from the request's own fields (operator 2, the
target fields filled in). What the request does pin is the field order and
widths, which the reply shares.
