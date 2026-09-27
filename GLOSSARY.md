# Glossary

Every domain term used in deepmsg, with a plain-English meaning. Kept names
follow ADR-0005; tiers say where each name lives:

- **T1** — frozen at the byte boundary: the name appears in URI parameters,
  file names, schemas or wire frames; renaming breaks interop.
- **T2** — a string visible to reference tooling (counter labels, error
  text); kept aligned so reference tools stay readable.
- **code** — lives only in our API and docs; the Aeron name is kept by
  policy, but we are free to change it (and we spell out the C-era
  abbreviations: `sender_limit`, not `snd_lmt`).

## Processes and shared memory

| Term | Tier | Plain meaning |
|---|---|---|
| driver (media driver) | code | The per-host agent process. Owns the CnC file, allocates term buffers, and performs all UDP I/O for network channels. IPC payloads never pass through it — clients map the same shared memory — so it is not a broker. Our binary is `deepmsg-driver`. |
| client | code | The library embedded in the application process. Talks to the driver exclusively through shared memory; never opens sockets for data. |
| CnC file | T1 | The shared-memory block (`cnc.dat`) where clients and the driver exchange commands, events, counters and the error log. The only interface between them. |
| aeron directory | T1 | Working directory (default `/dev/shm/aeron-<user>`) holding the CnC file and stream files. Liveness is detected via the CnC heartbeat, not file locks; a second driver gets EBUSY; a dead driver's directory is reclaimed on next start. |

## Addressing and streams

| Term | Tier | Plain meaning |
|---|---|---|
| channel | T1 | URI identifying transport and endpoints: `aeron:udp?endpoint=...`, `aeron:ipc`, `aeron-spy:...`. The URI grammar and parameter names are a byte-level contract. |
| stream id | T1 | Numeric id separating streams multiplexed on one channel. |
| session id | T1 | Numeric id separating publication instances within one stream; assigned by the driver or requested by the publisher. |
| publication | code | Writer handle for one (channel, stream, session). Offers messages into the current term buffer. |
| exclusive publication | code | Single-writer variant: appends directly without CAS on the tail; not thread-safe; offers extra controls such as revoke. |
| subscription | code | Reader handle for one (channel, stream); polls images and hands fragments to handlers. |
| image | code | The reader-side view of one publisher's session on a subscription: a mapped term triple plus position state, one per (session, source). The name is unintuitive but standard; kept for cross-reading. |
| position | code | Global, monotonically increasing address in a stream's log, computed across the three-term rotation. The currency of flow control, recording and replay. |
| term (term buffer) | T1 | One of three fixed-size rotating partitions of a stream's circular log, mapped by clients and driver alike (`term-length`, `term-id`, `initial-term-id` URI parameters). Not an archive segment — that is a disk concept. |
| fragment | code | Unit delivered to a poll handler: a whole message if it fits the max payload, otherwise one piece; the fragment assembler recombines pieces. |
| MTU / max payload | T1 | Largest datagram payload the network path allows (default 1408; applies to IPC too). Larger messages are fragmented by the client at offer time — the driver never fragments. |

## Driver internals

| Term | Tier | Plain meaning |
|---|---|---|
| conductor | code | The single-threaded control-plane agent loop: a client conductor in each client, a driver conductor in the driver. Commands in, duty-cycle housekeeping out. |
| sender / receiver | code | The driver's data-plane threads for network channels: the sender drains term buffers to sockets (sendmmsg, one frame per datagram); the receiver fills images from sockets. |
| idle strategy | code | What an agent does inside its loop when there is no work: busy-spin, yielding, parking, sleeping — configurable per agent. |
| publisher limit | code | Backpressure knob seen by `offer`: the highest position the client may append. Advanced only by the driver conductor on its duty cycle (reference name: `pub_lmt`). |
| sender limit | code | Backpressure knob on network publications: the highest position the sender may have in flight. Written only by the flow-control strategy (reference name: `snd_lmt`). |
| flow control | T1 | The strategy computing the sender limit from received status messages: `max` (unicast default), `min` (multicast, with a new-receiver admission gate), `tagged` (group tagging). Selected via the URI `fc=` parameter. |
| SM (status message) | T1 | Receiver-to-sender feedback frame carrying the highest received position and the receiver window; the meeting point of flow control (sender side) and congestion control (receiver side). |
| NAK | T1 | Loss-report frame requesting retransmission; multicast NAKs are suppressed with random exponentially distributed delays to avoid storms. |
| RTTM | T1 | Round-trip-time measurement frames; the REPLY flag means "please echo", not "this is a reply". |
| heartbeat | code | A zero-length DATA frame (BEGIN\|END) sent when a publication would otherwise be idle (default every 100 ms), keeping SM/NAK state machines alive. |
| linger | T1 | Post-close grace state keeping stream resources alive for late readers (`linger` URI parameter). IPC has no linger timeout; network has a configurable one. |
| spy | T1 | The `aeron-spy:` URI scheme: subscribe locally to a network publication without joining its remote/multicast group. Archive LOCAL recording rides on this mechanism. |
| MDC / MDS | code | Multi-destination-cast: client-managed destination control on a live publication (send) or subscription (receive); the manual `control-mode=` URI parameters are the frozen part. |
| counters | T2 | Typed, labeled slots in the CnC file. Type ids are byte-level contract; label text is interop-visible. Cross-process references pass the 4-byte registration id — counters never leave the host. |
| client heartbeat | code | The counter (type `11`) a driver allocates per client, labelled `client-heartbeat: id=<clientId>`, which the *client* writes on its own duty cycle. There is no registration command and no client record region: this counter is the registration, and it is what the driver reaps a client by. |
| client liveness timeout | code | `aeron.client.liveness.timeout`, ten seconds by default: how long a client's heartbeat may go unwritten before the driver destroys everything that client owned and announces `ON_CLIENT_TIMEOUT` — unless the client closed itself first, which is announced as a departure without a timeout. |
| counter reuse deadline | code | `aeron.counters.free.to.reuse.timeout`, one second by default: how long a freed counter stays out of reuse, so a client holding a stale id cannot read a fresh counter as the old one. The durable half of reclamation is this plus `state = RECLAIMED`; the free list itself lives in the driver's heap. |

## Archive

| Term | Tier | Plain meaning |
|---|---|---|
| archive | code | The recording/replay service. Must be co-located with the shared memory it records (LOCAL sources); moving data off-host happens via archive-to-archive replication. |
| recording | code | One recorded (channel, stream) span, identified by a recording id. |
| recording.log | T1 | The archive's index file — the one deliberately non-SBE format in the system: 48-byte entries, 64-byte alignment, bit 31 of the stop position as the INVALID flag. |
| catalog | T1 | Fixed-slot index of recording descriptors next to the recordings; must remain readable by the reference Java `ArchiveTool` (P2 acceptance). |
| segment | T1 | On-disk recording file: one term buffer's worth of recorded stream data with checksums. A disk concept — deliberately never called a "term". |

## Future track (not built yet)

| Term | Tier | Plain meaning |
|---|---|---|
| cluster | — | The replicated state machine built over archive-recorded logs (elections, snapshots). Placeholder in `crates/cluster`. |
| standby | — | A premium-style passive follower that tracks a cluster and can snapshot without disturbing it. Deferred; the seams it needs are preserved (ADR-0001, `crates/cluster/README.md`). |
