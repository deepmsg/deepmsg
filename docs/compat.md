# Compatibility matrix

deepmsg targets byte-level compatibility with Aeron **1.53.2** (ADR-0001).
Every row below is a testable contract; a row counts as verified only when
the interop suite (`tests/interop`, feature `interop`) or a golden byte test
covers it.

| Surface | Constant | Value | Reference source |
|---|---|---|---|
| CnC semantic version | `AERON_CNC_VERSION` | **0.2.0** | `aeron-client/src/main/c/aeron_cnc_file_descriptor.h:26` |
| Archive protocol SBE | schema id / version | 101 / 14 (semanticVersion 5.2) | `schemas/aeron-archive-codecs.xml` |
| Archive mark SBE | schema id / version | 100 / 2 (semanticVersion 5.2) | `schemas/aeron-archive-mark-codecs.xml` |
| Cluster protocol SBE | schema id / version | 111 / 17 (semanticVersion 5.4) | `schemas/aeron-cluster-codecs.xml` |
| Cluster mark SBE | schema id / version | 110 / 2 (semanticVersion 5.4) | `schemas/aeron-cluster-mark-codecs.xml` |
| Cluster node-state SBE | schema id / version | 112 / 10 | `schemas/aeron-cluster-node-state-codecs.xml` |
| Frame byte order | SBE `byteOrder` | little-endian; only the socket API converts (ports/addresses) | `schemas/*.xml` |
| Archive `recording.log` | — | hand-rolled: 48-byte entries, INVALID bit 31, 64-byte alignment, no file header | `aeron-cluster/src/main/java/io/aeron/cluster/RecordingLog.java` |

Rules:

- A CnC file is accepted when its major matches
  (`aeron-client/src/main/c/aeron_cnc_file_descriptor.c:103`) **and** its minor
  is >= ours (`aeron-client/src/main/java/io/aeron/CommonContext.java:1432`,
  `aeron-client/src/main/c/aeron_context.c:617`). The C reference disagrees
  with itself on the second rule — its read path omits it and its command path
  applies it — so `deepmsg` applies both everywhere and the choice is recorded
  in `docs/protocol/cnc-layout.md`. The rules are covered by unit tests in
  `crates/core/src/version.rs` and by
  `tests/integration/cnc_terminate.rs`; they have to be, because the reference
  driver only ever writes 0.2.0 and the interop suite can reach no other branch
  of the table.
- Wire-protocol constants (frame types, header layouts) live in
  `deepmsg-core::logbuffer` and `docs/protocol/` once migrated; this file
  tracks cross-component versions only.
- Changing anything in this table without extending the interop suite first
  is a review-blocking offence.

## Counters

Two of the forty-six system counter labels name the build that wrote the file:
`Errors: version=… commit=…` (id 15) and `Aeron software: …` (id 34)
(`aeron-driver/src/main/c/aeron_system_counters.c:41,60`). The reference writes
`AERON_VERSION_TXT` and its git sha. deepmsg writes `version=1.53.2` — the
version whose contracts it implements, and the same version its value field
packs as `79106` — and `commit=deepmsg-<crate version>`.

That is a recorded divergence rather than a copy, and it is deliberate: a git
sha is a fact about a build, the reference's own label changes from build to
build, and writing theirs would put a claim in the file that is not true. A
tool that *parses* the field (none in the reference does; `AeronStat` prints
it) would take `deepmsg-0.1.0` for a commit, which is the one honest thing it
could say.

The other labels the reference suffixes at runtime — the driver's threading
mode, the resolver's name, the duty-cycle thresholds
(`aeron_driver_conductor.c:848-951`) — are *configuration*, not contract: the
reference's own text changes with its settings. deepmsg appends `DEDICATED` to
the conductor's, the sender's and the receiver's cycle-time counters, matching
the reference's word for a driver whose agents are threads of their own — which
is what this build's three are. The name-resolver pair (32 and 33) is left
unsuffixed: resolution is synchronous here and there is no resolver agent to
time (the row on that is under the UDP data plane), so a threshold label would
name a thread that does not exist.

The consequence for testing: a golden comparison of the two catalogues
compares each label up to its first colon, and
`crates/driver/tests/system_counters.rs` asserts the masked halves separately
so that being *absent* cannot pass for being right.

## The counter a client asks for

The section above is the system catalogue the driver writes; this one is the
surface a client gets when it asks for a counter of its own — the
`ADD_COUNTER`/`REMOVE_COUNTER` round, whose messages (`ON_COUNTER_READY`,
`ON_OPERATION_SUCCEEDED`, `ON_UNAVAILABLE_COUNTER`) this build serves as the
reference does. Three divergences on the client's side of that surface, each
naming the test that covers it.

- **A blocking add.** The reference's C client registers a counter
  asynchronously — `aeron_add_counter` returns before any reply, and the
  counter surfaces through the available-counter callback the caller
  registered (`aeron_client_conductor.c:850-895` pairs the reply to the
  registration). deepmsg's `Client::add_counter` blocks until the reply
  lands or a caller-supplied deadline passes, and `remove_counter` waits the
  same way for its acknowledgement — the Java client's shape rather than the
  C one. Covered by
  `tests/interop/counters_and_error_log.rs::our_client_adds_and_removes_a_counter_on_the_reference_driver`,
  which adds, reads, writes, lists through the reference's `AeronStat`, and
  removes a counter on the reference's own driver.
- **Events, not callbacks.** The reference fires registered handlers inside
  its conductor's duty cycle — available handlers on every
  `ON_COUNTER_READY`, unavailable ones on every `ON_UNAVAILABLE_COUNTER`
  (`aeron_client_conductor.c:850-906`), whatever else the client was doing.
  deepmsg queues `CounterEvent`s on the same response pass and hands them
  over when the caller asks (`Client::counter_events`), so a handler can
  never run re-entrant inside the client's own poll. Covered by
  `crates/driver/src/conductor.rs::counter_announcements_arrive_as_events_for_the_watchers`
  and `::another_clients_counters_arrive_as_events_too`, and by the interop
  test above, whose events arrive from the reference's driver.
- **No static counters.** `ADD_STATIC_COUNTER` (`0x0F`,
  `aeron-client/src/main/c/command/aeron_control_protocol.h:41`) is in the
  command table but has no handler: the reference's driver allocates the
  counter (`aeron-driver/src/main/c/aeron_driver_conductor.c:3153-3166`) and
  answers `ON_STATIC_COUNTER` (`0x0F0B`,
  `aeron_control_protocol.h:56`), which its client pairs with the ready
  handler (`aeron_client_conductor.c:1147`). deepmsg counts the command as
  unhandled and stays silent, so a client asking for one waits until its
  deadline. Covered by
  `crates/driver/src/conductor.rs::a_static_counter_request_is_recognised_and_not_served`.

## Driver liveness and the directory

Two drivers cannot share an aeron directory, and the question "is somebody else
living here?" is the to-driver ring's consumer heartbeat — there is no lock file
anywhere (`aeron-driver/src/main/c/aeron_driver.c:136-235`). Two rules around it
are worth recording, because deepmsg answers them the way the *contract* does
rather than the way the reference's code happens to:

- **A file whose version is still zero is not a dead driver.** The reference
  waits for the version to appear before it judges anything, up to
  `aeron.driver.timeout` (`aeron_is_driver_active_with_cnc`, `aeron_driver_context.c:1603-1612`),
  because a driver creating a 46 MB file looks exactly like a dead one. So does
  deepmsg. Past the window, a file that never got a version is taken over. The
  *wait* is measured with a monotonic clock rather than the reference's epoch
  clock: a duration measured against a clock a VM can step is a different
  duration after the step, and the failure mode of the short side is a live
  driver's directory.
- **A file this build may not *read* is not a file nobody owns.** The reference
  judges liveness by major version alone; deepmsg also applies the Java client's
  minor rule when reading a file. Where the two meet — a same-major, older-minor
  file — deepmsg refuses the directory (`BusyIncompatible`) instead of deleting
  it. The reference would find a fresh heartbeat there and refuse it too; the
  difference is only in which test it took to get there, and the cost of the
  wrong answer is a live driver's directory.

A CnC file is also *published* differently: `CncFile::publish` refuses to store
the version until the to-driver ring has a heartbeat, because the reference
writes the heartbeat first for a reason — a file that passes a client's version
gate and then fails its liveness rule is a file every client reads as a driver
that is already gone (`aeron-driver/src/main/c/aeron_driver.c:971-972`).

## The space a log buffer needs

Before a log buffer is created, the filesystem it will land on is asked
whether it has room — the reference's check, on by default
(`perform.storage.checks`, `aeron_driver_context.c:1354-1375`) — and a
refusal reaches the client as `STORAGE_SPACE`, the code the reference
composes for an `ENOSPC`, whether the check's or the kernel's
(`aeron_driver_conductor.c:2326-2341`). The setting's names and defaults
are pinned by `crates/driver/src/config.rs::the_storage_check_answers_to_the_references_names`,
and the refusal by the native resource agent's own tests.

The check's second half: a filesystem that can hold the log buffer but
sits at or below `low.file.store.warning.threshold` is warned about in the
driver's distinct error log, in the reference's own words and under its
code (`-STORAGE_SPACE`, negated as its `AERON_SET_ERR` left it), and the
create goes ahead — the reference records the warning directly rather than
through its `log_explicit_error`, so the errors counter does not move
(`aeron_driver_context.c:1368-1377`). The reference's agent thread writes
the entry itself; deepmsg's agent hands the numbers to the conductor, which
owns the de-duplication table, so the entry lands on the duty cycle after
the check rather than during it. The setting's default — ten of the
reference's default term lengths (`aeron_driver_context.c:179-183`) — is
pinned by configuration's own tests, and the warning's shape by the
conductor's
`a_low_space_warning_is_recorded_without_counting_and_the_log_buffer_lands`.

## The distinct error log

An entry this driver records carries the reference's composition — the
negated code with its description, then the recording site and the message —
built by `deepmsg_cnc::error_log::compose_description` and laid out in
`docs/protocol/error-log-layout.md`. It is verified against the reference as
text, not structure: the same unknown command recorded by each driver, read
out by the reference's own `ErrorStat`, with only the timestamps masked
(`tests/interop/counters_and_error_log.rs::error_stat_reads_what_both_drivers_record_for_an_unknown_command`),
and the reference's region read by this build's reader matching the tool's
report verbatim
(`::our_reader_reads_the_reference_error_log_and_matches_error_stat`).

One recorded text diverges: a broadcast transmit failure. The reference
appends it with `AERON_APPEND_ERR` (`aeron_driver_conductor.c:2240`), which
resets neither the code nor the thread's error buffer — so the entry is the
tail of a chain that thread has accumulated, ended by the append's own site
line, and reads `[aeron_driver_conductor_client_transmit,
aeron_driver_conductor.c:2240] failed to transmit message\n` under code
zero (`aeron-client/src/main/c/util/aeron_error.c:380-388`; appending never
sets a code). deepmsg records the bare `"failed to transmit message"`: the
same code, the same de-duplication, without the site line that only means
something on the reference's own thread. Covered by
`crates/driver/src/conductor.rs::a_broadcast_failure_is_recorded_and_counted`.

What the reference's *agent thread* records on its own account is not here,
and the four recordings are named so the absence is a decision rather than a
gap. All of them are in
`aeron-driver/src/main/c/aeron_driver_native_resource_agent.c`:

- An agent command its switch does not know is
  `record(EINVAL, "unknown command")` (`:176-177`). This build's agent takes
  a typed enum, so the failure has no representation to report.
- A name resolver that would not start (`:239-247`, both its branches)
  arrives with UDP, which this build has not reached.
- The other two are allocation failures — a deque that would not grow
  (`:417-420`) and an error message it could not allocate (`:131-133`);
  this build's agent hands work over an unbounded channel and allocates
  through Rust, so neither has a moment to fail at.

The failure from that thread that *does* reach the log — a filesystem with
no room for the log buffer — reaches it as the answer the client gets,
recorded with that answer like any other error. Covered by
`crates/driver/src/conductor.rs::a_refused_log_buffer_is_answered_and_recorded`
and, for the agent's side of it,
`crates/driver/src/native_resource_agent.rs::a_refused_log_buffer_never_touches_the_filesystem`.

## The session id a new publication is given

A session id no client names is speculated: the driver takes the first id from
its cursor that no live publication on that stream holds. The reference scans a
bit set one longer than its publication count — there is always a free bit, so
the answer always exists (`aeron-driver/src/main/c/aeron_driver_conductor.c:3787-3789`,
with the search itself at `:1089-1105`). deepmsg scans a fixed window of 1024
(`SessionIds::MAX_SPECULATION`) instead of a per-driver-sized set, which finds
the same id for every driver whose live publications on one stream number fewer
than that, and answers the cursor's own id rather than failing when even that
window is full. Covered by
`crates/driver/src/ipc_publications.rs::a_speculated_session_is_the_first_one_the_stream_is_not_using`
and `::a_speculation_always_answers_even_when_every_id_is_in_use`.

## The channel URIs this driver refuses

Three shapes the reference *serves*, this driver refuses, because in each one
a parameter has silently gone missing rather than been absent: a trailing
`?key` with no `=` and a URI that ends on `|` — both dropped by the reference's
scanner, whose final flush only runs while it is inside a value
(`aeron-client/src/main/c/uri/aeron_uri.c:96-102`) — and a trailing `key=`,
whose null value the reference's readers treat as the parameter not being
there at all (`:357-361`). Covered by
`crates/driver/src/channel_uri.rs::the_destructive_shapes_are_refused_here`.

The error *codes* follow the reference's split rather than the shapes: a URI
the driver cannot read is `INVALID_CHANNEL`, which is what the reference
raises for a bad scheme, a bad transport, an over-long URI and a malformed
parameter (`aeron_uri.c:269-273`, `:311-314`, `:48`, `:84`); a parameter
*value* its readers would reject is the generic code, because that is what
the reference's bare `-1` and `EINVAL` returns compose to
(`aeron-driver/src/main/c/aeron_driver_conductor.c:2326-2341`). Covered by
`crates/driver/src/conductor.rs::a_parameter_value_the_reference_cannot_parse_is_generic`
alongside `::a_channel_this_driver_cannot_serve_is_refused_with_a_code_and_an_answer`.

The words that ride an `ON_ERROR` are diagnostic, not contract: the two
"unknown publication/subscription" answers carry the reference's `client_id=`
and `registration_id=` fields (`aeron_driver_conductor.c:4734`, `:5258`;
asserted by `crates/driver/src/conductor.rs::an_unknown_removal_is_answered_with_the_references_code`),
and the rest are single lines where the reference sends an accumulated chain
of every `AERON_SET_ERR` and `AERON_APPEND_ERR` on the path (e.g. `:4757`) —
less context, the same code and the same correlation.

Those words are also what an `ON_ERROR` records: answering an error and
recording it are one act in the reference, which transmits the response and
then falls through to its own `log_error:` label (`:2366-2370`), skipping the
log only for `RESOURCE_TEMPORARILY_UNAVAILABLE` (`:2367`). This build keeps
the two together the same way, so the entry holds the words the client was
handed — which are the ones this paragraph just called diagnostic, rather
than the composition an `AERON_SET_ERR` would have built. Asserted by
`crates/driver/src/conductor.rs::an_unknown_removal_is_answered_with_the_references_code`
and `::a_refused_log_buffer_is_answered_and_recorded`.

## The UDP data plane (P1-4)

The network channel is now served, both ways: a publication on
`aeron:udp?endpoint=…` sends, and a subscription on the same shape receives.
The wire contract is `docs/protocol/wire-frames.md`; what follows is what this
build does *differently* from the reference, and each row names where it can
be falsified.

| Divergence | Why, and where it shows |
|---|---|
| **Multicast, ATS and the timestamp-offset parameters are refused** (`NOT_SUPPORTED`) rather than served | The reference serves all three, and a driver that *ignored* them would serve a different channel: a multicast group that receives nothing, a timestamped image without timestamps, an ATS endpoint without encryption. Multicast would also need the `min` flow-control admission gate, `group`/`gtag` and setup-catchup windows on a group. Response channels were on this list and are served now — see the destination-plane section below. `crates/driver/src/udp_channel.rs::a_multicast_endpoint_is_refused_rather_than_served_as_unicast`, `::the_unsupported_parameters_are_named_rather_than_dropped`. |
| **`recvmmsg` is called without a timeout** | The reference passes a zero `timespec`, which the kernel reads as "return after the first datagram" — one syscall per datagram of a burst. This passes `NULL` on a non-blocking socket, which returns everything queued. Same datagrams, fewer syscalls (`aeron_udp_channel_transport.c:560`, `:577`). |
| **A frame is copied into a scratch buffer before it is sent** | The reference hands `sendmmsg` an iovec pointing into the mapped term. `deepmsg_core::buffer` refuses to mint a `&[u8]` over memory another thread is writing — a false immutability promise is licence for the optimiser to hoist loads — and a syscall taking `&[u8]` is exactly that borrow. `crates/driver/src/network_publication.rs`'s module note. |
| **A subscription's join position is the image's `rcv-pos`** | The reference takes the slowest *reader* when an image has several (`aeron_publication_image.h:376-396`), which needs the reader set — and that lives on the receiver thread that owns the image. `rcv-pos` is the position a reader may start at without seeing a hole, and for the one-reader case the two are the same number. `crates/driver/src/publication_images.rs::join_position`. |
| **Name resolution is synchronous** (`getaddrinfo` in the resolve step), so the RES/`csv` table gossip, the resolver's cycle-time counters **and the reference's re-resolution** are all absent | The resolver agent is a slice of its own (`aeron.driver.reresolution.check.interval` is 1 s, `aeron_driver_context.h:206`; the loop is `aeron_destination_tracker.c:402-445` and `aeron_send_channel_endpoint.c:754-774`). A literal address re-resolves to itself, so a literal channel loses nothing, and a name covers every interop case. What the absence costs is a *changed* name and an unresolved one: the reference keeps an addressless destination and re-resolves it until it answers (`aeron_driver_conductor.c:5339-5345`), while here it stays addressless for the channel's life. `crates/driver/src/udp_channel.rs::a_host_is_resolved_by_the_system_when_it_is_not_a_literal`. |
| **A channel-status counter's key is zero-filled past the channel** | The reference `memcpy`s the channel into an uninitialized struct, so the key's tail is whatever was on its stack (`aeron_position.c:220-222`). The bytes here are the same for the same channel every time, which is what a key is for. |
| **Loss injection is configured by this build's own property, and a datagram it withholds is reported as sent** | Two differences in one seam. The reference's debug loss surface is eight `AERON_DEBUG_{SEND,RECEIVE}_{DATA,CONTROL}_LOSS_{RATE,SEED}` variables (`media/aeron_debug_channel_endpoint_configuration.h:22-29`) read by an installer the driver context never calls — only a C++ test does (`aeron-driver/src/test/c/media/aeron_test_loss_generators_test.cpp:467`, `media/aeron_debug_channel_endpoint_configuration.c:126-172`) — so a driver started as a process has no way in, and every interop test here starts one. This build reads `deepmsg.debug.send.data.loss.drop.every` instead: a count, not a rate, over outgoing datagrams, on the send endpoint's data slot only. The second difference is what a withheld call reports: the reference answers `0` (`media/aeron_send_channel_endpoint.c:391-403`), so its sender never advances `snd-pos` and the frames go out on a later pass — no gap, nothing retransmitted. Here the withheld datagram counts as handed over, which is the only way a gap, a NAK and a retransmission become observable on a wire that does not lose anything. Covered by `tests/interop/udp_transport.rs::a_withheld_frame_is_retransmitted_until_the_reference_subscriber_has_it`; the generator itself is `crates/driver/src/media/loss_generator.rs`. |
| **Two native resource agents, not one** | `IpcPublications` and `PublicationsImages` each own one, because each maps its own log buffers. Invisible to a client (both are threads that map files); unifying them is a cleanup, not a contract. |

The first row is the one a client can see from outside, and it is the reason
the refusal exists: `NOT_SUPPORTED` is an answer, silence is not.

## Destinations and the response channel (P1-5)

A publication on a **multi-destination** channel sends to every destination it
holds, and which destinations those are is a property of the channel's control
mode, not of whether one has been added
(`aeron_udp_channel_is_multi_destination`, `media/aeron_udp_channel.h:147-151`):
a **manual** channel holds exactly the ones a client added and a destination
never times out; a **dynamic** one also learns them from the status messages
that arrive and drops one that has gone quiet. A subscription on the same shape
reads from each destination it was given, one socket each — the multi-destination
*receiver* — and both sides record the address their socket really bound in a
`rcv-local-sockaddr` / `snd-local-sockaddr` counter, which is the only way a
client learns the port a channel that named port zero was given
(`aeron_counter_local_sockaddr_indicator_allocate`, `aeron_position.c:276-306`).

An `aeron-spy:` subscription is served: it reads a **local** network
publication's log buffer with no socket of its own, and the client is sent an
ordinary image whose log file is that publication's own and whose source
identity is the IPC constant (`aeron_driver_conductor.c:4904-4916`), so a
client that reads images reads this one unchanged. A publication **counts**
such a reader when `aeron.spies.simulate.connection` is set — the `ssc`
parameter, by the driver's setting or the channel's — which is what lets a
stream whose only reader is a spy be live: `snd-pos` follows the furthest spy
and the producer's window opens from it
(`aeron_network_publication.c:612-636`, `:947-1009`). Acceptance is our client
on our driver and the reference's own client on our driver
(`tests/integration/spy_subscription.rs`, `tests/interop/spy_reference.rs`).

A **response channel** is served end to end: the `control-mode=response`
subscription, the `SEND_RESPONSE` bit a publication with a
`response-correlation-id=` puts in its SETUP, the RSP_SETUP an image sends back
once a response publication is created against it, and a response publication
that sends **only** to the address its SM came from. The frames are
`docs/protocol/wire-frames.md`'s.

Acceptance is two-sided and against the reference: the reference's own
`BasicPublisher` reaching a client of ours through a receive destination, and
two of the reference's own subscribers reached by our publication's
destinations (`tests/interop/multi_destination.rs`); and the reference's own
`response_client` and `response_server` running the whole handshake against a
driver this build wrote
(`tests/interop/response_channel_reference.rs`, `tests/interop/response_channel.rs`).

What follows is what this build does *differently*, and each row names where it
can be falsified.

| Divergence | Why, and where it shows |
|---|---|
| **`aeron:ipc` is refused as a receive destination** (`NOT_SUPPORTED`) rather than made into an IPC link | The reference builds one, keyed by the destination's registration id (`aeron_driver_conductor.c:5617`). Nothing shipped asks for it: `ReplayMerge` refuses to merge over IPC (`aeron-archive/.../client/ReplayMerge.java:124-129`). Refused by name rather than left unanswered, so a client is not left waiting out a timeout. `crates/driver/src/conductor.rs::a_receive_destination_is_triaged_by_the_prefix_it_names`. |
| **`aeron-spy:` is refused as a *send* destination only** — `INVALID_CHANNEL` | A spy names a publication's own log buffer rather than a place to put datagrams (`aeron_driver_conductor_validate_destination_uri_prefix`, `:392-407`). As a **receive** destination it is served, and has been since the spy link landed: it adds a local read of a publication to a `control-mode=manual` subscription (`aeron_driver_conductor_execute_add_receive_spy_destination`, `:5704-5806`). `crates/driver/src/udp_channel.rs::a_spy_is_refused_as_a_destination`, and the served half in `tests/integration/spy_subscription.rs::a_spy_can_be_added_to_a_subscription_as_a_source`. |
| **`REMOVE_DESTINATION_BY_ID` that names no publication answers nothing** | The reference calls that handler without taking its result (`aeron_driver_conductor.c:3188-3200`), so the `-1` for a publication it cannot find (`:5562-5578`) never reaches the `result < 0` that would send an `ON_ERROR` (`:3222-3225`). Every other command in the family is answered. The silence is reproduced rather than improved on, so a client that named one waits out its own deadline. `crates/driver/src/conductor.rs::a_remove_destination_by_id_that_finds_nothing_answers_nothing`, and on the caller's side `tests/integration/client_round_trip.rs::removing_a_destination_by_id_waits_for_an_answer_that_never_comes`. |
| **A gap's NAK delay is fixed** | The reference picks a *feedback* generator per image — a multicast-tuned one when the image has group semantics, a unicast one otherwise, or a static one when the channel names `nak-delay=` — and re-arms it from the RTT measurements its RTTMs carry (`aeron_publication_image.c:85-115`). This build uses unicast's fixed delays for every image and never adjusts them. Timing, not bytes; `crates/driver/src/loss_detector.rs`'s tests pin the delays used. |

## The client's view of the ring, and of a message

Four places where this build answers a question the reference answers
differently, all on the client's side. Each names the test that covers it.

- **A lap is counted, not fatal.** When the driver writes more events than the
  to-clients ring holds while a client is not reading, the events the client has
  not read are overwritten. The reference treats that as a system error and
  force-closes the client (`aeron-client/src/main/c/aeron_client_conductor.c:2728-2734`);
  deepmsg resynchronises forward, counts (`Client::laps`, `Client::discarded`)
  and passes the counts into a timeout's message, so a caller that sees a
  timeout can tell "the driver ignored me" from "my reply was overwritten".
  Covered by `crates/driver/src/conductor.rs::a_client_that_falls_a_ring_behind_counts_the_lap`
  and, for the receiver's own semantics, `crates/cnc/src/broadcast.rs::a_lap_resyncs_forwards_and_is_counted`.
- **One term per poll.** `Image::poll` fixes the partition at its entry and
  stops at that term's end, advancing into the next term on the *next* call —
  which is what `aeron_image_poll` does (`aeron-client/src/main/c/aeron_image.c:266-273`)
  and what a caller that throttles by counting fragments per poll depends on.
  Covered by `crates/driver/src/conductor.rs::a_poll_reads_one_term_at_a_time`.
- **An abandoned message is counted.** The fragment assembler drops a message
  whose fragments do not line up, as the reference does, and keeps a count of
  them (`FragmentAssembler::abandoned`) where the reference counts nothing
  (`aeron-client/src/main/c/aeron_fragment_assembler.c:170-181`). ADR-0003: a
  skipped input is a counted one. Covered by the assembler's own tests in
  `crates/client/src/fragment_assembler.rs`.
- **A message is assembled by session.** The assembler keeps one builder per
  session id, which is the Java client's rule
  (`aeron-client/src/main/java/io/aeron/FragmentAssembler.java:46`) and not
  the C file the rest of the module mirrors: the C assembler keeps a single
  builder, so two publications interleaving fragmented messages on one stream
  would reset each other's runs
  (`aeron-client/src/main/c/aeron_fragment_assembler.c:152-188`). deepmsg
  takes the stronger rule because a subscriber cannot tell the reference's
  behaviour there from a stream that loses messages. Covered by
  `crates/client/src/fragment_assembler.rs::two_sessions_are_assembled_apart`.
- **A delivered message is copied.** `Message::payload` points into the
  assembler's buffer, so every message is copied once; the reference hands out
  a pointer into the term for a message that arrived in one frame
  (`aeron_fragment_assembler.c:158-161`). A `&[u8]` over a term is a reference
  into memory a producer may be writing — in one process, in every test that
  publishes and subscribes at once — and `deepmsg-core`'s buffer API hands out
  no such slice for that reason. The copy is into a reused buffer, so a
  session's messages allocate nothing after the first. Covered by the same
  tests.

## Configuration names

The driver reads the reference's settings under both spellings: the property
name the reference documents, and deepmsg's prefix for the same name — so
`aeron.dir` and `deepmsg.dir` are the same setting, as a `-D` argument or in
the environment (`AERON_DIR` / `DEEPMSG_DIR`). Deepmsg's own spelling wins,
then its environment variable, then the reference's, which is the order a
one-off override wants. The suffixes are the reference's, including the three
environment names that are *not* the property name in capitals:
`aeron.to.conductor.buffer.length` is `AERON_CONDUCTOR_BUFFER_LENGTH`
(`aeronmd.h:97`), not `AERON_TO_CONDUCTOR_BUFFER_LENGTH`. A deployment's
existing configuration therefore works unchanged, which is the point.

The table is `crates/driver/src/config.rs`, and its tests pin every name; the
one divergence is recorded there too — the reference warns and clamps a value
it cannot parse, and this refuses.

One setting is **read and not acted on**, which is not the same thing as
being unknown:

- `aeron.send.to.status.poll.ratio` (`aeronmd.h:257`, the reference's
  sender idle-strategy duty-cycle ratio, `aeron_driver_sender.c:96`). This
  build's sender polls its control sockets on every pass, which is the
  strongest setting of the same knob; a value below `1` is refused and
  anything else is accepted and has no effect. Acting on it means the sender's
  idle strategy, which this build does not have.

`aeron.threading.mode` is the one setting of the reference's that this build
does not read at all: its four values choose between dedicated, shared,
shared-network and invoker threads
(`aeron_config_parse_threading_mode`, `aeron_driver_context.c:45-71`, applied
at `:447`), and this build has exactly one of them — dedicated, which is the
reference's default, so a deployment that leaves it alone is served the same
way. One that names another gets a driver that runs with the mode it named
having no effect: worth a line here rather than a silent difference, and the
honest place for the other three is a slice that wants them.

`aeron.counters.free.to.reuse.timeout` follows the same rule and is one more
name whose environment variable is not the property name in capitals
(`AERON_COUNTERS_FREE_TO_REUSE_TIMEOUT`, `aeronmd.h:525`).

One setting has a bound the reference does not: `aeron.timer.interval` is
capped at one hour. The reference parses up to `INT64_MAX` and then adds the
period to a nanosecond clock (`aeron_driver_conductor.c:3379`), which a period
near `INT64_MAX` turns into a wrapped deadline — a driver that looks healthy and
spins a core. deepmsg computes its deadlines with saturating arithmetic *and*
refuses the value, because a tier period above an hour is a typo rather than a
configuration.
