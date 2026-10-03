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

It is also the one divergence here with a **known cost**, and the cost is
worth stating plainly: `io.aeron.driver.SystemCountersTest::verifySystemCounters`
asserts every label starts with the Java `SystemCounterDescriptor`'s, and the
`Errors` descriptor carries the Java build's own `MediaDriverVersion.GIT_SHA` —
`664f58e705+guilty` for this checkout, dirty marker included. The reference's C
driver passes that assertion because both halves were built from the same tree,
not because the string is a contract a driver can implement; satisfying it means
writing down an environment fact. The test therefore stops there for this
driver, and the labels after id 15 are covered by the golden comparison
below instead.

The other labels the reference suffixes at runtime — the driver's threading
mode, the resolver's name, the duty-cycle thresholds
(`aeron_driver_conductor.c:848-951`) — are *configuration*, not contract: the
reference's own text changes with its settings, and deepmsg's now follows its
own. All eight suffixes are built from the settings at allocation
(`crates/driver/src/system_counters.rs`): `: driverName=<name>` on 25, the
configured `aeron.threading.mode` on 26/28/30, `: threshold=<duration> <mode>`
on 27/29/31 out of the three `*.cycle.threshold` settings — the same numbers
those counters count against — and `: threshold=<duration>` on 33, which is the
one suffix the reference prints without a mode, because a resolver runs on the
native resource agent and has no duty cycle of its own to name. Counter 32
keeps its bare label, which is also what the reference does. Durations are
printed the reference's way: the largest unit that divides them exactly, `s`,
`ms`, `us`, and nanoseconds otherwise (`aeron_format_duration_ns`,
`util/aeron_parse_util.c:270-330`).
`io.aeron.driver.DutyCycleLabelFormatTest` is the oracle, over all four
threading modes.

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
- **A static counter is a type, not a flag.** `ADD_STATIC_COUNTER` (`0x0F`,
  `aeron-client/src/main/c/command/aeron_control_protocol.h:41`) is served the
  reference's way — allocated with an owner id of `NULL_VALUE`, answered with
  `ON_STATIC_COUNTER` (`0x0F0B`, `:56`), reused when the same type id and
  registration id are asked for again
  (`aeron-driver/src/main/c/aeron_driver_conductor.c:6255-6316`) — but what a
  *client* gets back is a different type. The reference tracks one as an
  `AERON_CLIENT_MANAGED_RESOURCE_TYPE_STATIC_COUNTER` and matches that type
  when a removal arrives
  (`aeron-client/src/main/c/aeron_client_conductor.c:3061-3070`), and its close
  for one is a no-op (`:1283-1287`); here `Client::add_static_counter` returns a
  `StaticCounter` and `Client::remove_counter` takes a `Counter`, so the removal
  that must never happen cannot be written. Same outcome, no runtime match.
  Covered by `crates/driver/src/conductor.rs::a_static_counter_belongs_to_the_driver_and_not_to_the_client`
  (which checks the counter is never announced as one of the client's) and
  `::a_static_counter_outlives_the_client_that_asked_for_it` (which kills the
  client and finds the counter still there).


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

The description after the code is chosen by its sign, and that half arrived
late: a **positive** code is an errno and is described by the OS's own
`strerror` text, not by the protocol table (`aeron_error.c:361-373`). Until
G3-8b nothing here recorded one, so the composition modelled only the table;
the sender-MTU refusal below is the first site that does. Both drivers are
driven through that fault and their entries compared, masked only for the
session id, by `tests/interop/mtu_fault_entry.rs`.

An appended line is a site and a message and nothing else — no code of its own
— and a failure that crosses several layers collects one per layer as it
unwinds. `deepmsg_cnc::error_log::ErrorReport` is that buffer as a value, since
this build has no thread-local and the buffer has to reach the top to be
finished. Both drivers are driven through a subscription whose port cannot be
bound — the longest such chain, five lines — and the entry *and* the message
the client is answered with are compared, masked only for the descriptor
number, by `tests/interop/bind_fault_entry.rs`.

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

What the reference's *agent thread* records on its own account is mostly not
here, and the recordings that are missing are named so the absence is a
decision rather than a gap. All of them are in
`aeron-driver/src/main/c/aeron_driver_native_resource_agent.c`:

- An agent command its switch does not know is
  `record(EINVAL, "unknown command")` (`:176-177`). This build's agent takes
  a typed enum, so the failure has no representation to report.
- Two more are allocation failures — a deque that would not grow
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

## The event log

`AERON_EVENT_LOG` and the three names beside it are **accepted and ignored**:
this driver writes no event log, and creates no file for one.

The reference has two mechanisms under that name and neither is a contract a
replacement driver can meet. Its **C** driver starts a log-reader thread when
the variable is set and writes events to stdout, or to the file
`AERON_EVENT_LOG_FILENAME` names, exiting outright if that file will not open
(`aeron-driver/src/main/c/agent/aeron_driver_agent.c:499-540`) — nothing in the
tree asserts on that output, and the only test that sets the variable
(`aeron-driver/src/test/c/aeron_name_resolver_test.cpp:1034`) merely turns it
on. Its **Java** driver writes into a ring buffer that a Java agent in the same
JVM reads (`driver/logging/DriverEventLogger.java:46`,
`logging/CollectingEventLogReaderAgent.java:76-78`), which is what
`io.aeron.driver.DriverLoggingSystemTest` asserts on — a test the harness
excludes from its `test` task and runs in one of its own that points at no
external driver (`build.gradle:1145`, `:1148-1156`).

So there is nothing to be compatible *with*, and refusing the names is not an
option either: `CTestMediaDriver` sets `AERON_EVENT_LOG` and
`AERON_EVENT_LOG_DISABLE` on every driver it starts
(`CTestMediaDriver.java:459-466`). Accepting them and doing nothing is the only
position left, and it is the one this driver takes.
`tests/integration/driver_process_contract.rs::the_event_log_names_are_accepted_and_make_no_log`
pins both halves: the driver serves with all four names set, stops cleanly,
keeps stderr empty, and — the half a test can forget — does **not** create the
file the filename name points at.

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
| **ATS and the timestamp-offset parameters are refused** rather than served | The reference serves them, and a driver that *ignored* them would serve a different channel: a timestamped image without timestamps, an ATS endpoint without encryption. Multicast endpoints, the subscription parameter `group=`, `gtag=` and now **`cc=`** were all on this list and are served: `group=` from G3-1, `gtag=` from G3-2, and `cc=` from G3-5, which carries the static window and CUBIC (`crates/driver/src/congestion_control.rs`, the reference's own `congestion_control_test` as the oracle). `cc=` is read where the reference reads it — when an *image* is built, off the endpoint's channel (`aeron_congestion_control.c:165-205`, called from `aeron_driver_conductor.c:6629`) — so a name this build cannot serve is a subscription that exists and never reads, which is what the reference does with it too. `crates/driver/src/publication_params.rs::a_congestion_control_is_a_subscriptions_to_name_and_the_images_to_read`, and the round trip end to end in `tests/integration/congestion_control.rs`. |
(`crates/driver/src/publication_params.rs::a_congestion_control_this_driver_does_not_carry_is_refused`, `crates/driver/src/conductor.rs::a_channel_parameter_this_driver_cannot_serve_is_answered_rather_than_ignored`). Response channels were on this list and are served now — see the destination-plane section below. `crates/driver/src/udp_channel.rs::a_group_tag_is_a_parameter_like_any_other`, `::the_unsupported_parameters_are_named_rather_than_dropped`. |
| **The two flow-control supplier settings name a strategy, not a symbol to load** | `AERON_MULTICAST_FLOWCONTROL_SUPPLIER` and `AERON_UNICAST_FLOWCONTROL_SUPPLIER` are read and the four names the reference's own table holds are served, **under both the symbol and the short name each entry carries** — `aeron_unicast_flow_control_strategy_supplier`/`unicast_max`, `aeron_max_multicast_flow_control_strategy_supplier`/`multicast_max`, `aeron_min_flow_control_strategy_supplier`/`multicast_min`, `aeron_tagged_flow_control_strategy_supplier`/`multicast_tagged` (`aeron_flow_control.c:43-64`, whose scan takes either, and `aeronmd.h:286-289` for the short ones). The reference `dlsym`s the name, so a *custom* supplier compiles into a driver that loads it; this build has no dynamic loading (ADR-0002) and **refuses** a name it does not know, which is also what the reference does for one its table does not hold (its context init fails and the driver does not start). `crates/driver/src/config.rs::a_flow_control_supplier_is_named_by_its_the_references_symbol`, `crates/driver/src/flowcontrol.rs::Supplier::from_name`. |
| **A reader is told an image is gone as it leaves DRAINING for LINGER — except when the image was revoked** | The normal path is the reference's: `aeron_driver_conductor_image_transition_to_linger` tells every linked subscription the image is unavailable, and unlinks as it tells (`aeron_driver_conductor.c:1642-1675`), so a reader stops polling a stream that has stopped rather than a linger window later. It was not always: this build used to announce only at release, and `NameReResolutionTest.shouldReResolveMdcDynamicControlOnNotConnected` is what found it — its `while (subscription.isConnected())` after a publication closes waited out the whole linger window and hit the test's own deadline. The two places announce the same message (`crates/driver/src/conductor.rs::announce_image_unavailable`), and the transition is carried from the receiver thread as `ReceiverEvent::ImageLingering` because this build's image state machine runs there rather than on the conductor (`crates/driver/src/publication_image.rs::take_linger_notice`). A **revoked** image is the exception, and stays this build's own divergence: it reaches LINGER without a linger to announce on — its heartbeats already said `REVOKED` and it walks to DONE without waiting (`crates/driver/src/publication_image.rs::a_revoked_image_walks_to_done_without_waiting`) — so it is announced when it is released. The reader sees the same two things, an unavailable image whose `Image.isPublicationRevoked` is true; what differs for a revoked one is that it is told a pass or two later and that the log buffer is freed without a linger. `PublicationRevokeTest`. |
| **A multicast channel is served without `IP_MULTICAST_LOOP` ever being touched and without a leave-group call** | Both are what the reference does as well — they were looked for and are not there: `IP_MULTICAST_LOOP` appears **nowhere** in the tree, and a group is left by closing the socket rather than by `IP_DROP_MEMBERSHIP` (`aeron-driver/src/main/c/media/aeron_udp_channel_transport.c:394-409`). The kernel's own default of on is what both builds send with, and a group is left when its socket goes. `crates/driver/src/media/udp_transport.rs::two_subscribers_to_one_group_both_hear_what_is_sent_to_it`. |
| **The two `AERON_LOSS_DETECTOR_NAK_*` names multicast is usually described by are not implemented** | They are **dead macros** in 1.53.2: `AERON_LOSS_DETECTOR_NAK_UNICAST_DELAY_NS` and `AERON_LOSS_DETECTOR_NAK_MULTICAST_MAX_BACKOFF_NS` are definitions with no reader anywhere in the tree (`aeron-driver/src/main/c/aeron_loss_detector.h:61`, `:76`), and both say 60 — sixty milliseconds and sixty million. What a driver actually reads is the context's own pair, where the backoff is **ten** milliseconds and the unicast delay is **one** (`aeron-driver/src/main/c/aeron_driver_context.c:220-223`). This build's constant is that live value, not the macro's: `crates/driver/src/loss_detector.rs::a_channels_nak_delay_becomes_a_static_detector_at_the_ratio` pins the unicast pair, and `::a_groups_delays_are_drawn_and_not_fixed` the multicast scale. |
| **A `ttl=0` and a channel that named no `ttl` at all are one value** | `aeron_uri_multicast_ttl` answers zero for both (`aeron-client/src/main/c/uri/aeron_uri.c:323-334`) and an endpoint reads zero as "take the driver's default" (`aeron-driver/src/main/c/media/aeron_send_channel_endpoint.c:129`), so a channel that writes `ttl=0` meaning "this datagram never leaves the host" gets the driver's hop limit instead. The reference does exactly this; the divergence is only that a reader would not expect it from a parameter that can be written. `crates/driver/src/udp_channel.rs::the_hop_limit_is_read_the_way_strtoull_reads_it`. |
| **`recvmmsg` is called without a timeout** | The reference passes a zero `timespec`, which the kernel reads as "return after the first datagram" — one syscall per datagram of a burst. This passes `NULL` on a non-blocking socket, which returns everything queued. Same datagrams, fewer syscalls (`aeron_udp_channel_transport.c:560`, `:577`). |
| **A frame is copied into a scratch buffer before it is sent** | The reference hands `sendmmsg` an iovec pointing into the mapped term. `deepmsg_core::buffer` refuses to mint a `&[u8]` over memory another thread is writing — a false immutability promise is licence for the optimiser to hoist loads — and a syscall taking `&[u8]` is exactly that borrow. `crates/driver/src/network_publication.rs`'s module note. |
| **A subscription's join position is the image's `rcv-pos`** | The reference takes the slowest *reader* when an image has several (`aeron_publication_image.h:376-396`), which needs the reader set — and that lives on the receiver thread that owns the image. `rcv-pos` is the position a reader may start at without seeing a hole, and for the one-reader case the two are the same number. `crates/driver/src/publication_images.rs::join_position`. |
| **A resolution can be held on purpose, by a setting the reference does not have** | `deepmsg.debug.resolver.delay.millis` holds every resolution on the native resource agent for that many milliseconds before it is answered. There is no reference counterpart, for the reason the loss-injection row above gives about its own property: what it reproduces is a nameserver that does not answer, and no setting on a driver can make one of those — while the property it exists to pin, that the conductor keeps publishing its heartbeat while a channel's names are being resolved, is otherwise only observable on a host whose resolver happens to be slow, which is a test that passes on the days it is not. Zero, the default, delays nothing. Covered by `tests/integration/heartbeat.rs::the_heartbeat_advances_while_a_channel_is_being_parsed`. |
| **A channel-status counter's key is zero-filled past the channel** | The reference `memcpy`s the channel into an uninitialized struct, so the key's tail is whatever was on its stack (`aeron_position.c:220-222`). The bytes here are the same for the same channel every time, which is what a key is for. |
| **Loss injection is configured by this build's own property, and a datagram it withholds is reported as sent** | Two differences in one seam. The reference's debug loss surface is eight `AERON_DEBUG_{SEND,RECEIVE}_{DATA,CONTROL}_LOSS_{RATE,SEED}` variables (`media/aeron_debug_channel_endpoint_configuration.h:22-29`) read by an installer the driver context never calls — only a C++ test does (`aeron-driver/src/test/c/media/aeron_test_loss_generators_test.cpp:467`, `media/aeron_debug_channel_endpoint_configuration.c:126-172`) — so a driver started as a process has no way in, and every interop test here starts one. This build reads `deepmsg.debug.send.data.loss.drop.every` instead: a count, not a rate, over outgoing datagrams, on the send endpoint's data slot only. The second difference is what a withheld call reports: the reference answers `0` (`media/aeron_send_channel_endpoint.c:391-403`), so its sender never advances `snd-pos` and the frames go out on a later pass — no gap, nothing retransmitted. Here the withheld datagram counts as handed over, which is the only way a gap, a NAK and a retransmission become observable on a wire that does not lose anything. Covered by `tests/interop/udp_transport.rs::a_withheld_frame_is_retransmitted_until_the_reference_subscriber_has_it`; the generator itself is `crates/driver/src/media/loss_generator.rs`. |
| **An image's `sparse` byte is its channel's, not the oldest matching subscription's** | Both of the metadata bytes an image copies from a subscription (`aeron_publication_image.c:280-281`) are read here off the channel the `SETUP` carried (`crates/driver/src/publication_images.rs::begin_create`). The reference reads `reliable` off the link being linked and `sparse` off the **oldest** subscription matching the image (`aeron_driver_conductor_is_oldest_subscription_sparse`, `aeron_driver_conductor.c:6715-6717`). The channel that created the image is one of those subscriptions, so the two agree until two subscriptions that name different `sparse` share one image — a byte nothing in this build reads, and the only one of the pair where they can differ. Covered by `crates/driver/src/publication_image.rs::an_image_records_whether_it_is_reliable_and_whether_its_buffer_is_sparse`. |
| **A clashing subscription is refused on `reliable` alone** | The reference refuses a subscription whose options disagree with one already reading the same endpoint and stream, and checks three of them — `reliable`, `rejoin` and `isResponse` (`aeron_driver_conductor_has_clashing_subscription`, `aeron_driver_conductor.c:307-361`). This build checks the first, because it is the one that changes what an image *does* rather than what it advertises; the other two are read by nothing here yet. Covered by `crates/driver/src/conductor.rs::two_subscriptions_that_disagree_about_reliability_cannot_share_a_channel`. |
| **A gap on an unreliable stream is filled, not asked for** | Not a divergence but the opposite — it is the reference's behaviour, absent until now: `reliable=false` was parsed and dropped, so a client that named it got a reliable stream. An image whose channel said `false` now takes the reference's zero delay generator, which it returns **before** reading `nak-delay=` (`aeron_publication_image.c:92-95`), and covers each hole with a padding frame instead of sending a NAK, counting `loss-gap-fills` (`:1053-1066`). The data in the hole is gone, which is what the parameter means. Covered by `tests/integration/unreliable_stream.rs`; the reader half of it is `crates/client/src/image.rs`'s `Step::Padding`, which used to end the term at a padding frame where the reference steps over it (`aeron_image.c:375-379`) — invisible until an image started leaving padding **mid-term**. |

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
| **`aeron-spy:` is refused as a *send* destination only** — `INVALID_CHANNEL` | A spy names a publication's own log buffer rather than a place to put datagrams (`aeron_driver_conductor_validate_destination_uri_prefix`, `:392-407`). As a **receive** destination it is served, and has been since the spy link landed: it adds a local read of a publication to a `control-mode=manual` subscription (`aeron_driver_conductor_execute_add_receive_spy_destination`, `:5704-5806`). `crates/driver/src/udp_channel.rs::a_spy_is_refused_as_a_destination`, and the served half in `tests/integration/spy_subscription.rs::a_spy_can_be_added_to_a_subscription_as_a_source`. |
| **`REMOVE_DESTINATION_BY_ID` that names no publication answers nothing** | The reference calls that handler without taking its result (`aeron_driver_conductor.c:3188-3200`), so the `-1` for a publication it cannot find (`:5562-5578`) never reaches the `result < 0` that would send an `ON_ERROR` (`:3222-3225`). Every other command in the family is answered. The silence is reproduced rather than improved on, so a client that named one waits out its own deadline. `crates/driver/src/conductor.rs::a_remove_destination_by_id_that_finds_nothing_answers_nothing`, and on the caller's side `tests/integration/client_round_trip.rs::removing_a_destination_by_id_waits_for_an_answer_that_never_comes`. |
| **A publication's tether cycle is noticed on the sender's pass, not the conductor's timer** | The reference runs `aeron_network_publication_check_untethered_subscriptions` from the conductor, on the same tier as everything else that runs on a timer (`aeron_network_publication.c:1277`, reached from `:1692`). Here it runs where the readers are — the publication's own set belongs to the sender — so a reader that has stopped is put aside at the first send pass after its deadline rather than at the next tick. The deadlines are absolute, so the same transitions happen in the same order; what differs is how long after a deadline a client hears about it (microseconds against up to `aeron.timer.interval`). `tests/integration/spy_subscription.rs::a_publication_puts_its_laggards_aside_and_wakes_the_one_that_rejoins`. |
| **The NAK delays are chosen but never re-armed; `nak-delay=` is, and so is a group's backoff** | The reference picks a *feedback* generator per image — a multicast-tuned one when the image has group semantics, a unicast one otherwise, or a static one when the channel names `nak-delay=` — and then re-arms it from the RTT measurements its RTTMs carry (`aeron_publication_image.c:85-118`). This build has all three of the generators and not the loop that adapts them: a group draws from the log-normal its backoff settings describe (`crates/driver/src/loss_detector.rs`, `MulticastBackoff`), a channel with a `nak-delay` gets that delay and its ratio, and everything else gets unicast's fixed pair. What no image here does is *move* with the network, because this build sends no RTTM and reads none. `crates/driver/src/loss_detector.rs::a_groups_delays_are_drawn_and_not_fixed`, `::a_channels_nak_delay_becomes_a_static_detector_at_the_ratio`, and `crates/driver/src/publication_image.rs::an_image_asks_for_gaps_at_the_delay_its_channel_named` for the wiring that makes a parameter reach the image at all. |

## The client's view of the ring, and of a message

Places where this build answers a question the reference answers differently,
all on the client's side. Each names the test that covers it.

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
- **`rejectImage` writes its correlation header, and waits.** The C client
  claims a command record and fills in everything after the header, leaving the
  client id and the correlation id as whatever the ring held there, and returns
  `0` rather than an id (`aeron-client/src/main/c/aeron_client_conductor.c:3627-3654`)
  — so its driver answers a correlation nobody sent and its caller has nothing
  to match. The Java client writes both and returns the correlation id
  (`DriverProxy.rejectImage`, `aeron-client/src/main/java/io/aeron/DriverProxy.java:510-532`),
  and this build follows it: `Client::reject_image` waits for the driver's
  `ON_OPERATION_SUCCEEDED`, so a rejection the driver refused — an id that names
  no image and no IPC publication — comes back as a `CommandError` instead of
  silence. The record is otherwise the C client's, including the NUL past the
  reason, because its arithmetic is the one that is never shorter than the
  driver's minimum. Covered by
  `crates/cnc/src/command.rs::a_reject_image_is_the_c_clients_bytes_and_not_the_java_ones`
  and `tests/integration/reject_image.rs`.
One thing in this area is **not** a divergence and is written down anyway,
because the correct value is one edit away: the source address of a publication
error from an **IPC** rejection is byte-reversed. The reference assigns
`INADDR_LOOPBACK` straight into `sin_addr.s_addr`, with no `htonl`
(`aeron-driver/src/main/c/aeron_ipc_publication.c:231-236`), so the four bytes
it copies out at `aeron_driver_conductor.c:2303` are the little-endian image of
`0x7f000001` — `1.0.0.127` to a reader, which is exactly what the reference's
own Java client reports (`PublicationErrorFrameFlyweight.sourceAddress`,
`aeron-client/src/main/java/io/aeron/command/PublicationErrorFrameFlyweight.java:286-317`).
Its **network** path is not affected: a `sockaddr_in` the kernel filled in holds
the octets in order. This build writes the reference's bytes and not the
correct ones, because the bytes are the contract. Held there by
`tests/interop/our_client_rejects_on_the_reference_driver.rs`, which is the test
that found it — it compares the two drivers' responses for one event, and the
only field that differed was this one.

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

`aeron.send.to.status.poll.ratio` is read **and acted on** (`aeronmd.h:257`,
the reference's sending-to-polling ratio, `aeron_driver_context.c:199` for its
six and `:806-811` for where it is read). The decision is the reference's
(`aeron_driver_sender.c:149-152`): a pass reads the control sockets when the
send sent nothing, or when `ratio` passes that did send have gone by, or when
half the status-message timeout has elapsed since the last read (`:97`), or
when a short send happened during the pass. The pass counter only advances on a
pass that **sent** something, because a pass that did not reads anyway — which
is what the reference's short-circuiting `||` does. The four conditions are one
unit test each —
`crates/driver/src/sender.rs::tests::the_control_read_waits_for_its_share_of_passes`,
`::the_read_timeout_asks_for_a_read_by_itself`, `::a_short_send_asks_for_a_read`
and `::a_pass_that_sent_nothing_reads_and_does_not_count` — and the flow
control the poll exists for — status messages, NAKs, error frames — is the
interop suite's, which runs against this sender on every CI run.

One divergence remains, and it is in the configuration rather than the sender:
the reference parses `1..=INT32_MAX` and then casts through a `uint8_t`, so
`256` arrives as *zero* reads between passes, and `0` is out of its own range
and falls back to the six with a warning. This refuses everything outside
`1..=255` (`crates/driver/src/config.rs`) rather than letting a value mean a
value it does not spell.

`aeron.threading.mode` is read, and all four of its values are served —
dedicated, shared-network, shared and invoker
(`aeron_config_parse_threading_mode`, `aeron_driver_context.c:45-71`, applied
at `:447`; the runner set each one builds is `aeron_driver.c:1003-1122`). The
shape is **`aeronmd`'s**, which is the process this driver stands in for: slot
0 — the conductor under the first two modes, all four pieces under the last
two (`:723-755`) — runs on the process's own thread, because `aeronmd` starts
its driver with `manual_main_loop` true (`aeronmd.c:153`) and drives it itself
(`:165-168`). So `INVOKER` here is a working driver rather than the `EINVAL`
`aeron_driver.c:1225-1230` is the other side of, and `SHARED` and `INVOKER`
run the same runner, which is what the reference's `switch` does with them
(`:1003-1022`). `aeron.thread.naming` gives the threads the reference's two
sets of names (`aeron_driver_context.h:37-48`), with the process's own thread
renamed only under `new` — also what `aeronmd` does (`:160-163`).
`tests/integration/driver_process_contract.rs::every_threading_mode_runs_the_references_threads`
pins each mode's thread set off `/proc`, and
`::the_thread_names_follow_aeron_thread_naming` the two namings.

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
