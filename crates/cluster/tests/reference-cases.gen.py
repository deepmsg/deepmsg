#!/usr/bin/env python3
# The generator behind reference-cases-{consensus,other}.tsv, kept so the
# table's numbers can be reproduced and audited instead of trusted.
#
# It reads the *reference* tree (not ours) and writes both TSVs:
#     python3 -I reference-cases.gen.py <aeron-root> <deepmsg-root>
# with <aeron-root> at 664f58e705 (tag 1.53.2). Never hand-edit the TSVs;
# re-run this and review the diff.
#
# It is deliberately dumb: `assert_surface` is decided by direct syntactic
# touch inside each method body (comments and string literals stripped first),
# with a short table of helper names that carry a surface by themselves
# (awaitLeader -> counter, awaitServiceMessageCount -> stub-service, ...).
# That is a rule anyone can re-check by eye, which is the point. Where the
# rule and your reading disagree, fix the rule here and regenerate.
"""Extract every @Test/@ParameterizedTest from the reference cluster test trees,
classify what its assertions actually touch, and emit the two P3-0b TSVs.

Usage: python3 -I reference-cases.gen.py <aeron-root> <deepmsg-root>
"""
import os
import re
import sys

AERON = sys.argv[1]
DEEPMSG = sys.argv[2]
SYS_DIR = os.path.join(AERON, "aeron-system-tests/src/test/java/io/aeron/cluster")
UNIT_DIR = os.path.join(AERON, "aeron-cluster/src/test/java")

TEST_ANNOT = re.compile(r"^\s*@(Test|ParameterizedTest)\b", re.MULTILINE)

# --- class -> (lane, card, class_level_disposition) -------------------------
# lane: A = consensus critical path, B = instruments/periphery (plan §6.2)
SYSTEM_CLASS = {
    "ClusterTest": ("A", None),                    # split below by single-node
    "ClusterBackupTest": ("B", "P3-9"),
    "ClusterToolTest": ("B", "P3-8"),
    "ServiceIpcIngressMessageTest": ("B", "P3-2c"),
    "SingleNodeTest": ("A", "P3-4f"),
    "ClusterNetworkPartitionTest": ("A", "P3-6c"),
    "MultiNodeTest": ("A", "P3-6c"),
    "ClusterUncommittedStateTest": ("A", "P3-6a"),
    "ClusterSessionReliabilityTest": ("B", "P3-3c"),
    "ClusterNetworkTopologyTest": ("A", "P3-6c"),
    "MultiModuleSharedDriverTest": ("A", "P3-6c"),
    "MultiClusteredServicesTest": ("B", "P3-2c"),
    "ClusterThreadNamingTest": ("B", "P3-8"),
    "AppointedLeaderTest": ("A", "P3-6c"),
    "TestClusterTest": ("B", "P3-2c"),
    "StartFromTruncatedRecordingLogTest": ("A", "P3-6b"),
    "StalledLeaderLogReplicationClusterTest": ("A", "P3-6b"),
    "RecoverAfterFailedCatchupClusterTest": ("A", "P3-6b"),
    "RacingCatchupClusterTest": ("A", "P3-6b"),
    "OffsetMillisecondClusterClockTest": ("A", "P3-4a"),
    "InitiateShutdownThenImmediatelyCloseLeaderTest": ("A", "P3-4e"),
    "FailedFirstElectionClusterTest": ("A", "P3-6c"),
}

UNIT_CLASS = {
    "ConsensusModuleContextTest": ("B", "P3-8"),
    "RecordingLogTest": ("A", "P3-1e"),
    "ClusterMemberTest": ("A", "P3-1d"),
    "ElectionTest": ("B", "P3-5b"),
    "PriorityHeapTimerServiceTest": ("A", "P3-4c"),
    "ConsensusModuleAgentTest": ("A", "P3-4f"),
    "ClusterMarkFileTest": ("A", "P3-1b"),
    "ClusteredServiceContainerContextTest": ("B", "P3-8"),
    "EgressAdapterTest": ("B", "P3-3b"),
    "ClusterNodeRestartTest": ("A", "P3-4f"),
    "ClusterBackupContextTest": ("B", "P3-9"),
    "PublicationGroupTest": ("A", "P3-6b"),
    "EgressPublisherTest": ("A", "P3-4b"),
    "AeronClusterAsyncConnectTest": ("B", "P3-3a"),
    "PendingServiceMessageTrackerTest": ("A", "P3-4c"),
    "NodeStateFileTest": ("A", "P3-1f"),
    "AeronClusterContextTest": ("B", "P3-3a"),
    "ConsensusModuleSnapshotTakerTest": ("A", "P3-4d"),
    "AeronClusterTest": ("B", "P3-3b"),
    "ClusterNodeTest": ("A", "P3-4f"),
    "RecordingReplicationTest": ("A", "P3-6b"),
    "AuthenticationTest": ("A", "P3-4b"),
    "ServiceProxyTest": ("A", "P3-4c"),
    "ClusteredServiceAgentTest": ("B", "P3-2a"),
    "ClusterWithNoServicesTest": ("A", "P3-4f"),
    "ClusterBackupAgentTest": ("B", "P3-9"),
    # Snapshot replication is the archive-to-archive leg of a follower's
    # catch-up, which is P3-6b; P3-4d only takes and loads a local snapshot.
    "SnapshotReplicationTest": ("A", "P3-6b"),
    "SessionManagerTest": ("A", "P3-4b"),
    # LogSourceValidator is ClusterBackup's source-type check (P3-9).
    "LogSourceValidatorTest": ("A", "P3-9"),
    "ConsensusModuleContextCloseTests": ("B", "P3-8"),
    # ClusterTimerTest is three whole-cluster scenarios with a real service
    # container; P3-4c has no container to start, so they belong to the card
    # that ships one (P3-4f's LC-2 single node).
    "ClusterTimerTest": ("A", "P3-4f"),
    "ServiceSnapshotTakerTest": ("B", "P3-2b"),
    "EgressPollerTest": ("B", "P3-3a"),
    "ConsensusModuleConfigurationTest": ("B", "P3-8"),
    "SessionEventCodecCompatibilityTest": ("B", "P3-3b"),
    "RecordingLogValidatorTest": ("A", "P3-1e"),
    "NameResolutionClusterNodeTest": ("A", "P3-6c"),
    "IngressAdapterTest": ("A", "P3-4b"),
}

# classes whose whole purpose is out of P3 scope; they still get a lane/card
# slot so no row is ever `?` — `-` means "no card owns it, by decision".
UNIT_NA = {
    "StandbySnapshotReplicatorTest": "n/a:premium",
    "ClusterEventLoggerCborImplTest": "n/a:logging",
    "ClusterEventCodeTest": "n/a:logging",
    "CborClusterEventCodecTest": "n/a:logging",
    "ClusterLoggingAgentTest": "n/a:logging",
    "ClusterEventEnablementTest": "n/a:logging",
    "GenericLoggingTest": "n/a:logging",
}
for _c in UNIT_NA:
    UNIT_CLASS[_c] = ("-", "-")

# --- assertion-surface patterns -------------------------------------------
# Order fixes the output order. Each is (label, compiled regex over the body
# with comments and string literals stripped).
SURFACE = [
    ("java-injection", re.compile(
        r"TestConsensusModuleExtension|delaySessionMessageProcessing"
        r"|serviceSupplier|timerServiceSupplier|authenticationSupplier"
        r"|authorisationServiceSupplier|egressListener\s*\(|controlledEgressListener\s*\("
        r"|TestService\s*\[\s*\]")),
    ("stub-service", re.compile(
        r"\.messageCount\s*\(|\.timerCount\s*\(|\.service\s*\(|\.services\s*\("
        r"|\.wasSnapshotTaken\s*\(|\.wasSnapshotLoaded\s*\(|\.roleChangedTo\s*\("
        r"|awaitServices?MessageCount\s*\(|awaitServiceMessagePredicate\s*\(")),
    # A CnC counter read: reachable from an *external* node through its cnc.dat,
    # so it is portable to both our members and Java members.
    ("counter", re.compile(
        r"\.moduleState\s*\(|\.electionState\s*\(|\.electionCount\s*\("
        r"|\.commitPosition\s*\(|\.appendPosition\s*\("
        r"|\.logRecordingCounterId\s*\(|\.clusterMembership\s*\("
        r"|awaitSnapshotCount\s*\(|awaitLeader\s*\("
        r"|awaitElectionState\s*\(|awaitElectionClosed\s*\("
        r"|terminationsExpected\s*\(")),
    # The in-process CM object itself — only our own in-process nodes have it.
    ("cm-object", re.compile(
        r"\.consensusModule\s*\(|\.hasMemberTerminated\s*\(")),
    # Client-visible behaviour: what any implementation can be asked.
    ("client", re.compile(
        r"sendAndAwaitMessages\s*\(|sendMessages\s*\(|awaitResponseMessageCount\s*\("
        r"|asyncConnectClient\s*\(|connectClient\s*\(|reconnectClient\s*\("
        r"|pollUntilMessageSent\s*\(|awaitNewLeadershipEvent\s*\("
        r"|msgBuffer\s*\(|\.client\s*\(")),
]

# helper -> surface, when a test only calls the helper and never touches the
# underlying object. Applied by rewriting the helper name to a marker first.
HELPER_MARK = {
    "awaitServicesMessageCount": "stub-service",
    "awaitServiceMessageCount": "stub-service",
    "awaitServiceMessagePredicate": "stub-service",
    "awaitSnapshotCount": "counter",
    "awaitLeader": "counter",
}


def strip_comments_strings(text: str) -> str:
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == '/' and i + 1 < n and text[i + 1] == '/':
            while i < n and text[i] != '\n':
                i += 1
        elif c == '/' and i + 1 < n and text[i + 1] == '*':
            i += 2
            while i + 1 < n and not (text[i] == '*' and text[i + 1] == '/'):
                i += 1
            i += 2
        elif c == '"':
            i += 1
            while i < n:
                if text[i] == '\\':
                    i += 2
                    continue
                if text[i] == '"':
                    i += 1
                    break
                i += 1
        else:
            out.append(c)
            i += 1
    return ''.join(out)


def balanced(text: str, start: int, open_c: str, close_c: str) -> int:
    depth = 0
    i = start
    while i < len(text):
        if text[i] == open_c:
            depth += 1
        elif text[i] == close_c:
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def skip_annotation(text: str, i: int) -> int:
    """i points at '@'. Skip a possibly-parameterised annotation."""
    while i < len(text) and text[i] not in '(\n':
        i += 1
    if i < len(text) and text[i] == '(':
        j = balanced(text, i, '(', ')')
        return j + 1 if j >= 0 else i
    return i


def methods(path: str):
    """Yield (name, body_text, annotations_before) for every @Test method."""
    raw = open(path, encoding='utf-8', errors='replace').read()
    pos = 0
    while True:
        m = TEST_ANNOT.search(raw, pos)
        if not m:
            return
        i = m.end()
        # the matched annotation may carry its own parameters, e.g.
        # `@ParameterizedTest(name = "...")` — consume them first
        if i < len(raw) and raw[i] == '(':
            j = balanced(raw, i, '(', ')')
            if j > 0:
                i = j + 1
        # skip further annotations and whitespace
        while True:
            while i < len(raw) and raw[i] in ' \t\r\n':
                i += 1
            if i < len(raw) and raw[i] == '@':
                i = skip_annotation(raw, i)
                continue
            break
        anns = raw[m.start():i]
        # find the signature's '(' at top level, then the method name
        j = i
        while j < len(raw) and raw[j] != '(':
            if raw[j] == '{':          # malformed — bail
                break
            j += 1
        if j >= len(raw) or raw[j] != '(':
            pos = m.end()
            continue
        name_m = re.search(r'([A-Za-z_$][\w$]*)\s*$', raw[i:j])
        name = name_m.group(1) if name_m else '<unknown>'
        k = balanced(raw, j, '(', ')')
        if k < 0:
            pos = m.end()
            continue
        b = raw.find('{', k)
        if b < 0:
            pos = m.end()
            continue
        e = balanced(raw, b, '{', '}')
        if e < 0:
            pos = m.end()
            continue
        yield name, raw[b:e + 1], anns
        pos = e + 1


def class_flags(path: str) -> str:
    """@Disabled / @DisabledForJreRange applied to the class itself."""
    raw = open(path, encoding='utf-8', errors='replace').read()
    decl = re.search(r"^\s*(?:public\s+|final\s+|abstract\s+)*class\s+\w+", raw, re.MULTILINE)
    head = raw[:decl.start()] if decl else ""
    if "@DisabledForJreRange" in head:
        return "disabled:jre-range"
    if re.search(r"^\s*@Disabled\b", head, re.MULTILINE):
        reason = re.search(r'@Disabled\("([^"]*)"\)', head)
        return "disabled:" + (reason.group(1) if reason else "no-reason")
    return ""


def method_flags(anns: str) -> str:
    if "@DisabledForJreRange" in anns:
        return "disabled:jre-range"
    m = re.search(r'@Disabled\("([^"]*)"\)', anns)
    if m:
        return "disabled:" + m.group(1)
    if re.search(r"@Disabled\b", anns):
        return "disabled:no-reason"
    return ""


def classify(body: str) -> str:
    body = strip_comments_strings(body)
    hits = []
    for label, rx in SURFACE:
        if rx.search(body):
            hits.append(label)
    if not hits:
        for helper, label in HELPER_MARK.items():
            if re.search(r'\b' + helper + r'\s*\(', body) and label not in hits:
                hits.append(label)
    if not hits:
        return "client"
    order = {"java-injection": 0, "stub-service": 1, "counter": 2, "cm-object": 3, "client": 4}
    return ",".join(sorted(set(hits), key=lambda x: order[x]))


def java_injection_disposition(name: str, body: str) -> str:
    b = strip_comments_strings(body)
    if "delaySessionMessageProcessing" in b:
        return "n/a:java-injection"
    if "egressListener" in b or "controlledEgressListener" in b:
        # the listener lives on the *client* side, which is ours either way
        return "hook:egress-listener"
    if re.search(r"\b(timerServiceSupplier|authenticationSupplier"
                 r"|authorisationServiceSupplier|serviceSupplier)\b", b):
        return "hook:service-supplier"
    if "TestConsensusModuleExtension" in b:
        return "hook:cm-extension"
    return "n/a:java-injection"


def emit(kind: str, path: str, out_rows, cls_map, na_map):
    cls = os.path.basename(path)[:-5]
    cflags = class_flags(path)
    for name, body, anns in methods(path):
        lane, card = cls_map.get(cls, (None, None))
        flags = cflags or method_flags(anns)
        if kind == "unit":
            # The four-class taxonomy describes the *system* harness problem;
            # unit tests are ported by name against our own types.
            surface = "unit"
            disp = na_map.get(cls, "")
        else:
            surface = classify(body)
            disp = na_map.get(cls, "")
            lowered = name.lower()
            if not disp and ("standby" in lowered):
                disp = "n/a:premium"
            if not disp and "java-injection" in surface:
                disp = java_injection_disposition(name, body)
        if kind == "system" and cls == "ClusterTest":
            # single-member scenarios are the LC-2 single-node half (P3-4f);
            # everything else is the three-member suite (P3-6c). The reference
            # spells single-node as `.withStaticNodes(1)`, not a named helper.
            card = "P3-4f" if "withStaticNodes(1)" in body else "P3-6c"
        if kind == "system" and cls == "TestClusterTest":
            disp = "n/a:fixture-self-test"
        if lane is None:
            lane = "?"
        if card is None:
            card = "?"
        out_rows.append((kind, cls, name, lane, card, surface, flags, disp))


HEADER_COMMON = """\
# What the reference's cluster test suite runs, one row per case — the
# denominator P3-0b owes the rest of P3. Keys are the reference's own case
# names, read from the tree at 664f58e705 (tag 1.53.2).
#
# Why two files: P3 runs two agent lanes (plan §6.2). A lane owns consensus,
# B owns instruments and periphery. Each lane fills its own `ours` column, so
# each gets its own file — one shared file is a merge conflict every day.
#
# Why a table at all: the reference's cluster tests are a Java
# dependency-injection harness, not a black-box one (plan §2.3). LC-2/LC-3
# cannot reuse them, so the cases have to be re-expressed against our own
# fixture — and the only honest way to say how much of the reference we carry
# is to name every case and record what it actually asserts.
#
# Columns:
#   kind           system = aeron-system-tests/.../cluster (178 total)
#                  unit   = aeron-cluster/src/test/java      (396 total)
#   class          the reference's own class name
#   method         the reference's own method name
#   lane           A = consensus critical path, B = instruments/periphery
#                  `-` = no lane owns it, by decision (see disposition)
#   card           the P3 work package that ports it (plan §6.1)
#                  `-` = no card owns it, by decision
#   assert_surface what the case's assertions actually touch. System cases
#                  only; unit cases are `unit`, ported by name against our own
#                  types, with no harness problem.
#                    client        client-visible behaviour
#                    counter       a CnC counter read (role 201, state 200,
#                                  election 207, commit 203, append, snapshot
#                                  count 205). Readable from an *external* node
#                                  through its cnc.dat, so OUR members and
#                                  JAVA members both carry these.
#                    stub-service  the stub ClusteredService inside the test
#                                  JVM: messageCount, wasSnapshotTaken,
#                                  wasSnapshotLoaded, roleChangedTo,
#                                  awaitServiceMessageCount/Predicate.
#                                  Only when the node runs in the test process
#                                  — LC-2 in full, LC-3's Rust half (§7.2).
#                    cm-object     the ConsensusModule object itself
#                                  (consensusModule(), hasMemberTerminated()).
#                                  In-process only.
#                    java-injection Java code handed to the node:
#                                  serviceSupplier lambda, timer/auth/
#                                  authorisation suppliers, egress listeners,
#                                  ConsensusModuleExtension, the
#                                  delaySessionMessageProcessing static hook.
#                  Several may apply; separated by commas, most blocking first.
#   ref_flags      whether the reference itself runs the case. `disabled:*`
#                  rows are in the denominator and must NOT be read as green.
#   disposition    for cases not portable as-is: `n/a:<why>`, or
#                  `hook:<what>` when an equivalent hook in our binary is the
#                  plan. Empty = port as a normal case.
#   ours           the name of our test once it exists, or `n/a:<why>`.
#
# n/a vocabulary (closed; add a value deliberately, with a reason):
#   n/a:premium            Cluster Standby / Transition — out of scope (§1.2)
#   n/a:logging            the logging package is not ported (§2.1)
#   n/a:removed-upstream   dynamic join — deprecated upstream
#   n/a:java-injection     needs Java code we cannot hand a Rust node
#   n/a:fixture-self-test  a test *of* the reference fixture, not of a cluster
#
# ★ `ours` is filled by the OURS table below, which names our test before the
# reference row is written. A row with no entry is still empty, and that is
# still not a gate: P2 tried exactly this column in
# crates/archive/tests/reference-cases.tsv — `status` + `covered_by`, with a
# rule that a slice flips its rows when it lands a scenario. Four slices
# passed and not one row was flipped: "which of our tests owns which reference
# case" is a human judgement no gate can fail on, so P2 deleted the columns
# and let the acceptance run be the record. This file keeps `ours` because the
# P3 plan asks for it and because `assert_surface` is new and checkable — but
# **if it is still empty at P3-11, delete it rather than carry a promise
# nothing checks.** What this card's DoD actually gates is above: every system
# row classified, the counts stated below, and every java-injection row given
# a disposition.
#
# Regenerate with crates/cluster/tests/reference-cases.gen.py; re-run after a
# reference bump and review the diff. Do not hand-edit rows.
"""


# ★ P3-4b: the first rows with a named owner. A name here is the test that
# closes the row, and the words in parentheses say what the port lost — the
# column's reason to exist is that "green but testing less" is invisible in a
# red/green table.
OURS = {
    ("EgressPublisherTest", "sendEventShouldUseTryClaimIfEventIsSmall"):
        "send_event_should_use_try_claim_if_event_is_small",
    ("EgressPublisherTest", "sendEventShouldUseOfferWhenEventIsTooBig"):
        "send_event_should_use_offer_when_event_is_too_big",
    ("EgressPublisherTest", "shouldSendChallengeResponseUsingTryClaimIfSmall"):
        "should_send_challenge_response_using_try_claim_if_small",
    ("EgressPublisherTest", "shouldSendChallengeResponseUsingOfferIfLarge"):
        "should_send_challenge_response_using_offer_if_large",
    ("EgressPublisherTest", "shouldSendNewLeaderEventUsingTryClaimIfSmall"):
        "should_send_new_leader_event_using_try_claim_if_small",
    ("EgressPublisherTest", "shouldSendNewLeaderEventUsingOfferIfTooBig"):
        "should_send_new_leader_event_using_offer_if_too_big",
    ("EgressPublisherTest", "shouldSendAdminResponseUsingTryClaimIfSmall"):
        "should_send_admin_response_using_try_claim_if_small",
    ("EgressPublisherTest", "shouldSendAdminResponseUsingOfferIfTooBig"):
        "should_send_admin_response_using_offer_if_too_big",
    ("IngressAdapterTest", "shouldDelegateToConsensusModuleAgent"):
        ",".join([
            "should_delegate_unknown_schema_to_the_extension_hook",
            "should_delegate_session_message_header_to_on_ingress_message",
            "should_delegate_session_connect_request_to_on_session_connect",
            "should_delegate_session_close_request_to_on_session_close",
            "should_delegate_session_keep_alive_to_on_session_keep_alive",
            "should_delegate_challenge_response_to_on_ingress_challenge_response",
            "should_delegate_admin_request_to_on_admin_request",
        ]) + " (seven parameterised cases, one test each)",
    ("AuthenticationTest", "shouldAuthenticateOnConnectRequestWithEmptyCredentials"):
        "should_authenticate_and_reject_a_session_the_authenticator_asks_for "
        "(weakened: the session state, not the client's SessionEvent)",
    ("AuthenticationTest", "shouldAuthenticateOnConnectRequestWithCredentials"):
        "should_hand_an_accepted_connect_request_to_the_authenticator "
        "(weakened: the credentials reach the authenticator; the client's view "
        "is a_rejecting_authenticator_produces_an_authentication_rejected_event)",
    ("AuthenticationTest", "shouldAuthenticateOnChallengeResponse"):
        "should_route_a_challenge_response_to_the_authenticator_only_when_challenged "
        "(weakened: routing only, no challenge round trip)",
    ("AuthenticationTest", "shouldRejectOnConnectRequest"):
        "a_rejecting_authenticator_produces_an_authentication_rejected_event",
    ("AuthenticationTest", "shouldRejectOnChallengeResponse"):
        "should_authenticate_and_reject_a_session_the_authenticator_asks_for "
        "(weakened: the rejection state, no challenge round trip)",
    ("SessionManagerTest",
     "shouldRetryCreatingResponsePublicationWhenResourceTemporarilyUnavailable"):
        "the_resource_temporarily_unavailable_answer_reissues_the_add "
        "(weakened: the decision over the driver's error, not a driver)",
    # P3-4c.
    ("PriorityHeapTimerServiceTest", "throwsNullPointerExceptionIfTimerHandlerIsNull"):
        "the_constructor_requires_a_handler (type system: a Box<dyn TimerHandler> cannot be null)",
    ("PriorityHeapTimerServiceTest", "pollIsANoOpWhenNoTimersWhereScheduled"):
        "poll_is_a_no_op_when_no_timers_where_scheduled",
    ("PriorityHeapTimerServiceTest", "pollIsANoOpWhenNoScheduledTimersAreExpired"):
        "poll_is_a_no_op_when_no_scheduled_timers_are_expired",
    ("PriorityHeapTimerServiceTest", "pollShouldNotExpireTimerIfHandlerReturnsFalse"):
        "poll_should_not_expire_timer_if_handler_returns_false",
    ("PriorityHeapTimerServiceTest", "pollShouldExpireSingleTimer"):
        "poll_should_expire_single_timer",
    ("PriorityHeapTimerServiceTest", "pollShouldRemovedExpiredTimers"):
        "poll_should_removed_expired_timers",
    ("PriorityHeapTimerServiceTest", "pollShouldExpireTimersInOrderOfDeadlineButWithinTheDeadlineOrderIsUndefined"):
        "poll_should_expire_timers_in_order_of_deadline_but_within_the_deadline_order_is_undefined",
    ("PriorityHeapTimerServiceTest", "cancelTimerByCorrelationIdIsANoOpIfNoTimersRegistered"):
        "cancel_timer_by_correlation_id_is_a_no_op_if_no_timers_registered",
    ("PriorityHeapTimerServiceTest", "cancelTimerByCorrelationIdReturnsFalseForUnknownCorrelationId"):
        "cancel_timer_by_correlation_id_returns_false_for_unknown_correlation_id",
    ("PriorityHeapTimerServiceTest", "cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheTimer"):
        "cancel_timer_by_correlation_id_returns_true_after_cancelling_the_timer",
    ("PriorityHeapTimerServiceTest", "cancelTimerByCorrelationIdReturnsTrueAfterCancellingTheLastTimer"):
        "cancel_timer_by_correlation_id_returns_true_after_cancelling_the_last_timer",
    ("PriorityHeapTimerServiceTest", "cancelTimerByCorrelationIdAfterPoll"):
        "cancel_timer_by_correlation_id_after_poll",
    ("PriorityHeapTimerServiceTest", "scheduleTimerForAnExistingCorrelationIdIsANoOpIfDeadlineDoesNotChange"):
        "schedule_timer_for_an_existing_correlation_id_is_a_no_op_if_deadline_does_not_change",
    ("PriorityHeapTimerServiceTest", "scheduleTimerForAnExistingCorrelationIdShouldShiftEntryUpWhenDeadlineIsDecreasing"):
        "schedule_timer_for_an_existing_correlation_id_should_shift_entry_up_when_deadline_is_decreasing",
    ("PriorityHeapTimerServiceTest", "scheduleTimerForAnExistingCorrelationIdShouldShiftEntryDownWhenDeadlineIsIncreasing"):
        "schedule_timer_for_an_existing_correlation_id_should_shift_entry_down_when_deadline_is_increasing",
    ("PriorityHeapTimerServiceTest", "pollShouldStopAfterPollLimitIsReached"):
        "poll_should_stop_after_poll_limit_is_reached",
    ("PriorityHeapTimerServiceTest", "snapshotProcessesAllScheduledTimers"):
        "snapshot_processes_all_scheduled_timers",
    ("PriorityHeapTimerServiceTest", "expireThanCancelTimer"):
        "expire_than_cancel_timer",
    ("PriorityHeapTimerServiceTest", "moveUpAnExistingTimerAndCancelAnotherOne"):
        "move_up_an_existing_timer_and_cancel_another_one",
    ("PriorityHeapTimerServiceTest", "moveDownAnExistingTimerAndCancelAnotherOne"):
        "move_down_an_existing_timer_and_cancel_another_one",
    ("PriorityHeapTimerServiceTest", "cancelExpiredTimerIsANoOp"):
        "cancel_expired_timer_is_a_no_op",
    ("PriorityHeapTimerServiceTest", "scheduleMustRetainOrderBetweenDeadlines"):
        "schedule_must_retain_order_between_deadlines",
    ("PriorityHeapTimerServiceTest", "shouldReuseExpiredEntriesFromAFreeList"):
        "should_reuse_expired_entries_from_a_free_list",
    ("PriorityHeapTimerServiceTest", "shouldReuseCanceledTimerEntriesFromAFreeList"):
        "should_reuse_canceled_timer_entries_from_a_free_list",
    ("PriorityHeapTimerServiceTest", "manyRandomOperations"):
        "many_random_operations",
    ("PendingServiceMessageTrackerTest", "snapshotEmpty"):
        "snapshot_empty",
    ("PendingServiceMessageTrackerTest", "snapshotAfterEnqueueBeforePollAndSweep"):
        "snapshot_after_enqueue_before_poll_and_sweep",
    ("PendingServiceMessageTrackerTest", "snapshotAfterEnqueueAndPollBeforeSweep"):
        "snapshot_after_enqueue_and_poll_before_sweep",
    ("PendingServiceMessageTrackerTest", "snapshotAfterEnqueuePollAndSweepForLeader"):
        "snapshot_after_enqueue_poll_and_sweep_for_leader",
    ("PendingServiceMessageTrackerTest", "snapshotAfterEnqueuePollAndSweepForFollower"):
        "snapshot_after_enqueue_poll_and_sweep_for_follower",
    ("PendingServiceMessageTrackerTest", "loadInvalid"):
        "load_invalid",
    ("PendingServiceMessageTrackerTest", "loadValid"):
        "load_valid",
    ("ServiceProxyTest", "shouldLogWarningIfClusterMemberResponseFails"):
        "should_log_warning_if_cluster_member_response_fails",
    ("ServiceProxyTest", "shouldSendClusterMemberResponse"):
        "should_send_cluster_member_response",
    ("ServiceProxyTest", "shouldLogWarningIfClusterMemberExtendedResponseFails"):
        "should_log_warning_if_cluster_member_extended_response_fails",
    ("ServiceProxyTest", "shouldSendClusterMemberExtendedResponse"):
        "should_send_cluster_member_extended_response",
    # P3-4d.
    ("ConsensusModuleSnapshotTakerTest", "snapshotConsensusModuleState"):
        "snapshot_consensus_module_state (weakened: the idle reset/idle "
        "interactions are a Mockito assertion with no counterpart here)",
    ("ConsensusModuleSnapshotTakerTest", "snapshotPendingServiceMessageTracker"):
        "snapshot_pending_service_message_tracker",
    ("ConsensusModuleSnapshotTakerTest",
     "snapshotPendingServiceMessageTrackerWithServiceMessagesMissedByFollower"):
        "snapshot_pending_service_message_tracker_with_service_messages_missed_by_follower",
    ("ConsensusModuleSnapshotTakerTest", "snapshotTimer"):
        "snapshot_timer",
    ("ConsensusModuleSnapshotTakerTest",
     "snapshotSessionShouldUseTryClaimIfDataFitsIntoMaxPayloadSize"):
        "snapshot_session_should_use_try_claim_if_data_fits_into_max_payload_size",
    ("ConsensusModuleSnapshotTakerTest",
     "snapshotSessionShouldUseOfferIfDataDoesNotFitIntoMaxPayloadSize"):
        "snapshot_session_should_use_offer_if_data_does_not_fit_into_max_payload_size",
}

# Rows that cannot be ported even though the class they live in can.
DISPOSITION_OVERRIDE = {
    ("SessionManagerTest",
     "shouldProcessPendingStandbySnapshotNotificationsAfterReachingCommitPosition"):
        "n/a:premium",
    ("SessionManagerTest",
     "shouldProcessPendingStandbySnapshotNotificationsAfterProcessingDelay"):
        "n/a:premium",
}


def write_tsv(path, rows, title, extra):
    rows = [list(r) for r in rows]
    for r in rows:
        override = DISPOSITION_OVERRIDE.get((r[1], r[2]))
        if override is not None:
            r[7] = override

    sys_counts = {}
    for r in rows:
        if r[0] != "system":
            continue
        for s in r[5].split(","):
            sys_counts[s] = sys_counts.get(s, 0) + 1
    n_sys = sum(1 for r in rows if r[0] == "system")
    n_unit = sum(1 for r in rows if r[0] == "unit")
    disps = {}
    for r in rows:
        if r[7]:
            disps[r[7]] = disps.get(r[7], 0) + 1
    flags = {}
    for r in rows:
        if r[6]:
            flags[r[6]] = flags.get(r[6], 0) + 1

    parts = [HEADER_COMMON, f"#\n# {title}\n#\n"]
    parts.append(f"# rows: {len(rows)}  (system {n_sys}, unit {n_unit})\n")
    parts.append("# system assert_surface, multi-label:\n")
    for k in ("client", "counter", "stub-service", "cm-object", "java-injection"):
        if k in sys_counts:
            parts.append(f"#   {k:<16} {sys_counts[k]}\n")
    if disps:
        parts.append("# dispositions:\n")
        for k, v in sorted(disps.items()):
            parts.append(f"#   {k:<28} {v}\n")
    if flags:
        parts.append("# reference-disabled (in the denominator, not green):\n")
        for k, v in sorted(flags.items()):
            parts.append(f"#   {k:<40} {v}\n")
    if extra:
        parts.append(extra)
    parts.append("#\n# " + "\t".join(
        ["kind", "class", "method", "lane", "card", "assert_surface",
         "ref_flags", "disposition", "ours"]) + "\n")
    with open(path, "w", encoding="utf-8") as fh:
        fh.writelines(parts)
        for r in rows:
            ours = OURS.get((r[1], r[2]), "")
            fh.write("\t".join(list(r) + [ours]) + "\n")


def main():
    system_rows, unit_rows = [], []
    for f in sorted(os.listdir(SYS_DIR)):
        if f.endswith(".java"):
            emit("system", os.path.join(SYS_DIR, f), system_rows, SYSTEM_CLASS, {})
    for root, _, files in os.walk(UNIT_DIR):
        for f in sorted(files):
            if f.endswith(".java"):
                emit("unit", os.path.join(root, f), unit_rows, UNIT_CLASS, UNIT_NA)

    rows = system_rows + unit_rows
    unmapped = sorted({(r[1], r[3], r[4]) for r in rows
                       if r[1] not in SYSTEM_CLASS and r[1] not in UNIT_CLASS and r[1] not in UNIT_NA})
    if unmapped:
        print("UNMAPPED CLASSES (need a lane/card):", file=sys.stderr)
        for u in unmapped:
            print("  ", u, file=sys.stderr)

    print(f"system={len(system_rows)} unit={len(unit_rows)} total={len(rows)}", file=sys.stderr)

    a = [r for r in rows if r[3] == "A"]
    b = [r for r in rows if r[3] != "A"]

    out = os.path.join(DEEPMSG, "crates/cluster/tests")
    os.makedirs(out, exist_ok=True)
    write_tsv(os.path.join(out, "reference-cases-consensus.tsv"), a,
              "This file is lane A: consensus — 0b/0c/1a-1f, 4a-4f, 6a-6c, 7, 11.",
              "# Lane A owns the critical path (plan §6.2).\n")
    write_tsv(os.path.join(out, "reference-cases-other.tsv"), b,
              "This file is lane B: instruments and periphery — 5a/5b, 0a, 0d, 2a-2c, 3a-3c, 8, 9, 10, 0e.",
              "# Lane B owns the harness, the container, the client and the tooling.\n")
    print(f"wrote {len(a)} + {len(b)} rows to {out}", file=sys.stderr)


if __name__ == "__main__":
    main()
