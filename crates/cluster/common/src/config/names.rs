//! Every `aeron.cluster.*` name the reference reads, and what this build does
//! with each one.
//!
//! LC-6: **no name is silently ignored.** The table starts entirely `Pending`,
//! and each slice that touches a name replaces its row with one of
//! [`Disposition::Honoured`], [`Disposition::AcceptedNotHonoured`] or
//! [`Disposition::Refused`] — each carrying the reason. `Pending` at P3-11 is a
//! failure, which is what `tests/property_names.rs` exists to make visible.
//!
//! The set is not hand-written: it is the reference's own string literals, and
//! the test compares it against `tests/fixtures/property-names.txt` (the
//! `grep` output from 664f58e705) so a name cannot be dropped or invented.

/// What this build does with a property name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Nothing has been decided yet. Must be empty by `P3-11`.
    Pending,
    /// Read and behaves as the reference does.
    Honoured,
    /// Accepted without complaint, deliberately not implemented, with the
    /// reason. The cluster starts; the feature does not happen.
    AcceptedNotHonoured(&'static str),
    /// Startup fails naming the property. Used where silence would be a lie.
    Refused(&'static str),
}

impl Disposition {
    /// The word used in the table and in `ClusterTool describe`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Honoured => "honoured",
            Self::AcceptedNotHonoured(_) => "accepted-not-honoured",
            Self::Refused(_) => "refused",
        }
    }
}

// Generated from the reference at 664f58e705 by
//   grep -rhoE '"aeron\.cluster\.[A-Za-z0-9._-]+"' aeron-cluster/src/main/java | sort -u
// and then left as Pending. The set is pinned by tests/property_names.rs
// against tests/fixtures/property-names.txt; do not hand-edit.
pub const NAMES: &[(&str, Disposition)] = &[
    (
        "aeron.cluster.accept.standby.snapshots",
        Disposition::Pending,
    ),
    ("aeron.cluster.appointed.leader.id", Disposition::Honoured),
    (
        "aeron.cluster.authenticator.supplier",
        Disposition::Honoured,
    ),
    (
        "aeron.cluster.authorisation.service.supplier",
        Disposition::Honoured,
    ),
    ("aeron.cluster.backup.catchup.channel", Disposition::Pending),
    (
        "aeron.cluster.backup.catchup.endpoint",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.backup.cool.down.interval",
        Disposition::Pending,
    ),
    ("aeron.cluster.backup.interval", Disposition::Pending),
    (
        "aeron.cluster.backup.progress.timeout",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.backup.response.timeout",
        Disposition::Pending,
    ),
    ("aeron.cluster.backup.source.type", Disposition::Pending),
    ("aeron.cluster.client.name", Disposition::Pending),
    (
        "aeron.cluster.clock",
        Disposition::Refused("only the millisecond clock is implemented"),
    ),
    ("aeron.cluster.consensus.channel", Disposition::Honoured),
    (
        "aeron.cluster.consensus.control.enable",
        Disposition::Pending,
    ),
    ("aeron.cluster.consensus.endpoints", Disposition::Pending),
    (
        "aeron.cluster.consensus.module.agent.role.name",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.consensus.module.extension",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.consensus.module.stream.id",
        Disposition::Honoured,
    ),
    ("aeron.cluster.consensus.stream.id", Disposition::Honoured),
    ("aeron.cluster.control.channel", Disposition::Honoured),
    ("aeron.cluster.cycle.threshold", Disposition::Honoured),
    ("aeron.cluster.dir", Disposition::Honoured),
    ("aeron.cluster.egress.channel", Disposition::Honoured),
    ("aeron.cluster.egress.stream.id", Disposition::Honoured),
    (
        "aeron.cluster.election.status.interval",
        Disposition::Honoured,
    ),
    ("aeron.cluster.election.timeout", Disposition::Honoured),
    ("aeron.cluster.error.buffer.length", Disposition::Honoured),
    (
        "aeron.cluster.file.sync.level",
        Disposition::AcceptedNotHonoured("read and kept; the force() call sites are P3-4d"),
    ),
    (
        "aeron.cluster.follower.catchup.channel",
        Disposition::Pending,
    ),
    ("aeron.cluster.id", Disposition::Honoured),
    ("aeron.cluster.idle.strategy", Disposition::Honoured),
    ("aeron.cluster.ingress.channel", Disposition::Honoured),
    ("aeron.cluster.ingress.endpoints", Disposition::Pending),
    (
        "aeron.cluster.ingress.fragment.limit",
        Disposition::Honoured,
    ),
    ("aeron.cluster.ingress.ipc.allowed", Disposition::Honoured),
    ("aeron.cluster.ingress.stream.id", Disposition::Honoured),
    (
        "aeron.cluster.leader.archive.control.channel",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.leader.heartbeat.interval",
        Disposition::Honoured,
    ),
    (
        "aeron.cluster.leader.heartbeat.timeout",
        Disposition::Honoured,
    ),
    ("aeron.cluster.log.channel", Disposition::Honoured),
    ("aeron.cluster.log.control.enable", Disposition::Pending),
    ("aeron.cluster.log.fragment.limit", Disposition::Honoured),
    ("aeron.cluster.log.stream.id", Disposition::Honoured),
    ("aeron.cluster.mark.file.dir", Disposition::Honoured),
    ("aeron.cluster.max.sessions", Disposition::Honoured),
    ("aeron.cluster.member.endpoints", Disposition::Pending),
    ("aeron.cluster.member.id", Disposition::Honoured),
    ("aeron.cluster.members", Disposition::Honoured),
    ("aeron.cluster.message.timeout", Disposition::Pending),
    ("aeron.cluster.replay.channel", Disposition::Honoured),
    ("aeron.cluster.replay.stream.id", Disposition::Honoured),
    ("aeron.cluster.replication.channel", Disposition::Honoured),
    (
        "aeron.cluster.replication.progress.interval",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.replication.progress.timeout",
        Disposition::Pending,
    ),
    ("aeron.cluster.service.class.name", Disposition::Pending),
    ("aeron.cluster.service.count", Disposition::Honoured),
    (
        "aeron.cluster.service.cycle.threshold",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.service.delegating.error.handler",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.service.error.buffer.length",
        Disposition::Pending,
    ),
    ("aeron.cluster.service.id", Disposition::Pending),
    ("aeron.cluster.service.name", Disposition::Pending),
    ("aeron.cluster.service.responder", Disposition::Pending),
    (
        "aeron.cluster.service.snapshot.threshold",
        Disposition::Pending,
    ),
    ("aeron.cluster.service.stream.id", Disposition::Honoured),
    ("aeron.cluster.services.dir", Disposition::Pending),
    ("aeron.cluster.session.timeout", Disposition::Honoured),
    ("aeron.cluster.snapshot.channel", Disposition::Honoured),
    ("aeron.cluster.snapshot.stream.id", Disposition::Honoured),
    (
        "aeron.cluster.standby.snapshot.notification.processing.delay",
        Disposition::Pending,
    ),
    (
        "aeron.cluster.startup.canvass.timeout",
        Disposition::Honoured,
    ),
    ("aeron.cluster.termination.timeout", Disposition::Honoured),
    ("aeron.cluster.ticks.per.wheel", Disposition::Honoured),
    (
        "aeron.cluster.timer.service.supplier",
        Disposition::Honoured,
    ),
    ("aeron.cluster.tool.delay", Disposition::Pending),
    ("aeron.cluster.tool.replay.channel", Disposition::Pending),
    ("aeron.cluster.tool.replay.stream.id", Disposition::Pending),
    ("aeron.cluster.tool.timeout", Disposition::Pending),
    (
        "aeron.cluster.total.snapshot.threshold",
        Disposition::Honoured,
    ),
    ("aeron.cluster.wheel.tick.resolution", Disposition::Honoured),
];
// 80 names
