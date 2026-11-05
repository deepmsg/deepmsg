//! The controlled vocabulary: who is speaking, and what happened.
//!
//! Both are enums rather than strings. That is deliberate: a query such as
//! `event:role.change` and an alert on `event:log.dropped` are contracts with
//! the dashboards, and a free string would let a rename silently break them.
//! Adding a member is a code change with a test; the vocabulary is the list
//! below and nowhere else.
//!
//! The initial members are the ones the pipeline plan names. The event track
//! (L0b) grows this list as components start reporting; `LogDropped` is here
//! from the start because the writer thread emits it itself when its queue is
//! full, and an event that the logger emits must be in the logger's vocabulary.

use std::fmt;

/// Which part of the process is speaking.
///
/// The process, not the crate: a cluster node hosts a consensus module, a
/// service container and clients, and one log file carries all three. The
/// writer's filename uses the configured component; each event names its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Component {
    /// The media driver: conductor, sender, receiver, media.
    Driver,
    /// The archive: recording, replay, replication, catalog.
    Archive,
    /// The cluster track: consensus, container, client, operator tool.
    Cluster,
    /// A client of the driver.
    Client,
    /// An operator-facing tool.
    Tool,
}

impl Component {
    /// Every component, in declaration order. Used by the vocabulary tests and
    /// by the environment parser's error messages.
    pub const ALL: [Component; 5] = [
        Component::Driver,
        Component::Archive,
        Component::Cluster,
        Component::Client,
        Component::Tool,
    ];

    /// The lowercase name used in the environment, the filename and the log.
    pub const fn as_str(self) -> &'static str {
        match self {
            Component::Driver => "driver",
            Component::Archive => "archive",
            Component::Cluster => "cluster",
            Component::Client => "client",
            Component::Tool => "tool",
        }
    }

    /// Parse a component name, case-insensitively.
    ///
    /// Returns `None` for anything else, so a misspelled directive in
    /// `DEEPMSG_LOG_LEVEL` is an error rather than a silently ignored override.
    pub fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        Self::ALL
            .into_iter()
            .find(|component| value.eq_ignore_ascii_case(component.as_str()))
    }
}

impl fmt::Display for Component {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// What happened.
///
/// Names are lowercase, dot-separated, and stable: they are what LogsQL queries
/// and alerts match on. They describe the event, not the message text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    /// A long-running process started.
    DriverStart,
    /// A long-running process stopped; `reason` says why.
    DriverStop,
    /// The driver reaped a client that stopped speaking.
    ClientTimeout,
    /// A node's role changed (leader, follower, candidate).
    RoleChange,
    /// An election completed; the term is in the event's fields.
    ElectionComplete,
    /// A snapshot began.
    SnapshotBegin,
    /// A snapshot finished; the duration is in the event's fields.
    SnapshotEnd,
    /// An archive recording started.
    RecordingStart,
    /// An archive recording stopped.
    RecordingStop,
    /// An archive segment was rolled.
    SegmentRoll,
    /// A publication or subscription entered backpressure.
    BackpressureEnter,
    /// The log writer dropped records because its queue was full.
    LogDropped,
}

impl Event {
    /// Every event, in declaration order.
    pub const ALL: [Event; 12] = [
        Event::DriverStart,
        Event::DriverStop,
        Event::ClientTimeout,
        Event::RoleChange,
        Event::ElectionComplete,
        Event::SnapshotBegin,
        Event::SnapshotEnd,
        Event::RecordingStart,
        Event::RecordingStop,
        Event::SegmentRoll,
        Event::BackpressureEnter,
        Event::LogDropped,
    ];

    /// The stable name written to the log.
    pub const fn as_str(self) -> &'static str {
        match self {
            Event::DriverStart => "driver.start",
            Event::DriverStop => "driver.stop",
            Event::ClientTimeout => "client.timeout",
            Event::RoleChange => "role.change",
            Event::ElectionComplete => "election.complete",
            Event::SnapshotBegin => "snapshot.begin",
            Event::SnapshotEnd => "snapshot.end",
            Event::RecordingStart => "recording.start",
            Event::RecordingStop => "recording.stop",
            Event::SegmentRoll => "segment.roll",
            Event::BackpressureEnter => "backpressure.enter",
            Event::LogDropped => "log.dropped",
        }
    }
}

impl fmt::Display for Event {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::{Component, Event};

    #[test]
    fn component_names_are_the_environment_names() {
        for component in Component::ALL {
            assert_eq!(Some(component), Component::parse(component.as_str()));
        }
        assert_eq!(Some(Component::Driver), Component::parse("DRIVER"));
        assert_eq!(None, Component::parse("media-driver"));
    }

    #[test]
    fn event_names_are_stable() {
        assert_eq!("driver.start", Event::DriverStart.as_str());
        assert_eq!("driver.stop", Event::DriverStop.as_str());
        assert_eq!("client.timeout", Event::ClientTimeout.as_str());
        assert_eq!("role.change", Event::RoleChange.as_str());
        assert_eq!("election.complete", Event::ElectionComplete.as_str());
        assert_eq!("snapshot.begin", Event::SnapshotBegin.as_str());
        assert_eq!("snapshot.end", Event::SnapshotEnd.as_str());
        assert_eq!("recording.start", Event::RecordingStart.as_str());
        assert_eq!("recording.stop", Event::RecordingStop.as_str());
        assert_eq!("segment.roll", Event::SegmentRoll.as_str());
        assert_eq!("backpressure.enter", Event::BackpressureEnter.as_str());
        assert_eq!("log.dropped", Event::LogDropped.as_str());
    }
}
