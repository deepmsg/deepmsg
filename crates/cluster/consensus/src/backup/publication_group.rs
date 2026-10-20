//! `PublicationGroup` (`PublicationGroup.java`): one rotating publication over
//! a cluster's consensus endpoints.
//!
//! A backup does not know which member leads, and the answer it is waiting for
//! may never come from the member it asked. The reference's answer is to keep
//! **one** exclusive publication alive at a time, aimed at one consensus
//! endpoint, and to move to the next endpoint when the query times out. A
//! member that answers with a redirect is also closed and *excluded* for the
//! round, so the backup does not ask it again until the group is reshuffled.
//!
//! The endpoints are shuffled once when the agent starts, because every backup
//! in a deployment would otherwise ask the same member first.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_core::uri::ChannelUri;

use crate::error::ClusterError;

/// How long an add may take. The reference's adds are synchronous too, and the
/// agent's own turn is the only thing that would block.
pub const COMMAND_TIMEOUT: Duration = Duration::from_secs(10);

/// `CommonContext.ENDPOINT_PARAM_NAME`.
const ENDPOINT_PARAM_NAME: &str = "endpoint";

/// The rotating publications (`PublicationGroup.java:27-42`).
pub struct PublicationGroup {
    /// The consensus endpoints, already shuffled.
    endpoints: Vec<String>,
    /// The channel template the endpoint is laid over (`ctx.consensusChannel`).
    channel_template: String,
    stream_id: i32,
    /// Where the group is in the list (`cursor`, `:34`).
    cursor: usize,
    /// A cursor `next()` must skip — the member that redirected us
    /// (`excludedPublicationCursorValue`, `:35`).
    excluded: Option<usize>,
    /// The live publication's registration id, if one is current.
    current: Option<i64>,
}

impl PublicationGroup {
    /// A group over `endpoints`, before [`Self::shuffle`].
    #[must_use]
    pub fn new(endpoints: Vec<String>, channel_template: &str, stream_id: i32) -> Self {
        Self {
            endpoints,
            channel_template: channel_template.to_owned(),
            stream_id,
            // The reference starts at 0 and `next()` moves before it looks
            // (`:61-66`), so the first publication is for endpoint 1 unless the
            // group was reshuffled — which `shuffle` is careful to clear.
            cursor: 0,
            excluded: None,
            current: None,
        }
    }

    /// The endpoints this group can reach.
    #[must_use]
    pub fn endpoints(&self) -> &[String] {
        &self.endpoints
    }

    /// Where the group is.
    #[must_use]
    pub const fn cursor(&self) -> usize {
        self.cursor
    }

    /// The current publication's registration id, if any.
    #[must_use]
    pub const fn current(&self) -> Option<i64> {
        self.current
    }

    /// Whether the current publication is connected (`:120-123`).
    #[must_use]
    pub fn is_connected(&self, client: &Client) -> bool {
        self.current
            .and_then(|registration_id| client.exclusive_publication(registration_id))
            .and_then(deepmsg_client::publication::ExclusivePublication::is_connected)
            .unwrap_or(false)
    }

    /// Move to the next endpoint and open a publication there (`:48-59`).
    ///
    /// The current publication is closed first, which is what keeps a backup
    /// from leaving a publication behind on every timeout.
    ///
    /// # Errors
    ///
    /// The driver refusing the add, or not answering it.
    pub fn next(&mut self, client: &mut Client) -> Result<i64, ClusterError> {
        let cursor = self.next_cursor();
        self.close(client);

        let mut channel_uri = ChannelUri::parse(&self.channel_template).map_err(|error| {
            ClusterError::Cluster(format!(
                "a consensus channel template does not parse: {}: {error}",
                self.channel_template
            ))
        })?;
        channel_uri.put(ENDPOINT_PARAM_NAME, self.endpoints[cursor].clone());

        let registration_id = client
            .add_exclusive_publication(&channel_uri.build(), self.stream_id, COMMAND_TIMEOUT)
            .map_err(|error| {
                ClusterError::Cluster(format!(
                    "could not add the backup's consensus publication: {error}"
                ))
            })?;

        self.cursor = cursor;
        self.current = Some(registration_id);
        Ok(registration_id)
    }

    /// The cursor `next()` moves to (`:61-72`): one on, never the excluded one.
    fn next_cursor(&self) -> usize {
        let mut cursor = self.cursor;
        loop {
            cursor += 1;
            if self.endpoints.len() <= cursor {
                cursor = 0;
            }
            if Some(cursor) != self.excluded {
                return cursor;
            }
        }
    }

    /// Close the current publication and exclude it for the next round
    /// (`closeAndExcludeCurrent`, `:97-101`).
    pub fn close_and_exclude_current(&mut self, client: &mut Client) {
        self.excluded = Some(self.cursor);
        self.close(client);
    }

    /// Forget the exclusion (`clearExclusion`, `:92-95`).
    pub const fn clear_exclusion(&mut self) {
        self.excluded = None;
    }

    /// Shuffle the endpoints, closing the current publication first
    /// (`shuffle`, `:102-115`).
    pub fn shuffle(&mut self, client: &mut Client) {
        self.close(client);
        self.clear_exclusion();

        // Fisher-Yates, from the end down, as the reference does — with the
        // standard library's own randomness (`RandomState`), because this build
        // has no random-number crate and one 64-bit value per swap is enough.
        for i in (1..self.endpoints.len()).rev() {
            let j = (random_u64() % (i as u64 + 1)) as usize;
            self.endpoints.swap(i, j);
        }
    }

    /// Close the current publication (`:117-120`).
    pub fn close(&mut self, client: &mut Client) {
        if let Some(registration_id) = self.current.take() {
            let _ = client.remove_publication(registration_id, COMMAND_TIMEOUT);
        }
    }
}

/// A random `u64`, from `RandomState`'s per-call keys.
///
/// This is the standard library's only source of randomness that needs no
/// crate: `RandomState::new()` derives fresh keys on every call, and hashing
/// the empty input with them is a uniform enough value for a shuffle and for a
/// recording session id.
fn random_u64() -> u64 {
    RandomState::new().build_hasher().finish()
}

/// The recording session id a backup names on its recording and replay channels
/// (`BitUtil.generateRandomisedId()`, `ClusterBackupAgent.java:750`).
#[must_use]
pub fn randomised_session_id() -> i32 {
    // A session id is a positive int on the wire; the top bit is dropped rather
    // than allowed to make the channel text negative.
    (random_u64() >> 32) as u32 as i32 & i32::MAX
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_cursor_moves_on_and_skips_the_excluded_endpoint() {
        let mut group = PublicationGroup::new(
            vec!["a".to_owned(), "b".to_owned(), "c".to_owned()],
            "aeron:udp",
            108,
        );

        assert_eq!(0, group.cursor());
        assert_eq!(1, group.next_cursor());
        group.cursor = 1;
        assert_eq!(2, group.next_cursor());
        group.cursor = 2;
        assert_eq!(0, group.next_cursor(), "it wraps");

        // Excluding the next one makes the cursor skip it.
        group.cursor = 0;
        group.excluded = Some(1);
        assert_eq!(2, group.next_cursor());
    }

    #[test]
    fn a_session_id_is_positive() {
        for _ in 0..64 {
            assert!(randomised_session_id() >= 0);
        }
    }
}
