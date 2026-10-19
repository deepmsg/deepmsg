//! The container's session table and its per-session egress publication.
//!
//! The reference keeps two structures over the same sessions: an `ArrayList`
//! whose order is the snapshot's order, and a `Long2ObjectHashMap` for lookup
//! (`CSA:1.5`). The list is kept **sorted by session id on insertion**
//! (`CSA:628-652`), which is the order the snapshot carries — fixed here at
//! P3-2a, before there was a snapshot to carry it.
//!
//! The Rust table keeps one `Vec` and a `Long2LongHashMap` from session id to
//! index, which is the same pair of facts with one allocation strategy: the vec
//! is the order, the map is the lookup, and removal rebuilds the indices after
//! the hole (sessions are removed rarely — one per closed client).

use std::time::Duration;

use deepmsg_client::client::Client;
use deepmsg_cluster_common::collections::Long2LongHashMap;

use crate::error::ContainerError;

/// How long a publication add/remove may take before the driver is called
/// silent. The reference registers asynchronously; see the plan's deviation 4.
pub const EGRESS_COMMAND_TIMEOUT: Duration = Duration::from_secs(1);

/// `ContainerClientSession` (`ContainerClientSession.java`), as the container
/// holds it.
pub struct ContainerClientSession {
    id: i64,
    response_stream_id: i32,
    response_channel: String,
    encoded_principal: Vec<u8>,
    publication_registration_id: Option<i64>,
    is_closing: bool,
}

impl ContainerClientSession {
    /// `new ContainerClientSession(...)` (`CSA:507-535`).
    pub fn new(
        id: i64,
        response_stream_id: i32,
        response_channel: String,
        encoded_principal: Vec<u8>,
    ) -> Self {
        Self {
            id,
            response_stream_id,
            response_channel,
            encoded_principal,
            publication_registration_id: None,
            is_closing: false,
        }
    }

    /// `ClientSession.id()`.
    pub const fn id(&self) -> i64 {
        self.id
    }

    /// `ClientSession.responseStreamId()`.
    pub const fn response_stream_id(&self) -> i32 {
        self.response_stream_id
    }

    /// `ClientSession.responseChannel()`.
    pub fn response_channel(&self) -> &str {
        &self.response_channel
    }

    /// `ClientSession.encodedPrincipal()`.
    pub fn encoded_principal(&self) -> &[u8] {
        &self.encoded_principal
    }

    /// `ClientSession.isClosing()`.
    pub const fn is_closing(&self) -> bool {
        self.is_closing
    }

    /// `ClientSession.MOCKED_OFFER` is for followers; this is the publication
    /// an egress offer would use, or `None` on a follower and for a session
    /// whose connect failed.
    pub const fn publication_registration_id(&self) -> Option<i64> {
        self.publication_registration_id
    }

    /// `connect(Aeron)` (`ContainerClientSession.java:101-115`).
    ///
    /// The publication is the client's; only its registration id is kept here
    /// (the reference keeps the object, which is the same fact in Java).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] when the driver refuses or never answers.
    pub fn connect(&mut self, client: &mut Client) -> Result<(), ContainerError> {
        if self.publication_registration_id.is_some() {
            return Ok(());
        }

        let registration_id = client.add_publication(
            &self.response_channel,
            self.response_stream_id,
            EGRESS_COMMAND_TIMEOUT,
        )?;
        self.publication_registration_id = Some(registration_id);
        Ok(())
    }

    /// `disconnect(ErrorHandler)` (`:127-131`).
    ///
    /// # Errors
    ///
    /// [`ContainerError::Cluster`] when the removal is refused.
    pub fn disconnect(&mut self, client: &mut Client) -> Result<(), ContainerError> {
        let Some(registration_id) = self.publication_registration_id.take() else {
            return Ok(());
        };
        client.remove_publication(registration_id, EGRESS_COMMAND_TIMEOUT)?;
        Ok(())
    }

    /// `markClosing()` (`:117-120`).
    pub fn mark_closing(&mut self) {
        self.is_closing = true;
    }

    /// `resetClosing()` (`:122-125`).
    pub fn reset_closing(&mut self) {
        self.is_closing = false;
    }
}

/// The session table: insertion order by id, lookup by id.
pub struct SessionTable {
    sessions: Vec<ContainerClientSession>,
    index: Long2LongHashMap,
}

impl Default for SessionTable {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionTable {
    /// An empty table.
    pub fn new() -> Self {
        Self {
            sessions: Vec::new(),
            // `-1` is no session: ids start at 0 in the reference's allocator.
            index: Long2LongHashMap::new(-1),
        }
    }

    /// How many sessions are open.
    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    /// Whether no session is open.
    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    /// A session by id.
    pub fn get(&self, id: i64) -> Option<&ContainerClientSession> {
        let index = self.index.get(id);
        usize::try_from(index)
            .ok()
            .and_then(|i| self.sessions.get(i))
    }

    /// A session by id, mutably.
    pub fn get_mut(&mut self, id: i64) -> Option<&mut ContainerClientSession> {
        let index = self.index.get(id);
        usize::try_from(index)
            .ok()
            .and_then(|i| self.sessions.get_mut(i))
    }

    /// The sessions in snapshot order (`CSA.clientSessions()`).
    pub fn iter(&self) -> impl Iterator<Item = &ContainerClientSession> {
        self.sessions.iter()
    }

    /// The sessions in snapshot order, mutably.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut ContainerClientSession> {
        self.sessions.iter_mut()
    }

    /// `addSession` (`CSA:628-652`): insert **after** the last session with a
    /// smaller id, which is the reference's backward scan.
    pub fn add(&mut self, session: ContainerClientSession) {
        let id = session.id();
        let position = self
            .sessions
            .iter()
            .rposition(|existing| existing.id() < id)
            .map_or(0, |i| i + 1);

        self.sessions.insert(position, session);
        self.reindex_from(position);
    }

    /// Remove a session by id. The reference removes from the map and then
    /// scans the list for the same id (`CSA:537-568`).
    pub fn remove(&mut self, id: i64) -> Option<ContainerClientSession> {
        let index = usize::try_from(self.index.get(id)).ok()?;
        if self.sessions.get(index)?.id() != id {
            return None;
        }

        let session = self.sessions.remove(index);
        self.reindex_from(index);
        Some(session)
    }

    fn reindex_from(&mut self, position: usize) {
        let start = position.min(self.sessions.len());
        for (offset, session) in self.sessions[start..].iter().enumerate() {
            #[allow(clippy::cast_possible_truncation)] // session counts are far below i64::MAX
            let index = (start + offset) as i64;
            self.index.put(session.id(), index);
        }
    }
}

impl std::fmt::Debug for SessionTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionTable")
            .field("len", &self.sessions.len())
            .finish()
    }
}

impl std::fmt::Debug for ContainerClientSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContainerClientSession")
            .field("id", &self.id)
            .field("response_stream_id", &self.response_stream_id)
            .field("response_channel", &self.response_channel)
            .field("publication", &self.publication_registration_id)
            .field("is_closing", &self.is_closing)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(id: i64) -> ContainerClientSession {
        ContainerClientSession::new(id, 102, "aeron:ipc".to_owned(), Vec::new())
    }

    /// `addSession`'s backwards scan (`CSA:628-652`): the list stays ordered by
    /// id however the opens arrive.
    #[test]
    fn sessions_are_inserted_in_id_order() {
        let mut table = SessionTable::new();
        for id in [3, 1, 2] {
            table.add(session(id));
        }

        let ids: Vec<i64> = table.iter().map(ContainerClientSession::id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        assert_eq!(table.len(), 3);
        assert_eq!(table.get(2).map(ContainerClientSession::id), Some(2));
        assert!(table.get(4).is_none());
    }

    /// Removing a session leaves the ones after it findable: the index is
    /// rebuilt from the hole.
    #[test]
    fn removing_a_session_reindexes_the_tail() {
        let mut table = SessionTable::new();
        for id in [1, 2, 3] {
            table.add(session(id));
        }

        let removed = table.remove(2).expect("session 2 was there");
        assert_eq!(removed.id(), 2);
        assert_eq!(table.get(1).map(ContainerClientSession::id), Some(1));
        assert_eq!(table.get(3).map(ContainerClientSession::id), Some(3));
        assert!(table.remove(2).is_none());

        table.add(session(2));
        let ids: Vec<i64> = table.iter().map(ContainerClientSession::id).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }
}
