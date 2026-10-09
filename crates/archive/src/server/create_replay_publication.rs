//! Making the publication a replay writes into.
//!
//! Mirrors `io.aeron.archive.CreateReplayPublicationSession`
//! (`CreateReplayPublicationSession.java`). It exists because creating a
//! publication **is not instant**: `asyncAddExclusivePublication` answers with a
//! registration id, and the publication itself turns up a turn or more later.
//! The reference gives that its own session rather than making the replay wait,
//! and this is that session.
//!
//! There is no state enum here, and that is the reference's shape too (`:44-45`):
//! the state is the registration id — `None` means "not asked for yet" — and a
//! `done` flag.
//!
//! # The two ways it can fail are not one way
//!
//! `getExclusivePublication` (`:594`) throws `RegistrationException` while the
//! driver has not answered, and that is retried; **anything else** ends the
//! replay, gives back the slot it was holding and tells the client
//! (`:596-607`). A `bool` cannot carry that difference, which is why the archive's
//! `Publications::poll_exclusive_publication` answers with the whole
//! `AsyncAddPoll`.
//!
//! # The slot
//!
//! `ArchiveConductor.startReplay` counts the replay in **before** this session
//! runs (`AC:931-934`), so a failure here has to count it back out — that is
//! what the reference's `conductor.onReplayEnd()` at `:604` is doing. This build
//! reports [`Progress::Failed`] and leaves the counting to the conductor it
//! returns to, because this session has no conductor.

use deepmsg_client::client::{AsyncAddPoll, DEFAULT_TIMEOUT};

use crate::server::control_session::Publications;
use crate::server::replay_session::GENERIC;

/// A bounded replay's limit counter, as the conductor reads it.
///
/// `ArchiveConductor.startBoundedReplay` (`:996-1035`) resolves the request's
/// `limitCounterId` into a `Counter` handle and hands it to the replay, which
/// reads it every turn (`ReplaySession.notExtended`, `:544-578`). What travels
/// here is the three things that reading needs: which slot, which type, and
/// which registration must still own it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LimitCounter {
    /// The slot in the counters region.
    pub counter_id: i32,
    /// The type the slot must still carry: the driver reuses ids, and a slot
    /// that has become somebody else's counter is not this one
    /// (`aeron_counters_manager.c`).
    pub type_id: i32,
    /// The registration that must still own it (`:544-578`).
    pub registration_id: i64,
}

/// Everything one replay was asked for: `ArchiveConductor.startReplay`'s
/// parameter list (`:764-773`) as it travels from the control arm to the session
/// that carries it out.
///
/// It is one value rather than ten arguments because it is built once, by the
/// conductor, after every check has passed — and passed through this session
/// untouched, which is the only thing this session does with most of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Replay {
    /// The control session that asked, which is where the OK and any error go
    /// (`ControlSession.sendOkResponse`) — several turns after it asked.
    pub control_session_id: i64,
    /// The control request being answered.
    pub correlation_id: i64,
    /// The recording to replay.
    pub recording_id: i64,
    /// Where in the recording to start.
    pub replay_position: i64,
    /// How much of it to send, already resolved from the request's `length` —
    /// including the two sentinels (`-1` follows, `-2` counts to the stop).
    pub replay_length: i64,
    /// Where the recording began, which the segment geometry is measured from.
    pub start_position: i64,
    /// Where it ends now, which is what a read is bounded by.
    pub stop_position: i64,
    /// The recording's own frame geometry, from its catalog entry.
    pub segment_file_length: i32,
    /// See [`Replay::segment_file_length`].
    pub term_buffer_length: i32,
    /// See [`Replay::segment_file_length`].
    pub stream_id: i32,
    /// The channel to publish on, **already built** by the conductor with the
    /// recording's initial position on it (`AC:891-901`).
    pub replay_channel: String,
    /// The stream the replayed frames go out on.
    pub replay_stream_id: i32,
    /// The request's `fileIoMaxLength`. Not positive means "the whole buffer"
    /// (`AC:958-966`).
    pub file_io_max_length: i32,
    /// A `BoundedReplayRequest`'s limit counter, **resolved**. `None` for a
    /// plain `ReplayRequest`, whose limit is the live recording's own position
    /// (`AC:789-802`).
    pub limit: Option<LimitCounter>,
}

/// What one turn of publication-making did.
#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    /// The driver has not answered yet.
    Idle,
    /// The publication is in hand under this registration id, and the replay
    /// proper can start.
    Created { registration_id: i64 },
    /// It could not be made. The client is owed this, and the replay's slot has
    /// to be given back.
    Failed { code: i32, message: String },
}

/// One attempt at making a replay's publication.
pub struct CreateReplayPublicationSession {
    replay: Replay,
    registration_id: Option<i64>,
    done: bool,
}

impl CreateReplayPublicationSession {
    /// `new CreateReplayPublicationSession(...)` (`:47-81`).
    #[must_use]
    pub fn new(replay: Replay) -> Self {
        Self {
            replay,
            registration_id: None,
            done: false,
        }
    }

    /// Whether the session is over, one way or the other.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.done
    }

    /// What it was asked for, which is what the replay session is built from.
    #[must_use]
    pub const fn replay(&self) -> &Replay {
        &self.replay
    }

    /// `doWork` (`:126-183`).
    ///
    /// Ask once, then poll — the reference keeps the registration id in a field
    /// and only calls `asyncAddExclusivePublication` when it is `NULL_VALUE`
    /// (`:587-592`), so a driver that is slow is asked **once**, not once a turn.
    pub fn do_work<P: Publications + ?Sized>(&mut self, publications: &mut P) -> Progress {
        if self.done {
            return Progress::Idle;
        }

        let registration_id = match self.registration_id {
            Some(registration_id) => registration_id,
            None => {
                let asked = publications.async_add_exclusive_publication(
                    &self.replay.replay_channel,
                    self.replay.replay_stream_id,
                    DEFAULT_TIMEOUT,
                );

                match asked {
                    Ok(registration_id) => {
                        self.registration_id = Some(registration_id);

                        registration_id
                    }
                    Err(error) => {
                        return self.fail(format!("failed to create replay publication: {error}"));
                    }
                }
            }
        };

        match publications.poll_exclusive_publication(registration_id) {
            AsyncAddPoll::Ready => {
                self.done = true;

                Progress::Created { registration_id }
            }
            // `RESOURCE_TEMPORARILY_UNAVAILABLE`: idle and look again (`:599-602`).
            AsyncAddPoll::Awaiting => Progress::Idle,
            AsyncAddPoll::Failed(error) => {
                self.fail(format!("failed to create replay publication: {error}"))
            }
            AsyncAddPoll::Unknown => self.fail(
                "failed to create replay publication: registration is not this client's".to_owned(),
            ),
        }
    }

    /// Let the publication go, once a replay has taken it.
    ///
    /// **The reference has no such call, and the reason is worth stating.** It
    /// clears `publicationRegistrationId` *before* handing the publication to
    /// the conductor (`:161-165`), which is only safe because
    /// `ArchiveConductor.newReplaySession` (`:931-994`) cannot fail: it looks
    /// nothing up and opens no file, so a publication handed to it is always
    /// taken. This build's [`Sessions::open_replay`] **can** refuse — the
    /// recording may have gone from the catalog between the turn that checked it
    /// and this one, and `ReplaySession::new` rejects a geometry it cannot place
    /// a position in — and a registration cleared before a handover that then
    /// did not happen is stranded: nothing holds the id, so nothing will ever
    /// give the driver its publication back.
    ///
    /// [`Sessions::open_replay`]: crate::server::conductor::Sessions
    pub const fn hand_over(&mut self) {
        self.registration_id = None;
    }

    /// `close` (`:87-93`): a registration that never became a session is **given
    /// back**, and not revoked — there is no stream to tear down, only a
    /// registration to hand in.
    pub fn close<P: Publications + ?Sized>(&mut self, publications: &mut P) {
        if let Some(registration_id) = self.registration_id.take() {
            publications.async_remove_publication(registration_id, DEFAULT_TIMEOUT);
        }
    }

    /// `abort` (`:99-102`): over, with nothing to say.
    pub fn abort(&mut self) {
        self.done = true;
    }

    fn fail(&mut self, message: String) -> Progress {
        self.done = true;

        Progress::Failed {
            code: GENERIC,
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::time::Duration;

    use deepmsg_client::client::CommandError;
    use deepmsg_core::logbuffer::append::Appended;

    const REGISTRATION_ID: i64 = 0x5EED;

    fn replay() -> Replay {
        Replay {
            control_session_id: 0x1234,
            correlation_id: 30,
            recording_id: 1,
            replay_position: 0,
            replay_length: 128,
            start_position: 0,
            stop_position: 128,
            segment_file_length: 256 * 1024,
            term_buffer_length: 64 * 1024,
            stream_id: 1001,
            replay_channel: "aeron:udp?endpoint=localhost:6666".to_owned(),
            replay_stream_id: 66,
            file_io_max_length: 4096,
            limit: None,
        }
    }

    struct Fake {
        add_fails: bool,
        adds: usize,
        polls: VecDeque<AsyncAddPoll>,
        polled: usize,
        removed: Vec<i64>,
        channels: Vec<(String, i32)>,
    }

    impl Fake {
        fn new(polls: Vec<AsyncAddPoll>) -> Self {
            Self {
                add_fails: false,
                adds: 0,
                polls: polls.into(),
                polled: 0,
                removed: Vec::new(),
                channels: Vec::new(),
            }
        }

        /// The next poll answer; once the list runs out, `Awaiting` for ever.
        fn next_poll(&mut self) -> AsyncAddPoll {
            self.polled += 1;

            self.polls.pop_front().unwrap_or(AsyncAddPoll::Awaiting)
        }
    }

    impl Publications for Fake {
        fn async_add_exclusive_publication(
            &mut self,
            channel: &str,
            stream_id: i32,
            _timeout: Duration,
        ) -> Result<i64, CommandError> {
            self.adds += 1;
            self.channels.push((channel.to_owned(), stream_id));

            if self.add_fails {
                return Err(CommandError::Encoding);
            }

            Ok(REGISTRATION_ID)
        }

        fn poll_exclusive_publication(&mut self, _registration_id: i64) -> AsyncAddPoll {
            self.next_poll()
        }

        fn is_exclusive_connected(&self, _registration_id: i64) -> bool {
            unreachable!("the replay's publication is the replay session's business")
        }

        fn max_payload_length(&self, _registration_id: i64) -> usize {
            unreachable!("the replay's publication is the replay session's business")
        }

        fn offer_exclusive(&mut self, _registration_id: i64, _payload: &[u8]) -> Option<Appended> {
            unreachable!("the replay's publication is the replay session's business")
        }

        fn release_exclusive(&mut self, _registration_id: i64, _timeout: Duration) {
            unreachable!("a replay's publication is given back, not revoked")
        }

        fn async_remove_publication(&mut self, registration_id: i64, _timeout: Duration) {
            self.removed.push(registration_id);
        }
    }

    /// A slow driver is asked **once**: the registration id is kept and polled,
    /// not asked for again (`:587-592`).
    #[test]
    fn the_publication_is_asked_for_once_and_polled_until_it_arrives() {
        let mut publications = Fake::new(vec![
            AsyncAddPoll::Awaiting,
            AsyncAddPoll::Awaiting,
            AsyncAddPoll::Ready,
        ]);
        let mut session = CreateReplayPublicationSession::new(replay());

        assert_eq!(Progress::Idle, session.do_work(&mut publications));
        assert_eq!(Progress::Idle, session.do_work(&mut publications));
        assert_eq!(
            Progress::Created {
                registration_id: REGISTRATION_ID
            },
            session.do_work(&mut publications)
        );

        assert_eq!(
            1, publications.adds,
            "asked for once, however slow the driver"
        );
        assert_eq!(3, publications.polled);
        assert!(session.is_done());

        // And the channel is the one the conductor built, on the replay stream.
        assert_eq!(
            vec![("aeron:udp?endpoint=localhost:6666".to_owned(), 66)],
            publications.channels
        );
    }

    /// The refused case is **fatal**, not a retry: the reference sends the
    /// client an error and gives the slot back (`:596-607`).
    #[test]
    fn a_refused_publication_ends_the_replay_with_an_error() {
        let mut publications = Fake::new(vec![AsyncAddPoll::Failed(CommandError::Encoding)]);
        let mut session = CreateReplayPublicationSession::new(replay());

        match session.do_work(&mut publications) {
            Progress::Failed { code, message } => {
                assert_eq!(GENERIC, code);
                assert!(
                    message.contains("failed to create replay publication"),
                    "{message}"
                );
            }
            other => panic!("expected a failure, got {other:?}"),
        }

        assert!(session.is_done());
        assert_eq!(1, publications.adds, "and it is not asked for again");
    }

    /// So is an add that cannot even be sent.
    #[test]
    fn an_add_that_cannot_be_sent_ends_the_replay_with_an_error() {
        let mut publications = Fake::new(vec![AsyncAddPoll::Ready]);
        publications.add_fails = true;
        let mut session = CreateReplayPublicationSession::new(replay());

        assert!(matches!(
            session.do_work(&mut publications),
            Progress::Failed { code, .. } if code == GENERIC
        ));
        assert_eq!(0, publications.polled, "there is nothing to poll");
    }

    /// A registration that never became a session is handed back.
    #[test]
    fn a_registration_that_never_became_a_session_is_given_back() {
        let mut publications = Fake::new(vec![AsyncAddPoll::Awaiting]);
        let mut session = CreateReplayPublicationSession::new(replay());

        assert_eq!(Progress::Idle, session.do_work(&mut publications));

        session.close(&mut publications);

        assert_eq!(vec![REGISTRATION_ID], publications.removed);
    }

    /// A publication that has been created but **not yet taken** is still this
    /// session's, and a close gives it back.
    ///
    /// This is why [`CreateReplayPublicationSession::hand_over`] exists at all:
    /// the reference clears its registration as soon as the publication appears
    /// (`:161`), which is safe only because what it hands the publication to
    /// cannot refuse. Here it can, and a registration cleared before a handover
    /// that then did not happen is one nobody holds — so no later turn will ever
    /// give the driver its publication, its term buffer or its counters back.
    #[test]
    fn a_publication_the_replay_has_not_taken_is_given_back() {
        let mut publications = Fake::new(vec![AsyncAddPoll::Ready]);
        let mut session = CreateReplayPublicationSession::new(replay());

        assert!(matches!(
            session.do_work(&mut publications),
            Progress::Created { .. }
        ));

        session.close(&mut publications);

        assert_eq!(vec![REGISTRATION_ID], publications.removed);
    }

    /// One that **has** been handed over is not: it belongs to the replay.
    #[test]
    fn a_registration_that_became_a_session_is_not_taken_back() {
        let mut publications = Fake::new(vec![AsyncAddPoll::Ready]);
        let mut session = CreateReplayPublicationSession::new(replay());

        assert!(matches!(
            session.do_work(&mut publications),
            Progress::Created { .. }
        ));
        session.hand_over();
        session.close(&mut publications);

        assert!(publications.removed.is_empty(), "the replay owns it now");
    }

    /// An aborted session says nothing and gives nothing back on its own; the
    /// conductor's own close is what hands the registration in.
    #[test]
    fn an_abort_ends_it_without_a_word() {
        let mut publications = Fake::new(vec![AsyncAddPoll::Awaiting]);
        let mut session = CreateReplayPublicationSession::new(replay());

        assert_eq!(Progress::Idle, session.do_work(&mut publications));
        session.abort();

        assert!(session.is_done());
        assert_eq!(Progress::Idle, session.do_work(&mut publications));
    }
}
