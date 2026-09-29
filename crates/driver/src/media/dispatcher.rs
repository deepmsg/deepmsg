//! Which streams a receive endpoint cares about, and which session of each.
//!
//! Mirrors `aeron-driver/src/main/c/aeron_data_packet_dispatcher.c`. Every data
//! packet that arrives is a question — *does anything here want this?* — and
//! this module is the answer, because the answer has to be cheap: a subscriber
//! to one stream must not pay for the packets of another, and a channel that
//! nobody reads must not create an image for each of them.
//!
//! # Two levels of "no"
//!
//! A stream is in the map only while a subscription wants it
//! ([`DataPacketDispatcher::add_subscription`]); a session within it is wanted
//! either because the subscription named *all* sessions (`is_all_sessions`, the
//! usual case — `aeron:udp?endpoint=…` with no `session-id=`) or because one
//! was named. On top of that there is a per-session **state** whose whole
//! purpose is to stop a driver doing the same work twice:
//!
//! | state | what it means | set by |
//! |---|---|---|
//! | `Unknown` | nothing known yet — the map's initial value | the map itself |
//! | `PendingSetup` | a status message asked the sender for a `SETUP`; wait for it | [`DataPacketDispatcher::elicit_setup_from_source`] |
//! | `InitInProgress` | the conductor has a create-image request | [`DataPacketDispatcher::begin_image_creation`] |
//! | `Active` | an image serves this session | [`DataPacketDispatcher::add_image`] |
//! | `CoolDown` | an image was removed; do not make another | [`DataPacketDispatcher::remove_image`] |
//! | `NoInterest` | a tombstone: this session is not subscribed, stop looking | `mark_no_interest` |
//!
//! The tombstone is the one worth stating out loud, because it is the difference
//! between a driver that ignores a stream and one that ignores it *once*: a
//! stream with a subscription but a session nobody named would otherwise be
//! re-examined for every packet of that session, for ever
//! (`:376-383`, "mark as no interest to prevent repeated hash lookups").

/// What is known about one session of a stream
/// (`AERON_DATA_PACKET_DISPATCHER_IMAGE_*`,
/// `aeron-driver/src/main/c/aeron_data_packet_dispatcher.h:26-30`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImageState {
    /// Nothing is known; the map's initial value (`-1` there).
    Unknown,
    /// An image exists and takes packets (`IMAGE_ACTIVE`).
    Active,
    /// A status message asked for a `SETUP`; waiting (`IMAGE_PENDING_SETUP_FRAME`).
    PendingSetup,
    /// The conductor is creating the image (`IMAGE_INIT_IN_PROGRESS`).
    InitInProgress,
    /// An image was removed: a new one is not wanted yet (`IMAGE_COOL_DOWN`).
    CoolDown,
    /// A tombstone — nothing here wants this session (`IMAGE_NO_INTEREST`).
    NoInterest,
}

/// What a dispatcher decided about one packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Interest {
    /// A session an image serves: the packet belongs to it.
    Image {
        /// The image's registration id.
        registration_id: i64,
    },
    /// Subscribed, but no image yet: ask the source for a `SETUP`
    /// (`elicit_setup_from_source`).
    ElicitSetup,
    /// Nothing wants it.
    None,
}

/// One stream's interest, keyed by stream id
/// (`aeron_data_packet_dispatcher_stream_interest_t`, `:44-58`).
#[derive(Clone, Debug, Default)]
struct StreamInterest {
    /// Whether a subscription named no session, so every session on this stream
    /// is wanted.
    is_all_sessions: bool,
    /// The sessions named explicitly, in insertion order.
    subscribed: Vec<i32>,
    /// What is known per session.
    state: Vec<(i32, ImageState)>,
    /// The image serving each session, by registration id.
    images: Vec<(i32, i64)>,
}

impl StreamInterest {
    /// Whether a session is wanted at all (`stream_interest_for_session`, `:112-118`).
    fn wants(&self, session_id: i32) -> bool {
        self.is_all_sessions || self.subscribed.contains(&session_id)
    }

    fn state_of(&self, session_id: i32) -> ImageState {
        self.state
            .iter()
            .find(|(id, _)| *id == session_id)
            .map_or(ImageState::Unknown, |(_, state)| *state)
    }

    fn set_state(&mut self, session_id: i32, state: ImageState) -> bool {
        match self.state.iter_mut().find(|(id, _)| *id == session_id) {
            Some(entry) => entry.1 = state,
            None => self.state.push((session_id, state)),
        }

        true
    }

    fn image(&self, session_id: i32) -> Option<i64> {
        self.images
            .iter()
            .find(|(id, _)| *id == session_id)
            .map(|(_, registration_id)| *registration_id)
    }
}

/// The streams a receive endpoint is reading
/// (`aeron_data_packet_dispatcher_t`, `:20-28`).
#[derive(Debug)]
pub struct DataPacketDispatcher {
    streams: Vec<(i32, StreamInterest)>,
    /// How many sessions one stream may have
    /// (`stream.session.limit`, `:44`), which the reference checks before it
    /// creates an image.
    stream_session_limit: usize,
}

impl DataPacketDispatcher {
    /// A dispatcher that will allow `stream_session_limit` sessions per stream.
    pub const fn new(stream_session_limit: usize) -> Self {
        Self {
            streams: Vec::new(),
            stream_session_limit,
        }
    }

    /// How many streams have interest.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Whether a stream has any interest at all.
    pub fn has_stream(&self, stream_id: i32) -> bool {
        self.streams.iter().any(|(id, _)| *id == stream_id)
    }

    /// The sessions of a stream, with their images — for the receiver's sweep
    /// over the images it owns.
    pub fn images_of(&self, stream_id: i32) -> Vec<(i32, i64)> {
        self.streams
            .iter()
            .find(|(id, _)| *id == stream_id)
            .map(|(_, interest)| interest.images.clone())
            .unwrap_or_default()
    }

    /// Every (stream, session, image) the dispatcher knows.
    pub fn images(&self) -> Vec<(i32, i32, i64)> {
        let mut out = Vec::new();

        for (stream_id, interest) in &self.streams {
            for (session_id, registration_id) in &interest.images {
                out.push((*stream_id, *session_id, *registration_id));
            }
        }

        out
    }

    /// A subscription arrived (`add_subscription`, `:175-204`).
    ///
    /// A stream that already exists and was *not* reading every session becomes
    /// one that is — and the tombstones it wrote are dropped, because they said
    /// "nobody wants this session", which is no longer true.
    pub fn add_subscription(&mut self, stream_id: i32) {
        match self.streams.iter_mut().find(|(id, _)| *id == stream_id) {
            Some((_, interest)) => {
                if !interest.is_all_sessions {
                    interest.is_all_sessions = true;
                    interest
                        .state
                        .retain(|(_, state)| *state != ImageState::NoInterest);
                }
            }
            None => {
                self.streams.push((
                    stream_id,
                    StreamInterest {
                        is_all_sessions: true,
                        ..StreamInterest::default()
                    },
                ));
            }
        }
    }

    /// A subscription that named a session arrived
    /// (`add_subscription_by_session`, `:207-243`).
    pub fn add_subscription_by_session(&mut self, stream_id: i32, session_id: i32) {
        let interest = match self.streams.iter_mut().find(|(id, _)| *id == stream_id) {
            Some((_, interest)) => interest,
            None => {
                self.streams.push((stream_id, StreamInterest::default()));
                &mut self.streams.last_mut().expect("just pushed").1
            }
        };

        if !interest.subscribed.contains(&session_id) {
            interest.subscribed.push(session_id);
        }

        // A tombstone for a session a subscription has just named is a lie.
        if interest.state_of(session_id) == ImageState::NoInterest {
            interest.state.retain(|(id, _)| *id != session_id);
        }
    }

    /// A subscription for the whole stream went away
    /// (`remove_subscription`, `:246-275`).
    pub fn remove_subscription(&mut self, stream_id: i32) {
        let Some(index) = self.streams.iter().position(|(id, _)| *id == stream_id) else {
            return;
        };

        let interest = &mut self.streams[index].1;

        // Images and states for sessions nobody *named* go with it.
        let named: Vec<i32> = interest.subscribed.clone();
        interest
            .images
            .retain(|(session_id, _)| named.contains(session_id));
        interest
            .state
            .retain(|(session_id, _)| named.contains(session_id));
        interest.is_all_sessions = false;

        if interest.images.is_empty() && interest.subscribed.is_empty() {
            self.streams.swap_remove(index);
        }
    }

    /// A subscription for one session went away
    /// (`remove_subscription_by_session`, `:277-300`).
    pub fn remove_subscription_by_session(&mut self, stream_id: i32, session_id: i32) {
        let Some(index) = self.streams.iter().position(|(id, _)| *id == stream_id) else {
            return;
        };

        let is_all_sessions = self.streams[index].1.is_all_sessions;
        let interest = &mut self.streams[index].1;

        if !is_all_sessions {
            interest.images.retain(|(id, _)| *id != session_id);
            interest.state.retain(|(id, _)| *id != session_id);
        }

        interest.subscribed.retain(|id| *id != session_id);

        if !is_all_sessions && interest.subscribed.is_empty() {
            self.streams.swap_remove(index);
        }
    }

    /// An image was created: it takes this session's packets from now on
    /// (`add_publication_image`, `:305-322`).
    pub fn add_image(&mut self, stream_id: i32, session_id: i32, registration_id: i64) {
        let Some((_, interest)) = self.streams.iter_mut().find(|(id, _)| *id == stream_id) else {
            return;
        };

        interest.state.retain(|(id, _)| *id != session_id);
        interest.images.retain(|(id, _)| *id != session_id);
        interest.images.push((session_id, registration_id));
    }

    /// An image went away (`remove_publication_image`, `:324-357`).
    ///
    /// The cooldown is the point: an image that ended *without* an end of
    /// stream is one the far end may re-create, and a driver that immediately
    /// asked for another `SETUP` would spin; a cooldown makes the next packet
    /// wait until something asks again.
    pub fn remove_image(
        &mut self,
        stream_id: i32,
        session_id: i32,
        registration_id: i64,
        is_end_of_stream: bool,
    ) {
        let Some((_, interest)) = self.streams.iter_mut().find(|(id, _)| *id == stream_id) else {
            return;
        };

        if interest.image(session_id) != Some(registration_id) {
            return;
        }

        interest.images.retain(|(id, _)| *id != session_id);

        if !is_end_of_stream {
            interest.set_state(session_id, ImageState::CoolDown);
        }
    }

    /// What a data packet for this (stream, session) wants
    /// (`on_data`, `:385-431`).
    pub fn on_data(&mut self, stream_id: i32, session_id: i32, is_end_of_stream: bool) -> Interest {
        let Some((_, interest)) = self.streams.iter_mut().find(|(id, _)| *id == stream_id) else {
            return Interest::None;
        };

        if let Some(registration_id) = interest.image(session_id) {
            return Interest::Image { registration_id };
        }

        // No image: an end-of-stream packet is a session that is going away, so
        // there is nothing to ask for. A state that is not `Unknown` means this
        // was decided before — a setup is on its way, a create is in flight, or
        // nothing wants it.
        if !is_end_of_stream && interest.state_of(session_id) == ImageState::Unknown {
            if interest.wants(session_id) {
                return Interest::ElicitSetup;
            }

            interest.set_state(session_id, ImageState::NoInterest);
        }

        Interest::None
    }

    /// A `SETUP` arrived for this (stream, session)
    /// (`on_setup`, `:475-540`).
    ///
    /// Returns whether the conductor should be asked to create the image, and
    /// leaves the state `InitInProgress` when it says yes — so a second
    /// `SETUP`, from a second retransmission of the same request, does not ask
    /// twice.
    pub fn on_setup(&mut self, stream_id: i32, session_id: i32) -> bool {
        let Some((_, interest)) = self.streams.iter_mut().find(|(id, _)| *id == stream_id) else {
            return false;
        };

        if interest.image(session_id).is_some() {
            // A `SETUP` for a session an image already serves says only that
            // the sender is still saying `SETUP`: the connection is already
            // there (`aeron_publication_image_add_connection_if_unknown`).
            return false;
        }

        match interest.state_of(session_id) {
            ImageState::PendingSetup | ImageState::Unknown => {
                if !interest.wants(session_id) {
                    interest.set_state(session_id, ImageState::NoInterest);
                    return false;
                }

                if interest.state.len() >= self.stream_session_limit {
                    return false;
                }

                interest.set_state(session_id, ImageState::InitInProgress);
                true
            }
            _ => false,
        }
    }

    /// Mark that a `SETUP` was asked for and record it
    /// (`mark_image_pending_setup`, `:120-147`): returns whether the caller
    /// should send the status message, which is only when the state moved.
    pub fn elicit_setup_from_source(&mut self, stream_id: i32, session_id: i32) -> bool {
        let Some((_, interest)) = self.streams.iter_mut().find(|(id, _)| *id == stream_id) else {
            return false;
        };

        if !matches!(
            interest.state_of(session_id),
            ImageState::Active | ImageState::Unknown
        ) {
            return false;
        }

        interest.set_state(session_id, ImageState::PendingSetup);
        true
    }

    /// The state of one session, for the receiver's pending-setup sweep.
    pub fn state_of(&self, stream_id: i32, session_id: i32) -> ImageState {
        self.streams
            .iter()
            .find(|(id, _)| *id == stream_id)
            .map_or(ImageState::Unknown, |(_, interest)| {
                interest.state_of(session_id)
            })
    }

    /// Forget a pending setup once the image exists or the ask timed out
    /// (`remove_pending_setup`, `:661-680`).
    pub fn remove_pending_setup(&mut self, stream_id: i32, session_id: i32) {
        let Some((_, interest)) = self.streams.iter_mut().find(|(id, _)| *id == stream_id) else {
            return;
        };

        if interest.state_of(session_id) == ImageState::PendingSetup {
            interest.state.retain(|(id, _)| *id != session_id);
        }
    }

    /// Whether an image is wanted here at all
    /// (`has_interest_in`, `:359-374`).
    pub fn has_interest_in(&self, stream_id: i32, session_id: i32) -> bool {
        let Some((_, interest)) = self.streams.iter().find(|(id, _)| *id == stream_id) else {
            return false;
        };

        interest.image(session_id).is_some() || interest.wants(session_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stream_nobody_subscribes_to_has_no_interest() {
        let dispatcher = DataPacketDispatcher::new(16);

        assert!(!dispatcher.has_stream(1001));
        assert!(!dispatcher.has_interest_in(1001, 7));
        assert_eq!(0, dispatcher.stream_count());
    }

    #[test]
    fn a_subscription_to_a_whole_stream_wants_every_session() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);

        assert!(dispatcher.has_interest_in(1001, 7));
        assert!(dispatcher.has_interest_in(1001, -99));
        assert!(!dispatcher.has_interest_in(1002, 7));
    }

    #[test]
    fn a_subscription_that_named_a_session_wants_only_that_one() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription_by_session(1001, 7);

        assert!(dispatcher.has_interest_in(1001, 7));
        assert!(!dispatcher.has_interest_in(1001, 8));

        // And an unnamed session's packet is refused *and* remembered as
        // refused: the second packet does not look again.
        assert_eq!(Interest::None, dispatcher.on_data(1001, 8, false));
        assert_eq!(ImageState::NoInterest, dispatcher.state_of(1001, 8));

        // Naming that session later clears the tombstone.
        dispatcher.add_subscription_by_session(1001, 8);
        assert_eq!(ImageState::Unknown, dispatcher.state_of(1001, 8));
        assert_eq!(Interest::ElicitSetup, dispatcher.on_data(1001, 8, false));
    }

    #[test]
    fn a_packet_for_a_session_with_no_image_asks_for_a_setup_once() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);

        assert_eq!(Interest::ElicitSetup, dispatcher.on_data(1001, 7, false));

        // The status message is sent by whoever asked, and the state is where
        // the "once" lives: `elicit_setup_from_source` says whether to send it.
        assert!(dispatcher.elicit_setup_from_source(1001, 7));
        assert!(!dispatcher.elicit_setup_from_source(1001, 7));
        assert_eq!(ImageState::PendingSetup, dispatcher.state_of(1001, 7));

        // A second packet while the setup is pending asks for nothing.
        assert_eq!(Interest::None, dispatcher.on_data(1001, 7, false));
    }

    /// The "once" is a lease, not a promise: a session that never answers is
    /// given up on and the next frame from it asks again
    /// (`aeron_driver_receiver.c:215-227`, the branch that drops a
    /// non-periodic pending setup).
    ///
    /// Without this the state said `PendingSetup` for ever, so a sender that
    /// missed the first request was never asked a second time — and the
    /// receiver sat waiting for a `SETUP` nobody had been told to send.
    #[test]
    fn giving_up_on_a_pending_setup_lets_the_next_packet_ask_again() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);

        assert_eq!(Interest::ElicitSetup, dispatcher.on_data(1001, 7, false));
        assert!(dispatcher.elicit_setup_from_source(1001, 7));

        // The second packet asks for nothing while the first ask is live.
        assert_eq!(Interest::None, dispatcher.on_data(1001, 7, false));
        assert!(!dispatcher.elicit_setup_from_source(1001, 7));

        // The ask is given up on.
        dispatcher.remove_pending_setup(1001, 7);
        assert_eq!(ImageState::Unknown, dispatcher.state_of(1001, 7));

        // And the next packet asks again.
        assert_eq!(Interest::ElicitSetup, dispatcher.on_data(1001, 7, false));
        assert!(dispatcher.elicit_setup_from_source(1001, 7));
    }

    #[test]
    fn an_end_of_stream_packet_never_asks_for_a_setup() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);

        assert_eq!(Interest::None, dispatcher.on_data(1001, 7, true));
        assert_eq!(ImageState::Unknown, dispatcher.state_of(1001, 7));
    }

    #[test]
    fn a_setup_creates_an_image_once_and_a_second_setup_does_not_ask_again() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);
        dispatcher.on_data(1001, 7, false);
        dispatcher.elicit_setup_from_source(1001, 7);

        assert!(dispatcher.on_setup(1001, 7));
        assert_eq!(ImageState::InitInProgress, dispatcher.state_of(1001, 7));
        assert!(!dispatcher.on_setup(1001, 7), "the create is in flight");

        // The conductor answers with the image, and packets go to it.
        dispatcher.add_image(1001, 7, 42);
        assert_eq!(ImageState::Unknown, dispatcher.state_of(1001, 7));
        assert_eq!(
            Interest::Image {
                registration_id: 42
            },
            dispatcher.on_data(1001, 7, false)
        );
        assert!(!dispatcher.on_setup(1001, 7), "the image is already there");
    }

    #[test]
    fn a_setup_for_a_stream_nothing_subscribes_to_is_refused() {
        let mut dispatcher = DataPacketDispatcher::new(16);

        assert!(!dispatcher.on_setup(1001, 7));
        assert_eq!(0, dispatcher.stream_count());
    }

    #[test]
    fn an_image_that_was_not_told_the_stream_ended_is_cooled_down() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);
        dispatcher.add_image(1001, 7, 42);

        dispatcher.remove_image(1001, 7, 42, false);
        assert_eq!(ImageState::CoolDown, dispatcher.state_of(1001, 7));
        assert_eq!(
            Interest::None,
            dispatcher.on_data(1001, 7, false),
            "a cooled-down session is not asked for again"
        );

        // An end of stream, on the other hand, leaves nothing to cool down: the
        // session is gone for good and the state goes back to unknown.
        dispatcher.add_image(1001, 7, 43);
        dispatcher.remove_image(1001, 7, 43, true);
        assert_eq!(ImageState::Unknown, dispatcher.state_of(1001, 7));
    }

    #[test]
    fn removing_an_image_that_is_not_the_one_there_is_ignored() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);
        dispatcher.add_image(1001, 7, 42);

        dispatcher.remove_image(1001, 7, 43, false);

        assert_eq!(
            Interest::Image {
                registration_id: 42
            },
            dispatcher.on_data(1001, 7, false)
        );
    }

    #[test]
    fn a_subscription_that_goes_away_takes_its_unnamed_sessions_with_it() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);
        dispatcher.add_subscription_by_session(1001, 9);
        dispatcher.add_image(1001, 7, 42);
        dispatcher.add_image(1001, 9, 43);

        dispatcher.remove_subscription(1001);

        // The session that was named keeps its image; the one that was only
        // covered by "all sessions" loses it.
        assert_eq!(
            Interest::Image {
                registration_id: 43
            },
            dispatcher.on_data(1001, 9, false)
        );
        assert!(!dispatcher.has_interest_in(1001, 7));
        assert_eq!(1, dispatcher.stream_count());

        // And the last named session going takes the stream with it.
        dispatcher.remove_subscription_by_session(1001, 9);
        assert_eq!(0, dispatcher.stream_count());
    }

    #[test]
    fn a_stream_admitting_it_wants_every_session_drops_its_tombstones() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription_by_session(1001, 7);
        dispatcher.on_data(1001, 8, false);
        assert_eq!(ImageState::NoInterest, dispatcher.state_of(1001, 8));

        dispatcher.add_subscription(1001);

        assert_eq!(ImageState::Unknown, dispatcher.state_of(1001, 8));
        assert_eq!(Interest::ElicitSetup, dispatcher.on_data(1001, 8, false));
    }

    #[test]
    fn a_pending_setup_is_forgotten_when_the_image_arrives_or_the_ask_is_dropped() {
        let mut dispatcher = DataPacketDispatcher::new(16);
        dispatcher.add_subscription(1001);
        dispatcher.on_data(1001, 7, false);
        dispatcher.elicit_setup_from_source(1001, 7);

        dispatcher.remove_pending_setup(1001, 7);
        assert_eq!(ImageState::Unknown, dispatcher.state_of(1001, 7));

        // And a state that is not a pending setup is left alone.
        dispatcher.add_image(1001, 7, 42);
        dispatcher.remove_pending_setup(1001, 7);
        assert_eq!(
            Interest::Image {
                registration_id: 42
            },
            dispatcher.on_data(1001, 7, false)
        );
    }
}
