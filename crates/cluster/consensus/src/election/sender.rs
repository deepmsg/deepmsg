//! What the election sends to other members.
//!
//! **This trait is frozen** at the same time as [`super::ElectionHost`], and by
//! the same rule: add, never change.
//!
//! The surface is exactly what `Election.java` calls on its `ConsensusPublisher`
//! (`ConsensusPublisher.java:53-308`): `grep -o 'consensusPublisher\.[a-zA-Z]*('`
//! answers **6 names**, **6 signatures**, **8 calls** — no overloads here, unlike
//! the host side.
//!
//! # Departure: the publication is named, not passed
//!
//! Every reference method takes an `ExclusivePublication` as its first argument
//! (`member.publication()` at each call site). The publication belongs to the
//! consensus module's member table, and this trait is implemented by the module
//! itself, so it takes `member_id` and resolves its own handle.
//!
//! # Return values follow the reference's own signal
//!
//! `request_vote`, `append_position` and `catchup_position` return `bool` in the
//! reference and the election acts on it (`Election.java:787`, `:1168`, `:1340`);
//! the other three return nothing and are retried on the next duty cycle. This
//! is kept as-is: a failed send is not an error, it is the next cycle's work.

/// What the election sends to other members.
///
/// Line numbers are `ConsensusPublisher.java`.
pub trait ConsensusSender {
    /// `void canvassPosition(..)` at `:53`, called at `Election.java:1247`.
    fn canvass_position(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        log_position: i64,
        leadership_term_id: i64,
        follower_member_id: i32,
    );

    /// `boolean requestVote(..)` at `:91`, called at `Election.java:787`.
    fn request_vote(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_term_id: i64,
        candidate_member_id: i32,
    ) -> bool;

    /// `void placeVote(..)` at `:131`, called at `Election.java:1187`.
    ///
    /// Seven arguments is one over clippy's default; the wire message carries
    /// all six after the target member, and the reference passes them the same
    /// way. Same reasoning as [`Self::new_leadership_term`].
    #[allow(clippy::too_many_arguments)]
    fn place_vote(
        &mut self,
        member_id: i32,
        candidate_term_id: i64,
        log_leadership_term_id: i64,
        log_position: i64,
        candidate_member_id: i32,
        follower_member_id: i32,
        vote: bool,
    );

    /// `void newLeadershipTerm(..)` at `:172`, called at `Election.java:1281`.
    ///
    /// Fourteen arguments after the publication is dropped, which is what the
    /// wire message carries (`NewLeadershipTerm` in schema 111). Grouping them
    /// into a struct would read better but would also be a shape the reference
    /// does not have, and a transposition in a 14-field struct literal is the
    /// same class of bug as a transposition in a 14-argument call. The doc
    /// order below is the encoder order, so it can be checked against the
    /// schema line by line.
    #[allow(clippy::too_many_arguments)]
    fn new_leadership_term(
        &mut self,
        member_id: i32,
        log_leadership_term_id: i64,
        next_leadership_term_id: i64,
        next_term_base_log_position: i64,
        next_log_position: i64,
        leadership_term_id: i64,
        term_base_log_position: i64,
        log_position: i64,
        commit_position: i64,
        leader_recording_id: i64,
        timestamp: i64,
        leader_member_id: i32,
        log_session_id: i32,
        app_version: i32,
        is_startup: bool,
    );

    /// `boolean appendPosition(..)` at `:234`, called three times
    /// (`Election.java:1168`, `:1306`, `:1323`). This is also the follower's
    /// heartbeat: there is no separate heartbeat frame.
    fn append_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        flags: i16,
    ) -> bool;

    /// `boolean catchupPosition(..)` at `:308`, called at
    /// `Election.java:1340`.
    fn catchup_position(
        &mut self,
        member_id: i32,
        leadership_term_id: i64,
        log_position: i64,
        follower_member_id: i32,
        catchup_endpoint: &str,
    ) -> bool;
}

/// Every method on [`ConsensusSender`], in declaration order. Pinned by
/// `tests/election_surface.rs`; add a method here when you add one to the
/// trait.
pub const SENDER_METHODS: &[&str] = &[
    "canvass_position",
    "request_vote",
    "place_vote",
    "new_leadership_term",
    "append_position",
    "catchup_position",
];
