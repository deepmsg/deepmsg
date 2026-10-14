//! The archive client's **persistent subscription** — a replay that becomes a
//! live subscription, and heals itself when either end of it goes away
//! (`aeron_archive_persistent_subscription.c`, 2429 lines).
//!
//! # Why "persistent"
//!
//! Not because the subscription is. Because the *application's* listener is: an
//! archive restart, a live publisher restarting, a dropped replay channel — the
//! reference's state machine walks through all of them and comes back, and the
//! application is told about the transitions rather than about the failures.
//! The eighteen states that do it are `:2283-2338`'s dispatch, and two of the
//! edges are the self-healing ones (`:2247-2268`, `:804-829`).
//!
//! # What is here, and what is not
//!
//! [`context`] — the context and its ten refusals, which is where a program
//! that has configured one wrong finds out. The state machine itself, the
//! self-healing archive client it drives, and the four counters it publishes
//! (114–117) are the later commits of this slice; so is everything that needs a
//! driver, which is why [`context::Concluded`] answers with *whether* it will
//! own its client rather than with a client.
//!
//! # The listener is optional, and that is the reference's choice too
//!
//! The reference's `set_listener` refuses a null listener
//! (`aeron_archive_persistent_subscription.c:268-280`) — setting one to null is
//! a programming error — but never setting one at all is legal, which is what
//! `shouldNotRequireEventListener` (`…_persistent_subscription_test.cpp:2878`)
//! is about. So when the listener arrives here it arrives as an `Option`, and
//! the reference's null refusal has no counterpart: **one of its thirty-nine
//! client-unit cases is a case Rust cannot have**, which is recorded rather than
//! papered over with an `Option<()>` that nothing can call.

pub mod context;
pub mod counters;
pub mod state;

pub use context::{ConcludeError, Concluded, FROM_LIVE, FROM_START, PersistentSubscriptionContext};
pub use counters::{
    Counters, Identity, JOIN_DIFFERENCE_NAME, JOIN_DIFFERENCE_TYPE_ID, LIVE_JOINED_COUNT_TYPE_ID,
    LIVE_JOINED_NAME, LIVE_LEFT_COUNT_TYPE_ID, LIVE_LEFT_NAME, NOTHING_JOINED, Provided,
    STATE_NAME, STATE_TYPE_ID,
};
pub use state::{NewError, PersistentSubscription, State, SubscriptionError, SubscriptionListener};
