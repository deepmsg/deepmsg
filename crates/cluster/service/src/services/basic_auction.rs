//! `BasicAuctionClusteredService` — the reference's tutorial business, byte for
//! byte (`aeron-samples/.../cluster/tutorial/BasicAuctionClusteredService.java`).
//!
//! It exists here for one reason: it is the **twin**. A Rust container and a
//! Java one that process the same log must produce the same snapshot bytes, and
//! that claim can only be checked with a business that is the same on both
//! sides — same message layout, same state transitions, same snapshot record.
//! Every constant below is the reference's; the comment beside each says which
//! line writes it.
//!
//! # The three records
//!
//! | Record | Layout | Written by |
//! |---|---|---|
//! | bid (in) | `correlationId: i64 @0`, `customerId: i64 @8`, `price: i64 @16` — 24 B | the client (`:40-42`) |
//! | egress (out) | `correlationId @0`, `winningCustomerId @8`, `bestPrice @16`, `bidSucceeded: u8 @24` — 25 B | `onSessionMessage` (`:43-44`) |
//! | snapshot | `customerId: i64 @0`, `price: i64 @8` — 16 B | `onTakeSnapshot` (`:46-48`) |
//!
//! The sample's `System.out.println` calls in the session callbacks are **not**
//! ported: they are tutorial output, not behaviour, and they would put a line
//! per message into the interop logs.
//!
//! # Deviations from the sample
//!
//! 1. The sample's `idleStrategy` is `cluster.idleStrategy()`; here a callback
//!    idle is `Cluster::idle`, which is the same strategy the container already
//!    owns (the container cannot hand out a second borrow of it).
//! 2. `onTakeSnapshot`'s idle comes from [`SnapshotWriter::idle`], which polls
//!    the client the way the reference's agent invoker does.

use crate::cluster::Cluster;
use crate::error::ContainerError;
use crate::role::Role;
use crate::service::{ClusteredService, LogHeader, SessionId, SnapshotReader, SnapshotWriter};
use deepmsg_codec_cluster::close_reason::CloseReason;

/// `CORRELATION_ID_OFFSET` (`:36`).
pub const CORRELATION_ID_OFFSET: usize = 0;
/// `CUSTOMER_ID_OFFSET` (`:37`).
pub const CUSTOMER_ID_OFFSET: usize = CORRELATION_ID_OFFSET + 8;
/// `PRICE_OFFSET` (`:38`).
pub const PRICE_OFFSET: usize = CUSTOMER_ID_OFFSET + 8;
/// `BID_MESSAGE_LENGTH` (`:39`).
pub const BID_MESSAGE_LENGTH: usize = PRICE_OFFSET + 8;
/// `BID_SUCCEEDED_OFFSET` (`:40`).
pub const BID_SUCCEEDED_OFFSET: usize = BID_MESSAGE_LENGTH;
/// `EGRESS_MESSAGE_LENGTH` (`:41`).
pub const EGRESS_MESSAGE_LENGTH: usize = BID_SUCCEEDED_OFFSET + 1;

/// `SNAPSHOT_CUSTOMER_ID_OFFSET` (`:43`).
pub const SNAPSHOT_CUSTOMER_ID_OFFSET: usize = 0;
/// `SNAPSHOT_PRICE_OFFSET` (`:44`).
pub const SNAPSHOT_PRICE_OFFSET: usize = SNAPSHOT_CUSTOMER_ID_OFFSET + 8;
/// `SNAPSHOT_MESSAGE_LENGTH` (`:45`).
pub const SNAPSHOT_MESSAGE_LENGTH: usize = SNAPSHOT_PRICE_OFFSET + 8;

/// The state the snapshot carries (`Auction`, `:204-232`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Auction {
    best_price: i64,
    current_winning_customer_id: i64,
}

impl Auction {
    /// `new Auction()`: no price and no winner (`:206-207`).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            best_price: 0,
            current_winning_customer_id: -1,
        }
    }

    /// `loadInitialState` (`:209-213`).
    pub const fn load_initial_state(&mut self, price: i64, customer_id: i64) {
        self.best_price = price;
        self.current_winning_customer_id = customer_id;
    }

    /// `attemptBid` (`:215-227`): strictly higher wins.
    pub const fn attempt_bid(&mut self, price: i64, customer_id: i64) -> bool {
        if price <= self.best_price {
            return false;
        }
        self.best_price = price;
        self.current_winning_customer_id = customer_id;
        true
    }

    /// `getBestPrice` (`:229-232`).
    #[must_use]
    pub const fn best_price(&self) -> i64 {
        self.best_price
    }

    /// `getCurrentWinningCustomerId` (`:234-237`).
    #[must_use]
    pub const fn current_winning_customer_id(&self) -> i64 {
        self.current_winning_customer_id
    }
}

/// The tutorial business (`BasicAuctionClusteredService`, `:44`).
#[derive(Clone, Copy, Debug, Default)]
pub struct BasicAuction {
    auction: Auction,
}

impl BasicAuction {
    /// A fresh auction.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            auction: Auction::new(),
        }
    }

    /// The state, for a test that wants to read it directly.
    #[must_use]
    pub const fn auction(&self) -> &Auction {
        &self.auction
    }

    /// The 25-byte egress record (`:89-115`).
    #[must_use]
    pub fn egress_message(
        correlation_id: i64,
        winning_customer_id: i64,
        best_price: i64,
        bid_succeeded: bool,
    ) -> [u8; EGRESS_MESSAGE_LENGTH] {
        let mut buffer = [0_u8; EGRESS_MESSAGE_LENGTH];
        buffer[CORRELATION_ID_OFFSET..CUSTOMER_ID_OFFSET]
            .copy_from_slice(&correlation_id.to_le_bytes());
        buffer[CUSTOMER_ID_OFFSET..PRICE_OFFSET]
            .copy_from_slice(&winning_customer_id.to_le_bytes());
        buffer[PRICE_OFFSET..BID_MESSAGE_LENGTH].copy_from_slice(&best_price.to_le_bytes());
        buffer[BID_SUCCEEDED_OFFSET] = u8::from(bid_succeeded);
        buffer
    }

    /// The 16-byte snapshot record (`onTakeSnapshot`, `:116-127`).
    #[must_use]
    pub fn snapshot_message(&self) -> [u8; SNAPSHOT_MESSAGE_LENGTH] {
        let mut buffer = [0_u8; SNAPSHOT_MESSAGE_LENGTH];
        buffer[SNAPSHOT_CUSTOMER_ID_OFFSET..SNAPSHOT_PRICE_OFFSET]
            .copy_from_slice(&self.auction.current_winning_customer_id().to_le_bytes());
        buffer[SNAPSHOT_PRICE_OFFSET..SNAPSHOT_MESSAGE_LENGTH]
            .copy_from_slice(&self.auction.best_price().to_le_bytes());
        buffer
    }

    /// `loadSnapshot` (`:130-160`): poll one fragment at a time until the image
    /// is at its end, but stop as soon as the record has been read.
    fn load_snapshot(
        &mut self,
        cluster: &mut Cluster<'_>,
        reader: &mut SnapshotReader<'_>,
    ) -> Result<(), ContainerError> {
        let mut loaded = false;
        while !reader.is_end_of_stream() {
            let fragments = reader.poll(1, |fragment| {
                if fragment.payload_length() < SNAPSHOT_MESSAGE_LENGTH {
                    return;
                }
                let (Some(customer_id), Some(price)) = (
                    fragment.payload_i64_at(SNAPSHOT_CUSTOMER_ID_OFFSET),
                    fragment.payload_i64_at(SNAPSHOT_PRICE_OFFSET),
                ) else {
                    return;
                };
                self.auction.load_initial_state(price, customer_id);
                loaded = true;
            });

            if loaded {
                break;
            }

            cluster.idle(fragments)?;
        }

        if !loaded {
            return Err(ContainerError::Cluster(
                "the basic auction's snapshot carried no state".to_owned(),
            ));
        }

        Ok(())
    }

    /// `session.offer` until it is not back-pressured (`:107-114`).
    fn offer_to_session(
        cluster: &mut Cluster<'_>,
        session: SessionId,
        payload: &[u8],
    ) -> Result<(), ContainerError> {
        for _ in 0..3 {
            let offered = {
                let Some(mut view) = cluster.session(session) else {
                    return Ok(());
                };
                view.offer(payload)?
            };
            if offered > 0 {
                return Ok(());
            }
            cluster.idle(1)?;
        }
        Ok(())
    }
}

impl ClusteredService for BasicAuction {
    fn on_start(
        &mut self,
        cluster: &mut Cluster<'_>,
        snapshot: Option<&mut SnapshotReader<'_>>,
    ) -> Result<(), ContainerError> {
        if let Some(reader) = snapshot {
            self.load_snapshot(cluster, reader)?;
        }
        Ok(())
    }

    fn on_session_open(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_close(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _session: SessionId,
        _timestamp: i64,
        _reason: CloseReason,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_session_message(
        &mut self,
        cluster: &mut Cluster<'_>,
        session: Option<SessionId>,
        _timestamp: i64,
        payload: &[u8],
        _header: &LogHeader,
    ) -> Result<(), ContainerError> {
        if payload.len() < BID_MESSAGE_LENGTH {
            return Ok(());
        }

        let correlation_id = i64::from_le_bytes(payload[CORRELATION_ID_OFFSET..CUSTOMER_ID_OFFSET].try_into().expect("eight bytes"));
        let customer_id = i64::from_le_bytes(payload[CUSTOMER_ID_OFFSET..PRICE_OFFSET].try_into().expect("eight bytes"));
        let price = i64::from_le_bytes(payload[PRICE_OFFSET..BID_MESSAGE_LENGTH].try_into().expect("eight bytes"));

        let bid_succeeded = self.auction.attempt_bid(price, customer_id);

        if let Some(session) = session {
            let egress = Self::egress_message(
                correlation_id,
                self.auction.current_winning_customer_id(),
                self.auction.best_price(),
                bid_succeeded,
            );
            Self::offer_to_session(cluster, session, &egress)?;
        }

        Ok(())
    }

    fn on_timer_event(
        &mut self,
        _cluster: &mut Cluster<'_>,
        _correlation_id: i64,
        _timestamp: i64,
    ) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_take_snapshot(&mut self, writer: &mut SnapshotWriter<'_>) -> Result<(), ContainerError> {
        let record = self.snapshot_message();
        loop {
            if writer.offer(&record) > 0 {
                return Ok(());
            }
            writer.idle();
        }
    }

    fn on_role_change(&mut self, _new_role: Role) -> Result<(), ContainerError> {
        Ok(())
    }

    fn on_terminate(&mut self, _cluster: &mut Cluster<'_>) -> Result<(), ContainerError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bid_must_be_strictly_higher_to_win() {
        let mut auction = Auction::new();
        assert!(!auction.attempt_bid(0, 1));
        assert!(auction.attempt_bid(1, 1));
        assert!(!auction.attempt_bid(1, 2));
        assert!(auction.attempt_bid(2, 2));
        assert_eq!(auction.best_price(), 2);
        assert_eq!(auction.current_winning_customer_id(), 2);
    }

    #[test]
    fn the_records_are_the_reference_layout() {
        assert_eq!(BID_MESSAGE_LENGTH, 24);
        assert_eq!(EGRESS_MESSAGE_LENGTH, 25);
        assert_eq!(SNAPSHOT_MESSAGE_LENGTH, 16);

        let egress = BasicAuction::egress_message(7, 3, 1_000, true);
        assert_eq!(i64::from_le_bytes(egress[0..8].try_into().unwrap()), 7);
        assert_eq!(i64::from_le_bytes(egress[8..16].try_into().unwrap()), 3);
        assert_eq!(i64::from_le_bytes(egress[16..24].try_into().unwrap()), 1_000);
        assert_eq!(egress[24], 1);

        let mut auction = BasicAuction::new();
        auction.auction.load_initial_state(42, 9);
        let snapshot = auction.snapshot_message();
        assert_eq!(i64::from_le_bytes(snapshot[0..8].try_into().unwrap()), 9);
        assert_eq!(i64::from_le_bytes(snapshot[8..16].try_into().unwrap()), 42);
    }
}
