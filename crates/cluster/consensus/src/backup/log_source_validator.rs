//! `LogSourceValidator` (`LogSourceValidator.java`): which member a backup will
//! take its log from.
//!
//! The reference's whole class is one `switch` over [`SourceType`], and its
//! three cases are the three answers a deployment might want:
//!
//! * [`SourceType::Any`] — any member may supply the log;
//! * [`SourceType::Leader`] — only the leader may (a backup that wants the
//!   committed stream and nothing else);
//! * [`SourceType::Follower`] — only a non-leader may (the shape a standby
//!   deployment uses so the backup never adds load to the leader).
//!
//! The three cases are reproduced exactly, including the one that reads
//! strange: `FOLLOWER` also accepts a **null** `leaderMemberId`. The reference
//! writes it `NULL_VALUE == leaderMemberId || leaderMemberId != memberId`
//! (`LogSourceValidator.java:35`), and the null case is a real one — a member
//! that answered before it knew who the leader was.

use super::SourceType;

/// `Aeron.NULL_VALUE` as the reference uses it for the `long` member ids this
/// class is given (`Aeron.java:82`: an `int` -1, widened where it is compared).
pub const NULL_VALUE: i64 = -1;

/// The validator itself (`LogSourceValidator.java:24-42`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogSourceValidator {
    source_type: SourceType,
}

impl LogSourceValidator {
    /// Build one for a configured source type.
    #[must_use]
    pub const fn new(source_type: SourceType) -> Self {
        Self { source_type }
    }

    /// The configured source type.
    #[must_use]
    pub const fn source_type(self) -> SourceType {
        self.source_type
    }

    /// `isAcceptable(leaderMemberId, memberId)` (`LogSourceValidator.java:26-40`).
    ///
    /// `leader_member_id` is what the responder said the leader is (or
    /// [`NULL_VALUE`] when it did not know), and `member_id` is the responder
    /// itself — the member whose archive the backup would read.
    #[must_use]
    pub fn is_acceptable(self, leader_member_id: i64, member_id: i64) -> bool {
        match self.source_type {
            SourceType::Leader => NULL_VALUE != leader_member_id && leader_member_id == member_id,
            SourceType::Follower => NULL_VALUE == leader_member_id || leader_member_id != member_id,
            SourceType::Any => true,
        }
    }
}

#[cfg(test)]
mod tests {
    //! `LogSourceValidatorTest` (`LogSourceValidatorTest.java:25-65`), three
    //! cases.

    use super::*;

    /// `leaderLogSourceTypeShouldOnlyAcceptLeader` (`:29-40`).
    #[test]
    fn leader_log_source_type_should_only_accept_leader() {
        let validator = LogSourceValidator::new(SourceType::Leader);

        assert!(validator.is_acceptable(1, 1));
        assert!(!validator.is_acceptable(1, 2));
        assert!(!validator.is_acceptable(NULL_VALUE, NULL_VALUE));
        assert!(!validator.is_acceptable(NULL_VALUE, 1));
    }

    /// `followerLogSourceTypeShouldOnlyAcceptFollowerAndUnknown` (`:42-53`).
    #[test]
    fn follower_log_source_type_should_only_accept_follower_and_unknown() {
        let validator = LogSourceValidator::new(SourceType::Follower);

        assert!(validator.is_acceptable(1, 2));
        assert!(!validator.is_acceptable(1, 1));
        // A responder that does not know who the leader is counts as a follower:
        // the reference's `NULL_VALUE == leaderMemberId ||` (`:35`).
        assert!(validator.is_acceptable(NULL_VALUE, 2));
    }

    /// `anyLogSourceTypeShouldAny` (`:55-64`).
    #[test]
    fn any_log_source_type_should_any() {
        let validator = LogSourceValidator::new(SourceType::Any);

        assert!(validator.is_acceptable(1, 1));
        assert!(validator.is_acceptable(1, 2));
        assert!(validator.is_acceptable(NULL_VALUE, NULL_VALUE));
        assert!(validator.is_acceptable(NULL_VALUE, 2));
    }
}
