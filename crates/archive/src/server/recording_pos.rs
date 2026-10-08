//! The recording position counter (100): where a recording has got to.
//!
//! `RecordingPos.java` is the whole of it — a counter whose **key** carries the
//! recording id, the session id, the source identity and the archive id, and
//! whose **value** is the position the recording has reached. Everything that
//! asks about a live recording asks this counter: the reference's own client,
//! `AeronStat`, and the C system tests, which find it by session id and read
//! the recording id back out of its key (`aeron_archive_test.cpp:260-286`).
//!
//! # The two layouts, which are byte contracts
//!
//! Key (`RecordingPos.java:36-52`, written at `:103-112`):
//!
//! ```text
//! 0   recording id      i64
//! 8   session id        i32
//! 12  source identity len i32   — the length actually written, not the string's
//! 16  source identity   ASCII, truncated to 88 bytes
//!     archive id        i64     — right behind the identity, so its offset is
//!                                 the only part of the key that moves
//! ```
//!
//! Label (`:114-128`):
//!
//! ```text
//! "rec-pos: " <recordingId> " " <sessionId> " " <streamId> " "
//! <strippedChannel truncated so the suffix still fits> " - archiveId=" <archiveId>
//! ```
//!
//! The layout is confirmed from **two** sides, which is worth the sentence:
//! the writer is Java (`RecordingPos.java`, above), and the reader the C
//! harness uses declares the same three fixed fields in the same order —
//!
//! ```c
//! struct aeron_archive_recording_pos_key_defn   /* packed */
//! {
//!     int64_t recording_id;
//!     int32_t session_id;
//!     int32_t source_identity_length;
//! };
//! ```
//!
//! (`aeron_archive_recording_pos.c:24-30`) and reads the identity from
//! `sizeof(struct)` — offset **16** — onwards (`:149-152`). So 0/8/12/16 is not
//! this module's reading of one file; it is where two implementations
//! independently agree the fields are.
//!
//! # The three truncations, and why they are the shape they are
//!
//! The source identity is cut to `MAX_KEY_LENGTH - 16 - 8` = **88** bytes
//! (`:106-107`), which is what is left of the key region once the two fixed
//! ends are taken out. The channel is cut to
//! `MAX_LABEL_LENGTH - label-so-far - lengthOfArchiveIdLabel(archiveId)`
//! (`:123-127`) — the **suffix is measured, not guessed**, because it carries
//! the archive id and so its length depends on the id's digits. Both cuts are
//! byte cuts: Agrona writes one byte per char
//! (`putStringWithoutLengthAscii`), and a channel is ASCII.
//!
//! # What this module does not do
//!
//! Nothing here is allocated at archive start. The counter belongs to a
//! *recording*, so it appears when one does (`ArchiveConductor.java:2021-2030`)
//! and is given back when one ends (`RecordingSession.java:117-121`).

use std::time::Duration;

use deepmsg_cnc::counters::CountersReader;
use deepmsg_cnc::layout;
use deepmsg_core::buffer::ReadWrite;

use deepmsg_client::client::AsyncAddPoll;

use super::counters::{
    ARCHIVE_RECORDING_POSITION_TYPE_ID, CounterError, Counters, archive_id_suffix, check_type_id,
    length_of_archive_id_label,
};

/// `Aeron.NULL_VALUE`, which is what the reference's two finders take to mean
/// "any archive" (`RecordingPos.java:174`, `:230`).
pub const ANY_ARCHIVE_ID: i64 = layout::NULL_VALUE;

/// `RecordingPos.NAME` and the separator after it (`:69`, `:116`).
const NAME_PREFIX: &str = "rec-pos: ";

/// `RECORDING_ID_OFFSET` (`:75`).
pub const RECORDING_ID_OFFSET: usize = 0;
/// `SESSION_ID_OFFSET` (`:76`).
pub const SESSION_ID_OFFSET: usize = RECORDING_ID_OFFSET + 8;
/// `SOURCE_IDENTITY_LENGTH_OFFSET` (`:77`).
pub const SOURCE_IDENTITY_LENGTH_OFFSET: usize = SESSION_ID_OFFSET + 4;
/// `SOURCE_IDENTITY_OFFSET` (`:78`).
pub const SOURCE_IDENTITY_OFFSET: usize = SOURCE_IDENTITY_LENGTH_OFFSET + 4;

/// How much of the source identity the key has room for — the rest of the key
/// region once the recording id, the session id, the length and the archive id
/// are out of it (`:106-107`).
pub const SOURCE_IDENTITY_MAX_LENGTH: usize =
    layout::COUNTER_KEY_LENGTH - SOURCE_IDENTITY_OFFSET - 8;

/// The key of a recording position counter.
#[must_use]
pub fn key(recording_id: i64, session_id: i32, source_identity: &str, archive_id: i64) -> Vec<u8> {
    let identity = source_identity.as_bytes();
    let identity_length = identity.len().min(SOURCE_IDENTITY_MAX_LENGTH);
    let identity = &identity[..identity_length];

    let mut key = Vec::with_capacity(SOURCE_IDENTITY_OFFSET + identity_length + 8);
    key.extend_from_slice(&recording_id.to_le_bytes());
    key.extend_from_slice(&session_id.to_le_bytes());
    key.extend_from_slice(&(identity_length as i32).to_le_bytes());
    key.extend_from_slice(identity);
    key.extend_from_slice(&archive_id.to_le_bytes());

    key
}

/// The label of a recording position counter.
#[must_use]
pub fn label(
    recording_id: i64,
    session_id: i32,
    stream_id: i32,
    stripped_channel: &str,
    archive_id: i64,
) -> String {
    let mut label = format!("{NAME_PREFIX}{recording_id} {session_id} {stream_id} ");

    let allowed = layout::COUNTER_LABEL_LENGTH_MAX
        .saturating_sub(label.len())
        .saturating_sub(length_of_archive_id_label(archive_id));

    label.push_str(&first_bytes(stripped_channel, allowed));
    label.push_str(&archive_id_suffix(archive_id));

    label
}

/// `putStringWithoutLengthAscii(buffer, offset, value, 0, length)`: at most
/// `allowed` **bytes**, one per char, and never a panic on a cut.
fn first_bytes(value: &str, allowed: usize) -> String {
    let bytes = value.as_bytes();

    String::from_utf8_lossy(&bytes[..bytes.len().min(allowed)]).into_owned()
}

/// What a recording position counter's key says — the four fields, read back.
///
/// The C harness reads two of these out of the metadata region itself
/// (`aeron_archive_recording_pos.c`), which is why the offsets above are
/// public and why this is a parse rather than a struct the archive keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedIdentity {
    /// The recording this counter is for.
    pub recording_id: i64,
    /// The publication session being recorded.
    pub session_id: i32,
    /// Where the frames came from, as the far end named itself — truncated to
    /// [`SOURCE_IDENTITY_MAX_LENGTH`] like the key's copy of it.
    pub source_identity: String,
    /// The archive the recording belongs to.
    pub archive_id: i64,
}

/// Read a key back, or `None` for one too short to hold its own fields.
///
/// The declared source identity length is what bounds the archive id's offset,
/// so a key whose length field does not fit is refused rather than read past —
/// the region is a fixed 112 bytes and its tail is zeroes, so a short key is a
/// key that was never written.
#[must_use]
pub fn parse_key(key: &[u8]) -> Option<RecordedIdentity> {
    if key.len() < SOURCE_IDENTITY_OFFSET {
        return None;
    }

    let recording_id = i64::from_le_bytes(
        key[RECORDING_ID_OFFSET..SESSION_ID_OFFSET]
            .try_into()
            .ok()?,
    );
    let session_id = i32::from_le_bytes(
        key[SESSION_ID_OFFSET..SOURCE_IDENTITY_LENGTH_OFFSET]
            .try_into()
            .ok()?,
    );
    let identity_length = i32::from_le_bytes(
        key[SOURCE_IDENTITY_LENGTH_OFFSET..SOURCE_IDENTITY_OFFSET]
            .try_into()
            .ok()?,
    );

    let identity_length = usize::try_from(identity_length).ok()?;
    if identity_length > SOURCE_IDENTITY_MAX_LENGTH {
        return None;
    }

    let identity_end = SOURCE_IDENTITY_OFFSET + identity_length;
    let archive_id_end = identity_end + 8;
    if archive_id_end > key.len() {
        return None;
    }

    let archive_id = i64::from_le_bytes(key[identity_end..archive_id_end].try_into().ok()?);

    Some(RecordedIdentity {
        recording_id,
        session_id,
        source_identity: String::from_utf8_lossy(&key[SOURCE_IDENTITY_OFFSET..identity_end])
            .into_owned(),
        archive_id,
    })
}

/// The counter for a recording, or `None` (`RecordingPos.findCounterIdByRecording`,
/// `:157-188`).
///
/// `archive_id` of [`ANY_ARCHIVE_ID`] matches any archive, which is the
/// reference's deprecated two-argument form (`:143-146`).
#[must_use]
pub fn find_counter_id_by_recording<Access>(
    counters: &CountersReader<'_, Access>,
    recording_id: i64,
    archive_id: i64,
) -> Option<i32> {
    find(counters, archive_id, |identity| {
        identity.recording_id == recording_id
    })
}

/// The counter for a publication session, or `None`
/// (`RecordingPos.findCounterIdBySession`, `:213-244`).
///
/// This is how the C harness finds a recording it has just started: it knows
/// the session id off its own publication and nothing else
/// (`aeron_archive_test.cpp:275-286`).
#[must_use]
pub fn find_counter_id_by_session<Access>(
    counters: &CountersReader<'_, Access>,
    session_id: i32,
    archive_id: i64,
) -> Option<i32> {
    find(counters, archive_id, |identity| {
        identity.session_id == session_id
    })
}

/// The walk both finders make: allocated, type 100, and the key agreeing.
///
/// [`CountersReader::for_each`] stops at the first free record, which is what
/// the reference's own walk does (`:181-184`) rather than scanning the region.
fn find<Access>(
    counters: &CountersReader<'_, Access>,
    archive_id: i64,
    matches: impl Fn(&RecordedIdentity) -> bool,
) -> Option<i32> {
    let mut found = None;

    counters.for_each(|descriptor| {
        if found.is_some() || descriptor.type_id != ARCHIVE_RECORDING_POSITION_TYPE_ID {
            return;
        }

        let Some(key) = counters.key(descriptor.counter_id) else {
            return;
        };

        let Some(identity) = parse_key(&key) else {
            return;
        };

        if matches(&identity) && (ANY_ARCHIVE_ID == archive_id || identity.archive_id == archive_id)
        {
            found = Some(descriptor.counter_id);
        }
    });

    found
}

/// A recording session's position counter.
///
/// Made when the image arrives (`ArchiveConductor.java:2021-2030`), written
/// every turn the recording wrote (`RecordingSession.java:240`), and given back
/// when the session ends (`:120`).
///
/// The two steps are in this order for the reason every other counter here is:
/// the ask needs only the ids, and the claim needs the driver to have answered
/// — and a conductor cannot wait for one, because it runs inside the turn that
/// drives the driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordingPos {
    registration_id: i64,
    /// The slot, once the driver's answer has been read.
    counter_id: Option<i32>,
}

impl RecordingPos {
    /// Ask for a recording's position counter, and do not wait for the answer.
    ///
    /// # Errors
    ///
    /// [`CounterError`] if the command could not be written or sent.
    #[allow(clippy::too_many_arguments)] // one per field the key and the label carry
    pub fn request<C: Counters>(
        client: &mut C,
        archive_id: i64,
        recording_id: i64,
        session_id: i32,
        stream_id: i32,
        stripped_channel: &str,
        source_identity: &str,
        timeout: Duration,
    ) -> Result<Self, CounterError> {
        let key = key(recording_id, session_id, source_identity, archive_id);
        let label = label(
            recording_id,
            session_id,
            stream_id,
            stripped_channel,
            archive_id,
        );

        let registration_id =
            client.async_add_counter(ARCHIVE_RECORDING_POSITION_TYPE_ID, &key, &label, timeout)?;

        Ok(Self {
            registration_id,
            counter_id: None,
        })
    }

    /// Take up the counter once the driver has allocated it.
    ///
    /// `Ok(None)` means "not yet" rather than "no", as everywhere else here.
    ///
    /// # Errors
    ///
    /// [`CounterError`] if the driver refused the add, if what it allocated is
    /// not a 100, or if the slot cannot be read back.
    pub fn claim<C: Counters, Access>(
        &mut self,
        client: &mut C,
        counters: &CountersReader<'_, Access>,
    ) -> Result<bool, CounterError> {
        let counter_id = match client.poll_counter(self.registration_id) {
            AsyncAddPoll::Ready => {
                let Some(counter_id) = client.counter_id(self.registration_id) else {
                    return Ok(false);
                };
                counter_id
            }
            AsyncAddPoll::Awaiting | AsyncAddPoll::Unknown => return Ok(false),
            AsyncAddPoll::Failed(error) => return Err(CounterError::Command(error)),
        };

        check_type_id(counters, counter_id, ARCHIVE_RECORDING_POSITION_TYPE_ID)?;

        self.counter_id = Some(counter_id);

        Ok(true)
    }

    /// The registration the counter was asked for under, which is what
    /// [`RecordingPos::release`] gives back.
    pub const fn registration_id(&self) -> i64 {
        self.registration_id
    }

    /// The values-region slot, once the claim has read it.
    pub const fn counter_id(&self) -> Option<i32> {
        self.counter_id
    }

    /// Where the recording has got to, or `None` while the driver has not
    /// answered the add yet.
    pub fn value<Access>(&self, counters: &CountersReader<'_, Access>) -> Option<i64> {
        counters.value(self.counter_id?)
    }

    /// Publish the position a recording has reached
    /// (`RecordingSession.java:240`, `setRelease`).
    pub fn set_position(
        &self,
        counters: &CountersReader<'_, ReadWrite>,
        position: i64,
    ) -> Option<()> {
        counters.set_value(self.counter_id?, position)
    }

    /// Whether this counter is still the one for `recording_id`
    /// (`RecordingPos.isActive`, `:292-298`) — the question a reader asks after
    /// holding an id across a recording that may have ended and been replaced.
    #[must_use]
    pub fn is_active<Access>(
        &self,
        counters: &CountersReader<'_, Access>,
        recording_id: i64,
    ) -> bool {
        let Some(counter_id) = self.counter_id else {
            return false;
        };

        if counters
            .get(counter_id)
            .is_none_or(|descriptor| descriptor.type_id != ARCHIVE_RECORDING_POSITION_TYPE_ID)
        {
            return false;
        }

        counters
            .key(counter_id)
            .and_then(|key| parse_key(&key))
            .is_some_and(|identity| identity.recording_id == recording_id)
    }

    /// Give the counter back, without waiting for the driver to say it is gone
    /// (`RecordingSession.java:120` over `CloseHelper.close`).
    ///
    /// # Errors
    ///
    /// [`CounterError`] if the command could not be written or sent.
    pub fn release<C: Counters>(&self, client: &mut C) -> Result<(), CounterError> {
        client.release_counter(self.registration_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARCHIVE_ID: i64 = 42;

    /// The four offsets, which the C reader's packed key struct pins to
    /// 0/8/12/16 as well (`aeron_archive_recording_pos.c:24-30`, `:149-152`) —
    /// so this is the one test that fails if either side moves.
    #[test]
    fn the_key_is_the_four_fields_in_the_references_order() {
        assert_eq!(0, RECORDING_ID_OFFSET);
        assert_eq!(8, SESSION_ID_OFFSET);
        assert_eq!(12, SOURCE_IDENTITY_LENGTH_OFFSET);
        assert_eq!(16, SOURCE_IDENTITY_OFFSET);

        let key = key(7, 1001, "aeron:ipc", ARCHIVE_ID);

        assert_eq!(8 + 4 + 4 + 9 + 8, key.len());
        assert_eq!(7i64, i64::from_le_bytes(key[0..8].try_into().unwrap()));
        assert_eq!(1001i32, i32::from_le_bytes(key[8..12].try_into().unwrap()));
        assert_eq!(9i32, i32::from_le_bytes(key[12..16].try_into().unwrap()));
        assert_eq!(b"aeron:ipc", &key[16..25]);
        assert_eq!(
            ARCHIVE_ID,
            i64::from_le_bytes(key[25..33].try_into().unwrap())
        );
    }

    /// The archive id sits behind the identity, so its offset moves with the
    /// identity's length — which is the one part of the layout that does.
    #[test]
    fn the_archive_id_follows_the_identity_it_is_written_behind() {
        let short = key(1, 2, "", ARCHIVE_ID);
        let long = key(1, 2, "aeron:udp?endpoint=localhost:3333", ARCHIVE_ID);

        assert_eq!(16 + 8, short.len());
        assert_eq!(
            ARCHIVE_ID,
            i64::from_le_bytes(short[16..24].try_into().unwrap())
        );
        assert_eq!(
            ARCHIVE_ID,
            i64::from_le_bytes(long[long.len() - 8..].try_into().unwrap())
        );
    }

    /// 112 - 16 - 8 = 88, and the length written is the length **after** the
    /// cut, not the string's (`RecordingPos.java:106-109`).
    #[test]
    fn an_identity_too_long_for_the_key_is_cut_and_says_so() {
        let identity = "a".repeat(200);
        let key = key(1, 2, &identity, ARCHIVE_ID);

        assert_eq!(SOURCE_IDENTITY_MAX_LENGTH, 88);
        assert_eq!(16 + 88 + 8, key.len());
        assert_eq!(
            88i32,
            i32::from_le_bytes(key[12..16].try_into().unwrap()),
            "the length is what was written"
        );
    }

    #[test]
    fn a_key_reads_back_as_what_went_in() {
        let key = key(7, 1001, "aeron:ipc", ARCHIVE_ID);

        assert_eq!(
            Some(RecordedIdentity {
                recording_id: 7,
                session_id: 1001,
                source_identity: "aeron:ipc".to_owned(),
                archive_id: ARCHIVE_ID,
            }),
            parse_key(&key)
        );
    }

    #[test]
    fn a_key_that_was_never_written_is_not_read() {
        // The region is 112 zeroed bytes, so this is what a slot with no
        // counter in it reads as: a length of 0 and everything else zero.
        assert_eq!(
            Some(RecordedIdentity {
                recording_id: 0,
                session_id: 0,
                source_identity: String::new(),
                archive_id: 0,
            }),
            parse_key(&[0u8; layout::COUNTER_KEY_LENGTH]),
            "zeroes are a key that says zero, not a malformed one"
        );

        assert_eq!(None, parse_key(&[]));
        assert_eq!(None, parse_key(&[0u8; 12]), "no room for the length");

        // A length that would put the archive id past the end of the region.
        let mut key = key(1, 2, "aeron:ipc", ARCHIVE_ID);
        key[12..16].copy_from_slice(&500i32.to_le_bytes());
        assert_eq!(None, parse_key(&key));

        let mut negative = key.clone();
        negative[12..16].copy_from_slice(&(-1i32).to_le_bytes());
        assert_eq!(None, parse_key(&negative));
    }

    #[test]
    fn the_label_is_the_four_numbers_and_the_channel() {
        assert_eq!(
            "rec-pos: 7 1001 33 aeron:udp?endpoint=localhost:3333 - archiveId=42",
            label(7, 1001, 33, "aeron:udp?endpoint=localhost:3333", ARCHIVE_ID)
        );
    }

    /// The channel is cut so that the suffix still fits — and the suffix's
    /// length is measured, because it carries the archive id's digits.
    #[test]
    fn a_channel_too_long_for_the_label_is_cut_before_the_archive_id() {
        let channel = "a".repeat(1000);

        let cut = label(7, 1001, 33, &channel, ARCHIVE_ID);
        assert_eq!(layout::COUNTER_LABEL_LENGTH_MAX, cut.len());
        assert!(cut.ends_with(" - archiveId=42"), "{cut}");

        // A longer id leaves less room for the channel, so the cut moves.
        let longer = label(7, 1001, 33, &channel, -1234567890123456789);
        assert_eq!(layout::COUNTER_LABEL_LENGTH_MAX, longer.len());
        assert!(longer.ends_with(" - archiveId=-1234567890123456789"));
    }

    #[test]
    fn a_short_channel_is_written_whole() {
        let short = label(7, 1001, 33, "aeron:ipc", ARCHIVE_ID);

        assert!(short.len() < layout::COUNTER_LABEL_LENGTH_MAX);
        assert_eq!("rec-pos: 7 1001 33 aeron:ipc - archiveId=42", short);
    }

    /// The identity's 88-byte cut is the key's and the channel's is the
    /// label's: a long identity does not shorten the channel, and the other way
    /// round.
    #[test]
    fn the_two_cuts_do_not_reach_each_other() {
        let identity = "i".repeat(200);
        let channel = "c".repeat(50);

        let key = key(1, 2, &identity, ARCHIVE_ID);
        assert_eq!(
            SOURCE_IDENTITY_MAX_LENGTH,
            parse_key(&key).unwrap().source_identity.len()
        );

        let label = label(1, 2, 3, &channel, ARCHIVE_ID);
        assert!(label.contains(&channel), "the channel is written whole");
        assert!(label.ends_with(" - archiveId=42"));
    }
}
