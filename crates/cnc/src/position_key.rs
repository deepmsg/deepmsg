//! The key a stream-position counter carries
//! (`aeron-client/src/main/c/concurrent/aeron_counters_manager.h:37-47`).
//!
//! It lives in `deepmsg-cnc` rather than in the driver because both sides of
//! the file need it: the driver writes it, and the counters exporter reads it
//! without linking the driver. The label is for a human; this key is what says
//! *which* stream a counter measures.

/// One stream-position counter's key: the registration, the session, the
/// stream, and the channel as the client sent it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamPositionKey<'a> {
    /// The registration id the counter belongs to — a publication's, or a
    /// subscription's.
    pub registration_id: i64,
    /// The session it measures.
    pub session_id: i32,
    /// The stream it measures.
    pub stream_id: i32,
    /// The channel, as the client sent it, including any parameters.
    pub channel: &'a [u8],
}

impl<'a> StreamPositionKey<'a> {
    /// The key's width, and the one the counter manager is told about.
    pub const LENGTH: usize = 112;

    /// Where the channel field starts: after the two ids and the two lengths.
    const CHANNEL_OFFSET: usize = 20;

    /// How much of a channel the key can hold.
    pub const CHANNEL_LENGTH_MAX: usize = Self::LENGTH - Self::CHANNEL_OFFSET;

    /// The key as the bytes a reader would find.
    ///
    /// The reference builds this as a designated initialiser
    /// (`aeron_position.c:41-42`), so everything it does not name is **zero** —
    /// including the channel's tail past what fits. Writing a shorter array and
    /// leaving the rest of the buffer alone would put whatever was on the stack
    /// into a file other processes read.
    pub fn encode(&self) -> [u8; StreamPositionKey::LENGTH] {
        let mut out = [0u8; Self::LENGTH];
        let channel_length = self.channel.len().min(Self::CHANNEL_LENGTH_MAX);

        out[..8].copy_from_slice(&self.registration_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.session_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.stream_id.to_le_bytes());
        #[allow(clippy::cast_possible_truncation)] // bounded by CHANNEL_LENGTH_MAX
        out[16..20].copy_from_slice(&(channel_length as i32).to_le_bytes());
        out[Self::CHANNEL_OFFSET..Self::CHANNEL_OFFSET + channel_length]
            .copy_from_slice(&self.channel[..channel_length]);

        out
    }

    /// Read a key back.
    ///
    /// The reference hands every reader `sizeof(record->key)` bytes and lets
    /// the counter's type decide how many are meaningful; this asks the
    /// channel length in the key and refuses one that would run past the
    /// record, rather than returning a view that reaches outside it.
    pub fn decode(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < Self::LENGTH {
            return None;
        }

        let registration_id = i64::from_le_bytes(bytes[..8].try_into().ok()?);
        let session_id = i32::from_le_bytes(bytes[8..12].try_into().ok()?);
        let stream_id = i32::from_le_bytes(bytes[12..16].try_into().ok()?);
        let channel_length =
            usize::try_from(i32::from_le_bytes(bytes[16..20].try_into().ok()?)).ok()?;
        if channel_length > Self::CHANNEL_LENGTH_MAX {
            return None;
        }

        let channel = &bytes[Self::CHANNEL_OFFSET..Self::CHANNEL_OFFSET + channel_length];
        Some(Self {
            registration_id,
            session_id,
            stream_id,
            channel,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::StreamPositionKey;

    #[test]
    fn a_key_round_trips() {
        let key = StreamPositionKey {
            registration_id: 42,
            session_id: 7,
            stream_id: 1002,
            channel: b"aeron:udp?endpoint=localhost:33433",
        };

        assert_eq!(Some(key), StreamPositionKey::decode(&key.encode()));
    }

    #[test]
    fn a_channel_that_does_not_fit_is_cut_and_round_trips_as_the_cut_one() {
        let long = vec![b'x'; StreamPositionKey::CHANNEL_LENGTH_MAX + 20];
        let key = StreamPositionKey {
            registration_id: 1,
            session_id: 2,
            stream_id: 3,
            channel: &long,
        };

        let encoded = key.encode();
        let decoded = StreamPositionKey::decode(&encoded).expect("a valid key");
        assert_eq!(StreamPositionKey::CHANNEL_LENGTH_MAX, decoded.channel.len());
        assert_eq!(1, decoded.registration_id);
        assert_eq!(2, decoded.session_id);
        assert_eq!(3, decoded.stream_id);
    }

    #[test]
    fn a_short_key_is_refused() {
        assert_eq!(None, StreamPositionKey::decode(&[0_u8; 8]));
    }

    #[test]
    fn a_channel_length_past_the_record_is_refused() {
        let mut bytes = [0_u8; StreamPositionKey::LENGTH];
        bytes[16..20].copy_from_slice(
            &(i32::try_from(StreamPositionKey::CHANNEL_LENGTH_MAX + 1).unwrap()).to_le_bytes(),
        );
        assert_eq!(None, StreamPositionKey::decode(&bytes));
    }
}
