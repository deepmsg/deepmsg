//! Length guards in front of the generated SBE decoders.
//!
//! The flyweight decoders in `crates/codec/cluster*` **panic on a short
//! buffer**, exactly as the reference's `SbeMessageHeaderDecoder` throws. A
//! cluster node decodes messages that came off a UDP socket, so "short buffer"
//! is ordinary hostile input, not a bug in the caller — every decode therefore
//! goes through a guard first and the panic path is unreachable.
//!
//! The archive learned this the hard way in P2 (`control_adapter.rs`); this is
//! the same rule, moved to where all three cluster crates can use it.
//!
//! The guard answers a slice, not a decoded message: the generated codecs are
//! different types with different block lengths, and a generic "decode
//! anything" function would need a trait the codecs do not have. Callers write
//!
//! ```ignore
//! let block = guarded::with_length(buf, offset, BLOCK_LENGTH, "SessionMessageHeader")?;
//! let decoder = SessionMessageHeaderDecoder::default().wrap(ReadBuf::new(block), header);
//! ```
//!
//! which keeps the reference's decode order visible at the call site instead of
//! hiding it behind a generic.

/// A buffer was too short for the record the caller named.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeError {
    /// What the caller was decoding — a message name, so the log line says
    /// which parser to look at.
    pub what: &'static str,
    /// How many bytes were needed from `offset`.
    pub needed: usize,
    /// How many were available from `offset`.
    pub available: usize,
}

impl core::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}: needed {} bytes, {} available",
            self.what, self.needed, self.available
        )
    }
}

impl std::error::Error for DecodeError {}

/// The bytes from `offset` to the end, if there are at least `needed` of them.
pub fn with_length<'a>(
    buf: &'a [u8],
    offset: usize,
    needed: usize,
    what: &'static str,
) -> Result<&'a [u8], DecodeError> {
    let available = buf.len().saturating_sub(offset);
    if offset > buf.len() || available < needed {
        return Err(DecodeError {
            what,
            needed,
            available,
        });
    }
    Ok(&buf[offset..])
}

/// The same check when the caller already holds the slice it will decode.
pub fn at_least(buf: &[u8], needed: usize, what: &'static str) -> Result<(), DecodeError> {
    if buf.len() < needed {
        return Err(DecodeError {
            what,
            needed,
            available: buf.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exact_fit_passes_and_one_short_fails() {
        let buf = [0u8; 8];
        assert!(at_least(&buf, 8, "x").is_ok());
        assert_eq!(
            at_least(&buf, 9, "x"),
            Err(DecodeError {
                what: "x",
                needed: 9,
                available: 8
            })
        );
    }

    #[test]
    fn offset_past_the_end_is_the_same_error_as_short() {
        let buf = [0u8; 8];
        assert_eq!(
            with_length(&buf, 6, 4, "x"),
            Err(DecodeError {
                what: "x",
                needed: 4,
                available: 2
            })
        );
        assert!(with_length(&buf, 9, 1, "x").is_err());
    }

    #[test]
    fn a_zero_length_record_at_the_end_is_allowed() {
        let buf = [0u8; 8];
        assert_eq!(with_length(&buf, 8, 0, "x").unwrap().len(), 0);
    }
}
