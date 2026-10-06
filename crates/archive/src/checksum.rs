//! The recording checksums: CRC-32 and CRC-32C, by the names the reference
//! accepts.
//!
//! An archive may be configured to checksum what it records
//! (`aeron.archive.record.checksum`, `Archive.java:640`), and the value is a
//! **name** that selects an implementation (`Checksums.newInstance`,
//! `checksum/Checksums.java:65-80`): `CRC-32`, `CRC-32C`, or a class name that
//! resolves to one of the two. The same names answer on the reading side
//! (`ArchiveTool <dir> checksum <className>`), so a recording written under one
//! is a recording verified under it.
//!
//! # What the two algorithms are
//!
//! They are not this build's invention and not the reference's either: CRC-32 is
//! IEEE 802.3 (`0xEDB88320` reflected) and CRC-32C is Castagnoli
//! (`0x82F63B78` reflected), both initialised to `0xFFFFFFFF` and finalised by
//! complementing — which is what `java.util.zip.CRC32` and `CRC32C` compute, and
//! Agrona's two providers are those classes. What is a **contract** here is the
//! name-to-implementation mapping and the width (`int32`, which is what the
//! frame's session-id field holds); the polynomials are the world's.
//!
//! The tables are built at compile time by a `const fn`, so there is no lazy
//! initialisation, no allocation, and no dependency.
//!
//! # Verified against the reference, not against the specification
//!
//! Matching a specification is not the same as matching the code this build has
//! to interoperate with, so the four check values below were read out of
//! `Checksums.crc32()` and `Checksums.crc32c()` themselves — a direct buffer,
//! `"123456789"`:
//!
//! ```text
//! CRC-32  cbf43926   CRC-32C e3069283
//! one zero byte: CRC-32 d202ef8d, CRC-32C 527d5351
//! ```
//!
//! Two things that probe turned up, both worth knowing before writing anything
//! else against these:
//!
//! * the reference's checksums read a **raw address**
//!   (`Checksum.compute(address, offset, length)`), so a buffer handed to them
//!   has to be **direct**. A heap `byte[]` wrapped in an `UnsafeBuffer` gives an
//!   address the intrinsified CRC-32 stub then reads as off-heap memory, and the
//!   JVM dies with a `SIGSEGV` in `StubRoutines::updateBytesCRC32` rather than
//!   reporting anything;
//! * Agrona reaches `java.util.zip.CRC32` by **reflection**, so a JVM running
//!   either provider needs `--add-opens java.base/java.util.zip=ALL-UNNAMED` on
//!   top of the two flags Agrona needs for itself
//!   ([`deepmsg_tests::driver::AGRONA_JVM_ARGS`]).

/// A checksum implementation, by the name the reference gives it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Checksum {
    /// `CRC-32`, IEEE 802.3.
    Crc32,
    /// `CRC-32C`, Castagnoli.
    Crc32c,
}

/// The IEEE 802.3 polynomial, reflected (`0xEDB88320`).
const IEEE_POLYNOMIAL: u32 = 0xEDB8_8320;

/// The Castagnoli polynomial, reflected (`0x82F63B78`).
const CASTAGNOLI_POLYNOMIAL: u32 = 0x82F6_3B78;

/// A byte-at-a-time table for a reflected polynomial, built at compile time.
const fn table(polynomial: u32) -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0;

    while index < 256 {
        let mut crc = index as u32;
        let mut bit = 0;

        while bit < 8 {
            crc = if 0 != crc & 1 {
                (crc >> 1) ^ polynomial
            } else {
                crc >> 1
            };

            bit += 1;
        }

        table[index] = crc;
        index += 1;
    }

    table
}

const IEEE_TABLE: [u32; 256] = table(IEEE_POLYNOMIAL);
const CASTAGNOLI_TABLE: [u32; 256] = table(CASTAGNOLI_POLYNOMIAL);

impl Checksum {
    /// The checksum of `bytes`, as the `int32` a frame's session-id field holds.
    ///
    /// Signed because that is what the field is: the reference's
    /// `Checksum.compute` returns an `int` and its `frameSessionId` stores one,
    /// so a checksum with the top bit set is a **negative** number everywhere it
    /// is written down. The arithmetic inside is unsigned.
    pub fn compute(self, bytes: &[u8]) -> i32 {
        let table = match self {
            Self::Crc32 => &IEEE_TABLE,
            Self::Crc32c => &CASTAGNOLI_TABLE,
        };

        let mut crc = 0xFFFF_FFFF_u32;

        for &byte in bytes {
            let index = ((crc ^ u32::from(byte)) & 0xFF) as usize;
            crc = (crc >> 8) ^ table[index];
        }

        !crc as i32
    }

    /// The name this implementation is configured by, which is the short one of
    /// the two the reference accepts.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC-32",
            Self::Crc32c => "CRC-32C",
        }
    }

    /// The implementation a configured name selects, or `None` for a name that
    /// is not one of the two.
    ///
    /// The class names are the reference's own aliases (`Checksums.java:65-80`),
    /// kept because an archive configured by a **class name** is one this build
    /// has to read: a recording made under `org.agrona.checksum.Crc32c` is a
    /// recording checksummed with CRC-32C, and refusing the name would refuse
    /// the file.
    ///
    /// A name that resolves to neither is `None` rather than an error: what to
    /// do about it is the caller's — an archive starting up refuses it, a reader
    /// of somebody else's file may not.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "CRC-32" | "io.aeron.archive.checksum.Crc32" | "org.agrona.checksum.Crc32" => {
                Some(Self::Crc32)
            }

            "CRC-32C" | "io.aeron.archive.checksum.Crc32c" | "org.agrona.checksum.Crc32c" => {
                Some(Self::Crc32c)
            }

            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The check values every CRC-32 and CRC-32C implementation is measured
    /// against: the ones their specifications name (`"123456789"`).
    #[test]
    fn the_two_algorithms_are_the_two_algorithms() {
        assert_eq!(
            0xCBF4_3926_u32 as i32,
            Checksum::Crc32.compute(b"123456789"),
            "CRC-32's own check value"
        );
        assert_eq!(
            0xE306_9283_u32 as i32,
            Checksum::Crc32c.compute(b"123456789"),
            "and CRC-32C's"
        );

        // The two disagree, which is the whole reason a name selects one: a
        // build that computed the wrong one would verify none of the recordings
        // made under the other.
        assert_ne!(
            Checksum::Crc32.compute(b"123456789"),
            Checksum::Crc32c.compute(b"123456789")
        );
    }

    /// The empty input and a single byte, which is where an implementation that
    /// forgot the initial or the final complement shows up: both would still
    /// pass a longer vector's *shape* checks.
    #[test]
    fn the_ends_of_the_range_are_the_standard_ones() {
        assert_eq!(
            0_u32 as i32,
            Checksum::Crc32.compute(b""),
            "the empty CRC-32 is zero"
        );
        assert_eq!(0_u32 as i32, Checksum::Crc32c.compute(b""));

        // Known values for a single zero byte under each polynomial, which is
        // what catches an initial value that is not `0xFFFFFFFF`.
        assert_eq!(0xD202_EF8D_u32 as i32, Checksum::Crc32.compute(&[0]));
        assert_eq!(0x527D_5351_u32 as i32, Checksum::Crc32c.compute(&[0]));
    }

    /// A checksum with the top bit set is **negative** in the field it is
    /// stored in, which is not a mistake to be corrected: the reference stores
    /// an `int`.
    #[test]
    fn a_checksum_can_be_negative_and_that_is_the_field() {
        let computed = Checksum::Crc32.compute(b"123456789");

        assert!(
            computed < 0,
            "CRC-32 of the check string has its top bit set"
        );
        assert_eq!(0xCBF4_3926_u32 as i32, computed);
    }

    /// Every name the reference accepts selects one of the two, and a name it
    /// does not is nobody's.
    #[test]
    fn the_names_are_the_references_own() {
        for (name, expected) in [
            ("CRC-32", Checksum::Crc32),
            ("io.aeron.archive.checksum.Crc32", Checksum::Crc32),
            ("org.agrona.checksum.Crc32", Checksum::Crc32),
            ("CRC-32C", Checksum::Crc32c),
            ("io.aeron.archive.checksum.Crc32c", Checksum::Crc32c),
            ("org.agrona.checksum.Crc32c", Checksum::Crc32c),
        ] {
            assert_eq!(Some(expected), Checksum::from_name(name), "{name}");
        }

        for name in ["", "crc32", "CRC32", "java.util.zip.CRC32", "SHA-256"] {
            assert_eq!(None, Checksum::from_name(name), "{name}");
        }
    }
}
