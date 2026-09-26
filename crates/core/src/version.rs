//! Semantic versioning helpers and the CnC file version this build speaks.
//!
//! The reference stores a packed semantic version in the first field of the
//! CnC metadata block. A client refuses a file whose *major* differs
//! (ADR-0001), and — following the Java client rather than the C one — also a
//! file whose *minor* is older than its own. See
//! [`CncVersionCompatibility`] for which reference each rule comes from.

/// CnC semantic version targeted by this build: `0.2.0`.
///
/// Source of truth: `aeron-client/src/main/c/aeron_cnc_file_descriptor.h:26`
/// (`AERON_CNC_VERSION`) in the Aeron 1.53.2 reference checkout.
pub const CNC_SEMANTIC_VERSION: (u8, u8, u8) = (0, 2, 0);

/// Compose a semantic version into the packed representation used in the CnC
/// metadata block: `(major << 16) | (minor << 8) | patch`.
pub const fn semantic_version_compose(major: u8, minor: u8, patch: u8) -> i32 {
    ((major as i32) << 16) | ((minor as i32) << 8) | patch as i32
}

/// Major component of a packed semantic version.
pub const fn semantic_version_major(version: i32) -> u8 {
    ((version >> 16) & 0xFF) as u8
}

/// Minor component of a packed semantic version.
pub const fn semantic_version_minor(version: i32) -> u8 {
    ((version >> 8) & 0xFF) as u8
}

/// Patch component of a packed semantic version.
pub const fn semantic_version_patch(version: i32) -> u8 {
    (version & 0xFF) as u8
}

/// The packed CnC version constant written/read by this build.
pub const CNC_VERSION: i32 = semantic_version_compose(
    CNC_SEMANTIC_VERSION.0,
    CNC_SEMANTIC_VERSION.1,
    CNC_SEMANTIC_VERSION.2,
);

/// Whether a CnC file's version is one this build can use.
///
/// Two rules, from two different references. Knowing which is which matters
/// before "fixing" either:
///
/// - **The major must match.** `aeron-client/src/main/c/aeron_cnc_file_descriptor.c:103`
///   — the C reader's check, and the authority wherever both languages have
///   one (ADR-0001).
/// - **The file's minor must be at least ours.** `aeron-client/src/main/java/io/aeron/CommonContext.java:1432`
///   — the Java client's *additional* check. The C client does not implement
///   it, so the two references genuinely diverge here and this build follows
///   the stricter one. The divergence is written down in
///   `docs/protocol/cnc-layout.md`.
///
/// The patch component is never compared. A file is classified in the
/// reference's order — readiness first — so a driver that has not published
/// yet reads as [`CncVersionCompatibility::NotReady`] rather than as a
/// mismatch of some kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CncVersionCompatibility {
    /// Majors match and the file is not older than this build.
    Compatible,
    /// `cnc_version == 0`: the file exists but its metadata has not been
    /// published yet. This is the readiness gate, not a failure — retry.
    NotReady,
    /// Different major: the layout may have changed incompatibly. Fatal.
    MajorMismatch,
    /// Same major, older minor: the file predates fields this build expects.
    /// Fatal, per the Java rule cited above.
    InsufficientMinor,
}

/// Classify a CnC file whose version field reads `file_version`.
pub const fn check_cnc_version(file_version: i32) -> CncVersionCompatibility {
    if 0 == file_version {
        return CncVersionCompatibility::NotReady;
    }

    if semantic_version_major(CNC_VERSION) != semantic_version_major(file_version) {
        return CncVersionCompatibility::MajorMismatch;
    }

    if semantic_version_minor(file_version) < semantic_version_minor(CNC_VERSION) {
        return CncVersionCompatibility::InsufficientMinor;
    }

    CncVersionCompatibility::Compatible
}

impl CncVersionCompatibility {
    /// Whether waiting and looking again could produce a different answer.
    ///
    /// Only [`CncVersionCompatibility::NotReady`] can: a mismatch is a
    /// property of the file, not of how long we waited.
    pub const fn is_retryable(self) -> bool {
        matches!(self, Self::NotReady)
    }
}

/// Render a packed version the way the reference does, `major.minor.patch`.
pub fn format_version(version: i32) -> String {
    format!(
        "{}.{}.{}",
        semantic_version_major(version),
        semantic_version_minor(version),
        semantic_version_patch(version)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cnc_version_round_trips() {
        assert_eq!(semantic_version_major(CNC_VERSION), 0);
        assert_eq!(semantic_version_minor(CNC_VERSION), 2);
        assert_eq!(semantic_version_patch(CNC_VERSION), 0);
    }

    /// The reference driver always writes 0.2.0, so every case but
    /// `Compatible` at 0.2.0 can only be reached with a synthetic version --
    /// which is why the acceptance rules are unit-tested here rather than left
    /// to the interop suite.
    #[test]
    fn acceptance_follows_the_two_reference_rules() {
        use CncVersionCompatibility::{Compatible, InsufficientMinor, MajorMismatch, NotReady};

        // Readiness is checked first, so an unpublished file is never reported
        // as a mismatch.
        assert_eq!(NotReady, check_cnc_version(0));
        assert!(check_cnc_version(0).is_retryable());

        // The version this build speaks.
        assert_eq!(Compatible, check_cnc_version(CNC_VERSION));
        assert!(!check_cnc_version(CNC_VERSION).is_retryable());

        // Driver newer than us: its minor is higher, which is tolerated.
        assert_eq!(
            Compatible,
            check_cnc_version(semantic_version_compose(0, 3, 0))
        );
        // Patch is never compared.
        assert_eq!(
            Compatible,
            check_cnc_version(semantic_version_compose(0, 2, 9))
        );
        assert_eq!(
            Compatible,
            check_cnc_version(semantic_version_compose(0, 2, 255))
        );

        // Driver older than us: same major, lower minor. Fatal -- and this is
        // the Java-only rule, which the C reader does not apply.
        assert_eq!(
            InsufficientMinor,
            check_cnc_version(semantic_version_compose(0, 1, 0))
        );
        assert_eq!(
            InsufficientMinor,
            check_cnc_version(semantic_version_compose(0, 0, 9))
        );
        assert!(!check_cnc_version(semantic_version_compose(0, 1, 0)).is_retryable());

        // Different major: fatal regardless of everything else.
        assert_eq!(
            MajorMismatch,
            check_cnc_version(semantic_version_compose(1, 0, 0))
        );
        assert_eq!(
            MajorMismatch,
            check_cnc_version(semantic_version_compose(2, 9, 9))
        );
        // A higher minor does not rescue a major mismatch.
        assert_eq!(
            MajorMismatch,
            check_cnc_version(semantic_version_compose(1, 99, 0))
        );
    }

    #[test]
    fn formats_a_version_the_way_the_reference_does() {
        assert_eq!("0.2.0", format_version(CNC_VERSION));
        assert_eq!("1.53.2", format_version(semantic_version_compose(1, 53, 2)));
        assert_eq!("0.0.0", format_version(0));
    }
}
