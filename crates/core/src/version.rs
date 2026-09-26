//! Semantic versioning helpers and the CnC file version this build speaks.
//!
//! The reference implementation stores a packed semantic version in the
//! first field of the CnC metadata block. Clients must refuse a CnC file
//! whose *major* component differs (ADR-0001); a higher minor signals
//! additive layout changes and is tolerated.

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cnc_version_round_trips() {
        assert_eq!(semantic_version_major(CNC_VERSION), 0);
        assert_eq!(semantic_version_minor(CNC_VERSION), 2);
        assert_eq!(semantic_version_patch(CNC_VERSION), 0);
    }
}
