//! SBE codecs, one generated crate per schema.
//!
//! SBE covers the archive and cluster control protocols, the mark files and the
//! node-state file (M19). It does not cover the UDP wire protocol — those frames
//! are hand-written structs, not generated (`docs/protocol/wire-frames.md`) —
//! nor the archive `recording.log`, hand-rolled inside `deepmsg-archive`.
//!
//! This crate is a façade and nothing more: it puts the five generated crates
//! under one roof so a caller need not know which schema a message came from.
//! The codecs live in `crates/codec-*`, are generated from `schemas/` by
//! `just gen`, and are checked in (ADR-0004). Regeneration replaces their
//! `src/` wholesale, which is why nothing hand-written may go there; each
//! crate's `Cargo.toml` is ours and the generator never touches it.

#![forbid(unsafe_code)]

pub use deepmsg_codec_archive as archive;
pub use deepmsg_codec_archive_mark as archive_mark;
pub use deepmsg_codec_cluster as cluster;
pub use deepmsg_codec_cluster_mark as cluster_mark;
pub use deepmsg_codec_cluster_node_state as cluster_node_state;

#[cfg(test)]
mod tests {
    use super::*;

    /// The façade has one job, and one way to get it wrong that nothing else
    /// would catch: the generator's table pairing a schema with the wrong
    /// crate. Each crate states the id of the schema it was built from, so a
    /// mix-up shows up here rather than as bytes that decode into nonsense
    /// much later.
    ///
    /// The ids are `docs/compat.md`'s, read back through the generated code.
    /// That is not what verifies those rows — a golden byte test is — but it
    /// does establish that the crates underneath are the schemas they claim.
    #[test]
    fn each_crate_is_the_schema_it_is_named_for() {
        assert_eq!(
            (archive::SBE_SCHEMA_ID, archive::SBE_SCHEMA_VERSION),
            (101, 14)
        );
        assert_eq!(
            (
                archive_mark::SBE_SCHEMA_ID,
                archive_mark::SBE_SCHEMA_VERSION
            ),
            (100, 2)
        );
        assert_eq!(
            (cluster::SBE_SCHEMA_ID, cluster::SBE_SCHEMA_VERSION),
            (111, 17)
        );
        assert_eq!(
            (
                cluster_mark::SBE_SCHEMA_ID,
                cluster_mark::SBE_SCHEMA_VERSION
            ),
            (110, 2)
        );
        assert_eq!(
            (
                cluster_node_state::SBE_SCHEMA_ID,
                cluster_node_state::SBE_SCHEMA_VERSION
            ),
            (112, 10)
        );
    }
}
