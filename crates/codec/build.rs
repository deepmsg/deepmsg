//! Build script: check that the five SBE schemas are where `just gen` expects
//! them.
//!
//! It is a leftover of the placeholder era and it is thin on purpose: the
//! generator ran once, its crates are checked in beside this one (ADR-0004),
//! and nothing in this build reads `schemas/`. The check duplicates what
//! `just gen` already does — it refuses to run against a missing schema — so
//! its only live effect is to fail `cargo build` early and by name.

use std::{env, fs, path::Path};

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let schemas_dir = Path::new(&manifest_dir).join("../../schemas");

    // Keep in sync with schemas/README.md and docs/compat.md.
    const EXPECTED: &[&str] = &[
        "aeron-archive-codecs.xml",
        "aeron-archive-mark-codecs.xml",
        "aeron-cluster-codecs.xml",
        "aeron-cluster-mark-codecs.xml",
        "aeron-cluster-node-state-codecs.xml",
    ];

    for name in EXPECTED {
        let path = schemas_dir.join(name);
        let bytes = fs::read(&path)
            .unwrap_or_else(|e| panic!("missing SBE schema {}: {e}", path.display()));
        // Cheap sanity check that the file is an SBE schema at all.
        let head = String::from_utf8_lossy(&bytes[..bytes.len().min(256)]);
        assert!(
            head.contains("sbe:messageSchema"),
            "{} does not look like an SBE schema",
            path.display()
        );
        println!("cargo:rerun-if-changed={}", path.display());
    }
}
