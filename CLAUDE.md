# deepmsg — notes for AI sessions

A from-scratch Rust rewrite of the open-source Aeron stack — client, media
driver and archive — targeting byte-level compatibility with **Aeron 1.53.2**.

Read `README.md` first; it is short and authoritative. This file adds only
what a fresh session tends to get wrong.

## Gates

Green before you call anything done. This is exactly what CI runs:

    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets -- -D warnings
    cargo test --workspace

Interop tests additionally need the reference checkout (`docs/reference.md`):

    cargo test -p deepmsg-tests --features interop

`just` wraps all of these (`just lint`, `just test`, `just interop`, …).

## Branch discipline

`main` is what the world clones, so **never commit to it directly**. Every
change — a feature, a bug fix, a documentation edit, a config tweak — goes on
a branch and lands through a pull request, so CI runs on it:

    git switch -c <type>/<short-slug>     # feat/ fix/ docs/ chore/ refactor/
    … commit …
    git push -u origin <branch>
    gh pr create --fill

If a change looks too small to need a branch, it is still not too small.

## Non-negotiables

Each rule has one authority. Read that file, not this table.

| Rule | Authority |
|---|---|
| `unsafe` only in `core::buffer`, `cnc`, and the driver syscall shim | `docs/adr/0002` |
| No async runtime, no FFI, no allocation on hot paths | `docs/adr/0003` |
| Aeron's concept nouns are kept; no C-era abbreviations in code | `docs/adr/0005` |
| Every byte-level claim cites an upstream `file:line` | `docs/roadmap.md` |
| `schemas/*.xml` are verbatim forks; a deviation is recorded in `docs/compat.md` first | `schemas/README.md` |
| A `docs/compat.md` row changes only with a matching interop or golden test | `docs/compat.md` |

## The `(Mxx)` markers in code comments

Comments such as `//! Subscriptions (M05): …` carry the maintainer's private
cross-reference keys. They are **deliberate**. Do not remove, renumber or
"tidy" them, and do not propose doing so. Everything a reader meets first —
`README.md` and all of `docs/` — is kept free of them.

## Citing the reference

The reference is a sibling checkout of Aeron 1.53.2 (commit `664f58e705`),
plus `../aeron-rs` for prior art; `docs/reference.md` documents the layout.

**Resolve every path before you cite it.** Guessed upstream paths are this
repo's most common defect; several exist in the tree today, all in code
comments. A near-miss is still a miss — check for a module subdirectory
(`concurrent/`, `protocol/`, `uri/`, `media/`, `service/`) and for a
`_driver_` infix before concluding that a file does not exist.

`media/` and `concurrent/` exist in **both** the client and the driver tree
but hold different files, so know which module you are in. And where a module
exists in both C and Java, the **C implementation is the authority**; Java is
the authority only where no C exists — the archive server and the cluster.

## Docs map

| Question | File |
|---|---|
| Why 1.53.2, and what "compatible" means | `docs/adr/0001` |
| Numbers, versions, byte contracts | `docs/compat.md` |
| What this build measures, and on what | `docs/benchmarks.md` |
| Scope, phases, gates that apply from day one | `docs/roadmap.md` |
| Vocabulary | `GLOSSARY.md` |
| Reference checkout layout | `docs/reference.md` |
| Crate layout | `README.md` |
