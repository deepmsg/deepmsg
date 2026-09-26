# ADR-0005: Vocabulary and naming policy

- Status: accepted (2026-09-25)

## Context

deepmsg reuses Aeron's domain vocabulary wholesale, and some of it feels
unintuitive at first ("driver", "image", "term"). Renaming concepts is
tempting, but three costs dominate:

- the byte-level compatibility baseline (ADR-0001) freezes a large part of
  the vocabulary at process boundaries;
- the entire Aeron ecosystem — papers, docs, tooling — is keyed to these
  names;
- several intuitive-sounding replacements actively mislead: "broker" implies
  the process owns payload data (it does not; IPC payloads never pass through
  the driver), and "segment" is already taken by archive disk files and must
  never collide with "term".

## Decision

### Three tiers of vocabulary

1. **Tier 1 — byte boundary (frozen).** URI parameter names, the
   `aeron-spy:` scheme, on-disk file names (`cnc.dat`, stream files), SBE
   message and field names (schemas are forked verbatim), counter type ids.
   Renaming any of these breaks interop with the reference implementation.
2. **Tier 2 — interop-visible strings (keep aligned).** Counter label text
   and error-message wording stay close to the reference so that reference
   tooling (`aeron-stat` and friends) reads a deepmsg CnC sensibly.
   Genuinely new deepmsg counters and labels may carry a `deepmsg-` prefix.
3. **Tier 3 — code vocabulary (ours).** Rust type, function and module
   names; CLI binary names; the `DEEPMSG_*` environment prefix.

### Concept names are kept

Aeron's concept nouns stay unchanged in deepmsg: driver, CnC, conductor,
sender, receiver, term, image, publication, subscription, stream, session,
channel, position, spy, linger, MDC/MDS. Their plain-English meaning lives
in `GLOSSARY.md`, not in new names.

### No C-era abbreviation jargon in code

Tier 3 identifiers spell things out: `sender_limit` (not `snd_lmt`),
`publisher_limit` (not `pub_lmt`), `receive_high_watermark` (not
`rcv_hwm`). Industry-standard short forms are exempt: `mtu`, `uri`, `cnc`,
`id`. When a comment cites reference source code, the original identifier
is kept alongside (`/* snd_lmt in the reference */`).

### Branding lives at the process boundary

Binaries and configuration carry the deepmsg name (`deepmsg-driver`,
`deepmsg-stat`, `DEEPMSG_*`), so ownership is clear without inventing a
parallel concept vocabulary.

### New concepts get fresh names

deepmsg-only mechanisms with no reference counterpart are named freely and
descriptively, and are added to `GLOSSARY.md` marked `free`.

## Consequences

- The "unintuitive name" problem is solved once, centrally, by the glossary
  — not permanently, by a translation tax on every document and
  conversation.
- Review gains a checklist item: no new abbreviation jargon; the glossary is
  updated when a term is introduced.
- If a term ever must diverge from the reference, this ADR is amended
  first.
