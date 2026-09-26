---
name: compat-row
description: Add, change or verify a row in docs/compat.md, the gated byte-compatibility matrix. Use when touching a byte-level constant, a protocol or schema version, the CnC file, or an on-disk format — and before asserting compatibility anywhere in prose.
when_to_use: When a version number, protocol constant or on-disk format changes; when adding a compatibility claim; when a schema under schemas/ is bumped.
paths:
  - "docs/compat.md"
  - "schemas/**"
---

# Changing docs/compat.md

`docs/compat.md` is not documentation — it is a list of **testable contracts**.
Its own rules say a row counts as verified only when the interop suite
(`tests/interop`, feature `interop`) or a golden byte test covers it, and that
changing the table without extending the tests first is a review-blocking
offence. Take that literally.

## Procedure

1. **Find the authority.** Resolve it in the 1.53.2 checkout with the
   `verify-citation` skill. The `Reference source` cell accepts only two kinds
   of value:

   - an upstream path that resolves under `../aeron`, or
   - an in-repo artifact (`schemas/…`, `tests/…`).

   Never a private note, an unchecked path, or a bare URL.

2. **Extend the test first.** Add the interop or golden case that fails before
   the change and passes after it. A row with no covering test is a claim, not
   a contract.

3. **Then edit the row**, preserving the four columns' meanings:

   | Column | Meaning |
   |---|---|
   | Surface | the contract's name in this codebase |
   | Constant | the named constant, or `—` where the format is hand-rolled |
   | Value | the value, with the semantic version where one exists |
   | Reference source | where that value is authoritative |

4. **Re-read the rules at the foot of the file** — the CnC major/minor
   acceptance rule, and the pointer that wire-protocol constants belong in
   `deepmsg-core::logbuffer` and `docs/protocol/`.

## Do not

- **Duplicate row contents here or in any other document.** `docs/compat.md`
  is the single source of truth; this skill describes only the procedure.
- **Change a value to match the code.** The matrix records the reference's
  behaviour (`docs/adr/0001`). If the implementation disagrees, the
  implementation is the defect.
- **Edit neighbouring rows in the same change** without a test covering each
  one you touch.
