---
name: verify-citation
description: Verify an Aeron reference path or `file:line` citation against the sibling `../aeron` checkout before writing it into deepmsg code or docs. Use before adding any reference path, any `:line` citation, or any claim about how the reference implementation behaves — guessed upstream paths are this repository's most common defect.
when_to_use: Before writing "the reference does X" into a comment or a doc; when filling a "Reference source" cell; whenever a path or line number may have been recalled rather than resolved.
---

# Verify a citation

Every byte-level claim in this repo must cite an upstream `file:line`
(`docs/roadmap.md`). A citation that does not resolve is worse than no
citation at all, because it reads as if it were checked.

## Procedure

1. **Resolve the path.**

       ls ../aeron/<path>

   A near-miss is still a miss. Before concluding that a file does not exist,
   look for a module subdirectory and for a `_driver_` infix:

   - Subdirectories that hold files you might expect at the module root:
     `concurrent/`, `protocol/`, `uri/`, `media/`, `util/`, `agent/`,
     `service/`, `collections/`, `command/`.
   - Under `aeron-driver/src/main/c/`, sender, receiver, conductor, context
     and version all carry a `_driver_` infix (`aeron_driver_sender.c`), while
     `aeron_flow_control.c` and `aeron_congestion_control.c` do not.
   - `media/` and `concurrent/` exist in **both** the client and the driver
     tree but hold different files. Confirm which module you are in.

2. **Read the line you cite.**

       sed -n '<line>p' ../aeron/<path>

   Cite a line only if it says what you claim. Prefer a line that will not
   drift — a named constant or a function signature survives edits better than
   a line inside a long method body.

3. **Pick the authority.** Where a module exists in both C and Java, cite the
   C implementation (`docs/adr/0001`). Java is the authority only where no C
   exists — the archive server and the cluster.

4. **Pin the version.** The baseline is 1.53.2, commit `664f58e705`. A path
   that resolves today may not resolve after an upstream bump, so a citation
   meant to survive one should say which baseline it was written against.

## Known-bad paths

[references/known-bad-paths.md](references/known-bad-paths.md) lists paths that
look plausible but do not exist at 1.53.2, together with where they actually
live. Read it before trusting any path you did not resolve with `ls`.
