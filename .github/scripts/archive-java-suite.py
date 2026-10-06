#!/usr/bin/env python3
"""Turn the g0-2 runner's JUnit XML into the Java suite's ledger and its result.

Two artifacts, because they have two lifetimes:

* **the ledger** (`reference-java-cases.tsv`) — the denominator, pinned. It is
  the set of case names the reference's *Java* archive suite runs at
  `664f58e705`, and it exists so that "we ran the whole suite" is a fact about
  a name set rather than a count (review E-3, the same lesson the C suite's
  `reference-cases.tsv` already carries). It is committed.
* **the result** — one row per case per run, with the outcome and the
  attribution. It is not committed; it is a measurement.

Why case level at all, when the runner already writes a class-level tsv: a
class verdict cannot show the thing P2-0c most needs to see. `skipped` is a
first-class outcome here — `TestMediaDriver.notSupportedOnCMediaDriver` is
`assumeFalse(shouldRunCMediaDriver())`, and `shouldRunCMediaDriver()` is true
exactly when `aeron.test.system.aeronmd.path` is set, which is the whole point
of this exam. A class that passed with most of its cases skipped is not
evidence, and only the case rows say so.

Case identity is `(classname, name)` from the XML. The `name` attribute is
JUnit's *display* name, which for a `@ParameterizedTest` is the argument
rendering without the method name — so uniqueness within a class is a property
to be checked, not assumed, and this refuses to write a ledger where it does
not hold.

A class that hangs produces no XML at all, so its case names are not in the run
that matters — but they are in the ledger's job description. `--fill DIR` takes
those names from a second report (a control run against the reference's own
Java driver, say) and records them as `absent`: the name is known, the run did
not happen, and those are different things. `absent` is never a pass.

    .github/scripts/archive-java-suite.py <xml-dir> <class-list> \
        --ledger crates/archive/tests/reference-java-cases.tsv [--write-ledger] \
        [--out out.tsv] [--fill <control-xml-dir>]
"""

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

LEDGER_HEADER = """\
# The reference's *Java* archive suite, one row per case — the denominator for
# the P2-0c entry exam and, later, for LA-1.
#
# The sibling of `reference-cases.tsv`, which does this job for the reference's
# *C* suite. The two instruments are not redundant: the C suite drives through
# the C client from a separate process, this one through the Java client with
# the archive in-process. The C ledger tracks what this crate owes; this one is
# only ever a name set — the exam asks whether the driver can carry the
# reference's archive, not whether our archive has caught up.
#
# Keys are the reference's own: `name` is JUnit's display name and `class` is
# its `classname`. A `@ParameterizedTest`'s display name carries the arguments
# and not the method name, so (class, name) must be unique within a class —
# names are rendered with a method prefix by
# `junit-parameterized-displayname.init.gradle`, and this refuses to write the
# file if two cases in one class still collide.
#
# Growth rule: regenerating from a different reference commit is a deliberate
# edit that shows up as a diff of names. Nothing is deleted quietly.
#
# case<TAB>class
"""


def outcome(case: ET.Element) -> str:
    if case.find("failure") is not None or case.find("error") is not None:
        return "failed"
    if case.find("skipped") is not None:
        return "skipped"
    return "passed"


def read_reports(xml_dir: Path) -> tuple[dict[tuple[str, str], str], dict[str, str]]:
    """(class, display name) -> outcome, and class -> why it produced nothing."""
    outcomes: dict[tuple[str, str], str] = {}
    broken: dict[str, str] = {}
    for path in sorted(xml_dir.glob("TEST-*.xml")):
        root = ET.parse(path).getroot()
        suites = [root] if root.tag == "testsuite" else list(root)
        cases = [c for s in suites for c in s.iter("testcase")]
        if not cases:
            # A class that produced no <testcase> at all: a filter that matched
            # nothing, an abstract name handed to `--tests`, a JVM that died.
            # That must not read as "no failures".
            broken[path.stem.removeprefix("TEST-")] = root.get("timestamp", "no cases")
            continue
        for case in cases:
            outcomes[(case.get("classname"), case.get("name"))] = outcome(case)
    return outcomes, broken


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("xml_dir", type=Path)
    parser.add_argument("class_list", type=Path)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--out", type=Path)
    parser.add_argument(
        "--write-ledger",
        action="store_true",
        help="write the ledger from this run; without it the ledger is only checked against",
    )
    parser.add_argument(
        "--fill",
        type=Path,
        help="a second report to take case names from for classes this run did not reach; "
        "those cases are recorded as `absent`, never as a pass",
    )
    args = parser.parse_args()

    wanted = [line.strip() for line in args.class_list.read_text().splitlines() if line.strip()]
    outcomes, broken = read_reports(args.xml_dir)

    filled: dict[tuple[str, str], str] = {}
    if args.fill:
        from_control, _ = read_reports(args.fill)
        filled = {k: "absent" for k in from_control if k not in outcomes}
        if filled:
            print(f"{len(filled)} case names taken from {args.fill}, recorded as absent")
        outcomes.update(filled)

    trouble = False

    for cls in sorted(broken):
        print(f"NO CASES AT ALL: {cls} ({broken[cls]})")
        trouble = True

    ran_classes = {cls for cls, _ in outcomes}
    for cls in wanted:
        # Gradle writes a nested class's `classname` as the full `Outer$Inner`,
        # so an entry in the class list is satisfied by itself or by anything
        # under it. Matching on `Outer` alone would fail every nested entry —
        # which is what the first version of this check did.
        if not any(ran == cls or ran.startswith(cls + "$") for ran in ran_classes):
            print(f"CLASS PRODUCED NOTHING: {cls}")
            trouble = True

    seen: dict[tuple[str, str], int] = {}
    for key in outcomes:
        seen[key] = seen.get(key, 0) + 1
    collisions = sorted(k for k, n in seen.items() if n > 1)
    if collisions:
        print(f"\n{len(collisions)} AMBIGUOUS CASE NAMES — the ledger cannot be written:")
        for cls, name in collisions[:10]:
            print(f"  {cls} :: {name}")
        print("(two cases in one class share a display name; the key needs the method name)")
        trouble = True

    rows = sorted(outcomes)

    if args.write_ledger:
        if trouble:
            print("\nrefusing to write a ledger from a run that did not hold up")
            return 1
        args.ledger.parent.mkdir(parents=True, exist_ok=True)
        with args.ledger.open("w") as out:
            out.write(LEDGER_HEADER)
            for cls, name in rows:
                out.write(f"{name}\t{cls}\n")
        print(f"\nledger written: {args.ledger} ({len(rows)} cases)")
    elif args.ledger.is_file():
        expected = set()
        for line in args.ledger.read_text().splitlines():
            if line.startswith("#") or not line.strip():
                continue
            name, cls = line.split("\t")
            expected.add((cls, name))
        missing = sorted(expected - set(rows))
        extra = sorted(set(rows) - expected)
        if missing:
            print(f"\nIN THE LEDGER BUT DID NOT RUN ({len(missing)}):")
            for cls, name in missing[:20]:
                print(f"  {cls} :: {name}")
            trouble = True
        if extra:
            print(f"\nRAN BUT NOT IN THE LEDGER ({len(extra)}) — the ledger is out of date:")
            for cls, name in extra[:20]:
                print(f"  {cls} :: {name}")
            trouble = True

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        with args.out.open("w") as out:
            out.write("# case\tclass\tstatus\tattribution\n")
            for cls, name in rows:
                out.write(f"{name}\t{cls}\t{outcomes[(cls, name)]}\t\n")

    tally: dict[str, int] = {}
    for value in outcomes.values():
        tally[value] = tally.get(value, 0) + 1
    print(f"\n{len(rows)} cases: " + ", ".join(f"{v} {k}" for k, v in sorted(tally.items())))
    if args.out:
        print(f"written to {args.out}")

    return 1 if trouble else 0


if __name__ == "__main__":
    sys.exit(main())
