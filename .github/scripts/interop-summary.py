#!/usr/bin/env python3
"""The interop job's summary: what ran, what stopped, and what the red means.

A raw list of failures is a queue of investigations. The point of this script is
to turn it into a list of **named gaps**, so that a red job says which slice of
the repair plan it is waiting for rather than "72 tests failed".

Usage:  interop-summary.py <results-dir> <status-file>
"""

import collections
import pathlib
import sys
import xml.etree.ElementTree as ET

# The first line of a failure message decides which gap it belongs to. This is a
# table of what has been observed; a message that matches nothing is reported as
# unattributed, which is itself information — it means a gap nobody has named.
CLUSTERS = [
    (
        "multicast channels are not served by this driver",
        "multicast (MDC/MDS) — unbuilt by design",
    ),
    (
        "Expected io.aeron.exceptions.RegistrationException to be thrown",
        "channel/parameter validation this driver accepts and the reference refuses",
    ),
    (
        "is not served by this driver, errorCodeValue=8",
        "a channel URI parameter with no meaning here yet (`group`, `*-ts-offset`)",
    ),
    ("unknown flow control strategy", "flow control suppliers other than static"),
    ("mismatched endpoint", "tag-based flow control groups"),
    ("failed to lookup address information", "name resolution, which has no agent here"),
    (
        "unexpected interrupt",
        "the harness interrupting a test that had already hung — a symptom, not a cause",
    ),
    ("Errors observed in", "the driver's error log was non-empty at the end of the test"),
    ("no response from MediaDriver", "a command the driver never answered"),
]


def main():
    results, status = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])

    suites = []
    for path in sorted(results.glob("TEST-*.xml")):
        root = ET.parse(path).getroot()
        failures = [
            ((node.get("message") or "").strip().splitlines() or [""])[0]
            for case in root.iter("testcase")
            for node in (*case.findall("failure"), *case.findall("error"))
        ]
        suites.append(
            {
                "name": root.get("name"),
                "tests": int(root.get("tests")),
                "skipped": int(root.get("skipped")),
                "failures": failures,
                "time": float(root.get("time")),
            }
        )

    verdicts = collections.Counter()
    stopped = []
    if status.exists():
        for line in status.read_text().splitlines():
            if "\t" not in line:
                continue
            name, verdict = line.split("\t", 1)
            verdicts[verdict] += 1

    # "Did not finish" means exactly that: no result file. A class that ran and
    # failed is a failure like any other and is counted above; a class the
    # default `test` task excludes never ran at all and is a third thing.
    ran = {s["name"] for s in suites}
    if status.exists():
        for line in status.read_text().splitlines():
            if "\t" not in line:
                continue
            name, verdict = line.split("\t", 1)
            if verdict != "ok" and not any(n == name or n.startswith(name + "$") for n in ran):
                stopped.append((name, verdict))

    tests = sum(s["tests"] for s in suites)
    failures = sum(len(s["failures"]) for s in suites)
    skipped = sum(s["skipped"] for s in suites)

    print("## The OSS system tests, against this build's driver\n")
    print("| | |")
    print("|---|---|")
    print(f"| classes attempted | {sum(verdicts.values())} |")
    print(f"| classes green | {verdicts['ok']} |")
    print(f"| classes with failures | {verdicts['failed']} |")
    print(f"| classes that produced no result | {len(stopped)} |")
    print(f"| tests | {tests} |")
    print(f"| tests skipped | {skipped} |")
    print(f"| test failures | {failures} |")
    print()

    if stopped:
        print("### Classes that did not finish\n")
        print(
            "A class here produced no result at all. The timeout is the reference's own\n"
            "problem to have: at least one of its tests waits on something this driver does\n"
            "not do in a loop with no timeout, and `Thread.yield()` swallows the interrupt\n"
            "its own `InterruptingTestCallback` sends. See `CounterTest.java:640`.\n"
        )
        for name, verdict in stopped:
            print(f"- `{name}` — {verdict}")
        print()

    clusters = collections.Counter()
    unattributed = collections.Counter()
    for suite in suites:
        for first in suite["failures"]:
            for needle, gap in CLUSTERS:
                if needle in first:
                    clusters[needle] = clusters.get(needle, 0) + 1
                    break
            else:
                unattributed[first[:100]] = unattributed.get(first[:100], 0) + 1

    print("### What the red is\n")
    print("| failures | the message says | the gap |")
    print("|---|---|---|")
    for needle, gap in CLUSTERS:
        if clusters[needle]:
            print(f"| {clusters[needle]} | `{needle[:58]}` | {gap} |")
    for first, count in unattributed.most_common(10):
        print(f"| {count} | `{first}` | **unattributed — a gap with no name yet** |")
    print()
    print(
        "A job failing on **named gaps** is doing its job. The unattributed rows are the\n"
        "interesting ones: each is either a new gap or a cluster this table should learn.\n"
    )


if __name__ == "__main__":
    main()
