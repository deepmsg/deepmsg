#!/usr/bin/env python3
"""Run the reference's archive C suite and report it one case at a time.

`ctest` cannot give what the acceptance needs. The reference registers one test
per *binary* (`aeron-archive/src/test/c/CMakeLists.txt:40`, `:53`), so its
output is "9/9 passed" — which is also what it says when one binary lost three
cases, because gtest only makes the process exit non-zero. And running `ctest`
at the top of the build tree runs the whole tree: the driver's unit tests, the
client's, everything, for a result about none of them.

So this runs the nine archive binaries itself and lets gtest write its own XML.
Nothing in the reference tree is modified.

    .github/scripts/archive-suite.py <build-dir> <out.tsv> [--log <shim-log>]

The build directory must be one configured with `-DJava_JAVA_EXECUTABLE` set to
`archive-shim` or to the real `java` — both are useful, and the two runs are the
control against each other. When it is the shim, this writes the shim's
configuration **as a file beside the shim binary**, which is where the shim
reads it: `TestArchive.h:122` spawns with `posix_spawn(..., envp = NULL)` and
this glibc hands that child an empty environment, so there are no environment
variables to set and none to forget. A stale configuration from another run is
overwritten rather than inherited.

Three things are checked rather than trusted:

* the cases that ran are the cases in `crates/archive/tests/reference-cases.tsv`
  — the committed ledger is the denominator, and a binary that quietly lost a
  case is what this reports;
* in a shim build, every main class the shim saw is one it knows, asked of the
  shim itself (`--deepmsg-shim-table`) so that the list has one home;
* nothing is run in parallel. The suite is serial by construction: the ports are
  hard-coded, and both build directories share `/dev/shm/aeron-<user>` with
  `aeron.dir.delete.on.start=true`, so two runs delete each other's driver.

Exits non-zero if a case failed, if the case set does not match the ledger, or
if the shim saw a class it does not know.
"""

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

BINARIES = [
    "aeron_archive_test",
    "aeron_archive_persistent_subscription_test",
    "aeron_archive_persistent_subscription_resilience_test",
    "aeron_archive_async_client_test",
    "aeron_archive_persistent_subscription_context_test",
    "aeron_archive_async_connect_leak_test",
    "archiveTestW",
    "persistentSubscriptionTestW",
    "persistentSubscriptionContextTestW",
]

# ctest's TIMEOUT for every one of these (`CMakeLists.txt:41`, `:54`), kept so
# that a hang costs what it costs under ctest rather than forever.
PER_BINARY_TIMEOUT = 300

# The committed denominator, two directories up from `.github/scripts`.
LEDGER = Path(__file__).resolve().parents[2] / "crates/archive/tests/reference-cases.tsv"


def working_directory(build: Path, binary: str) -> Path:
    """Where ctest would have run it: the directory of its own CTestTestfile.

    Found rather than assumed. The compile-time `ARCHIVE_DIR` is absolute, so it
    may not matter today — but "it may not matter" is how a run stops matching
    the thing it is standing in for.
    """
    for path in build.rglob("CTestTestfile.cmake"):
        if f'binaries/{binary}"' in path.read_text(errors="replace"):
            return path.parent
    return build


def shim_build(build: Path) -> Path | None:
    """The shim this tree was configured with, or None if it is the real java."""
    cache = build / "CMakeCache.txt"
    if not cache.is_file():
        sys.exit(f"{build} has no CMakeCache.txt; is it a configured build directory?")
    for line in cache.read_text(errors="replace").splitlines():
        if line.startswith("Java_JAVA_EXECUTABLE:"):
            configured = Path(line.split("=", 1)[1].strip())
            return configured if configured.name == "archive-shim" else None
    sys.exit(f"{cache} does not say what Java_JAVA_EXECUTABLE is")


def run_binary(build: Path, binary: str, reports: Path) -> dict[str, str]:
    """One binary, one gtest XML, a mapping of case name to outcome."""
    path = build / "binaries" / binary
    if not os.access(path, os.X_OK):
        sys.exit(f"{path} is not an executable; build {build} first")

    report = reports / f"{binary}.xml"
    command = [str(path), f"--gtest_output=xml:{report}"]
    try:
        subprocess.run(
            command,
            cwd=working_directory(build, binary),
            timeout=PER_BINARY_TIMEOUT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        # The suite's own readiness wait has no timeout, so a process that
        # never comes up is a hang. Naming the cases here is the only way the
        # report does not read as "the whole binary passed".
        print(f"  TIMEOUT after {PER_BINARY_TIMEOUT}s", flush=True)

    if not report.is_file():
        # A binary that died before gtest could write leaves nothing behind,
        # and "nothing" must not read as "no failures".
        return {}

    outcomes: dict[str, str] = {}
    for case in ET.parse(report).iter("testcase"):
        name = f"{case.get('classname')}.{case.get('name')}"
        if case.find("failure") is not None:
            outcomes[name] = "failed"
        elif case.find("skipped") is not None:
            outcomes[name] = "skipped"
        else:
            outcomes[name] = "passed"
    return outcomes


def write_shim_config(shim: Path, *, mode: str, java: str, log: Path) -> None:
    """Beside the binary, which is the only place it looks.

    Paths are absolute because the shim's working directory is whatever the C
    test's was, and one of the nine runs from a different subdirectory than the
    others.
    """
    config = shim.parent / "archive-shim.conf"
    config.write_text(
        "# Written by p2-0b-run.py. Read by the shim, which has no environment.\n"
        f"java={Path(java).resolve()}\n"
        f"mode={mode}\n"
        f"log={log.resolve()}\n"
    )


def ledger_cases() -> dict[str, str]:
    """The committed denominator, as case name -> binary."""
    cases: dict[str, str] = {}
    for line in LEDGER.read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        case, binary = line.split("\t")[:2]
        cases[case] = binary
    return cases


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("build", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument(
        "--mode",
        default="transparent",
        choices=("transparent", "hybrid", "deepmsg"),
        help="the mode to configure the shim with; ignored for a real-java build",
    )
    parser.add_argument(
        "--java",
        default=shutil.which("java") or "/usr/lib/jvm/java-21-openjdk-amd64/bin/java",
        help="the java the shim replaces",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=Path("/tmp/deepmsg-archive-shim.log"),
        help="the shim's log, checked for classes it did not recognise",
    )
    args = parser.parse_args()

    shim = shim_build(args.build)
    if shim is not None:
        if not os.access(args.java, os.X_OK):
            sys.exit(f"--java {args.java} is not an executable file")
        write_shim_config(shim, mode=args.mode, java=args.java, log=args.log)
        print(f"shim       {shim} (mode {args.mode})")
    else:
        print("shim       none (the real java)")

    # The log is appended to, so a stale one would report another run's classes.
    if args.log.is_file():
        args.log.unlink()

    reports = Path(tempfile.mkdtemp(prefix="p2-0b-run-"))
    try:
        outcomes: dict[str, str] = {}
        for binary in BINARIES:
            print(f"  {binary}", end=" ... ", flush=True)
            found = run_binary(args.build, binary, reports)
            outcomes.update(found)
            passed = sum(1 for v in found.values() if v == "passed")
            print(f"{passed}/{len(found)}", flush=True)
    finally:
        shutil.rmtree(reports, ignore_errors=True)

    expected = ledger_cases()
    missing = sorted(set(expected) - set(outcomes))
    extra = sorted(set(outcomes) - set(expected))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as out:
        out.write("# case\tbinary\tstatus\n")
        for case in sorted(expected):
            out.write(f"{case}\t{expected[case]}\t{outcomes.get(case, 'absent')}\n")

    failed = [c for c, s in outcomes.items() if s == "failed"]
    print(f"\n{len(outcomes)} cases ran, {len(outcomes) - len(failed)} passed, {len(failed)} failed")
    print(f"written to {args.out}")

    trouble = False
    if failed:
        print(f"\nFAILED ({len(failed)}):")
        for case in sorted(failed):
            print(f"  {case}")
        trouble = True
    if missing:
        print(f"\nIN THE LEDGER BUT DID NOT RUN ({len(missing)}):")
        for case in missing[:20]:
            print(f"  {case}")
        trouble = True
    if extra:
        print(f"\nRAN BUT NOT IN THE LEDGER ({len(extra)}) — the ledger is out of date:")
        for case in extra[:20]:
            print(f"  {case}")
        trouble = True

    if shim is not None:
        known = set(
            subprocess.run([str(shim), "--deepmsg-shim-table"], capture_output=True, text=True)
            .stdout.split()
        )
        seen = set(re.findall(r"\bmain=(\S+)", args.log.read_text(errors="replace")))
        seen.discard("")
        unknown = sorted(seen - known)
        print(f"\nshim saw {sorted(seen)}")
        if unknown:
            print(f"\nTHE SHIM SAW MAIN CLASSES IT DOES NOT KNOW: {unknown}")
            print("this run is not evidence about our archive; add them to the table first")
            trouble = True

    return 1 if trouble else 0


if __name__ == "__main__":
    sys.exit(main())
