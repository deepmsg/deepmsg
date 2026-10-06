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

    .github/scripts/archive-suite.py <build-dir> <out.tsv> \
        [--mode transparent|hybrid|deepmsg] [--driver <binary>] [--log <shim-log>]

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

# The fallback runs one case per process, and a case that hangs there is one
# case, not a suite: it gets ctest's per-test timeout (`CMakeLists.txt:45`),
# not the per-binary one. 204 cases at 300 s each would be a day.
PER_CASE_TIMEOUT = 60

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


def read_report(report: Path) -> dict[str, str]:
    """A gtest XML as case name -> outcome, or {} if it was never written.

    `{}` is not "no failures": a binary that died before gtest could write
    leaves nothing behind, and the caller has to treat that as unknown.
    """
    if not report.is_file():
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


def run_binary(
    build: Path,
    binary: str,
    reports: Path,
    cases: list[str],
    driver: str | None,
    java: str,
) -> dict[str, str]:
    """One binary, one gtest XML, a mapping of case name to outcome.

    With a fallback, because one red does not stay one red here. Measured:
    `aeron_archive_test` runs its first eight cases green, `shouldConnectFromTwoClientsUsingIpc`
    fails on the archive client's connect timeout, and the *next* case's SetUp
    meets `ERROR: (1000): MediaDriver has been shutdown` and the process exits
    with gtest having written nothing. Running it as one process reported 74
    cases as `absent` on the strength of a single failure.

    So when the aggregate report is missing or empty and the ledger says what
    the binary holds, it is re-run one case per process. Slower, and only for a
    binary that already failed.
    """
    path = build / "binaries" / binary
    if not os.access(path, os.X_OK):
        sys.exit(f"{path} is not an executable; build {build} first")

    # Once, not per case: `working_directory` walks the build tree.
    cwd = working_directory(build, binary)
    report = reports / f"{binary}.xml"
    try:
        subprocess.run(
            [str(path), f"--gtest_output=xml:{report}"],
            cwd=cwd,
            timeout=PER_BINARY_TIMEOUT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.TimeoutExpired:
        # The suite's own readiness wait has no timeout, so a process that
        # never comes up is a hang. Naming the cases here is the only way the
        # report does not read as "the whole binary passed".
        print(f"  TIMEOUT after {PER_BINARY_TIMEOUT}s", flush=True)

    outcomes = read_report(report)
    if outcomes or not cases:
        return outcomes

    print(f"\n    it wrote no report; one process per case instead ({len(cases)})", flush=True)
    for index, case in enumerate(cases):
        one = reports / f"{binary}.{index}.xml"
        try:
            subprocess.run(
                [str(path), f"--gtest_output=xml:{one}", f"--gtest_filter={case}"],
                cwd=cwd,
                timeout=PER_CASE_TIMEOUT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except subprocess.TimeoutExpired:
            outcomes[case] = "timeout"
            continue
        outcomes.update(read_report(one) or {case: "absent"})
        # Between cases too, and for the same reason: a case that dies without
        # tearing down leaves its supervised pair holding the control port, and
        # the next case then fails to start a driver rather than failing on its
        # own merits. `kill_leftovers` is called by the caller between binaries;
        # inside the fallback the cases are the loop.
        for line in kill_leftovers(driver, java):
            print(f"\n    killed a leftover between cases: {line}", flush=True)
    return outcomes


def write_shim_config(shim: Path, *, mode: str, java: str, log: Path, driver: str | None) -> None:
    """Beside the binary, which is the only place it looks.

    Paths are absolute because the shim's working directory is whatever the C
    test's was, and one of the nine runs from a different subdirectory than the
    others.

    `driver=` is written only when one was named. It is what a mode that
    replaces the reference's driver points the shim at, and without it `hybrid`
    has nothing to start — but writing a path nobody gave would be this script
    guessing, which is the failure it exists to avoid.
    """
    config = shim.parent / "archive-shim.conf"
    lines = [
        "# Written by archive-suite.py. Read by the shim, which has no environment.",
        f"java={Path(java).resolve()}",
        f"mode={mode}",
        f"log={log.resolve()}",
    ]
    if driver is not None:
        lines.append(f"driver={Path(driver).resolve()}")
    config.write_text("\n".join(lines) + "\n")


def kill_leftovers(driver: str | None, java: str) -> list[str]:
    """Kill what a previous binary left behind, and say what it killed.

    A hybrid run does not start one process, it starts a pair — the shim
    supervises our driver and the reference's archive — and when a test binary
    dies without tearing down, the pair outlives it. The archive half holds the
    control port the reference hard-codes (`CMakeLists.txt:42`), so the next
    binary cannot bind and reports zero cases, which reads as "this binary
    produced nothing" rather than "something else is holding the port".

    Matched on `/proc/<pid>/exe` and not on the environment, because a
    supervised half is started with `-D` arguments rather than environment
    variables and has no `AERON_DIR` to grep for.
    """
    killed = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            exe = os.readlink(entry / "exe")
        except OSError:
            continue
        cmdline = ""
        try:
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
        except OSError:
            pass

        ours = driver is not None and exe == str(Path(driver).resolve())
        # The archive half: the reference's java, running an archive main class.
        theirs = exe == str(Path(java).resolve()) and "io.aeron.archive." in cmdline
        if ours or theirs:
            killed.append(f"{entry.name} {exe} {'(archive half)' if theirs else '(driver)'}")
            try:
                os.kill(int(entry.name), 9)
            except OSError:
                pass
    return killed


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
    parser.add_argument(
        "--driver",
        help="the binary a mode replaces the reference's driver with; required by "
        "every mode but `transparent`, because the shim will not guess",
    )
    args = parser.parse_args()

    # Absolute before anything uses it. A binary is spawned with `cwd=` set to
    # its own directory (`working_directory`), and a relative executable path is
    # resolved by the child *after* that chdir — so `../aeron/cppbuild/…` stops
    # pointing at the build and the failure is a bare FileNotFoundError naming a
    # path that plainly exists.
    args.build = args.build.resolve()

    shim = shim_build(args.build)
    if shim is not None:
        if not os.access(args.java, os.X_OK):
            sys.exit(f"--java {args.java} is not an executable file")
        if args.mode != "transparent" and args.driver is None:
            # Refused here rather than left to the shim, which would refuse too
            # but only once a spawned process is already waiting on a driver
            # that is never going to appear — and the suite's readiness wait has
            # no timeout, so that reads as a hang.
            sys.exit(f"--mode {args.mode} needs --driver: it replaces the reference's driver with ours")
        if args.driver is not None and not os.access(args.driver, os.X_OK):
            sys.exit(f"--driver {args.driver} is not an executable file")
        write_shim_config(shim, mode=args.mode, java=args.java, log=args.log, driver=args.driver)
        print(f"shim       {shim} (mode {args.mode})")
        if args.driver is not None:
            print(f"driver     {args.driver}")
    else:
        print("shim       none (the real java)")

    # The log is appended to, so a stale one would report another run's classes.
    if args.log.is_file():
        args.log.unlink()

    reports = Path(tempfile.mkdtemp(prefix="p2-0b-run-"))
    try:
        # Before the first binary too: a run started on top of an earlier run's
        # leftovers fails on the control port, and the failure lands on whichever
        # binary happened to go first.
        for line in kill_leftovers(args.driver, args.java):
            print(f"killed a leftover before starting: {line}")

        # Before the run, because a binary that dies is re-run case by case and
        # that needs to know what the binary holds.
        expected = ledger_cases()
        cases_for: dict[str, list[str]] = {}
        for case, binary in expected.items():
            cases_for.setdefault(binary, []).append(case)

        outcomes: dict[str, str] = {}
        for binary in BINARIES:
            print(f"  {binary}", end=" ... ", flush=True)
            found = run_binary(
                args.build,
                binary,
                reports,
                sorted(cases_for.get(binary, [])),
                args.driver,
                args.java,
            )
            outcomes.update(found)
            left = kill_leftovers(args.driver, args.java)
            if left:
                print()
                for line in left:
                    print(f"    killed a leftover: {line}")
            passed = sum(1 for v in found.values() if v == "passed")
            print(f"{passed}/{len(found)}", flush=True)
    finally:
        shutil.rmtree(reports, ignore_errors=True)

    missing = sorted(set(expected) - set(outcomes))
    extra = sorted(set(outcomes) - set(expected))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w") as out:
        out.write("# case\tbinary\tstatus\n")
        for case in sorted(expected):
            out.write(f"{case}\t{expected[case]}\t{outcomes.get(case, 'absent')}\n")

    # Counted by outcome, not as "everything that is not failed". A case with no
    # outcome at all is not a pass, and the first version of this line said it
    # was — 45 unnamed cases reported as green in the one summary a reader sees.
    failed = [c for c, s in outcomes.items() if s == "failed"]
    tally: dict[str, int] = {}
    for status in outcomes.values():
        tally[status] = tally.get(status, 0) + 1
    print(f"\n{len(outcomes)} cases: " + ", ".join(f"{n} {s}" for s, n in sorted(tally.items())))
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
