# Reference material

## Reference implementations (checked out next to this repo)

| Path | What it is | Used for |
|---|---|---|
| `../aeron` | Aeron 1.53.2 (C client + driver sources, Java archive + cluster) | source of truth for every byte-level decision |
| `../aeron-rs` | UnitedTraders pure-Rust client (CnC 0.0.16 era, compat ceiling ~1.44) | prior art; also a catalogue of pitfalls to avoid |

## The reference driver, for interop tests

The interop suite (`tests/interop`, feature `interop`) runs against a real
driver process, so it needs a built `aeronmd`:

    ../aeron/cppbuild/Release/binaries/aeronmd

`DEEPMSG_REF_AERONMD` overrides that path. The harness refuses a binary whose
`-v` output does not name Aeron 1.53.2 at `664f58e705`, which turns "the wrong
driver is on `PATH`" into a clear message rather than a confusing decode
failure deep inside a test.

Each test gets its own aeron directory under `/dev/shm`, and the driver is
started with `aeron.dir.delete.on.shutdown=true` so a clean signal removes it
again — that is what keeps a day of test runs from filling the tmpfs.

With no driver available the interop tests print a `SKIPPED` line and pass:
the feature is opt-in, and failing would make it unusable on any machine
without the checkout. The cost is real and worth stating plainly — a green run
that skipped verified nothing. What keeps that honest is
`tests/integration/cnc_fixture.rs`, which needs no checkout and does run in
CI; it decodes a committed header captured from a real driver.

## The reference archive tests, and the shim

The reference's archive suite is the acceptance instrument for `P2`: it asks
whether our archive can replace the Java one, and it is worth asking because
its cases are not ours. It is not this repository's test suite — that is
`crates/archive/tests/` — and the two do different jobs. `crates/archive/tests/reference-cases.tsv`
is the join: every case the suite runs, with what this repository owes it.

### Why it takes a second build directory

Those tests reach the archive through a **compile-time constant**.
`JAVA_EXECUTABLE` is injected by CMake (`aeron-archive/src/test/c/CMakeLists.txt:22`)
and every spawn uses it (`TestArchive.h:122`, `TestStandaloneArchive.h:144`,
`TestMediaDriver.h:66`). Configure a second build directory with
`-DJava_JAVA_EXECUTABLE=archive-shim` and the whole suite spawns the shim
instead, with no reference source touched.

    cd ../aeron
    cppbuild/cmake/bin/cmake -S . -B cppbuild/deepmsg-archive-shim \
      -DCMAKE_BUILD_TYPE=Release \
      -DJava_JAVA_EXECUTABLE=<this repo>/target/debug/archive-shim \
      -DAERON_SYSTEM_TESTS=ON -DAERON_UNIT_TESTS=ON
    cmake --build cppbuild/deepmsg-archive-shim -j"$(nproc)"

Use `cppbuild/cmake/bin/cmake` (4.3): the reference tree needs CMake 3.30+ and
a distribution's is usually older. `AERON_ALL_JAR` is **not** passed — the top
level derives it from the Gradle build (`CMakeLists.txt:374`), which is also
why a fresh clone needs `./gradlew` to have run at least once.

### The three main classes

The suite spawns **three**, which is a measurement and not something the plan
guessed: `ArchivingMediaDriver` (214 times, `TestArchive.h:107`), `Archive`
(56, `TestStandaloneArchive.h:136`) and `MediaDriver` (6,
`TestMediaDriver.h:58`). A shim that knew only the first would forward the
other sixty-two to the real Java archive and report on a system nobody
configured.

`crates/tools/src/bin/archive-shim.rs` reads a **file beside itself**,
`archive-shim.conf`:

    java=/usr/lib/jvm/java-21-openjdk-amd64/bin/java
    mode=transparent
    log=/tmp/deepmsg-archive-shim.log
    driver=<this repo>/target/debug/deepmsg-driver

Not the environment, and that is a measurement: `TestArchive.h:122` spawns with
`posix_spawn(..., envp = NULL)` and this glibc hands that child an **empty**
environment, `$PATH` included. The mode is one of:

| mode | what the archive and driver halves are |
|---|---|
| `transparent` | the real java, both. The control: it is what says the shim is invisible |
| `hybrid` | our driver, the reference's archive. The entry physical exam for the archive track |
| `deepmsg` | ours for both. The acceptance criterion itself; the archive is not written yet |

`archive-shim --deepmsg-shim-table` prints the classes it knows, which is what
the runner checks a run against.

### Running it

    DEEPMSG_SHIM_MODE=transparent .github/scripts/archive-suite.py \
      ../aeron/cppbuild/deepmsg-archive-shim  out.tsv

It runs the nine binaries itself rather than through `ctest`, because the
reference registers **one test per binary** (`aeron-archive/src/test/c/CMakeLists.txt:40`)
— so `ctest` says "9/9 passed" when a binary lost three cases — and because
`ctest` at the top of the build tree runs every unit test in the reference.
gtest writes the per-case XML; nothing in the reference tree is modified.

Three things it checks rather than trusts: the cases that ran are the cases in
the committed ledger; the shim saw no main class it does not know; and it
writes the shim's configuration before the run, overwriting whatever a previous
one left.

**Never run two of these at once, and never run one beside the `Release`
build's archive suite.** Both build directories use `/dev/shm/aeron-<user>`
with `aeron.dir.delete.on.start=true`, so the second deletes the first's
driver, and the tests' ports are hard-coded.

**A case that hangs is a case that hangs forever.** The suite's readiness wait
polls for the mark file with no timeout (`TestArchive.h:144-152`), so a process
that never comes up is a hang and not a failure. `timeout` around a binary
does not clean up either: killing the test skips its teardown, which is what
signals the shim, so the two halves are left orphaned *and holding the test's
stdout* — a shell pipeline after it will wait for an EOF that never comes.
