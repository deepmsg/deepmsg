# Test fixtures

## `cnc-header.bin`

The first 512 bytes of a `cnc.dat` written by a **default-configured
`aeronmd` 1.53.2** (commit `664f58e705`), captured on 2026-09-26.

The file it came from was **48,238,592 bytes**. That number is asserted in
`tests/integration/cnc_fixture.rs` as the length the region arithmetic has to
reproduce from the metadata alone, so a regenerated fixture may need that
constant updated — which is the point of writing it down.

Only a prefix is committed, because a default driver's CnC file is 46 MB and
40 MB of that is the counter regions. The prefix carries the entire metadata
block (128 bytes) plus the first 384 bytes of the to-driver region, which is
enough to pin every field's offset and width and the packed version encoding.
The contents of the regions are covered by the synthetic buffers in
`deepmsg-cnc` and by the interop suite.

### Regenerating

    export AERON_DIR=/tmp/deepmsg-fixture-$$
    /path/to/aeron/cppbuild/Release/binaries/aeronmd &
    PID=$!
    sleep 4                       # until cnc.dat exists and is non-zero
    stat -c %s "$AERON_DIR/cnc.dat"          # note this; update the test if it changed
    head -c 512 "$AERON_DIR/cnc.dat" > tests/fixtures/cnc-header.bin
    kill -TERM "$PID"; wait "$PID"
    rm -rf "$AERON_DIR"

Signal the pid you spawned, never `pkill aeronmd`: a developer may have their
own driver running, and killing it would take their work with it. The harness
in `tests/src/driver.rs` exists partly so that no test has to remember this.

## `counters-metadata.bin`

The first **46 counter metadata records** — 46 × 512 = 23,552 bytes — of the
same captured `cnc.dat`: the counters a default-configured `aeronmd` 1.53.2
allocates before it publishes anything.

Captured from the same run as `cnc-header.bin` (2026-09-27), from the start of
the counters metadata region, which `docs/protocol/cnc-layout.md` places at
128 + to_driver + to_clients = 2,098,176 for the default layout.

The records are the contract `AeronStat` reads: state, type id, a four-byte
little-endian index as the key, the label, and — in the values region — the
index as the registration id and `-1` as the owner. What a *golden test* may
compare directly is everything except three things that are runtime or build
identity rather than contract:

- the counter values (byte counts, cycle times, the pid of the run),
- the two labels that name a build (`Errors: …`, `Aeron software: …`), and
- the labels' runtime suffixes: the threading mode, the resolver name and the
  duty-cycle thresholds, which a driver appends from its own configuration.

`crates/driver/tests/system_counters.rs` compares up to the first `:` of each
label for exactly that reason, and says so where it does it.

### Regenerating

Same as `cnc-header.bin`, with one extra step before the kill:

    python3 -c "
    import sys
    data = open(sys.argv[1], 'rb').read()[2098176:2098176 + 46 * 512]
    open('tests/fixtures/counters-metadata.bin', 'wb').write(data)
    " "$AERON_DIR/cnc.dat"

If a future reference build changes a label, the diff will be in this file —
which is the point of committing it whole rather than as a list of strings.

## `sbe/`

Sixty-two SBE messages encoded by the reference's own `sbe-tool` output, with
the reference's own decoder's reading of each one recorded in `sbe/golden.tsv`.
They are the golden for `deepmsg-codec`, and they are a directory of their own
because they are a set rather than a capture: `sbe/README.md` says how the set
is built, what the value rule is, and what it deliberately leaves uncovered.
