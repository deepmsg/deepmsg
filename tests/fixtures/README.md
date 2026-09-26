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
