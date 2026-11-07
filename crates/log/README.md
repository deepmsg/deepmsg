# deepmsg-log

The application-side log facade. It writes **local JSON files** and nothing
else: no collector, no network, no stdout, no stderr. Collection is a node-level
service that reads the files later, so turning it off degrades queries to `grep`
and leaves the process untouched.

The design is `analysis/observability/deepmsg-l0a-plan-log-crate.md`; the
pipeline it feeds is `analysis/observability/plan-2026-11-05-observability-pipeline.md`.

## Off by default

`DEEPMSG_LOG_DIR` unset or empty means logging is off: no thread, no directory,
no file. Production turns it on in its supervisor unit; tests and benchmarks pay
nothing. `DEEPMSG_NODE_ID` is required when logging is enabled.

| variable | default | meaning |
|---|---|---|
| `DEEPMSG_LOG_DIR` | unset = **off** | where the JSON files go |
| `DEEPMSG_LOG_LEVEL` | `info` | a level, or `component=level` overrides, e.g. `driver=debug,cluster=info` |
| `DEEPMSG_LOG_ROLL` | `100MB:5` | `<size>:<file count>`; sizes are binary, the count is the **total** on disk |
| `DEEPMSG_NODE_ID` | required when enabled | written into every line and used as the `node_id` stream field |
| `DEEPMSG_LOG_CPU` | unset = unpinned | CPU for the writer thread; the binary must supply a pinner |

## Usage

```rust
use deepmsg_log::{Component, Event, Level};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Off unless DEEPMSG_LOG_DIR is set. `open(Config)` is the same without
    // installing the process-global logger; tests use that.
    let Some(handle) = deepmsg_log::init_from_env(Component::Driver)? else {
        return Ok(());
    };

    deepmsg_log::event(Level::Info, Component::Driver, Event::RoleChange)
        .message("became leader")
        .field_str("role", "leader")
        .field_i64("term", 7)
        .emit();

    handle.flush();
    handle.shutdown();
    Ok(())
}
```

A full queue is a **counted drop**, never a block. The writer reports the count
as its own `log.dropped` event, so the loss is queryable next to everything else.
`handle.dropped_lines()` is the same number for an in-process reader.

## Application vocabulary

`Component` and `Event` are the **infrastructure's** controlled vocabulary. An
application with its own components and events does not add members here; it
implements `Name` for its own types and keeps its own list and test:

```rust
use deepmsg_log::{Guard, Level, Name};

#[derive(Clone, Copy)]
enum Engine { Container }

impl Name for Engine {
    fn as_str(&self) -> &'static str { "engine" }
}

#[derive(Clone, Copy)]
enum EngineEvent { Start, Role }

impl Name for EngineEvent {
    fn as_str(&self) -> &'static str {
        match self {
            EngineEvent::Start => "engine.start",
            EngineEvent::Role => "engine.role",
        }
    }
}

// The application's list is what `DEEPMSG_LOG_LEVEL=engine=debug` may name.
static COMPONENTS: [&str; 1] = ["engine"];

let _guard = Guard::from_env_with(Engine::Container, &COMPONENTS, None)?;
deepmsg_log::event_named(Level::Info, Engine::Container, EngineEvent::Start)
    .field_str("symbol", "AAPL")
    .emit();
```

`Name::as_str` returns `&'static str`, so the encoder stays a string writer and
the same event cost applies. The rule that keeps dashboards stable travels with
the names: keep one `ALL` list and one "no duplicates" test in the application's
crate, and treat a rename as a dashboard change. `Config::from_env_with` and
`Guard::from_env_with` are the configuration half; `event_named` (and
`Handle::event_named` / `Logger::event_named`) are the emission half.

## The line

One JSON object per line, fields flat (not nested under `fields`), fixed order:

```json
{"ts":"2026-11-05T12:34:56.123456789Z","level":"info","component":"driver","node_id":"node-1","event":"role.change","msg":"became leader","role":"leader","term":7}
```

- `ts` is RFC3339Nano UTC (nine fractional digits, `Z`).
- `event` is a controlled name (`Event`, or an application's `Name`), not free
  text.
- `component` and `node_id` are the VictoriaLogs stream fields; `level`,
  `role` and `event` are ordinary query fields.
- `truncated: true` appears only when a value did not fit the 512-byte record.
- Reserved names (`ts`, `level`, `component`, `node_id`, `event`, `msg`,
  `truncated`, `trace_id`, `span_id`, `target`, `file`, `line`) cannot be set as
  extra fields.

## Files and rotation

The current file is `<component>-<safe(node_id)>.json`; rotated files are
`<component>-<safe(node_id)>.1.json`, `.2.json`, … so every name still matches a
`*.json` include glob. A write that would cross the size limit rotates first; a
single oversized batch rotates after it lands. The oldest file is removed, which
makes the configured count the total on disk. Opening a file that is already
full rotates it before the first append. With a count of 1 the policy is
truncate-in-place, and the newest line is kept.

## Costs

The calling thread allocates nothing: the record is a 512-byte stack buffer and
the queue is preallocated (ADR-0003; `tests/zero_alloc.rs` asserts this with
100 000 events). The writer thread does allocate, and it can be pinned to a CPU
through `Config::cpu` + `Config::pinner`; the crate itself has no `unsafe`, so
the binary that owns a platform seam supplies the pinning function.

`cargo bench -p deepmsg-bench --bench logging` is the caller-side harness; the
readings and the C14 budget are in the L0a plan's implementation record.
