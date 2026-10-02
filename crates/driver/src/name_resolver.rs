//! Name resolution: what turns the name in a channel into an address
//! (`aeron-driver/src/main/c/aeron_name_resolver.c`, 229 lines, and its
//! header's 111).
//!
//! The reference has three resolvers behind one interface — `default`,
//! `csv_table` and `driver` (`aeron_name_resolver.h:25-27`) — and five things
//! each of them does (`:39-56`): **resolve** a name to an address, **look** a
//! name up (which answers with a *name* rather than an address, and is how the
//! driver's resolver asks its neighbors), **start**, **do work** on its own
//! clock, and **close**.
//!
//! This module is the interface and the default resolver, which is the one this
//! build has always had: a synchronous turn through the system's own lookup
//! (`udp_channel::resolve_host_and_port`, the port of
//! `aeron_default_name_resolver_resolve`).
//!
//! The driver's own resolver — the one that gossips with its neighbors over
//! `RES` datagrams and can re-resolve a name while a stream is running — and
//! the CSV table the reference's tests steer it with arrive in the commits
//! after this one.

pub mod cache;
pub mod driver;

use std::net::SocketAddr;

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::sys::AddressFamily;
use crate::udp_channel::{self, UdpChannelError};

/// What a resolver's `resolve` answers (`aeron_name_resolver_resolve_func_t`):
/// the reference's `0` and `-1`, and **no third answer**.
///
/// The three-valued contract — "0 if not found, 1 if found, -1 on error" — is
/// the one documented in that header, but it belongs to **`lookup`**, the
/// typedef the comment sits under (`aeron_name_resolver.h:30-44`), and the
/// default resolver's `resolve` says so: it starts at `-1` and is set to `0` on
/// success (`aeron_name_resolver.c:150-196`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The name is this address (`0`).
    Found(SocketAddr),
    /// The name is refused (`-1`), with what the resolver said. The CSV
    /// table's "disable resolution" operation is the reference's own way to put
    /// a name in this state on purpose
    /// (`aeron_csv_table_name_resolver.c:73-77`).
    Failed(String),
}

impl Resolution {
    /// The address, or the failure as the channel error a caller of
    /// [`crate::udp_channel::resolve_host_and_port`] already knows.
    ///
    /// # Errors
    ///
    /// [`UdpChannelError::Resolution`] for the failure.
    pub fn into_address(self, channel_text: &str) -> Result<SocketAddr, UdpChannelError> {
        match self {
            Self::Found(address) => Ok(address),
            Self::Failed(what) => Err(UdpChannelError::Resolution(format!(
                "{channel_text}: {what}"
            ))),
        }
    }
}

/// What a resolver's `lookup` answers (`aeron_name_resolver_lookup_func_t`) —
/// the three-valued one.
///
/// A lookup answers with a **name**, not an address, and that is what makes a
/// decorator possible: the driver's resolver asks its neighbors whether they
/// know a name, and the CSV table may answer with a *different* name for the
/// same question at different times. `0` is not "no answer": the default
/// resolver writes the name back and answers `0` (`aeron_name_resolver.c:103-112`),
/// which is a resolver saying "the name stands for itself".
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Lookup {
    /// The name stands for itself (`0`).
    Unchanged,
    /// This name instead (`1`).
    Found(String),
    /// Refused (`-1`).
    Failed(String),
}

/// One resolver — the five functions of `aeron_name_resolver.h:39-56`.
///
/// `start` and `close` are the resolver's own life, and `do_work` is its clock:
/// the default resolver does nothing on any of them, which is what makes it
/// synchronous, and the driver's resolver is the one that uses all three.
///
/// The counter view travels with `resolve`, `do_work` and `close` because the
/// reference's resolvers hold **value addresses** into the counter region — the
/// CSV table's per-row operation counter is one, and it is how the reference's
/// tests steer a name while a driver runs
/// (`aeron_csv_table_name_resolver.c:60-90`). In Rust the region belongs to the
/// caller, so it is lent to the call instead of living in the resolver.
pub trait Resolver {
    /// `host:port` into an address (`resolve_func`).
    ///
    /// `family` is the one the answer has to come back in, and it is an
    /// argument here because it is one in the reference too: its `resolve_func`
    /// is handed an **out**-parameter whose family the caller has already set,
    /// and the driver's resolver reads it back to decide *which cached row*
    /// answers the question (`aeron_driver_name_resolver.c:1210-1211`, and the
    /// reference's own test sets it before every call,
    /// `aeron_name_resolver_test.cpp:529-531`, `:567-569`). The synchronous
    /// resolvers have nothing to choose between and ignore it.
    fn resolve(
        &mut self,
        name: &str,
        uri_param_name: &str,
        is_re_resolution: bool,
        family: AddressFamily,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Resolution;

    /// The *name* this resolver has for that one (`lookup_func`).
    ///
    /// The default resolver answers [`Lookup::Unchanged`] for everything: it
    /// has nothing to say about names, only about addresses.
    fn lookup(
        &mut self,
        _name: &str,
        _uri_param_name: &str,
        _is_re_lookup: bool,
        _counters: &CounterManager,
        _regions: &CounterRegions<'_>,
    ) -> Lookup {
        Lookup::Unchanged
    }

    /// Take whatever resources the resolver needs (`start_func`).
    ///
    /// The counters come with it because the driver's resolver resolves its
    /// bootstrap neighbors here — and labels their counters with what they
    /// resolved to (`aeron_driver_name_resolver_on_start`, `:1238-1250`, which
    /// calls `resolve_bootstrap_neighbors`). The default resolver and the CSV
    /// table do nothing at all on start.
    ///
    /// # Errors
    ///
    /// What went wrong, for a caller that has to fail the driver's start.
    fn start(
        &mut self,
        _counters: &CounterManager,
        _regions: &CounterRegions<'_>,
    ) -> Result<(), String> {
        Ok(())
    }

    /// One pass of whatever the resolver does on its own clock (`do_work_func`),
    /// answering with the work done.
    fn do_work(
        &mut self,
        _now_ms: i64,
        _counters: &CounterManager,
        _regions: &CounterRegions<'_>,
    ) -> usize {
        0
    }

    /// Let go of them (`close_func`) — the counters it took among them, which
    /// is why the region comes with it.
    fn close(
        &mut self,
        _counters: &mut CounterManager,
        _regions: &CounterRegions<'_>,
        _now_ms: i64,
    ) {
    }
}

/// The resolver this build has always had: the system's own lookup, in the
/// caller's thread (`aeron_default_name_resolver_resolve`,
/// `aeron_name_resolver.c:117-215`).
///
/// It is not a fallback in the reference — it is what `NULL` args to
/// `aeron_name_resolver_init` gives you — and it is the resolver the other two
/// end at: `csv_table` resolves a name it does not know *through* it
/// (`aeron_csv_table_name_resolver.c:88`).
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultResolver;

impl Resolver for DefaultResolver {
    fn resolve(
        &mut self,
        name: &str,
        _uri_param_name: &str,
        _is_re_resolution: bool,
        _family: AddressFamily,
        _counters: &CounterManager,
        _regions: &CounterRegions<'_>,
    ) -> Resolution {
        match udp_channel::resolve_host_and_port(name) {
            Ok(address) => Resolution::Found(address),
            Err(error) => Resolution::Failed(error.to_string()),
        }
    }
}

/// The CSV lookup table (`aeron_csv_table_name_resolver.c`, 228 lines) — the
/// resolver the reference's own tests steer.
///
/// It is a decorator: a name it has a row for is **replaced** by that row's
/// host and then resolved (through [`DefaultResolver`], which is what the
/// reference's `:88` ends in); a name it has no row for is resolved as it is.
/// What makes it a test instrument is that **each row carries a counter**, and
/// the counter's *value* is the operation: a test flips it while a driver runs
/// and a name changes its answer underneath a live stream — which is the whole
/// of `NameReResolutionTest`.
///
/// The counter is a byte contract (`aeron_csv_table_name_resolver.c:175-210`),
/// and the harness is what makes it one: a test's own Java resolver
/// (`aeron-test-support/.../RedirectingNameResolver.java:34`, `NAME_ENTRY_COUNTER_TYPE_ID = 2001`)
/// is translated into this resolver's arguments when the driver is a separate
/// process (`CTestMediaDriver.java:289-293`), so the two have to agree on the
/// type id, the operations and the label — the Java one finds the counter by
/// **name**, from the key.
pub struct CsvTableResolver {
    rows: Vec<CsvRow>,
}

/// One row: a name, the two hosts it may stand for, and the counter that says
/// which (`aeron_csv_table_name_resolver_row_t`, `:33-41`).
struct CsvRow {
    name: String,
    /// What this name resolves to **before** any re-resolution (`columns[1]`).
    initial_resolution_host: String,
    /// And what it resolves to once a test says so (`columns[0]`).
    re_resolution_host: String,
    /// The counter a caller flips (`operation_toggle`).
    operation: i32,
}

/// `AERON_NAME_RESOLVER_CSV_ENTRY_COUNTER_TYPE_ID`
/// (`aeron_csv_table_name_resolver.h:21`).
pub const CSV_ENTRY_COUNTER_TYPE_ID: i32 = 2001;

/// The three operations a row's counter can hold
/// (`aeron_csv_table_name_resolver.h:22-24`).
pub const DISABLE_RESOLUTION: i64 = -1;
/// Use the row's `initial_resolution_host`.
pub const USE_INITIAL_RESOLUTION_HOST: i64 = 0;
/// Use the row's `re_resolution_host`.
pub const USE_RE_RESOLUTION_HOST: i64 = 1;

/// The columns one row has (`AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS`, `:29`),
/// and the separator between rows.
const COLUMNS: usize = 3;

impl CsvTableResolver {
    /// Read a configuration into rows, and take a counter for each
    /// (`aeron_csv_table_name_resolver_init`, `:110-228`).
    ///
    /// The configuration is rows separated by `|`, each row three
    /// comma-separated columns written **in reverse**: `reResolutionHost`,
    /// `initialResolutionHost`, `name` (`:150-156`). A row that is not three
    /// columns is skipped, as the reference's `if` does.
    ///
    /// # Errors
    ///
    /// What could not be done: no configuration at all (the reference's own
    /// error, `:123-127`), or a counter that could not be taken.
    pub fn new(
        args: Option<&str>,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) -> Result<Self, String> {
        let Some(args) = args else {
            return Err(format!(
                "No CSV configuration, please specify: {INIT_ARGS_ENV}"
            ));
        };

        let mut rows = Vec::new();

        for row in args.split('|') {
            let columns: Vec<&str> = row.split(',').collect();

            if columns.len() != COLUMNS {
                continue;
            }

            let (re_resolution_host, initial_resolution_host, name) =
                (columns[0], columns[1], columns[2]);

            // The key is the name's length as a `u32` and then the name bytes
            // — **not** a text label (`:175-179`), and the label is the
            // `NameEntry{...}` a reader finds the counter by (`:184-188`).
            let mut key = Vec::with_capacity(4 + name.len());
            key.extend_from_slice(&u32_from(name.len()).to_le_bytes());
            key.extend_from_slice(name.as_bytes());

            let label = format!(
                "NameEntry{{name='{name}', initialResolutionHost='{initial_resolution_host}', \
                 reResolutionHost='{re_resolution_host}'}}"
            );

            let Some(operation) = counters.allocate(
                regions,
                CSV_ENTRY_COUNTER_TYPE_ID,
                &key,
                label.as_bytes(),
                now_ms,
            ) else {
                return Err(format!("could not allocate a counter for {name}"));
            };

            rows.push(CsvRow {
                name: name.to_owned(),
                initial_resolution_host: initial_resolution_host.to_owned(),
                re_resolution_host: re_resolution_host.to_owned(),
                operation,
            });
        }

        Ok(Self { rows })
    }

    /// The counters this resolver took, for a caller that has to free them
    /// (the reference's own `close` **does not**: it frees the configuration
    /// string, the rows and the state, and leaves the counters to the counters
    /// manager — `aeron_csv_table_name_resolver.c:110-122`).
    pub fn counter_ids(&self) -> Vec<i32> {
        self.rows.iter().map(|row| row.operation).collect()
    }
}

/// The name the driver reads a resolver's arguments from
/// (`AERON_NAME_RESOLVER_INIT_ARGS`, `aeronmd.h:818`) — which is **not** the
/// `AERON_NAME_RESOLVER_CSV_TABLE_ARGS_ENV_VAR` the header declares
/// (`aeron_name_resolver.h:29`): that macro has no user in the C driver, and
/// the harness sets this one (`CTestMediaDriver.java:292`).
pub const INIT_ARGS_ENV: &str = "AERON_NAME_RESOLVER_INIT_ARGS";

fn u32_from(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

impl Resolver for CsvTableResolver {
    fn resolve(
        &mut self,
        name: &str,
        uri_param_name: &str,
        is_re_resolution: bool,
        family: AddressFamily,
        counters: &CounterManager,
        regions: &CounterRegions<'_>,
    ) -> Resolution {
        let mut hostname = name;

        for row in &self.rows {
            if row.name != name {
                continue;
            }

            let Some(operation) = counters.value(regions, row.operation) else {
                continue;
            };

            if DISABLE_RESOLUTION == operation {
                return Resolution::Failed(format!("Unable to resolve host=({name}): (forced)"));
            } else if USE_RE_RESOLUTION_HOST == operation {
                hostname = &row.re_resolution_host;
            } else if USE_INITIAL_RESOLUTION_HOST == operation {
                hostname = &row.initial_resolution_host;
            }
            // Any other value is no substitution at all, which is what the
            // reference's `else if` chain leaves behind (`:78-86`).
        }

        // And the default resolver is what both halves end in: the substituted
        // host, or the name itself when no row matched (`:88`).
        let mut default = DefaultResolver;
        default.resolve(
            hostname,
            uri_param_name,
            is_re_resolution,
            family,
            counters,
            regions,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deepmsg_core::buffer::AtomicBuffer;

    struct Counters {
        metadata: Vec<u8>,
        values: Vec<u8>,
    }

    impl Counters {
        fn new() -> Self {
            Self {
                metadata: vec![0u8; 64 * 1024 * 4],
                values: vec![0u8; 64 * 1024],
            }
        }

        fn open(&mut self) -> CounterRegions<'_> {
            CounterRegions::new(
                AtomicBuffer::from_slice_mut(&mut self.metadata).expect("aligned"),
                AtomicBuffer::from_slice_mut(&mut self.values).expect("aligned"),
            )
            .expect("four-to-one")
        }
    }

    /// The counters and the region a resolver reads and takes them from.
    struct Fixture {
        holder: Counters,
        counters: CounterManager,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                holder: Counters::new(),
                counters: CounterManager::new(64 * 1024, 1_000).expect("room"),
            }
        }

        fn resolver(&mut self, args: &str) -> CsvTableResolver {
            let regions = self.holder.open();

            CsvTableResolver::new(Some(args), &mut self.counters, &regions, 0).expect("a table")
        }

        fn set(&mut self, counter_id: i32, operation: i64) {
            let regions = self.holder.open();
            let _ = self.counters.set_value(&regions, counter_id, operation);
        }

        fn resolve(&mut self, resolver: &mut CsvTableResolver, name: &str) -> Resolution {
            let regions = self.holder.open();
            resolver.resolve(
                name,
                "endpoint",
                false,
                AddressFamily::Inet,
                &self.counters,
                &regions,
            )
        }
    }

    /// The default resolver is the system's own lookup, and it needs nothing
    /// from the caller to do it.
    #[test]
    fn the_default_resolver_is_the_systems_own_lookup() {
        let mut holder = Counters::new();
        let regions = holder.open();
        let counters = CounterManager::new(64 * 1024, 1_000).expect("room");
        let mut resolver = DefaultResolver;

        assert_eq!(
            Resolution::Found("127.0.0.1:40456".parse().expect("an address")),
            resolver.resolve(
                "127.0.0.1:40456",
                "endpoint",
                false,
                AddressFamily::Inet,
                &counters,
                &regions
            ),
            "a literal address is taken as itself, with no lookup"
        );

        let hostname = resolver.resolve(
            "localhost:40456",
            "endpoint",
            false,
            AddressFamily::Inet,
            &counters,
            &regions,
        );

        match hostname {
            Resolution::Found(address) => assert_eq!(40456, address.port()),
            other => panic!("localhost resolves: {other:?}"),
        }

        assert_eq!(
            Lookup::Unchanged,
            resolver.lookup("localhost:40456", "endpoint", false, &counters, &regions),
            "the default resolver has nothing to say about names"
        );
        assert_eq!(0, resolver.do_work(0, &counters, &regions));
    }

    /// A name the system cannot make an address of is a **failure**: a
    /// synchronous resolver has no one to pass the name to.
    #[test]
    fn a_synchronous_resolver_answers_yes_or_no() {
        let mut holder = Counters::new();
        let regions = holder.open();
        let counters = CounterManager::new(64 * 1024, 1_000).expect("room");
        let mut resolver = DefaultResolver;

        assert!(matches!(
            resolver.resolve(
                "not-a-host-at-all.invalid:40456",
                "endpoint",
                false,
                AddressFamily::Inet,
                &counters,
                &regions
            ),
            Resolution::Failed(_)
        ));
        assert!(matches!(
            resolver.resolve(
                "127.0.0.1:not-a-port",
                "endpoint",
                false,
                AddressFamily::Inet,
                &counters,
                &regions
            ),
            Resolution::Failed(_)
        ));
    }

    /// The table steers: a row's counter is the operation, and flipping it
    /// **changes what a running driver resolves a name to** — which is the
    /// whole of what `NameReResolutionTest` does.
    #[test]
    fn a_rows_counter_is_the_operation() {
        let mut fixture = Fixture::new();
        let mut resolver = fixture.resolver("127.0.0.1:40457,127.0.0.1:40456,somewhere:40456");
        let ids = resolver.counter_ids();

        assert_eq!(1, ids.len());
        assert_eq!(
            Resolution::Found("127.0.0.1:40456".parse().expect("an address")),
            fixture.resolve(&mut resolver, "somewhere:40456"),
            "a fresh row is the initial resolution host, which is `0`"
        );

        fixture.set(ids[0], USE_RE_RESOLUTION_HOST);
        assert_eq!(
            Resolution::Found("127.0.0.1:40457".parse().expect("an address")),
            fixture.resolve(&mut resolver, "somewhere:40456"),
            "and the re-resolution host is what the other operation picks"
        );

        fixture.set(ids[0], DISABLE_RESOLUTION);
        assert_eq!(
            Resolution::Failed("Unable to resolve host=(somewhere:40456): (forced)".to_owned()),
            fixture.resolve(&mut resolver, "somewhere:40456"),
            "the third operation is a refusal, in the reference's own words"
        );

        // A value that is none of the three substitutes nothing, so it is the
        // **name itself** that goes to the default resolver — and this name is
        // not an address, which is why the answer is a failure rather than the
        // initial host.
        fixture.set(ids[0], 7);
        assert!(matches!(
            fixture.resolve(&mut resolver, "somewhere:40456"),
            Resolution::Failed(_)
        ));
    }

    /// A name the table has no row for is resolved **as it is**, through the
    /// default resolver (`aeron_csv_table_name_resolver.c:88`): the table is a
    /// decorator, not a wall.
    #[test]
    fn a_name_the_table_does_not_know_goes_through_it() {
        let mut fixture = Fixture::new();
        let mut resolver = fixture.resolver("127.0.0.1:40457,127.0.0.1:40456,somewhere:40456");

        assert_eq!(
            Resolution::Found("127.0.0.1:40458".parse().expect("an address")),
            fixture.resolve(&mut resolver, "127.0.0.1:40458"),
            "which is how every channel that names an address keeps working"
        );

        assert!(matches!(
            fixture.resolve(&mut resolver, "not-a-host-at-all.invalid:40456"),
            Resolution::Failed(_)
        ));
    }

    /// The counter's type id and label are a byte contract: the reference's own
    /// tests find one of these counters and flip it
    /// (`RedirectingNameResolver.updateNameResolutionStatus`,
    /// `aeron-test-support/.../RedirectingNameResolver.java:135-155`).
    ///
    /// It finds it by its **key**, which is not asserted here because this
    /// build's `CountersReader` hands out a descriptor's label and type id and
    /// not its key — the reference's own `foreach` passes both
    /// (`filter_counters`, `aeron-driver/src/test/c/aeron_congestion_control_test.cpp:104-124`).
    /// The key this resolver writes is the shape the Java side reads: a `u32`
    /// length and then the name, which is what Agrona's
    /// `keyBuffer.getStringAscii(0)` decodes.
    #[test]
    fn a_rows_counter_is_named_the_way_a_reader_finds_it() {
        let mut fixture = Fixture::new();
        let resolver = fixture.resolver("127.0.0.1:40457,127.0.0.1:40456,somewhere:40456");
        let ids = resolver.counter_ids();

        let regions = fixture.holder.open();
        let descriptor = regions.reader().get(ids[0]).expect("an allocated counter");

        assert_eq!(CSV_ENTRY_COUNTER_TYPE_ID, descriptor.type_id);
        assert_eq!(
            "NameEntry{name='somewhere:40456', initialResolutionHost='127.0.0.1:40456', \
             reResolutionHost='127.0.0.1:40457'}",
            descriptor.label
        );
    }

    /// The two answers of a `resolve` and the three of a `lookup` are what a
    /// decorator is built out of, and the port of them is the channel error the
    /// synchronous path already answers with.
    #[test]
    fn a_failure_carries_what_the_resolver_said() {
        assert_eq!(
            Ok("127.0.0.1:40456".parse().expect("an address")),
            Resolution::Found("127.0.0.1:40456".parse().expect("an address"))
                .into_address("endpoint=127.0.0.1:40456")
        );

        let refused = Resolution::Failed("Unable to resolve host=(x): (forced)".to_owned())
            .into_address("endpoint=x:40456")
            .expect_err("a refused name is not an address");
        assert!(refused.to_string().contains("(forced)"), "{refused}");

        assert_eq!(
            Lookup::Found("elsewhere:40456".to_owned()),
            Lookup::Found("elsewhere:40456".to_owned()),
            "and a lookup's third answer is a name, not an address"
        );
    }
}
