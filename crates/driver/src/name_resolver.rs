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

use std::net::{IpAddr, SocketAddr};

use deepmsg_cnc::{CounterManager, CounterRegions};

use crate::sys::AddressFamily;
use crate::udp_channel;

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
    /// A **host** into an address (`resolve_func`).
    ///
    /// Not `host:port`: the reference's own wrapper splits the name in two and
    /// hands the resolver the host alone (`parsed_address.host`,
    /// `aeron_name_resolver.c:145`, `:164`, `:176`), then writes the port into
    /// the answer itself (`:181-191`). Three things in the reference say the
    /// same: the default resolver hands what it is given straight to
    /// `getaddrinfo(host, NULL, ...)` (`:87-95`,
    /// `aeron-netutil.c:56-100`), the driver's resolver compares it with its own
    /// **name** (`:1219`), and the CSV table's rows are bare names
    /// (`aeron_csv_table_name_resolver.c:150-156`). So an answer's port is
    /// zero — it is the caller's to set.
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

/// The three resolvers the reference's supplier table maps names to
/// (`aeron_name_resolver.c:214-229`, whose entries are `default`, `csv_table`
/// and `driver`), and what a driver picks one with
/// (`aeron.name.resolver.supplier`, `aeronmd.h:808-809`).
///
/// The name is checked where the setting is read, so a name that is none of the
/// three fails the driver's start-up — which is what the reference's
/// `aeron_name_resolver_supplier_load` returning `NULL` does to its context
/// init (`aeron_driver_context.c:588-594`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Supplier {
    /// `aeron_default_name_resolver_supplier`: the system's own lookup.
    #[default]
    Default,
    /// `aeron_csv_table_name_resolver_supplier`: the table the reference's own
    /// tests steer a name with.
    CsvTable,
    /// `aeron_driver_name_resolver_supplier`: the gossip resolver.
    Driver,
}

impl Supplier {
    /// The supplier a name stands for (`aeron_name_resolver_supplier_load`,
    /// `aeron_name_resolver.c:214-229`), or `None` for a name that is not in
    /// the table.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "default" => Some(Self::Default),
            "csv_table" => Some(Self::CsvTable),
            "driver" => Some(Self::Driver),
            _ => None,
        }
    }

    /// The name it answers to, which is what the reference compares.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::CsvTable => "csv_table",
            Self::Driver => "driver",
        }
    }

    /// Build the resolver (`aeron_default_name_resolver_supplier`,
    /// `aeron_csv_table_name_resolver_supplier` and
    /// `aeron_driver_name_resolver_supplier`, each of which takes the same
    /// `(resolver, args, context)`).
    ///
    /// `Send`, because the reference hands the built resolver to the native
    /// resource agent's thread, which then runs its clock
    /// (`aeron_driver_native_resource_agent.c:260`).
    ///
    /// `driver` is the parameters the gossip resolver needs — read from the
    /// settings either way, and ignored by the two that do not gossip.
    ///
    /// # Errors
    ///
    /// What the resolver could not do at construction: the CSV table's own
    /// parse of `args` (`aeron_csv_table_name_resolver.c:110-160`), or the
    /// driver's interface, socket or counters.
    pub fn build(
        self,
        driver: &driver::Params,
        args: Option<&str>,
        counters: &mut CounterManager,
        regions: &CounterRegions<'_>,
        now_ms: i64,
    ) -> Result<Box<dyn Resolver + Send>, String> {
        match self {
            Self::Default => Ok(Box::new(DefaultResolver)),
            Self::CsvTable => Ok(Box::new(CsvTableResolver::new(
                args, counters, regions, now_ms,
            )?)),
            Self::Driver => Ok(Box::new(driver::DriverResolver::new(
                driver, counters, regions, now_ms,
            )?)),
        }
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
    /// A host into an address (`aeron_default_name_resolver_resolve`,
    /// `aeron_name_resolver.c:87-95`): IPv4 first, IPv6 only if that fails,
    /// which is the reference calling `aeron_ip_addr_resolver` twice.
    ///
    /// The caller's `family` is **ignored**, and that is the reference's shape
    /// too — this resolver has no cache to choose a row from, so it takes
    /// whatever family answers. The address it answers with carries port zero,
    /// because there is no port in this contract; the caller writes the one the
    /// channel named (`:181-191`).
    fn resolve(
        &mut self,
        name: &str,
        _uri_param_name: &str,
        _is_re_resolution: bool,
        _family: AddressFamily,
        _counters: &CounterManager,
        _regions: &CounterRegions<'_>,
    ) -> Resolution {
        // A literal is an address whatever family was hinted at, which is what
        // `getaddrinfo` does with one (`aeron_netutil.c:73-91`).
        if let Ok(address) = name.parse::<IpAddr>() {
            return Resolution::Found(SocketAddr::new(address, 0));
        }

        let mut failure = String::new();

        for family in [AddressFamily::Inet, AddressFamily::Inet6] {
            match udp_channel::lookup_host(name, family) {
                Ok(Some(address)) => return Resolution::Found(address),
                Ok(None) => {}
                Err(error) => failure = error.to_string(),
            }
        }

        // The line, and the site that owns it, are the reference's
        // (`util/aeron_netutil.c:75-76`, inside `aeron_ip_addr_resolver`).
        // The detail after the colon is this build's wording of the same
        // failure — `getaddrinfo`'s `(%d) %s` there, Rust's own io error here,
        // because the resolver reaches the system through `ToSocketAddrs` and
        // that does not surface the `EAI_*` code. The wrapper above adds the
        // code line and the `Unresolved - …` line around it.
        Resolution::Failed(format!(
            "[aeron_ip_addr_resolver, aeron_netutil.c:75] Unable to resolve host=({name}): {failure}"
        ))
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
    /// comma-separated columns: **`name`, `initialResolutionHost`,
    /// `reResolutionHost`** (`AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS`,
    /// `:150-156`). A row that is not three columns is skipped, as the
    /// reference's `if` does.
    ///
    /// The reference's own comment on those three lines says *"fields are in
    /// reverse order"*, and that is true of the **array it reads**, not of the
    /// text: its `aeron_tokenise` fills backwards (`util/aeron_strutil.c:112-161`),
    /// so `columns[0]` is the last field and the assignments
    /// `re = columns[0]; initial = columns[1]; name = columns[2]` come out in
    /// the order the text is written. Reading the comment instead of the
    /// tokeniser reverses every row — a name becomes a host and a host becomes
    /// a name — and the reference's own configuration is the proof:
    /// `"ReResTestEndpoint,127.0.0.1,127.0.0.2"` (`NameReResolutionTest.java:79`)
    /// and `"server0,127.0.0.1,127.0.0.2"` (`aeron_name_resolver_test.cpp:465`).
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

            let (name, initial_resolution_host, re_resolution_host) =
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
                // The reference's own words and site for a row switched off
                // (`aeron_csv_table_name_resolver.c:73`, in
                // `aeron_csv_table_name_resolver_resolve`), `(forced)` and all.
                return Resolution::Failed(format!(
                    "[aeron_csv_table_name_resolver_resolve, \
                     aeron_csv_table_name_resolver.c:73] \
                     Unable to resolve host=({name}): (forced)"
                ));
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

    /// A port nobody is using, taken by binding and letting go.
    fn free_port() -> u16 {
        std::net::UdpSocket::bind("127.0.0.1:0")
            .expect("a port")
            .local_addr()
            .expect("an address")
            .port()
    }

    /// The three names the reference's supplier table has
    /// (`aeron_name_resolver.c:214-229`), each building a resolver that
    /// answers — and a fourth name that builds nothing, because a driver that
    /// names it does not start.
    #[test]
    fn a_supplier_name_builds_the_resolver_it_names() {
        let mut fixture = Fixture::new();
        let regions = fixture.holder.open();

        let port = free_port();
        let driver_params = driver::Params {
            name: "A".to_owned(),
            interface: format!("127.0.0.1:{port}"),
            ..driver::Params::default()
        };

        assert_eq!(None, Supplier::from_name("dns"));
        assert_eq!("driver", Supplier::Driver.name());

        let mut default = Supplier::Default
            .build(&driver_params, None, &mut fixture.counters, &regions, 0)
            .expect("a default resolver");
        assert_eq!(
            Resolution::Found("127.0.0.1:0".parse().expect("an address")),
            default.resolve(
                "127.0.0.1",
                "endpoint",
                false,
                AddressFamily::Inet,
                &fixture.counters,
                &regions
            )
        );

        let mut table = Supplier::CsvTable
            .build(
                &driver_params,
                Some("server0,127.0.0.1,127.0.0.2"),
                &mut fixture.counters,
                &regions,
                0,
            )
            .expect("a csv table");
        assert_eq!(
            Resolution::Found("127.0.0.1:0".parse().expect("an address")),
            table.resolve(
                "server0",
                "endpoint",
                false,
                AddressFamily::Inet,
                &fixture.counters,
                &regions
            ),
            "the table steers a name only it knows"
        );

        let mut gossip = Supplier::Driver
            .build(&driver_params, None, &mut fixture.counters, &regions, 0)
            .expect("a driver resolver");
        assert_eq!(
            Resolution::Found(
                format!(
                    "127.0.0.1:{}",
                    driver_params.interface.rsplit(':').next().expect("a port")
                )
                .parse()
                .expect("an address")
            ),
            gossip.resolve(
                "A",
                "endpoint",
                false,
                AddressFamily::Inet,
                &fixture.counters,
                &regions
            ),
            "and the driver's own resolver knows its own name"
        );

        // The CSV table without its configuration is the reference's own
        // error, and it is the *resolver* that fails rather than the driver.
        assert!(
            Supplier::CsvTable
                .build(&driver_params, None, &mut fixture.counters, &regions, 0)
                .is_err()
        );
    }

    /// The default resolver is the system's own lookup, and what it is given
    /// is a **host**: the answer carries no port, because the port belongs to
    /// the channel the caller is parsing (`aeron_name_resolver.c:181-191`).
    #[test]
    fn the_default_resolver_is_the_systems_own_lookup() {
        let mut holder = Counters::new();
        let regions = holder.open();
        let counters = CounterManager::new(64 * 1024, 1_000).expect("room");
        let mut resolver = DefaultResolver;

        assert_eq!(
            Resolution::Found("127.0.0.1:0".parse().expect("an address")),
            resolver.resolve(
                "127.0.0.1",
                "endpoint",
                false,
                AddressFamily::Inet,
                &counters,
                &regions
            ),
            "a literal host is taken as itself, with no lookup"
        );

        let hostname = resolver.resolve(
            "localhost",
            "endpoint",
            false,
            AddressFamily::Inet,
            &counters,
            &regions,
        );

        match hostname {
            Resolution::Found(address) => assert_eq!(0, address.port(), "and with no port"),
            other => panic!("localhost resolves: {other:?}"),
        }

        assert_eq!(
            Lookup::Unchanged,
            resolver.lookup("localhost", "endpoint", false, &counters, &regions),
            "the default resolver has nothing to say about names"
        );
        assert_eq!(0, resolver.do_work(0, &counters, &regions));
    }

    /// A name the system cannot make an address of is a **failure**: a
    /// synchronous resolver has no one to pass the name to.
    ///
    /// And a `host:port` is one of those, which is the contract in one
    /// assertion: what a resolver is handed is the host, split off by the
    /// caller (`aeron_name_resolver.c:145`).
    #[test]
    fn a_synchronous_resolver_answers_yes_or_no() {
        let mut holder = Counters::new();
        let regions = holder.open();
        let counters = CounterManager::new(64 * 1024, 1_000).expect("room");
        let mut resolver = DefaultResolver;

        assert!(matches!(
            resolver.resolve(
                "not-a-host-at-all.invalid",
                "endpoint",
                false,
                AddressFamily::Inet,
                &counters,
                &regions
            ),
            Resolution::Failed(_)
        ));
        assert!(
            matches!(
                resolver.resolve(
                    "127.0.0.1:40456",
                    "endpoint",
                    false,
                    AddressFamily::Inet,
                    &counters,
                    &regions
                ),
                Resolution::Failed(_)
            ),
            "a host and a port is not a host"
        );
    }

    /// The table steers: a row's counter is the operation, and flipping it
    /// **changes what a running driver resolves a name to** — which is the
    /// whole of what `NameReResolutionTest` does.
    #[test]
    fn a_rows_counter_is_the_operation() {
        let mut fixture = Fixture::new();
        let mut resolver = fixture.resolver("server0,127.0.0.1,127.0.0.2");
        let ids = resolver.counter_ids();

        assert_eq!(1, ids.len());
        assert_eq!(
            Resolution::Found("127.0.0.1:0".parse().expect("an address")),
            fixture.resolve(&mut resolver, "server0"),
            "a fresh row is the initial resolution host, which is `0`"
        );

        fixture.set(ids[0], USE_RE_RESOLUTION_HOST);
        assert_eq!(
            Resolution::Found("127.0.0.2:0".parse().expect("an address")),
            fixture.resolve(&mut resolver, "server0"),
            "and the re-resolution host is what the other operation picks"
        );

        fixture.set(ids[0], DISABLE_RESOLUTION);
        assert_eq!(
            Resolution::Failed(
                "[aeron_csv_table_name_resolver_resolve, aeron_csv_table_name_resolver.c:73] \
                 Unable to resolve host=(server0): (forced)"
                    .to_owned()
            ),
            fixture.resolve(&mut resolver, "server0"),
            "the third operation is a refusal, in the reference's own words and at its own site"
        );

        // A value that is none of the three substitutes nothing, so it is the
        // **name itself** that goes to the default resolver — and this name is
        // not an address, which is why the answer is a failure rather than the
        // initial host.
        fixture.set(ids[0], 7);
        assert!(matches!(
            fixture.resolve(&mut resolver, "server0"),
            Resolution::Failed(_)
        ));
    }

    /// A name the table has no row for is resolved **as it is**, through the
    /// default resolver (`aeron_csv_table_name_resolver.c:88`): the table is a
    /// decorator, not a wall.
    #[test]
    fn a_name_the_table_does_not_know_goes_through_it() {
        let mut fixture = Fixture::new();
        let mut resolver = fixture.resolver("server0,127.0.0.1,127.0.0.2");

        assert_eq!(
            Resolution::Found("127.0.0.1:0".parse().expect("an address")),
            fixture.resolve(&mut resolver, "127.0.0.1"),
            "which is how every channel that names an address keeps working"
        );

        assert!(matches!(
            fixture.resolve(&mut resolver, "not-a-host-at-all.invalid"),
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
        let resolver = fixture.resolver("server0,127.0.0.1,127.0.0.2");
        let ids = resolver.counter_ids();

        let regions = fixture.holder.open();
        let descriptor = regions.reader().get(ids[0]).expect("an allocated counter");

        assert_eq!(CSV_ENTRY_COUNTER_TYPE_ID, descriptor.type_id);
        assert_eq!(
            "NameEntry{name='server0', initialResolutionHost='127.0.0.1', \
             reResolutionHost='127.0.0.2'}",
            descriptor.label
        );
    }

    /// The two answers of a `resolve` and the three of a `lookup` are what a
    /// decorator is built out of, and what a caller does with the failure is
    /// the wrapper's business — it is the one that knows the URI parameter's
    /// name and the text the channel named
    /// (`crate::udp_channel::resolution_failure`), which is why a failure is
    /// carried here rather than converted here.
    #[test]
    fn a_failure_carries_what_the_resolver_said() {
        assert_eq!(
            Resolution::Found("127.0.0.1:40456".parse().expect("an address")),
            Resolution::Found("127.0.0.1:40456".parse().expect("an address"))
        );

        let refused = Resolution::Failed("Unable to resolve host=(x): (forced)".to_owned());
        let Resolution::Failed(what) = refused else {
            panic!("a refused name is not an address");
        };
        assert!(what.contains("(forced)"), "{what}");

        assert_eq!(
            Lookup::Found("elsewhere:40456".to_owned()),
            Lookup::Found("elsewhere:40456".to_owned()),
            "and a lookup's third answer is a name, not an address"
        );
    }
}
