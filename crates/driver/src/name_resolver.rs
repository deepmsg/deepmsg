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

use std::net::SocketAddr;

use deepmsg_cnc::{CounterManager, CounterRegions};

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
    fn resolve(
        &mut self,
        name: &str,
        uri_param_name: &str,
        is_re_resolution: bool,
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
    /// # Errors
    ///
    /// What went wrong, for a caller that has to fail the driver's start.
    fn start(&mut self) -> Result<(), String> {
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
        _counters: &CounterManager,
        _regions: &CounterRegions<'_>,
    ) -> Resolution {
        match udp_channel::resolve_host_and_port(name) {
            Ok(address) => Resolution::Found(address),
            Err(error) => Resolution::Failed(error.to_string()),
        }
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
            resolver.resolve("127.0.0.1:40456", "endpoint", false, &counters, &regions),
            "a literal address is taken as itself, with no lookup"
        );

        let hostname = resolver.resolve("localhost:40456", "endpoint", false, &counters, &regions);

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
                &counters,
                &regions
            ),
            Resolution::Failed(_)
        ));
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
