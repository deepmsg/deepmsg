//! The businesses that ship with this build.
//!
//! The reference selects a business by Java class name
//! (`aeron.cluster.service.class.name`) and refuses anything it cannot load;
//! this build selects one from a **compiled-in registry** (spec §6) and refuses
//! a class name by name (`config.rs`). [`by_name`] is the registry: the binary
//! `deepmsg-clustered-service-container` reads `deepmsg.cluster.service` and
//! asks it for the business.
//!
//! A name in this table is a kept contract: a deployment's properties file
//! stays valid across versions, so entries are added, never renamed.
//!
//! | Name | Business | Why it is here |
//! |---|---|---|
//! | `echo` | [`EchoService`] | LC-4a's echo, timer and service-message scenarios; the benchmark's business |
//! | `basic-auction` | [`BasicAuction`] | the byte-for-byte twin of the reference tutorial, and LC-4a's snapshot comparison |
//! | `noop` | [`NoopService`] | the container under test with no business state in the way |

pub mod basic_auction;
pub mod echo;
pub mod noop;

pub use basic_auction::BasicAuction;
pub use echo::EchoService;
pub use noop::NoopService;

use crate::service::ClusteredService;

/// Every name [`by_name`] knows, in the order the error message lists them.
pub const NAMES: &[&str] = &["echo", "basic-auction", "noop"];

/// The business called `name`, or `None` when the registry has no such entry.
///
/// The box is `Send` because a container is a thread: the in-process half of a
/// fixture (and any deployment that runs the agent on one) moves the business
/// to the thread that drives it.
///
/// A caller that gets `None` must say so and name [`NAMES`]; this build never
/// falls back to a default business, because a deployment that misspells a
/// service name should not silently run a different one.
#[must_use]
pub fn by_name(name: &str) -> Option<Box<dyn ClusteredService + Send>> {
    match name {
        "echo" => Some(Box::new(EchoService::new())),
        "basic-auction" => Some(Box::new(BasicAuction::new())),
        "noop" => Some(Box::new(NoopService::new())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_registry_answers_every_name_it_advertises() {
        for name in NAMES {
            assert!(
                by_name(name).is_some(),
                "`{name}` is in NAMES but the registry does not answer it"
            );
        }
    }

    #[test]
    fn an_unknown_name_is_not_a_default() {
        assert!(by_name("auction").is_none());
        assert!(by_name("Echo").is_none());
        assert!(by_name("").is_none());
    }

    #[test]
    fn the_names_are_the_documented_ones() {
        assert_eq!(NAMES, &["echo", "basic-auction", "noop"]);
    }
}
