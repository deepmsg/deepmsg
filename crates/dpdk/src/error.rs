//! What went wrong, in DPDK's words where DPDK said it and in this crate's
//! where this crate refused.

use std::ffi::CStr;
use std::fmt;

use crate::sys;

/// A failure from the shim, or a refusal by this crate.
#[derive(Debug)]
pub struct Error {
    /// DPDK's `rte_errno` where the failure was DPDK's, `0` where this crate
    /// refused the request itself.
    pub rte_errno: i32,
    message: String,
}

impl Error {
    /// The current `rte_errno`, worded by DPDK, with `what` saying which call
    /// answered it.
    pub(crate) fn from_rte_errno(what: &str) -> Self {
        // SAFETY: `rte_errno` is a thread-local DPDK maintains and
        // `rte_strerror` answers a static string for any value it is given,
        // including the ones it does not know.
        let (rte_errno, text) = unsafe {
            let rte_errno = sys::shim_rte_errno();
            let text = sys::shim_rte_strerror(rte_errno);
            debug_assert!(
                !text.is_null(),
                "rte_strerror answers a string for any errno"
            );
            (
                rte_errno,
                CStr::from_ptr(text).to_string_lossy().into_owned(),
            )
        };

        Self {
            rte_errno,
            message: format!("{what}: {text}"),
        }
    }

    /// This crate's own refusal: nothing was called.
    pub(crate) fn refused(message: impl Into<String>) -> Self {
        Self {
            rte_errno: 0,
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.rte_errno == 0 {
            write!(f, "{}", self.message)
        } else {
            write!(f, "{} (rte_errno={})", self.message, self.rte_errno)
        }
    }
}

impl std::error::Error for Error {}
