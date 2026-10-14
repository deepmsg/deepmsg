//! What went wrong, in DPDK's words where DPDK said it and in this crate's
//! where this crate refused.

use std::ffi::CStr;
use std::fmt;

use crate::sys;

/// A failure from the shim, or a refusal by this crate.
#[derive(Debug)]
pub struct Error {
    /// DPDK's `rte_errno` where DPDK set one, the call's own return code where
    /// it answered one without setting an errno, and `0` where this crate
    /// refused the request itself and called nothing.
    pub code: i32,
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
            code: rte_errno,
            message: format!("{what}: {text} (rte_errno={rte_errno})"),
        }
    }

    /// A call that answered a return code, where DPDK may have left `rte_errno`
    /// alone.
    ///
    /// Some calls do: `net_pcap` answers `-ENOTSUP` to an MTU it will not take
    /// and sets nothing, and "Success" is not a useful thing for a caller to
    /// read — a case this crate found by asking one.
    pub(crate) fn from_call(what: &str, rc: i32) -> Self {
        // SAFETY: as in `from_rte_errno`.
        let rte_errno = unsafe { sys::shim_rte_errno() };
        if rte_errno == 0 {
            Self {
                code: rc,
                message: format!("{what}: the call answered {rc}"),
            }
        } else {
            Self::from_rte_errno(what)
        }
    }

    /// This crate's own refusal: nothing was called.
    pub(crate) fn refused(message: impl Into<String>) -> Self {
        Self {
            code: 0,
            message: message.into(),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {}
