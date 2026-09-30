//! A publication this client holds that failed.
//!
//! `ON_PUBLICATION_ERROR` is the one client message about a resource the client
//! **owns** rather than one it asked for: it names a publication by registration
//! id, carries no correlation id, and is the way a publisher learns that a
//! reader refused its stream. Both ways into it arrive here — a client that
//! rejected an image, and a driver whose own publication heard the `ERR` frame
//! (`aeron_driver_conductor.c:2263-2323`, reached from
//! `aeron_ipc_publication.c:249` and `aeron_network_publication.c:873`).
//!
//! The reference delivers it through a callback registered on the context
//! (`Aeron.Context.publicationErrorFrameHandler`, and on the C side
//! `aeron_publication_error_values_t` handed to an error-frame handler). This
//! crate's shape is a queue the caller drains
//! ([`crate::client::Client::publication_errors`]), for the reason
//! [`crate::counter::CounterEvent`] is one: a poll-driven client has no thread
//! to run a callback on.
//!
//! Nothing here closes the publication. A publication whose image was refused is
//! still a publication — the driver's rejection is a *pause*, and the reference
//! keeps the client's handle either way — so what to do about it is the
//! application's, and the event carries everything it needs to decide.

use std::net::SocketAddr;

use deepmsg_cnc::command::{ERROR_CODE_IMAGE_REJECTED, ERROR_CODE_PUBLICATION_REVOKED};

/// One `ON_PUBLICATION_ERROR`, as this client read it.
///
/// Every field the driver sent, including the ones this crate has no use for:
/// the destination and group tag are for a multi-destination channel and a
/// group, and an application reading a byte dump of the message should not have
/// to open the protocol header to find them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PublicationErrorEvent {
    /// The publication's registration id — the client's own correlation id for
    /// the `ADD_PUBLICATION` that made it.
    pub registration_id: i64,
    /// The destination the error came from, or `-1`. Always `-1` on the IPC
    /// path, and `-1` on the network path when the channel has no destination
    /// tracker to name one from
    /// (`media/aeron_send_channel_endpoint.c:685-691`).
    pub destination_registration_id: i64,
    /// The stream's session id.
    pub session_id: i32,
    /// The stream id.
    pub stream_id: i32,
    /// Who refused it, or `-1`. An IPC publication answers no receiver.
    pub receiver_id: i64,
    /// The group tag, or `-1` — which is what the driver writes when the `ERR`
    /// frame carried no `HAS_GROUP_TAG` flag, so this field is never stale.
    pub group_tag: i64,
    /// Where the error came from. [`None`] when the driver sent no address
    /// (`address_type` of zero), which is also what an address family this
    /// build does not know reads as.
    pub source: Option<SocketAddr>,
    /// Why, as one of the reference's client error codes —
    /// `ERROR_CODE_IMAGE_REJECTED` (13) for a refused image,
    /// `ERROR_CODE_PUBLICATION_REVOKED` (14) for a revoked publication.
    pub error_code: i32,
    /// The words that go with it. A client's own on the IPC path (the reason it
    /// gave), and a receiver's on the network path.
    pub message: Vec<u8>,
}

impl PublicationErrorEvent {
    /// Whether this is a reader refusing the stream
    /// (`AERON_ERROR_CODE_IMAGE_REJECTED`, `aeron_client_error.h:23`).
    ///
    /// The distinction matters to a caller: a rejected publication comes back
    /// after one liveness timeout, where a revoked one does not.
    pub const fn is_rejected(&self) -> bool {
        ERROR_CODE_IMAGE_REJECTED == self.error_code
    }

    /// Whether the publication was revoked
    /// (`AERON_ERROR_CODE_PUBLICATION_REVOKED`, `aeron_client_error.h:24`) —
    /// cut off on purpose by its own client, which is not something to retry.
    pub const fn is_revoked(&self) -> bool {
        ERROR_CODE_PUBLICATION_REVOKED == self.error_code
    }

    /// The message as text, for a log line.
    ///
    /// Lossy on purpose: the words a receiver sends are its own and nothing
    /// guarantees they are UTF-8, and a caller that wants the bytes has
    /// [`Self::message`].
    pub fn message_lossy(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.message)
    }
}

impl std::fmt::Display for PublicationErrorEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "publication {} (session={}, stream={}) failed: {} ({})",
            self.registration_id,
            self.session_id,
            self.stream_id,
            self.message_lossy(),
            deepmsg_cnc::error_log::error_code_description(self.error_code),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(error_code: i32) -> PublicationErrorEvent {
        PublicationErrorEvent {
            registration_id: 42,
            destination_registration_id: -1,
            session_id: 1001,
            stream_id: 7,
            receiver_id: -1,
            group_tag: -1,
            source: Some("127.0.0.1:40456".parse().expect("an address")),
            error_code,
            message: b"Needs to be closed".to_vec(),
        }
    }

    #[test]
    fn the_two_codes_name_the_two_things_that_can_happen() {
        assert!(event(ERROR_CODE_IMAGE_REJECTED).is_rejected());
        assert!(!event(ERROR_CODE_IMAGE_REJECTED).is_revoked());

        assert!(event(ERROR_CODE_PUBLICATION_REVOKED).is_revoked());
        assert!(!event(ERROR_CODE_PUBLICATION_REVOKED).is_rejected());

        // And a code that is neither is neither, rather than being guessed at.
        let other = event(deepmsg_cnc::command::ERROR_CODE_GENERIC_ERROR);
        assert!(!other.is_rejected());
        assert!(!other.is_revoked());
    }

    #[test]
    fn a_message_that_is_not_text_still_has_one() {
        let mut broken = event(ERROR_CODE_IMAGE_REJECTED);
        broken.message = vec![0xff, 0xfe];

        assert_eq!("\u{fffd}\u{fffd}", broken.message_lossy());
        assert_eq!(vec![0xff, 0xfe], broken.message, "the bytes are kept");
    }

    #[test]
    fn the_line_names_the_publication_the_words_and_the_code() {
        assert_eq!(
            "publication 42 (session=1001, stream=7) failed: Needs to be closed (image rejected)",
            event(ERROR_CODE_IMAGE_REJECTED).to_string()
        );
    }
}
