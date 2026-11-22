#![no_main]

//! Target 04: the driver's control-plane command decoders.
//!
//! Every byte here was written by a client process. `deepmsg-cnc`'s decoders
//! are the first thing the driver runs on them, and each answers `Option` or
//! `Result` — the assertion is that none of them panics on any input, and that
//! what the encoders write decodes back field for field.
//!
//! Only the decoders a client can reach are here; the `encode_into` half is
//! exercised for the three payloads whose decoded and encoded forms both carry
//! every field (`ADD_PUBLICATION`, `ADD_COUNTER`, `REMOVE_COUNTER`).

use arbitrary::{Arbitrary, Unstructured};
use deepmsg_cnc::command::{
    AddCounter, AddPublication, RemoveCounter, decode_add_counter, decode_add_publication,
    decode_add_static_counter, decode_add_subscription, decode_correlated,
    decode_destination_by_id_command, decode_destination_command,
    decode_get_next_available_session_id, decode_reject_image, decode_remove_counter,
    decode_remove_publication, decode_remove_subscription, decode_response,
};
use libfuzzer_sys::fuzz_target;

/// A length the driver would accept, so a case spends its bytes on content
/// rather than on an allocation that only measures the fuzzer's `max_len`.
fn bounded(bytes: Vec<u8>) -> Vec<u8> {
    let mut bytes = bytes;
    bytes.truncate(4096);
    bytes
}

fn decode_everything(data: &[u8]) {
    let _ = decode_correlated(data);
    let _ = decode_add_counter(data);
    let _ = decode_remove_counter(data);
    let _ = decode_add_publication(data);
    let _ = decode_add_subscription(data);
    let _ = decode_remove_publication(data);
    let _ = decode_remove_subscription(data);
    let _ = decode_reject_image(data);
    let _ = decode_add_static_counter(data);
    let _ = decode_get_next_available_session_id(data);
    let _ = decode_destination_command(data);
    let _ = decode_destination_by_id_command(data);

    // `decode_response` dispatches on a type id the ring record carries, so
    // every byte of the payload doubles as a type id here.
    let (type_id, body) = match data.split_first() {
        Some((first, rest)) => (i32::from(*first), rest),
        None => (0, data),
    };
    let _ = decode_response(type_id, body);
    let _ = decode_response(i32::from(data.first().copied().unwrap_or(0)), data);
}

#[derive(Arbitrary, Debug)]
struct PublicationCase {
    client_id: i64,
    correlation_id: i64,
    stream_id: i32,
    channel: String,
}

impl PublicationCase {
    fn check(self) {
        let channel = self.channel;
        let command = AddPublication {
            client_id: self.client_id,
            correlation_id: self.correlation_id,
            stream_id: self.stream_id,
            channel: &channel,
        };
        let mut out = vec![0_u8; command.encoded_length()];
        assert!(
            command.encode_into(&mut out),
            "encode_into refused its own length"
        );

        let decoded = decode_add_publication(&out).expect("what encode_into wrote must decode");
        assert_eq!(decoded.client_id, self.client_id);
        assert_eq!(decoded.correlation_id, self.correlation_id);
        assert_eq!(decoded.stream_id, self.stream_id);
        assert_eq!(decoded.channel, channel.as_bytes());
    }
}

#[derive(Arbitrary, Debug)]
struct CounterCase {
    client_id: i64,
    correlation_id: i64,
    type_id: i32,
    key: Vec<u8>,
    label: String,
}

impl CounterCase {
    fn check(self) {
        let key = bounded(self.key);
        let label = self.label.into_bytes();
        let command = AddCounter {
            correlated: deepmsg_cnc::command::Correlated {
                client_id: self.client_id,
                correlation_id: self.correlation_id,
            },
            type_id: self.type_id,
            key: &key,
            label: &label,
        };
        let mut out = vec![0_u8; command.encoded_length()];
        assert!(
            command.encode_into(&mut out),
            "encode_into refused its own length"
        );

        let decoded = decode_add_counter(&out).expect("what encode_into wrote must decode");
        assert_eq!(decoded.correlated.client_id, self.client_id);
        assert_eq!(decoded.correlated.correlation_id, self.correlation_id);
        assert_eq!(decoded.type_id, self.type_id);
        assert_eq!(decoded.key, key.as_slice());
        assert_eq!(decoded.label, label.as_slice());
    }
}

#[derive(Arbitrary, Debug)]
struct RemoveCounterCase {
    client_id: i64,
    correlation_id: i64,
    registration_id: i64,
}

impl RemoveCounterCase {
    fn check(self) {
        let command = RemoveCounter {
            correlated: deepmsg_cnc::command::Correlated {
                client_id: self.client_id,
                correlation_id: self.correlation_id,
            },
            registration_id: self.registration_id,
        };
        let mut out = vec![0_u8; command.encoded_length()];
        assert!(
            command.encode_into(&mut out),
            "encode_into refused its own length"
        );

        let decoded = decode_remove_counter(&out).expect("what encode_into wrote must decode");
        assert_eq!(decoded, command);
    }
}

fuzz_target!(|data: &[u8]| {
    decode_everything(data);

    let mut unstructured = Unstructured::new(data);
    if let Ok(case) = PublicationCase::arbitrary(&mut unstructured) {
        case.check();
    }
    let mut unstructured = Unstructured::new(data);
    if let Ok(case) = CounterCase::arbitrary(&mut unstructured) {
        case.check();
    }
    let mut unstructured = Unstructured::new(data);
    if let Ok(case) = RemoveCounterCase::arbitrary(&mut unstructured) {
        case.check();
    }
});
