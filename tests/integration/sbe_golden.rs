//! Golden tests over the SBE fixtures in `tests/fixtures/sbe/`.
//!
//! The bytes were written by the reference's own `sbe-tool` output and
//! `golden.tsv` records what the reference's own decoder answers for them
//! (`fixtures/sbe/README.md`). So both sides of every comparison below are the
//! reference's, and a disagreement is this repository's defect.
//!
//! Each message gets the same three assertions:
//!
//! 1. **Read back.** Our decoder is handed the reference's bytes and every
//!    field is held to the value the reference's decoder read — including at
//!    the lower acting versions the golden carries for the messages that have
//!    version-gated fields.
//! 2. **Write back.** Our encoder is handed the same values and its bytes are
//!    compared with the reference's, byte for byte.
//! 3. **Length.** The encoded length equals the fixture's, which is the same
//!    thing stated in a way that fails with a number rather than a diff.
//!
//! The per-message list of fields is all a message contributes; `golden!`
//! writes the rest, so a message cannot quietly be given two of the three
//! assertions or none. The field list is in schema order and that matters:
//! reading a variable-length field advances the decoder's cursor, so the order
//! here is the order they were written in.
//!
//! The fixtures are read at run time rather than `include_bytes!`d, because
//! their names come from `golden.tsv` and there are sixty-two of them. A
//! missing file is a panic naming it, and the length recorded in the table is
//! checked against the file that was found.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::OnceLock;

use deepmsg_codec::archive;
use deepmsg_codec::archive_mark;

/// The message header's `version` field, which is what tells a decoder how much
/// of the block it may believe.
const HEADER_VERSION_OFFSET: usize = 6;

/// Where the body starts in a message.
const MESSAGE_HEADER_LENGTH: usize = 8;

/// See the module comment.
macro_rules! golden {
    (
        $test:ident, $template:literal,
        $crate_root:path, $header:path, $module:path,
        $encoder:ident, $decoder:ident,
        { $( $kind:ident $field:ident $(/ $coordinates:ident)? $(as $ty:ty)? : $schema:literal ),* $(,)? }
    ) => {
        #[test]
        fn $test() {
            use $crate_root::{ReadBuf, WriteBuf};
            // The braces are not decoration: a `path` fragment cannot be
            // followed by `::`, so a path metavariable only ever splices as a
            // group.
            use $header::{MessageHeaderDecoder};
            use $module::{$decoder, $encoder, SBE_SCHEMA_ID};

            let fixtures = fixtures_for($template);
            assert!(!fixtures.is_empty(), "no fixture carries template id {}", $template);

            for fixture in fixtures {
                // The fixture's schema and the crate's, which is the pairing a
                // copy-paste in the list below would otherwise get wrong: the
                // header check further down reads the id out of the bytes, and
                // this one reads it out of the code being tested.
                assert_eq!(fixture.schema_id, SBE_SCHEMA_ID, "{}: schema id", fixture.name);

                for version in fixture.versions() {
                    let bytes = fixture.bytes_at(version);

                    // The four header fields are the same four for every
                    // message, so they are checked here rather than in the
                    // per-message list.
                    let header = MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
                    for field in ["blockLength", "templateId", "schemaId", "version"] {
                        let read = match field {
                            "blockLength" => header.block_length(),
                            "templateId" => header.template_id(),
                            "schemaId" => header.schema_id(),
                            _ => header.version(),
                        };
                        assert_eq!(read, fixture.header(version, field),
                            "{}: header {field} at acting version {version}", fixture.name);
                    }

                    let mut decoder = <$decoder>::default().header(header, 0);
                    $(
                        golden_decode!(&mut decoder, fixture, version, &bytes,
                            $kind $field $(/ $coordinates)? $(as $ty)?: $schema);
                    )*

                    // Only the version the reference can actually encode: an
                    // old one is a reading, not a message (see the fixtures'
                    // README), and the encoder has no version guard to make it
                    // differ anyway.
                    if version == fixture.version {
                        let mut buffer = vec![0u8; fixture.bytes.len() + 64];
                        let encoder = <$encoder>::default()
                            .wrap(WriteBuf::new(&mut buffer), MESSAGE_HEADER_LENGTH);
                        let mut header = encoder.header(0);
                        let mut encoder = header.parent().unwrap();
                        $(
                            golden_encode!(&mut encoder, fixture, version,
                                $kind $field $(/ $coordinates)? $(as $ty)?: $schema);
                        )*

                        let length = MESSAGE_HEADER_LENGTH + encoder.encoded_length();
                        assert_eq!(length, fixture.bytes.len(),
                            "{}: encoded length", fixture.name);
                        assert_eq!(&buffer[..length], &fixture.bytes[..],
                            "{}: encoded bytes", fixture.name);
                    }
                }
            }
        }
    };
}

/// Reading a field back and holding it to the reference's answer.
macro_rules! golden_decode {
    ($decoder:expr, $f:expr, $v:expr, $bytes:expr, i8 $field:ident: $schema:literal) => {
        assert_eq!(
            i64::from($decoder.$field()),
            $f.int($v, $schema, "i8"),
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
    };
    ($decoder:expr, $f:expr, $v:expr, $bytes:expr, i16 $field:ident: $schema:literal) => {
        assert_eq!(
            i64::from($decoder.$field()),
            $f.int($v, $schema, "i16"),
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
    };
    ($decoder:expr, $f:expr, $v:expr, $bytes:expr, i32 $field:ident: $schema:literal) => {
        assert_eq!(
            i64::from($decoder.$field()),
            $f.int($v, $schema, "i32"),
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
    };
    ($decoder:expr, $f:expr, $v:expr, $bytes:expr, i64 $field:ident: $schema:literal) => {
        assert_eq!(
            $decoder.$field(),
            $f.int($v, $schema, "i64"),
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
    };

    // An optional field decodes to an `Option`, and the reference's answer is
    // the type's null value where it is absent. Which value that is comes from
    // the schema, so it travels in the golden's type column (`i32:null=0`)
    // rather than being written a second time in the list below.
    ($decoder:expr, $f:expr, $v:expr, $bytes:expr, optional $field:ident: $schema:literal) => {
        assert_eq!(
            $decoder.$field().map(i64::from),
            $f.optional_int($v, $schema),
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
    };

    // An enum is pinned twice: the integer says our constants are numbered the
    // way the schema numbers them, and the name says they are spelled the way
    // the schema spells them. Either alone would miss a swap of two variants
    // that happened to keep the ordinals in order.
    ($decoder:expr, $f:expr, $v:expr, $bytes:expr, enum $field:ident as $ty:ty: $schema:literal) => {{
        let (name, raw) = $f.enum_value($v, $schema);
        assert_eq!(
            i32::from($decoder.$field()),
            raw,
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
        assert_eq!(
            $decoder.$field().to_string(),
            name,
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
    }};

    // Variable-length fields are compared as the bytes the decoder points at,
    // which is the only way to see a wrong length prefix — a decoder that read
    // one byte too few would answer with a shorter slice that still matched.
    //
    // Two names, because `sbe-tool` gives the encoder `channel` and the
    // decoder `channel_decoder` and there is no way to build the second from
    // the first in a macro (that takes `paste`, and this workspace has no
    // dependencies). The invocation writes both.
    ($decoder:expr, $f:expr, $v:expr, $bytes:expr,
        text $field:ident / $coordinates:ident: $schema:literal) => {{
        let (offset, length) = $decoder.$coordinates();
        assert_eq!(
            &$bytes[offset..offset + length],
            $f.text($v, $schema).as_bytes(),
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
    }};
    ($decoder:expr, $f:expr, $v:expr, $bytes:expr,
        data $field:ident / $coordinates:ident: $schema:literal) => {{
        let (offset, length) = $decoder.$coordinates();
        assert_eq!(
            &$bytes[offset..offset + length],
            $f.data($v, $schema).as_slice(),
            "{}: {} at acting version {}",
            $f.name,
            $schema,
            $v
        );
    }};
}

/// Writing the reference's values and comparing bytes.
macro_rules! golden_encode {
    ($encoder:expr, $f:expr, $v:expr, i8 $field:ident: $schema:literal) => {
        $encoder.$field($f.int($v, $schema, "i8") as i8);
    };
    ($encoder:expr, $f:expr, $v:expr, i16 $field:ident: $schema:literal) => {
        $encoder.$field($f.int($v, $schema, "i16") as i16);
    };
    ($encoder:expr, $f:expr, $v:expr, i32 $field:ident: $schema:literal) => {
        $encoder.$field($f.int($v, $schema, "i32") as i32);
    };
    ($encoder:expr, $f:expr, $v:expr, i64 $field:ident: $schema:literal) => {
        $encoder.$field($f.int($v, $schema, "i64"));
    };
    ($encoder:expr, $f:expr, $v:expr, enum $field:ident as $ty:ty: $schema:literal) => {
        $encoder.$field(<$ty>::from($f.enum_value($v, $schema).1));
    };
    // The reference's encoder has no notion of an absent optional field
    // either: it writes the null value, and that is what its bytes carry. So
    // this goes through the raw setter with the value the golden holds.
    ($encoder:expr, $f:expr, $v:expr, optional $field:ident: $schema:literal) => {
        $encoder.$field($f.raw_int($v, $schema) as _);
    };
    ($encoder:expr, $f:expr, $v:expr,
        text $field:ident / $coordinates:ident: $schema:literal) => {
        $encoder.$field($f.text($v, $schema).as_bytes());
    };
    ($encoder:expr, $f:expr, $v:expr,
        data $field:ident / $coordinates:ident: $schema:literal) => {
        $encoder.$field(&$f.data($v, $schema));
    };
}

/// Strips the `:null=` an optional field's type carries, leaving the width.
fn base_type(kind: &str) -> &str {
    kind.split_once(':').map_or(kind, |(base, _)| base)
}

fn fixtures_for(template_id: u16) -> Vec<&'static Fixture> {
    golden()
        .fixtures
        .iter()
        .filter(|f| f.template_id == template_id)
        .collect()
}

fn golden() -> &'static Golden {
    static GOLDEN: OnceLock<Golden> = OnceLock::new();
    GOLDEN.get_or_init(Golden::load)
}

/// `tests/fixtures/sbe/golden.tsv` and the files it names, in memory.
struct Golden {
    fixtures: Vec<Fixture>,
}

/// Where a reading came from — the message header, or the body — and the name
/// the schema gives the field. The two are kept apart because a message may
/// have a body field called `version` while the header has one of its own.
type Origin = (String, String);

/// A reading as `golden.tsv` writes it: the type, and the value.
type Reading = (String, String);

/// Acted versions, each with what the reference read at it.
type Readings = BTreeMap<Origin, Reading>;

struct Fixture {
    name: String,
    schema_id: u16,
    template_id: u16,
    /// The version the reference encoded at: the schema's own. Always the
    /// highest in `readings`, and the only one a fixture may be re-encoded at.
    version: u16,
    bytes: Vec<u8>,
    readings: BTreeMap<u16, Readings>,
}

impl Golden {
    fn load() -> Self {
        let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("fixtures/sbe");
        let table = fs::read_to_string(directory.join("golden.tsv"))
            .expect("tests/fixtures/sbe/golden.tsv is missing");

        let mut fixtures: Vec<Fixture> = Vec::new();
        let mut by_name: BTreeMap<String, usize> = BTreeMap::new();

        for line in table.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let columns: Vec<&str> = line.split('\t').collect();

            match columns[0] {
                "fixture" => {
                    let [_, name, file, schema_id, template_id, version, bytes] = columns[..]
                    else {
                        panic!("malformed fixture line: {line}");
                    };
                    let content =
                        fs::read(directory.join(file)).unwrap_or_else(|e| panic!("{file}: {e}"));
                    // The table states the length, so a fixture truncated on
                    // the way into the repository is caught here rather than
                    // as a decoder that reads past the end of something.
                    assert_eq!(
                        content.len(),
                        bytes.parse::<usize>().unwrap(),
                        "{file}: length does not match golden.tsv"
                    );

                    by_name.insert(name.to_string(), fixtures.len());
                    fixtures.push(Fixture {
                        name: name.to_string(),
                        schema_id: schema_id.parse().unwrap(),
                        template_id: template_id.parse().unwrap(),
                        version: version.parse().unwrap(),
                        bytes: content,
                        readings: BTreeMap::new(),
                    });
                }
                tag @ ("header" | "field") => {
                    let [_, name, version, field, kind, value] = columns[..] else {
                        panic!("malformed reading line: {line}");
                    };
                    let index = by_name[name];
                    fixtures[index]
                        .readings
                        .entry(version.parse().unwrap())
                        .or_default()
                        .insert(
                            (tag.to_string(), field.to_string()),
                            (kind.to_string(), value.to_string()),
                        );
                }
                other => panic!("unknown record {other:?} in golden.tsv"),
            }
        }

        // The schema's own version is the one the reference encoded at, and it
        // is what the message header carries. Anything else in the table is a
        // reading at a version nothing can be encoded at.
        for fixture in &fixtures {
            assert_eq!(
                Some(&fixture.version),
                fixture.readings.keys().next_back(),
                "{}: the schema's version is not the highest one recorded",
                fixture.name,
            );
        }

        Self { fixtures }
    }
}

impl Fixture {
    /// The acting versions to read this fixture at, highest first.
    fn versions(&self) -> Vec<u16> {
        self.readings.keys().copied().rev().collect()
    }

    /// The fixture's bytes, with the header's version lowered when the reading
    /// is not at the schema's own. That byte is the whole difference: the
    /// reference's encoder has no version guard, so an old version exists only
    /// as something to read (fixtures/sbe/README.md).
    fn bytes_at(&self, version: u16) -> Vec<u8> {
        let mut bytes = self.bytes.clone();
        if version != self.version {
            bytes[HEADER_VERSION_OFFSET..HEADER_VERSION_OFFSET + 2]
                .copy_from_slice(&version.to_le_bytes());
        }
        bytes
    }

    fn reading(&self, version: u16, tag: &str, field: &str) -> (&str, &str) {
        self.readings
            .get(&version)
            .and_then(|readings| readings.get(&(tag.to_string(), field.to_string())))
            .map(|(kind, value)| (kind.as_str(), value.as_str()))
            .unwrap_or_else(|| {
                panic!(
                    "{}: golden.tsv has no {tag} {field} at acting version {version}",
                    self.name
                )
            })
    }

    fn header(&self, version: u16, field: &str) -> u16 {
        let (kind, value) = self.reading(version, "header", field);
        assert_eq!("u16", kind, "{}: header {field}", self.name);
        value.parse().unwrap()
    }

    /// The reference's value for a whole-number field, held to the type the
    /// schema gives it: a field we believe is `i64` and the schema calls `i32`
    /// would otherwise agree on every small value.
    fn int(&self, version: u16, field: &str, expected: &str) -> i64 {
        let (kind, value) = self.reading(version, "field", field);
        assert_eq!(expected, base_type(kind), "{}: field {field}", self.name);
        value.parse().unwrap()
    }

    /// The reference's answer as written, null value and all — what an encoder
    /// that has no way to say "absent" has to write.
    fn raw_int(&self, version: u16, field: &str) -> i64 {
        self.reading(version, "field", field).1.parse().unwrap()
    }

    /// The same, as a decoder's `Option`: `None` where the answer is the type's
    /// null value, which is the one thing an `Option`-returning accessor and a
    /// raw one can disagree about.
    fn optional_int(&self, version: u16, field: &str) -> Option<i64> {
        let (kind, value) = self.reading(version, "field", field);
        let (_, null) = kind.split_once(":null=").unwrap_or_else(|| {
            panic!(
                "{}: field {field} is {kind}, not an optional type",
                self.name
            )
        });
        let raw: i64 = value.parse().unwrap();
        if raw == null.parse::<i64>().unwrap() {
            None
        } else {
            Some(raw)
        }
    }

    fn text(&self, version: u16, field: &str) -> &str {
        let (kind, value) = self.reading(version, "field", field);
        assert_eq!("ascii", kind, "{}: field {field}", self.name);
        value
    }

    fn data(&self, version: u16, field: &str) -> Vec<u8> {
        let (kind, value) = self.reading(version, "field", field);
        assert_eq!("data", kind, "{}: field {field}", self.name);
        assert_eq!(
            0,
            value.len() % 2,
            "{}: field {field} is not a whole number of bytes",
            self.name
        );
        (0..value.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&value[i..i + 2], 16).unwrap())
            .collect()
    }

    /// The reference's enum as `(name, raw)`. Both are pinned; see
    /// `golden_decode!`.
    fn enum_value(&self, version: u16, field: &str) -> (&str, i32) {
        let (kind, value) = self.reading(version, "field", field);
        assert!(
            kind.starts_with("enum:"),
            "{}: field {field} is {kind}, not an enum",
            self.name
        );
        let (name, raw) = value
            .split_once(':')
            .expect("an enum reading is <NAME>:<raw>");
        (name, raw.parse().unwrap())
    }
}

// The messages, in schema order — schema 101 first, then the mark header.
// `golden!` writes each one's test from the list of its fields.
golden!(control_response, 1,
    archive, archive::message_header_codec, archive::control_response_codec,
    ControlResponseEncoder, ControlResponseDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 relevant_id: "relevantId",
    enum code as archive::control_response_code::ControlResponseCode: "code",
    optional version: "version",
    text error_message / error_message_decoder: "errorMessage",
});

golden!(close_session_request, 3,
    archive, archive::message_header_codec, archive::close_session_request_codec,
    CloseSessionRequestEncoder, CloseSessionRequestDecoder, {
    i64 control_session_id: "controlSessionId",
});

golden!(start_recording_request, 4,
    archive, archive::message_header_codec, archive::start_recording_request_codec,
    StartRecordingRequestEncoder, StartRecordingRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i32 stream_id: "streamId",
    enum source_location as archive::source_location::SourceLocation: "sourceLocation",
    text channel / channel_decoder: "channel",
});

golden!(stop_recording_request, 5,
    archive, archive::message_header_codec, archive::stop_recording_request_codec,
    StopRecordingRequestEncoder, StopRecordingRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i32 stream_id: "streamId",
    text channel / channel_decoder: "channel",
});

golden!(replay_request, 6,
    archive, archive::message_header_codec, archive::replay_request_codec,
    ReplayRequestEncoder, ReplayRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i64 position: "position",
    i64 length: "length",
    i32 replay_stream_id: "replayStreamId",
    i32 file_io_max_length: "fileIoMaxLength",
    i64 replay_token: "replayToken",
    text replay_channel / replay_channel_decoder: "replayChannel",
});

golden!(stop_replay_request, 7,
    archive, archive::message_header_codec, archive::stop_replay_request_codec,
    StopReplayRequestEncoder, StopReplayRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 replay_session_id: "replaySessionId",
});

golden!(list_recordings_request, 8,
    archive, archive::message_header_codec, archive::list_recordings_request_codec,
    ListRecordingsRequestEncoder, ListRecordingsRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 from_recording_id: "fromRecordingId",
    i32 record_count: "recordCount",
});

golden!(list_recordings_for_uri_request, 9,
    archive, archive::message_header_codec, archive::list_recordings_for_uri_request_codec,
    ListRecordingsForUriRequestEncoder, ListRecordingsForUriRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 from_recording_id: "fromRecordingId",
    i32 record_count: "recordCount",
    i32 stream_id: "streamId",
    text channel / channel_decoder: "channel",
});

golden!(list_recording_request, 10,
    archive, archive::message_header_codec, archive::list_recording_request_codec,
    ListRecordingRequestEncoder, ListRecordingRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(extend_recording_request, 11,
    archive, archive::message_header_codec, archive::extend_recording_request_codec,
    ExtendRecordingRequestEncoder, ExtendRecordingRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i32 stream_id: "streamId",
    enum source_location as archive::source_location::SourceLocation: "sourceLocation",
    text channel / channel_decoder: "channel",
});

golden!(recording_position_request, 12,
    archive, archive::message_header_codec, archive::recording_position_request_codec,
    RecordingPositionRequestEncoder, RecordingPositionRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(truncate_recording_request, 13,
    archive, archive::message_header_codec, archive::truncate_recording_request_codec,
    TruncateRecordingRequestEncoder, TruncateRecordingRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i64 position: "position",
});

golden!(stop_recording_subscription_request, 14,
    archive, archive::message_header_codec, archive::stop_recording_subscription_request_codec,
    StopRecordingSubscriptionRequestEncoder, StopRecordingSubscriptionRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 subscription_id: "subscriptionId",
});

golden!(stop_position_request, 15,
    archive, archive::message_header_codec, archive::stop_position_request_codec,
    StopPositionRequestEncoder, StopPositionRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(find_last_matching_recording_request, 16,
    archive, archive::message_header_codec, archive::find_last_matching_recording_request_codec,
    FindLastMatchingRecordingRequestEncoder, FindLastMatchingRecordingRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 min_recording_id: "minRecordingId",
    i32 session_id: "sessionId",
    i32 stream_id: "streamId",
    text channel / channel_decoder: "channel",
});

golden!(list_recording_subscriptions_request, 17,
    archive, archive::message_header_codec, archive::list_recording_subscriptions_request_codec,
    ListRecordingSubscriptionsRequestEncoder, ListRecordingSubscriptionsRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i32 pseudo_index: "pseudoIndex",
    i32 subscription_count: "subscriptionCount",
    enum apply_stream_id as archive::boolean_type::BooleanType: "applyStreamId",
    i32 stream_id: "streamId",
    text channel / channel_decoder: "channel",
});

golden!(bounded_replay_request, 18,
    archive, archive::message_header_codec, archive::bounded_replay_request_codec,
    BoundedReplayRequestEncoder, BoundedReplayRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i64 position: "position",
    i64 length: "length",
    i32 limit_counter_id: "limitCounterId",
    i32 replay_stream_id: "replayStreamId",
    i32 file_io_max_length: "fileIoMaxLength",
    i64 replay_token: "replayToken",
    text replay_channel / replay_channel_decoder: "replayChannel",
});

golden!(stop_all_replays_request, 19,
    archive, archive::message_header_codec, archive::stop_all_replays_request_codec,
    StopAllReplaysRequestEncoder, StopAllReplaysRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(catalog_header, 20,
    archive, archive::message_header_codec, archive::catalog_header_codec,
    CatalogHeaderEncoder, CatalogHeaderDecoder, {
    i32 version: "version",
    i32 length: "length",
    i64 next_recording_id: "nextRecordingId",
    i32 alignment: "alignment",
    i8 reserved: "reserved",
});

golden!(recording_descriptor_header, 21,
    archive, archive::message_header_codec, archive::recording_descriptor_header_codec,
    RecordingDescriptorHeaderEncoder, RecordingDescriptorHeaderDecoder, {
    i32 length: "length",
    enum state as archive::recording_state::RecordingState: "state",
    i32 checksum: "checksum",
    i8 reserved: "reserved",
});

golden!(recording_descriptor, 22,
    archive, archive::message_header_codec, archive::recording_descriptor_codec,
    RecordingDescriptorEncoder, RecordingDescriptorDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i64 start_timestamp: "startTimestamp",
    i64 stop_timestamp: "stopTimestamp",
    i64 start_position: "startPosition",
    i64 stop_position: "stopPosition",
    i32 initial_term_id: "initialTermId",
    i32 segment_file_length: "segmentFileLength",
    i32 term_buffer_length: "termBufferLength",
    i32 mtu_length: "mtuLength",
    i32 session_id: "sessionId",
    i32 stream_id: "streamId",
    text stripped_channel / stripped_channel_decoder: "strippedChannel",
    text original_channel / original_channel_decoder: "originalChannel",
    text source_identity / source_identity_decoder: "sourceIdentity",
});

golden!(recording_subscription_descriptor, 23,
    archive, archive::message_header_codec, archive::recording_subscription_descriptor_codec,
    RecordingSubscriptionDescriptorEncoder, RecordingSubscriptionDescriptorDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 subscription_id: "subscriptionId",
    i32 stream_id: "streamId",
    text stripped_channel / stripped_channel_decoder: "strippedChannel",
});

golden!(recording_signal_event, 24,
    archive, archive::message_header_codec, archive::recording_signal_event_codec,
    RecordingSignalEventEncoder, RecordingSignalEventDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i64 subscription_id: "subscriptionId",
    i64 position: "position",
    enum signal as archive::recording_signal::RecordingSignal: "signal",
});

golden!(replicate_request, 50,
    archive, archive::message_header_codec, archive::replicate_request_codec,
    ReplicateRequestEncoder, ReplicateRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 src_recording_id: "srcRecordingId",
    i64 dst_recording_id: "dstRecordingId",
    i32 src_control_stream_id: "srcControlStreamId",
    text src_control_channel / src_control_channel_decoder: "srcControlChannel",
    text live_destination / live_destination_decoder: "liveDestination",
});

golden!(stop_replication_request, 51,
    archive, archive::message_header_codec, archive::stop_replication_request_codec,
    StopReplicationRequestEncoder, StopReplicationRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 replication_id: "replicationId",
});

golden!(start_position_request, 52,
    archive, archive::message_header_codec, archive::start_position_request_codec,
    StartPositionRequestEncoder, StartPositionRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(detach_segments_request, 53,
    archive, archive::message_header_codec, archive::detach_segments_request_codec,
    DetachSegmentsRequestEncoder, DetachSegmentsRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i64 new_start_position: "newStartPosition",
});

golden!(delete_detached_segments_request, 54,
    archive, archive::message_header_codec, archive::delete_detached_segments_request_codec,
    DeleteDetachedSegmentsRequestEncoder, DeleteDetachedSegmentsRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(purge_segments_request, 55,
    archive, archive::message_header_codec, archive::purge_segments_request_codec,
    PurgeSegmentsRequestEncoder, PurgeSegmentsRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i64 new_start_position: "newStartPosition",
});

golden!(attach_segments_request, 56,
    archive, archive::message_header_codec, archive::attach_segments_request_codec,
    AttachSegmentsRequestEncoder, AttachSegmentsRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(migrate_segments_request, 57,
    archive, archive::message_header_codec, archive::migrate_segments_request_codec,
    MigrateSegmentsRequestEncoder, MigrateSegmentsRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 src_recording_id: "srcRecordingId",
    i64 dst_recording_id: "dstRecordingId",
});

golden!(auth_connect_request, 58,
    archive, archive::message_header_codec, archive::auth_connect_request_codec,
    AuthConnectRequestEncoder, AuthConnectRequestDecoder, {
    i64 correlation_id: "correlationId",
    i32 response_stream_id: "responseStreamId",
    optional version: "version",
    text response_channel / response_channel_decoder: "responseChannel",
    data encoded_credentials / encoded_credentials_decoder: "encodedCredentials",
    text client_info / client_info_decoder: "clientInfo",
});

golden!(challenge, 59,
    archive, archive::message_header_codec, archive::challenge_codec,
    ChallengeEncoder, ChallengeDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    optional version: "version",
    data encoded_challenge / encoded_challenge_decoder: "encodedChallenge",
});

golden!(challenge_response, 60,
    archive, archive::message_header_codec, archive::challenge_response_codec,
    ChallengeResponseEncoder, ChallengeResponseDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    data encoded_credentials / encoded_credentials_decoder: "encodedCredentials",
});

golden!(keep_alive_request, 61,
    archive, archive::message_header_codec, archive::keep_alive_request_codec,
    KeepAliveRequestEncoder, KeepAliveRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
});

golden!(tagged_replicate_request, 62,
    archive, archive::message_header_codec, archive::tagged_replicate_request_codec,
    TaggedReplicateRequestEncoder, TaggedReplicateRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 src_recording_id: "srcRecordingId",
    i64 dst_recording_id: "dstRecordingId",
    i64 channel_tag_id: "channelTagId",
    i64 subscription_tag_id: "subscriptionTagId",
    i32 src_control_stream_id: "srcControlStreamId",
    text src_control_channel / src_control_channel_decoder: "srcControlChannel",
    text live_destination / live_destination_decoder: "liveDestination",
});

golden!(start_recording_request_2, 63,
    archive, archive::message_header_codec, archive::start_recording_request_2_codec,
    StartRecordingRequest2Encoder, StartRecordingRequest2Decoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i32 stream_id: "streamId",
    enum source_location as archive::source_location::SourceLocation: "sourceLocation",
    enum auto_stop as archive::boolean_type::BooleanType: "autoStop",
    text channel / channel_decoder: "channel",
});

golden!(extend_recording_request_2, 64,
    archive, archive::message_header_codec, archive::extend_recording_request_2_codec,
    ExtendRecordingRequest2Encoder, ExtendRecordingRequest2Decoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    i32 stream_id: "streamId",
    enum source_location as archive::source_location::SourceLocation: "sourceLocation",
    enum auto_stop as archive::boolean_type::BooleanType: "autoStop",
    text channel / channel_decoder: "channel",
});

golden!(stop_recording_by_identity_request, 65,
    archive, archive::message_header_codec, archive::stop_recording_by_identity_request_codec,
    StopRecordingByIdentityRequestEncoder, StopRecordingByIdentityRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(replicate_request_2, 66,
    archive, archive::message_header_codec, archive::replicate_request_2_codec,
    ReplicateRequest2Encoder, ReplicateRequest2Decoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 src_recording_id: "srcRecordingId",
    i64 dst_recording_id: "dstRecordingId",
    i64 stop_position: "stopPosition",
    i64 channel_tag_id: "channelTagId",
    i64 subscription_tag_id: "subscriptionTagId",
    i32 src_control_stream_id: "srcControlStreamId",
    i32 file_io_max_length: "fileIoMaxLength",
    i32 replication_session_id: "replicationSessionId",
    text src_control_channel / src_control_channel_decoder: "srcControlChannel",
    text live_destination / live_destination_decoder: "liveDestination",
    text replication_channel / replication_channel_decoder: "replicationChannel",
    data encoded_credentials / encoded_credentials_decoder: "encodedCredentials",
    text src_response_channel / src_response_channel_decoder: "srcResponseChannel",
});

golden!(max_recorded_position_request, 67,
    archive, archive::message_header_codec, archive::max_recorded_position_request_codec,
    MaxRecordedPositionRequestEncoder, MaxRecordedPositionRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(archive_id_request, 68,
    archive, archive::message_header_codec, archive::archive_id_request_codec,
    ArchiveIdRequestEncoder, ArchiveIdRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
});

golden!(recording_started, 101,
    archive, archive::message_header_codec, archive::recording_started_codec,
    RecordingStartedEncoder, RecordingStartedDecoder, {
    i64 recording_id: "recordingId",
    i64 start_position: "startPosition",
    i32 session_id: "sessionId",
    i32 stream_id: "streamId",
    text channel / channel_decoder: "channel",
    text source_identity / source_identity_decoder: "sourceIdentity",
});

golden!(recording_progress, 102,
    archive, archive::message_header_codec, archive::recording_progress_codec,
    RecordingProgressEncoder, RecordingProgressDecoder, {
    i64 recording_id: "recordingId",
    i64 start_position: "startPosition",
    i64 position: "position",
});

golden!(recording_stopped, 103,
    archive, archive::message_header_codec, archive::recording_stopped_codec,
    RecordingStoppedEncoder, RecordingStoppedDecoder, {
    i64 recording_id: "recordingId",
    i64 start_position: "startPosition",
    i64 stop_position: "stopPosition",
});

golden!(purge_recording_request, 104,
    archive, archive::message_header_codec, archive::purge_recording_request_codec,
    PurgeRecordingRequestEncoder, PurgeRecordingRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(replay_token_request, 105,
    archive, archive::message_header_codec, archive::replay_token_request_codec,
    ReplayTokenRequestEncoder, ReplayTokenRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
});

golden!(ping, 106,
    archive, archive::message_header_codec, archive::ping_codec,
    PingEncoder, PingDecoder, {
    i64 control_session_id: "controlSessionId",
});

golden!(update_channel_request, 107,
    archive, archive::message_header_codec, archive::update_channel_request_codec,
    UpdateChannelRequestEncoder, UpdateChannelRequestDecoder, {
    i64 control_session_id: "controlSessionId",
    i64 correlation_id: "correlationId",
    i64 recording_id: "recordingId",
    text channel / channel_decoder: "channel",
});

golden!(mark_file_header, 200,
    archive_mark, archive_mark::message_header_codec, archive_mark::mark_file_header_codec,
    MarkFileHeaderEncoder, MarkFileHeaderDecoder, {
    i32 version: "version",
    i64 activity_timestamp: "activityTimestamp",
    i64 start_timestamp: "startTimestamp",
    i64 pid: "pid",
    i32 control_stream_id: "controlStreamId",
    i32 local_control_stream_id: "localControlStreamId",
    i32 events_stream_id: "eventsStreamId",
    optional header_length: "headerLength",
    optional error_buffer_length: "errorBufferLength",
    optional archive_id: "archiveId",
    text control_channel / control_channel_decoder: "controlChannel",
    text local_control_channel / local_control_channel_decoder: "localControlChannel",
    text events_channel / events_channel_decoder: "eventsChannel",
    text aeron_directory / aeron_directory_decoder: "aeronDirectory",
});
