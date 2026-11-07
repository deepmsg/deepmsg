//! The record: one JSON object, encoded into a fixed-size buffer.
//!
//! The calling thread does no allocation (ADR-0003). [`Encoder`] writes straight
//! into a stack buffer; [`Record`] is that buffer plus its length, and the only
//! thing the queue will ever move (512 bytes, `Copy`). Formatting, escaping and
//! truncation all happen here, on the caller, so the writer thread only has to
//! append a newline and write bytes.
//!
//! The schema and its field order are a contract:
//!
//! ```text
//! {"ts":RFC3339Nano,"level":..,"component":..,"node_id":..,"event":..,
//!  ["msg":..,][fields...][,"truncated":true]}
//! ```
//!
//! `ts`, `level`, `component`, `node_id` and `event` are always present. `msg`
//! is optional; extra fields are flat (not nested under `fields`) because that
//! is what LogsQL queries best. `truncated` appears only when a value did not
//! fit, and anything the encoder could not fit is dropped whole, so the line is
//! always valid JSON and never ends mid-key.
//!
//! # Reserved names
//!
//! The five fixed fields plus `msg` and `truncated` are reserved, as are the
//! trace fields the schema keeps for later. A caller may only set them through
//! their dedicated methods (`message`); `field_str` and friends ignore a
//! reserved name in release builds and trip a `debug_assert!` in debug builds.
//! A test keeps the list and the methods honest.

use crate::event::{Component, Event};
use crate::level::Level;
use crate::time::Timestamp;

/// Bytes in one record. A line with the fixed fields and a short message fits
/// comfortably; the queue's memory bound is `RECORD_CAPACITY` times its length.
pub const RECORD_CAPACITY: usize = 512;

/// Room kept for `,"truncated":true}` after the body.
const RESERVE: usize = 32;
/// The body must end before this offset; the reserve belongs to `finish`.
const BODY_LIMIT: usize = RECORD_CAPACITY - RESERVE;

const RESERVED_FIELDS: [&str; 12] = [
    "ts",
    "level",
    "component",
    "node_id",
    "event",
    "msg",
    "truncated",
    "trace_id",
    "span_id",
    "target",
    "file",
    "line",
];

/// One encoded log line.
#[derive(Clone, Copy)]
pub struct Record {
    len: u16,
    bytes: [u8; RECORD_CAPACITY],
}

impl Record {
    /// The JSON bytes, without a trailing newline.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }
}

/// Whether a field name belongs to the schema and cannot be set as an extra.
pub fn is_reserved(name: &str) -> bool {
    RESERVED_FIELDS.contains(&name)
}

/// Builds one [`Record`].
///
/// Consuming `self` lets a whole line be written as one expression; the buffer
/// is `Copy`, so moving it between methods is free in practice.
pub struct Encoder {
    bytes: [u8; RECORD_CAPACITY],
    len: usize,
    truncated: bool,
}

impl Encoder {
    /// Start a record from the built-in vocabulary. The five fixed fields are
    /// written here.
    pub fn begin(
        nanos: i64,
        level: Level,
        component: Component,
        node_id: &str,
        event: Event,
    ) -> Self {
        Self::begin_named(nanos, level, component.as_str(), node_id, event.as_str())
    }

    /// Start a record from names, which may come from an application's own
    /// [`crate::Name`] implementations.
    pub fn begin_named(
        nanos: i64,
        level: Level,
        component: &'static str,
        node_id: &str,
        event: &'static str,
    ) -> Self {
        let mut encoder = Self {
            bytes: [0; RECORD_CAPACITY],
            len: 0,
            truncated: false,
        };

        // Fixed text is small and always fits before `node_id`. The suffix
        // after `node_id` is reserved out of the node id's budget, because the
        // fixed writes that follow cannot roll themselves back.
        encoder.push_fixed("{\"ts\":\"");
        let timestamp = Timestamp::new(nanos);
        encoder.raw(timestamp.as_str().as_bytes());
        encoder.push_fixed("\",\"level\":\"");
        encoder.push_fixed(level.as_str());
        encoder.push_fixed("\",\"component\":\"");
        encoder.push_fixed(component);
        encoder.push_fixed("\",\"node_id\":");

        let suffix_length = ",\"event\":\"".len() + event.len() + 1;
        let node_limit = BODY_LIMIT - suffix_length;
        encoder.push_value_string(node_id, node_limit);

        encoder.push_fixed(",\"event\":\"");
        encoder.push_fixed(event);
        encoder.push_fixed("\"");

        encoder
    }

    /// Set the human-readable message.
    pub fn message(mut self, text: &str) -> Self {
        self.string_field("msg", text);
        self
    }

    /// Set a string field.
    pub fn field_str(mut self, name: &'static str, value: &str) -> Self {
        if is_reserved(name) {
            debug_assert!(false, "reserved log field set as an extra: {name}");
            return self;
        }
        self.string_field(name, value);
        self
    }

    /// Set a signed integer field.
    pub fn field_i64(mut self, name: &'static str, value: i64) -> Self {
        if is_reserved(name) {
            debug_assert!(false, "reserved log field set as an extra: {name}");
            return self;
        }
        let checkpoint = self.len;
        if !self.key(name) || !self.push_i64(value) {
            self.len = checkpoint;
            self.truncated = true;
        }
        self
    }

    /// Set an unsigned integer field.
    pub fn field_u64(mut self, name: &'static str, value: u64) -> Self {
        if is_reserved(name) {
            debug_assert!(false, "reserved log field set as an extra: {name}");
            return self;
        }
        let checkpoint = self.len;
        if !self.key(name) || !self.push_u64(value) {
            self.len = checkpoint;
            self.truncated = true;
        }
        self
    }

    /// Set a boolean field.
    pub fn field_bool(mut self, name: &'static str, value: bool) -> Self {
        if is_reserved(name) {
            debug_assert!(false, "reserved log field set as an extra: {name}");
            return self;
        }
        let checkpoint = self.len;
        let text = if value { "true" } else { "false" };
        if !self.key(name) || !self.try_push(text) {
            self.len = checkpoint;
            self.truncated = true;
        }
        self
    }

    /// Close the object and hand the bytes to the queue.
    pub fn finish(mut self) -> Record {
        if self.truncated {
            // The reserve exists for exactly this suffix and the closing brace.
            self.push_final(",\"truncated\":true");
        }
        self.push_final("}");

        let mut record = Record {
            len: 0,
            bytes: [0; RECORD_CAPACITY],
        };
        record.bytes[..self.len].copy_from_slice(&self.bytes[..self.len]);
        record.len = u16::try_from(self.len).expect("a record is smaller than u16::MAX");
        record
    }

    /// Write `,"name":`, or answer false without writing anything.
    fn key(&mut self, name: &str) -> bool {
        let checkpoint = self.len;
        if self.try_push(",\"") && self.try_push(name) && self.try_push("\":") {
            true
        } else {
            self.len = checkpoint;
            false
        }
    }

    /// Write a string field, truncating at a character boundary if needed.
    fn string_field(&mut self, name: &str, value: &str) {
        let checkpoint = self.len;
        // The key plus both quotes have to fit before the prefix is written;
        // otherwise the line would end with a key and no value.
        let needed = name.len() + 4 + 2;
        if self.len + needed > BODY_LIMIT {
            self.truncated = true;
            return;
        }
        if !self.key(name) {
            self.len = checkpoint;
            self.truncated = true;
            return;
        }
        self.push_value_string(value, BODY_LIMIT);
    }

    /// Write a quoted, escaped string, stopping at `end_limit`.
    ///
    /// Truncation is at a `char` boundary: a multi-byte character is either
    /// whole or absent, never replaced by half a code point.
    fn push_value_string(&mut self, value: &str, end_limit: usize) -> bool {
        if self.len + 2 > end_limit {
            self.truncated = true;
            return true;
        }

        self.raw(b"\"");

        let mut cut = false;
        for character in value.chars() {
            let width = escaped_width(character);
            // One byte is kept for the closing quote.
            if self.len + width + 1 > end_limit {
                cut = true;
                break;
            }
            self.write_escaped(character);
        }

        self.raw(b"\"");
        if cut {
            self.truncated = true;
        }
        cut
    }

    fn write_escaped(&mut self, character: char) {
        match character {
            '"' => self.raw(b"\\\""),
            '\\' => self.raw(b"\\\\"),
            '\n' => self.raw(b"\\n"),
            '\r' => self.raw(b"\\r"),
            '\t' => self.raw(b"\\t"),
            control if (control as u32) < 0x20 => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let code = control as u32;
                self.raw(b"\\u00");
                self.raw(&[HEX[(code >> 4) as usize], HEX[(code & 0xF) as usize]]);
            }
            other => {
                let mut buffer = [0_u8; 4];
                self.raw(other.encode_utf8(&mut buffer).as_bytes());
            }
        }
    }

    fn push_i64(&mut self, value: i64) -> bool {
        let mut buffer = [0_u8; 20];
        let text = format_i64(value, &mut buffer);
        self.try_push(text)
    }

    fn push_u64(&mut self, value: u64) -> bool {
        let mut buffer = [0_u8; 20];
        let text = format_u64(value, &mut buffer);
        self.try_push(text)
    }

    /// Append fixed schema text. It always fits by construction; a failure is
    /// a bug in the budget, caught in debug builds.
    fn push_fixed(&mut self, text: &str) {
        let pushed = self.try_push(text);
        debug_assert!(pushed, "fixed record text did not fit: {text}");
    }

    /// Append text if it fits the body; no bytes are written on failure.
    fn try_push(&mut self, text: &str) -> bool {
        let end = self.len + text.len();
        if end > BODY_LIMIT {
            return false;
        }
        self.raw(text.as_bytes());
        true
    }

    /// Append text into the reserve. Only `finish` uses this, and only for the
    /// two suffixes the reserve was sized for.
    fn push_final(&mut self, text: &str) {
        let end = self.len + text.len();
        debug_assert!(end <= RECORD_CAPACITY);
        if end <= RECORD_CAPACITY {
            self.raw(text.as_bytes());
        }
    }

    /// Copy bytes without a bounds check. Every caller has checked first.
    fn raw(&mut self, bytes: &[u8]) {
        let end = self.len + bytes.len();
        debug_assert!(end <= RECORD_CAPACITY);
        self.bytes[self.len..end].copy_from_slice(bytes);
        self.len = end;
    }
}

/// How many bytes an escaped character occupies.
fn escaped_width(character: char) -> usize {
    match character {
        '"' | '\\' | '\n' | '\r' | '\t' => 2,
        control if (control as u32) < 0x20 => 6,
        other => other.len_utf8(),
    }
}

fn format_i64(value: i64, buffer: &mut [u8; 20]) -> &str {
    let negative = value < 0;
    let mut magnitude = value.unsigned_abs();
    let mut index = buffer.len();

    loop {
        index -= 1;
        buffer[index] = b'0' + (magnitude % 10) as u8;
        magnitude /= 10;
        if magnitude == 0 {
            break;
        }
    }
    if negative {
        index -= 1;
        buffer[index] = b'-';
    }

    std::str::from_utf8(&buffer[index..]).expect("digits are ASCII")
}

fn format_u64(value: u64, buffer: &mut [u8; 20]) -> &str {
    let mut magnitude = value;
    let mut index = buffer.len();

    loop {
        index -= 1;
        buffer[index] = b'0' + (magnitude % 10) as u8;
        magnitude /= 10;
        if magnitude == 0 {
            break;
        }
    }

    std::str::from_utf8(&buffer[index..]).expect("digits are ASCII")
}

#[cfg(test)]
mod tests {
    use super::{Encoder, RECORD_CAPACITY, is_reserved};
    use crate::event::{Component, Event};
    use crate::level::Level;

    fn line(encoder: Encoder) -> String {
        let record = encoder.finish();
        std::str::from_utf8(record.as_bytes())
            .expect("the encoder writes UTF-8")
            .to_owned()
    }

    #[test]
    fn a_short_line_is_exactly_the_schema() {
        assert_eq!(
            "{\"ts\":\"1970-01-01T00:00:00.000000000Z\",\"level\":\"info\",\"component\":\"driver\",\"node_id\":\"node-1\",\"event\":\"role.change\",\"msg\":\"became leader\",\"role\":\"leader\",\"term\":42}",
            line(
                Encoder::begin(
                    0,
                    Level::Info,
                    Component::Driver,
                    "node-1",
                    Event::RoleChange
                )
                .message("became leader")
                .field_str("role", "leader")
                .field_i64("term", 42)
            )
        );
    }

    #[test]
    fn strings_are_escaped_and_unicode_is_not() {
        assert_eq!(
            "{\"ts\":\"1970-01-01T00:00:00.000000000Z\",\"level\":\"warn\",\"component\":\"tool\",\"node_id\":\"n\\\"1\",\"event\":\"log.dropped\",\"msg\":\"a\\\"b\\\\c\\nd\\te\\u0001f😀\"}",
            line(
                Encoder::begin(0, Level::Warn, Component::Tool, "n\"1", Event::LogDropped)
                    .message("a\"b\\c\nd\te\u{1}f😀")
            )
        );
    }

    #[test]
    fn an_empty_message_is_still_a_field() {
        assert_eq!(
            "{\"ts\":\"1970-01-01T00:00:00.000000000Z\",\"level\":\"info\",\"component\":\"client\",\"node_id\":\"n\",\"event\":\"segment.roll\",\"msg\":\"\"}",
            line(
                Encoder::begin(0, Level::Info, Component::Client, "n", Event::SegmentRoll)
                    .message("")
            )
        );
    }

    #[test]
    fn the_smallest_and_largest_integers_survive() {
        let smallest = line(
            Encoder::begin(0, Level::Info, Component::Archive, "n", Event::SegmentRoll)
                .field_i64("smallest", i64::MIN),
        );
        assert!(smallest.ends_with(",\"smallest\":-9223372036854775808}"));

        let largest = line(
            Encoder::begin(0, Level::Info, Component::Archive, "n", Event::SegmentRoll)
                .field_u64("largest", u64::MAX),
        );
        assert!(largest.ends_with(",\"largest\":18446744073709551615}"));
    }

    #[test]
    fn a_long_message_is_truncated_at_a_character_boundary() {
        let message = "é".repeat(400);
        let text = line(
            Encoder::begin(0, Level::Info, Component::Client, "n", Event::RoleChange)
                .message(&message),
        );

        assert!(text.ends_with(",\"truncated\":true}"));
        assert!(!text.contains('\u{fffd}'), "a code point was split");
        assert!(std::str::from_utf8(text.as_bytes()).is_ok());

        // Every character in the message was whole; 400 chars would not fit.
        let message_start = text.find("\"msg\":\"").expect("message") + 7;
        let message_end = message_start + text[message_start..].find('"').expect("closing quote");
        let prefix = &text[message_start..message_end];
        assert!(prefix.chars().all(|character| character == 'é'));
    }

    #[test]
    fn a_long_node_id_is_truncated_before_the_event() {
        let node_id = "n".repeat(RECORD_CAPACITY * 2);
        let text = line(Encoder::begin(
            0,
            Level::Info,
            Component::Driver,
            &node_id,
            Event::RoleChange,
        ));
        assert!(text.ends_with(",\"event\":\"role.change\",\"truncated\":true}"));
        assert!(std::str::from_utf8(text.as_bytes()).is_ok());
    }

    #[test]
    fn too_many_fields_drop_whole_fields_and_stay_valid_json() {
        let mut encoder = Encoder::begin(0, Level::Info, Component::Driver, "n", Event::RoleChange);
        for index in 0..200 {
            encoder = encoder.field_u64("f", index);
        }
        let text = line(encoder);

        assert!(text.starts_with('{'));
        assert!(text.ends_with('}'));
        assert!(text.contains(",\"truncated\":true"));
        assert!(!text.contains(",,\""));
        assert!(std::str::from_utf8(text.as_bytes()).is_ok());
    }

    #[test]
    fn reserved_names_are_the_schema() {
        for name in [
            "ts",
            "level",
            "component",
            "node_id",
            "event",
            "msg",
            "truncated",
        ] {
            assert!(is_reserved(name), "{name} is not reserved");
        }
        assert!(!is_reserved("term"));
        assert!(!is_reserved("stream_id"));
    }
}
