//! `ts`: nanoseconds since the Unix epoch, written as RFC3339Nano UTC.
//!
//! The collector parses this field with the Go layout
//! `2006-01-02T15:04:05.999999999Z07:00` (`layout_type: gotime`), so the
//! format is a contract, not a preference: nine fractional digits, a `Z`, and
//! no local offset. The conversion is here rather than in a dependency because
//! the crate is dependency-free by design (L0a plan §5, Q1) and because the
//! date arithmetic is small and can be pinned with golden values.
//!
//! The civil-date inverse is Howard Hinnant's `civil_from_days`, the same
//! algorithm the C++ standard library uses for `days_from_civil`. It is exact
//! for the whole range of `i64` nanoseconds (1677-09-21 through
//! 2262-04-11), which is the only range this clock can produce.

/// The length of `YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ`.
pub(crate) const LENGTH: usize = 30;

const NANOS_PER_SECOND: i64 = 1_000_000_000;
const SECONDS_PER_DAY: i64 = 86_400;

/// A formatted timestamp, held on the stack.
#[derive(Clone, Copy)]
pub(crate) struct Timestamp {
    bytes: [u8; LENGTH],
}

impl Timestamp {
    /// Format nanoseconds since the Unix epoch.
    ///
    /// Negative values are the year 1969 and earlier; the clock in
    /// `deepmsg-core` saturates rather than going negative, but the test suite
    /// checks the arithmetic around the epoch anyway.
    pub(crate) fn new(nanos: i64) -> Self {
        let seconds = nanos.div_euclid(NANOS_PER_SECOND);
        let subsecond = nanos.rem_euclid(NANOS_PER_SECOND) as u32;
        let days = seconds.div_euclid(SECONDS_PER_DAY);
        let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY) as u32;

        let (year, month, day) = civil_from_days(days);
        let year = u32::try_from(year).expect("an i64 nanosecond timestamp has a four-digit year");

        let mut bytes = [0_u8; LENGTH];
        write_digits(&mut bytes, 0, year, 4);
        bytes[4] = b'-';
        write_digits(&mut bytes, 5, month, 2);
        bytes[7] = b'-';
        write_digits(&mut bytes, 8, day, 2);
        bytes[10] = b'T';
        write_digits(&mut bytes, 11, second_of_day / 3600, 2);
        bytes[13] = b':';
        write_digits(&mut bytes, 14, (second_of_day / 60) % 60, 2);
        bytes[16] = b':';
        write_digits(&mut bytes, 17, second_of_day % 60, 2);
        bytes[19] = b'.';
        write_digits(&mut bytes, 20, subsecond, 9);
        bytes[29] = b'Z';

        Self { bytes }
    }

    pub(crate) fn as_str(&self) -> &str {
        std::str::from_utf8(&self.bytes).expect("the timestamp is ASCII")
    }
}

/// Days since 1970-01-01 to a proleptic Gregorian `(year, month, day)`.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097; // [0, 146096]
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    let year = if month <= 2 { year + 1 } else { year };

    (year, month as u32, day as u32)
}

fn write_digits(buffer: &mut [u8; LENGTH], offset: usize, value: u32, width: usize) {
    let mut value = value;
    for index in (0..width).rev() {
        buffer[offset + index] = b'0' + (value % 10) as u8;
        value /= 10;
    }
}

#[cfg(test)]
mod tests {
    use super::Timestamp;

    fn at(nanos: i64) -> String {
        Timestamp::new(nanos).as_str().to_owned()
    }

    #[test]
    fn the_epoch_is_the_epoch() {
        assert_eq!("1970-01-01T00:00:00.000000000Z", at(0));
    }

    #[test]
    fn a_known_second_and_its_nanoseconds_are_exact() {
        assert_eq!(
            "2023-11-14T22:13:20.000000000Z",
            at(1_700_000_000_000_000_000)
        );
        assert_eq!(
            "2023-11-14T22:13:20.123456789Z",
            at(1_700_000_000_123_456_789)
        );
    }

    #[test]
    fn a_leap_day_is_a_day() {
        // 2024-02-29T00:00:00Z.
        assert_eq!(
            "2024-02-29T00:00:00.000000000Z",
            at(1_709_164_800_000_000_000)
        );
    }

    #[test]
    fn the_32_bit_second_is_2038() {
        assert_eq!(
            "2038-01-19T03:14:07.000000000Z",
            at(2_147_483_647_000_000_000)
        );
    }

    #[test]
    fn the_largest_i64_nanosecond_is_2262() {
        assert_eq!("2262-04-11T23:47:16.854775807Z", at(i64::MAX));
    }

    #[test]
    fn before_the_epoch_still_formats() {
        assert_eq!("1969-12-31T23:59:59.999999999Z", at(-1));
    }
}
