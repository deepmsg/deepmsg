//! Formatting of the epoch-millisecond stamps a mark file and an error log carry.
//!
//! The reference formats them with `SimpleDateFormat` / `String.format("%tF …")`
//! in the **JVM's default time zone** (`ClusterToolOperator.java:1271-1276`,
//! `CommonContext.java:1233`). This build has no time-zone database and no date
//! crate, so it formats **UTC** and `docs/compat.md` records the difference: the
//! fields a test reads (counts, ids, paths, error text) are the same, the wall
//! clock is printed in UTC rather than in the machine's local zone.
//!
//! The civil-date conversion is Howard Hinnant's `civil_from_days`, which is
//! exact for every `i64` millisecond value a mark file can hold.

/// `yyyy-MM-dd HH:mm:ss`, UTC.
#[must_use]
pub fn date_time(epoch_ms: i64) -> String {
    let (year, month, day, hour, minute, second, _) = split(epoch_ms);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

/// `HH:mm:ss`, UTC.
#[must_use]
pub fn time_of_day(epoch_ms: i64) -> String {
    let (_, _, _, hour, minute, second, _) = split(epoch_ms);
    format!("{hour:02}:{minute:02}:{second:02}")
}

/// `yyyy-MM-dd HH:mm:ss.SSSZ`, UTC — the shape
/// `CommonContext.printErrorLog` gives an entry's first/last observation.
#[must_use]
pub fn date_time_millis_zone(epoch_ms: i64) -> String {
    let (year, month, day, hour, minute, second, millis) = split(epoch_ms);
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}.{millis:03}+0000")
}

fn split(epoch_ms: i64) -> (i64, u32, u32, u32, u32, u32, u32) {
    // Floor division, so a pre-epoch stamp still lands on the right second.
    let millis_of_day = epoch_ms.rem_euclid(86_400_000);
    let days = epoch_ms.div_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    let hour = u32::try_from(millis_of_day / 3_600_000).unwrap_or(0);
    let minute = u32::try_from((millis_of_day / 60_000) % 60).unwrap_or(0);
    let second = u32::try_from((millis_of_day / 1_000) % 60).unwrap_or(0);
    let millis = u32::try_from(millis_of_day % 1_000).unwrap_or(0);
    (year, month, day, hour, minute, second, millis)
}

/// Days since 1970-01-01 to a proleptic Gregorian date (`civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let day = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let year = year + i64::from(month <= 2);
    (
        year,
        u32::try_from(month).unwrap_or(1),
        u32::try_from(day).unwrap_or(1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_epoch_is_a_thursday_in_january() {
        assert_eq!(date_time(0), "1970-01-01 00:00:00");
        assert_eq!(time_of_day(0), "00:00:00");
        assert_eq!(date_time_millis_zone(0), "1970-01-01 00:00:00.000+0000");
    }

    #[test]
    fn a_known_stamp_round_trips() {
        // 2026-10-19 12:34:56.789 UTC.
        let ms = 1_792_413_296_789;
        assert_eq!(date_time_millis_zone(ms), "2026-10-19 12:34:56.789+0000");
    }

    #[test]
    fn a_leap_day_is_the_29th() {
        // 2024-02-29 00:00:00 UTC.
        assert_eq!(date_time(1_709_164_800_000), "2024-02-29 00:00:00");
    }

    #[test]
    fn a_stamp_before_the_epoch_floors_rather_than_truncates() {
        assert_eq!(date_time(-1), "1969-12-31 23:59:59");
        assert_eq!(date_time_millis_zone(-1), "1969-12-31 23:59:59.999+0000");
    }
}
