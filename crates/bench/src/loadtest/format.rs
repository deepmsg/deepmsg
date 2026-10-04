//! The reference's number formatting, in the two shapes it uses.
//!
//! Java's `%,d` groups thousands and its `%.4f` rounds; Rust's `{}` does
//! neither. Both appear in text this port has to produce exactly — the progress
//! line and the two warnings a run can end with — so both are spelled out here
//! rather than assembled at each use.

/// `%,d`: the number with commas every three digits from the right, and a minus
/// sign in front when it is negative.
#[must_use]
pub fn grouped(value: i64) -> String {
    let digits = value.unsigned_abs().to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3 + 1);

    if value < 0 {
        grouped.push('-');
    }

    for (position, digit) in digits.chars().enumerate() {
        if position > 0 && (digits.len() - position) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }

    grouped
}

/// `%.4f`: four decimal places, rounding a half away from zero.
///
/// Rust's `{:.4}` rounds a half to even, Java's `%.4f` rounds it away from
/// zero, and the two disagree on exactly the values that sit on a half — which
/// for a loss percentage off integer counts is reachable: 3199 of 3200 lost is
/// 0.03125, and Java prints `0.0313` where Rust prints `0.0312`.
///
/// Detecting the half without losing the other cases means asking for enough
/// digits to see the whole expansion — Rust's formatted output is correctly
/// rounded at any precision, so twenty-five digits of a value under 100 are its
/// exact decimal digits and then zeros. A tie is a `5` in the fifth place with
/// nothing after it; only then is the value nudged off the half before the
/// ordinary format does the rest.
#[must_use]
pub fn four_decimals(value: f64) -> String {
    const ENOUGH_DIGITS: usize = 25;
    const PLACES: usize = 4;

    let exact = format!("{value:.ENOUGH_DIGITS$}");
    let fraction = exact.split_once('.').map_or("", |(_, fraction)| fraction);

    let on_a_half = fraction.len() > PLACES + 1
        && fraction.as_bytes()[PLACES] == b'5'
        && fraction[PLACES + 1..].bytes().all(|digit| digit == b'0');

    if on_a_half {
        let half_of_the_last_place = 0.5 / 10_f64.powi(PLACES as i32);
        let away_from_zero = if value.is_sign_negative() {
            -half_of_the_last_place
        } else {
            half_of_the_last_place
        };

        format!("{:.PLACES$}", value + away_from_zero)
    } else {
        format!("{value:.PLACES$}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thousands_are_grouped_the_way_java_groups_them() {
        assert_eq!(grouped(0), "0");
        assert_eq!(grouped(1), "1");
        assert_eq!(grouped(999), "999");
        assert_eq!(grouped(1000), "1,000");
        assert_eq!(grouped(12345), "12,345");
        assert_eq!(grouped(1_000_000), "1,000,000");
        assert_eq!(grouped(i64::MAX), "9,223,372,036,854,775,807");
        assert_eq!(grouped(-1234), "-1,234");
    }

    /// The reference's own, from `LoadTestRigTest.runWarnsAboutMissedTargetRate`:
    /// two sent of fifteen expected is 86.66666666666667 percent lost, and Java
    /// prints it as `86.6667`.
    #[test]
    fn four_decimals_matches_the_reference_on_ordinary_values() {
        assert_eq!(four_decimals(86.666_666_666_666_67), "86.6667");
        assert_eq!(four_decimals(0.0), "0.0000");
        assert_eq!(four_decimals(100.0), "100.0000");
        assert_eq!(four_decimals(1.0 / 3.0), "0.3333");
    }

    /// The case the two rounding rules disagree on, and the reason this function
    /// exists: a real loss percentage that lands exactly on a half.
    ///
    /// 3199 messages of 3200 is 99.96875 percent achieved, so 0.03125 percent
    /// lost — a dyadic value, exactly representable, and therefore an exact tie
    /// at four places. Java rounds it away from zero; `{:.4}` alone would round
    /// it to even and print 0.0312.
    #[test]
    fn four_decimals_rounds_a_half_away_from_zero() {
        let lost = 100.0 - 100.0 * 3199.0 / 3200.0;

        assert_eq!(lost, 0.031_25);
        assert_eq!(four_decimals(lost), "0.0313");
        assert_eq!(
            format!("{lost:.4}"),
            "0.0312",
            "which is what we are correcting"
        );
        assert_eq!(four_decimals(-lost), "-0.0313");
    }
}
