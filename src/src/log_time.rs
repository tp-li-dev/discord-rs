use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// UTC time as `YYYY-MM-DDTHH:MM:SS.sssZ` without a date-time dependency (civil-from-days).
pub fn utc_stamp() -> String {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format_utc(elapsed)
}

fn format_utc(elapsed: Duration) -> String {
    let secs = elapsed.as_secs() as i64;
    let millis = elapsed.subsec_millis();
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{millis:03}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utc_stamp_formats_known_instants() {
        assert_eq!(format_utc(Duration::ZERO), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_utc(Duration::new(951_782_400, 7_000_000)),
            "2000-02-29T00:00:00.007Z"
        );
        assert_eq!(
            format_utc(Duration::new(1_794_812_232, 123_456_789)),
            "2026-11-16T06:57:12.123Z"
        );
        assert_eq!(
            format_utc(Duration::new(4_102_444_799, 999_999_999)),
            "2099-12-31T23:59:59.999Z"
        );
        assert_eq!(utc_stamp().len(), 24);
    }
}
