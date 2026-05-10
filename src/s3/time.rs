// SPDX-License-Identifier: Apache-2.0

use time::{Month, OffsetDateTime, UtcOffset, Weekday};

pub fn http_date(timestamp: OffsetDateTime) -> String {
    let timestamp = timestamp.to_offset(UtcOffset::UTC);
    format!(
        "{}, {:02} {} {:04} {:02}:{:02}:{:02} GMT",
        weekday_name(timestamp.weekday()),
        timestamp.day(),
        month_name(timestamp.month()),
        timestamp.year(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second()
    )
}

pub fn s3_xml_timestamp(timestamp: OffsetDateTime) -> String {
    let timestamp = timestamp.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}Z",
        timestamp.year(),
        timestamp.month() as u8,
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second(),
        timestamp.millisecond()
    )
}

fn weekday_name(weekday: Weekday) -> &'static str {
    match weekday {
        Weekday::Monday => "Mon",
        Weekday::Tuesday => "Tue",
        Weekday::Wednesday => "Wed",
        Weekday::Thursday => "Thu",
        Weekday::Friday => "Fri",
        Weekday::Saturday => "Sat",
        Weekday::Sunday => "Sun",
    }
}

fn month_name(month: Month) -> &'static str {
    match month {
        Month::January => "Jan",
        Month::February => "Feb",
        Month::March => "Mar",
        Month::April => "Apr",
        Month::May => "May",
        Month::June => "Jun",
        Month::July => "Jul",
        Month::August => "Aug",
        Month::September => "Sep",
        Month::October => "Oct",
        Month::November => "Nov",
        Month::December => "Dec",
    }
}

#[cfg(test)]
mod tests {
    use super::{http_date, s3_xml_timestamp};
    use time::{Date, Month, OffsetDateTime, PrimitiveDateTime, Time, UtcOffset};

    #[test]
    fn http_date_uses_utc_http_header_format() {
        assert_eq!(
            http_date(fixed_timestamp()),
            "Sun, 10 May 2026 12:34:56 GMT"
        );
    }

    #[test]
    fn s3_xml_timestamp_uses_utc_millisecond_iso_format() {
        assert_eq!(
            s3_xml_timestamp(fixed_timestamp()),
            "2026-05-10T12:34:56.789Z"
        );
    }

    #[test]
    fn http_date_formats_all_weekday_abbreviations() {
        let dates = [
            (2024, Month::January, 1, "Mon"),
            (2024, Month::January, 2, "Tue"),
            (2024, Month::January, 3, "Wed"),
            (2024, Month::January, 4, "Thu"),
            (2024, Month::January, 5, "Fri"),
            (2024, Month::January, 6, "Sat"),
            (2024, Month::January, 7, "Sun"),
        ];

        for (year, month, day, weekday) in dates {
            let timestamp = utc_timestamp(year, month, day, 1, 2, 3, 0);

            assert_eq!(
                http_date(timestamp),
                format!("{weekday}, {day:02} Jan {year:04} 01:02:03 GMT")
            );
        }
    }

    #[test]
    fn http_date_formats_all_month_abbreviations() {
        let dates = [
            (2026, Month::January, 1, "Thu, 01 Jan 2026 00:00:00 GMT"),
            (2026, Month::February, 1, "Sun, 01 Feb 2026 00:00:00 GMT"),
            (2026, Month::March, 1, "Sun, 01 Mar 2026 00:00:00 GMT"),
            (2026, Month::April, 1, "Wed, 01 Apr 2026 00:00:00 GMT"),
            (2026, Month::May, 1, "Fri, 01 May 2026 00:00:00 GMT"),
            (2026, Month::June, 1, "Mon, 01 Jun 2026 00:00:00 GMT"),
            (2026, Month::July, 1, "Wed, 01 Jul 2026 00:00:00 GMT"),
            (2026, Month::August, 1, "Sat, 01 Aug 2026 00:00:00 GMT"),
            (2026, Month::September, 1, "Tue, 01 Sep 2026 00:00:00 GMT"),
            (2026, Month::October, 1, "Thu, 01 Oct 2026 00:00:00 GMT"),
            (2026, Month::November, 1, "Sun, 01 Nov 2026 00:00:00 GMT"),
            (2026, Month::December, 1, "Tue, 01 Dec 2026 00:00:00 GMT"),
        ];

        for (year, month, day, expected) in dates {
            let timestamp = utc_timestamp(year, month, day, 0, 0, 0, 0);

            assert_eq!(http_date(timestamp), expected);
        }
    }

    #[test]
    fn timestamps_normalize_non_utc_offsets_to_utc() {
        let timestamp = PrimitiveDateTime::new(
            Date::from_calendar_date(2026, Month::May, 10).expect("valid test date"),
            Time::from_hms_milli(14, 34, 56, 789).expect("valid test time"),
        )
        .assume_offset(UtcOffset::from_hms(2, 0, 0).expect("valid test offset"));

        assert_eq!(http_date(timestamp), "Sun, 10 May 2026 12:34:56 GMT");
        assert_eq!(s3_xml_timestamp(timestamp), "2026-05-10T12:34:56.789Z");
    }

    fn fixed_timestamp() -> OffsetDateTime {
        utc_timestamp(2026, Month::May, 10, 12, 34, 56, 789)
    }

    fn utc_timestamp(
        year: i32,
        month: Month,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        millisecond: u16,
    ) -> OffsetDateTime {
        PrimitiveDateTime::new(
            Date::from_calendar_date(year, month, day).expect("valid test date"),
            Time::from_hms_milli(hour, minute, second, millisecond).expect("valid test time"),
        )
        .assume_utc()
    }
}
