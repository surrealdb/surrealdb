//! Duration unit conversions shared by the duration lexer and the value layer.
//!
//! A SurrealQL duration literal names its unit (`1y`, `2w`, `3h`), so both the
//! side that parses one and the side that reports its components need the same
//! seconds-per-unit table. A year is a flat 365 days: durations carry no
//! calendar, so leap years never enter the conversion.

/// Seconds in a year, taken as 365 days.
pub static SECONDS_PER_YEAR: u64 = 365 * SECONDS_PER_DAY;
/// Seconds in a week.
pub static SECONDS_PER_WEEK: u64 = 7 * SECONDS_PER_DAY;
/// Seconds in a day.
pub static SECONDS_PER_DAY: u64 = 24 * SECONDS_PER_HOUR;
/// Seconds in an hour.
pub static SECONDS_PER_HOUR: u64 = 60 * SECONDS_PER_MINUTE;
/// Seconds in a minute.
pub static SECONDS_PER_MINUTE: u64 = 60;
/// Nanoseconds in a millisecond.
pub static NANOSECONDS_PER_MILLISECOND: u32 = 1000000;
/// Nanoseconds in a microsecond.
pub static NANOSECONDS_PER_MICROSECOND: u32 = 1000;
