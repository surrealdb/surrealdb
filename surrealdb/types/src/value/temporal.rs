use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use anyhow::{Result, anyhow, bail};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use serde::{Deserialize, Serialize};
use surrealdb_types_derive::write_sql;

use crate::sql::{SqlFormat, ToSql};
use crate::utils::escape::QuoteStr;

/// Days from 0001-01-01 (day 1 of the common era) to 1970-01-01.
const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;
const NANOS_PER_SECOND: u64 = 1_000_000_000;
const NANOS_PER_DAY: u64 = 86_400 * NANOS_PER_SECOND;
const MICROS_PER_SECOND: i128 = 1_000_000;
const MICROS_PER_MINUTE: i128 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i128 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i128 = 24 * MICROS_PER_HOUR;
const MICROS_PER_MONTH: i128 = 30 * MICROS_PER_DAY;

/// The seconds and nanoseconds with whole seconds of the nanoseconds carried into the seconds.
fn fold_leap_second(seconds: i64, nanos: u32) -> (i64, u32) {
	let carry = u64::from(nanos) / NANOS_PER_SECOND;
	(seconds.saturating_add(carry as i64), (u64::from(nanos) % NANOS_PER_SECOND) as u32)
}

fn push_fraction(out: &mut String, nanos: u32) {
	use std::fmt::Write;
	if nanos == 0 {
		return;
	}
	let _ = if nanos.is_multiple_of(1_000_000) {
		write!(out, ".{:03}", nanos / 1_000_000)
	} else if nanos.is_multiple_of(1_000) {
		write!(out, ".{:06}", nanos / 1_000)
	} else {
		write!(out, ".{nanos:09}")
	};
}

/// Represents a calendar date without a time zone.
///
/// The value is a count of days since 1970-01-01 in the proleptic Gregorian calendar, limited
/// to the range of [`chrono::NaiveDate`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "i32", into = "i32")]
pub struct Date(i32);

impl Date {
	/// Creates a date from a count of days since 1970-01-01, or `None` if it is out of range.
	pub fn from_days(days: i32) -> Option<Self> {
		days.checked_add(UNIX_EPOCH_DAYS_FROM_CE)
			.and_then(NaiveDate::from_num_days_from_ce_opt)
			.map(|_| Self(days))
	}

	/// Creates a finite date from a count of days since 1970-01-01, or `None` if it is out of
	/// range or is the day that stands for `infinity` or `-infinity`. Client input goes through
	/// this constructor, so a written date never reads back as an infinity.
	pub fn from_finite_days(days: i32) -> Option<Self> {
		Self::from_days(days).filter(|date| !date.is_infinite())
	}

	/// The finite date of a [`chrono::NaiveDate`], or `None` for the first or last day of its
	/// range, which stand for `-infinity` and `infinity`.
	pub fn finite(date: NaiveDate) -> Option<Self> {
		Some(Self::from(date)).filter(|date| !date.is_infinite())
	}

	/// The number of days since 1970-01-01.
	pub fn days(&self) -> i32 {
		self.0
	}

	/// The earliest representable date.
	pub fn min_value() -> Self {
		Self::from(NaiveDate::MIN)
	}

	/// The latest representable date.
	pub fn max_value() -> Self {
		Self::from(NaiveDate::MAX)
	}

	/// `infinity`, the latest date. It is the greatest value of the kind, so it orders after
	/// every finite date.
	pub fn infinity() -> Self {
		Self::max_value()
	}

	/// `-infinity`, the earliest date. It is the least value of the kind.
	pub fn neg_infinity() -> Self {
		Self::min_value()
	}

	/// Whether this date is `infinity` or `-infinity`.
	pub fn is_infinite(&self) -> bool {
		*self == Self::infinity() || *self == Self::neg_infinity()
	}

	/// Converts into a [`chrono::NaiveDate`].
	pub fn into_inner(self) -> NaiveDate {
		NaiveDate::from_num_days_from_ce_opt(self.0 + UNIX_EPOCH_DAYS_FROM_CE)
			.unwrap_or(NaiveDate::MIN)
	}
}

impl TryFrom<i32> for Date {
	type Error = anyhow::Error;
	fn try_from(days: i32) -> Result<Self> {
		Self::from_days(days).ok_or_else(|| anyhow!("Date out of range: {days} days"))
	}
}

impl From<Date> for i32 {
	fn from(v: Date) -> Self {
		v.0
	}
}

impl From<NaiveDate> for Date {
	fn from(v: NaiveDate) -> Self {
		Self(v.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE)
	}
}

impl From<Date> for NaiveDate {
	fn from(v: Date) -> Self {
		v.into_inner()
	}
}

impl FromStr for Date {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self> {
		let v = s.trim();
		if let Some(positive) = parse_infinity(v) {
			return Ok(if positive {
				Self::infinity()
			} else {
				Self::neg_infinity()
			});
		}
		if v.eq_ignore_ascii_case("epoch") {
			return Ok(Self(0));
		}
		let mut chars = v.chars();
		if !matches!(chars.next(), Some('-' | '+' | '0'..='9')) {
			bail!("Invalid date: {s:?}");
		}
		let date = NaiveDate::from_str(v).map_err(|e| anyhow!("Invalid date {s:?}: {e}"))?;
		Self::finite(date).ok_or_else(|| anyhow!("Date out of range: {s:?}"))
	}
}

/// The sign of `infinity` (`Some(true)`) or `-infinity` (`Some(false)`), with an optional `+`
/// before `infinity` and any letter case, as PostgreSQL reads them. `None` for any other text.
pub(crate) fn parse_infinity(text: &str) -> Option<bool> {
	let (sign, rest) = match text.strip_prefix('-') {
		Some(rest) => (false, rest),
		None => (true, text.strip_prefix('+').unwrap_or(text)),
	};
	rest.eq_ignore_ascii_case("infinity").then_some(sign)
}

impl Display for Date {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if *self == Self::infinity() {
			f.write_str("infinity")
		} else if *self == Self::neg_infinity() {
			f.write_str("-infinity")
		} else {
			Display::fmt(&self.into_inner(), f)
		}
	}
}

impl ToSql for Date {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use crate as surrealdb_types;
		write_sql!(f, fmt, "pd{}", QuoteStr(&self.to_string()));
	}
}

/// Represents a time of day without a date or time zone.
///
/// The value is a count of nanoseconds since midnight, from `00:00:00` to `24:00:00` inclusive.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(try_from = "u64", into = "u64")]
pub struct Time(u64);

impl Time {
	/// The latest time of day, `24:00:00`.
	pub const MAX: Time = Time(NANOS_PER_DAY);
	/// Midnight, `00:00:00`.
	pub const MIN: Time = Time(0);

	/// Creates a time from nanoseconds since midnight, or `None` if it is past `24:00:00`.
	pub fn from_nanos(nanos: u64) -> Option<Self> {
		(nanos <= NANOS_PER_DAY).then_some(Self(nanos))
	}

	/// The number of nanoseconds since midnight.
	pub fn nanos(&self) -> u64 {
		self.0
	}

	/// Converts into a [`chrono::NaiveTime`], or `None` for `24:00:00`.
	pub fn into_inner(self) -> Option<NaiveTime> {
		let secs = u32::try_from(self.0 / NANOS_PER_SECOND).ok()?;
		let nanos = (self.0 % NANOS_PER_SECOND) as u32;
		NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
	}
}

impl TryFrom<u64> for Time {
	type Error = anyhow::Error;
	fn try_from(nanos: u64) -> Result<Self> {
		Self::from_nanos(nanos).ok_or_else(|| anyhow!("Time out of range: {nanos} nanoseconds"))
	}
}

impl From<Time> for u64 {
	fn from(v: Time) -> Self {
		v.0
	}
}

impl From<NaiveTime> for Time {
	fn from(v: NaiveTime) -> Self {
		let nanos =
			u64::from(v.num_seconds_from_midnight()) * NANOS_PER_SECOND + u64::from(v.nanosecond());
		Self(nanos.min(NANOS_PER_DAY))
	}
}

/// Parses `H:MM[:SS[.fraction]]` into nanoseconds since midnight.
fn parse_time_nanos(s: &str) -> Result<u64> {
	let invalid = || anyhow!("Invalid time: {s:?}");
	let digits = |p: &str, max_len: usize| -> Result<u64> {
		if p.is_empty() || p.len() > max_len || !p.bytes().all(|b| b.is_ascii_digit()) {
			return Err(invalid());
		}
		p.parse::<u64>().map_err(|_| invalid())
	};
	let mut parts = s.trim().split(':');
	let hours = digits(parts.next().ok_or_else(invalid)?, 2)?;
	let mins = digits(parts.next().ok_or_else(invalid)?, 2)?;
	let (secs, frac) = match parts.next() {
		None => (0, 0),
		Some(p) => {
			let (whole, frac) = match p.split_once('.') {
				Some((w, f)) => (w, Some(f)),
				None => (p, None),
			};
			let secs = digits(whole, 2)?;
			let frac = match frac {
				None => 0,
				Some(f) => {
					let n = digits(f, 9)?;
					n * 10u64.pow(9 - f.len() as u32)
				}
			};
			(secs, frac)
		}
	};
	if parts.next().is_some() || hours > 24 || mins > 59 || secs > 59 {
		return Err(invalid());
	}
	let nanos = hours * 3600 * NANOS_PER_SECOND
		+ mins * 60 * NANOS_PER_SECOND
		+ secs * NANOS_PER_SECOND
		+ frac;
	if nanos > NANOS_PER_DAY {
		return Err(invalid());
	}
	Ok(nanos)
}

impl FromStr for Time {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self> {
		parse_time_nanos(s).map(Self)
	}
}

impl Display for Time {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let secs = self.0 / NANOS_PER_SECOND;
		let mut out = format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
		push_fraction(&mut out, (self.0 % NANOS_PER_SECOND) as u32);
		f.write_str(&out)
	}
}

impl ToSql for Time {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use crate as surrealdb_types;
		write_sql!(f, fmt, "pt{}", QuoteStr(&self.to_string()));
	}
}

/// Represents a date and time of day without a time zone.
///
/// The value is a wall-clock time with nanosecond resolution, in the range of
/// [`chrono::NaiveDateTime`]. It orders by `(seconds, nanoseconds)` since
/// 1970-01-01T00:00:00.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Timestamp(NaiveDateTime);

impl Timestamp {
	/// Creates a timestamp from seconds and nanoseconds since 1970-01-01T00:00:00, or `None`
	/// if it is out of range. Nanoseconds of one second or more carry into the seconds, so a
	/// leap second is the first instant of the next second.
	pub fn from_parts(seconds: i64, nanos: u32) -> Option<Self> {
		let (seconds, nanos) = fold_leap_second(seconds, nanos);
		DateTime::from_timestamp(seconds, nanos).map(|d| Self(d.naive_utc()))
	}

	/// Creates a finite timestamp from seconds and nanoseconds since 1970-01-01T00:00:00, or
	/// `None` if it is out of range or is the instant that stands for `infinity` or `-infinity`.
	/// Client input goes through this constructor, so a written timestamp never reads back as an
	/// infinity.
	pub fn from_finite_parts(seconds: i64, nanos: u32) -> Option<Self> {
		Self::from_parts(seconds, nanos).filter(|timestamp| !timestamp.is_infinite())
	}

	/// The finite timestamp of a [`chrono::NaiveDateTime`], or `None` for the first or last
	/// instant of its range, which stand for `-infinity` and `infinity`.
	pub fn finite(at: NaiveDateTime) -> Option<Self> {
		Some(Self(at)).filter(|timestamp| !timestamp.is_infinite())
	}

	/// `infinity`, the latest timestamp. It is the greatest value of the kind, so it orders
	/// after every finite timestamp.
	pub fn infinity() -> Self {
		Self(NaiveDateTime::MAX)
	}

	/// `-infinity`, the earliest timestamp. It is the least value of the kind.
	pub fn neg_infinity() -> Self {
		Self(NaiveDateTime::MIN)
	}

	/// Whether this timestamp is `infinity` or `-infinity`.
	pub fn is_infinite(&self) -> bool {
		*self == Self::infinity() || *self == Self::neg_infinity()
	}

	/// The whole seconds since 1970-01-01T00:00:00.
	pub fn seconds(&self) -> i64 {
		self.0.and_utc().timestamp()
	}

	/// The nanoseconds within the second. A leap second reports a value of one billion or more.
	pub fn nanos(&self) -> u32 {
		self.0.and_utc().timestamp_subsec_nanos()
	}

	/// Converts into a [`chrono::NaiveDateTime`].
	pub fn into_inner(self) -> NaiveDateTime {
		self.0
	}
}

impl From<NaiveDateTime> for Timestamp {
	fn from(v: NaiveDateTime) -> Self {
		Self(v)
	}
}

impl From<Timestamp> for NaiveDateTime {
	fn from(v: Timestamp) -> Self {
		v.0
	}
}

impl FromStr for Timestamp {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self> {
		let v = s.trim();
		if let Some(positive) = parse_infinity(v) {
			return Ok(if positive {
				Self::infinity()
			} else {
				Self::neg_infinity()
			});
		}
		if v.eq_ignore_ascii_case("epoch") {
			return Ok(Self(DateTime::<chrono::Utc>::UNIX_EPOCH.naive_utc()));
		}
		let (date, time) =
			v.split_once(['T', ' ']).ok_or_else(|| anyhow!("Invalid timestamp: {s:?}"))?;
		if !matches!(date.chars().next(), Some('-' | '+' | '0'..='9')) {
			bail!("Invalid timestamp: {s:?}");
		}
		let date = NaiveDate::from_str(date).map_err(|_| anyhow!("Invalid timestamp: {s:?}"))?;
		let nanos = parse_time_nanos(time).map_err(|_| anyhow!("Invalid timestamp: {s:?}"))?;
		if nanos >= NANOS_PER_DAY {
			bail!("Invalid timestamp: {s:?}");
		}
		let time = Time(nanos).into_inner().ok_or_else(|| anyhow!("Invalid timestamp: {s:?}"))?;
		Self::finite(date.and_time(time)).ok_or_else(|| anyhow!("Timestamp out of range: {s:?}"))
	}
}

impl Display for Timestamp {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		if *self == Self::infinity() {
			return f.write_str("infinity");
		}
		if *self == Self::neg_infinity() {
			return f.write_str("-infinity");
		}
		// A leap second held in the wall time is written as the first instant of the next
		// second, the form `from_parts` reads it as.
		let (seconds, nanos) = fold_leap_second(self.seconds(), self.nanos());
		let at = DateTime::from_timestamp(seconds, nanos).map_or(self.0, |d| d.naive_utc());
		let mut out = at.format("%Y-%m-%dT%H:%M:%S").to_string();
		push_fraction(&mut out, at.nanosecond());
		f.write_str(&out)
	}
}

impl ToSql for Timestamp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use crate as surrealdb_types;
		write_sql!(f, fmt, "ts{}", QuoteStr(&self.to_string()));
	}
}

/// Represents a calendar-aware interval of months, days, and microseconds.
///
/// Equality, ordering, and hashing use the normalised total in which a month is 30 days and a
/// day is 24 hours, so `1 month` equals `30 days`. [`Interval::identical`] compares the
/// components as written.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
pub struct Interval {
	months: i32,
	days: i32,
	micros: i64,
}

impl Interval {
	/// The zero interval.
	pub const ZERO: Interval = Interval {
		months: 0,
		days: 0,
		micros: 0,
	};

	/// Creates an interval from its components.
	pub const fn new(months: i32, days: i32, micros: i64) -> Self {
		Self {
			months,
			days,
			micros,
		}
	}

	/// The months component.
	pub fn months(&self) -> i32 {
		self.months
	}

	/// The days component.
	pub fn days(&self) -> i32 {
		self.days
	}

	/// The microseconds component.
	pub fn micros(&self) -> i64 {
		self.micros
	}

	/// The total in microseconds in which a month is 30 days and a day is 24 hours.
	pub fn normalised_micros(&self) -> i128 {
		i128::from(self.months) * MICROS_PER_MONTH
			+ i128::from(self.days) * MICROS_PER_DAY
			+ i128::from(self.micros)
	}

	/// Whether both intervals carry the same components, which is stricter than equality.
	pub fn identical(&self, other: &Self) -> bool {
		self.months == other.months && self.days == other.days && self.micros == other.micros
	}
}

impl PartialEq for Interval {
	fn eq(&self, other: &Self) -> bool {
		self.normalised_micros() == other.normalised_micros()
	}
}

impl Eq for Interval {}

impl PartialOrd for Interval {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Interval {
	fn cmp(&self, other: &Self) -> Ordering {
		self.normalised_micros().cmp(&other.normalised_micros())
	}
}

impl Hash for Interval {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.normalised_micros().hash(state);
	}
}

/// A decimal number with an exact fractional part.
struct Decimal {
	int: i128,
	frac_num: i128,
	frac_den: i128,
}

impl Decimal {
	fn parse(s: &str) -> Result<Self> {
		let invalid = || anyhow!("Invalid interval number: {s:?}");
		let (neg, rest) = match s.as_bytes().first() {
			Some(b'-') => (true, &s[1..]),
			Some(b'+') => (false, &s[1..]),
			_ => (false, s),
		};
		let (whole, frac) = match rest.split_once('.') {
			Some((w, f)) => (w, f),
			None => (rest, ""),
		};
		if (whole.is_empty() && frac.is_empty())
			|| frac.len() > 9
			|| !whole.bytes().all(|b| b.is_ascii_digit())
			|| !frac.bytes().all(|b| b.is_ascii_digit())
			|| whole.len() > 20
		{
			return Err(invalid());
		}
		let int = if whole.is_empty() {
			0
		} else {
			whole.parse::<i128>().map_err(|_| invalid())?
		};
		let frac_num = if frac.is_empty() {
			0
		} else {
			frac.parse::<i128>().map_err(|_| invalid())?
		};
		let frac_den = 10i128.pow(frac.len() as u32);
		let sign = if neg {
			-1
		} else {
			1
		};
		Ok(Self {
			int: int * sign,
			frac_num: frac_num * sign,
			frac_den,
		})
	}

	/// Multiplies by a unit: the whole part, truncated toward zero, and the remainder as a
	/// numerator over `frac_den`, of the same sign.
	fn split(&self, unit: i128) -> Result<(i128, i128)> {
		let out = || anyhow!("Interval value out of range");
		let frac = self.frac_num.checked_mul(unit).ok_or_else(out)?;
		let whole = self
			.int
			.checked_mul(unit)
			.and_then(|v| v.checked_add(frac / self.frac_den))
			.ok_or_else(out)?;
		Ok((whole, frac % self.frac_den))
	}
}

/// `numerator / denominator` rounded half away from zero.
fn round_div(numerator: i128, denominator: i128) -> i128 {
	let half = denominator / 2;
	if numerator < 0 {
		-((-numerator + half) / denominator)
	} else {
		(numerator + half) / denominator
	}
}

#[derive(Default)]
struct IntervalParts {
	months: i128,
	days: i128,
	micros: i128,
}

impl IntervalParts {
	fn add_months(&mut self, n: i128) -> Result<()> {
		self.months = self.months.checked_add(n).ok_or_else(|| anyhow!("Interval out of range"))?;
		Ok(())
	}

	fn add_days(&mut self, n: i128) -> Result<()> {
		self.days = self.days.checked_add(n).ok_or_else(|| anyhow!("Interval out of range"))?;
		Ok(())
	}

	fn add_micros(&mut self, n: i128) -> Result<()> {
		self.micros = self.micros.checked_add(n).ok_or_else(|| anyhow!("Interval out of range"))?;
		Ok(())
	}

	/// `num` units of `unit` months, as PostgreSQL reads a year or longer: rounded to whole months.
	fn add_years(&mut self, num: &Decimal, unit: i128) -> Result<()> {
		let (whole, rem) = num.split(unit)?;
		self.add_months(whole + round_div(rem, num.frac_den))
	}

	/// `num` months: the fraction of a month carried into days, a month being 30 days.
	fn add_fractional_months(&mut self, num: &Decimal) -> Result<()> {
		let (whole, rem) = num.split(1)?;
		self.add_months(whole)?;
		self.add_fraction_of_days(rem * 30, num.frac_den)
	}

	/// `num` units of `unit` days: the fraction of a day carried into the time.
	fn add_fractional_days(&mut self, num: &Decimal, unit: i128) -> Result<()> {
		let (whole, rem) = num.split(unit)?;
		self.add_days(whole)?;
		self.add_micros(round_div(rem * MICROS_PER_DAY, num.frac_den))
	}

	/// `num` units of `unit` micros, rounded to the microsecond.
	fn add_fractional_micros(&mut self, num: &Decimal, unit: i128) -> Result<()> {
		let (whole, rem) = num.split(unit)?;
		self.add_micros(whole)?;
		self.add_micros(round_div(rem, num.frac_den))
	}

	/// `numerator / denominator` days: the whole days, then the rest into the time.
	fn add_fraction_of_days(&mut self, numerator: i128, denominator: i128) -> Result<()> {
		self.add_days(numerator / denominator)?;
		self.add_micros(round_div(numerator % denominator * MICROS_PER_DAY, denominator))
	}

	fn finish(self) -> Result<Interval> {
		let out = || anyhow!("Interval out of range");
		Ok(Interval::new(
			i32::try_from(self.months).map_err(|_| out())?,
			i32::try_from(self.days).map_err(|_| out())?,
			i64::try_from(self.micros).map_err(|_| out())?,
		))
	}
}

fn parse_iso_interval(s: &str) -> Result<Interval> {
	let invalid = || anyhow!("Invalid interval: {s:?}");
	let body = s.get(1..).ok_or_else(invalid)?;
	if let Some(interval) = parse_iso_alternative(body) {
		return interval;
	}
	let mut parts = IntervalParts::default();
	let mut in_time = false;
	let mut seen = false;
	let mut rest = body;
	while !rest.is_empty() {
		if let Some(r) = rest.strip_prefix(['T', 't']) {
			if in_time {
				return Err(invalid());
			}
			in_time = true;
			rest = r;
			continue;
		}
		let end =
			rest.find(|c: char| c.is_ascii_alphabetic()).filter(|&i| i > 0).ok_or_else(invalid)?;
		let num = Decimal::parse(&rest[..end])?;
		let unit = rest[end..].chars().next().ok_or_else(invalid)?.to_ascii_uppercase();
		rest = &rest[end + 1..];
		match (in_time, unit) {
			(false, 'Y') => parts.add_years(&num, 12)?,
			(false, 'M') => parts.add_fractional_months(&num)?,
			(false, 'W') => parts.add_fractional_days(&num, 7)?,
			(false, 'D') => parts.add_fractional_days(&num, 1)?,
			(true, 'H') => parts.add_fractional_micros(&num, MICROS_PER_HOUR)?,
			(true, 'M') => parts.add_fractional_micros(&num, MICROS_PER_MINUTE)?,
			(true, 'S') => parts.add_fractional_micros(&num, MICROS_PER_SECOND)?,
			_ => return Err(invalid()),
		}
		seen = true;
	}
	if !seen {
		return Err(invalid());
	}
	parts.finish()
}

/// The ISO 8601 alternative form after its `P`: `YYYY-MM-DD`, `THH:MM:SS[.f]`, or both, as
/// `P0001-02-03T04:05:06`. `None` when `body` is not in that form.
fn parse_iso_alternative(body: &str) -> Option<Result<Interval>> {
	let (date, time) = match body.split_once(['T', 't']) {
		Some((date, time)) => (date, Some(time)),
		None => (body, None),
	};
	let digits = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
	let ymd: Vec<&str> = date.split('-').collect();
	let date_ok = date.is_empty() || (ymd.len() == 3 && ymd.iter().all(|p| digits(p)));
	let time_ok = time.is_none_or(|t| t.contains(':'));
	if !date_ok || !time_ok || (date.is_empty() && time.is_none()) {
		return None;
	}
	let mut text = match ymd.as_slice() {
		[y, m, d] => format!("{y} years {m} months {d} days"),
		_ => String::new(),
	};
	if let Some(t) = time {
		text.push(' ');
		text.push_str(t);
	}
	Some(parse_postgres_interval(text.trim()))
}

/// A SQL-standard year-month field, `[+-]Y-M`, as signed months.
fn year_month(token: &str) -> Option<i128> {
	let (negative, unsigned) = match token.as_bytes().first() {
		Some(b'-') => (true, &token[1..]),
		Some(b'+') => (false, &token[1..]),
		_ => (false, token),
	};
	let (years, months) = unsigned.split_once('-')?;
	let digits = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
	if !digits(years) || !digits(months) {
		return None;
	}
	let total =
		years.parse::<i128>().ok()?.checked_mul(12)?.checked_add(months.parse::<i128>().ok()?)?;
	Some(if negative {
		-total
	} else {
		total
	})
}

fn parse_postgres_interval(s: &str) -> Result<Interval> {
	let invalid = || anyhow!("Invalid interval: {s:?}");
	let mut parts = IntervalParts::default();
	let mut tokens = s.split_whitespace().peekable();
	let mut ago = false;
	let mut seen = false;
	while let Some(token) = tokens.next() {
		if token.eq_ignore_ascii_case("ago") {
			if tokens.peek().is_some() || !seen {
				return Err(invalid());
			}
			ago = true;
			break;
		}
		if token.contains(':') {
			let (neg, clock) = match token.as_bytes().first() {
				Some(b'-') => (true, &token[1..]),
				Some(b'+') => (false, &token[1..]),
				_ => (false, token),
			};
			let (clock, frac) = match clock.split_once('.') {
				Some((c, f)) => (c, Some(f)),
				None => (clock, None),
			};
			let mut segs = clock.split(':');
			let mut next = |max_len: usize| -> Result<i128> {
				let p = segs.next().ok_or_else(invalid)?;
				if p.is_empty() || p.len() > max_len || !p.bytes().all(|b| b.is_ascii_digit()) {
					return Err(invalid());
				}
				p.parse::<i128>().map_err(|_| invalid())
			};
			let hours = next(10)?;
			let mins = next(2)?;
			let secs = if clock.matches(':').count() == 2 {
				next(2)?
			} else {
				0
			};
			if segs.next().is_some() || mins > 59 || secs > 59 {
				return Err(invalid());
			}
			let mut micros =
				hours * MICROS_PER_HOUR + mins * MICROS_PER_MINUTE + secs * MICROS_PER_SECOND;
			if let Some(f) = frac {
				let n = Decimal::parse(&format!("0.{f}"))?;
				let (whole, rem) = n.split(MICROS_PER_SECOND)?;
				micros += whole + round_div(rem, n.frac_den);
			}
			parts.add_micros(if neg {
				-micros
			} else {
				micros
			})?;
		} else if let Some(months) = year_month(token) {
			// The SQL-standard year-month field, as `1-2`.
			parts.add_months(months)?;
		} else {
			let num = Decimal::parse(token)?;
			// A bare number before a time of day is days (`3 4:05:06`), and a bare number alone
			// is seconds, as PostgreSQL reads the SQL-standard form.
			match tokens.peek() {
				None => {
					parts.add_fractional_micros(&num, MICROS_PER_SECOND)?;
					seen = true;
					continue;
				}
				Some(next) if next.contains(':') => {
					parts.add_fractional_days(&num, 1)?;
					seen = true;
					continue;
				}
				Some(_) => {}
			}
			let unit = tokens.next().ok_or_else(invalid)?.to_ascii_lowercase();
			match unit.as_str() {
				"millennium" | "millennia" | "millenniums" => parts.add_years(&num, 12_000)?,
				"century" | "centuries" => parts.add_years(&num, 1_200)?,
				"decade" | "decades" | "dec" | "decs" => parts.add_years(&num, 120)?,
				"year" | "years" | "yr" | "yrs" | "y" => parts.add_years(&num, 12)?,
				"month" | "months" | "mon" | "mons" => parts.add_fractional_months(&num)?,
				"week" | "weeks" | "w" => parts.add_fractional_days(&num, 7)?,
				"day" | "days" | "d" => parts.add_fractional_days(&num, 1)?,
				"hour" | "hours" | "hr" | "hrs" | "h" => {
					parts.add_fractional_micros(&num, MICROS_PER_HOUR)?
				}
				"minute" | "minutes" | "min" | "mins" | "m" => {
					parts.add_fractional_micros(&num, MICROS_PER_MINUTE)?
				}
				"second" | "seconds" | "sec" | "secs" | "s" => {
					parts.add_fractional_micros(&num, MICROS_PER_SECOND)?
				}
				"millisecond" | "milliseconds" | "msec" | "msecs" | "ms" => {
					parts.add_fractional_micros(&num, 1_000)?
				}
				"microsecond" | "microseconds" | "usec" | "usecs" | "us" => {
					parts.add_fractional_micros(&num, 1)?
				}
				_ => return Err(invalid()),
			}
		}
		seen = true;
	}
	if !seen {
		return Err(invalid());
	}
	if ago {
		parts.months = -parts.months;
		parts.days = -parts.days;
		parts.micros = -parts.micros;
	}
	parts.finish()
}

impl FromStr for Interval {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self> {
		let v = s.trim();
		if v.starts_with(['P', 'p']) {
			parse_iso_interval(v)
		} else {
			parse_postgres_interval(v)
		}
	}
}

impl Display for Interval {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		use std::fmt::Write;
		let mut out = String::from("P");
		let (years, mons) = (self.months / 12, self.months % 12);
		if years != 0 {
			let _ = write!(out, "{years}Y");
		}
		if mons != 0 {
			let _ = write!(out, "{mons}M");
		}
		if self.days != 0 {
			let _ = write!(out, "{}D", self.days);
		}
		if self.micros != 0 {
			out.push('T');
			let sign = if self.micros < 0 {
				"-"
			} else {
				""
			};
			let abs = self.micros.unsigned_abs();
			let hours = abs / 3_600_000_000;
			let mins = abs % 3_600_000_000 / 60_000_000;
			let secs = abs % 60_000_000 / 1_000_000;
			let frac = abs % 1_000_000;
			if hours != 0 {
				let _ = write!(out, "{sign}{hours}H");
			}
			if mins != 0 {
				let _ = write!(out, "{sign}{mins}M");
			}
			if secs != 0 || frac != 0 {
				let _ = write!(out, "{sign}{secs}");
				if frac != 0 {
					let digits = format!("{frac:06}");
					let _ = write!(out, ".{}", digits.trim_end_matches('0'));
				}
				out.push('S');
			}
		}
		if out == "P" {
			out.push_str("T0S");
		}
		f.write_str(&out)
	}
}

impl ToSql for Interval {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use crate as surrealdb_types;
		write_sql!(f, fmt, "ivl{}", QuoteStr(&self.to_string()));
	}
}

#[cfg(feature = "arbitrary")]
mod arb {
	use arbitrary::{Arbitrary, Unstructured};

	use super::*;

	impl<'a> Arbitrary<'a> for Date {
		fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
			Ok(Self::from(u.arbitrary::<NaiveDate>()?))
		}
	}

	impl<'a> Arbitrary<'a> for Time {
		fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
			Ok(Self(u.int_in_range(0..=NANOS_PER_DAY)?))
		}
	}

	impl<'a> Arbitrary<'a> for Timestamp {
		fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
			let date = u.arbitrary::<NaiveDate>()?;
			let nanos = u.int_in_range(0..=NANOS_PER_DAY - 1)?;
			let time = Time(nanos).into_inner().ok_or(arbitrary::Error::IncorrectFormat)?;
			Ok(Self(date.and_time(time)))
		}
	}

	impl<'a> Arbitrary<'a> for Interval {
		fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
			Ok(Self::new(u.arbitrary()?, u.arbitrary()?, u.arbitrary()?))
		}
	}
}

#[cfg(test)]
mod tests {
	use std::collections::hash_map::DefaultHasher;

	use super::super::datetime::Datetime;
	use super::*;

	fn hash_of<T: Hash>(v: &T) -> u64 {
		let mut h = DefaultHasher::new();
		v.hash(&mut h);
		h.finish()
	}

	#[test]
	fn infinity_is_the_greatest_and_least_value_of_each_kind() {
		let finite = Date::from_days(0).unwrap();
		assert!(Date::infinity() > finite && finite > Date::neg_infinity());
		assert!(Date::infinity().is_infinite() && Date::neg_infinity().is_infinite());
		assert!(!finite.is_infinite());
		let max_day = Date::max_value();
		assert!(max_day.is_infinite(), "the last representable day is the infinity itself");

		let finite = Timestamp::from_parts(0, 0).unwrap();
		assert!(Timestamp::infinity() > finite && finite > Timestamp::neg_infinity());
		assert!(Timestamp::infinity().is_infinite() && Timestamp::neg_infinity().is_infinite());
		assert!(!finite.is_infinite());

		assert!(Datetime::MAX_UTC.is_infinite() && Datetime::MIN_UTC.is_infinite());
		assert!(!Datetime::from_timestamp(0, 0).unwrap().is_infinite());
	}

	#[test]
	fn a_finite_value_never_reads_as_an_infinity() {
		let last = NaiveDate::MAX.to_string();
		let first = NaiveDate::MIN.to_string();
		assert!(last.parse::<Date>().is_err(), "{last}");
		assert!(first.parse::<Date>().is_err(), "{first}");
		assert!(Date::finite(NaiveDate::MAX).is_none());
		assert!(Date::from_finite_days(Date::infinity().days()).is_none());
		assert!(Date::from_finite_days(Date::neg_infinity().days()).is_none());
		let near = Date::from_finite_days(Date::infinity().days() - 1).unwrap();
		assert!(!near.is_infinite());
		assert_eq!(near.to_string().parse::<Date>().unwrap(), near);
		assert!(format!("{last}T23:59:59.999999999").parse::<Timestamp>().is_err());
		assert!(format!("{first}T00:00:00").parse::<Timestamp>().is_err());
		let max = Timestamp::infinity();
		assert!(Timestamp::from_finite_parts(max.seconds(), max.nanos()).is_none());
		assert!(Timestamp::finite(NaiveDateTime::MIN).is_none());
		assert!(Timestamp::from_finite_parts(max.seconds(), 0).is_some());
		// The raw constructors still decode a stored infinity.
		assert_eq!(Date::from_days(Date::infinity().days()), Some(Date::infinity()));
		assert_eq!(Timestamp::from_parts(max.seconds(), max.nanos()), Some(max));
	}

	#[test]
	fn infinity_text_round_trips_through_display_and_parse() {
		assert_eq!(Date::infinity().to_string(), "infinity");
		assert_eq!(Date::neg_infinity().to_string(), "-infinity");
		assert_eq!(Timestamp::infinity().to_string(), "infinity");
		assert_eq!(Timestamp::neg_infinity().to_string(), "-infinity");
		assert_eq!("infinity".parse::<Date>().unwrap(), Date::infinity());
		assert_eq!("+InFiNiTy".parse::<Date>().unwrap(), Date::infinity());
		assert_eq!("-infinity".parse::<Date>().unwrap(), Date::neg_infinity());
		assert_eq!("-Infinity".parse::<Timestamp>().unwrap(), Timestamp::neg_infinity());
		assert_eq!("infinity".parse::<Datetime>().unwrap(), Datetime::MAX_UTC);
		assert_eq!("-infinity".parse::<Datetime>().unwrap(), Datetime::MIN_UTC);
		assert!("infinite".parse::<Date>().is_err());
		assert!("--infinity".parse::<Date>().is_err());
	}

	#[test]
	fn epoch_parses_as_the_unix_epoch() {
		assert_eq!("epoch".parse::<Date>().unwrap().to_string(), "1970-01-01");
		assert_eq!("EPOCH".parse::<Timestamp>().unwrap(), Timestamp::from_parts(0, 0).unwrap());
		assert_eq!("epoch".parse::<Datetime>().unwrap(), Datetime::from_timestamp(0, 0).unwrap());
	}

	#[test]
	fn date_round_trips_text_and_range() {
		for s in ["1970-01-01", "2026-10-27", "0000-01-01", "-0001-12-31", "+10000-01-01"] {
			let d: Date = s.parse().unwrap();
			assert_eq!(d.to_string(), s);
		}
		for d in [Date::min_value(), Date::max_value()] {
			assert_eq!(d.to_string().parse::<Date>().unwrap(), d);
			assert_eq!(Date::from_days(d.days()), Some(d));
		}
		assert!(Date::from_days(i32::MAX).is_none());
		assert!("2026-02-30".parse::<Date>().is_err());
		assert!("not a date".parse::<Date>().is_err());
		assert_eq!(Date::from_days(0).unwrap().to_string(), "1970-01-01");
		assert!(Date::from_days(-1).unwrap() < Date::from_days(0).unwrap());
	}

	#[test]
	fn time_round_trips_text_and_range() {
		for s in [
			"00:00:00",
			"12:30:00",
			"12:30:00.123",
			"12:30:00.123456",
			"12:30:00.123456789",
			"24:00:00",
		] {
			let t: Time = s.parse().unwrap();
			assert_eq!(t.to_string(), s);
		}
		assert_eq!("12:30".parse::<Time>().unwrap().to_string(), "12:30:00");
		assert_eq!("00:00:00.5".parse::<Time>().unwrap().nanos(), 500_000_000);
		assert!("24:00:01".parse::<Time>().is_err());
		assert!("24:00:00.1".parse::<Time>().is_err());
		assert!("12:60:00".parse::<Time>().is_err());
		assert!("12:00:60".parse::<Time>().is_err());
		assert!("12".parse::<Time>().is_err());
		assert!(Time::from_nanos(NANOS_PER_DAY + 1).is_none());
		assert!(Time::MAX.into_inner().is_none());
		assert_eq!(
			Time::from(NaiveTime::from_hms_nano_opt(1, 2, 3, 4).unwrap()).into_inner(),
			NaiveTime::from_hms_nano_opt(1, 2, 3, 4)
		);
	}

	#[test]
	fn timestamp_round_trips_text() {
		for s in ["1970-01-01T00:00:00", "2026-10-27T12:30:00.5", "-0001-01-01T00:00:00.000000001"]
		{
			let t: Timestamp = s.parse().unwrap();
			let again = t.to_string();
			assert_eq!(again.parse::<Timestamp>().unwrap(), t);
		}
		assert_eq!(
			"2026-10-27 12:30:00".parse::<Timestamp>().unwrap(),
			"2026-10-27T12:30:00".parse::<Timestamp>().unwrap()
		);
		assert_eq!(
			"2026-10-27T12:30:00.5".parse::<Timestamp>().unwrap().to_string(),
			"2026-10-27T12:30:00.500"
		);
		assert!("2026-10-27".parse::<Timestamp>().is_err());
		assert!("2026-10-27T24:00:00".parse::<Timestamp>().is_err());
		let t = Timestamp::from_parts(-1, 5).unwrap();
		assert_eq!((t.seconds(), t.nanos()), (-1, 5));
		assert!(Timestamp::from_parts(-1, 5).unwrap() < Timestamp::from_parts(0, 0).unwrap());
	}

	#[test]
	fn a_leap_second_is_the_first_instant_of_the_next_second() {
		let leap = Timestamp::from_parts(59, 1_500_000_000).unwrap();
		assert_eq!(leap, Timestamp::from_parts(60, 500_000_000).unwrap());
		assert_eq!(leap.to_string(), "1970-01-01T00:01:00.500");
		assert_eq!(leap.to_string().parse::<Timestamp>().unwrap(), leap);
		let max = Timestamp::infinity();
		assert!(Timestamp::from_parts(max.seconds(), 1_500_000_000).is_none());
	}

	#[test]
	fn a_timestamp_on_the_first_or_last_day_reads_back() {
		let last = NaiveDate::MAX.to_string();
		let text = format!("{last}T12:00:00");
		let t: Timestamp = text.parse().unwrap();
		assert!(!t.is_infinite());
		assert_eq!(t.to_string().parse::<Timestamp>().unwrap(), t);
		assert!("infinity 12:00:00".parse::<Timestamp>().is_err());
		assert!("epoch 10:00:00".parse::<Timestamp>().is_err());
	}

	#[test]
	fn interval_text_forms() {
		let cases = [
			("P1Y2M3DT4H5M6.5S", Interval::new(14, 3, 14_706_500_000)),
			("P-1Y2M-3DT4H", Interval::new(-10, -3, 14_400_000_000)),
			("PT0S", Interval::ZERO),
			("PT-1H-30M", Interval::new(0, 0, -5_400_000_000)),
			("P1W", Interval::new(0, 7, 0)),
		];
		for (s, v) in cases {
			let parsed: Interval = s.parse().unwrap();
			assert!(parsed.identical(&v), "{s}");
		}
		let v: Interval = "1 year 2 mons 3 days 04:05:06.5".parse().unwrap();
		assert!(v.identical(&Interval::new(14, 3, 14_706_500_000)));
		let v: Interval = "-1 day -02:00:00".parse().unwrap();
		assert!(v.identical(&Interval::new(0, -1, -7_200_000_000)));
		let v: Interval = "1 day ago".parse().unwrap();
		assert!(v.identical(&Interval::new(0, -1, 0)));
		let v = Interval::new(14, 3, 14_706_500_000);
		assert_eq!(v.to_string(), "P1Y2M3DT4H5M6.5S");
		assert!(v.to_string().parse::<Interval>().unwrap().identical(&v));
		for v in [
			Interval::new(i32::MIN, i32::MIN, i64::MIN),
			Interval::new(i32::MAX, i32::MAX, i64::MAX),
			Interval::new(-13, 0, -1),
		] {
			assert!(v.to_string().parse::<Interval>().unwrap().identical(&v), "{v}");
		}
		// A fraction of a unit is carried into the next: a fraction of a year rounds to whole
		// months, a month is 30 days, and the time rounds to the microsecond.
		let hours = |n: i64| n * 3_600_000_000;
		for (text, months, days, micros) in [
			("P1.5D", 0, 1, hours(12)),
			("1.5 days", 0, 1, hours(12)),
			("-1.5 days", 0, -1, -hours(12)),
			("1.5 weeks", 0, 10, hours(12)),
			("1.5 months", 1, 15, 0),
			("0.1 months", 0, 3, 0),
			("1.5 years", 18, 0, 0),
			("1.01 years", 12, 0, 0),
			("PT0.0000005S", 0, 0, 1),
			("PT0.0000001S", 0, 0, 0),
			("12:00:00.0000005", 0, 0, hours(12) + 1),
		] {
			let v: Interval = text.parse().unwrap();
			assert!(v.identical(&Interval::new(months, days, micros)), "{text}: {v}");
		}
		// PostgreSQL's SQL-standard and ISO alternative forms.
		let hms = hours(4) + 5 * 60_000_000 + 6_000_000;
		for (text, months, days, micros) in [
			("3 4:05:06", 0, 3, hms),
			("1-2", 14, 0, 0),
			("-1-2", -14, 0, 0),
			("1-2 3 4:05:06", 14, 3, hms),
			("P0001-02-03T04:05:06", 14, 3, hms),
			("P0001-02-03", 14, 3, 0),
			("PT04:05:06", 0, 0, hms),
			("90", 0, 0, 90_000_000),
			("1 day 2", 0, 1, 2_000_000),
		] {
			let v: Interval = text.parse().unwrap();
			assert!(v.identical(&Interval::new(months, days, micros)), "{text}: {v}");
		}
		assert!("P1-2".parse::<Interval>().is_err());
		assert!("1-".parse::<Interval>().is_err());
		assert!("P".parse::<Interval>().is_err());
		assert!("1 fortnight".parse::<Interval>().is_err());
		assert!("P3000000000Y".parse::<Interval>().is_err());
		assert!("99999999999999999999999999999999999999-0".parse::<Interval>().is_err());
		assert!("3000000000-0".parse::<Interval>().is_err());
	}

	#[test]
	fn interval_equality_uses_normalised_total() {
		let month = Interval::new(1, 0, 0);
		let days = Interval::new(0, 30, 0);
		let hours = Interval::new(0, 0, 720 * 3_600_000_000);
		assert_eq!(month, days);
		assert_eq!(days, hours);
		assert_eq!(hash_of(&month), hash_of(&hours));
		assert!(!month.identical(&days));
		assert!(Interval::new(0, 29, 0) < month);
		assert!(Interval::new(i32::MAX, i32::MAX, i64::MAX) > Interval::new(i32::MAX, i32::MAX, 0));
		assert_ne!(Interval::new(i32::MIN, 0, 0).normalised_micros(), i128::from(i64::MIN));
	}
}
