//! The `time::*` functions that accept `date`, `time` and `timestamp` beside `datetime`.
//!
//! A `date` and a `timestamp` carry no zone, so a function that reads or rewrites their
//! calendar fields does so through the UTC datetime with the same wall time. A function that
//! rewrites a value returns the kind it was given.

use anyhow::{Result, bail};
use chrono::{Datelike, Months, NaiveDate, NaiveDateTime, NaiveTime, TimeDelta, Timelike};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use surrealdb_expr::expr::Error as ExprError;
use surrealdb_expr::val::{Date, Datetime, Duration, Interval, Number, Time, Timestamp, Value};
use surrealdb_types::ToSql;

use crate::args::Optional;

const NANOS_PER_SEC: i64 = 1_000_000_000;

fn invalid(name: &str, message: impl Into<String>) -> anyhow::Error {
	anyhow::Error::new(ExprError::InvalidFunctionArguments {
		name: name.to_owned(),
		message: message.into(),
	})
}

/// The refusal of an argument that is not one of `accepted`: a value of no temporal kind reads as
/// the datetime mismatch every `time::` function has always reported.
fn reject(name: &str, accepted: &str, v: &Value) -> anyhow::Error {
	if matches!(
		v,
		Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Datetime(_)
			| Value::Interval(_)
	) {
		invalid(name, format!("The argument must be {accepted}, found {}.", kind_name(v)))
	} else {
		invalid(
			name,
			format!(
				"Argument 1 was the wrong type. Expected `datetime` but found `{}`",
				v.to_sql()
			),
		)
	}
}

/// A value of one of the four zone-free and UTC temporal kinds.
#[derive(Clone, Copy)]
enum Temporal {
	Date(Date),
	Time(Time),
	Timestamp(Timestamp),
	Datetime(Datetime),
}

fn temporal(v: &Value) -> Option<Temporal> {
	if let Value::Date(d) = v {
		Some(Temporal::Date(*d))
	} else if let Value::Time(t) = v {
		Some(Temporal::Time(*t))
	} else if let Value::Timestamp(t) = v {
		Some(Temporal::Timestamp(*t))
	} else if let Value::Datetime(d) = v {
		Some(Temporal::Datetime(*d))
	} else {
		None
	}
}

/// The kinds a temporal argument may hold.
fn kind_name(v: &Value) -> &'static str {
	match temporal(v) {
		Some(Temporal::Date(_)) => "date",
		Some(Temporal::Time(_)) => "time",
		Some(Temporal::Timestamp(_)) => "timestamp",
		Some(Temporal::Datetime(_)) => "datetime",
		None if v.is_interval() => "interval",
		None => "a value that is not a date, time, timestamp or datetime",
	}
}

/// The datetime a calendar-reading function evaluates: a date at midnight, a timestamp as UTC.
fn calendar(name: &str, v: &Value) -> Result<Datetime> {
	match temporal(v) {
		Some(Temporal::Datetime(d)) => Ok(d),
		Some(Temporal::Date(d)) => Ok(d.into()),
		Some(Temporal::Timestamp(t)) => Ok(t.into()),
		Some(Temporal::Time(_)) | None => Err(reject(name, "a date, timestamp or datetime", v)),
	}
}

/// The nanoseconds since midnight a clock-reading function evaluates, with the day fields of a
/// date-bearing value dropped.
fn clock_nanos(name: &str, v: &Value) -> Result<u64> {
	match temporal(v) {
		Some(Temporal::Time(t)) => Ok(t.nanos()),
		Some(Temporal::Datetime(d)) => Ok(d.time().nanos()),
		Some(Temporal::Timestamp(t)) => Ok(t.time().nanos()),
		Some(Temporal::Date(_)) | None => Err(reject(name, "a time, timestamp or datetime", v)),
	}
}

/// Applies `f` to the datetime a value stands for and returns the result as the value's own kind.
fn same_kind(name: &str, v: &Value, f: impl FnOnce(Datetime) -> Result<Datetime>) -> Result<Value> {
	let at = calendar(name, v)?;
	let out = f(at)?;
	Ok(match temporal(v) {
		Some(Temporal::Date(_)) => Value::Date(out.exact_date().ok_or_else(|| {
			invalid(
				name,
				"The result is not a whole date, so the argument must be cast to a timestamp first.",
			)
		})?),
		Some(Temporal::Timestamp(_)) => Value::Timestamp(out.into()),
		Some(Temporal::Time(_) | Temporal::Datetime(_)) | None => Value::Datetime(out),
	})
}

fn unwrap_datetime(v: Result<Value>, name: &str) -> Result<Datetime> {
	match temporal(&v?) {
		Some(Temporal::Datetime(d)) => Ok(d),
		Some(Temporal::Date(_) | Temporal::Time(_) | Temporal::Timestamp(_)) | None => {
			Err(invalid(name, "The result is not a datetime."))
		}
	}
}

macro_rules! calendar_accessor {
	($name:ident, $path:literal, $old:path) => {
		pub fn $name((Optional(val),): (Optional<Value>,)) -> Result<Value> {
			match val {
				None => $old((Optional(None),)),
				Some(v) => $old((Optional(Some(calendar($path, &v)?)),)),
			}
		}
	};
}

calendar_accessor!(year, "time::year", super::time::year);
calendar_accessor!(month, "time::month", super::time::month);
calendar_accessor!(day, "time::day", super::time::day);
calendar_accessor!(wday, "time::wday", super::time::wday);
calendar_accessor!(yday, "time::yday", super::time::yday);
calendar_accessor!(week, "time::week", super::time::week);

pub fn is_leap_year((Optional(val),): (Optional<Value>,)) -> Result<Value> {
	match val {
		None => super::time::is::leap_year((Optional(None),)),
		Some(v) => {
			super::time::is::leap_year((Optional(Some(calendar("time::is_leap_year", &v)?)),))
		}
	}
}

macro_rules! clock_accessor {
	($name:ident, $path:literal, $old:path, $time:expr) => {
		pub fn $name((Optional(val),): (Optional<Value>,)) -> Result<Value> {
			match val {
				None => $old((Optional(None),)),
				Some(Value::Time(t)) => {
					let f: fn(u64) -> Result<Value> = $time;
					f(t.nanos())
				}
				Some(v) => {
					clock_nanos($path, &v)?;
					$old((Optional(Some(calendar($path, &v)?)),))
				}
			}
		}
	};
}

clock_accessor!(hour, "time::hour", super::time::hour, |n| Ok(Value::from(
	(n / 3_600_000_000_000) as i64
)));
clock_accessor!(minute, "time::minute", super::time::minute, |n| Ok(Value::from(
	((n / 60_000_000_000) % 60) as i64
)));
clock_accessor!(second, "time::second", super::time::second, |n| Ok(Value::from(
	((n / 1_000_000_000) % 60) as i64
)));
clock_accessor!(nano, "time::nano", super::time::nano, |n| Ok(Value::from(n as i64)));
clock_accessor!(micros, "time::micros", super::time::micros, |n| Ok(Value::from(
	(n / 1_000) as i64
)));
clock_accessor!(millis, "time::millis", super::time::millis, |n| Ok(Value::from(
	(n / 1_000_000) as i64
)));

pub fn format((val, format): (Value, String)) -> Result<Value> {
	super::time::format((calendar("time::format", &val)?, format))
}

pub fn group((val, group): (Value, String)) -> Result<Value> {
	same_kind("time::group", &val, |at| {
		unwrap_datetime(super::time::group((at, group)), "time::group")
	})
}

pub fn floor((val, duration): (Value, Duration)) -> Result<Value> {
	same_kind("time::floor", &val, |at| {
		unwrap_datetime(super::time::floor((at, duration)), "time::floor")
	})
}

pub fn ceil((val, duration): (Value, Duration)) -> Result<Value> {
	same_kind("time::ceil", &val, |at| {
		unwrap_datetime(super::time::ceil((at, duration)), "time::ceil")
	})
}

pub fn round((val, duration): (Value, Duration)) -> Result<Value> {
	same_kind("time::round", &val, |at| {
		unwrap_datetime(super::time::round((at, duration)), "time::round")
	})
}

macro_rules! setter {
	($name:ident, $path:literal, $old:path) => {
		pub fn $name((val, n): (Value, Number)) -> Result<Value> {
			same_kind($path, &val, |at| unwrap_datetime($old((at, n)), $path))
		}
	};
}

setter!(set_year, "time::set_year", super::time::set_year);
setter!(set_month, "time::set_month", super::time::set_month);
setter!(set_day, "time::set_day", super::time::set_day);
setter!(set_hour, "time::set_hour", super::time::set_hour);
setter!(set_minute, "time::set_minute", super::time::set_minute);
setter!(set_second, "time::set_second", super::time::set_second);
setter!(set_nanosecond, "time::set_nanosecond", super::time::set_nanosecond);

fn extremum(name: &str, array: Vec<Value>, max: bool) -> Result<Value> {
	let mut best: Option<Value> = None;
	for v in array {
		if !matches!(
			v,
			Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
		) {
			bail!(invalid(
				name,
				format!(
					"The elements must be dates, times, timestamps, intervals or datetimes, found {}.",
					kind_name(&v)
				)
			));
		}
		best = Some(match best {
			None => v,
			Some(b) => {
				if std::mem::discriminant(&b) != std::mem::discriminant(&v) {
					bail!(invalid(name, "The elements must all be of one kind."));
				}
				if (max && v > b) || (!max && v < b) {
					v
				} else {
					b
				}
			}
		});
	}
	Ok(best.unwrap_or(Value::None))
}

fn elements(name: &str, v: Value) -> Result<Vec<Value>> {
	if let Value::Array(a) = v {
		Ok(a.0)
	} else if let Value::Set(s) = v {
		Ok(s.into_iter().collect())
	} else {
		Err(invalid(name, format!("The argument must be an array, found {}.", v.kind_of())))
	}
}

pub fn max((array,): (Value,)) -> Result<Value> {
	extremum("time::max", elements("time::max", array)?, true)
}

pub fn min((array,): (Value,)) -> Result<Value> {
	extremum("time::min", elements("time::min", array)?, false)
}

fn int_arg(name: &str, what: &str, n: &Number) -> Result<i64> {
	n.as_int_lossless().ok_or_else(|| invalid(name, format!("The {what} must be an integer.")))
}

fn seconds_arg(name: &str, n: &Number) -> Result<(u32, u32)> {
	let d =
		n.as_decimal_lossless().ok_or_else(|| invalid(name, "The seconds are out of range."))?;
	if d.is_sign_negative() && !d.is_zero() {
		bail!(invalid(name, "The seconds must not be negative."));
	}
	let whole = d.trunc();
	let out_of_range = || invalid(name, "The seconds are out of range.");
	let secs = whole.to_u32().ok_or_else(out_of_range)?;
	let nanos =
		((d - whole) * Decimal::from(NANOS_PER_SEC)).round().to_u32().ok_or_else(out_of_range)?;
	// A fraction that rounds up to a whole second carries into the seconds.
	if nanos >= NANOS_PER_SEC as u32 {
		return Ok((secs.checked_add(1).ok_or_else(out_of_range)?, 0));
	}
	Ok((secs, nanos))
}

pub fn make_date((year, month, day): (Number, Number, Number)) -> Result<Value> {
	let name = "time::make_date";
	let (y, m, d) = (
		int_arg(name, "year", &year)?,
		int_arg(name, "month", &month)?,
		int_arg(name, "day", &day)?,
	);
	// A negative year counts years before the common era with no year zero, so -1 is 1 BC, the
	// astronomical year 0. The year 0 does not exist.
	let astronomical = match y {
		0 => None,
		y if y < 0 => Some(y + 1),
		y => Some(y),
	};
	astronomical
		.and_then(|y| i32::try_from(y).ok())
		.zip(u32::try_from(m).ok())
		.zip(u32::try_from(d).ok())
		.and_then(|((y, m), d)| NaiveDate::from_ymd_opt(y, m, d))
		.and_then(surrealdb_types::Date::finite)
		.map(Date::from)
		.map(Value::Date)
		.ok_or_else(|| invalid(name, format!("{y}-{m}-{d} is not a valid date.")))
}

fn make_time_of(name: &str, hour: &Number, min: &Number, sec: &Number) -> Result<u64> {
	let (h, m) = (int_arg(name, "hour", hour)?, int_arg(name, "minute", min)?);
	let (s, ns) = seconds_arg(name, sec)?;
	// The seconds given are below 60; rounding them may carry a whole 60, which carries on into
	// the minutes and hours of the total. The total is at most 24:00:00.
	let given_below_60 = sec.as_decimal_lossless().is_some_and(|d| d < Decimal::from(60));
	// The fields are range-checked before the total is summed, so the sum cannot overflow.
	if !((0..=24).contains(&h) && (0..60).contains(&m) && given_below_60) {
		bail!(invalid(name, "The time fields are out of range."));
	}
	let total = h as u64 * 3_600_000_000_000
		+ m as u64 * 60_000_000_000
		+ u64::from(s) * 1_000_000_000
		+ u64::from(ns);
	if total > 24 * 3_600_000_000_000 || (h == 24 && total != 24 * 3_600_000_000_000) {
		bail!(invalid(name, "The time fields are out of range."));
	}
	Ok(total)
}

pub fn make_time((hour, min, sec): (Number, Number, Number)) -> Result<Value> {
	let nanos = make_time_of("time::make_time", &hour, &min, &sec)?;
	Time::from_nanos(nanos)
		.map(Value::Time)
		.ok_or_else(|| invalid("time::make_time", "The time is out of range."))
}

pub fn make_timestamp(
	(year, month, day, hour, min, sec): (Number, Number, Number, Number, Number, Number),
) -> Result<Value> {
	let name = "time::make_timestamp";
	let Value::Date(date) =
		make_date((year, month, day)).map_err(|e| invalid(name, e.to_string()))?
	else {
		unreachable!("a date");
	};
	let nanos = make_time_of(name, &hour, &min, &sec)?;
	let at = date
		.naive()
		.and_time(NaiveTime::MIN)
		.checked_add_signed(TimeDelta::nanoseconds(nanos as i64))
		.and_then(surrealdb_types::Timestamp::finite)
		.map(Timestamp::from)
		.ok_or_else(|| invalid(name, "The timestamp is out of range."))?;
	Ok(Value::Timestamp(at))
}

fn as_timestamp(name: &str, v: &Value) -> Result<Timestamp> {
	match temporal(v) {
		Some(Temporal::Timestamp(t)) => Ok(t),
		Some(Temporal::Datetime(d)) => Ok(d.into()),
		Some(Temporal::Date(d)) => Ok(d.at_midnight()),
		Some(Temporal::Time(_)) | None => Err(reject(name, "a date, timestamp or datetime", v)),
	}
}

/// An astronomical year (1 BC is 0) as PostgreSQL numbers years, with no year zero: 1 BC is -1.
fn no_year_zero(year: i64) -> i64 {
	if year > 0 {
		year
	} else {
		year - 1
	}
}

/// Whether `v` is the `infinity` or `-infinity` of a date, timestamp or datetime.
fn is_infinite(v: &Value) -> bool {
	match temporal(v) {
		Some(Temporal::Date(d)) => d.is_infinite(),
		Some(Temporal::Timestamp(t)) => t.is_infinite(),
		Some(Temporal::Datetime(d)) => d.is_infinite(),
		Some(Temporal::Time(_)) | None => false,
	}
}

fn days_in_month(year: i32, month: u32) -> i64 {
	let first = NaiveDate::from_ymd_opt(year, month, 1).expect("a valid month");
	let next = first.checked_add_months(Months::new(1)).expect("a following month");
	next.signed_duration_since(first).num_days()
}

/// The symbolic difference `a - b` as years, months, days and a time part, by PostgreSQL's
/// `age` algorithm. An interval is always finite, so the age of `infinity` or `-infinity` is
/// refused as out of range.
pub fn age((a, b): (Value, Value)) -> Result<Value> {
	let name = "time::age";
	let (ta, tb) = (as_timestamp(name, &a)?, as_timestamp(name, &b)?);
	if is_infinite(&a) || is_infinite(&b) {
		bail!(invalid(name, "The age of an infinite value is out of range."));
	}
	let (x, y) = (ta.naive(), tb.naive());
	let negative = x < y;
	let (later, earlier) = if negative {
		(y, x)
	} else {
		(x, y)
	};
	let mut micros = (i64::from(later.time().num_seconds_from_midnight())
		- i64::from(earlier.time().num_seconds_from_midnight()))
		* 1_000_000
		+ (i64::from(later.nanosecond()) / 1_000 - i64::from(earlier.nanosecond()) / 1_000);
	let mut days = i64::from(later.day()) - i64::from(earlier.day());
	let mut months = i64::from(later.month()) - i64::from(earlier.month());
	let mut years = i64::from(later.year()) - i64::from(earlier.year());
	const DAY: i64 = 86_400_000_000;
	while micros < 0 {
		micros += DAY;
		days -= 1;
	}
	while days < 0 {
		// Borrow the length of the earlier timestamp's month, as PostgreSQL does.
		days += days_in_month(earlier.year(), earlier.month());
		months -= 1;
	}
	while months < 0 {
		months += 12;
		years -= 1;
	}
	let total_months = years * 12 + months;
	let (total_months, days, micros) = if negative {
		(-total_months, -days, -micros)
	} else {
		(total_months, days, micros)
	};
	let months = i32::try_from(total_months).map_err(|_| invalid(name, "The age overflows."))?;
	let days = i32::try_from(days).map_err(|_| invalid(name, "The age overflows."))?;
	Ok(Value::Interval(Interval::new(months, days, micros)))
}

fn decimal_value(d: Decimal) -> Value {
	Value::Number(Number::Decimal(d.normalize()))
}

fn iso_year(d: NaiveDate) -> i64 {
	i64::from(d.iso_week().year())
}

/// The interval a value of the interval kinds stands for: a duration is a count of microseconds
/// with no months or days.
fn as_interval(name: &str, v: &Value) -> Result<Option<Interval>> {
	match v {
		Value::Interval(i) => Ok(Some(*i)),
		Value::Duration(d) => {
			let micros = i64::try_from(d.micros())
				.map_err(|_| invalid(name, "The duration is out of range."))?;
			Ok(Some(Interval::new(0, 0, micros)))
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_) => Ok(None),
	}
}

/// One field of an interval, as PostgreSQL's `extract` reads it. The calendar fields of a date
/// are not fields of an interval.
fn extract_interval(name: &str, f: &str, iv: Interval) -> Result<Value> {
	const MICROS_PER_SEC: i64 = 1_000_000;
	const MICROS_PER_MIN: i64 = 60 * MICROS_PER_SEC;
	const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MIN;
	let (months, days, micros) = (i64::from(iv.months), i64::from(iv.days), iv.micros);
	let (years, month) = (months / 12, months % 12);
	let int = |v: i64| Ok(Value::from(v));
	match f {
		"year" => int(years),
		"month" => int(month),
		"day" => int(days),
		"quarter" => int(month / 3 + 1),
		"decade" => int(years / 10),
		"century" => int(years / 100),
		"millennium" => int(years / 1000),
		"hour" => int(micros / MICROS_PER_HOUR),
		"minute" => int((micros / MICROS_PER_MIN) % 60),
		"second" => Ok(decimal_value(
			Decimal::from(micros % MICROS_PER_MIN) / Decimal::from(MICROS_PER_SEC),
		)),
		"milliseconds" => {
			Ok(decimal_value(Decimal::from(micros % MICROS_PER_MIN) / Decimal::from(1_000)))
		}
		"microseconds" => int(micros % MICROS_PER_MIN),
		// A year is 365.25 days and a month a twelfth of it.
		"epoch" => Ok(decimal_value(
			Decimal::from(31_557_600 * years + 2_629_800 * month + 86_400 * days)
				+ Decimal::from(micros) / Decimal::from(MICROS_PER_SEC),
		)),
		"week" => int(days / 7),
		"dow" | "isodow" | "doy" | "isoyear" | "julian" => {
			Err(invalid(name, format!("`{f}` cannot be read from an interval.")))
		}
		_ => Err(invalid(name, UNKNOWN_EXTRACT_FIELD)),
	}
}

/// Truncates an interval to the start of `unit`, as PostgreSQL's `date_trunc`.
fn trunc_interval(name: &str, u: &str, iv: Interval) -> Result<Interval> {
	const MICROS_PER_HOUR: i64 = 3_600_000_000;
	let (months, days, micros) = (iv.months, iv.days, iv.micros);
	let (years, month) = (months / 12, months % 12);
	let out_of_range = || invalid(name, "The result is out of range.");
	// The years kept, in whole multiples of `by`, with no months, days or time.
	let by_years = |by: i32| {
		(years / by).checked_mul(by).and_then(|y| y.checked_mul(12)).ok_or_else(out_of_range)
	};
	Ok(match u {
		"millennium" => Interval::new(by_years(1000)?, 0, 0),
		"century" => Interval::new(by_years(100)?, 0, 0),
		"decade" => Interval::new(by_years(10)?, 0, 0),
		"year" => Interval::new(by_years(1)?, 0, 0),
		"quarter" => Interval::new(years * 12 + month / 3 * 3, 0, 0),
		"month" => Interval::new(months, 0, 0),
		"day" => Interval::new(months, days, 0),
		"hour" => Interval::new(months, days, micros - micros % MICROS_PER_HOUR),
		"minute" => Interval::new(months, days, micros - micros % 60_000_000),
		"second" => Interval::new(months, days, micros - micros % 1_000_000),
		"milliseconds" | "millisecond" => Interval::new(months, days, micros - micros % 1_000),
		"microseconds" | "microsecond" => iv,
		"week" => return Err(invalid(name, "A week cannot be truncated from an interval.")),
		_ => return Err(invalid(name, UNKNOWN_TRUNC_UNIT)),
	})
}

const UNKNOWN_EXTRACT_FIELD: &str = "The field must be one of century, day, decade, dow, doy, epoch, hour, isodow, isoyear, julian, microseconds, millennium, milliseconds, minute, month, quarter, second, week or year.";

const UNKNOWN_TRUNC_UNIT: &str = "The unit must be one of microseconds, milliseconds, second, minute, hour, day, week, month, quarter, year, decade, century or millennium.";

/// The PostgreSQL `extract` field set, over `date`, `time`, `timestamp`, `datetime`, `interval`
/// and `duration`.
pub fn extract((val, field): (Value, String)) -> Result<Value> {
	let name = "time::extract";
	let f = field.to_ascii_lowercase();
	if let Some(iv) = as_interval(name, &val)? {
		return extract_interval(name, &f, iv);
	}
	let given =
		temporal(&val).ok_or_else(|| reject(name, "a date, time, timestamp or datetime", &val))?;
	// `infinity` and `-infinity` are not instants, so their fields are not read from the
	// sentinel they are stored as. As in PostgreSQL, a field that only grows with time is that
	// infinity, and any other field is NULL; a time field of a date is refused as for any date.
	let positive = match given {
		Temporal::Date(d) if d.is_infinite() => Some(d == Date::infinity()),
		Temporal::Timestamp(t) if t.is_infinite() => Some(t == Timestamp::infinity()),
		Temporal::Datetime(d) if d.is_infinite() => Some(d == Datetime::MAX_UTC),
		Temporal::Date(_) | Temporal::Time(_) | Temporal::Timestamp(_) | Temporal::Datetime(_) => {
			None
		}
	};
	if let Some(positive) = positive {
		let infinity = Value::Number(Number::Float(if positive {
			f64::INFINITY
		} else {
			f64::NEG_INFINITY
		}));
		let time_field =
			matches!(f.as_str(), "hour" | "minute" | "second" | "milliseconds" | "microseconds");
		match f.as_str() {
			"year" | "decade" | "century" | "millennium" | "isoyear" | "julian" | "epoch" => {
				return Ok(infinity);
			}
			"month" | "day" | "quarter" | "dow" | "isodow" | "doy" | "week" => {
				return Ok(Value::Null);
			}
			_ if time_field && !matches!(given, Temporal::Date(_)) => return Ok(Value::Null),
			_ => {}
		}
	}
	let (date, nanos): (Option<NaiveDate>, Option<u64>) = match given {
		Temporal::Date(d) => (Some(d.naive()), None),
		Temporal::Time(t) => (None, Some(t.nanos())),
		Temporal::Timestamp(t) => (Some(t.naive().date()), Some(t.time().nanos())),
		Temporal::Datetime(d) => (Some(d.date_naive()), Some(d.time().nanos())),
	};
	let need_date = |what: &str| {
		date.ok_or_else(|| invalid(name, format!("`{what}` cannot be read from a time.")))
	};
	let need_time = |what: &str| {
		nanos.ok_or_else(|| invalid(name, format!("`{what}` cannot be read from a date.")))
	};
	let int = |v: i64| Ok(Value::from(v));
	match f.as_str() {
		"year" => int(no_year_zero(i64::from(need_date("year")?.year()))),
		"month" => int(i64::from(need_date("month")?.month())),
		"day" => int(i64::from(need_date("day")?.day())),
		"quarter" => int(i64::from((need_date("quarter")?.month() - 1) / 3 + 1)),
		"decade" => int(i64::from(need_date("decade")?.year()).div_euclid(10)),
		"century" => {
			let y = i64::from(need_date("century")?.year());
			int(if y > 0 {
				(y + 99) / 100
			} else {
				-((-y) / 100 + 1)
			})
		}
		"millennium" => {
			let y = i64::from(need_date("millennium")?.year());
			int(if y > 0 {
				(y + 999) / 1000
			} else {
				-((-y) / 1000 + 1)
			})
		}
		"dow" => int(i64::from(need_date("dow")?.weekday().num_days_from_sunday())),
		"isodow" => int(i64::from(need_date("isodow")?.weekday().number_from_monday())),
		"doy" => int(i64::from(need_date("doy")?.ordinal())),
		"week" => int(i64::from(need_date("week")?.iso_week().week())),
		"isoyear" => int(no_year_zero(iso_year(need_date("isoyear")?))),
		"julian" => {
			let d = need_date("julian")?;
			let days = i64::from(Date::from(d).days()) + 2_440_588;
			match nanos {
				Some(n) => {
					let frac = Decimal::from(n) / Decimal::from(86_400_000_000_000u64);
					Ok(decimal_value(Decimal::from(days) + frac))
				}
				None => int(days),
			}
		}
		"hour" => int((need_time("hour")? / 3_600_000_000_000) as i64),
		"minute" => int(((need_time("minute")? / 60_000_000_000) % 60) as i64),
		"second" => {
			let n = need_time("second")? % 60_000_000_000;
			Ok(decimal_value(Decimal::from(n) / Decimal::from(NANOS_PER_SEC)))
		}
		"milliseconds" => {
			let n = need_time("milliseconds")? % 60_000_000_000;
			Ok(decimal_value(Decimal::from(n) / Decimal::from(1_000_000)))
		}
		"microseconds" => int(((need_time("microseconds")? % 60_000_000_000) / 1_000) as i64),
		"epoch" => match given {
			Temporal::Time(t) => {
				Ok(decimal_value(Decimal::from(t.nanos()) / Decimal::from(NANOS_PER_SEC)))
			}
			Temporal::Date(d) => int(i64::from(d.days()) * 86_400),
			Temporal::Timestamp(t) => Ok(decimal_value(
				Decimal::from(t.secs) + Decimal::from(t.nanos) / Decimal::from(NANOS_PER_SEC),
			)),
			Temporal::Datetime(d) => Ok(decimal_value(
				Decimal::from(d.timestamp())
					+ Decimal::from(d.timestamp_subsec_nanos()) / Decimal::from(NANOS_PER_SEC),
			)),
		},
		_ => Err(invalid(name, UNKNOWN_EXTRACT_FIELD)),
	}
}

/// Truncates a value to the start of `unit`, as PostgreSQL's `date_trunc`.
pub fn trunc((val, unit): (Value, String)) -> Result<Value> {
	let name = "time::trunc";
	let u = unit.to_ascii_lowercase();
	match &val {
		Value::Interval(iv) => return trunc_interval(name, &u, *iv).map(Value::Interval),
		Value::Duration(_) => {
			let Some(iv) = as_interval(name, &val)? else {
				unreachable!("a duration is an interval");
			};
			let out = trunc_interval(name, &u, iv)?;
			let micros = u64::try_from(out.micros)
				.map_err(|_| invalid(name, "The result is out of range."))?;
			return Ok(Value::Duration(Duration::from_micros(micros)));
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Datetime(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_) => {}
	}
	if let Value::Time(t) = &val {
		let n = t.nanos();
		let step = match u.as_str() {
			"hour" => 3_600_000_000_000,
			"minute" => 60_000_000_000,
			"second" => 1_000_000_000,
			"milliseconds" | "millisecond" => 1_000_000,
			"microseconds" | "microsecond" => 1_000,
			_ => {
				return Err(invalid(
					name,
					"A time can be truncated to hour, minute, second, milliseconds or microseconds.",
				));
			}
		};
		return Ok(Value::Time(Time(n - n % step)));
	}
	// `infinity` and `-infinity` truncate to themselves: they are not instants in a calendar.
	let infinite = match temporal(&val) {
		Some(Temporal::Date(d)) => d.is_infinite(),
		Some(Temporal::Timestamp(t)) => t.is_infinite(),
		Some(Temporal::Datetime(d)) => d.is_infinite(),
		Some(Temporal::Time(_)) | None => false,
	};
	if infinite {
		return Ok(val);
	}
	same_kind(name, &val, |at| {
		let d = at.naive_utc();
		let date = d.date();
		let t = d.time();
		let midnight = |date: NaiveDate| date.and_time(NaiveTime::MIN);
		let out: NaiveDateTime = match u.as_str() {
			"microseconds" | "microsecond" => {
				d.with_nanosecond(d.nanosecond() / 1_000 * 1_000).unwrap_or(d)
			}
			"milliseconds" | "millisecond" => {
				d.with_nanosecond(d.nanosecond() / 1_000_000 * 1_000_000).unwrap_or(d)
			}
			"second" => d.with_nanosecond(0).unwrap_or(d),
			"minute" => date.and_hms_opt(t.hour(), t.minute(), 0).unwrap_or(d),
			"hour" => date.and_hms_opt(t.hour(), 0, 0).unwrap_or(d),
			"day" => midnight(date),
			"week" => midnight(
				date.checked_sub_signed(TimeDelta::days(i64::from(
					date.weekday().num_days_from_monday(),
				)))
				.ok_or_else(|| invalid(name, "The result is out of range."))?,
			),
			"month" => midnight(date.with_day(1).unwrap_or(date)),
			"quarter" => midnight(
				NaiveDate::from_ymd_opt(date.year(), (date.month() - 1) / 3 * 3 + 1, 1)
					.unwrap_or(date),
			),
			"year" => midnight(NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap_or(date)),
			"decade" => midnight(
				NaiveDate::from_ymd_opt(date.year().div_euclid(10) * 10, 1, 1)
					.ok_or_else(|| invalid(name, "The result is out of range."))?,
			),
			"century" => {
				let y = if date.year() > 0 {
					(date.year() - 1) / 100 * 100 + 1
				} else {
					-((-date.year()) / 100 * 100 + 99)
				};
				midnight(
					NaiveDate::from_ymd_opt(y, 1, 1)
						.ok_or_else(|| invalid(name, "The result is out of range."))?,
				)
			}
			"millennium" => {
				let y = if date.year() > 0 {
					(date.year() - 1) / 1000 * 1000 + 1
				} else {
					-((-date.year()) / 1000 * 1000 + 999)
				};
				midnight(
					NaiveDate::from_ymd_opt(y, 1, 1)
						.ok_or_else(|| invalid(name, "The result is out of range."))?,
				)
			}
			_ => return Err(invalid(name, UNKNOWN_TRUNC_UNIT)),
		};
		Ok(Datetime(out.and_utc()))
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_bc_year_has_no_year_zero() {
		let field = |f: &str, y: i32| {
			let d = NaiveDate::from_ymd_opt(y, 6, 15).unwrap();
			extract((Value::Date(d.into()), f.to_string())).unwrap()
		};
		assert_eq!(field("year", 0), Value::from(-1i64));
		assert_eq!(field("year", -1), Value::from(-2i64));
		assert_eq!(field("year", 1), Value::from(1i64));
		assert_eq!(field("isoyear", 0), Value::from(-1i64));
		assert_eq!(field("century", 0), Value::from(-1i64));
	}

	#[test]
	fn the_age_of_an_infinity_is_refused() {
		let finite = Value::Timestamp(Timestamp::from_parts(0, 0).unwrap());
		for infinite in [
			Value::Timestamp(Timestamp::infinity()),
			Value::Timestamp(Timestamp::neg_infinity()),
			Value::Datetime(Datetime::MAX_UTC),
			Value::Date(Date::neg_infinity()),
		] {
			assert!(age((infinite.clone(), finite.clone())).is_err());
			assert!(age((finite.clone(), infinite.clone())).is_err());
			assert!(age((infinite.clone(), infinite)).is_err());
		}
		assert!(age((finite.clone(), finite)).is_ok());
	}

	#[test]
	fn a_carried_second_carries_into_the_minutes_and_hours() {
		let n = |v: i64| Number::Int(v);
		let almost = Number::Decimal(Decimal::new(599_999_999_999, 10));
		let at = |h, m| make_time((n(h), n(m), almost.clone())).unwrap();
		assert_eq!(
			at(12, 0),
			Value::Time(Time::from_nanos(12 * 3_600_000_000_000 + 60_000_000_000).unwrap())
		);
		assert_eq!(at(23, 59), Value::Time(Time::from_nanos(24 * 3_600_000_000_000).unwrap()));
		assert!(make_time((n(12), n(0), n(60))).is_err());
		assert!(make_time((n(24), n(0), almost.clone())).is_err());
		assert!(make_time((n(24), n(0), n(0))).is_ok());
	}

	#[test]
	fn a_fraction_rounding_to_a_whole_second_carries() {
		let n = Number::Decimal(Decimal::new(19_999_999_999, 10));
		assert_eq!(seconds_arg("t", &n).unwrap(), (2, 0));
		let max = Number::Decimal(Decimal::from(u32::MAX) + Decimal::new(9_999_999_999, 10));
		assert!(seconds_arg("t", &max).is_err());
	}

	#[test]
	fn truncating_the_week_at_the_start_of_the_calendar_returns() {
		// Some of these days have a Monday before the calendar begins; the call errors for them
		// and never panics.
		let mut day = NaiveDate::MIN;
		for _ in 0..7 {
			let at = Value::Datetime(Datetime(day.and_time(NaiveTime::MIN).and_utc()));
			let _ = trunc((at, "week".to_string()));
			day = day.succ_opt().unwrap();
		}
	}

	#[test]
	fn make_date_reads_a_negative_year_as_before_the_common_era() {
		let n = |v: i64| Number::Int(v);
		let at = |y| make_date((n(y), n(6), n(15))).unwrap();
		let expect = |y: i32| Value::Date(NaiveDate::from_ymd_opt(y, 6, 15).unwrap().into());
		assert_eq!(at(-1), expect(0));
		assert_eq!(at(-44), expect(-43));
		assert_eq!(at(1), expect(1));
		assert!(make_date((n(0), n(6), n(15))).is_err());
	}

	#[test]
	fn an_interval_is_extracted_and_truncated() {
		let iv = Value::Interval(Interval::new(14, 3, 3_723_500_000));
		let field = |f: &str| extract((iv.clone(), f.to_string())).unwrap();
		assert_eq!(field("year"), Value::from(1i64));
		assert_eq!(field("month"), Value::from(2i64));
		assert_eq!(field("day"), Value::from(3i64));
		assert_eq!(field("hour"), Value::from(1i64));
		assert_eq!(field("minute"), Value::from(2i64));
		assert_eq!(field("microseconds"), Value::from(3_500_000i64));
		assert_eq!(
			extract((Value::Interval(Interval::new(0, 13, 0)), "week".to_string())).unwrap(),
			Value::from(1i64)
		);
		assert!(extract((iv.clone(), "dow".to_string())).is_err());
		let hour = trunc((iv.clone(), "hour".to_string())).unwrap();
		assert_eq!(hour, Value::Interval(Interval::new(14, 3, 3_600_000_000)));
		let year = trunc((iv, "year".to_string())).unwrap();
		assert_eq!(year, Value::Interval(Interval::new(12, 0, 0)));
	}

	#[test]
	fn a_duration_is_extracted_as_microseconds() {
		let d = Value::Duration(Duration::from_secs(90_000));
		assert_eq!(extract((d, "hour".to_string())).unwrap(), Value::from(25i64));
		let hour = trunc((Value::Duration(Duration::from_secs(5_400)), "hour".to_string()));
		assert_eq!(hour.unwrap(), Value::Duration(Duration::from_secs(3_600)));
	}
}
