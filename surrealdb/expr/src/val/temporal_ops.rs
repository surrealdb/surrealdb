//! Arithmetic on `date`, `time`, `timestamp`, `interval` and the temporal combinations with
//! `datetime`, `duration` and numbers.
//!
//! Each entry point answers `None` when the operand pair is not a temporal combination, so the
//! caller falls through to its own error for an unsupported pair. A temporal pair either
//! produces a value or an `ArithmeticOverflow` error; only `time` wraps (modulo 24 hours).
//!
//! An infinite `date`, `timestamp` or `datetime` (`infinity` or `-infinity`) absorbs a shift by an
//! interval, a duration or a number of days: the result is the same infinity, as in PostgreSQL.
//! Taking the difference of an infinite operand is an error, because no finite interval
//! represents it.

use anyhow::{Result, bail};
use chrono::{Months, NaiveDateTime, TimeDelta};

use crate::expr::Error;
use crate::val::number::Wide;
use crate::val::{Date, Duration, Interval, Number, Time, Timestamp, Value};

const MICROS_PER_DAY: i128 = 86_400_000_000;

fn overflow(what: &str) -> anyhow::Error {
	Error::ArithmeticOverflow(what.to_owned()).into()
}

/// The sign an operand contributes: `+` adds, `-` subtracts.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Sign {
	Plus,
	Minus,
}

/// A `timestamp` moved by `interval`: months first, with the day clamped to the end of the
/// month, then days, then the time part. An infinite timestamp is returned unchanged.
fn shift(ts: Timestamp, iv: Interval, sign: Sign) -> Result<Timestamp> {
	if ts.is_infinite() {
		return Ok(ts);
	}
	let (months, days, micros) = match sign {
		Sign::Plus => (i64::from(iv.months), i64::from(iv.days), iv.micros),
		Sign::Minus => (
			-i64::from(iv.months),
			-i64::from(iv.days),
			iv.micros.checked_neg().ok_or_else(|| overflow("interval"))?,
		),
	};
	let mut at: NaiveDateTime = ts.naive();
	if months != 0 {
		let m =
			Months::new(u32::try_from(months.unsigned_abs()).map_err(|_| overflow("timestamp"))?);
		at = if months > 0 {
			at.checked_add_months(m)
		} else {
			at.checked_sub_months(m)
		}
		.ok_or_else(|| overflow("timestamp"))?;
	}
	if days != 0 {
		let d = TimeDelta::try_days(days).ok_or_else(|| overflow("timestamp"))?;
		at = at.checked_add_signed(d).ok_or_else(|| overflow("timestamp"))?;
	}
	if micros != 0 {
		at = at
			.checked_add_signed(TimeDelta::microseconds(micros))
			.ok_or_else(|| overflow("timestamp"))?;
	}
	checked_timestamp(at)
}

fn checked_timestamp(at: NaiveDateTime) -> Result<Timestamp> {
	let ts = Timestamp::from(at);
	// The first and last instants stand for the infinities, so a finite timestamp never shifts
	// onto them.
	if ts.is_valid() && !ts.is_infinite() {
		Ok(ts)
	} else {
		Err(overflow("timestamp"))
	}
}

/// A `timestamp` moved by a duration. An infinite timestamp is returned unchanged.
fn shift_duration(ts: Timestamp, d: Duration, sign: Sign) -> Result<Timestamp> {
	if ts.is_infinite() {
		return Ok(ts);
	}
	let delta = TimeDelta::from_std(d.0).map_err(|_| overflow("timestamp"))?;
	let at = match sign {
		Sign::Plus => ts.naive().checked_add_signed(delta),
		Sign::Minus => ts.naive().checked_sub_signed(delta),
	}
	.ok_or_else(|| overflow("timestamp"))?;
	checked_timestamp(at)
}

/// A `date` moved by a whole number of days. An infinite date is returned unchanged.
fn shift_days(date: Date, days: i64, sign: Sign) -> Result<Date> {
	if date.is_infinite() {
		return Ok(date);
	}
	let days = match sign {
		Sign::Plus => days,
		Sign::Minus => days.checked_neg().ok_or_else(|| overflow("date"))?,
	};
	i64::from(date.days())
		.checked_add(days)
		.and_then(|d| i32::try_from(d).ok())
		.and_then(Date::from_days)
		// The first and last days stand for the infinities, so a finite date never shifts onto
		// them.
		.filter(|date| !date.is_infinite())
		.ok_or_else(|| overflow("date"))
}

/// A `time` moved by `nanos` and wrapped into a day. `24:00:00` is a midnight like any other.
fn wrap_time(time: Time, nanos: i128) -> Time {
	let day = i128::from(Time::NANOS_PER_DAY);
	Time((i128::from(time.nanos()) + nanos).rem_euclid(day) as u64)
}

fn duration_nanos(d: Duration) -> Result<i128> {
	i128::try_from(d.0.as_nanos()).map_err(|_| overflow("time"))
}

fn integer(n: &Number) -> Option<i64> {
	n.as_int_lossless()
}

fn combine(a: Interval, b: Interval, sign: Sign) -> Result<Interval> {
	let op = |x: i32, y: i32| match sign {
		Sign::Plus => x.checked_add(y),
		Sign::Minus => x.checked_sub(y),
	};
	let months = op(a.months, b.months).ok_or_else(|| overflow("interval"))?;
	let days = op(a.days, b.days).ok_or_else(|| overflow("interval"))?;
	let micros = match sign {
		Sign::Plus => a.micros.checked_add(b.micros),
		Sign::Minus => a.micros.checked_sub(b.micros),
	}
	.ok_or_else(|| overflow("interval"))?;
	Ok(Interval::new(months, days, micros))
}

/// `timestamp - timestamp`: whole days and a time part, no months. An infinite operand has no
/// finite difference, so it is an overflow, as PostgreSQL reports it.
fn timestamp_diff(a: Timestamp, b: Timestamp) -> Result<Interval> {
	if a.is_infinite() || b.is_infinite() {
		return Err(overflow("interval"));
	}
	let delta = a.naive().signed_duration_since(b.naive());
	let days = delta.num_days();
	let rest = delta - TimeDelta::try_days(days).ok_or_else(|| overflow("interval"))?;
	let micros = rest.num_microseconds().ok_or_else(|| overflow("interval"))?;
	let days = i32::try_from(days).map_err(|_| overflow("interval"))?;
	Ok(Interval::new(0, days, micros))
}

/// The PostgreSQL rule for scaling an interval: a fractional month spills into days, and a
/// fractional day into the time part.
fn scale(iv: Interval, factor: f64, divide: bool) -> Result<Interval> {
	if divide && factor == 0.0 {
		bail!("An interval cannot be divided by zero.");
	}
	if !factor.is_finite() {
		bail!(Error::ArithmeticOverflow("interval".into()));
	}
	let apply = |v: f64| {
		if divide {
			v / factor
		} else {
			v * factor
		}
	};
	let to_i32 = |v: f64| {
		let t = v.trunc();
		if t >= f64::from(i32::MIN) && t <= f64::from(i32::MAX) {
			Ok(t as i32)
		} else {
			Err(overflow("interval"))
		}
	};
	let months_f = apply(f64::from(iv.months));
	let months = to_i32(months_f)?;
	let month_days = (months_f - f64::from(months)) * 30.0;
	let days_f = apply(f64::from(iv.days));
	let mut days = to_i32(days_f)?;
	let mut day_frac = days_f - f64::from(days);
	let whole = month_days.trunc();
	days = days.checked_add(whole as i32).ok_or_else(|| overflow("interval"))?;
	day_frac += month_days - whole;
	let micros_f = apply(iv.micros as f64) + day_frac * MICROS_PER_DAY as f64;
	let micros_f = micros_f.round();
	if !(micros_f >= i64::MIN as f64 && micros_f < i64::MAX as f64) {
		bail!(overflow("interval"));
	}
	Ok(Interval::new(months, days, micros_f as i64))
}

fn factor(n: &Number) -> f64 {
	match n.wide() {
		Wide::Int(i) => i as f64,
		Wide::Float(f) => f,
		Wide::Decimal(d) => d.to_string().parse().unwrap_or(f64::NAN),
		Wide::BigDecimal(d) => d.to_f64(),
	}
}

/// `a + b`, or `None` if the pair is not a temporal combination.
pub fn add(a: &Value, b: &Value) -> Option<Result<Value>> {
	match (a, b) {
		(Value::Date(d), Value::Number(n)) | (Value::Number(n), Value::Date(d)) => Some(
			integer(n)
				.ok_or_else(|| overflow("date"))
				.and_then(|n| shift_days(*d, n, Sign::Plus))
				.map(Value::Date),
		),
		(Value::Date(d), Value::Interval(i)) | (Value::Interval(i), Value::Date(d)) => {
			Some(shift(d.at_midnight(), *i, Sign::Plus).map(Value::Timestamp))
		}
		(Value::Date(d), Value::Duration(x)) | (Value::Duration(x), Value::Date(d)) => {
			Some(shift_duration(d.at_midnight(), *x, Sign::Plus).map(Value::Timestamp))
		}
		(Value::Date(d), Value::Time(t)) | (Value::Time(t), Value::Date(d)) => Some(
			shift_duration(d.at_midnight(), Duration::from_nanos(t.nanos()), Sign::Plus)
				.map(Value::Timestamp),
		),
		(Value::Time(t), Value::Interval(i)) | (Value::Interval(i), Value::Time(t)) => {
			Some(Ok(Value::Time(wrap_time(*t, i128::from(i.micros) * 1_000))))
		}
		(Value::Time(t), Value::Duration(x)) | (Value::Duration(x), Value::Time(t)) => {
			Some(duration_nanos(*x).map(|n| Value::Time(wrap_time(*t, n))))
		}
		(Value::Timestamp(t), Value::Interval(i)) | (Value::Interval(i), Value::Timestamp(t)) => {
			Some(shift(*t, *i, Sign::Plus).map(Value::Timestamp))
		}
		(Value::Timestamp(t), Value::Duration(x)) | (Value::Duration(x), Value::Timestamp(t)) => {
			Some(shift_duration(*t, *x, Sign::Plus).map(Value::Timestamp))
		}
		(Value::Datetime(t), Value::Interval(i)) | (Value::Interval(i), Value::Datetime(t)) => {
			Some(shift((*t).into(), *i, Sign::Plus).map(|t| Value::Datetime(t.into())))
		}
		(Value::Interval(x), Value::Interval(y)) => {
			Some(combine(*x, *y, Sign::Plus).map(Value::Interval))
		}
		_ => None,
	}
}

/// `a - b`, or `None` if the pair is not a temporal combination.
pub fn sub(a: &Value, b: &Value) -> Option<Result<Value>> {
	match (a, b) {
		(Value::Date(d), Value::Number(n)) => Some(
			integer(n)
				.ok_or_else(|| overflow("date"))
				.and_then(|n| shift_days(*d, n, Sign::Minus))
				.map(Value::Date),
		),
		(Value::Date(x), Value::Date(y)) => Some(if x.is_infinite() || y.is_infinite() {
			Err(overflow("date"))
		} else {
			Ok(Value::from(i64::from(x.days()) - i64::from(y.days())))
		}),
		(Value::Date(d), Value::Interval(i)) => {
			Some(shift(d.at_midnight(), *i, Sign::Minus).map(Value::Timestamp))
		}
		(Value::Date(d), Value::Duration(x)) => {
			Some(shift_duration(d.at_midnight(), *x, Sign::Minus).map(Value::Timestamp))
		}
		(Value::Time(t), Value::Interval(i)) => {
			Some(Ok(Value::Time(wrap_time(*t, -(i128::from(i.micros) * 1_000)))))
		}
		(Value::Time(t), Value::Duration(x)) => {
			Some(duration_nanos(*x).map(|n| Value::Time(wrap_time(*t, -n))))
		}
		(Value::Time(x), Value::Time(y)) => {
			let micros = (i128::from(x.nanos()) - i128::from(y.nanos())) / 1_000;
			Some(
				i64::try_from(micros)
					.map(|m| Value::Interval(Interval::new(0, 0, m)))
					.map_err(|_| overflow("interval")),
			)
		}
		(Value::Timestamp(t), Value::Interval(i)) => {
			Some(shift(*t, *i, Sign::Minus).map(Value::Timestamp))
		}
		(Value::Timestamp(t), Value::Duration(x)) => {
			Some(shift_duration(*t, *x, Sign::Minus).map(Value::Timestamp))
		}
		(Value::Timestamp(x), Value::Timestamp(y)) => {
			Some(timestamp_diff(*x, *y).map(Value::Interval))
		}
		(Value::Datetime(t), Value::Interval(i)) => {
			Some(shift((*t).into(), *i, Sign::Minus).map(|t| Value::Datetime(t.into())))
		}
		(Value::Interval(x), Value::Interval(y)) => {
			Some(combine(*x, *y, Sign::Minus).map(Value::Interval))
		}
		_ => None,
	}
}

/// `a * b`, or `None` if the pair is not a temporal combination.
pub fn mul(a: &Value, b: &Value) -> Option<Result<Value>> {
	match (a, b) {
		(Value::Interval(i), Value::Number(n)) | (Value::Number(n), Value::Interval(i)) => {
			Some(scale(*i, factor(n), false).map(Value::Interval))
		}
		_ => None,
	}
}

/// `a / b`, or `None` if the pair is not a temporal combination.
pub fn div(a: &Value, b: &Value) -> Option<Result<Value>> {
	match (a, b) {
		(Value::Interval(i), Value::Number(n)) => {
			Some(scale(*i, factor(n), true).map(Value::Interval))
		}
		_ => None,
	}
}

/// `-a`, or `None` if the value is not an interval.
pub fn neg(a: &Value) -> Option<Result<Value>> {
	let Value::Interval(i) = a else {
		return None;
	};
	Some(combine(Interval::new(0, 0, 0), *i, Sign::Minus).map(Value::Interval))
}
