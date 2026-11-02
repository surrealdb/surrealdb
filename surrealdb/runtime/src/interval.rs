//! The `interval::*` functions: construction, the PostgreSQL-style field extractors, and the
//! normalising `justify_*` family.

use anyhow::Result;
use rust_decimal::Decimal;
use surrealdb_expr::expr::Error as ExprError;
use surrealdb_expr::val::{Interval, Number, Value};

use crate::args::Optional;

const MICROS_PER_SEC: i64 = 1_000_000;
const MICROS_PER_MIN: i64 = 60 * MICROS_PER_SEC;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MIN;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
const DAYS_PER_MONTH: i64 = 30;

fn invalid(name: &str, message: &str) -> anyhow::Error {
	anyhow::Error::new(ExprError::InvalidFunctionArguments {
		name: name.to_owned(),
		message: message.to_owned(),
	})
}

fn overflow(name: &str) -> anyhow::Error {
	anyhow::Error::new(ExprError::ArithmeticOverflow(name.to_owned()))
}

/// A fractional count of `unit` micros, rounded to the interval's resolution. A count that no
/// `decimal` holds, or whose micros overflow one, is an overflow.
fn scaled(name: &str, n: &Option<Number>, unit: i64) -> Result<i128> {
	let Some(n) = n else {
		return Ok(0);
	};
	let micros = n
		.as_decimal_lossless()
		.and_then(|n| n.checked_mul(Decimal::from(unit)))
		.ok_or_else(|| overflow(name))?
		.round();
	micros.mantissa().checked_div(10i128.pow(micros.scale())).ok_or_else(|| overflow(name))
}

/// `interval::make(?years, ?months, ?weeks, ?days, ?hours, ?mins, ?secs)`, each defaulting to
/// zero. A fractional year or month is refused; a fractional week or day spills into the time part.
#[allow(clippy::type_complexity)]
pub fn make(
	(years, months, weeks, days, hours, mins, secs): (
		Optional<Number>,
		Optional<Number>,
		Optional<Number>,
		Optional<Number>,
		Optional<Number>,
		Optional<Number>,
		Optional<Number>,
	),
) -> Result<Value> {
	let name = "interval::make";
	let whole = |n: &Option<Number>, what: &str| -> Result<i64> {
		match n {
			None => Ok(0),
			Some(n) => n
				.as_int_lossless()
				.ok_or_else(|| invalid(name, &format!("The {what} must be an integer."))),
		}
	};
	let total_months = whole(&years.0, "years")?
		.checked_mul(12)
		.and_then(|y| y.checked_add(whole(&months.0, "months").ok()?))
		.ok_or_else(|| overflow(name))?;
	let total_days = whole(&weeks.0, "weeks")?
		.checked_mul(7)
		.and_then(|w| w.checked_add(whole(&days.0, "days").ok()?))
		.ok_or_else(|| overflow(name))?;
	let micros = scaled(name, &hours.0, MICROS_PER_HOUR)?
		+ scaled(name, &mins.0, MICROS_PER_MIN)?
		+ scaled(name, &secs.0, MICROS_PER_SEC)?;
	Ok(Value::Interval(Interval::new(
		i32::try_from(total_months).map_err(|_| overflow(name))?,
		i32::try_from(total_days).map_err(|_| overflow(name))?,
		i64::try_from(micros).map_err(|_| overflow(name))?,
	)))
}

pub fn years((v,): (Interval,)) -> Result<Value> {
	Ok(Value::from(i64::from(v.months / 12)))
}

pub fn months((v,): (Interval,)) -> Result<Value> {
	Ok(Value::from(i64::from(v.months % 12)))
}

pub fn days((v,): (Interval,)) -> Result<Value> {
	Ok(Value::from(i64::from(v.days)))
}

pub fn hours((v,): (Interval,)) -> Result<Value> {
	Ok(Value::from(v.micros / MICROS_PER_HOUR))
}

pub fn mins((v,): (Interval,)) -> Result<Value> {
	Ok(Value::from((v.micros / MICROS_PER_MIN) % 60))
}

/// The seconds field, with its fraction, as a decimal.
pub fn secs((v,): (Interval,)) -> Result<Value> {
	let rest = v.micros % MICROS_PER_MIN;
	Ok(Value::Number(Number::Decimal(
		(Decimal::from(rest) / Decimal::from(MICROS_PER_SEC)).normalize(),
	)))
}

/// The seconds field in microseconds.
pub fn micros((v,): (Interval,)) -> Result<Value> {
	Ok(Value::from(v.micros % MICROS_PER_MIN))
}

fn split(total: i64, unit: i64) -> (i64, i64) {
	(total / unit, total % unit)
}

pub fn justify_hours((v,): (Interval,)) -> Result<Value> {
	let (whole, rest) = split(v.micros, MICROS_PER_DAY);
	let days =
		i64::from(v.days).checked_add(whole).ok_or_else(|| overflow("interval::justify_hours"))?;
	let days = i32::try_from(days).map_err(|_| overflow("interval::justify_hours"))?;
	Ok(Value::Interval(Interval::new(v.months, days, rest)))
}

pub fn justify_days((v,): (Interval,)) -> Result<Value> {
	let (whole, rest) = split(i64::from(v.days), DAYS_PER_MONTH);
	let months =
		i64::from(v.months).checked_add(whole).ok_or_else(|| overflow("interval::justify_days"))?;
	let months = i32::try_from(months).map_err(|_| overflow("interval::justify_days"))?;
	Ok(Value::Interval(Interval::new(months, rest as i32, v.micros)))
}

/// Both justifications, with the signs of the fields aligned.
pub fn justify_interval((v,): (Interval,)) -> Result<Value> {
	let name = "interval::justify_interval";
	let (whole_days, mut micros) = split(v.micros, MICROS_PER_DAY);
	let mut days = i64::from(v.days).checked_add(whole_days).ok_or_else(|| overflow(name))?;
	let (whole_months, rest_days) = split(days, DAYS_PER_MONTH);
	days = rest_days;
	let mut months = i64::from(v.months).checked_add(whole_months).ok_or_else(|| overflow(name))?;
	if months > 0 && (days < 0 || (days == 0 && micros < 0)) {
		days += DAYS_PER_MONTH;
		months -= 1;
	} else if months < 0 && (days > 0 || (days == 0 && micros > 0)) {
		days -= DAYS_PER_MONTH;
		months += 1;
	}
	if days > 0 && micros < 0 {
		micros += MICROS_PER_DAY;
		days -= 1;
	} else if days < 0 && micros > 0 {
		micros -= MICROS_PER_DAY;
		days += 1;
	}
	Ok(Value::Interval(Interval::new(
		i32::try_from(months).map_err(|_| overflow(name))?,
		i32::try_from(days).map_err(|_| overflow(name))?,
		micros,
	)))
}
