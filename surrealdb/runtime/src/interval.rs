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
/// zero. The years, months, weeks and days must be integers and are refused otherwise; the hours,
/// minutes and seconds may be fractional, to the microsecond.
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
	// Every field is validated before any total is summed, so a fractional field is reported as
	// such rather than as an overflow.
	let (years, months) = (whole(&years.0, "years")?, whole(&months.0, "months")?);
	let (weeks, days) = (whole(&weeks.0, "weeks")?, whole(&days.0, "days")?);
	let total_months =
		years.checked_mul(12).and_then(|y| y.checked_add(months)).ok_or_else(|| overflow(name))?;
	let total_days =
		weeks.checked_mul(7).and_then(|w| w.checked_add(days)).ok_or_else(|| overflow(name))?;
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

/// Moves whole days out of the time part, then aligns the signs of the days and the time part.
pub fn justify_hours((v,): (Interval,)) -> Result<Value> {
	let name = "interval::justify_hours";
	let (whole, mut micros) = split(v.micros, MICROS_PER_DAY);
	let mut days = i64::from(v.days).checked_add(whole).ok_or_else(|| overflow(name))?;
	if days > 0 && micros < 0 {
		micros += MICROS_PER_DAY;
		days -= 1;
	} else if days < 0 && micros > 0 {
		micros -= MICROS_PER_DAY;
		days += 1;
	}
	let days = i32::try_from(days).map_err(|_| overflow(name))?;
	Ok(Value::Interval(Interval::new(v.months, days, micros)))
}

/// Moves whole months out of the days, then aligns the signs of the months and the days.
pub fn justify_days((v,): (Interval,)) -> Result<Value> {
	let name = "interval::justify_days";
	let (whole, mut days) = split(i64::from(v.days), DAYS_PER_MONTH);
	let mut months = i64::from(v.months).checked_add(whole).ok_or_else(|| overflow(name))?;
	if months > 0 && days < 0 {
		days += DAYS_PER_MONTH;
		months -= 1;
	} else if months < 0 && days > 0 {
		days -= DAYS_PER_MONTH;
		months += 1;
	}
	let months = i32::try_from(months).map_err(|_| overflow(name))?;
	Ok(Value::Interval(Interval::new(months, days as i32, v.micros)))
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

#[cfg(test)]
mod tests {
	use super::*;

	fn parts(v: &Value) -> (i32, i32, i64) {
		let Value::Interval(i) = v else {
			panic!("an interval");
		};
		(i.months, i.days, i.micros)
	}

	#[test]
	fn justify_hours_aligns_the_signs() {
		// 1 day -1 hour is 23 hours, not 1 day and -1 hour.
		let v = Interval::new(0, 1, -MICROS_PER_HOUR);
		assert_eq!(parts(&justify_hours((v,)).unwrap()), (0, 0, 23 * MICROS_PER_HOUR));
		let v = Interval::new(0, -1, MICROS_PER_HOUR);
		assert_eq!(parts(&justify_hours((v,)).unwrap()), (0, 0, -23 * MICROS_PER_HOUR));
	}

	#[test]
	fn justify_days_aligns_the_signs() {
		// 1 month -1 day is 29 days.
		let v = Interval::new(1, -1, 0);
		assert_eq!(parts(&justify_days((v,)).unwrap()), (0, 29, 0));
		let v = Interval::new(-1, 1, 0);
		assert_eq!(parts(&justify_days((v,)).unwrap()), (0, -29, 0));
		let v = Interval::new(0, 45, 0);
		assert_eq!(parts(&justify_days((v,)).unwrap()), (1, 15, 0));
	}

	#[test]
	fn make_reports_a_fractional_month_as_invalid() {
		let months = Optional(Some(Number::Decimal(Decimal::new(15, 1))));
		let none = || Optional(None);
		let err = make((none(), months, none(), none(), none(), none(), none())).unwrap_err();
		assert!(err.to_string().contains("months must be an integer"), "{err}");
	}
}
