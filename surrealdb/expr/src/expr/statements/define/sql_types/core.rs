//! The character, bit, binary, numeric, and fractional-second types of a declared SQL type.
//!
//! A value of a type this family owns is checked against the declared length, precision, scale,
//! or fractional-second digits, and canonicalised to them: trailing spaces beyond a character
//! length are dropped, a `numeric(p,s)` is rounded half away from zero to `s` digits, a
//! `BINARY(n)` is padded with `0x00`, and a temporal value is rounded to its digits with any carry
//! propagated.

#[cfg(test)]
mod tests;

use chrono::Timelike;
use rust_decimal::{Decimal, RoundingStrategy};
use surrealdb_types::BigDecimal;

use super::super::SqlTypeDialect;
use super::super::declared_type::{
	DeclaredSqlType, IntervalFinestField, LengthUnit, TypeViolation,
};
use super::Validated;
use crate::val::{Bytes, Datetime, Number, Strand, Time, Timestamp, Value};

/// SQLSTATE `22003`, numeric value out of range.
const NUMERIC_OVERFLOW: &str = "22003";
/// SQLSTATE `22008`, datetime field overflow.
const DATETIME_OVERFLOW: &str = "22008";
/// SQLSTATE `22P02`, invalid text representation (a bit string with a digit other than 0 or 1).
const INVALID_TEXT: &str = "22P02";

/// Nanoseconds in one second.
const NANOS_PER_SECOND: u32 = 1_000_000_000;

/// The validation of the core family. It owns a declared type when the type has a length, a
/// bit length, a numeric precision, or fractional-second digits, and the value is of the kind that
/// the type stores; any other pairing is declined.
pub fn validate(declared: &DeclaredSqlType, value: Value, column: &str) -> Validated {
	let owned = match &value {
		Value::String(_) => match declared.max_length() {
			Some(length) => match length.unit {
				LengthUnit::Chars | LengthUnit::Bytes => true,
				LengthUnit::Bits => declared.dialect() == SqlTypeDialect::PostgreSql,
			},
			None => false,
		},
		Value::Bytes(_) => {
			declared.max_length().is_some_and(|length| length.unit == LengthUnit::Bytes)
		}
		Value::Number(Number::Decimal(_) | Number::Int(_)) => declared.precision().is_some(),
		Value::Number(Number::Float(_) | Number::F32(_) | Number::F16(_)) => {
			is_unsigned(declared) || is_mysql_float(declared)
		}
		Value::Number(Number::BigDecimal(_)) => {
			declared.precision().is_some() || is_dynamodb_number(declared)
		}
		Value::Set(_) => is_dynamodb_number_set(declared),
		Value::Time(_) | Value::Timestamp(_) | Value::Datetime(_) | Value::Interval(_) => {
			declared.fractional_seconds().is_some()
		}
		// A MySQL `DATE` holds a finite date of the years 0000 to 9999.
		Value::Date(_) => {
			declared.dialect() == SqlTypeDialect::MySql
				&& declared.base() == "date"
				&& declared.array_dimensions().is_empty()
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => false,
	};
	if !owned {
		return Validated::Declined(value);
	}
	Validated::Decided(store(declared, value, column))
}

/// The value to store for a value the family owns, or the violation that refuses it.
fn store(declared: &DeclaredSqlType, value: Value, column: &str) -> Result<Value, TypeViolation> {
	match value {
		Value::String(text) => {
			let is_bits = declared.max_length().is_some_and(|l| l.unit == LengthUnit::Bits);
			if is_bits {
				// PostgreSQL judges the length before the digits.
				declared.check_bits(&text)?;
				check_bit_digits(&text)?;
				return Ok(Value::String(text));
			}
			// The kept text is a prefix of the written one: only trailing spaces are dropped.
			let kept = declared.check_text(&text, column)?.len();
			if kept == text.len() {
				return Ok(Value::String(text));
			}
			Ok(Value::String(Strand::new(&text[..kept])))
		}
		Value::Bytes(bytes) => {
			let stored = declared.check_bytes(bytes.0.to_vec(), column)?;
			Ok(Value::Bytes(Bytes::from(stored)))
		}
		Value::Number(number) => {
			let decimal = match number {
				Number::Decimal(decimal) => decimal,
				Number::BigDecimal(big) => {
					if is_dynamodb_number(declared) {
						check_dynamodb_number(&big)?;
						return Ok(Value::Number(Number::BigDecimal(big)));
					}
					let stored = fit_bigdecimal(declared, &big)
						.and_then(|stored| check_unsigned(declared, stored.signum() < 0, stored))
						.map_err(|violation| in_dialect(declared, column, violation))?;
					return Ok(Value::Number(Number::BigDecimal(Box::new(stored))));
				}
				Number::Int(int) => Decimal::from(int),
				// A float is owned by an unsigned type, which refuses a negative one, and by a
				// MySQL floating-point type; MySQL refuses NaN and the infinities in either.
				Number::Float(float) => {
					return fit_float(declared, float)
						.and_then(|float| {
							check_float(
								declared,
								float.is_finite(),
								float < 0.0,
								Number::Float(float),
							)
						})
						.map(Value::Number)
						.map_err(|violation| in_dialect(declared, column, violation));
				}
				Number::F32(float) => {
					return fit_float(declared, f64::from(float))
						.and_then(|fitted| {
							let float = fitted as f32;
							check_float(
								declared,
								float.is_finite(),
								float < 0.0,
								Number::F32(float),
							)
						})
						.map(Value::Number)
						.map_err(|violation| in_dialect(declared, column, violation));
				}
				Number::F16(float) => {
					return check_float(
						declared,
						float.is_finite(),
						float < half::f16::ZERO,
						Number::F16(float),
					)
					.map(Value::Number)
					.map_err(|violation| in_dialect(declared, column, violation));
				}
				// The owner only admits decimal, integer and float numbers.
				other @ (Number::I8(_)
				| Number::U8(_)
				| Number::I16(_)
				| Number::U16(_)
				| Number::I32(_)
				| Number::U32(_)
				| Number::U64(_)) => return Ok(Value::Number(other)),
			};
			let stored = fit_decimal(declared, decimal)
				.and_then(|stored| {
					check_unsigned(declared, stored.is_sign_negative() && !stored.is_zero(), stored)
				})
				.map_err(|violation| in_dialect(declared, column, violation))?;
			Ok(Value::Number(Number::Decimal(stored)))
		}
		Value::Set(set) => {
			for element in set.iter() {
				if let Value::Number(Number::BigDecimal(big)) = element {
					check_dynamodb_number(big)?;
				}
			}
			Ok(Value::Set(set))
		}
		Value::Date(date) if date.is_infinite() => keep_infinity(declared, Value::Date(date)),
		Value::Date(date) => {
			in_mysql_range(declared, i64::from(date.days()) * 86_400, 0).map(|()| Value::Date(date))
		}
		Value::Time(time) => Ok(Value::Time(round_time(declared, time))),
		Value::Timestamp(timestamp) => round_timestamp(declared, timestamp)
			.and_then(|t| in_mysql_range(declared, t.secs, t.nanos).map(|()| Value::Timestamp(t))),
		Value::Datetime(datetime) => round_datetime(declared, datetime).and_then(|d| {
			let (secs, nanos) = (d.0.timestamp(), d.0.timestamp_subsec_nanos());
			in_mysql_range(declared, secs, nanos).map(|()| Value::Datetime(d))
		}),
		Value::Interval(mut interval) => {
			truncate_to_fields(declared, &mut interval);
			interval.micros = round_micros(declared, interval.micros)?;
			if is_mysql_time(declared) {
				check_mysql_time(&interval, column)?;
			}
			Ok(Value::Interval(interval))
		}
		// The owner only admits the kinds above.
		other @ (Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Duration(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_)) => Ok(other),
	}
}

/// A PostgreSQL bit string holds only the digits `0` and `1`.
fn check_bit_digits(bits: &str) -> Result<(), TypeViolation> {
	match bits.chars().find(|digit| *digit != '0' && *digit != '1') {
		Some(digit) => Err(TypeViolation {
			code: INVALID_TEXT,
			message: format!("\"{digit}\" is not a valid binary digit"),
		}),
		None => Ok(()),
	}
}

/// `violation` as the declared type's dialect reports it: MySQL names the column of a numeric
/// overflow (`Out of range value for column 'n' at row 1`), where PostgreSQL reports
/// `numeric field overflow`.
fn in_dialect(declared: &DeclaredSqlType, column: &str, violation: TypeViolation) -> TypeViolation {
	match declared.dialect() {
		SqlTypeDialect::MySql if violation.code == NUMERIC_OVERFLOW => TypeViolation {
			code: violation.code,
			message: format!("Out of range value for column '{column}' at row 1"),
		},
		SqlTypeDialect::MySql | SqlTypeDialect::PostgreSql | SqlTypeDialect::DynamoDb => violation,
	}
}

/// Whether the type is MySQL `unsigned`, which `zerofill` implies.
fn is_unsigned(declared: &DeclaredSqlType) -> bool {
	declared.attributes().iter().any(|attribute| attribute == "unsigned" || attribute == "zerofill")
}

/// Whether the type is a MySQL floating-point type, which holds finite values only.
fn is_mysql_float(declared: &DeclaredSqlType) -> bool {
	declared.dialect() == SqlTypeDialect::MySql
		&& declared.array_dimensions().is_empty()
		&& matches!(
			declared.base(),
			"float" | "double" | "double precision" | "real" | "float4" | "float8"
		)
}

/// The display width `M` and decimals `D` of a MySQL `FLOAT(M,D)` or `DOUBLE(M,D)`.
fn mysql_float_digits(declared: &DeclaredSqlType) -> Option<(u32, u32)> {
	if !is_mysql_float(declared) {
		return None;
	}
	let modifiers = declared.modifiers();
	let width = modifiers.first()?.parse::<u32>().ok()?;
	let decimals = modifiers.get(1)?.parse::<u32>().ok()?;
	(decimals <= width).then_some((width, decimals))
}

/// A float rounded half away from zero to the `D` decimals of a MySQL `FLOAT(M,D)` or
/// `DOUBLE(M,D)`, and refused when its magnitude reaches `10^(M-D)`, which the `M - D` integer
/// digits cannot hold. Any other float, and a NaN or infinity, is returned as it is.
fn fit_float(declared: &DeclaredSqlType, value: f64) -> Result<f64, TypeViolation> {
	let Some((width, decimals)) = mysql_float_digits(declared) else {
		return Ok(value);
	};
	if !value.is_finite() {
		return Ok(value);
	}
	let overflow = || TypeViolation {
		code: NUMERIC_OVERFLOW,
		message: "numeric field overflow".to_string(),
	};
	let decimals = i32::try_from(decimals).map_err(|_| overflow())?;
	let integer_digits = i32::try_from(width).map_err(|_| overflow())? - decimals;
	let scale = 10f64.powi(decimals);
	let rounded = (value * scale).round() / scale;
	if !rounded.is_finite() || rounded.abs() >= 10f64.powi(integer_digits) {
		return Err(overflow());
	}
	Ok(rounded)
}

/// A float owned by a MySQL `unsigned` or floating-point type: refused when it is NaN or infinite,
/// which MySQL does not store, or negative under `unsigned`.
fn check_float<T>(
	declared: &DeclaredSqlType,
	finite: bool,
	negative: bool,
	stored: T,
) -> Result<T, TypeViolation> {
	if !finite && declared.dialect() == SqlTypeDialect::MySql {
		return Err(TypeViolation {
			code: NUMERIC_OVERFLOW,
			message: "numeric field overflow".to_string(),
		});
	}
	check_unsigned(declared, negative, stored)
}

/// `stored`, refused as an overflow when it is `negative` and the type is MySQL `unsigned`. The
/// sign is judged after rounding, so a value that rounds to zero is kept.
fn check_unsigned<T>(
	declared: &DeclaredSqlType,
	negative: bool,
	stored: T,
) -> Result<T, TypeViolation> {
	if negative && is_unsigned(declared) {
		return Err(TypeViolation {
			code: NUMERIC_OVERFLOW,
			message: "numeric field overflow".to_string(),
		});
	}
	Ok(stored)
}

/// The decimal rounded to the declared scale, half away from zero, and refused when its integer
/// digits exceed the declared precision less the scale.
fn fit_decimal(declared: &DeclaredSqlType, decimal: Decimal) -> Result<Decimal, TypeViolation> {
	let (Some(precision), Some(scale)) = (declared.precision(), declared.scale()) else {
		return Ok(decimal);
	};
	let Ok(scale_digits) = u32::try_from(scale) else {
		// A negative scale rounds to a power of ten (`numeric(2,-3)` to thousands), which the
		// bigdecimal rounding expresses; the result is a decimal again when it fits one.
		let stored = fit_bigdecimal(declared, &BigDecimal::from(decimal))?;
		return Decimal::try_from(&stored).map_err(|_| TypeViolation {
			code: NUMERIC_OVERFLOW,
			message: "numeric field overflow".to_string(),
		});
	};
	let mut rounded =
		decimal.round_dp_with_strategy(scale_digits, RoundingStrategy::MidpointAwayFromZero);
	if rounded.scale() < scale_digits {
		rounded.rescale(scale_digits);
	}
	check_integer_digits(rounded, precision, scale).map(|()| rounded)
}

/// [`fit_decimal`] for a value beyond the range of `decimal`: rounded half away from zero to the
/// declared scale, then refused when its integer digits exceed `precision - scale`.
fn fit_bigdecimal(
	declared: &DeclaredSqlType,
	value: &BigDecimal,
) -> Result<BigDecimal, TypeViolation> {
	let overflow = || TypeViolation {
		code: NUMERIC_OVERFLOW,
		message: "numeric field overflow".to_string(),
	};
	let (Some(precision), Some(scale)) = (declared.precision(), declared.scale()) else {
		return Ok(value.clone());
	};
	let rounded = value.round(scale).map_err(|_| overflow())?;
	let allowed = i64::try_from(precision).unwrap_or(i64::MAX).saturating_sub(scale);
	if allowed < 0 {
		// A scale beyond the precision leaves no integer digits and `-allowed` leading zeros of
		// the fraction: the value must stay below `10^allowed` in magnitude.
		let limit = usize::try_from(-allowed)
			.ok()
			.and_then(|zeros| format!("0.{}1", "0".repeat(zeros - 1)).parse::<BigDecimal>().ok());
		let within = match limit {
			Some(limit) => rounded.abs() < limit,
			None => rounded.is_zero(),
		};
		return if within {
			Ok(rounded)
		} else {
			Err(overflow())
		};
	}
	let integer = rounded.trunc(0).map_err(|_| overflow())?;
	let integer_digits = if integer.is_zero() {
		0
	} else {
		integer.abs().to_string().len()
	};
	let integer_digits = i64::try_from(integer_digits).unwrap_or(i64::MAX);
	if integer_digits > allowed {
		return Err(overflow());
	}
	Ok(rounded)
}

/// The most significant digits a DynamoDB number holds.
const DYNAMODB_DIGITS: usize = 38;
/// The range of the power of ten of a nonzero DynamoDB number's leading digit.
const DYNAMODB_EXPONENTS: std::ops::RangeInclusive<i64> = -130..=125;

/// Whether the declared type is DynamoDB's number attribute `N`.
fn is_dynamodb_number(declared: &DeclaredSqlType) -> bool {
	declared.dialect() == SqlTypeDialect::DynamoDb && declared.base() == "n"
}

/// Whether the declared type is DynamoDB's number set attribute `NS`.
fn is_dynamodb_number_set(declared: &DeclaredSqlType) -> bool {
	declared.dialect() == SqlTypeDialect::DynamoDb && declared.base() == "ns"
}

/// Refuse a number DynamoDB cannot hold: more than 38 significant digits, or a nonzero value whose
/// leading digit is outside `1E-130` to `9E+125` in magnitude.
fn check_dynamodb_number(value: &BigDecimal) -> Result<(), TypeViolation> {
	if value.is_zero() {
		return Ok(());
	}
	let normalized = value.normalized();
	let shown = normalized.abs().to_string();
	let (integer, fraction) = shown.split_once('.').unwrap_or((shown.as_str(), ""));
	let integer = integer.trim_start_matches('0');
	let leading = if integer.is_empty() {
		let zeros = fraction.len() - fraction.trim_start_matches('0').len();
		-(i64::try_from(zeros).unwrap_or(i64::MAX)) - 1
	} else {
		i64::try_from(integer.len()).unwrap_or(i64::MAX) - 1
	};
	if normalized.precision() > DYNAMODB_DIGITS {
		return Err(TypeViolation {
			code: NUMERIC_OVERFLOW,
			message: format!(
				"a DynamoDB number holds at most {DYNAMODB_DIGITS} significant digits"
			),
		});
	}
	if !DYNAMODB_EXPONENTS.contains(&leading) {
		return Err(TypeViolation {
			code: NUMERIC_OVERFLOW,
			message: "a DynamoDB number is between 1E-130 and 9.9999999999999999999999999999999999999E+125 in magnitude".to_string(),
		});
	}
	Ok(())
}

/// Refuse a decimal whose integer digits exceed `precision - scale`.
fn check_integer_digits(decimal: Decimal, precision: u64, scale: i64) -> Result<(), TypeViolation> {
	let allowed = i64::try_from(precision).unwrap_or(i64::MAX).saturating_sub(scale);
	if allowed < 0 {
		// A scale beyond the precision leaves no integer digits and `-allowed` leading zeros of
		// the fraction: the value must stay below `10^allowed` in magnitude.
		let within =
			match u32::try_from(-allowed).ok().and_then(|zeros| Decimal::try_new(1, zeros).ok()) {
				Some(limit) => decimal.abs() < limit,
				None => decimal.is_zero(),
			};
		return if within {
			Ok(())
		} else {
			Err(TypeViolation {
				code: NUMERIC_OVERFLOW,
				message: "numeric field overflow".to_string(),
			})
		};
	}
	let integer = decimal.trunc().abs().to_string();
	let digits = integer.split('.').next().unwrap_or("");
	let significant = if digits == "0" {
		0
	} else {
		digits.len()
	};
	if significant > 0 && i64::try_from(significant).unwrap_or(i64::MAX) > allowed {
		return Err(TypeViolation {
			code: NUMERIC_OVERFLOW,
			message: "numeric field overflow".to_string(),
		});
	}
	Ok(())
}

/// A time of day rounded to the declared fractional digits. A carry out of the last second of the
/// day is `24:00:00`, the end of the day PostgreSQL's `time` holds, and `24:00:00` itself is kept.
fn round_time(declared: &DeclaredSqlType, time: Time) -> Time {
	let nanos = time.0;
	let sub = u32::try_from(nanos % u64::from(NANOS_PER_SECOND)).unwrap_or(0);
	let whole = nanos - u64::from(sub);
	let (rounded, carry) = declared.round_nanos(sub);
	let total = whole
		+ u64::from(rounded)
		+ if carry {
			u64::from(NANOS_PER_SECOND)
		} else {
			0
		};
	// The time is at most the end of the day, so a carry reaches the end of the day at most.
	Time(total.min(Time::NANOS_PER_DAY))
}

/// A timestamp rounded to the declared fractional digits, with a carry into the seconds.
/// `infinity` and `-infinity` are kept as they are where the dialect has them (PostgreSQL): they
/// are the ends of the range, not instants to round, and a carry out of `infinity` would leave the
/// range. MySQL has no temporal infinity, so there they are refused.
fn round_timestamp(
	declared: &DeclaredSqlType,
	timestamp: Timestamp,
) -> Result<Timestamp, TypeViolation> {
	if timestamp.is_infinite() {
		return keep_infinity(declared, timestamp);
	}
	if timestamp.nanos >= NANOS_PER_SECOND {
		return keep_leap_second(declared, timestamp);
	}
	let (nanos, carry) = declared.round_nanos(timestamp.nanos);
	let secs = if carry {
		timestamp.secs.checked_add(1).ok_or_else(datetime_overflow)?
	} else {
		timestamp.secs
	};
	Timestamp::from_parts(secs, nanos).ok_or_else(datetime_overflow)
}

/// A datetime rounded to the declared fractional digits, with a carry into the seconds. A value
/// that holds a leap second is left as it is, and so are `infinity` and `-infinity`, for the
/// reason [`round_timestamp`] gives.
fn round_datetime(
	declared: &DeclaredSqlType,
	datetime: Datetime,
) -> Result<Datetime, TypeViolation> {
	if datetime.is_infinite() {
		return keep_infinity(declared, datetime);
	}
	let value = datetime.0;
	if value.timestamp_subsec_nanos() >= NANOS_PER_SECOND {
		return keep_leap_second(declared, datetime);
	}
	let (nanos, carry) = declared.round_nanos(value.timestamp_subsec_nanos());
	let rounded = value.with_nanosecond(nanos).ok_or_else(datetime_overflow)?;
	let rounded = if carry {
		rounded.checked_add_signed(chrono::TimeDelta::seconds(1)).ok_or_else(datetime_overflow)?
	} else {
		rounded
	};
	Ok(Datetime(rounded))
}

/// An interval truncated toward zero below the finest field of the declared type's field
/// qualifier, as PostgreSQL's `AdjustIntervalForTypmod` does: `YEAR` keeps whole years of months,
/// `MONTH` and `YEAR TO MONTH` keep months, each dropping the days and the time; `DAY` drops the
/// time; a qualifier ending in `HOUR` or `MINUTE` truncates the time to whole hours or minutes.
/// A qualifier ending in `SECOND`, and an interval of every field, keep the value whole.
fn truncate_to_fields(declared: &DeclaredSqlType, interval: &mut crate::val::Interval) {
	const MICROS_PER_HOUR: i64 = 3_600_000_000;
	const MICROS_PER_MINUTE: i64 = 60_000_000;
	let Some(fields) = declared.interval_fields() else {
		return;
	};
	match fields.finest() {
		IntervalFinestField::Year => {
			interval.months = interval.months / 12 * 12;
			interval.days = 0;
			interval.micros = 0;
		}
		IntervalFinestField::Month => {
			interval.days = 0;
			interval.micros = 0;
		}
		IntervalFinestField::Day => interval.micros = 0,
		IntervalFinestField::Hour => {
			interval.micros = interval.micros / MICROS_PER_HOUR * MICROS_PER_HOUR;
		}
		IntervalFinestField::Minute => {
			interval.micros = interval.micros / MICROS_PER_MINUTE * MICROS_PER_MINUTE;
		}
		IntervalFinestField::Second => {}
	}
}

/// An interval's microseconds rounded to the declared fractional digits, half away from zero.
fn round_micros(declared: &DeclaredSqlType, micros: i64) -> Result<i64, TypeViolation> {
	let Some(digits) = declared.fractional_seconds() else {
		return Ok(micros);
	};
	if digits >= 6 {
		return Ok(micros);
	}
	let unit = 10i64.pow(6 - digits);
	let quotient = micros / unit;
	let remainder = micros % unit;
	let quotient = if remainder.abs() * 2 >= unit && remainder != 0 {
		quotient + micros.signum()
	} else {
		quotient
	};
	quotient.checked_mul(unit).ok_or_else(datetime_overflow)
}

/// Whether the declared type is MySQL's `TIME`, which holds an interval of whole days and time.
fn is_mysql_time(declared: &DeclaredSqlType) -> bool {
	declared.dialect() == SqlTypeDialect::MySql && declared.base() == "time"
}

/// The most microseconds a MySQL `TIME` holds either side of zero: `838:59:59`.
const MYSQL_TIME_MAX_MICROS: i128 = (838 * 3600 + 59 * 60 + 59) * 1_000_000;

/// Refuse an interval a MySQL `TIME` cannot hold: one with months, which have no fixed length, or
/// one beyond `-838:59:59` to `838:59:59`. MySQL refuses such a value in strict mode rather than
/// storing it, and a stored one would be read back clamped.
fn check_mysql_time(interval: &crate::val::Interval, column: &str) -> Result<(), TypeViolation> {
	let total = i128::from(interval.days) * 86_400_000_000 + i128::from(interval.micros);
	if interval.months == 0 && total.abs() <= MYSQL_TIME_MAX_MICROS {
		return Ok(());
	}
	Err(TypeViolation {
		code: "22007",
		message: format!("Incorrect time value: '{interval}' for column '{column}' at row 1"),
	})
}

/// A finite instant, `secs` and `nanos` from the Unix epoch (UTC), held to what a MySQL column
/// stores: `DATE` and `DATETIME` the years 0000 to 9999, and `TIMESTAMP` from `1970-01-01 00:00:01`
/// to `2038-01-19 03:14:07.999999` UTC. Other dialects and types are not limited here.
fn in_mysql_range(declared: &DeclaredSqlType, secs: i64, nanos: u32) -> Result<(), TypeViolation> {
	if declared.dialect() != SqlTypeDialect::MySql || !declared.array_dimensions().is_empty() {
		return Ok(());
	}
	let (first, last) = match declared.base() {
		"date" | "datetime" => (-62_167_219_200, 253_402_300_799),
		"timestamp" => (1, i64::from(i32::MAX)),
		_ => return Ok(()),
	};
	if (first..=last).contains(&secs) && (secs < last || nanos < NANOS_PER_SECOND) {
		Ok(())
	} else {
		Err(datetime_overflow())
	}
}

/// A value holding a leap second, kept as it is where the dialect admits a 60th second and
/// refused by MySQL, whose temporal fields hold seconds 0 to 59.
fn keep_leap_second<T>(declared: &DeclaredSqlType, value: T) -> Result<T, TypeViolation> {
	match declared.dialect() {
		SqlTypeDialect::MySql => Err(datetime_overflow()),
		SqlTypeDialect::PostgreSql | SqlTypeDialect::DynamoDb => Ok(value),
	}
}

/// An infinite temporal value, kept where the dialect has infinity and refused where it does not.
fn keep_infinity<T>(declared: &DeclaredSqlType, value: T) -> Result<T, TypeViolation> {
	match declared.dialect() {
		SqlTypeDialect::PostgreSql => Ok(value),
		SqlTypeDialect::MySql | SqlTypeDialect::DynamoDb => Err(datetime_overflow()),
	}
}

fn datetime_overflow() -> TypeViolation {
	TypeViolation {
		code: DATETIME_OVERFLOW,
		message: "date/time field value out of range".to_string(),
	}
}
