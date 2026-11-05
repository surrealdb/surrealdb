//! Number-shaped SQL types: PostgreSQL `money`, PostgreSQL `pg_lsn`, MySQL `YEAR`, MySQL
//! `BIT(n)`, and MySQL `MEDIUMINT`. Each is stored as an existing number kind (`decimal`, `u64`,
//! `u16`, `u64`, `i32` or `u32`), so the kind does not say what the declared type admits. This
//! family decides the scale, the range, and the bit width, and canonicalises the value it stores.

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};

use super::super::SqlTypeDialect;
use super::super::declared_type::{DeclaredSqlType, TypeViolation};
use super::Validated;
use crate::val::Value;
use crate::val::number::Number;

/// SQLSTATE `22003`, numeric value out of range.
const NUMERIC_OUT_OF_RANGE: &str = "22003";
/// SQLSTATE `22001`, string data right truncation. MySQL reports a `BIT` value that is too wide
/// with this state, as `ER_DATA_TOO_LONG`.
const DATA_TOO_LONG: &str = "22001";
/// SQLSTATE `22023`, invalid parameter value.
const INVALID_PARAMETER: &str = "22023";

/// Validate a value of a number-shaped type. The family owns `money` and `pg_lsn` (PostgreSQL),
/// and `year` and `bit` (MySQL). It declines every other declared type, including arrays. NULL and
/// NONE, an absent value, are stored as they are.
pub fn validate(declared: &DeclaredSqlType, value: Value, column: &str) -> Validated {
	if !declared.array_dimensions().is_empty() {
		return Validated::Declined(value);
	}
	let owned = matches!(
		(declared.dialect(), declared.base()),
		(SqlTypeDialect::PostgreSql, "money" | "pg_lsn") | (SqlTypeDialect::MySql, "year" | "bit")
	);
	if owned && matches!(value, Value::None | Value::Null) {
		return Validated::Decided(Ok(value));
	}
	match (declared.dialect(), declared.base()) {
		(SqlTypeDialect::PostgreSql, "money") => Validated::Decided(money(&value, column)),
		(SqlTypeDialect::PostgreSql, "pg_lsn") => Validated::Decided(pg_lsn(&value, column)),
		(SqlTypeDialect::MySql, "year") => Validated::Decided(year(&value, column)),
		(SqlTypeDialect::MySql, "bit") => Validated::Decided(bit(declared, &value, column)),
		(SqlTypeDialect::MySql, "mediumint" | "middleint" | "int3") => match integer_of(&value) {
			Some(n) => Validated::Decided(mediumint(declared, n, column).map(|()| value)),
			None => Validated::Declined(value),
		},
		_ => Validated::Declined(value),
	}
}

/// MySQL `MEDIUMINT` is 24 bits wide: -8388608 to 8388607, or 0 to 16777215 when `UNSIGNED`
/// (which `ZEROFILL` implies). Its kind is the 32-bit integer of the same signedness.
fn mediumint(declared: &DeclaredSqlType, n: i128, column: &str) -> Result<(), TypeViolation> {
	let unsigned =
		declared.attributes().iter().any(|word| word == "unsigned" || word == "zerofill");
	let range = if unsigned {
		0..=0xFF_FFFF
	} else {
		-0x80_0000..=0x7F_FFFF
	};
	if range.contains(&n) {
		Ok(())
	} else {
		Err(out_of_range(column, "mediumint"))
	}
}

/// `money` keeps two fractional digits, rounded half away from zero. The stored amount must
/// fit the cents of PostgreSQL's `int8` representation, so `|cents| <= i64::MAX`.
fn money(value: &Value, column: &str) -> Result<Value, TypeViolation> {
	let Some(amount) = decimal_of(value) else {
		return Err(out_of_range(column, "money"));
	};
	let mut rounded = amount.round_dp_with_strategy(2, RoundingStrategy::MidpointAwayFromZero);
	rounded.rescale(2);
	match rounded.checked_mul(Decimal::from(100)).and_then(|cents| cents.to_i64()) {
		Some(_) => Ok(Value::Number(Number::Decimal(rounded))),
		None => Err(out_of_range(column, "money")),
	}
}

/// `pg_lsn` is an unsigned 64-bit position; the value is stored as its number.
fn pg_lsn(value: &Value, column: &str) -> Result<Value, TypeViolation> {
	match integer_of(value).and_then(|n| u64::try_from(n).ok()) {
		Some(lsn) => Ok(Value::Number(Number::U64(lsn))),
		None => Err(out_of_range(column, "pg_lsn")),
	}
}

/// MySQL `YEAR` holds 0 and 1901 to 2155. An integer from 1 to 99 is a two-digit year: 1 to 69
/// are 2001 to 2069 and 70 to 99 are 1970 to 1999, as MySQL reads them. Text of up to four digits
/// is read the same way, except that a zero of one or two digits (`'0'`, `'00'`) is 2000, where
/// the number 0 and `'0000'` are the year 0.
fn year(value: &Value, column: &str) -> Result<Value, TypeViolation> {
	let (n, two_digit_text) = if let Value::String(text) = value {
		let digits = text.trim();
		if digits.is_empty() || digits.len() > 4 || !digits.bytes().all(|b| b.is_ascii_digit()) {
			return Err(out_of_range(column, "year"));
		}
		(digits.parse::<i128>().map_err(|_| out_of_range(column, "year"))?, digits.len() <= 2)
	} else {
		match integer_of(value) {
			Some(n) => (n, false),
			None => return Err(out_of_range(column, "year")),
		}
	};
	let year = match n {
		0 if two_digit_text => 2000,
		0 => 0,
		1..=69 => n + 2000,
		70..=99 => n + 1900,
		1901..=2155 => n,
		_ => return Err(out_of_range(column, "year")),
	};
	match u16::try_from(year) {
		Ok(year) => Ok(Value::Number(Number::U16(year))),
		Err(_) => Err(out_of_range(column, "year")),
	}
}

/// MySQL `BIT(n)` holds an unsigned value of `n` bits, for `n` from 1 to 64; `BIT` alone is
/// `BIT(1)`. A value that needs more bits is refused.
fn bit(declared: &DeclaredSqlType, value: &Value, column: &str) -> Result<Value, TypeViolation> {
	let width = match declared.modifiers().first() {
		None => 1,
		Some(text) => match text.parse::<u32>() {
			Ok(width) => width,
			Err(_) => {
				return Err(TypeViolation {
					code: INVALID_PARAMETER,
					message: format!("BIT width \"{text}\" is not a number"),
				});
			}
		},
	};
	if !(1..=64).contains(&width) {
		return Err(TypeViolation {
			code: INVALID_PARAMETER,
			message: format!("BIT width {width} is outside 1 to 64"),
		});
	}
	let Some(bits) = integer_of(value).and_then(|n| u64::try_from(n).ok()) else {
		return Err(out_of_range(column, "bit"));
	};
	if u128::from(bits) >= (1u128 << width) {
		return Err(TypeViolation {
			code: DATA_TOO_LONG,
			message: format!("Data too long for column '{column}' at row 1"),
		});
	}
	Ok(Value::Number(Number::U64(bits)))
}

/// The integer a number value holds. A decimal counts only when it has no fractional part.
fn integer_of(value: &Value) -> Option<i128> {
	let Value::Number(number) = value else {
		return None;
	};
	match number {
		Number::Int(n) => Some(i128::from(*n)),
		Number::I8(n) => Some(i128::from(*n)),
		Number::U8(n) => Some(i128::from(*n)),
		Number::I16(n) => Some(i128::from(*n)),
		Number::U16(n) => Some(i128::from(*n)),
		Number::I32(n) => Some(i128::from(*n)),
		Number::U32(n) => Some(i128::from(*n)),
		Number::U64(n) => Some(i128::from(*n)),
		Number::Decimal(d) if d.is_integer() => d.to_i128(),
		Number::BigDecimal(b) => b.to_i64().map(i128::from).or_else(|| b.to_u64().map(i128::from)),
		Number::Decimal(_) | Number::Float(_) | Number::F16(_) | Number::F32(_) => None,
	}
}

/// The exact decimal a number value holds. A float has no exact decimal here and is refused.
fn decimal_of(value: &Value) -> Option<Decimal> {
	let Value::Number(number) = value else {
		return None;
	};
	match number {
		Number::Decimal(d) => Some(*d),
		Number::BigDecimal(b) => Decimal::try_from(b.as_ref()).ok(),
		Number::Float(_) | Number::F16(_) | Number::F32(_) => None,
		Number::Int(_)
		| Number::I8(_)
		| Number::U8(_)
		| Number::I16(_)
		| Number::U16(_)
		| Number::I32(_)
		| Number::U32(_)
		| Number::U64(_) => integer_of(value).and_then(Decimal::from_i128),
	}
}

fn out_of_range(column: &str, type_name: &str) -> TypeViolation {
	TypeViolation {
		code: NUMERIC_OUT_OF_RANGE,
		message: format!("the value of column \"{column}\" is out of range for {type_name}"),
	}
}

#[cfg(test)]
mod tests {
	use std::str::FromStr;

	use super::*;

	fn run(dialect: SqlTypeDialect, text: &str, value: Value) -> Result<Value, TypeViolation> {
		let declared = DeclaredSqlType::parse(dialect, text).expect("declared type parses");
		match validate(&declared, value, "c") {
			Validated::Decided(result) => result,
			Validated::Declined(_) => panic!("the family declined {text}"),
		}
	}

	fn dec(text: &str) -> Value {
		Value::Number(Number::Decimal(Decimal::from_str(text).expect("decimal literal")))
	}

	fn int(n: i64) -> Value {
		Value::Number(Number::Int(n))
	}

	fn money_of(text: &str) -> String {
		match run(SqlTypeDialect::PostgreSql, "money", dec(text)) {
			Ok(Value::Number(Number::Decimal(d))) => d.to_string(),
			other => panic!("money {text} gave {other:?}"),
		}
	}

	#[test]
	fn an_absent_or_null_value_is_stored_as_it_is() {
		for (dialect, base) in [
			(SqlTypeDialect::PostgreSql, "money"),
			(SqlTypeDialect::PostgreSql, "pg_lsn"),
			(SqlTypeDialect::MySql, "year"),
			(SqlTypeDialect::MySql, "bit(4)"),
		] {
			assert_eq!(run(dialect, base, Value::None).unwrap(), Value::None, "{base}");
			assert_eq!(run(dialect, base, Value::Null).unwrap(), Value::Null, "{base}");
		}
	}

	#[test]
	fn mediumint_holds_24_bits() {
		let int = |n: i64| Value::Number(Number::Int(n));
		for (declared, low, high) in [
			("mediumint", -8_388_608, 8_388_607),
			("mediumint unsigned", 0, 16_777_215),
			("int3 zerofill", 0, 16_777_215),
		] {
			assert!(run(SqlTypeDialect::MySql, declared, int(low)).is_ok(), "{declared}");
			assert!(run(SqlTypeDialect::MySql, declared, int(high)).is_ok(), "{declared}");
			assert!(run(SqlTypeDialect::MySql, declared, int(low - 1)).is_err(), "{declared}");
			assert!(run(SqlTypeDialect::MySql, declared, int(high + 1)).is_err(), "{declared}");
		}
		let int_type = DeclaredSqlType::parse(SqlTypeDialect::MySql, "int").unwrap();
		assert!(matches!(validate(&int_type, int(8_388_608), "c"), Validated::Declined(_)));
	}

	#[test]
	fn money_rounds_to_cents_half_away_from_zero() {
		assert_eq!(money_of("1234.5"), "1234.50");
		assert_eq!(money_of("0.005"), "0.01");
		assert_eq!(money_of("-0.005"), "-0.01");
		assert_eq!(money_of("1.004"), "1.00");
		assert_eq!(money_of("2.675"), "2.68");
		assert_eq!(money_of("7"), "7.00");
	}

	#[test]
	fn money_refuses_beyond_the_int8_cents_range() {
		assert_eq!(money_of("92233720368547758.07"), "92233720368547758.07");
		assert_eq!(money_of("-92233720368547758.07"), "-92233720368547758.07");
		let high = run(SqlTypeDialect::PostgreSql, "money", dec("92233720368547758.08"));
		assert_eq!(high.unwrap_err().code, NUMERIC_OUT_OF_RANGE);
		let huge = run(SqlTypeDialect::PostgreSql, "money", dec("10000000000000000000000000000"));
		assert_eq!(huge.unwrap_err().code, NUMERIC_OUT_OF_RANGE);
	}

	#[test]
	fn money_refuses_a_float_it_cannot_hold_exactly() {
		let float = Value::Number(Number::Float(1.5));
		let error = run(SqlTypeDialect::PostgreSql, "money", float).unwrap_err();
		assert_eq!(error.code, NUMERIC_OUT_OF_RANGE);
	}

	#[test]
	fn pg_lsn_accepts_its_number_and_refuses_a_negative_one() {
		let lsn = run(SqlTypeDialect::PostgreSql, "pg_lsn", int(0x16B_3748));
		assert_eq!(lsn, Ok(Value::Number(Number::U64(0x16B_3748))));
		let max = run(SqlTypeDialect::PostgreSql, "pg_lsn", Value::Number(Number::U64(u64::MAX)));
		assert_eq!(max, Ok(Value::Number(Number::U64(u64::MAX))));
		let negative = run(SqlTypeDialect::PostgreSql, "pg_lsn", int(-1));
		assert_eq!(negative.unwrap_err().code, NUMERIC_OUT_OF_RANGE);
	}

	#[test]
	fn year_accepts_zero_and_the_four_digit_range() {
		let year = |n| run(SqlTypeDialect::MySql, "year", int(n));
		assert_eq!(year(0), Ok(Value::Number(Number::U16(0))));
		assert_eq!(year(1901), Ok(Value::Number(Number::U16(1901))));
		assert_eq!(year(2155), Ok(Value::Number(Number::U16(2155))));
		assert_eq!(year(2024), Ok(Value::Number(Number::U16(2024))));
		assert_eq!(year(1900).unwrap_err().code, NUMERIC_OUT_OF_RANGE);
		assert_eq!(year(2156).unwrap_err().code, NUMERIC_OUT_OF_RANGE);
		assert_eq!(year(100).unwrap_err().code, NUMERIC_OUT_OF_RANGE);
		assert_eq!(year(-1).unwrap_err().code, NUMERIC_OUT_OF_RANGE);
	}

	#[test]
	fn year_reads_text_as_mysql_does() {
		let year = |text: &str| run(SqlTypeDialect::MySql, "year", Value::String(text.into()));
		let stored = |n| Ok(Value::Number(Number::U16(n)));
		assert_eq!(year("0"), stored(2000));
		assert_eq!(year("00"), stored(2000));
		assert_eq!(year("0000"), stored(0));
		assert_eq!(year("5"), stored(2005));
		assert_eq!(year("99"), stored(1999));
		assert_eq!(year("2024"), stored(2024));
		assert_eq!(year("abc").unwrap_err().code, NUMERIC_OUT_OF_RANGE);
		assert_eq!(year("20245").unwrap_err().code, NUMERIC_OUT_OF_RANGE);
	}

	#[test]
	fn year_maps_two_digit_years_as_mysql_does() {
		let year = |n| run(SqlTypeDialect::MySql, "year", int(n));
		assert_eq!(year(1), Ok(Value::Number(Number::U16(2001))));
		assert_eq!(year(69), Ok(Value::Number(Number::U16(2069))));
		assert_eq!(year(70), Ok(Value::Number(Number::U16(1970))));
		assert_eq!(year(99), Ok(Value::Number(Number::U16(1999))));
	}

	#[test]
	fn bit_refuses_a_value_that_needs_more_bits() {
		let bit = |text, n| run(SqlTypeDialect::MySql, text, Value::Number(Number::U64(n)));
		assert_eq!(bit("bit(8)", 255), Ok(Value::Number(Number::U64(255))));
		assert_eq!(bit("bit(8)", 256).unwrap_err().code, DATA_TOO_LONG);
		assert_eq!(bit("bit", 1), Ok(Value::Number(Number::U64(1))));
		assert_eq!(bit("bit", 2).unwrap_err().code, DATA_TOO_LONG);
		assert_eq!(bit("bit(64)", u64::MAX), Ok(Value::Number(Number::U64(u64::MAX))));
		assert_eq!(bit("bit(63)", u64::MAX).unwrap_err().code, DATA_TOO_LONG);
	}

	#[test]
	fn bit_refuses_a_width_outside_one_to_sixty_four() {
		// The declaration itself is refused, so no value ever reaches a width outside the range.
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "bit(0)").is_err());
		assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, "bit(65)").is_err());
	}

	#[test]
	fn other_declared_types_are_declined() {
		let declined = |dialect, text: &str| {
			let declared = DeclaredSqlType::parse(dialect, text).expect("declared type parses");
			matches!(validate(&declared, int(1), "c"), Validated::Declined(_))
		};
		assert!(declined(SqlTypeDialect::PostgreSql, "numeric(10,2)"));
		assert!(declined(SqlTypeDialect::PostgreSql, "money[]"));
		assert!(declined(SqlTypeDialect::PostgreSql, "oid"));
		assert!(declined(SqlTypeDialect::PostgreSql, "xid8"));
		assert!(declined(SqlTypeDialect::MySql, "year[]"));
		assert!(declined(SqlTypeDialect::MySql, "decimal(10,2)"));
		assert!(declined(SqlTypeDialect::DynamoDb, "money"));
		assert!(declined(SqlTypeDialect::MySql, "money"));
	}
}
