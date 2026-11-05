use std::str::FromStr;

use super::super::super::SqlTypeDialect as Dialect;
use super::super::validate;
use super::*;
use crate::val::{Interval, TableName, Time, Timestamp};

fn check(dialect: Dialect, declared: &str, value: Value) -> Result<Value, TypeViolation> {
	let declared = DeclaredSqlType::parse(dialect, declared).expect("a valid declared type");
	validate(&declared, value, "c")
}

fn text(value: &str) -> Value {
	Value::String(Strand::new(value))
}

fn decimal(value: &str) -> Value {
	Value::Number(Number::Decimal(Decimal::from_str(value).expect("a decimal")))
}

fn as_text(value: Value) -> String {
	let Value::String(text) = value else {
		panic!("expected a string, found {}", value.kind_of())
	};
	text.as_str().to_string()
}

fn as_decimal(value: &Value) -> String {
	let Value::Number(Number::Decimal(decimal)) = value else {
		panic!("expected a decimal, found {}", value.kind_of())
	};
	decimal.to_string()
}

#[test]
fn a_postgres_varchar_refuses_a_value_over_its_length() {
	let refused = check(Dialect::PostgreSql, "varchar(3)", text("abcd")).unwrap_err();
	assert_eq!(refused.code, "22001");
	assert_eq!(refused.message, "value too long for type character varying(3)");
	assert_eq!(as_text(check(Dialect::PostgreSql, "varchar(3)", text("abc")).unwrap()), "abc");
}

#[test]
fn a_postgres_char_drops_trailing_spaces_beyond_its_length() {
	let stored = check(Dialect::PostgreSql, "char(3)", text("ab   ")).unwrap();
	assert_eq!(as_text(stored), "ab");
	let refused = check(Dialect::PostgreSql, "char(3)", text("abcd")).unwrap_err();
	assert_eq!(refused.code, "22001");
}

#[test]
fn a_mysql_char_counts_characters_and_a_text_counts_bytes() {
	let refused = check(Dialect::MySql, "varchar(3)", text("abcd")).unwrap_err();
	assert_eq!(refused.code, "22001");
	assert!(refused.message.starts_with("Data too long for column 'c'"));
	// 200 characters of two bytes each: within a TINYTEXT of 255 characters, past its 255 bytes.
	let refused = check(Dialect::MySql, "tinytext", text(&"é".repeat(200))).unwrap_err();
	assert_eq!(refused.code, "22001");
}

#[test]
fn a_numeric_rounds_half_away_from_zero_to_its_scale() {
	let rounded = |value: &str| {
		as_decimal(&check(Dialect::PostgreSql, "numeric(5,2)", decimal(value)).unwrap())
	};
	assert_eq!(rounded("1.005"), "1.01");
	assert_eq!(rounded("-1.005"), "-1.01");
	assert_eq!(rounded("123.456"), "123.46");
	assert_eq!(rounded("7"), "7.00");
}

#[test]
fn a_numeric_refuses_integer_digits_beyond_its_precision() {
	// 999.999 rounds to 1000.00, which needs four integer digits under numeric(5,2).
	let refused = check(Dialect::PostgreSql, "numeric(5,2)", decimal("999.999")).unwrap_err();
	assert_eq!(refused.code, "22003");
	assert_eq!(refused.message, "numeric field overflow");
	let refused = check(Dialect::PostgreSql, "numeric(5)", decimal("100000")).unwrap_err();
	assert_eq!(refused.code, "22003");
	// MySQL names the column in its message.
	let refused = check(Dialect::MySql, "DECIMAL(5,2)", decimal("999.999")).unwrap_err();
	assert_eq!(refused.code, "22003");
	assert!(refused.message.starts_with("Out of range value for column '"), "{}", refused.message);
}

#[test]
fn an_unconstrained_numeric_is_left_as_it_is() {
	let stored = check(Dialect::PostgreSql, "numeric", decimal("1.23456")).unwrap();
	assert_eq!(as_decimal(&stored), "1.23456");
}

#[test]
fn a_timestamp_rounds_half_away_from_zero_with_a_carry_into_the_seconds() {
	let half_second = Timestamp {
		secs: 36_000,
		nanos: 500_000_000,
	};
	let stored = check(Dialect::PostgreSql, "timestamp(0)", Value::Timestamp(half_second)).unwrap();
	let Value::Timestamp(ts) = stored else {
		panic!("expected a timestamp, found {}", stored.kind_of())
	};
	assert_eq!((ts.secs, ts.nanos), (36_001, 0));
}

#[test]
fn a_time_rounds_up_to_the_end_of_the_day_and_keeps_it() {
	let as_nanos = |stored: Value| {
		let Value::Time(time) = stored else {
			panic!("expected a time, found {}", stored.kind_of())
		};
		time.0
	};
	let almost_midnight = Time(86_399 * 1_000_000_000 + 600_000_000);
	let stored = check(Dialect::PostgreSql, "time(0)", Value::Time(almost_midnight)).unwrap();
	assert_eq!(as_nanos(stored), Time::NANOS_PER_DAY);
	let end_of_day = Value::Time(Time(Time::NANOS_PER_DAY));
	assert_eq!(
		as_nanos(check(Dialect::PostgreSql, "time", end_of_day).unwrap()),
		Time::NANOS_PER_DAY
	);
}

#[test]
fn an_infinite_timestamp_is_not_rounded() {
	for infinite in [Timestamp::infinity(), Timestamp::neg_infinity()] {
		let stored = check(Dialect::PostgreSql, "timestamp", Value::Timestamp(infinite)).unwrap();
		assert_eq!(stored, Value::Timestamp(infinite));
		let stored =
			check(Dialect::PostgreSql, "timestamp(0)", Value::Timestamp(infinite)).unwrap();
		assert_eq!(stored, Value::Timestamp(infinite));
	}
}

#[test]
fn mysql_refuses_an_infinite_datetime() {
	for infinite in [Timestamp::infinity(), Timestamp::neg_infinity()] {
		let refused = check(Dialect::MySql, "datetime", Value::Timestamp(infinite)).unwrap_err();
		assert_eq!(refused.code, "22008");
	}
	for infinite in [Datetime::MAX_UTC, Datetime::MIN_UTC] {
		assert!(check(Dialect::MySql, "timestamp(3)", Value::Datetime(infinite)).is_err());
	}
}

#[test]
fn mysql_holds_datetime_and_timestamp_to_their_ranges() {
	let ts = |text: &str| Value::Timestamp(Timestamp::from_str(text).unwrap());
	let dt = |text: &str| Value::Datetime(Datetime::from_str(text).unwrap());
	assert!(check(Dialect::MySql, "datetime", ts("0500-01-01T00:00:00")).is_ok());
	assert!(check(Dialect::MySql, "datetime", ts("9999-12-31T23:59:59")).is_ok());
	assert_eq!(
		check(Dialect::MySql, "datetime", ts("+10000-01-01T00:00:00")).unwrap_err().code,
		"22008"
	);
	assert!(check(Dialect::MySql, "timestamp", dt("2038-01-19T03:14:07Z")).is_ok());
	assert!(check(Dialect::MySql, "timestamp", dt("2038-01-19T03:14:08Z")).is_err());
	assert!(check(Dialect::MySql, "timestamp", dt("1970-01-01T00:00:00Z")).is_err());
	assert!(check(Dialect::MySql, "timestamp(6)", dt("1970-01-01T00:00:01Z")).is_ok());
	assert!(check(Dialect::PostgreSql, "timestamptz", dt("2100-01-01T00:00:00Z")).is_ok());
}

#[test]
fn mysql_holds_a_date_to_the_finite_years_0000_to_9999() {
	let date = |text: &str| Value::Date(crate::val::Date::from_str(text).unwrap());
	assert!(check(Dialect::MySql, "date", date("0000-01-01")).is_ok());
	assert!(check(Dialect::MySql, "date", date("9999-12-31")).is_ok());
	for refused in ["+10000-01-01", "-0001-12-31", "infinity", "-infinity"] {
		assert_eq!(
			check(Dialect::MySql, "date", date(refused)).unwrap_err().code,
			"22008",
			"{refused}"
		);
	}
	assert_eq!(check(Dialect::PostgreSql, "date", date("infinity")).unwrap(), date("infinity"));
	assert!(check(Dialect::PostgreSql, "date", date("+10000-01-01")).is_ok());
}

#[test]
fn an_infinite_timestamptz_is_not_rounded() {
	for infinite in [Datetime::MAX_UTC, Datetime::MIN_UTC] {
		for declared in ["timestamptz", "timestamptz(0)"] {
			let stored = check(Dialect::PostgreSql, declared, Value::Datetime(infinite)).unwrap();
			assert_eq!(stored, Value::Datetime(infinite), "{declared}");
		}
	}
}

#[test]
fn a_mysql_time_refuses_an_interval_beyond_its_range() {
	let interval = |months: i32, days: i32, micros: i64| {
		Value::Interval(Interval {
			months,
			days,
			micros,
		})
	};
	let limit = (838 * 3600 + 59 * 60 + 59) * 1_000_000;
	assert!(check(Dialect::MySql, "time", interval(0, 0, limit)).is_ok());
	assert!(check(Dialect::MySql, "time", interval(0, 0, -limit)).is_ok());
	assert!(check(Dialect::MySql, "time", interval(0, 34, 0)).is_ok());
	let refused = check(Dialect::MySql, "time", interval(0, 0, limit + 1_000_000)).unwrap_err();
	assert_eq!(refused.code, "22007");
	assert!(refused.message.ends_with("for column 'c' at row 1"), "{}", refused.message);
	assert_eq!(check(Dialect::MySql, "time", interval(0, 35, 0)).unwrap_err().code, "22007");
	assert_eq!(check(Dialect::MySql, "time", interval(1, 0, 0)).unwrap_err().code, "22007");
	// A PostgreSQL interval holds any of them.
	assert!(check(Dialect::PostgreSql, "interval", interval(1, 35, 0)).is_ok());
}

#[test]
fn an_interval_rounds_its_microseconds_to_the_declared_digits() {
	let micros_after = |declared: &str, micros: i64| {
		let value = Value::Interval(Interval {
			months: 0,
			days: 0,
			micros,
		});
		let stored = check(Dialect::PostgreSql, declared, value).unwrap();
		let Value::Interval(interval) = stored else {
			panic!("expected an interval, found {}", stored.kind_of())
		};
		interval.micros
	};
	assert_eq!(micros_after("interval(2)", 1_234_567), 1_230_000);
	assert_eq!(micros_after("interval(2)", 1_235_000), 1_240_000);
	assert_eq!(micros_after("interval(2)", -1_235_000), -1_240_000);
}

#[test]
fn a_bit_string_is_held_to_its_length_and_its_digits() {
	assert_eq!(check(Dialect::PostgreSql, "bit(4)", text("101")).unwrap_err().code, "22026");
	assert_eq!(
		check(Dialect::PostgreSql, "bit varying(4)", text("10101")).unwrap_err().code,
		"22001"
	);
	assert_eq!(check(Dialect::PostgreSql, "bit(4)", text("10x1")).unwrap_err().code, "22P02");
	assert_eq!(as_text(check(Dialect::PostgreSql, "bit(4)", text("1010")).unwrap()), "1010");
}

#[test]
fn a_mysql_binary_is_padded_with_zero_bytes() {
	let stored = check(Dialect::MySql, "binary(4)", Value::Bytes(Bytes::from(vec![1, 2]))).unwrap();
	let Value::Bytes(bytes) = stored else {
		panic!("expected bytes, found {}", stored.kind_of())
	};
	assert_eq!(bytes.0.to_vec(), vec![1, 2, 0, 0]);
	let refused =
		check(Dialect::MySql, "binary(2)", Value::Bytes(Bytes::from(vec![1, 2, 3]))).unwrap_err();
	assert_eq!(refused.code, "22001");
}

#[test]
fn a_value_of_a_type_no_core_family_owns_is_stored_unchanged() {
	assert_eq!(as_text(check(Dialect::PostgreSql, "uuid", text("abc")).unwrap()), "abc");
	let table = Value::Table(TableName::from("t"));
	assert!(check(Dialect::PostgreSql, "varchar(3)", table).is_ok());
}

fn bigdecimal(value: &str) -> Value {
	Value::Number(Number::BigDecimal(Box::new(BigDecimal::from_str(value).expect("a bigdecimal"))))
}

fn as_bigdecimal(value: &Value) -> String {
	let Value::Number(Number::BigDecimal(big)) = value else {
		panic!("expected a bigdecimal, found {}", value.kind_of())
	};
	big.to_string()
}

#[test]
fn a_wide_numeric_rounds_a_bigdecimal_half_away_from_zero_to_its_scale() {
	let stored = |declared: &str, value: &str| {
		as_bigdecimal(&check(Dialect::PostgreSql, declared, bigdecimal(value)).unwrap())
	};
	assert_eq!(
		stored("numeric(40,10)", "123456789012345678901234567890.12345678905"),
		"123456789012345678901234567890.1234567891"
	);
	assert_eq!(
		stored("numeric(40,10)", "-123456789012345678901234567890.12345678905"),
		"-123456789012345678901234567890.1234567891"
	);
	assert_eq!(stored("numeric(40,10)", "1.5"), "1.5000000000");
	assert_eq!(stored("numeric(40,10)", "0.00000000004"), "0.0000000000");
	assert_eq!(
		as_bigdecimal(&check(Dialect::MySql, "decimal(65,30)", bigdecimal("1.5")).unwrap()),
		"1.500000000000000000000000000000"
	);
}

#[test]
fn a_wide_numeric_counts_the_integer_digits_of_a_bigdecimal_with_trailing_zeros() {
	// 31 integer digits with trailing zeros exceed the 30 that numeric(40,10) leaves.
	let refused =
		check(Dialect::PostgreSql, "numeric(40,10)", bigdecimal("1000000000000000000000000000000"))
			.unwrap_err();
	assert_eq!(refused.code, "22003");
	assert!(
		check(Dialect::PostgreSql, "numeric(40,10)", bigdecimal("100000000000000000000000000000"))
			.is_ok()
	);
	let refused = check(
		Dialect::PostgreSql,
		"numeric(40,10)",
		bigdecimal("999999999999999999999999999999.99999999995"),
	)
	.unwrap_err();
	assert_eq!(refused.code, "22003");
}

#[test]
fn a_negative_scale_rounds_to_a_power_of_ten() {
	let stored = |value: &str| {
		as_decimal(&check(Dialect::PostgreSql, "numeric(2,-3)", decimal(value)).unwrap())
	};
	assert_eq!(stored("1234"), "1000");
	assert_eq!(stored("1500"), "2000");
	assert_eq!(stored("-1500"), "-2000");
	assert_eq!(stored("99499"), "99000");
	let refused = check(Dialect::PostgreSql, "numeric(2,-3)", decimal("99500")).unwrap_err();
	assert_eq!(refused.code, "22003");
	let stored = check(
		Dialect::PostgreSql,
		"numeric(40,-3)",
		bigdecimal("1234567890123456789012345678901234"),
	)
	.unwrap();
	assert_eq!(as_bigdecimal(&stored), "1234567890123456789012345678901000");
}

#[test]
fn an_unconstrained_numeric_keeps_a_bigdecimal_as_written() {
	let stored = check(Dialect::PostgreSql, "numeric", bigdecimal("1.2300")).unwrap();
	assert_eq!(as_bigdecimal(&stored), "1.2300");
}

#[test]
fn a_dynamodb_number_holds_38_digits_and_its_exponent_range() {
	let n = |value: &str| check(Dialect::DynamoDb, "N", bigdecimal(value));
	assert!(n("12345678901234567890123456789012345678").is_ok());
	assert_eq!(n("123456789012345678901234567890123456789").unwrap_err().code, "22003");
	assert!(n("1e-130").is_ok());
	assert_eq!(n("1e-131").unwrap_err().code, "22003");
	assert!(n("9.9999999999999999999999999999999999999e125").is_ok());
	assert_eq!(n("1e126").unwrap_err().code, "22003");
	assert_eq!(n("-1e126").unwrap_err().code, "22003");
	assert!(n("0").is_ok());
	// Trailing zeros of an integer are not significant digits.
	assert!(n("1000000000000000000000000000000000000000000").is_ok());
}

#[test]
fn an_unsigned_mysql_decimal_refuses_a_negative_value() {
	let stored = check(Dialect::MySql, "decimal(10,2) unsigned", decimal("1.005")).unwrap();
	assert_eq!(stored, decimal("1.01"));
	let refused = check(Dialect::MySql, "decimal(10,2) unsigned", decimal("-1")).unwrap_err();
	assert_eq!(refused.code, "22003");
	assert_eq!(refused.message, "Out of range value for column 'c' at row 1");
	// A value that rounds to zero is zero.
	assert!(check(Dialect::MySql, "decimal(10,2) unsigned", decimal("-0.001")).is_ok());
	let wide = bigdecimal("-1234567890123456789012345678901234567890");
	assert!(check(Dialect::MySql, "decimal(65,0) unsigned", wide.clone()).is_err());
	assert!(check(Dialect::MySql, "decimal(65,0)", wide).is_ok());
	assert!(check(Dialect::MySql, "decimal(10,2)", decimal("-1")).is_ok());
}

#[test]
fn an_unsigned_mysql_float_refuses_a_negative_value() {
	let float = |f: f64| Value::Number(crate::val::Number::Float(f));
	let f32v = |f: f32| Value::Number(crate::val::Number::F32(f));
	assert_eq!(check(Dialect::MySql, "double unsigned", float(1.5)).unwrap(), float(1.5));
	assert_eq!(check(Dialect::MySql, "double unsigned", float(-1.5)).unwrap_err().code, "22003");
	assert_eq!(check(Dialect::MySql, "float unsigned", f32v(-0.5)).unwrap_err().code, "22003");
	assert!(check(Dialect::MySql, "float zerofill", f32v(-0.5)).is_err());
	assert!(check(Dialect::MySql, "decimal(10,2) zerofill", decimal("-1")).is_err());
	assert!(check(Dialect::MySql, "double", float(-1.5)).is_ok());
}

#[test]
fn a_mysql_float_refuses_nan_and_the_infinities() {
	let float = |f: f64| Value::Number(crate::val::Number::Float(f));
	let f32v = |f: f32| Value::Number(crate::val::Number::F32(f));
	for declared in ["double", "float", "double unsigned", "real", "decimal(10,2) unsigned"] {
		for refused in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			assert_eq!(
				check(Dialect::MySql, declared, float(refused)).unwrap_err().code,
				"22003",
				"{declared} {refused}"
			);
		}
	}
	assert!(check(Dialect::MySql, "float", f32v(f32::NAN)).is_err());
	assert_eq!(check(Dialect::MySql, "double", float(2.5)).unwrap(), float(2.5));
	// PostgreSQL floats hold NaN and the infinities.
	assert!(check(Dialect::PostgreSql, "float8", float(f64::NAN)).is_ok());
}

#[test]
fn mysql_refuses_a_leap_second() {
	let leap = chrono::NaiveDate::from_ymd_opt(2016, 12, 31)
		.unwrap()
		.and_hms_nano_opt(23, 59, 59, 1_500_000_000)
		.unwrap();
	let datetime = Value::Datetime(crate::val::Datetime(leap.and_utc()));
	assert_eq!(check(Dialect::MySql, "timestamp(6)", datetime.clone()).unwrap_err().code, "22008");
	assert_eq!(check(Dialect::PostgreSql, "timestamptz", datetime.clone()).unwrap(), datetime);
	let timestamp = Value::Timestamp(Timestamp::from(leap));
	assert_eq!(check(Dialect::MySql, "datetime", timestamp).unwrap_err().code, "22008");
}

#[test]
fn a_multidimensional_array_is_rectangular() {
	let array = |items: Vec<Value>| Value::Array(crate::val::Array(items));
	let int = |n: i64| Value::Number(crate::val::Number::Int(n));
	let square = array(vec![array(vec![int(1), int(2)]), array(vec![int(3), int(4)])]);
	assert_eq!(check(Dialect::PostgreSql, "int8[][]", square.clone()).unwrap(), square);
	assert!(check(Dialect::PostgreSql, "int8[][]", array(vec![])).is_ok());
	for ragged in [
		array(vec![array(vec![int(1), int(2)]), array(vec![int(3)])]),
		array(vec![array(vec![int(1)]), int(2)]),
		array(vec![array(vec![]), array(vec![])]),
	] {
		let refused = check(Dialect::PostgreSql, "int8[][]", ragged).unwrap_err();
		assert_eq!(refused.code, "2202E");
	}
	// A one-dimensional type does not judge the shape of what its elements hold.
	assert!(check(Dialect::PostgreSql, "int8[]", array(vec![int(1), int(2)])).is_ok());
}

#[test]
fn an_array_type_checks_each_element_at_every_dimension() {
	let array = |items: Vec<Value>| Value::Array(crate::val::Array(items));
	let stored =
		check(Dialect::PostgreSql, "numeric(5,2)[]", array(vec![decimal("1.005"), Value::Null]))
			.unwrap();
	assert_eq!(stored, array(vec![decimal("1.01"), Value::Null]));
	let refused =
		check(Dialect::PostgreSql, "numeric(5,2)[]", array(vec![decimal("1000")])).unwrap_err();
	assert_eq!(refused.code, "22003");
	let refused =
		check(Dialect::PostgreSql, "varchar(3)[][]", array(vec![array(vec![text("abcd")])]))
			.unwrap_err();
	assert_eq!(refused.code, "22001");
	let point = Value::Geometry(crate::val::Geometry::Point((1.0, 2.0).into()));
	let stored = check(Dialect::PostgreSql, "point[]", array(vec![point.clone()])).unwrap();
	assert_eq!(stored, array(vec![point]));
}

#[test]
fn an_interval_is_truncated_below_the_finest_field_of_its_qualifier() {
	// 1 year 2 months 3 days 04:05:06.789, and its negation.
	let micros = ((4 * 60 + 5) * 60 + 6) * 1_000_000 + 789_000;
	let stored = |declared: &str, sign: i32| {
		let value = Value::Interval(Interval::new(14 * sign, 3 * sign, micros * i64::from(sign)));
		let Value::Interval(interval) = check(Dialect::PostgreSql, declared, value).unwrap() else {
			panic!("expected an interval");
		};
		(interval.months, interval.days, interval.micros)
	};
	let hours = 4 * 3_600_000_000;
	let minutes = hours + 5 * 60_000_000;
	let seconds = minutes + 6_000_000;
	for sign in [1, -1] {
		let s = i64::from(sign);
		assert_eq!(stored("interval", sign), (14 * sign, 3 * sign, micros * s));
		assert_eq!(stored("interval year", sign), (12 * sign, 0, 0));
		assert_eq!(stored("interval month", sign), (14 * sign, 0, 0));
		assert_eq!(stored("interval year to month", sign), (14 * sign, 0, 0));
		assert_eq!(stored("interval day", sign), (14 * sign, 3 * sign, 0));
		assert_eq!(stored("interval hour", sign), (14 * sign, 3 * sign, hours * s));
		assert_eq!(stored("interval day to hour", sign), (14 * sign, 3 * sign, hours * s));
		assert_eq!(stored("interval minute", sign), (14 * sign, 3 * sign, minutes * s));
		assert_eq!(stored("interval day to minute", sign), (14 * sign, 3 * sign, minutes * s));
		assert_eq!(stored("interval hour to minute", sign), (14 * sign, 3 * sign, minutes * s));
		assert_eq!(stored("interval second", sign), (14 * sign, 3 * sign, micros * s));
		assert_eq!(stored("interval day to second", sign), (14 * sign, 3 * sign, micros * s));
		assert_eq!(
			stored("interval hour to second(1)", sign),
			(14 * sign, 3 * sign, (seconds + 800_000) * s)
		);
		assert_eq!(
			stored("interval minute to second(0)", sign),
			(14 * sign, 3 * sign, (seconds + 1_000_000) * s)
		);
		assert_eq!(
			stored("interval day to second(2)", sign),
			(14 * sign, 3 * sign, (seconds + 790_000) * s)
		);
	}
	// A year keeps only the whole years of its months, truncated toward zero.
	assert_eq!(stored("interval year", 1).0, 12);
	let value = Value::Interval(Interval::new(-23, 0, 0));
	let Value::Interval(interval) = check(Dialect::PostgreSql, "interval year", value).unwrap()
	else {
		panic!("expected an interval");
	};
	assert_eq!(interval.months, -12);
}

#[test]
fn a_numeric_scale_beyond_its_precision_bounds_the_magnitude() {
	// numeric(2,5) holds magnitudes below 10^-3 and rounds to five fractional digits.
	assert_eq!(
		as_decimal(&check(Dialect::PostgreSql, "numeric(2,5)", decimal("0.00099")).unwrap()),
		"0.00099"
	);
	assert_eq!(
		as_decimal(&check(Dialect::PostgreSql, "numeric(2,5)", decimal("-0.000994")).unwrap()),
		"-0.00099"
	);
	assert!(check(Dialect::PostgreSql, "numeric(2,5)", decimal("0")).is_ok());
	for value in ["0.001", "0.0009951", "1", "-0.5"] {
		let refused = check(Dialect::PostgreSql, "numeric(2,5)", decimal(value)).unwrap_err();
		assert_eq!(refused.code, "22003", "{value}");
	}
	assert!(check(Dialect::PostgreSql, "numeric(2,5)", bigdecimal("0.00099")).is_ok());
	let refused = check(Dialect::PostgreSql, "numeric(2,5)", bigdecimal("0.001")).unwrap_err();
	assert_eq!(refused.code, "22003");
}

#[test]
fn a_mysql_float_with_a_width_and_decimals_rounds_and_bounds_its_magnitude() {
	let float = |f: f64| Value::Number(crate::val::Number::Float(f));
	assert_eq!(check(Dialect::MySql, "double(7,4)", float(1.234_56)).unwrap(), float(1.2346));
	assert_eq!(check(Dialect::MySql, "double(7,4)", float(-999.999_94)).unwrap(), float(-999.9999));
	for refused in [1000.0, 999.999_96, -1000.5] {
		let error = check(Dialect::MySql, "double(7,4)", float(refused)).unwrap_err();
		assert_eq!(error.code, "22003", "{refused}");
	}
	// A float with no decimals written is left as it is.
	assert_eq!(check(Dialect::MySql, "double", float(1000.123456)).unwrap(), float(1000.123456));
}
