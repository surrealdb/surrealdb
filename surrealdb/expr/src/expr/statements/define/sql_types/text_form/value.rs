//! The text PostgreSQL's cast to `text` gives a value, by the value's own kind: a date, time,
//! timestamp or interval in PostgreSQL's default output style, a number in positional notation
//! with its scale, a boolean as `true` or `false`, and a range as its bracketed text.
//!
//! A value whose kind is shared by several declared types (a string holding an `inet`, an object
//! holding a `circle`) has the text of its declared type, which [`super::write`] and the
//! text-shaped families give; this module answers for the value alone.

use std::fmt::Write as _;

use super::super::range::Element;
use super::{write_element, write_range};
use crate::val::{Interval, Number, Range, Value};

/// The PostgreSQL text of `value`, or `None` for a value with no PostgreSQL text of its own:
/// `NONE`, `NULL`, and the kinds no PostgreSQL type maps to.
pub fn postgres_text(value: &Value) -> Option<String> {
	match value {
		Value::String(text) => Some(text.as_str().to_owned()),
		Value::Bool(truth) => Some(
			if *truth {
				"true"
			} else {
				"false"
			}
			.to_owned(),
		),
		Value::Number(number) => Some(number_text(number)),
		Value::Date(_) => write_element(Element::Date, value),
		Value::Timestamp(_) => write_element(Element::Timestamp, value),
		Value::Datetime(_) => write_element(Element::Timestamptz, value),
		Value::Time(time) => Some(clock_text(time.nanos() / 1_000)),
		Value::Interval(interval) => Some(interval_text(interval)),
		Value::Uuid(uuid) => Some(uuid.to_string()),
		Value::Range(range) => range_text(range),
		Value::None
		| Value::Null
		| Value::Duration(_)
		| Value::Bytes(_)
		| Value::Array(_)
		| Value::Object(_)
		| Value::Set(_)
		| Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Closure(_) => None,
	}
}

/// A number in positional notation. A decimal keeps its scale (`1.50`), and a float is written as
/// PostgreSQL writes a `double precision`, with `NaN` and `Infinity` spelled out.
fn number_text(number: &Number) -> String {
	// A float is written at its own precision: the special values by name, any other value by
	// `written`, its shortest text at that precision, so a `real` 0.1 is `0.1`.
	let float = |value: f64, written: String| {
		if value.is_nan() {
			"NaN".to_owned()
		} else if value == f64::INFINITY {
			"Infinity".to_owned()
		} else if value == f64::NEG_INFINITY {
			"-Infinity".to_owned()
		} else {
			written
		}
	};
	match number {
		Number::Int(n) => n.to_string(),
		Number::I8(n) => n.to_string(),
		Number::U8(n) => n.to_string(),
		Number::I16(n) => n.to_string(),
		Number::U16(n) => n.to_string(),
		Number::I32(n) => n.to_string(),
		Number::U32(n) => n.to_string(),
		Number::U64(n) => n.to_string(),
		Number::Float(n) => float(*n, n.to_string()),
		Number::F32(n) => float(f64::from(*n), n.to_string()),
		Number::F16(n) => float(f64::from(*n), n.to_string()),
		Number::Decimal(n) => n.to_string(),
		Number::BigDecimal(n) => n.to_string(),
	}
}

/// A count of microseconds as `HH:MM:SS`, then the fraction without trailing zeros when there is
/// one. The hours are not wrapped, so a time of `24:00:00` is written as such.
fn clock_text(micros: u64) -> String {
	let secs = micros / 1_000_000;
	let mut out = format!("{:02}:{:02}:{:02}", secs / 3600, secs / 60 % 60, secs % 60);
	push_fraction(&mut out, micros % 1_000_000);
	out
}

fn push_fraction(out: &mut String, micros: u64) {
	if micros != 0 {
		let fraction = format!("{micros:06}");
		out.push('.');
		out.push_str(fraction.trim_end_matches('0'));
	}
}

/// An interval in the `postgres` output style, `1 year 2 mons 3 days 04:05:06.5`: each non-zero
/// field of the date part, then the time part, with a `+` on a positive field that follows a
/// negative one, and `00:00:00` for the zero interval.
fn interval_text(interval: &Interval) -> String {
	let (years, mons) = (interval.months / 12, interval.months % 12);
	let mut out = String::new();
	let mut is_zero = true;
	let mut is_before = false;
	for (value, unit) in
		[(i64::from(years), "year"), (i64::from(mons), "mon"), (i64::from(interval.days), "day")]
	{
		if value == 0 {
			continue;
		}
		if !is_zero {
			out.push(' ');
		}
		if is_before && value > 0 {
			out.push('+');
		}
		let plural = if value == 1 {
			""
		} else {
			"s"
		};
		let _ = write!(out, "{value} {unit}{plural}");
		is_before = value < 0;
		is_zero = false;
	}
	let micros = interval.micros;
	if is_zero || micros != 0 {
		if !is_zero {
			out.push(' ');
		}
		if micros < 0 {
			out.push('-');
		} else if is_before {
			out.push('+');
		}
		out.push_str(&clock_text(micros.unsigned_abs()));
	}
	out
}

/// A range in its bracketed text, its element kind read from its bounds. `None` for a range whose
/// bounds are of no range element kind.
fn range_text(range: &Range) -> Option<String> {
	let bound = [&range.start, &range.end].into_iter().find_map(|bound| match bound {
		std::ops::Bound::Included(value) | std::ops::Bound::Excluded(value) => Some(value),
		std::ops::Bound::Unbounded => None,
	});
	let element = match bound {
		None => Element::Int8,
		Some(Value::Number(Number::I32(_))) => Element::Int4,
		Some(Value::Number(
			Number::Int(_)
			| Number::I8(_)
			| Number::U8(_)
			| Number::I16(_)
			| Number::U16(_)
			| Number::U32(_)
			| Number::U64(_),
		)) => Element::Int8,
		Some(Value::Number(_)) => Element::Numeric,
		Some(Value::Timestamp(_)) => Element::Timestamp,
		Some(Value::Datetime(_)) => Element::Timestamptz,
		Some(Value::Date(_)) => Element::Date,
		Some(_) => return None,
	};
	write_range(element, range)
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;
	use std::str::FromStr;

	use rust_decimal::Decimal;

	use super::*;
	use crate::val::{Date, Time, Timestamp};

	fn text(value: &Value) -> String {
		postgres_text(value).expect("a text")
	}

	#[test]
	fn a_real_is_written_at_its_own_precision() {
		assert_eq!(text(&Value::Number(Number::F32(0.1))), "0.1");
		assert_eq!(text(&Value::Number(Number::F32(f32::NAN))), "NaN");
		assert_eq!(text(&Value::Number(Number::Float(0.1))), "0.1");
	}

	#[test]
	fn temporal_values_take_the_default_output_style() {
		assert_eq!(text(&Value::Date(Date::from_str("2026-10-27").unwrap())), "2026-10-27");
		assert_eq!(
			text(&Value::Timestamp(Timestamp::from_str("2026-10-27T12:30:00.25").unwrap())),
			"2026-10-27 12:30:00.25"
		);
		assert_eq!(text(&Value::Time(Time::from_str("12:30:05.5").unwrap())), "12:30:05.5");
		assert_eq!(
			text(&Value::Interval(Interval::new(14, 3, 3_600_000_000))),
			"1 year 2 mons 3 days 01:00:00"
		);
		assert_eq!(text(&Value::Interval(Interval::new(0, -1, 5_000_000))), "-1 days +00:00:05");
		assert_eq!(text(&Value::Interval(Interval::new(0, 0, 0))), "00:00:00");
	}

	#[test]
	fn numbers_keep_their_scale() {
		assert_eq!(
			text(&Value::Number(Number::Decimal(Decimal::from_str("1.50").unwrap()))),
			"1.50"
		);
		assert_eq!(text(&Value::Number(Number::Float(f64::NAN))), "NaN");
		assert_eq!(text(&Value::Number(Number::Int(-7))), "-7");
		assert_eq!(text(&Value::Bool(true)), "true");
	}

	#[test]
	fn a_range_takes_its_bracketed_text() {
		let range = Range {
			start: Bound::Included(Value::Number(Number::I32(1))),
			end: Bound::Excluded(Value::Number(Number::I32(5))),
		};
		assert_eq!(text(&Value::Range(Box::new(range))), "[1,5)");
		let empty = Range {
			start: Bound::Included(Value::Number(Number::Int(3))),
			end: Bound::Excluded(Value::Number(Number::Int(3))),
		};
		assert_eq!(text(&Value::Range(Box::new(empty))), "empty");
	}
}
