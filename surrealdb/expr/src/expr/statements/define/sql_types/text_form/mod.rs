//! The PostgreSQL text forms of the declared types whose kind is not a string: `money`, `pg_lsn`,
//! the geometric types, and the ranges and multiranges.
//!
//! A value of one of these types is stored as its kind (a decimal, a `u64`, a geometry, an object
//! of a fixed shape, a range, or an array of ranges), so the text a client writes for it must be
//! read before the value reaches the field, and the stored value must be written back as the text
//! PostgreSQL prints. [`read`] and [`write`] are those two directions. The write path reads a
//! string written to such a field with [`read`], the SQL front end reads a cast from text with it,
//! and both then hand the value to the type's family, which canonicalises it. The submodules hold
//! the forms over plain values, which the PostgreSQL wire shares.
//!
//! A text-shaped type (`inet`, `macaddr`, `bit`, ...) is stored as its text and has no entry here.

pub mod geometry;
pub mod lsn;
pub mod money;
pub mod range;
pub mod value;

use std::ops::Bound;
use std::str::FromStr;

use common::decimal::DecimalExt;
use geo_types::{Coord, LineString, Point, Polygon};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use super::super::SqlTypeDialect;
use super::super::declared_type::{DeclaredSqlType, TypeViolation};
use super::range::Element;
use crate::val::{Array, Date, Datetime, Geometry, Number, Object, Range, Timestamp, Value};

/// SQLSTATE `22P02`, invalid text representation.
const INVALID_TEXT: &str = "22P02";

/// A declared PostgreSQL type with a text form of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextShape {
	Money,
	PgLsn,
	Point,
	Lseg,
	Path,
	Polygon,
	Box,
	Line,
	Circle,
	Range(Element),
	Multirange(Element),
}

impl TextShape {
	/// The shape of a declared type, or `None` when it has no text form of its own. Only scalar
	/// PostgreSQL declarations have one.
	pub fn of(declared: &DeclaredSqlType) -> Option<Self> {
		if declared.dialect() != SqlTypeDialect::PostgreSql
			|| !declared.array_dimensions().is_empty()
		{
			return None;
		}
		Self::of_base(declared.base())
	}

	/// The shape of a PostgreSQL base type name, such as `point` or `int4range`.
	pub fn of_base(base: &str) -> Option<Self> {
		Some(match base {
			"money" => Self::Money,
			"pg_lsn" => Self::PgLsn,
			"point" => Self::Point,
			"lseg" => Self::Lseg,
			"path" => Self::Path,
			"polygon" => Self::Polygon,
			"box" => Self::Box,
			"line" => Self::Line,
			"circle" => Self::Circle,
			other => match super::range::shape(other)? {
				(element, false) => Self::Range(element),
				(element, true) => Self::Multirange(element),
			},
		})
	}

	/// The PostgreSQL name of the type, as a message names it.
	pub fn name(self) -> &'static str {
		match self {
			Self::Money => "money",
			Self::PgLsn => "pg_lsn",
			Self::Point => "point",
			Self::Lseg => "lseg",
			Self::Path => "path",
			Self::Polygon => "polygon",
			Self::Box => "box",
			Self::Line => "line",
			Self::Circle => "circle",
			Self::Range(element) => match element {
				Element::Int4 => "int4range",
				Element::Int8 => "int8range",
				Element::Numeric => "numrange",
				Element::Timestamp => "tsrange",
				Element::Timestamptz => "tstzrange",
				Element::Date => "daterange",
			},
			Self::Multirange(element) => match element {
				Element::Int4 => "int4multirange",
				Element::Int8 => "int8multirange",
				Element::Numeric => "nummultirange",
				Element::Timestamp => "tsmultirange",
				Element::Timestamptz => "tstzmultirange",
				Element::Date => "datemultirange",
			},
		}
	}

	/// The value `text` reads as, before the type's family canonicalises it, or the violation
	/// PostgreSQL reports for a text that is not of the type.
	pub fn read(self, text: &str) -> Result<Value, TypeViolation> {
		let invalid = |detail: String| invalid_input(self.name(), text, &detail);
		match self {
			Self::Money => money::parse_money_text(text)
				.map(|cents| Value::Number(Number::Decimal(Decimal::new(cents, 2))))
				.ok_or_else(|| invalid(String::new())),
			Self::PgLsn => lsn::parse_lsn_text(text)
				.map(|lsn| Value::Number(Number::U64(lsn)))
				.ok_or_else(|| invalid(String::new())),
			Self::Point => geometry::parse_point(text)
				.map(|point| Value::Geometry(Geometry::Point(Point::from(point))))
				.map_err(|error| invalid(error.to_string())),
			Self::Lseg => geometry::parse_lseg(text)
				.map(|(start, end)| line_value(&[start, end]))
				.map_err(|error| invalid(error.to_string())),
			Self::Path => geometry::parse_path(text)
				.map(|(points, closed)| {
					if closed {
						ring_value(&points)
					} else {
						line_value(&points)
					}
				})
				.map_err(|error| invalid(error.to_string())),
			Self::Polygon => geometry::parse_polygon(text)
				.map(|points| ring_value(&points))
				.map_err(|error| invalid(error.to_string())),
			Self::Box => geometry::parse_box(text)
				.map(|(first, second)| ring_value(&[first, second]))
				.map_err(|error| invalid(error.to_string())),
			Self::Line => geometry::parse_line(text)
				.map(|(a, b, c)| fixed_object([("a", a), ("b", b), ("c", c)]))
				.map_err(|error| invalid(error.to_string())),
			Self::Circle => geometry::parse_circle(text)
				.map(|((x, y), r)| fixed_object([("x", x), ("y", y), ("r", r)]))
				.map_err(|error| invalid(error.to_string())),
			Self::Range(element) => range::decode_text(text, |bound| read_element(element, bound))
				.map(|range| range_value(element, range))
				.map_err(|error| invalid(error.message)),
			Self::Multirange(element) => {
				range::decode_multirange_text(text, |bound| read_element(element, bound))
					.map(|ranges| {
						Value::Array(Array(
							ranges
								.into_iter()
								.map(|range| range_value(element, range))
								.collect::<Vec<Value>>(),
						))
					})
					.map_err(|error| invalid(error.message))
			}
		}
	}

	/// The text PostgreSQL prints for a stored value of the type. `None` for a value that is not
	/// of the shape the type stores.
	pub fn write(self, value: &Value) -> Option<String> {
		match self {
			Self::Money => {
				let cents = (decimal_of(value)? * Decimal::from(100)).round().to_i64()?;
				Some(money::money_text(cents))
			}
			Self::PgLsn => {
				let Value::Number(number) = value else {
					return None;
				};
				let lsn = if let Number::U64(lsn) = number {
					*lsn
				} else {
					u64::try_from(number.as_int_lossless()?).ok()?
				};
				Some(lsn::lsn_text(lsn))
			}
			Self::Point => {
				let Value::Geometry(Geometry::Point(point)) = value else {
					return None;
				};
				Some(geometry::format_point((point.x(), point.y())))
			}
			Self::Lseg => {
				let Value::Geometry(Geometry::Line(line)) = value else {
					return None;
				};
				let [start, end] = line.0.as_slice() else {
					return None;
				};
				Some(geometry::format_lseg(pair(start), pair(end)))
			}
			Self::Path => {
				if let Value::Geometry(Geometry::Line(line)) = value {
					return Some(geometry::format_path(&points_of(&line.0), false));
				}
				let Value::Geometry(Geometry::Polygon(polygon)) = value else {
					return None;
				};
				Some(geometry::format_path(&ring_points(polygon), true))
			}
			Self::Polygon => {
				let Value::Geometry(Geometry::Polygon(polygon)) = value else {
					return None;
				};
				Some(geometry::format_polygon(&ring_points(polygon)))
			}
			Self::Box => {
				let Value::Geometry(Geometry::Polygon(polygon)) = value else {
					return None;
				};
				let (high, low) = box_corners(polygon)?;
				Some(geometry::format_box(high, low))
			}
			Self::Line => {
				let [a, b, c] = fixed_fields(value, ["a", "b", "c"])?;
				Some(geometry::format_line(a, b, c))
			}
			Self::Circle => {
				let [x, y, r] = fixed_fields(value, ["x", "y", "r"])?;
				Some(geometry::format_circle((x, y), r))
			}
			Self::Range(element) => {
				let Value::Range(range) = value else {
					return None;
				};
				write_range(element, range)
			}
			Self::Multirange(element) => {
				let Value::Array(items) = value else {
					return None;
				};
				let ranges = items
					.iter()
					.map(|item| {
						let Value::Range(range) = item else {
							return None;
						};
						text_range(range)
					})
					.collect::<Option<Vec<_>>>()?;
				Some(range::encode_multirange_text(&ranges, |bound| {
					write_element(element, bound).unwrap_or_default()
				}))
			}
		}
	}
}

/// The value `text` reads as for a field declared as `declared`, before the family's `validate`
/// canonicalises it. `None` when the declared type has no text form of its own, so a string is a
/// value of it as it is.
pub fn read(declared: &DeclaredSqlType, text: &str) -> Option<Result<Value, TypeViolation>> {
	TextShape::of(declared).map(|shape| shape.read(text))
}

/// The text PostgreSQL prints for `value` stored in a field declared as `declared`. `None` when
/// the declared type has no text form of its own or the value is not of its shape.
pub fn write(declared: &DeclaredSqlType, value: &Value) -> Option<String> {
	TextShape::of(declared)?.write(value)
}

fn invalid_input(type_name: &str, text: &str, detail: &str) -> TypeViolation {
	let message = if detail.is_empty() {
		format!("invalid input syntax for type {type_name}: \"{text}\"")
	} else {
		format!("invalid input syntax for type {type_name}: \"{text}\" ({detail})")
	};
	TypeViolation {
		code: INVALID_TEXT,
		message,
	}
}

fn coord((x, y): geometry::Point) -> Coord<f64> {
	Coord {
		x,
		y,
	}
}

fn pair(coord: &Coord<f64>) -> geometry::Point {
	(coord.x, coord.y)
}

fn points_of(coords: &[Coord<f64>]) -> Vec<geometry::Point> {
	coords.iter().map(pair).collect()
}

/// The points of a polygon's exterior ring without the closing point the ring repeats.
fn ring_points(polygon: &Polygon<f64>) -> Vec<geometry::Point> {
	let mut points = points_of(&polygon.exterior().0);
	if points.len() > 1 && points.first() == points.last() {
		points.pop();
	}
	points
}

/// The high and low corners of the stored ring of a `box`, which is its bounding box.
fn box_corners(polygon: &Polygon<f64>) -> Option<(geometry::Point, geometry::Point)> {
	let mut points = polygon.exterior().0.iter();
	let first = points.next()?;
	let (mut low, mut high) = (pair(first), pair(first));
	for point in points {
		low = (low.0.min(point.x), low.1.min(point.y));
		high = (high.0.max(point.x), high.1.max(point.y));
	}
	Some((high, low))
}

fn line_value(points: &[geometry::Point]) -> Value {
	Value::Geometry(Geometry::Line(LineString::new(points.iter().copied().map(coord).collect())))
}

fn ring_value(points: &[geometry::Point]) -> Value {
	let ring = LineString::new(points.iter().copied().map(coord).collect());
	Value::Geometry(Geometry::Polygon(Polygon::new(ring, Vec::new())))
}

fn fixed_object<const N: usize>(fields: [(&str, f64); N]) -> Value {
	let mut object = Object::default();
	for (key, value) in fields {
		object.insert(key.to_string(), Value::Number(Number::Float(value)));
	}
	Value::Object(object)
}

/// The numeric fields `keys` of an object of exactly those keys.
fn fixed_fields<const N: usize>(value: &Value, keys: [&str; N]) -> Option<[f64; N]> {
	let Value::Object(object) = value else {
		return None;
	};
	if object.len() != N {
		return None;
	}
	let mut out = [0.0; N];
	for (slot, key) in out.iter_mut().zip(keys) {
		let Some(Value::Number(number)) = object.get(key) else {
			return None;
		};
		*slot = number.to_float();
	}
	Some(out)
}

fn decimal_of(value: &Value) -> Option<Decimal> {
	let Value::Number(number) = value else {
		return None;
	};
	number.as_decimal_lossless()
}

fn range_value(element: Element, range: range::Range<Value>) -> Value {
	let range = match range {
		// An empty range is stored as equal bounds with an exclusive end. Any value of the element
		// kind serves; the text `empty` takes the element's zero.
		range::Range::Empty => {
			let zero = element.zero();
			Range {
				start: Bound::Included(zero.clone()),
				end: Bound::Excluded(zero),
			}
		}
		range::Range::Span {
			lower,
			upper,
		} => Range {
			start: lower,
			end: upper,
		},
	};
	Value::Range(Box::new(range))
}

/// A stored range as the text codec's range: empty when it holds no value.
fn text_range(range: &Range) -> Option<range::Range<Value>> {
	if range.is_empty() {
		return Some(range::Range::Empty);
	}
	Some(range::Range::Span {
		lower: range.start.clone(),
		upper: range.end.clone(),
	})
}

fn write_range(element: Element, range: &Range) -> Option<String> {
	let range = text_range(range)?;
	Some(range::encode_text(&range, |bound| write_element(element, bound).unwrap_or_default()))
}

/// A range bound read as a value of the element kind.
fn read_element(element: Element, text: &str) -> Option<Value> {
	let text = text.trim();
	match element {
		Element::Int4 => text.parse::<i32>().ok().map(|n| Value::Number(Number::I32(n))),
		Element::Int8 => text.parse::<i64>().ok().map(|n| Value::Number(Number::Int(n))),
		// A numeric bound keeps the scale it is written with, as PostgreSQL prints it back.
		Element::Numeric => {
			let decimal = Decimal::from_str_exact_scale(text)
				.or_else(|_| Decimal::from_scientific(text).map(|d| d.normalize()))
				.ok()?;
			Some(Value::Number(Number::Decimal(decimal)))
		}
		Element::Timestamp => Timestamp::from_str(text).ok().map(Value::Timestamp),
		Element::Timestamptz => read_timestamptz(text).map(Value::Datetime),
		Element::Date => Date::from_str(text).ok().map(Value::Date),
	}
}

/// A `timestamp with time zone` in PostgreSQL's text form: the date and time separated by a space
/// or `T`, then an offset of `+hh`, `+hh:mm`, or `Z`. A text with no offset is read as UTC.
fn read_timestamptz(text: &str) -> Option<Datetime> {
	let lower = text.to_ascii_lowercase();
	if lower.trim_start_matches(['+', '-']) == "infinity" || lower == "epoch" {
		return Datetime::from_str(text).ok();
	}
	let (date, time) = text.split_once([' ', 'T'])?;
	let time = time.trim();
	let zone_at = time.find(['+', '-', 'Z', 'z']);
	let (clock, zone) = match zone_at {
		Some(at) => time.split_at(at),
		None => (time, "Z"),
	};
	let zone = match zone {
		"Z" | "z" => "Z".to_string(),
		offset if offset.len() == 3 => format!("{offset}:00"),
		offset if offset.len() == 5 && offset.is_ascii() && !offset.contains(':') => {
			format!("{}:{}", &offset[..3], &offset[3..])
		}
		offset => offset.to_string(),
	};
	Datetime::from_str(&format!("{}T{}{zone}", date.trim(), clock.trim())).ok()
}

/// A range bound as PostgreSQL prints it for the element kind.
fn write_element(element: Element, value: &Value) -> Option<String> {
	match (element, value) {
		(Element::Int4 | Element::Int8, Value::Number(number)) => {
			number.as_int_lossless().map(|n| n.to_string())
		}
		(Element::Numeric, Value::Number(number)) => {
			number.as_decimal_lossless().map(|d| d.to_string())
		}
		(Element::Timestamp, Value::Timestamp(timestamp)) => {
			let public = surrealdb_types::Timestamp::from(*timestamp);
			if public.is_infinite() {
				return Some(public.to_string());
			}
			Some(wall_clock(public.into_inner()))
		}
		(Element::Timestamptz, Value::Datetime(datetime)) => {
			if *datetime == Datetime::MAX_UTC {
				return Some("infinity".to_string());
			}
			if *datetime == Datetime::MIN_UTC {
				return Some("-infinity".to_string());
			}
			Some(format!("{}+00", wall_clock(datetime.0.naive_utc())))
		}
		(Element::Date, Value::Date(date)) => {
			let public = surrealdb_types::Date::from(*date);
			if public.is_infinite() {
				return Some(public.to_string());
			}
			Some(public.into_inner().format("%Y-%m-%d").to_string())
		}
		_ => None,
	}
}

/// `YYYY-MM-DD HH:MM:SS`, then the microseconds without trailing zeros when there are any.
fn wall_clock(at: chrono::NaiveDateTime) -> String {
	use chrono::Timelike as _;
	let mut out = at.format("%Y-%m-%d %H:%M:%S").to_string();
	let micros = at.nanosecond() / 1_000;
	if micros != 0 {
		let fraction = format!("{micros:06}");
		out.push('.');
		out.push_str(fraction.trim_end_matches('0'));
	}
	out
}

#[cfg(test)]
mod tests;
