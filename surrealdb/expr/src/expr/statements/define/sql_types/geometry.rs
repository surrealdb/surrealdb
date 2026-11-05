//! The geometric and spatial declared types (ADR 0014 D6).
//!
//! PostgreSQL `point`, `lseg`, `path`, `polygon`, and `box` are geometries. `line` and `circle` are
//! objects of a fixed shape, `{a, b, c}` and `{x, y, r}`. A PostgreSQL `path` is an open path (a
//! line) or a closed one (a polygon ring), so its kind admits both. A `box` is stored as the
//! polygon ring of its bounding box, starting at the high corner.
//!
//! MySQL spatial types are geometries of the matching kind; `GEOMETRY` admits any kind. Geometry
//! values hold longitude and latitude and carry no SRID, so the `SRID` attribute, if written, is 0
//! or 4326, the SRIDs those coordinates can be in.

use geo_types::{LineString, Polygon};

use super::super::declared_type::{DeclaredSqlType, TypeViolation};
use super::Validated;
use crate::expr::kind::GeometryKind;
use crate::expr::statements::define::field::SqlTypeDialect;
use crate::val::{Geometry, Object, Value};

/// SQLSTATE `22P02`, invalid text representation. The PostgreSQL wire reports a geometric value
/// that does not fit its type with it, and the MySQL wire maps it to its geometry error.
const INVALID_GEOMETRY: &str = "22P02";

/// Validate a value of a geometric declared type. Answers `Declined` for any other type.
pub fn validate(declared: &DeclaredSqlType, value: Value, column: &str) -> Validated {
	let Some(shape) = Shape::of(declared) else {
		return Validated::Declined(value);
	};
	Validated::Decided(shape.check(value, column))
}

/// The geometric type a declared type names.
enum Shape {
	PostgreSql(PgShape),
	/// The geometry kinds the type admits (empty for `GEOMETRY`, which admits any), and whether the
	/// `SRID` attribute, if written, is 0 or 4326.
	MySql {
		kinds: Vec<GeometryKind>,
		srid_valid: bool,
	},
}

#[derive(Clone, Copy)]
enum PgShape {
	Point,
	Lseg,
	Path,
	Polygon,
	Box,
	Line,
	Circle,
}

impl Shape {
	/// The shape of `declared`, or `None` when it is not a geometric type.
	fn of(declared: &DeclaredSqlType) -> Option<Self> {
		match declared.dialect() {
			SqlTypeDialect::PostgreSql => {
				let shape = match declared.base() {
					"point" => PgShape::Point,
					"lseg" => PgShape::Lseg,
					"path" => PgShape::Path,
					"polygon" => PgShape::Polygon,
					"box" => PgShape::Box,
					"line" => PgShape::Line,
					"circle" => PgShape::Circle,
					_ => return None,
				};
				Some(Shape::PostgreSql(shape))
			}
			SqlTypeDialect::MySql => {
				// The base is the type word, then optionally `srid <n>`: the parser keeps the
				// attribute in the base because it follows no modifier list.
				let mut words = declared.base().split(' ');
				let kinds = mysql_kinds(words.next()?)?;
				let srid_valid = match (words.next(), words.next(), words.next()) {
					(None, _, _) => true,
					(Some("srid"), Some(srid), None) => matches!(srid, "0" | "4326"),
					_ => false,
				};
				Some(Shape::MySql {
					kinds,
					srid_valid,
				})
			}
			SqlTypeDialect::DynamoDb => None,
		}
	}

	/// The value to store, or the violation that refuses it.
	/// NULL and NONE, an absent value, are stored as they are.
	fn check(&self, value: Value, _column: &str) -> Result<Value, TypeViolation> {
		let absent = matches!(value, Value::None | Value::Null);
		match self {
			Shape::PostgreSql(_) if absent => Ok(value),
			Shape::PostgreSql(shape) => shape.check(value),
			Shape::MySql {
				kinds,
				srid_valid,
			} => {
				if !srid_valid {
					return Err(refuse("geometry", "the SRID is not 0 or 4326"));
				}
				if absent {
					return Ok(value);
				}
				let admitted = if let Value::Geometry(geometry) = &value {
					kinds.is_empty() || kinds.contains(&geometry.geometry_kind())
				} else {
					false
				};
				if admitted {
					Ok(value)
				} else {
					Err(refuse("geometry", "the value is not a geometry of the declared kind"))
				}
			}
		}
	}
}

impl PgShape {
	fn check(self, value: Value) -> Result<Value, TypeViolation> {
		match self {
			PgShape::Point => {
				if matches!(value, Value::Geometry(Geometry::Point(_))) {
					Ok(value)
				} else {
					Err(refuse("point", "a point is required"))
				}
			}
			PgShape::Lseg => {
				if matches!(&value, Value::Geometry(Geometry::Line(line)) if line.0.len() == 2) {
					Ok(value)
				} else {
					Err(refuse("lseg", "a line segment has exactly two points"))
				}
			}
			PgShape::Path => {
				// An open path is a line; a closed path is a polygon ring.
				let admitted = matches!(&value, Value::Geometry(Geometry::Line(line)) if !line.0.is_empty())
					|| matches!(&value, Value::Geometry(Geometry::Polygon(ring)) if !ring.exterior().0.is_empty());
				if admitted {
					Ok(value)
				} else {
					Err(refuse("path", "a path has at least one point"))
				}
			}
			PgShape::Polygon => {
				if matches!(&value, Value::Geometry(Geometry::Polygon(ring)) if !ring.exterior().0.is_empty())
				{
					Ok(value)
				} else {
					Err(refuse("polygon", "a polygon has at least one point"))
				}
			}
			PgShape::Box => {
				let bounds = if let Value::Geometry(Geometry::Polygon(ring)) = &value {
					bounding_box(ring)
				} else {
					return Err(refuse("box", "a box is required"));
				};
				match bounds {
					Some(canonical) => Ok(Value::Geometry(Geometry::Polygon(canonical))),
					None => Err(refuse("box", "a box has at least one point")),
				}
			}
			PgShape::Line => match fixed_fields(&value, ["a", "b", "c"]) {
				Some([Some(a), Some(b), Some(_)]) if a == 0.0 && b == 0.0 => {
					Err(refuse("line", "A and B cannot both be zero"))
				}
				Some([Some(_), Some(_), Some(_)]) => Ok(value),
				_ => Err(refuse("line", "a line is {a, b, c} with numbers")),
			},
			PgShape::Circle => match fixed_fields(&value, ["x", "y", "r"]) {
				Some([Some(_), Some(_), Some(r)]) if r.is_nan() || r < 0.0 => {
					Err(refuse("circle", "a radius is zero or positive"))
				}
				Some([Some(_), Some(_), Some(_)]) => Ok(value),
				_ => Err(refuse("circle", "a circle is {x, y, r} with numbers")),
			},
		}
	}
}

/// The geometry kinds of a MySQL spatial type word. `GEOMETRY` gives an empty list, which admits
/// every kind.
fn mysql_kinds(word: &str) -> Option<Vec<GeometryKind>> {
	let kinds = match word {
		"geometry" => Vec::new(),
		"point" => vec![GeometryKind::Point],
		"linestring" => vec![GeometryKind::Line],
		"polygon" => vec![GeometryKind::Polygon],
		"multipoint" => vec![GeometryKind::MultiPoint],
		"multilinestring" => vec![GeometryKind::MultiLine],
		"multipolygon" => vec![GeometryKind::MultiPolygon],
		"geometrycollection" | "geomcollection" => vec![GeometryKind::Collection],
		_ => return None,
	};
	Some(kinds)
}

/// The polygon ring of the axis-aligned box that bounds `ring`, written from the high corner
/// `(max x, max y)` round to the low corner `(min x, min y)`. `None` when `ring` has no point.
fn bounding_box(ring: &Polygon<f64>) -> Option<Polygon<f64>> {
	let mut points = ring.exterior().0.iter();
	let first = points.next()?;
	let (mut low_x, mut low_y, mut high_x, mut high_y) = (first.x, first.y, first.x, first.y);
	for point in points {
		low_x = low_x.min(point.x);
		low_y = low_y.min(point.y);
		high_x = high_x.max(point.x);
		high_y = high_y.max(point.y);
	}
	let corners =
		vec![(high_x, high_y), (low_x, high_y), (low_x, low_y), (high_x, low_y), (high_x, high_y)];
	Some(Polygon::new(LineString::from(corners), Vec::new()))
}

/// Whether `object` holds exactly the keys `keys`.
fn has_exactly(object: &Object, keys: &[&str]) -> bool {
	object.len() == keys.len() && keys.iter().all(|key| object.get(key).is_some())
}

/// The numeric fields `keys` of `value`, when it is an object of exactly those keys.
fn fixed_fields<const N: usize>(value: &Value, keys: [&str; N]) -> Option<[Option<f64>; N]> {
	let Value::Object(object) = value else {
		return None;
	};
	if !has_exactly(object, &keys) {
		return None;
	}
	Some(keys.map(|key| number(object, key)))
}

/// The numeric field `key` of `object`, if it is a number.
fn number(object: &Object, key: &str) -> Option<f64> {
	if let Some(Value::Number(number)) = object.get(key) {
		Some(number.to_float())
	} else {
		None
	}
}

fn refuse(type_name: &str, reason: &str) -> TypeViolation {
	TypeViolation {
		code: INVALID_GEOMETRY,
		message: format!("invalid input syntax for type {type_name}: {reason}"),
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use geo_types::{Coord, Point};

	use super::*;
	use crate::val::Number;

	fn declared(dialect: SqlTypeDialect, text: &str) -> DeclaredSqlType {
		DeclaredSqlType::parse(dialect, text).expect("a declared type")
	}

	fn run(dialect: SqlTypeDialect, text: &str, value: Value) -> Result<Value, TypeViolation> {
		let ty = declared(dialect, text);
		match validate(&ty, value, "g") {
			Validated::Decided(result) => result,
			Validated::Declined(_) => panic!("{text} is not owned by the geometry family"),
		}
	}

	fn pg(text: &str, value: Value) -> Result<Value, TypeViolation> {
		run(SqlTypeDialect::PostgreSql, text, value)
	}

	fn point(x: f64, y: f64) -> Value {
		Value::Geometry(Geometry::Point(Point::new(x, y)))
	}

	fn line(points: &[(f64, f64)]) -> Value {
		let coords: Vec<Coord<f64>> = points
			.iter()
			.map(|&(x, y)| Coord {
				x,
				y,
			})
			.collect();
		Value::Geometry(Geometry::Line(LineString::new(coords)))
	}

	fn ring(points: &[(f64, f64)]) -> Value {
		let coords: Vec<(f64, f64)> = points.to_vec();
		Value::Geometry(Geometry::Polygon(Polygon::new(LineString::from(coords), Vec::new())))
	}

	fn fixed(fields: &[(&str, f64)]) -> Value {
		let map: BTreeMap<&str, Value> =
			fields.iter().map(|(k, v)| (*k, Value::Number(Number::from(*v)))).collect();
		Value::Object(Object::from(map))
	}

	fn exterior(value: &Value) -> Vec<(f64, f64)> {
		let Value::Geometry(Geometry::Polygon(ring)) = value else {
			panic!("expected a polygon, found {value:?}");
		};
		ring.exterior().0.iter().map(|c| (c.x, c.y)).collect()
	}

	#[test]
	fn a_point_must_be_a_point() {
		assert!(pg("point", point(1.0, 2.0)).is_ok());
		assert!(pg("point", line(&[(0.0, 0.0), (1.0, 1.0)])).is_err());
	}

	#[test]
	fn an_lseg_has_exactly_two_points() {
		assert!(pg("lseg", line(&[(0.0, 0.0), (1.0, 1.0)])).is_ok());
		assert!(pg("lseg", line(&[(0.0, 0.0)])).is_err());
		assert!(pg("lseg", line(&[(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)])).is_err());
	}

	#[test]
	fn a_path_is_open_as_a_line_or_closed_as_a_ring() {
		assert!(pg("path", line(&[(0.0, 0.0), (1.0, 1.0)])).is_ok());
		assert!(pg("path", ring(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)])).is_ok());
		assert!(pg("path", line(&[])).is_err());
		assert!(pg("path", point(0.0, 0.0)).is_err());
	}

	#[test]
	fn a_polygon_needs_a_point() {
		assert!(pg("polygon", ring(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)])).is_ok());
		assert!(pg("polygon", ring(&[])).is_err());
	}

	#[test]
	fn a_box_is_canonicalised_to_its_bounding_ring_from_the_high_corner() {
		// Corners given low first; the stored ring starts at the high corner (3,4).
		let stored = pg("box", ring(&[(0.0, 0.0), (3.0, 4.0)])).expect("a box");
		assert_eq!(
			exterior(&stored),
			vec![(3.0, 4.0), (0.0, 4.0), (0.0, 0.0), (3.0, 0.0), (3.0, 4.0)]
		);
		assert!(pg("box", point(0.0, 0.0)).is_err());
		assert!(pg("box", ring(&[])).is_err());
	}

	#[test]
	fn a_line_refuses_a_and_b_both_zero() {
		assert!(pg("line", fixed(&[("a", 1.0), ("b", 0.0), ("c", 5.0)])).is_ok());
		assert!(pg("line", fixed(&[("a", 0.0), ("b", 0.0), ("c", 5.0)])).is_err());
		assert!(pg("line", fixed(&[("a", 1.0), ("b", 2.0)])).is_err());
		assert!(pg("line", fixed(&[("a", 1.0), ("b", 2.0), ("z", 3.0)])).is_err());
	}

	#[test]
	fn a_circle_refuses_a_negative_radius() {
		assert!(pg("circle", fixed(&[("x", 1.0), ("y", 2.0), ("r", 0.0)])).is_ok());
		assert!(pg("circle", fixed(&[("x", 1.0), ("y", 2.0), ("r", -1.0)])).is_err());
		assert!(pg("circle", fixed(&[("x", 1.0), ("y", 2.0), ("r", f64::NAN)])).is_err());
	}

	#[test]
	fn mysql_kinds_are_checked_and_srid_must_be_0_or_4326() {
		let my = |text: &str, value: Value| run(SqlTypeDialect::MySql, text, value);
		assert!(my("geometry", point(1.0, 2.0)).is_ok());
		assert!(my("POINT", point(1.0, 2.0)).is_ok());
		assert!(my("point srid 4326", point(1.0, 2.0)).is_ok());
		assert!(my("point", line(&[(0.0, 0.0), (1.0, 1.0)])).is_err());
		assert!(my("linestring", line(&[(0.0, 0.0), (1.0, 1.0)])).is_ok());
		assert!(my("geomcollection", point(1.0, 2.0)).is_err());
		assert!(my("point srid 0", point(1.0, 2.0)).is_ok());
		for refused in ["point srid abc", "point srid 3857"] {
			assert!(DeclaredSqlType::parse(SqlTypeDialect::MySql, refused).is_err(), "{refused}");
		}
	}

	#[test]
	fn an_absent_or_null_value_is_stored_as_it_is() {
		let my = |text: &str, value: Value| run(SqlTypeDialect::MySql, text, value);
		for absent in [Value::None, Value::Null] {
			assert_eq!(pg("point", absent.clone()).unwrap(), absent);
			assert_eq!(pg("circle", absent.clone()).unwrap(), absent);
			assert_eq!(my("point srid 4326", absent.clone()).unwrap(), absent);
		}
	}

	#[test]
	fn other_types_are_declined() {
		let ty = declared(SqlTypeDialect::PostgreSql, "varchar(4)");
		assert!(matches!(validate(&ty, Value::None, "c"), Validated::Declined(_)));
	}
}
