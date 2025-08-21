use std::fmt::{self, Write as _};

//use async_graphql::dynamic::Object;
use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use rust_decimal::Decimal;

use crate::sql::escape::EscapeKey;
use crate::sql::fmt::{Fmt, Pretty, is_pretty, pretty_indent};
use crate::sql::{Closure, Expr, RecordIdLit};
use crate::val::{Bytes, Datetime, Duration, File, Geometry, Regex, Strand, Uuid};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum Literal {
	None,
	Null,
	// and unbounded range: `..`
	UnboundedRange,
	Bool(bool),
	Float(f64),
	Integer(i64),
	//TODO: Possibly remove wrapper.
	Decimal(Decimal),
	Duration(Duration),

	Strand(Strand),
	RecordId(RecordIdLit),
	Datetime(Datetime),
	Uuid(Uuid),
	Regex(Regex),

	//TODO: Possibly remove wrapper.
	Array(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Geometry(Geometry),
	File(File),
	Bytes(Bytes),
	Closure(Box<Closure>),
}

impl PartialEq for Literal {
	fn eq(&self, other: &Self) -> bool {
		match (self, other) {
			(Literal::None, Literal::None) => true,
			(Literal::Null, Literal::Null) => true,
			(Literal::Bool(a), Literal::Bool(b)) => a == b,
			(Literal::Float(a), Literal::Float(b)) => a.to_bits() == b.to_bits(),
			(Literal::Integer(a), Literal::Integer(b)) => a == b,
			(Literal::Decimal(a), Literal::Decimal(b)) => a == b,
			(Literal::Strand(a), Literal::Strand(b)) => a == b,
			(Literal::Bytes(a), Literal::Bytes(b)) => a == b,
			(Literal::Regex(a), Literal::Regex(b)) => a == b,
			(Literal::RecordId(a), Literal::RecordId(b)) => a == b,
			(Literal::Array(a), Literal::Array(b)) => a == b,
			(Literal::Object(a), Literal::Object(b)) => a == b,
			(Literal::Duration(a), Literal::Duration(b)) => a == b,
			(Literal::Datetime(a), Literal::Datetime(b)) => a == b,
			(Literal::Uuid(a), Literal::Uuid(b)) => a == b,
			(Literal::Geometry(a), Literal::Geometry(b)) => a == b,
			(Literal::File(a), Literal::File(b)) => a == b,
			(Literal::Closure(a), Literal::Closure(b)) => a == b,
			_ => false,
		}
	}
}
impl Eq for Literal {}

impl fmt::Display for Literal {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		match self {
			Literal::None => write!(f, "NONE"),
			Literal::Null => write!(f, "NULL"),
			Literal::UnboundedRange => write!(f, ".."),
			Literal::Bool(x) => {
				if *x {
					write!(f, "true")
				} else {
					write!(f, "false")
				}
			}
			Literal::Float(float) => {
				if float.is_finite() {
					write!(f, "{float}f")
				} else {
					write!(f, "{float}")
				}
			}
			Literal::Integer(x) => write!(f, "{x}"),
			Literal::Decimal(d) => write!(f, "{d}dec"),
			Literal::Strand(strand) => write!(f, "{strand}"),
			Literal::Bytes(bytes) => write!(f, "{bytes}"),
			Literal::Regex(regex) => write!(f, "{regex}"),
			Literal::RecordId(record_id_lit) => write!(f, "{record_id_lit}"),
			Literal::Array(exprs) => {
				f.write_char('[')?;
				if !exprs.is_empty() {
					let indent = pretty_indent();
					write!(f, "{}", Fmt::pretty_comma_separated(exprs.as_slice()))?;
					drop(indent);
				}
				f.write_char(']')
			}
			Literal::Object(items) => {
				if is_pretty() {
					f.write_char('{')?;
				} else {
					f.write_str("{ ")?;
				}
				if !items.is_empty() {
					let indent = pretty_indent();
					write!(
						f,
						"{}",
						Fmt::pretty_comma_separated(items.iter().map(|args| Fmt::new(
							args,
							|entry, f| write!(f, "{}: {}", EscapeKey(&entry.key), entry.value)
						)),)
					)?;
					drop(indent);
				}
				if is_pretty() {
					f.write_char('}')
				} else {
					f.write_str(" }")
				}
			}
			Literal::Duration(duration) => write!(f, "{duration}"),
			Literal::Datetime(datetime) => write!(f, "{datetime}"),
			Literal::Uuid(uuid) => write!(f, "{uuid}"),
			Literal::Geometry(geometry) => write!(f, "{geometry}"),
			Literal::File(file) => write!(f, "{file}"),
			Literal::Closure(closure) => write!(f, "{closure}"),
		}
	}
}

impl From<Literal> for crate::expr::Literal {
	fn from(value: Literal) -> Self {
		match value {
			Literal::None => crate::expr::Literal::None,
			Literal::Null => crate::expr::Literal::Null,
			Literal::UnboundedRange => crate::expr::Literal::UnboundedRange,
			Literal::Bool(x) => crate::expr::Literal::Bool(x),
			Literal::Float(x) => crate::expr::Literal::Float(x),
			Literal::Integer(x) => crate::expr::Literal::Integer(x),
			Literal::Decimal(decimal) => crate::expr::Literal::Decimal(decimal),
			Literal::Duration(duration) => crate::expr::Literal::Duration(duration),
			Literal::Strand(strand) => crate::expr::Literal::Strand(strand),
			Literal::RecordId(record_id_lit) => {
				crate::expr::Literal::RecordId(record_id_lit.into())
			}
			Literal::Datetime(datetime) => crate::expr::Literal::Datetime(datetime),
			Literal::Uuid(uuid) => crate::expr::Literal::Uuid(uuid),
			Literal::Regex(regex) => crate::expr::Literal::Regex(regex),
			Literal::Array(exprs) => {
				crate::expr::Literal::Array(exprs.into_iter().map(From::from).collect())
			}
			Literal::Object(items) => convert_geometry(items),
			Literal::Geometry(geometry) => crate::expr::Literal::Geometry(geometry),
			Literal::File(file) => crate::expr::Literal::File(file),
			Literal::Bytes(bytes) => crate::expr::Literal::Bytes(bytes),
			Literal::Closure(closure) => crate::expr::Literal::Closure(Box::new((*closure).into())),
		}
	}
}

impl From<crate::expr::Literal> for Literal {
	fn from(value: crate::expr::Literal) -> Self {
		match value {
			crate::expr::Literal::None => Literal::None,
			crate::expr::Literal::Null => Literal::Null,
			crate::expr::Literal::UnboundedRange => Literal::UnboundedRange,
			crate::expr::Literal::Bool(x) => Literal::Bool(x),
			crate::expr::Literal::Float(x) => Literal::Float(x),
			crate::expr::Literal::Integer(x) => Literal::Integer(x),
			crate::expr::Literal::Decimal(decimal) => Literal::Decimal(decimal),
			crate::expr::Literal::Duration(duration) => Literal::Duration(duration),
			crate::expr::Literal::Strand(strand) => Literal::Strand(strand),
			crate::expr::Literal::RecordId(record_id_lit) => {
				Literal::RecordId(record_id_lit.into())
			}
			crate::expr::Literal::Datetime(datetime) => Literal::Datetime(datetime),
			crate::expr::Literal::Uuid(uuid) => Literal::Uuid(uuid),
			crate::expr::Literal::Regex(regex) => Literal::Regex(regex),
			crate::expr::Literal::Array(exprs) => {
				Literal::Array(exprs.into_iter().map(From::from).collect())
			}
			crate::expr::Literal::Object(items) => {
				Literal::Object(items.into_iter().map(From::from).collect())
			}
			crate::expr::Literal::Geometry(geometry) => Literal::Geometry(geometry),
			crate::expr::Literal::File(file) => Literal::File(file),
			crate::expr::Literal::Bytes(bytes) => Literal::Bytes(bytes),
			crate::expr::Literal::Closure(closure) => Literal::Closure(Box::new((*closure).into())),
		}
	}
}

/// A hack to convert objects to geometries like they previously would.
/// If it fails to convert to geometry it just returns an object like previous
/// behaviour>
///
/// The behaviour around geometries needs to be improved but until then this is
/// her to ensure they still work like they previously would.
fn convert_geometry(map: Vec<ObjectEntry>) -> crate::expr::Literal {
	if let Some(geom) = collect_geometry(&map) {
		crate::expr::Literal::Geometry(geom)
	} else {
		crate::expr::Literal::Object(map.into_iter().map(From::from).collect())
	}
}

fn collect_geometry(map: &[ObjectEntry]) -> Option<Geometry> {
	if map.len() != 2 {
		return None;
	}

	let ty_idx = map.iter().position(|x| x.key == "type")?;

	let other = 1 ^ ty_idx;

	let Expr::Literal(Literal::Strand(ty)) = &map[ty_idx].value else {
		return None;
	};

	match ty.as_str() {
		"Point" => {
			let other = &map[other];
			if other.key != "coordinates" {
				return None;
			}
			let geom = collect_point(&other.value)?;
			Some(Geometry::Point(geom))
		}
		"LineString" => {
			let other = &map[other];
			if other.key != "coordinates" {
				return None;
			}

			let geom = collect_array(&other.value, collect_point)?;

			Some(Geometry::Line(LineString::from(geom)))
		}
		"Polygon" => {
			let other = &map[other];
			if other.key != "coordinates" {
				return None;
			}
			let geom = collect_polygon(&other.value)?;

			Some(Geometry::Polygon(geom))
		}
		"MultiPoint" => {
			let other = &map[other];
			if other.key != "coordinates" {
				return None;
			}

			let geom = collect_array(&other.value, collect_point)?;

			Some(Geometry::MultiPoint(MultiPoint::new(geom)))
		}
		"MultiLineString" => {
			let other = &map[other];
			if other.key != "coordinates" {
				return None;
			}

			let geom = collect_array(&other.value, |x| {
				collect_array(x, collect_point).map(LineString::from)
			})?;

			Some(Geometry::MultiLine(MultiLineString::new(geom)))
		}
		"MultiPolygon" => {
			let other = &map[other];
			if other.key != "coordinates" {
				return None;
			}

			let geom = collect_array(&other.value, collect_polygon)?;

			Some(Geometry::MultiPolygon(MultiPolygon::new(geom)))
		}
		"GeometryCollection" => {
			let other = &map[other];
			if other.key != "geometries" {
				return None;
			}

			let geom = collect_array(&other.value, |x| {
				let Expr::Literal(Literal::Object(x)) = x else {
					return None;
				};
				collect_geometry(x)
			})?;

			Some(Geometry::Collection(geom))
		}
		_ => None,
	}
}

fn collect_polygon(expr: &Expr) -> Option<Polygon<f64>> {
	let Expr::Literal(Literal::Array(x)) = expr else {
		return None;
	};

	if x.is_empty() {
		return None;
	}

	let first = LineString::from(collect_array(&x[0], collect_point)?);
	let mut res = Vec::new();
	for x in &x[1..] {
		res.push(LineString::from(collect_array(x, collect_point)?))
	}

	Some(Polygon::new(first, res))
}

fn collect_point(expr: &Expr) -> Option<Point<f64>> {
	let Expr::Literal(Literal::Array(array)) = expr else {
		return None;
	};

	if array.len() != 2 {
		return None;
	};

	let x = collect_number(&array[0])?;
	let y = collect_number(&array[1])?;

	Some(Point::new(x, y))
}

fn collect_number(expr: &Expr) -> Option<f64> {
	let Expr::Literal(l) = expr else {
		return None;
	};
	match l {
		Literal::Integer(x) => Some(*x as f64),
		Literal::Float(f) => Some(*f),
		Literal::Decimal(_) => None,
		_ => None,
	}
}

fn collect_array<R, F: Fn(&Expr) -> Option<R>>(expr: &Expr, f: F) -> Option<Vec<R>> {
	let Expr::Literal(Literal::Array(x)) = expr else {
		return None;
	};
	x.iter().map(f).collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ObjectEntry {
	pub key: String,
	pub value: Expr,
}

impl From<ObjectEntry> for crate::expr::literal::ObjectEntry {
	fn from(value: ObjectEntry) -> Self {
		crate::expr::literal::ObjectEntry {
			key: value.key,
			value: value.value.into(),
		}
	}
}

impl From<crate::expr::literal::ObjectEntry> for ObjectEntry {
	fn from(value: crate::expr::literal::ObjectEntry) -> Self {
		ObjectEntry {
			key: value.key,
			value: value.value.into(),
		}
	}
}

impl fmt::Display for ObjectEntry {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}: {}", self.key, self.value)
	}
}
