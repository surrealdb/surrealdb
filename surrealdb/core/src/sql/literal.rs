//use async_graphql::dynamic::Object;
use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use rust_decimal::Decimal;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::fmt::{CoverStmts, EscapeKey, Float, QuoteStr};
use crate::sql::{Expr, RecordIdLit};
use crate::types::{
	PublicBytes, PublicDatetime, PublicDuration, PublicFile, PublicGeometry, PublicRegex,
	PublicUuid,
};
use crate::val::Geometry;

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
	Decimal(
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::sql::arbitrary::arb_decimal))]
		Decimal,
	),
	Duration(PublicDuration),

	String(String),
	RecordId(RecordIdLit),
	Datetime(PublicDatetime),
	Uuid(PublicUuid),
	Regex(PublicRegex),

	Array(Vec<Expr>),
	Set(Vec<Expr>),
	Object(Vec<ObjectEntry>),
	Geometry(PublicGeometry),
	File(PublicFile),
	Bytes(PublicBytes),
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
			(Literal::String(a), Literal::String(b)) => a == b,
			(Literal::Bytes(a), Literal::Bytes(b)) => a == b,
			(Literal::Regex(a), Literal::Regex(b)) => a == b,
			(Literal::RecordId(a), Literal::RecordId(b)) => a == b,
			(Literal::Array(a), Literal::Array(b)) => a == b,
			(Literal::Set(a), Literal::Set(b)) => a == b,
			(Literal::Object(a), Literal::Object(b)) => a == b,
			(Literal::Duration(a), Literal::Duration(b)) => a == b,
			(Literal::Datetime(a), Literal::Datetime(b)) => a == b,
			(Literal::Uuid(a), Literal::Uuid(b)) => a == b,
			(Literal::Geometry(a), Literal::Geometry(b)) => a == b,
			(Literal::File(a), Literal::File(b)) => a == b,
			_ => false,
		}
	}
}
impl Eq for Literal {}

impl ToSql for Literal {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			Literal::None => f.push_str("NONE"),
			Literal::Null => f.push_str("NULL"),
			Literal::UnboundedRange => f.push_str(".."),
			Literal::Bool(x) => {
				if *x {
					f.push_str("true");
				} else {
					f.push_str("false");
				}
			}
			Literal::Float(float) => write_sql!(f, fmt, "{}", Float(*float)),
			Literal::Integer(x) => f.push_str(&x.to_string()),
			Literal::Decimal(d) => d.fmt_sql(f, fmt),
			Literal::String(strand) => write_sql!(f, fmt, "{}", QuoteStr(strand)),
			Literal::Bytes(bytes) => bytes.fmt_sql(f, fmt),
			Literal::Regex(regex) => regex.fmt_sql(f, fmt),
			Literal::RecordId(record_id_lit) => record_id_lit.fmt_sql(f, fmt),
			Literal::Array(exprs) => {
				f.push('[');
				if !exprs.is_empty() {
					let fmt = fmt.increment();
					if fmt.is_pretty() {
						f.push('\n');
						fmt.write_indent(f);
					}
					for (i, expr) in exprs.iter().enumerate() {
						if i > 0 {
							fmt.write_separator(f);
						}
						CoverStmts(expr).fmt_sql(f, fmt);
					}
					if fmt.is_pretty() {
						f.push('\n');
						// One level less indentation for closing bracket
						if let SqlFormat::Indented(level) = fmt
							&& level > 0
						{
							for _ in 0..(level - 1) {
								f.push('\t');
							}
						}
					}
				}
				f.push(']');
			}
			Literal::Set(exprs) => {
				f.push('{');
				if !exprs.is_empty() {
					let fmt = fmt.increment();
					if fmt.is_pretty() {
						f.push('\n');
						fmt.write_indent(f);
					}
					for (i, expr) in exprs.iter().enumerate() {
						if i > 0 {
							fmt.write_separator(f);
						}
						CoverStmts(expr).fmt_sql(f, fmt);
					}
					if fmt.is_pretty() {
						f.push('\n');
						// One level less indentation for closing bracket
						if let SqlFormat::Indented(level) = fmt
							&& level > 0
						{
							for _ in 0..(level - 1) {
								f.push('\t');
							}
						}
					}
				}
				f.push('}');
			}
			Literal::Object(items) => {
				if fmt.is_pretty() {
					f.push('{');
				} else {
					f.push_str("{ ");
				}
				if !items.is_empty() {
					let fmt = fmt.increment();
					if fmt.is_pretty() {
						f.push('\n');
						fmt.write_indent(f);
					}
					for (i, entry) in items.iter().enumerate() {
						if i > 0 {
							fmt.write_separator(f);
						}
						write_sql!(
							f,
							fmt,
							"{}: {}",
							EscapeKey(&entry.key),
							CoverStmts(&entry.value)
						);
					}
					if fmt.is_pretty() {
						f.push('\n');
						// One level less indentation for closing bracket
						if let SqlFormat::Indented(level) = fmt
							&& level > 0
						{
							for _ in 0..(level - 1) {
								f.push('\t');
							}
						}
					}
				}
				if fmt.is_pretty() {
					f.push('}');
				} else {
					f.push_str(" }");
				}
			}
			Literal::Duration(duration) => duration.fmt_sql(f, fmt),
			Literal::Datetime(datetime) => {
				f.push('d');
				write_sql!(f, fmt, "{}", QuoteStr(&datetime.to_string()));
			}
			Literal::Uuid(uuid) => uuid.fmt_sql(f, fmt),
			Literal::Geometry(geometry) => geometry.fmt_sql(f, fmt),
			Literal::File(file) => file.fmt_sql(f, fmt),
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
			Literal::Duration(duration) => crate::expr::Literal::Duration(duration.into()),
			Literal::String(strand) => crate::expr::Literal::String(strand),
			Literal::RecordId(record_id_lit) => {
				crate::expr::Literal::RecordId(record_id_lit.into())
			}
			Literal::Datetime(datetime) => crate::expr::Literal::Datetime(datetime.into()),
			Literal::Uuid(uuid) => crate::expr::Literal::Uuid(uuid.into()),
			Literal::Regex(regex) => crate::expr::Literal::Regex(regex.into()),
			Literal::Array(exprs) => {
				crate::expr::Literal::Array(exprs.into_iter().map(From::from).collect())
			}
			Literal::Set(exprs) => {
				crate::expr::Literal::Set(exprs.into_iter().map(From::from).collect())
			}
			Literal::Object(items) => convert_geometry(items),
			Literal::Geometry(geometry) => crate::expr::Literal::Geometry(geometry.into()),
			Literal::File(file) => crate::expr::Literal::File(file.into()),
			Literal::Bytes(bytes) => crate::expr::Literal::Bytes(bytes.into()),
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
			crate::expr::Literal::Duration(duration) => Literal::Duration(duration.into()),
			crate::expr::Literal::String(strand) => Literal::String(strand),
			crate::expr::Literal::RecordId(record_id_lit) => {
				Literal::RecordId(record_id_lit.into())
			}
			crate::expr::Literal::Datetime(datetime) => Literal::Datetime(datetime.into()),
			crate::expr::Literal::Uuid(uuid) => Literal::Uuid(uuid.into()),
			crate::expr::Literal::Regex(regex) => Literal::Regex(regex.into()),
			crate::expr::Literal::Array(exprs) => {
				Literal::Array(exprs.into_iter().map(From::from).collect())
			}
			crate::expr::Literal::Set(exprs) => {
				Literal::Set(exprs.into_iter().map(From::from).collect())
			}
			crate::expr::Literal::Object(items) => {
				Literal::Object(items.into_iter().map(From::from).collect())
			}
			crate::expr::Literal::Geometry(geometry) => Literal::Geometry(geometry.into()),
			crate::expr::Literal::File(file) => Literal::File(file.into()),
			crate::expr::Literal::Bytes(bytes) => Literal::Bytes(bytes.into()),
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

	let Expr::Literal(Literal::String(ty)) = &map[ty_idx].value else {
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
pub(crate) struct ObjectEntry {
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

impl ToSql for ObjectEntry {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{}: {}", EscapeKey(&self.key), self.value);
	}
}
