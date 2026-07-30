//! `sql` -> `expr` conversions for [`crate::sql::literal`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};

use crate::sql::Expr;
use crate::sql::literal::*;
use crate::val::Geometry;

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
