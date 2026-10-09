use geo::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};

use crate::expr::{Expr, Literal, ObjectEntry};
use crate::val::Geometry;

pub(crate) fn collect_geometry(map: &[ObjectEntry]) -> Option<Geometry> {
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

			let geom = collect_array(&other.value, |x| match x {
				Expr::Literal(Literal::Object(x)) => collect_geometry(x),
				Expr::Literal(Literal::Geometry(x)) => Some(x.clone()),
				_ => None,
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
