//! The parameters of the MySQL types whose wire form is bytes in both protocols, `BIT(n)` and the
//! spatial types (ADR 0014 D5 and D6), and the four-digit text of a `YEAR` cell.
//!
//! A `BIT(n)` parameter arrives as `ceil(n / 8)` big-endian bytes and a spatial one as its SRID
//! and WKB; each is read back to the value. The cells of those types are written by
//! `typing::raw_cell`.

use anyhow::{Result, bail, ensure};
use surrealdb_types::{Array, Geometry, Number, Object, Value};

use crate::codec::bit::bit_from_bytes;
use crate::codec::geometry::{self as wkb, Pt, Wkb};
use crate::codec::year::year_text;
use crate::constants::MYSQL_TYPE_YEAR;

/// The text of a cell of a `YEAR` column: four digits. `None` for another column type or a value
/// that is not a year.
pub fn year_cell(column_type: u8, value: &Value) -> Option<String> {
	if column_type != MYSQL_TYPE_YEAR {
		return None;
	}
	let Value::Number(number) = value else {
		return None;
	};
	unsigned(number).and_then(|year| u16::try_from(year).ok()).map(year_text)
}

/// The value of a `BIT` parameter: its big-endian bytes, at most eight.
pub fn bit_param(bytes: &[u8]) -> Result<Value> {
	let width = u32::try_from(bytes.len() * 8).unwrap_or(u32::MAX);
	match bit_from_bytes(bytes, width.clamp(1, 64)) {
		Some(bits) if !bytes.is_empty() && bytes.len() <= 8 => Ok(Value::Number(Number::U64(bits))),
		Some(_) | None => bail!("a BIT parameter has 1 to 8 bytes, found {}", bytes.len()),
	}
}

/// The value of a spatial parameter: the geometry its SRID and WKB describe. A geometry value
/// holds longitude and latitude and carries no SRID, so only SRID 0 (unspecified) and 4326
/// (WGS 84, the system those coordinates are in) are accepted; any other SRID is refused rather
/// than stored as if its coordinates were WGS 84.
pub fn geometry_param(bytes: &[u8]) -> Result<Value> {
	let (srid, geometry) = wkb::decode(bytes)?;
	ensure!(
		matches!(srid, 0 | 4326),
		"geometry parameter SRID {srid} is not supported; only SRID 0 and 4326 are"
	);
	match geometry_value(&geometry) {
		Some(value) => Ok(Value::Geometry(value)),
		None => bail!("the WKB does not describe a geometry SurrealDB holds"),
	}
}

/// The value a whole number holds, when it is not negative.
fn unsigned(number: &Number) -> Option<u64> {
	match number {
		Number::Int(n) => u64::try_from(*n).ok(),
		Number::I8(n) => u64::try_from(*n).ok(),
		Number::U8(n) => Some(u64::from(*n)),
		Number::I16(n) => u64::try_from(*n).ok(),
		Number::U16(n) => Some(u64::from(*n)),
		Number::I32(n) => u64::try_from(*n).ok(),
		Number::U32(n) => Some(u64::from(*n)),
		Number::U64(n) => Some(*n),
		Number::Decimal(_)
		| Number::BigDecimal(_)
		| Number::Float(_)
		| Number::F16(_)
		| Number::F32(_) => None,
	}
}

/// The geometry value of a WKB geometry, built from its GeoJSON form.
fn geometry_value(geometry: &Wkb) -> Option<Geometry> {
	let mut object = Object::new();
	let (kind, body) = match geometry {
		Wkb::Point(point) => ("Point", coordinate(*point)),
		Wkb::LineString(points) => ("LineString", line(points)),
		Wkb::Polygon(rings) => ("Polygon", polygon(rings)),
		Wkb::MultiPoint(points) => ("MultiPoint", line(points)),
		Wkb::MultiLineString(lines) => (
			"MultiLineString",
			Value::Array(Array::from(lines.iter().map(|l| line(l)).collect::<Vec<_>>())),
		),
		Wkb::MultiPolygon(polygons) => (
			"MultiPolygon",
			Value::Array(Array::from(polygons.iter().map(|p| polygon(p)).collect::<Vec<_>>())),
		),
		Wkb::GeometryCollection(members) => {
			let members = members
				.iter()
				.map(|member| geometry_value(member).map(Value::Geometry))
				.collect::<Option<Vec<Value>>>()?;
			object.insert("type".to_string(), Value::String("GeometryCollection".to_string()));
			object.insert("geometries".to_string(), Value::Array(Array::from(members)));
			return Geometry::try_from_object(&object);
		}
	};
	object.insert("type".to_string(), Value::String(kind.to_string()));
	object.insert("coordinates".to_string(), body);
	Geometry::try_from_object(&object)
}

fn coordinate((x, y): Pt) -> Value {
	Value::Array(Array::from(vec![
		Value::Number(Number::Float(x)),
		Value::Number(Number::Float(y)),
	]))
}

fn line(points: &[Pt]) -> Value {
	Value::Array(Array::from(points.iter().map(|point| coordinate(*point)).collect::<Vec<_>>()))
}

fn polygon(rings: &[Vec<Pt>]) -> Value {
	Value::Array(Array::from(rings.iter().map(|ring| line(ring)).collect::<Vec<_>>()))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::constants::{MYSQL_TYPE_BIT, MYSQL_TYPE_GEOMETRY};
	use crate::msg::ColumnDefinition41;
	use crate::typing::raw_cell as cell_bytes;

	fn column(column_type: u8) -> ColumnDefinition41 {
		ColumnDefinition41::new("db", "t", "c", column_type)
	}

	#[test]
	fn a_bit_cell_is_its_width_in_big_endian_bytes() {
		let mut bits = column(MYSQL_TYPE_BIT);
		bits.column_length = 12;
		assert_eq!(cell_bytes(&bits, &Value::Number(Number::U64(0xABC))), Some(vec![0x0A, 0xBC]));
		assert_eq!(cell_bytes(&bits, &Value::Number(Number::U64(1 << 12))), None);
		assert_eq!(bit_param(&[0x0A, 0xBC]).unwrap(), Value::Number(Number::U64(0xABC)));
		assert!(bit_param(&[]).is_err());
		assert!(bit_param(&[0; 9]).is_err());
	}

	#[test]
	fn a_spatial_cell_is_wkb_after_the_column_srid() {
		let mut spatial = column(MYSQL_TYPE_GEOMETRY);
		spatial.srid = 4326;
		let point = Value::Geometry(Geometry::Point((1.0, 2.0).into()));
		let bytes = cell_bytes(&spatial, &point).expect("a geometry cell");
		assert_eq!(&bytes[..4], &4326u32.to_le_bytes());
		assert_eq!(bytes[4], 1, "little-endian WKB");
		assert_eq!(&bytes[5..9], &1u32.to_le_bytes(), "a point");
		assert_eq!(geometry_param(&bytes).unwrap(), point);
		let unspecified = [&0u32.to_le_bytes()[..], &bytes[4..]].concat();
		assert_eq!(geometry_param(&unspecified).unwrap(), point);
		let mercator = [&3857u32.to_le_bytes()[..], &bytes[4..]].concat();
		assert!(geometry_param(&mercator).is_err());
	}

	#[test]
	fn every_geometry_kind_reads_back_from_its_wkb() {
		let mut spatial = column(MYSQL_TYPE_GEOMETRY);
		spatial.srid = 0;
		let square = geometry_value(&Wkb::Polygon(vec![vec![
			(0.0, 0.0),
			(1.0, 0.0),
			(1.0, 1.0),
			(0.0, 0.0),
		]]))
		.expect("a polygon");
		let geometries = [
			Geometry::Line(vec![(0.0, 0.0), (2.0, 3.0)].into()),
			square.clone(),
			geometry_value(&Wkb::MultiPoint(vec![(1.0, 1.0), (2.0, 2.0)])).expect("points"),
			Geometry::Collection(vec![Geometry::Point((5.0, 6.0).into()), square]),
		];
		for geometry in geometries {
			let value = Value::Geometry(geometry);
			let bytes = cell_bytes(&spatial, &value).expect("a geometry cell");
			assert_eq!(geometry_param(&bytes).unwrap(), value);
		}
	}

	#[test]
	fn a_year_cell_is_four_digits() {
		assert_eq!(
			year_cell(MYSQL_TYPE_YEAR, &Value::Number(Number::U16(0))).as_deref(),
			Some("0000")
		);
		assert_eq!(
			year_cell(MYSQL_TYPE_YEAR, &Value::Number(Number::U16(2024))).as_deref(),
			Some("2024")
		);
		assert_eq!(year_cell(MYSQL_TYPE_BIT, &Value::Number(Number::U16(2024))), None);
	}
}
