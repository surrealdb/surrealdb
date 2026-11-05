//! The MySQL wire form of the spatial types (ADR 0014 D6): WKB, with the 4-byte little-endian
//! SRID in front.
//!
//! The codec is pure. A geometry is a [`Wkb`] of plain coordinates, so the caller maps the
//! geometry values of the engine to and from it. Encoding writes little-endian WKB, the form
//! MySQL writes. Decoding accepts either byte order.

use anyhow::{Result, bail};

/// A coordinate pair.
pub type Pt = (f64, f64);

/// The deepest `GeometryCollection` nesting the decoder follows. The bound keeps a crafted form
/// from exhausting the stack.
const MAX_NESTING: usize = 32;

/// A geometry of any of the seven WKB kinds, in plain coordinates.
#[derive(Clone, Debug, PartialEq)]
pub enum Wkb {
	Point(Pt),
	LineString(Vec<Pt>),
	/// A polygon's rings: the exterior ring first, then the interior rings. As WKB requires, each
	/// ring repeats its first point as its last.
	Polygon(Vec<Vec<Pt>>),
	MultiPoint(Vec<Pt>),
	MultiLineString(Vec<Vec<Pt>>),
	MultiPolygon(Vec<Vec<Vec<Pt>>>),
	GeometryCollection(Vec<Wkb>),
}

const TYPE_POINT: u32 = 1;
const TYPE_LINESTRING: u32 = 2;
const TYPE_POLYGON: u32 = 3;
const TYPE_MULTIPOINT: u32 = 4;
const TYPE_MULTILINESTRING: u32 = 5;
const TYPE_MULTIPOLYGON: u32 = 6;
const TYPE_GEOMETRYCOLLECTION: u32 = 7;

/// Encodes a geometry with its SRID: the 4-byte little-endian SRID, then the little-endian WKB.
/// MySQL writes SRID 0 unless the column declares another.
pub fn encode(srid: u32, geometry: &Wkb) -> Vec<u8> {
	let mut out = Vec::new();
	out.extend_from_slice(&srid.to_le_bytes());
	write_geometry(&mut out, geometry);
	out
}

/// Decodes a geometry and its SRID. The form must be exactly one geometry after the SRID.
pub fn decode(bytes: &[u8]) -> Result<(u32, Wkb)> {
	let mut reader = Reader {
		bytes,
	};
	let srid = reader.u32_le()?;
	let geometry = reader.geometry(0)?;
	if !reader.bytes.is_empty() {
		bail!("{} trailing bytes after a geometry", reader.bytes.len());
	}
	Ok((srid, geometry))
}

fn write_geometry(out: &mut Vec<u8>, geometry: &Wkb) {
	out.push(1);
	match geometry {
		Wkb::Point(point) => {
			write_u32(out, TYPE_POINT);
			write_point(out, *point);
		}
		Wkb::LineString(points) => {
			write_u32(out, TYPE_LINESTRING);
			write_points(out, points);
		}
		Wkb::Polygon(rings) => {
			write_u32(out, TYPE_POLYGON);
			write_rings(out, rings);
		}
		Wkb::MultiPoint(points) => {
			write_u32(out, TYPE_MULTIPOINT);
			write_u32(out, count(points.len()));
			for point in points {
				write_geometry(out, &Wkb::Point(*point));
			}
		}
		Wkb::MultiLineString(lines) => {
			write_u32(out, TYPE_MULTILINESTRING);
			write_u32(out, count(lines.len()));
			for line in lines {
				write_geometry(out, &Wkb::LineString(line.clone()));
			}
		}
		Wkb::MultiPolygon(polygons) => {
			write_u32(out, TYPE_MULTIPOLYGON);
			write_u32(out, count(polygons.len()));
			for rings in polygons {
				write_geometry(out, &Wkb::Polygon(rings.clone()));
			}
		}
		Wkb::GeometryCollection(members) => {
			write_u32(out, TYPE_GEOMETRYCOLLECTION);
			write_u32(out, count(members.len()));
			for member in members {
				write_geometry(out, member);
			}
		}
	}
}

fn write_u32(out: &mut Vec<u8>, value: u32) {
	out.extend_from_slice(&value.to_le_bytes());
}

fn write_point(out: &mut Vec<u8>, (x, y): Pt) {
	out.extend_from_slice(&x.to_le_bytes());
	out.extend_from_slice(&y.to_le_bytes());
}

fn write_points(out: &mut Vec<u8>, points: &[Pt]) {
	write_u32(out, count(points.len()));
	for point in points {
		write_point(out, *point);
	}
}

fn write_rings(out: &mut Vec<u8>, rings: &[Vec<Pt>]) {
	write_u32(out, count(rings.len()));
	for ring in rings {
		write_points(out, ring);
	}
}

/// A count as WKB writes it. No geometry a column can hold has more than `u32::MAX` members, so
/// the count saturates rather than wrapping.
fn count(len: usize) -> u32 {
	u32::try_from(len).unwrap_or(u32::MAX)
}

/// A cursor over WKB that refuses any read past the end.
struct Reader<'a> {
	bytes: &'a [u8],
}

impl Reader<'_> {
	fn take(&mut self, count: usize) -> Result<&[u8]> {
		if self.bytes.len() < count {
			bail!("a geometry is {count} bytes longer than its form");
		}
		let (head, tail) = self.bytes.split_at(count);
		self.bytes = tail;
		Ok(head)
	}

	fn u32_le(&mut self) -> Result<u32> {
		let raw: [u8; 4] = self.take(4)?.try_into()?;
		Ok(u32::from_le_bytes(raw))
	}

	/// Reads a WKB geometry with its own byte-order flag and type.
	fn geometry(&mut self, depth: usize) -> Result<Wkb> {
		if depth > MAX_NESTING {
			bail!("geometry collections nest deeper than {MAX_NESTING} levels");
		}
		let little = match self.take(1)?[0] {
			1 => true,
			0 => false,
			other => bail!("a WKB byte order is 0 or 1, found {other}"),
		};
		match self.u32(little)? {
			TYPE_POINT => Ok(Wkb::Point(self.point(little)?)),
			TYPE_LINESTRING => Ok(Wkb::LineString(self.points(little)?)),
			TYPE_POLYGON => Ok(Wkb::Polygon(self.rings(little)?)),
			TYPE_MULTIPOINT => {
				let members = self.members(little, depth, TYPE_POINT)?;
				let points = members
					.into_iter()
					.map(|member| {
						if let Wkb::Point(point) = member {
							Ok(point)
						} else {
							Err(anyhow::anyhow!("a multipoint holds a geometry of another kind"))
						}
					})
					.collect::<Result<Vec<Pt>>>()?;
				Ok(Wkb::MultiPoint(points))
			}
			TYPE_MULTILINESTRING => {
				let members = self.members(little, depth, TYPE_LINESTRING)?;
				let lines = members
					.into_iter()
					.map(|member| {
						if let Wkb::LineString(points) = member {
							Ok(points)
						} else {
							Err(anyhow::anyhow!(
								"a multilinestring holds a geometry of another kind"
							))
						}
					})
					.collect::<Result<Vec<Vec<Pt>>>>()?;
				Ok(Wkb::MultiLineString(lines))
			}
			TYPE_MULTIPOLYGON => {
				let members = self.members(little, depth, TYPE_POLYGON)?;
				let polygons = members
					.into_iter()
					.map(|member| {
						if let Wkb::Polygon(rings) = member {
							Ok(rings)
						} else {
							Err(anyhow::anyhow!("a multipolygon holds a geometry of another kind"))
						}
					})
					.collect::<Result<Vec<Vec<Vec<Pt>>>>>()?;
				Ok(Wkb::MultiPolygon(polygons))
			}
			TYPE_GEOMETRYCOLLECTION => {
				let len = self.count(little, 5)?;
				let members =
					(0..len).map(|_| self.geometry(depth + 1)).collect::<Result<Vec<Wkb>>>()?;
				Ok(Wkb::GeometryCollection(members))
			}
			other => bail!("unknown WKB geometry type {other}"),
		}
	}

	/// The members of a multi geometry, which must each be of the `expected` WKB type.
	fn members(&mut self, little: bool, depth: usize, expected: u32) -> Result<Vec<Wkb>> {
		let len = self.count(little, 5)?;
		let mut members = Vec::with_capacity(len);
		for _ in 0..len {
			let member = self.geometry(depth + 1)?;
			let is_expected = match expected {
				TYPE_POINT => matches!(member, Wkb::Point(_)),
				TYPE_LINESTRING => matches!(member, Wkb::LineString(_)),
				_ => matches!(member, Wkb::Polygon(_)),
			};
			if !is_expected {
				bail!("a multi geometry holds a geometry of another kind");
			}
			members.push(member);
		}
		Ok(members)
	}

	fn u32(&mut self, little: bool) -> Result<u32> {
		let raw: [u8; 4] = self.take(4)?.try_into()?;
		Ok(if little {
			u32::from_le_bytes(raw)
		} else {
			u32::from_be_bytes(raw)
		})
	}

	fn float(&mut self, little: bool) -> Result<f64> {
		let raw: [u8; 8] = self.take(8)?.try_into()?;
		Ok(if little {
			f64::from_le_bytes(raw)
		} else {
			f64::from_be_bytes(raw)
		})
	}

	fn point(&mut self, little: bool) -> Result<Pt> {
		Ok((self.float(little)?, self.float(little)?))
	}

	/// A count of items, each at least `min_item` bytes long. A count the remaining bytes cannot
	/// hold is refused before anything is allocated.
	fn count(&mut self, little: bool, min_item: usize) -> Result<usize> {
		let len = usize::try_from(self.u32(little)?)?;
		if self.bytes.len() / min_item < len {
			bail!("a count of {len} exceeds the bytes of the form");
		}
		Ok(len)
	}

	fn points(&mut self, little: bool) -> Result<Vec<Pt>> {
		let len = self.count(little, 16)?;
		(0..len).map(|_| self.point(little)).collect()
	}

	fn rings(&mut self, little: bool) -> Result<Vec<Vec<Pt>>> {
		let len = self.count(little, 4)?;
		(0..len).map(|_| self.points(little)).collect()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn round_trip(geometry: &Wkb) {
		let bytes = encode(0, geometry);
		let (srid, decoded) = decode(&bytes).expect("a valid form");
		assert_eq!(srid, 0);
		assert_eq!(&decoded, geometry);
	}

	#[test]
	fn every_kind_round_trips() {
		round_trip(&Wkb::Point((1.0, 2.0)));
		round_trip(&Wkb::LineString(vec![(0.0, 0.0), (1.0, 1.0)]));
		round_trip(&Wkb::Polygon(vec![vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)]]));
		round_trip(&Wkb::MultiPoint(vec![(1.0, 2.0), (3.0, 4.0)]));
		round_trip(&Wkb::MultiLineString(vec![vec![(0.0, 0.0), (1.0, 1.0)]]));
		round_trip(&Wkb::MultiPolygon(vec![vec![vec![(0.0, 0.0), (1.0, 0.0), (0.0, 0.0)]]]));
		round_trip(&Wkb::GeometryCollection(vec![
			Wkb::Point((1.0, 2.0)),
			Wkb::GeometryCollection(vec![Wkb::LineString(vec![(0.0, 0.0), (2.0, 2.0)])]),
		]));
	}

	#[test]
	fn the_srid_prefix_is_little_endian_and_leads_the_form() {
		let bytes = encode(4326, &Wkb::Point((1.0, 2.0)));
		assert_eq!(&bytes[..4], &4326u32.to_le_bytes());
		assert_eq!(bytes[4], 1, "little-endian byte order");
		assert_eq!(&bytes[5..9], &TYPE_POINT.to_le_bytes());
		let (srid, geometry) = decode(&bytes).unwrap();
		assert_eq!(srid, 4326);
		assert_eq!(geometry, Wkb::Point((1.0, 2.0)));
	}

	#[test]
	fn big_endian_wkb_decodes() {
		let mut bytes = 0u32.to_le_bytes().to_vec();
		bytes.push(0);
		bytes.extend_from_slice(&TYPE_POINT.to_be_bytes());
		bytes.extend_from_slice(&1.5f64.to_be_bytes());
		bytes.extend_from_slice(&(-2.0f64).to_be_bytes());
		assert_eq!(decode(&bytes).unwrap(), (0, Wkb::Point((1.5, -2.0))));
	}

	#[test]
	fn a_point_is_twenty_one_bytes_with_the_srid() {
		assert_eq!(encode(0, &Wkb::Point((0.0, 0.0))).len(), 4 + 1 + 4 + 16);
	}

	#[test]
	fn malformed_forms_are_refused() {
		let good = encode(0, &Wkb::LineString(vec![(0.0, 0.0), (1.0, 1.0)]));
		assert!(decode(&good[..good.len() - 1]).is_err(), "a short form");
		let mut trailing = good.clone();
		trailing.push(0);
		assert!(decode(&trailing).is_err(), "a trailing byte");
		let mut bad_order = good.clone();
		bad_order[4] = 7;
		assert!(decode(&bad_order).is_err(), "a byte order that is neither 0 nor 1");
		let mut bad_kind = good.clone();
		bad_kind[5..9].copy_from_slice(&99u32.to_le_bytes());
		assert!(decode(&bad_kind).is_err(), "an unknown type");
		let mut huge = good;
		huge[9..13].copy_from_slice(&u32::MAX.to_le_bytes());
		assert!(decode(&huge).is_err(), "a count the bytes cannot hold");
		assert!(decode(&[0, 0]).is_err(), "no SRID");
	}

	#[test]
	fn a_multi_geometry_must_hold_its_kind() {
		let mut bytes = 0u32.to_le_bytes().to_vec();
		bytes.push(1);
		bytes.extend_from_slice(&TYPE_MULTIPOINT.to_le_bytes());
		bytes.extend_from_slice(&1u32.to_le_bytes());
		bytes.push(1);
		bytes.extend_from_slice(&TYPE_LINESTRING.to_le_bytes());
		bytes.extend_from_slice(&0u32.to_le_bytes());
		assert!(decode(&bytes).is_err());
	}

	#[test]
	fn nesting_is_bounded() {
		let mut geometry = Wkb::Point((0.0, 0.0));
		for _ in 0..(MAX_NESTING + 2) {
			geometry = Wkb::GeometryCollection(vec![geometry]);
		}
		assert!(decode(&encode(0, &geometry)).is_err());
	}
}
