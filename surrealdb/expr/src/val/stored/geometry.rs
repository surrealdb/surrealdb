use geo_types::{Coord, LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};

use super::encode::Encoder;
use super::tag::*;
use super::{DEPTH_LIMIT, Error, split_clen};
use crate::val::Geometry;
use crate::val::geometry::ring_before_closing;

/// Prepends `g`'s kind and coordinates.
pub(super) fn encode(e: &mut Encoder, g: &Geometry, depth: usize) -> Result<(), Error> {
	if depth > DEPTH_LIMIT {
		return Err(Error::TooDeep(DEPTH_LIMIT));
	}
	match g {
		Geometry::Point(p) => {
			points(e, std::iter::once(p.0), 1);
			e.put_u8(GEO_POINT);
		}
		Geometry::Line(l) => {
			points(e, l.0.iter().copied(), l.0.len());
			e.put_u8(GEO_LINE);
		}
		Geometry::Polygon(p) => {
			polygon(e, p)?;
			e.put_u8(GEO_POLYGON);
		}
		Geometry::MultiPoint(m) => {
			points(e, m.0.iter().map(|p| p.0), m.0.len());
			e.put_u8(GEO_MULTIPOINT);
		}
		Geometry::MultiLine(m) => {
			for l in m.0.iter().rev() {
				line(e, l)?;
			}
			count(e, m.0.len())?;
			e.put_u8(GEO_MULTILINE);
		}
		Geometry::MultiPolygon(m) => {
			for p in m.0.iter().rev() {
				polygon(e, p)?;
			}
			count(e, m.0.len())?;
			e.put_u8(GEO_MULTIPOLYGON);
		}
		Geometry::Collection(c) => {
			for g in c.iter().rev() {
				let before = e.written();
				encode(e, g, depth + 1)?;
				e.put_clen(e.written() - before)?;
			}
			count(e, c.len())?;
			e.put_u8(GEO_COLLECTION);
		}
	}
	Ok(())
}

fn count(e: &mut Encoder, n: usize) -> Result<(), Error> {
	let n = u32::try_from(n).map_err(|_| Error::Unstorable("a geometry with too many parts"))?;
	e.put(&n.to_le_bytes());
	Ok(())
}

/// Prepends `n` coordinates, each as `x f64, y f64`.
fn points(e: &mut Encoder, coords: impl Iterator<Item = Coord<f64>>, n: usize) {
	let (out, _) = e.front(n * 16).as_chunks_mut::<16>();
	for (slot, c) in out.iter_mut().zip(coords) {
		slot[..8].copy_from_slice(&c.x.to_le_bytes());
		slot[8..].copy_from_slice(&c.y.to_le_bytes());
	}
}

/// Prepends a counted ring or line.
fn line(e: &mut Encoder, l: &LineString<f64>) -> Result<(), Error> {
	points(e, l.0.iter().copied(), l.0.len());
	count(e, l.0.len())
}

/// Prepends a polygon's ring count and rings, the exterior first.
fn polygon(e: &mut Encoder, p: &Polygon<f64>) -> Result<(), Error> {
	for r in p.interiors().iter().rev() {
		line(e, r)?;
	}
	line(e, p.exterior())?;
	count(e, 1 + p.interiors().len())
}

/// Decodes a geometry payload: its kind byte and coordinates.
pub(super) fn decode(p: &[u8], depth: usize) -> Result<Geometry, Error> {
	if depth > DEPTH_LIMIT {
		return Err(Error::TooDeep(DEPTH_LIMIT));
	}
	let (&kind, p) = p.split_first().ok_or(Error::Truncated)?;
	let mut r = Reader(p);
	let g = match kind {
		GEO_POINT => {
			let c = r.take_array::<16>()?;
			Geometry::Point(Point(coord16(&c)))
		}
		GEO_LINE => Geometry::Line(LineString(coords(r.rest())?)),
		GEO_POLYGON => Geometry::Polygon(r.polygon()?),
		GEO_MULTIPOINT => {
			Geometry::MultiPoint(MultiPoint(coords(r.rest())?.into_iter().map(Point).collect()))
		}
		GEO_MULTILINE => {
			let n = r.count(4)?;
			let mut lines = Vec::with_capacity(n);
			for _ in 0..n {
				lines.push(r.line()?);
			}
			Geometry::MultiLine(MultiLineString(lines))
		}
		GEO_MULTIPOLYGON => {
			let n = r.count(4)?;
			let mut polygons = Vec::with_capacity(n);
			for _ in 0..n {
				polygons.push(r.polygon()?);
			}
			Geometry::MultiPolygon(MultiPolygon(polygons))
		}
		GEO_COLLECTION => {
			let n = r.count(2)?;
			// Grown by the members that decode, never reserved from the claimed count: a
			// nested collection would otherwise reserve its claim at every level.
			let mut members = Vec::new();
			for _ in 0..n {
				let (g, rest) = split_clen(r.0)?;
				r.0 = rest;
				members.push(decode(g, depth + 1)?);
			}
			Geometry::Collection(members)
		}
		_ => return Err(Error::Malformed("a geometry of an unknown kind")),
	};
	if !r.0.is_empty() {
		return Err(Error::Malformed("bytes after the end of a geometry"));
	}
	Ok(g)
}

/// Coordinates, `x f64, y f64` each, filling `b` exactly.
fn coords(b: &[u8]) -> Result<Vec<Coord<f64>>, Error> {
	let (chunks, tail) = b.as_chunks::<16>();
	if !tail.is_empty() {
		return Err(Error::Malformed("a geometry coordinate of the wrong width"));
	}
	Ok(chunks.iter().map(coord16).collect())
}

/// The coordinate `x f64, y f64` held in 16 bytes.
#[inline(always)]
fn coord16(c: &[u8; 16]) -> Coord<f64> {
	let v = u128::from_le_bytes(*c);
	Coord {
		x: f64::from_bits(v as u64),
		y: f64::from_bits((v >> 64) as u64),
	}
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
	fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
		let (b, rest) = self.0.split_at_checked(n).ok_or(Error::Truncated)?;
		self.0 = rest;
		Ok(b)
	}

	/// The next `N` bytes, as an array.
	fn take_array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
		let (b, rest) = self.0.split_first_chunk::<N>().ok_or(Error::Truncated)?;
		self.0 = rest;
		Ok(*b)
	}

	fn rest(&mut self) -> &'a [u8] {
		std::mem::take(&mut self.0)
	}

	/// A u32 count of parts each taking at least `min` bytes, refused when the
	/// bytes left cannot hold that many.
	fn count(&mut self, min: usize) -> Result<usize, Error> {
		let n = u32::from_le_bytes(self.take_array::<4>()?) as usize;
		if n > self.0.len() / min.max(1) {
			return Err(Error::Malformed("a geometry part count past the end of the geometry"));
		}
		Ok(n)
	}

	fn line(&mut self) -> Result<LineString<f64>, Error> {
		let n = self.count(16)?;
		// `count` bounds `n` by the bytes left, so `n * 16` cannot overflow.
		Ok(LineString(coords(self.take(n * 16)?)?))
	}

	/// A polygon whose every ring is prepared for closing by
	/// [`ring_before_closing`].
	fn polygon(&mut self) -> Result<Polygon<f64>, Error> {
		let n = self.count(4)?;
		if n == 0 {
			return Err(Error::Malformed("a polygon without an exterior ring"));
		}
		let exterior = ring_before_closing(self.line()?);
		let mut interiors = Vec::with_capacity(n - 1);
		for _ in 1..n {
			interiors.push(ring_before_closing(self.line()?));
		}
		Ok(Polygon::new(exterior, interiors))
	}
}
