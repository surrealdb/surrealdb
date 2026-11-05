//! The PostgreSQL binary forms of the geometric types (ADR 0014 D6): `point`, `lseg`, `path`,
//! `polygon`, `box`, `line`, and `circle`. They are PostgreSQL's `send` and `recv` layouts:
//! big-endian `float8` values and `int4` counts, with a `box` stored high corner first and a `path`
//! led by its closed byte. The text forms are the declared types' own, in
//! `surrealdb_sqlplan::sql_types::text_form::geometry`.

use anyhow::{Result, bail};
pub use surrealdb_sqlplan::sql_types::text_form::geometry::Point;

/// Encodes a point as its binary form: two `float8` values.
pub fn encode_point_binary(point: Point) -> Vec<u8> {
	let mut out = Vec::with_capacity(16);
	push_points(&mut out, &[point]);
	out
}

/// Decodes the binary form of a point.
pub fn decode_point_binary(bytes: &[u8]) -> Result<Point> {
	let mut reader = Reader::new(bytes);
	let point = reader.point()?;
	reader.finish()?;
	Ok(point)
}

/// Encodes a line segment as its binary form: the start point, then the end point.
pub fn encode_lseg_binary(start: Point, end: Point) -> Vec<u8> {
	let mut out = Vec::with_capacity(32);
	push_points(&mut out, &[start, end]);
	out
}

/// Decodes the binary form of a line segment.
pub fn decode_lseg_binary(bytes: &[u8]) -> Result<(Point, Point)> {
	let mut reader = Reader::new(bytes);
	let start = reader.point()?;
	let end = reader.point()?;
	reader.finish()?;
	Ok((start, end))
}

/// Encodes a path as its binary form: one byte for closed (0 or 1), an `int4` point count, then
/// the points.
pub fn encode_path_binary(points: &[Point], closed: bool) -> Vec<u8> {
	let mut out = Vec::with_capacity(5 + 16 * points.len());
	out.push(u8::from(closed));
	push_count(&mut out, points.len());
	push_points(&mut out, points);
	out
}

/// Decodes the binary form of a path. Returns the points and whether the path is closed.
pub fn decode_path_binary(bytes: &[u8]) -> Result<(Vec<Point>, bool)> {
	let mut reader = Reader::new(bytes);
	let closed = match reader.byte()? {
		0 => false,
		1 => true,
		other => bail!("a path's closed flag is 0 or 1, found {other}"),
	};
	let points = reader.points()?;
	reader.finish()?;
	Ok((points, closed))
}

/// Encodes a polygon as its binary form: an `int4` point count, then the points.
pub fn encode_polygon_binary(points: &[Point]) -> Vec<u8> {
	let mut out = Vec::with_capacity(4 + 16 * points.len());
	push_count(&mut out, points.len());
	push_points(&mut out, points);
	out
}

/// Decodes the binary form of a polygon.
pub fn decode_polygon_binary(bytes: &[u8]) -> Result<Vec<Point>> {
	let mut reader = Reader::new(bytes);
	let points = reader.points()?;
	reader.finish()?;
	Ok(points)
}

/// Encodes a box as its binary form: the high corner, then the low corner.
pub fn encode_box_binary(high: Point, low: Point) -> Vec<u8> {
	let mut out = Vec::with_capacity(32);
	push_points(&mut out, &[high, low]);
	out
}

/// Decodes the binary form of a box. Returns the high corner and the low corner.
pub fn decode_box_binary(bytes: &[u8]) -> Result<(Point, Point)> {
	let mut reader = Reader::new(bytes);
	let high = reader.point()?;
	let low = reader.point()?;
	reader.finish()?;
	Ok((high, low))
}

/// Encodes a line `A x + B y + C = 0` as its binary form: three `float8` values.
pub fn encode_line_binary(a: f64, b: f64, c: f64) -> Vec<u8> {
	let mut out = Vec::with_capacity(24);
	out.extend_from_slice(&a.to_be_bytes());
	out.extend_from_slice(&b.to_be_bytes());
	out.extend_from_slice(&c.to_be_bytes());
	out
}

/// Decodes the binary form of a line.
pub fn decode_line_binary(bytes: &[u8]) -> Result<(f64, f64, f64)> {
	let mut reader = Reader::new(bytes);
	let a = reader.float()?;
	let b = reader.float()?;
	let c = reader.float()?;
	reader.finish()?;
	Ok((a, b, c))
}

/// Encodes a circle as its binary form: the centre `x` and `y`, then the radius.
pub fn encode_circle_binary(center: Point, radius: f64) -> Vec<u8> {
	let mut out = Vec::with_capacity(24);
	push_points(&mut out, &[center]);
	out.extend_from_slice(&radius.to_be_bytes());
	out
}

/// Decodes the binary form of a circle. Returns the centre and the radius.
pub fn decode_circle_binary(bytes: &[u8]) -> Result<(Point, f64)> {
	let mut reader = Reader::new(bytes);
	let center = reader.point()?;
	let radius = reader.float()?;
	reader.finish()?;
	Ok((center, radius))
}

fn push_points(out: &mut Vec<u8>, points: &[Point]) {
	for (x, y) in points {
		out.extend_from_slice(&x.to_be_bytes());
		out.extend_from_slice(&y.to_be_bytes());
	}
}

fn push_count(out: &mut Vec<u8>, count: usize) {
	// A count beyond `i32::MAX` cannot be written; such a value is not a geometry a column holds,
	// so the saturated count is one the decoder refuses against the length of the bytes.
	let count = i32::try_from(count).unwrap_or(i32::MAX);
	out.extend_from_slice(&count.to_be_bytes());
}

/// A cursor over a binary form that refuses a short read and any trailing byte.
struct Reader<'a> {
	bytes: &'a [u8],
}

impl<'a> Reader<'a> {
	fn new(bytes: &'a [u8]) -> Self {
		Self {
			bytes,
		}
	}

	fn take(&mut self, count: usize) -> Result<&'a [u8]> {
		if self.bytes.len() < count {
			bail!("a geometric value is {count} bytes longer than its form");
		}
		let (head, tail) = self.bytes.split_at(count);
		self.bytes = tail;
		Ok(head)
	}

	fn byte(&mut self) -> Result<u8> {
		Ok(self.take(1)?[0])
	}

	fn float(&mut self) -> Result<f64> {
		let raw: [u8; 8] = self.take(8)?.try_into()?;
		Ok(f64::from_be_bytes(raw))
	}

	fn point(&mut self) -> Result<Point> {
		Ok((self.float()?, self.float()?))
	}

	/// An `int4` point count and the points it counts. A count the remaining bytes cannot hold
	/// is refused before anything is allocated.
	fn points(&mut self) -> Result<Vec<Point>> {
		let raw: [u8; 4] = self.take(4)?.try_into()?;
		let count = usize::try_from(i32::from_be_bytes(raw))
			.map_err(|_| anyhow::anyhow!("a point count is negative"))?;
		if self.bytes.len() / 16 < count {
			bail!("a point count of {count} exceeds the bytes of the form");
		}
		(0..count).map(|_| self.point()).collect()
	}

	fn finish(self) -> Result<()> {
		if !self.bytes.is_empty() {
			bail!("{} trailing bytes after a geometric value", self.bytes.len());
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn point_binary_is_two_float8_values_big_endian() {
		let bytes = encode_point_binary((1.0, -2.0));
		assert_eq!(bytes.len(), 16);
		assert_eq!(&bytes[..8], &1.0f64.to_be_bytes());
		assert_eq!(decode_point_binary(&bytes).unwrap(), (1.0, -2.0));
		assert!(decode_point_binary(&bytes[..15]).is_err());
		let mut long = bytes;
		long.push(0);
		assert!(decode_point_binary(&long).is_err());
	}

	#[test]
	fn lseg_and_box_binary_round_trip_in_order() {
		let lseg = encode_lseg_binary((1.0, 2.0), (3.0, 4.0));
		assert_eq!(decode_lseg_binary(&lseg).unwrap(), ((1.0, 2.0), (3.0, 4.0)));
		let boxed = encode_box_binary((3.0, 4.0), (0.0, 0.0));
		assert_eq!(&boxed[..8], &3.0f64.to_be_bytes(), "the high corner comes first");
		assert_eq!(decode_box_binary(&boxed).unwrap(), ((3.0, 4.0), (0.0, 0.0)));
	}

	#[test]
	fn path_binary_leads_with_the_closed_byte_and_count() {
		let open = encode_path_binary(&[(1.0, 2.0), (3.0, 4.0)], false);
		assert_eq!(open[0], 0);
		assert_eq!(&open[1..5], &2i32.to_be_bytes());
		assert_eq!(open.len(), 5 + 32);
		assert_eq!(decode_path_binary(&open).unwrap(), (vec![(1.0, 2.0), (3.0, 4.0)], false));
		let closed = encode_path_binary(&[(0.0, 0.0)], true);
		assert_eq!(closed[0], 1);
		assert_eq!(decode_path_binary(&closed).unwrap(), (vec![(0.0, 0.0)], true));
	}

	#[test]
	fn path_binary_refuses_a_bad_flag_and_a_count_the_bytes_cannot_hold() {
		let mut bad_flag = encode_path_binary(&[(0.0, 0.0)], false);
		bad_flag[0] = 2;
		assert!(decode_path_binary(&bad_flag).is_err());
		let mut too_many = encode_path_binary(&[(0.0, 0.0)], false);
		too_many[1..5].copy_from_slice(&1_000_000i32.to_be_bytes());
		assert!(decode_path_binary(&too_many).is_err());
		let mut negative = encode_path_binary(&[(0.0, 0.0)], false);
		negative[1..5].copy_from_slice(&(-1i32).to_be_bytes());
		assert!(decode_path_binary(&negative).is_err());
	}

	#[test]
	fn polygon_binary_round_trips() {
		let points = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0)];
		let bytes = encode_polygon_binary(&points);
		assert_eq!(bytes.len(), 4 + 48);
		assert_eq!(decode_polygon_binary(&bytes).unwrap(), points.to_vec());
	}

	#[test]
	fn line_and_circle_binary_round_trip() {
		let line = encode_line_binary(1.0, 2.0, 3.0);
		assert_eq!(decode_line_binary(&line).unwrap(), (1.0, 2.0, 3.0));
		assert!(decode_line_binary(&line[..23]).is_err());
		let circle = encode_circle_binary((1.0, 2.0), 3.0);
		assert_eq!(decode_circle_binary(&circle).unwrap(), ((1.0, 2.0), 3.0));
	}

	#[test]
	fn binary_forms_keep_the_float_bits_exactly() {
		let value = 0.1 + 0.2;
		let bytes = encode_point_binary((value, -0.0));
		let (x, y) = decode_point_binary(&bytes).unwrap();
		assert_eq!(x.to_bits(), value.to_bits());
		assert_eq!(y.to_bits(), (-0.0f64).to_bits());
	}
}
