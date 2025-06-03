use std::{iter::once, ops::Bound};

use crate::sql::{
	Array, Bytes, Datetime, Duration, Future, Geometry, Id, IdRange, Number, Object, Range,
	SqlValue, Strand, Table, Thing, Uuid,
};
use geo_types::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use half::f16;
use rust_decimal::Decimal;

use super::{err::Error, simple::Simple, tags::Tag, writer::Writer};

pub trait Encode {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error>;
}

impl Encode for () {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(4, 0);
		Ok(())
	}
}

impl<A> Encode for (A,)
where
	A: Encode,
{
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(4, 1);
		self.0.encode(writer)?;
		Ok(())
	}
}

impl<A, B> Encode for (A, B)
where
	A: Encode,
	B: Encode,
{
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(4, 2);
		self.0.encode(writer)?;
		self.1.encode(writer)?;
		Ok(())
	}
}

impl<T> Encode for &T
where
	T: Encode,
{
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		(*self).encode(writer)
	}
}

impl Encode for SqlValue {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		match self {
			// Simple values
			SqlValue::None => {
				// We use the custom `NONE` tag here, as some cbor decoders decode CBOR's null and undefined into the same value,
				// while we view them as separate values
				writer.write_tag(Tag::NONE);
				SqlValue::Null.encode(writer)?;
			}
			SqlValue::Null => writer.write_u8(0xF6),
			SqlValue::Bool(x) => x.encode(writer)?,
			SqlValue::Strand(x) => x.encode(writer)?,
			SqlValue::Number(x) => x.encode(writer)?,
			SqlValue::Bytes(x) => x.encode(writer)?,
			SqlValue::Array(x) => x.encode(writer)?,
			SqlValue::Object(x) => x.encode(writer)?,
			SqlValue::Datetime(x) => x.encode(writer)?,
			SqlValue::Duration(x) => x.encode(writer)?,
			SqlValue::Future(x) => x.encode(writer)?,
			SqlValue::Geometry(x) => x.encode(writer)?,
			SqlValue::Range(x) => x.encode(writer)?,
			SqlValue::Table(x) => x.encode(writer)?,
			SqlValue::Thing(x) => x.encode(writer)?,
			SqlValue::Uuid(x) => x.encode(writer)?,

			_ => return Err(Error::UnsupportedEncodingValue),
		}

		Ok(())
	}
}

impl Encode for bool {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		if *self {
			writer.write_u8(0xF5)
		} else {
			writer.write_u8(0xF4)
		}

		Ok(())
	}
}

impl Encode for &str {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		let bytes = self.as_bytes();
		writer.write_major(3, bytes.len() as u64);
		writer.write_bytes(bytes);
		Ok(())
	}
}

impl Encode for String {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		self.as_str().encode(writer)
	}
}

impl Encode for Strand {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		self.as_str().encode(writer)
	}
}

impl Encode for &[u8] {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(2, self.len() as u64);
		writer.write_bytes(self);
		Ok(())
	}
}

impl Encode for Bytes {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(2, self.len() as u64);
		writer.write_bytes(self);
		Ok(())
	}
}

impl Encode for bytes::Bytes {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(2, self.len() as u64);
		writer.write_bytes(self);
		Ok(())
	}
}

impl Encode for uuid::Bytes {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(2, self.len() as u64);
		writer.write_bytes(self.as_slice());
		Ok(())
	}
}

impl Encode for Array {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		self.0.encode(writer)
	}
}

impl<T> Encode for Vec<T>
where
	T: Encode,
{
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(4, self.len() as u64);
		for v in self.iter() {
			v.encode(writer)?
		}

		Ok(())
	}
}

impl Encode for Object {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(5, self.len() as u64);
		for (k, v) in self.iter() {
			k.encode(writer)?;
			v.encode(writer)?
		}

		Ok(())
	}
}

impl Encode for i64 {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		if *self >= 0 {
			writer.write_major(0, *self as u64)
		} else {
			writer.write_major(1, -self as u64)
		}

		Ok(())
	}
}

impl Encode for u64 {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(0, *self);
		Ok(())
	}
}

impl Encode for u32 {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_major(0, *self as u64);
		Ok(())
	}
}

impl Encode for f16 {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_simple(Simple::F16);
		writer.write_f16(*self);
		Ok(())
	}
}

impl Encode for f32 {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		let n = *self;
		let f_16 = f16::from_f32(n);
		if n == f_16.to_f32() {
			writer.write_simple(Simple::F16);
			writer.write_f16(f_16);
		} else {
			writer.write_simple(Simple::F32);
			writer.write_f32(n);
		}

		Ok(())
	}
}

impl Encode for f64 {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		let n = *self;
		let f_16 = f16::from_f64(n);
		if n == f_16.to_f64() {
			writer.write_simple(Simple::F16);
			writer.write_f16(f_16);
		} else if n == n as f32 as f64 {
			writer.write_simple(Simple::F32);
			writer.write_f32(n as f32);
		} else {
			writer.write_simple(Simple::F64);
			writer.write_f64(n);
		}

		Ok(())
	}
}

impl Encode for Decimal {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::STRING_DECIMAL);
		self.to_string().encode(writer)
	}
}

impl Encode for Number {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		match self {
			Number::Int(x) => x.encode(writer),
			Number::Float(x) => x.encode(writer),
			Number::Decimal(x) => x.encode(writer),
		}
	}
}

impl Encode for uuid::Uuid {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::SPEC_UUID);
		self.as_bytes().encode(writer)
	}
}

impl Encode for Uuid {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		self.0.encode(writer)
	}
}

impl Encode for Datetime {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::CUSTOM_DATETIME);
		match (self.timestamp(), self.timestamp_subsec_nanos()) {
			(0, 0) => ().encode(writer),
			(s, 0) => (s,).encode(writer),
			(s, ns) => (s, ns).encode(writer),
		}
	}
}

impl Encode for Duration {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::CUSTOM_DURATION);
		match (self.secs(), self.subsec_nanos()) {
			(0, 0) => ().encode(writer),
			(s, 0) => (s,).encode(writer),
			(s, ns) => (s, ns).encode(writer),
		}
	}
}

impl Encode for Id {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		match self {
			Id::Number(x) => x.encode(writer),
			Id::String(x) => x.encode(writer),
			Id::Uuid(x) => x.encode(writer),
			Id::Array(x) => x.encode(writer),
			Id::Object(x) => x.encode(writer),
			Id::Range(x) => x.encode(writer),
			_ => Err(Error::UnsupportedEncodingValue),
		}
	}
}

impl Encode for IdRange {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::RANGE);
		(&self.beg, &self.end).encode(writer)
	}
}

impl<T> Encode for Bound<T>
where
	T: Encode,
{
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		match self {
			Bound::Unbounded => SqlValue::Null.encode(writer),
			Bound::Included(x) => {
				writer.write_tag(Tag::BOUND_INCLUDED);
				x.encode(writer)
			}
			Bound::Excluded(x) => {
				writer.write_tag(Tag::BOUND_EXCLUDED);
				x.encode(writer)
			}
		}
	}
}

impl Encode for Table {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::TABLE);
		self.0.encode(writer)
	}
}

impl Encode for Thing {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::RECORDID);
		(&self.tb, &self.id).encode(writer)
	}
}

impl Encode for Future {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::FUTURE);
		format!("{}", self.0).encode(writer)
	}
}

impl Encode for Range {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::RANGE);
		(&self.beg, &self.end).encode(writer)
	}
}

impl Encode for Geometry {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		match self {
			Self::Point(x) => x.encode(writer),
			Self::Line(x) => x.encode(writer),
			Self::Polygon(x) => x.encode(writer),
			Self::MultiPoint(x) => x.encode(writer),
			Self::MultiLine(x) => x.encode(writer),
			Self::MultiPolygon(x) => x.encode(writer),
			Self::Collection(x) => x.encode(writer),
		}
	}
}

impl Encode for Point {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::GEOMETRY_POINT);
		self.x_y().encode(writer)
	}
}

impl Encode for LineString {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::GEOMETRY_LINE);
		self.points().collect::<Vec<Point>>().encode(writer)
	}
}

impl Encode for Polygon {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::GEOMETRY_POLYGON);
		once(self.exterior()).chain(self.interiors()).collect::<Vec<&LineString>>().encode(writer)
	}
}

impl Encode for MultiPoint {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::GEOMETRY_MULTIPOINT);
		self.0.encode(writer)
	}
}

impl Encode for MultiLineString {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::GEOMETRY_MULTILINE);
		self.0.encode(writer)
	}
}

impl Encode for MultiPolygon {
	fn encode(&self, writer: &mut Writer) -> Result<(), Error> {
		writer.write_tag(Tag::GEOMETRY_MULTIPOLYGON);
		self.0.encode(writer)
	}
}
