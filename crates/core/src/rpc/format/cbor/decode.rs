use std::{collections::BTreeMap, ops::Bound, str::FromStr};

use super::{
	either::{Either, Either6},
	err::{Error, ExpectDecoded},
	major::Major,
	reader::Reader,
	simple::Simple,
	tags::*,
	types::TypeName,
};
use crate::{
	expr::Value,
	sql::{
		Array, Bytes, Datetime, Duration, Future, Geometry, Id, IdRange, Number, Object, Range,
		SqlValue, Strand, Table, Thing, Uuid,
	},
	syn,
};
use geo_types::{LineString, MultiLineString, MultiPoint, MultiPolygon, Point, Polygon};
use rust_decimal::Decimal;

pub struct Decoder<'a>(pub Reader<'a>);

impl<'a> Decoder<'a> {
	pub fn new_from_slice(slice: &'a [u8]) -> Self {
		Self(Reader::new(slice))
	}

	pub fn new_from_reader(reader: Reader<'a>) -> Self {
		Self(reader)
	}

	pub fn decode<T>(&mut self) -> Result<T, Error>
	where
		T: Decode + TypeName,
	{
		let major = self.0.read_major()?;
		T::decode(self, major)
	}

	pub fn decode_with_major<T>(&mut self, major: Major) -> Result<T, Error>
	where
		T: Decode + TypeName,
	{
		T::decode(self, major)
	}

	pub fn try_decode<T>(&mut self, major: &Major) -> Result<Option<T>, Error>
	where
		T: TryDecode,
	{
		T::try_decode(self, major)
	}
}

impl<'a> From<&'a [u8]> for Decoder<'a> {
	fn from(value: &'a [u8]) -> Self {
		Self(Reader::new(value))
	}
}

impl<'a> From<Reader<'a>> for Decoder<'a> {
	fn from(value: Reader<'a>) -> Self {
		Self(value)
	}
}

pub trait TryDecode {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error>
	where
		Self: Sized;
}

impl TryDecode for String {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Text(len) = major {
			let bytes = match len {
				31 => dec.0.read_bytes_infinite(3)?,
				len => {
					let len = dec.0.read_major_length(*len)?;
					dec.0.read_bytes(len)?.to_vec()
				}
			};

			let Ok(text) = String::from_utf8(bytes) else {
				return Err(Error::InvalidText);
			};

			Ok(Some(text))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for Strand {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		dec.try_decode::<String>(major).map(|x| x.map(Strand::from))
	}
}

impl TryDecode for Number {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Positive(n) => Ok(Some(Number::Int(*n))),
			Major::Negative(n) => Ok(Some(Number::Int(*n))),
			Major::Tagged(Tag::STRING_DECIMAL) => {
				Ok(Some(Number::Decimal(dec.try_decode(major)?.expect_decoded()?)))
			}
			Major::Simple(Simple::F16) => Ok(Some(Number::Float(dec.0.read_f16()?.to_f64()))),
			Major::Simple(Simple::F32) => Ok(Some(Number::Float(dec.0.read_f32()? as f64))),
			Major::Simple(Simple::F64) => Ok(Some(Number::Float(dec.0.read_f64()?))),
			_ => Ok(None),
		}
	}
}

impl TryDecode for Bytes {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Bytes(len) = major {
			let bytes = match len {
				31 => dec.0.read_bytes_infinite(2)?,
				len => {
					let len = dec.0.read_major_length(*len)?;
					dec.0.read_bytes(len)?.to_vec()
				}
			};

			Ok(Some(bytes.into()))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for Array {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		dec.try_decode::<Vec<SqlValue>>(major).map(|x| x.map(Array::from))
	}
}

impl<T> TryDecode for Vec<T>
where
	T: Decode + TryDecode + TypeName,
{
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Array(len) = major {
			match len {
				31 => {
					let mut arr = Vec::new();
					loop {
						let peek = dec.0.peek()?;
						if peek == 0xFF {
							dec.0.pop_peek()?;
							break;
						}

						arr.push(dec.decode::<T>()?)
					}

					Ok(Some(arr))
				}
				_ => {
					let len = dec.0.read_major_length(*len)?;
					let mut arr = Vec::with_capacity(len);
					for _ in 0..len {
						arr.push(dec.decode::<T>()?)
					}

					Ok(Some(arr))
				}
			}
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for Object {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Map(len) = major {
			match len {
				31 => {
					let mut obj = BTreeMap::new();
					loop {
						let peek = dec.0.peek()?;
						if peek == 0xFF {
							dec.0.pop_peek()?;
							break;
						}

						let key: String = dec.decode()?;
						let value = dec.decode::<SqlValue>()?;
						obj.insert(key, value);
					}

					Ok(Some(obj.into()))
				}
				_ => {
					let len = dec.0.read_major_length(*len)?;
					let mut obj = BTreeMap::new();
					for _ in 0..len {
						let key: String = dec.decode()?;
						let value = dec.decode::<SqlValue>()?;
						obj.insert(key, value);
					}

					Ok(Some(obj.into()))
				}
			}
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for i64 {
	fn try_decode(_: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Positive(n) => Ok(Some(*n)),
			Major::Negative(n) => Ok(Some(*n)),
			_ => Ok(None),
		}
	}
}

impl TryDecode for u64 {
	fn try_decode(_: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Positive(n) => Ok(u64::try_from(*n).ok()),
			_ => Ok(None),
		}
	}
}

impl TryDecode for u32 {
	fn try_decode(_: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Positive(n) => Ok(u32::try_from(*n).ok()),
			_ => Ok(None),
		}
	}
}

impl TryDecode for uuid::Uuid {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Tagged(Tag::SPEC_UUID) => {
				let bytes: Bytes = dec.decode()?;
				let Ok(slice) = bytes.as_slice().try_into() else {
					return Err(Error::ExpectedValue("a byte array with 16 bytes".into()));
				};

				Ok(Some(uuid::Uuid::from_bytes(slice)))
			}
			Major::Tagged(Tag::STRING_UUID) => {
				let text: String = dec.decode()?;
				uuid::Uuid::parse_str(&text).map(Some).map_err(|_| Error::InvalidUuid)
			}
			_ => Err(Error::ExpectedValue("a uuid".into())),
		}
	}
}

impl TryDecode for Uuid {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		uuid::Uuid::try_decode(dec, major).map(|x| x.map(Uuid::from))
	}
}

impl TryDecode for Datetime {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Tagged(Tag::SPEC_DATETIME) => Datetime::try_from(dec.decode::<String>()?)
				.map(Some)
				.map_err(|_| Error::InvalidDatetime),
			Major::Tagged(Tag::CUSTOM_DATETIME) => Datetime::try_from(dec.decode::<(i64, u32)>()?)
				.map(Some)
				.map_err(|_| Error::InvalidDatetime),
			_ => Err(Error::ExpectedValue("a uuid".into())),
		}
	}
}

impl TryDecode for Id {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		let Some(either) = Either6::try_decode(dec, major)? else {
			return Ok(None);
		};

		let id = match either {
			Either6::A(n) => Id::Number(n),
			Either6::B(n) => Id::String(n),
			Either6::C(n) => Id::Array(n),
			Either6::D(n) => Id::Object(n),
			Either6::E(n) => Id::Uuid(n),
			Either6::F(n) => Id::Range(Box::new(n)),
		};

		Ok(Some(id))
	}
}

impl TryDecode for IdRange {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::RANGE) = major {
			let (beg, end) = dec.decode::<(Bound<Id>, Bound<Id>)>()?;
			// If the try_from fails, we encountered a normal range instead of an ID range. Error to be thrown elsewhere
			Ok(IdRange::try_from((beg, end)).ok())
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for Table {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::TABLE) = major {
			Ok(Some(Table::from(dec.decode::<String>()?)))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for Thing {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::RECORDID) = major {
			let major = dec.0.read_major()?;
			if let Some(text) = String::try_decode(dec, &major)? {
				Thing::try_from(text)
					.map(Some)
					.map_err(|_| Error::ExpectedValue("a record id".to_string()))
			} else {
				let (tb, id) = dec.decode_with_major::<(Either<String, Table>, Id)>(major)?;
				let tb = match tb {
					Either::A(tb) => tb,
					Either::B(tb) => tb.0,
				};
				Ok(Some(Thing::from((tb, id))))
			}
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for Decimal {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::STRING_DECIMAL) = major {
			let text = dec.decode::<String>()?;
			Decimal::from_str(&text).map(|x| Some(x.normalize())).map_err(|_| Error::InvalidDecimal)
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for Duration {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Tagged(Tag::SPEC_DATETIME) => Duration::try_from(dec.decode::<String>()?)
				.map(Some)
				.map_err(|_| Error::InvalidDuration),
			Major::Tagged(Tag::CUSTOM_DATETIME) => {
				let (s, ns) = dec.decode::<(u64, u32)>()?;
				Ok(Some(Duration::new(s, ns)))
			}
			_ => Err(Error::ExpectedValue("a duration".into())),
		}
	}
}

impl TryDecode for Future {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::FUTURE) = major {
			let text = dec.decode::<String>()?;
			syn::block(&text)
				// Todo get rid of Value wrapper
				.map(|x| Some(Future(x)))
				.map_err(|_| Error::InvalidFuture)
		} else {
			return Ok(None);
		}
	}
}

impl TryDecode for Range {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::RANGE) = major {
			let (beg, end) = dec.decode::<(Bound<SqlValue>, Bound<SqlValue>)>()?;
			Ok(Some(Range::new(beg, end)))
		} else {
			Ok(None)
		}
	}
}

impl<T> TryDecode for Bound<T>
where
	T: Decode + TryDecode + TypeName,
{
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Tagged(Tag::BOUND_INCLUDED) => Ok(Some(Bound::Included(dec.decode::<T>()?))),
			Major::Tagged(Tag::BOUND_EXCLUDED) => Ok(Some(Bound::Excluded(dec.decode::<T>()?))),
			Major::Simple(Simple::Null | Simple::Undefined) => Ok(Some(Bound::Unbounded)),
			_ => Ok(None),
		}
	}
}

impl TryDecode for Geometry {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		let geo = match major {
			Major::Tagged(Tag::GEOMETRY_POINT) => {
				Geometry::Point(dec.try_decode(major)?.expect_decoded()?)
			}
			Major::Tagged(Tag::GEOMETRY_LINE) => {
				Geometry::Line(dec.try_decode(major)?.expect_decoded()?)
			}
			Major::Tagged(Tag::GEOMETRY_POLYGON) => {
				Geometry::Polygon(dec.try_decode(major)?.expect_decoded()?)
			}
			Major::Tagged(Tag::GEOMETRY_MULTIPOINT) => {
				Geometry::MultiPoint(dec.try_decode(major)?.expect_decoded()?)
			}
			Major::Tagged(Tag::GEOMETRY_MULTILINE) => {
				Geometry::MultiLine(dec.try_decode(major)?.expect_decoded()?)
			}
			Major::Tagged(Tag::GEOMETRY_MULTIPOLYGON) => {
				Geometry::MultiPolygon(dec.try_decode(major)?.expect_decoded()?)
			}
			Major::Tagged(Tag::GEOMETRY_COLLECTION) => {
				Geometry::Collection(dec.try_decode(major)?.expect_decoded()?)
			}
			_ => return Ok(None),
		};

		Ok(Some(geo))
	}
}

impl TryDecode for Point {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::GEOMETRY_POINT) = major {
			let (x, y) = dec.decode::<(Number, Number)>()?;
			Ok(Some((x.as_float(), y.as_float()).into()))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for LineString {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::GEOMETRY_LINE) = major {
			Ok(Some(LineString::from(dec.decode::<Vec<Point>>()?)))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for Polygon {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::GEOMETRY_POLYGON) = major {
			let lines = dec.decode::<Vec<LineString>>()?;

			let Some(exterior) = lines.first() else {
				return Err(Error::GeometryPolygonEmpty);
			};

			let interiors = Vec::from(&lines[1..]);

			Ok(Some(Polygon::new(exterior.clone(), interiors)))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for MultiPoint {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::GEOMETRY_LINE) = major {
			Ok(Some(MultiPoint::from(dec.decode::<Vec<Point>>()?)))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for MultiLineString {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::GEOMETRY_LINE) = major {
			Ok(Some(MultiLineString::new(dec.decode::<Vec<LineString>>()?)))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for MultiPolygon {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		if let Major::Tagged(Tag::GEOMETRY_LINE) = major {
			Ok(Some(MultiPolygon::new(dec.decode::<Vec<Polygon>>()?)))
		} else {
			Ok(None)
		}
	}
}

impl TryDecode for SqlValue {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		match major {
			Major::Positive(n) => Ok(Some(SqlValue::Number(Number::Int(*n)))),
			Major::Negative(n) => Ok(Some(SqlValue::Number(Number::Int(*n)))),
			Major::Bytes(_) => Ok(Some(SqlValue::Bytes(dec.try_decode(major)?.expect_decoded()?))),
			Major::Text(_) => Ok(Some(SqlValue::Strand(dec.try_decode(major)?.expect_decoded()?))),
			Major::Array(_) => Ok(Some(SqlValue::Array(dec.try_decode(major)?.expect_decoded()?))),
			Major::Map(_) => Ok(Some(SqlValue::Object(dec.try_decode(major)?.expect_decoded()?))),
			Major::Tagged(tag) => tag.decode(dec).map(Some),
			Major::Simple(simple) => simple.decode(dec).map(Some),
		}
	}
}

impl TryDecode for Value {
	fn try_decode(dec: &mut Decoder, major: &Major) -> Result<Option<Self>, Error> {
		dec.try_decode::<SqlValue>(major).map(|x| x.map(Into::into))
	}
}

pub trait Decode {
	fn decode(dec: &mut Decoder, major: Major) -> Result<Self, Error>
	where
		Self: Sized;
}

impl<T> Decode for T
where
	T: TryDecode + TypeName,
{
	fn decode(dec: &mut Decoder, major: Major) -> Result<T, Error>
	where
		Self: TryDecode + TypeName,
	{
		T::try_decode(dec, &major)?.expect_decoded()
	}
}

impl<A, B> Decode for (A, B)
where
	A: Decode + TryDecode + TypeName,
	B: Decode + TryDecode + TypeName,
{
	fn decode(dec: &mut Decoder, major: Major) -> Result<Self, Error>
	where
		Self: TypeName,
	{
		if let Major::Array(len) = major {
			if len != 2 {
				return Err(Error::ExpectedValue(Self::type_name()));
			}

			let a = dec.decode::<A>()?;
			let b = dec.decode::<B>()?;
			Ok((a, b))
		} else {
			Err(Error::ExpectedValue(Self::type_name()))
		}
	}
}

impl<A, B> TypeName for (A, B)
where
	A: TypeName,
	B: TypeName,
{
	fn type_name() -> String {
		format!("an array of [{}, {}]", A::type_name(), B::type_name())
	}
}
