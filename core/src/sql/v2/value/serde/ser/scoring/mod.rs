use crate::err::Error;
use crate::sql::scoring::Scoring;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Scoring;
	type Error = Error;

	type SerializeSeq = Impossible<Scoring, Error>;
	type SerializeTuple = Impossible<Scoring, Error>;
	type SerializeTupleStruct = Impossible<Scoring, Error>;
	type SerializeTupleVariant = Impossible<Scoring, Error>;
	type SerializeMap = Impossible<Scoring, Error>;
	type SerializeStruct = Impossible<Scoring, Error>;
	type SerializeStructVariant = SerializeScoring;

	const EXPECTED: &'static str = "an enum `Scoring`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Vs" => Ok(Scoring::Vs),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}

	#[inline]
	fn serialize_struct_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStructVariant, Self::Error> {
		match (name, variant) {
			("Scoring", "Bm") => Ok(SerializeScoring::Bm(Default::default())),
			_ => Err(Error::custom(format!("unexpected `{name}::{variant}`"))),
		}
	}
}

pub(super) enum SerializeScoring {
	Bm(SerializeBm),
}

impl serde::ser::SerializeStructVariant for SerializeScoring {
	type Ok = Scoring;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match self {
			Self::Bm(bm) => bm.serialize_field(key, value),
		}
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match self {
			Self::Bm(bm) => bm.end(),
		}
	}
}

#[derive(Default)]
pub(super) struct SerializeBm {
	k1: f32,
	b: f32,
}

impl serde::ser::SerializeStructVariant for SerializeBm {
	type Ok = Scoring;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"k1" => {
				self.k1 = value.serialize(ser::primitive::f32::Serializer.wrap())?;
			}
			"b" => {
				self.b = value.serialize(ser::primitive::f32::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Scoring::Bm {{ {key} }}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(Scoring::Bm {
			k1: self.k1,
			b: self.b,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn vs() {
		let idx = Scoring::Vs;
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}

	#[test]
	fn bm() {
		let idx = Scoring::Bm {
			k1: Default::default(),
			b: Default::default(),
		};
		let serialized = idx.serialize(Serializer.wrap()).unwrap();
		assert_eq!(idx, serialized);
	}
}
