use crate::err::Error;
use crate::sql::index::VectorType;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = VectorType;
	type Error = Error;

	type SerializeSeq = Impossible<VectorType, Error>;
	type SerializeTuple = Impossible<VectorType, Error>;
	type SerializeTupleStruct = Impossible<VectorType, Error>;
	type SerializeTupleVariant = Impossible<VectorType, Error>;
	type SerializeMap = Impossible<VectorType, Error>;
	type SerializeStruct = Impossible<VectorType, Error>;
	type SerializeStructVariant = Impossible<VectorType, Error>;
	const EXPECTED: &'static str = "an enum `VectorType`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"F64" => Ok(VectorType::F64),
			"F32" => Ok(VectorType::F32),
			"I64" => Ok(VectorType::I64),
			"I32" => Ok(VectorType::I32),
			"I16" => Ok(VectorType::I16),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::value::serde::ser::Serializer;
	use serde::Serialize;

	#[test]
	fn vector_type_f64() {
		let vt = VectorType::F64;
		let serialized = vt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vt, serialized);
	}

	#[test]
	fn vector_type_f32() {
		let vt = VectorType::F32;
		let serialized = vt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vt, serialized);
	}

	#[test]
	fn vector_type_i64() {
		let vt = VectorType::I64;
		let serialized = vt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vt, serialized);
	}
	#[test]
	fn vector_type_i32() {
		let vt = VectorType::I32;
		let serialized = vt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vt, serialized);
	}

	#[test]
	fn vector_type_i16() {
		let vt = VectorType::I16;
		let serialized = vt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(vt, serialized);
	}
}
