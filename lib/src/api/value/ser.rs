use revision::Revisioned;
use serde::Serialize;

use super::{Number, Value};

// Manually implemented serialize so we can align the serialization with surrealdb_core::Value
impl Serialize for Value {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		const NAME: &str = "$surrealdb::private::sql::Value";

		match self {
			Value::None => serializer.serialize_unit_variant(NAME, 0u32, "None"),
			Value::Bool(x) => serializer.serialize_newtype_variant(NAME, 2u32, "Bool", x),
			Value::Number(number) => {
				serializer.serialize_newtype_variant(NAME, 3u32, "Number", number)
			}
			Value::String(s) => serializer.serialize_newtype_variant(NAME, 4u32, "Strand", s),
			Value::Duration(d) => serializer.serialize_newtype_variant(NAME, 5u32, "Duration", d),
			Value::Datetime(d) => serializer.serialize_newtype_variant(NAME, 6u32, "Datetime", d),
			Value::Uuid(u) => serializer.serialize_newtype_variant(NAME, 7u32, "Uuid", u),
			Value::Array(a) => serializer.serialize_newtype_variant(NAME, 8u32, "Array", a),
			Value::Object(o) => serializer.serialize_newtype_variant(NAME, 9u32, "Object", o),
			// TODO: Geometry
			Value::Bytes(b) => serializer.serialize_newtype_variant(NAME, 11u32, "Bytes", b),
			Value::RecordId(id) => serializer.serialize_newtype_variant(NAME, 12u32, "Thing", id),
		}
	}
}

impl Serialize for Number {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: serde::Serializer,
	{
		const NAME: &str = "$surrealdb::private::sql::Number";

		match self {
			Number::Integer(ref x) => serializer.serialize_newtype_variant(NAME, 0, "Int", x),
			Number::Float(ref x) => serializer.serialize_newtype_variant(NAME, 1, "Float", x),
			Number::Decimal(ref x) => serializer.serialize_newtype_variant(NAME, 2, "Decimal", x),
		}
	}
}

impl Revisioned for Number {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(&self, w: &mut W) -> Result<(), revision::Error> {
		match self {
			Self::Integer(x) => {
				0u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Float(x) => {
				1u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Decimal(x) => {
				2u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
		}
	}

	fn deserialize_revisioned<R: std::io::Read>(r: &mut R) -> Result<Self, revision::Error>
	where
		Self: Sized,
	{
		let rev = u16::deserialize_revisioned(r)?;
		let variant = u32::deserialize_revisioned(r)?;

		match rev {
			1 => match variant {
				0u32 => Ok(Self::Integer(Revisioned::deserialize_revisioned(r)?)),
				1u32 => Ok(Self::Float(Revisioned::deserialize_revisioned(r)?)),
				2u32 => Ok(Self::Decimal(Revisioned::deserialize_revisioned(r)?)),
				v => {
					return Err(revision::Error::Deserialize(format!(
						"Unknown \'Number\' variant {0}.",
						revision
					)))
				}
			},
			v => {
				return Err(revision::Error::Deserialize(format!(
					"Unknown \'Number\' revision {0}.",
					revision
				)))
			}
		}
	}
}

impl Revisioned for Value {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(&self, w: &mut W) -> Result<(), revision::Error> {
		1u16.serialize_revisioned(w)?;
		match self {
			Self::None => 0u32.serialize_revisioned(w),
			Self::Bool(x) => {
				2u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Number(x) => {
				3u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::String(x) => {
				4u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Duration(x) => {
				5u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Datetime(x) => {
				6u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Uuid(x) => {
				7u32.serialize_revisioned(w)?;
				// for the Uuid wrapper.
				1u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Array(x) => {
				8u32.serialize_revisioned(w)?;
				// for the Array wrapper.
				1u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Object(x) => {
				9u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::Bytes(x) => {
				11u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
			Self::RecordId(x) => {
				12u32.serialize_revisioned(w)?;
				x.serialize_revisioned(w)
			}
		}
	}

	fn deserialize_revisioned<R: std::io::Read>(r: &mut R) -> Result<Self, revision::Error>
	where
		Self: Sized,
	{
		todo!()
	}
}
