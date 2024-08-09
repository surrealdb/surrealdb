pub(super) mod opt;
pub(super) mod vec;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Kind;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Kind;
	type Error = Error;

	type SerializeSeq = Impossible<Kind, Error>;
	type SerializeTuple = Impossible<Kind, Error>;
	type SerializeTupleStruct = Impossible<Kind, Error>;
	type SerializeTupleVariant = SerializeKindTuple;
	type SerializeMap = Impossible<Kind, Error>;
	type SerializeStruct = Impossible<Kind, Error>;
	type SerializeStructVariant = Impossible<Kind, Error>;

	const EXPECTED: &'static str = "an enum `Kind`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Any" => Ok(Kind::Any),
			"Null" => Ok(Kind::Null),
			"Bool" => Ok(Kind::Bool),
			"Bytes" => Ok(Kind::Bytes),
			"Datetime" => Ok(Kind::Datetime),
			"Decimal" => Ok(Kind::Decimal),
			"Duration" => Ok(Kind::Duration),
			"Float" => Ok(Kind::Float),
			"Int" => Ok(Kind::Int),
			"Number" => Ok(Kind::Number),
			"Object" => Ok(Kind::Object),
			"Point" => Ok(Kind::Point),
			"String" => Ok(Kind::String),
			"Uuid" => Ok(Kind::Uuid),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Record" => Ok(Kind::Record(value.serialize(ser::table::vec::Serializer.wrap())?)),
			"Geometry" => Ok(Kind::Geometry(value.serialize(ser::string::vec::Serializer.wrap())?)),
			"Option" => Ok(Kind::Option(Box::new(value.serialize(Serializer.wrap())?))),
			"Either" => Ok(Kind::Either(value.serialize(vec::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}

	#[inline]
	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		let inner = match variant {
			"Set" => Inner::Set(Default::default(), Default::default()),
			"Array" => Inner::Array(Default::default(), Default::default()),
			"Function" => Inner::Function(Default::default(), Default::default()),
			variant => {
				return Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`")));
			}
		};
		Ok(SerializeKindTuple {
			inner,
			index: 0,
		})
	}
}

pub(super) struct SerializeKindTuple {
	index: usize,
	inner: Inner,
}

enum Inner {
	Set(Box<Kind>, Option<u64>),
	Array(Box<Kind>, Option<u64>),
	Function(Option<Vec<Kind>>, Option<Box<Kind>>),
}

impl serde::ser::SerializeTupleVariant for SerializeKindTuple {
	type Ok = Kind;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match (self.index, &mut self.inner) {
			(0, Inner::Set(ref mut var, _) | Inner::Array(ref mut var, _)) => {
				*var = Box::new(value.serialize(Serializer.wrap())?);
			}
			(1, Inner::Set(_, ref mut var) | Inner::Array(_, ref mut var)) => {
				*var = value.serialize(ser::primitive::u64::opt::Serializer.wrap())?;
			}
			(0, Inner::Function(ref mut var, _)) => {
				*var = value.serialize(ser::kind::vec::opt::Serializer.wrap())?;
			}
			(1, Inner::Function(_, ref mut var)) => {
				*var = value.serialize(ser::kind::opt::Serializer.wrap())?.map(Box::new);
			}
			(index, inner) => {
				let variant = match inner {
					Inner::Set(..) => "Set",
					Inner::Array(..) => "Array",
					Inner::Function(..) => "Function",
				};
				return Err(Error::custom(format!("unexpected `Kind::{variant}` index `{index}`")));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match self.inner {
			Inner::Set(one, two) => Ok(Kind::Set(one, two)),
			Inner::Array(one, two) => Ok(Kind::Array(one, two)),
			Inner::Function(one, two) => Ok(Kind::Function(one, two)),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn any() {
		let kind = Kind::Any;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn bool() {
		let kind = Kind::Bool;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn bytes() {
		let kind = Kind::Bytes;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn datetime() {
		let kind = Kind::Datetime;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn decimal() {
		let kind = Kind::Decimal;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn duration() {
		let kind = Kind::Duration;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn float() {
		let kind = Kind::Float;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn int() {
		let kind = Kind::Int;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn number() {
		let kind = Kind::Number;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn object() {
		let kind = Kind::Object;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn point() {
		let kind = Kind::Point;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn string() {
		let kind = Kind::String;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn uuid() {
		let kind = Kind::Uuid;
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn function() {
		let kind = Kind::Function(Default::default(), Default::default());
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn record() {
		let kind = Kind::Record(Default::default());
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);

		let kind = Kind::Record(vec![Default::default()]);
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn geometry() {
		let kind = Kind::Geometry(Default::default());
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);

		let kind = Kind::Geometry(vec![Default::default()]);
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn option() {
		let kind = Kind::Option(Box::default());
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn either() {
		let kind = Kind::Either(Default::default());
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);

		let kind = Kind::Either(vec![Default::default()]);
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn set() {
		let kind = Kind::Set(Box::default(), None);
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);

		let kind = Kind::Set(Box::default(), Some(Default::default()));
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}

	#[test]
	fn array() {
		let kind = Kind::Array(Box::default(), None);
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);

		let kind = Kind::Array(Box::default(), Some(Default::default()));
		let serialized = kind.serialize(Serializer.wrap()).unwrap();
		assert_eq!(kind, serialized);
	}
}
