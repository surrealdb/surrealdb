pub(super) mod vec;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Ident;
use crate::sql::Part;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Part;
	type Error = Error;

	type SerializeSeq = Impossible<Part, Error>;
	type SerializeTuple = Impossible<Part, Error>;
	type SerializeTupleStruct = Impossible<Part, Error>;
	type SerializeTupleVariant = SerializePart;
	type SerializeMap = Impossible<Part, Error>;
	type SerializeStruct = Impossible<Part, Error>;
	type SerializeStructVariant = Impossible<Part, Error>;

	const EXPECTED: &'static str = "an enum `Part`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"All" => Ok(Part::All),
			"Last" => Ok(Part::Last),
			"First" => Ok(Part::First),
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
			"Field" => Ok(Part::Field(Ident(value.serialize(ser::string::Serializer.wrap())?))),
			"Index" => Ok(Part::Index(value.serialize(ser::number::Serializer.wrap())?)),
			"Where" => Ok(Part::Where(value.serialize(ser::value::Serializer.wrap())?)),
			"Graph" => Ok(Part::Graph(value.serialize(ser::graph::Serializer.wrap())?)),
			"Start" => Ok(Part::Start(value.serialize(ser::value::Serializer.wrap())?)),
			"Value" => Ok(Part::Value(value.serialize(ser::value::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}

	fn serialize_tuple_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleVariant, Self::Error> {
		let inner = match variant {
			"Method" => Inner::Method(Default::default(), Default::default()),
			variant => {
				return Err(Error::custom(format!("unexpected tuple variant `{name}::{variant}`")));
			}
		};
		Ok(SerializePart {
			inner,
			index: 0,
		})
	}
}

pub(super) struct SerializePart {
	index: usize,
	inner: Inner,
}

enum Inner {
	Method(String, Vec<Value>),
}

impl serde::ser::SerializeTupleVariant for SerializePart {
	type Ok = Part;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match (self.index, &mut self.inner) {
			(0, Inner::Method(ref mut var, _)) => {
				*var = value.serialize(ser::string::Serializer.wrap())?;
			}
			(1, Inner::Method(_, ref mut var)) => {
				*var = value.serialize(ser::value::vec::Serializer.wrap())?;
			}
			(index, inner) => {
				let variant = match inner {
					Inner::Method(..) => "Method",
				};
				return Err(Error::custom(format!("unexpected `Part::{variant}` index `{index}`")));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match self.inner {
			Inner::Method(one, two) => Ok(Part::Method(one, two)),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn all() {
		let part = Part::All;
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn last() {
		let part = Part::Last;
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn first() {
		let part = Part::First;
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn field() {
		let part = Part::Field(Default::default());
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn index() {
		let part = Part::Index(Default::default());
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn r#where() {
		let part = Part::Where(Default::default());
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn graph() {
		let part = Part::Graph(Default::default());
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn start() {
		let part = Part::Start(sql::thing("foo:bar").unwrap().into());
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn value() {
		let part = Part::Value(sql::thing("foo:bar").unwrap().into());
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}

	#[test]
	fn method() {
		let part = Part::Method(Default::default(), Default::default());
		let serialized = part.serialize(Serializer.wrap()).unwrap();
		assert_eq!(part, serialized);
	}
}
