pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Output;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Output;
	type Error = Error;

	type SerializeSeq = Impossible<Output, Error>;
	type SerializeTuple = Impossible<Output, Error>;
	type SerializeTupleStruct = Impossible<Output, Error>;
	type SerializeTupleVariant = Impossible<Output, Error>;
	type SerializeMap = Impossible<Output, Error>;
	type SerializeStruct = Impossible<Output, Error>;
	type SerializeStructVariant = Impossible<Output, Error>;

	const EXPECTED: &'static str = "an enum `Output`";

	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"None" => Ok(Output::None),
			"Null" => Ok(Output::Null),
			"Diff" => Ok(Output::Diff),
			"After" => Ok(Output::After),
			"Before" => Ok(Output::Before),
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
			"Fields" => Ok(Output::Fields(value.serialize(ser::fields::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;
	use serde::Serialize;

	#[test]
	fn none() {
		let output = Output::None;
		let serialized = output.serialize(Serializer.wrap()).unwrap();
		assert_eq!(output, serialized);
	}

	#[test]
	fn null() {
		let output = Output::Null;
		let serialized = output.serialize(Serializer.wrap()).unwrap();
		assert_eq!(output, serialized);
	}

	#[test]
	fn diff() {
		let output = Output::Diff;
		let serialized = output.serialize(Serializer.wrap()).unwrap();
		assert_eq!(output, serialized);
	}

	#[test]
	fn after() {
		let output = Output::After;
		let serialized = output.serialize(Serializer.wrap()).unwrap();
		assert_eq!(output, serialized);
	}

	#[test]
	fn before() {
		let output = Output::Before;
		let serialized = output.serialize(Serializer.wrap()).unwrap();
		assert_eq!(output, serialized);
	}

	#[test]
	fn fields() {
		let output = Output::Fields(Default::default());
		let serialized = output.serialize(Serializer.wrap()).unwrap();
		assert_eq!(output, serialized);
	}
}
