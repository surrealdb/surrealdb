pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::value::Values;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Values;
	type Error = Error;

	type SerializeSeq = Impossible<Values, Error>;
	type SerializeTuple = SerializeCompactValuesTuple;
	type SerializeTupleStruct = Impossible<Values, Error>;
	type SerializeTupleVariant = Impossible<Values, Error>;
	type SerializeMap = Impossible<Values, Error>;
	type SerializeStruct = Impossible<Values, Error>;
	type SerializeStructVariant = Impossible<Values, Error>;

	const EXPECTED: &'static str = "a UUID";

	fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		Ok(SerializeCompactValuesTuple::default())
	}

	#[inline]
	fn serialize_newtype_struct<T>(
		self,
		_name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		value.serialize(self.wrap())
	}
}

#[derive(Default)]
pub(super) struct SerializeCompactValuesTuple {
	index: usize,
	values: Vec<Value>,
}

impl serde::ser::SerializeTuple for SerializeCompactValuesTuple {
	type Ok = Values;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => self.values = value.serialize(ser::value::vec::Serializer.wrap())?,
			index => {
				return Err(Error::custom(format!("unexpected `Values` index `{index}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(Values(self.values))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let values = Values::default();
		let serialized = values.serialize(Serializer.wrap()).unwrap();
		assert_eq!(values, serialized);
	}
}
