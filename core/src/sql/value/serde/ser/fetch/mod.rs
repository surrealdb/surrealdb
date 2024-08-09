use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::{Fetch, Idiom, Value};
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) mod vec;

struct FetchSerializer;

impl ser::Serializer for FetchSerializer {
	type Ok = Fetch;
	type Error = Error;

	type SerializeSeq = Impossible<Fetch, Error>;
	type SerializeTuple = Impossible<Fetch, Error>;
	type SerializeTupleStruct = SerializeFetch;
	type SerializeTupleVariant = Impossible<Fetch, Error>;
	type SerializeMap = Impossible<Fetch, Error>;
	type SerializeStruct = Impossible<Fetch, Error>;
	type SerializeStructVariant = Impossible<Fetch, Error>;

	const EXPECTED: &'static str = "a `Fetch`";

	fn serialize_tuple_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeTupleStruct, Error> {
		Ok(SerializeFetch::default())
	}
}

#[derive(Default)]
struct SerializeFetch {
	index: usize,
	idiom: Option<Idiom>,
	value: Option<Value>,
}

impl serde::ser::SerializeTupleStruct for SerializeFetch {
	type Ok = Fetch;
	type Error = Error;

	fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.index {
			0 => {
				self.idiom = Some(Idiom(value.serialize(ser::part::vec::Serializer.wrap())?));
			}
			1 => {
				self.value = Some(value.serialize(ser::value::Serializer.wrap())?);
			}
			index => {
				return Err(Error::custom(format!("unexpected `Fetch` index `{index}`")));
			}
		}
		self.index += 1;
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.idiom, self.value) {
			(Some(idiom), Some(value)) => Ok(Fetch(idiom, value)),
			_ => Err(ser::Error::custom("`Fetch` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn fetch() {
		let fetch = Fetch::default();
		let serialized = fetch.serialize(FetchSerializer.wrap()).unwrap();
		assert_eq!(fetch, serialized);
	}
}
