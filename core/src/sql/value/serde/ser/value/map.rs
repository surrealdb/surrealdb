use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Value;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;
use std::collections::BTreeMap;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = BTreeMap<String, Value>;
	type Error = Error;

	type SerializeSeq = Impossible<BTreeMap<String, Value>, Error>;
	type SerializeTuple = Impossible<BTreeMap<String, Value>, Error>;
	type SerializeTupleStruct = Impossible<BTreeMap<String, Value>, Error>;
	type SerializeTupleVariant = Impossible<BTreeMap<String, Value>, Error>;
	type SerializeMap = SerializeValueMap;
	type SerializeStruct = Impossible<BTreeMap<String, Value>, Error>;
	type SerializeStructVariant = Impossible<BTreeMap<String, Value>, Error>;

	const EXPECTED: &'static str = "a struct or map";

	fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
		Ok(SerializeValueMap::default())
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
#[non_exhaustive]
pub struct SerializeValueMap {
	map: BTreeMap<String, Value>,
	next_key: Option<String>,
}

impl serde::ser::SerializeMap for SerializeValueMap {
	type Ok = BTreeMap<String, Value>;
	type Error = Error;

	fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		self.next_key = Some(key.serialize(ser::string::Serializer.wrap())?);
		Ok(())
	}

	fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		match self.next_key.take() {
			Some(key) => {
				let value = value.serialize(ser::value::Serializer.wrap())?;
				self.map.insert(key, value);
				Ok(())
			}
			None => Err(Error::custom("`serialize_value` called before `serialize_key`")),
		}
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		Ok(self.map)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn empty() {
		let map: BTreeMap<String, Value> = Default::default();
		let serialized = map.serialize(Serializer.wrap()).unwrap();
		assert_eq!(map, serialized);
	}

	#[test]
	fn map() {
		let map = map! {
			String::from("foo") => Value::from("bar"),
		};
		let serialized = map.serialize(Serializer.wrap()).unwrap();
		assert_eq!(map, serialized);
	}
}
