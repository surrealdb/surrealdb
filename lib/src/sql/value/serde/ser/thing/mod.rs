use crate::err::Error;
use crate::sql;
use crate::sql::value::serde::ser;
use crate::sql::Id;
use crate::sql::Thing;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Thing;
	type Error = Error;

	type SerializeSeq = Impossible<Thing, Error>;
	type SerializeTuple = Impossible<Thing, Error>;
	type SerializeTupleStruct = Impossible<Thing, Error>;
	type SerializeTupleVariant = Impossible<Thing, Error>;
	type SerializeMap = Impossible<Thing, Error>;
	type SerializeStruct = SerializeThing;
	type SerializeStructVariant = Impossible<Thing, Error>;

	const EXPECTED: &'static str = "a struct `Thing`";

	fn serialize_str(self, value: &str) -> Result<Self::Ok, Self::Error> {
		sql::thing(value)
	}

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeThing::default())
	}
}

#[derive(Default)]
pub(super) struct SerializeThing {
	tb: Option<String>,
	id: Option<Id>,
}

impl serde::ser::SerializeStruct for SerializeThing {
	type Ok = Thing;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"tb" => {
				self.tb = Some(value.serialize(ser::string::Serializer.wrap())?);
			}
			"id" => {
				self.id = Some(value.serialize(ser::id::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Thing::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.tb, self.id) {
			(Some(tb), Some(id)) => Ok(Thing {
				tb,
				id,
			}),
			_ => Err(Error::custom("`Thing` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql;
	use crate::sql::serde::serialize_internal;
	use serde::Serialize;

	#[test]
	fn thing() {
		let thing = sql::thing("foo:bar").unwrap();
		let serialized = serialize_internal(|| thing.serialize(Serializer.wrap())).unwrap();
		assert_eq!(thing, serialized);
	}
}
