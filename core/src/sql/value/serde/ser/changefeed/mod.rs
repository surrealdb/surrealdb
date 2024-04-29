pub(super) mod opt;

use crate::err::Error;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;
use std::time::Duration;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = ChangeFeed;
	type Error = Error;

	type SerializeSeq = Impossible<ChangeFeed, Error>;
	type SerializeTuple = Impossible<ChangeFeed, Error>;
	type SerializeTupleStruct = Impossible<ChangeFeed, Error>;
	type SerializeTupleVariant = Impossible<ChangeFeed, Error>;
	type SerializeMap = Impossible<ChangeFeed, Error>;
	type SerializeStruct = SerializeChangeFeed;
	type SerializeStructVariant = Impossible<ChangeFeed, Error>;

	const EXPECTED: &'static str = "a struct `ChangeFeed`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeChangeFeed::default())
	}
}

#[derive(Default)]
#[non_exhaustive]
pub struct SerializeChangeFeed {
	expiry: Duration,
	store_diff: bool,
}

impl serde::ser::SerializeStruct for SerializeChangeFeed {
	type Ok = ChangeFeed;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"expiry" => {
				self.expiry = value.serialize(ser::duration::Serializer.wrap())?;
			}
			"store_diff" => {
				self.store_diff = value.serialize(ser::primitive::bool::Serializer.wrap())?;
			}
			key => {
				return Err(Error::custom(format!("unexpected field `ChangeFeed::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		Ok(ChangeFeed {
			expiry: self.expiry,
			store_diff: self.store_diff,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn default() {
		let stmt = ChangeFeed::default();
		let value: ChangeFeed = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(value, stmt);
	}
}
