use crate::err::Error;
use crate::sql::value::serde::ser;
use chrono::offset::Utc;
use chrono::DateTime;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;
use std::fmt::Display;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DateTime<Utc>;
	type Error = Error;

	type SerializeSeq = Impossible<DateTime<Utc>, Error>;
	type SerializeTuple = Impossible<DateTime<Utc>, Error>;
	type SerializeTupleStruct = Impossible<DateTime<Utc>, Error>;
	type SerializeTupleVariant = Impossible<DateTime<Utc>, Error>;
	type SerializeMap = Impossible<DateTime<Utc>, Error>;
	type SerializeStruct = Impossible<DateTime<Utc>, Error>;
	type SerializeStructVariant = Impossible<DateTime<Utc>, Error>;

	const EXPECTED: &'static str = "a struct `DateTime<Utc>`";

	#[inline]
	fn collect_str<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: Display + ?Sized,
	{
		value.to_string().parse().map_err(Error::custom)
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

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn now() {
		let dt = Utc::now();
		let serialized = dt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dt, serialized);
	}
}
