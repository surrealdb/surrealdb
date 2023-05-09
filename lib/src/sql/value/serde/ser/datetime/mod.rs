use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Datetime;
use chrono::offset::Utc;
use chrono::DateTime;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

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

	fn serialize_i64(self, value: i64) -> Result<Self::Ok, Self::Error> {
		Datetime::from_nanos(value)
			.map(|d| d.0)
			.ok_or_else(|| Error::custom("invalid datetime nanos"))
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
	use serde::Serialize;

	#[test]
	fn now() {
		let dt = Utc::now();
		let serialized = dt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dt, serialized);
	}
}
