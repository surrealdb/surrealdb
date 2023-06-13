use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Datetime;
use chrono::offset::Utc;
use chrono::TimeZone;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Datetime;
	type Error = Error;

	type SerializeSeq = Impossible<Datetime, Error>;
	type SerializeTuple = SerializeDatetime;
	type SerializeTupleStruct = Impossible<Datetime, Error>;
	type SerializeTupleVariant = Impossible<Datetime, Error>;
	type SerializeMap = Impossible<Datetime, Error>;
	type SerializeStruct = Impossible<Datetime, Error>;
	type SerializeStructVariant = Impossible<Datetime, Error>;

	const EXPECTED: &'static str = "a struct `Datetime`";

	#[inline]
	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		debug_assert_eq!(len, 2);
		Ok(SerializeDatetime::default())
	}

	#[inline]
	fn serialize_newtype_struct<T>(
		self,
		name: &'static str,
		value: &T,
	) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		debug_assert_eq!(name, crate::sql::datetime::TOKEN);
		value.serialize(self.wrap())
	}
}

#[derive(Default)]
pub(super) struct SerializeDatetime {
	secs: Option<i64>,
	nanos: Option<u32>,
}

impl serde::ser::SerializeTuple for SerializeDatetime {
	type Ok = Datetime;
	type Error = Error;

	fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
	where
		T: Serialize + ?Sized,
	{
		if self.secs.is_none() {
			self.secs = Some(value.serialize(ser::primitive::i64::Serializer.wrap())?);
		} else if self.nanos.is_none() {
			self.nanos = Some(value.serialize(ser::primitive::u32::Serializer.wrap())?);
		} else {
			return Err(Error::custom(format!("unexpected `Datetime` 3rd field`")));
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Self::Error> {
		match (self.secs, self.nanos) {
			(Some(secs), Some(nanos)) => Utc
				.timestamp_opt(secs, nanos)
				.single()
				.map(Datetime)
				.ok_or_else(|| Error::custom("invalid `Datetime`")),
			_ => Err(Error::custom("`Datetime` missing required value(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::Datetime;
	use serde::Serialize;

	#[test]
	fn now() {
		let dt = Datetime::from(Utc::now());
		let serialized = dt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(dt, serialized);
	}
}
