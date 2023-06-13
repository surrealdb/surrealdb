use crate::err::Error;
use crate::sql::value::serde::ser;
use chrono::offset::Utc;
use chrono::DateTime;
use chrono::TimeZone;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = DateTime<Utc>;
	type Error = Error;

	type SerializeSeq = Impossible<DateTime<Utc>, Error>;
	type SerializeTuple = SerializeDateTime;
	type SerializeTupleStruct = Impossible<DateTime<Utc>, Error>;
	type SerializeTupleVariant = Impossible<DateTime<Utc>, Error>;
	type SerializeMap = Impossible<DateTime<Utc>, Error>;
	type SerializeStruct = Impossible<DateTime<Utc>, Error>;
	type SerializeStructVariant = Impossible<DateTime<Utc>, Error>;

	const EXPECTED: &'static str = "a struct `DateTime<Utc>`";

	#[inline]
	fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
		debug_assert_eq!(len, 2);
		Ok(SerializeDateTime::default())
	}
}

#[derive(Default)]
pub(super) struct SerializeDateTime {
	secs: Option<i64>,
	nanos: Option<u32>,
}

impl serde::ser::SerializeTuple for SerializeDateTime {
	type Ok = DateTime<Utc>;
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
				.ok_or_else(|| Error::custom("invalid `Datetime`")),
			_ => Err(Error::custom("`Datetime` missing required value(s)")),
		}
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
