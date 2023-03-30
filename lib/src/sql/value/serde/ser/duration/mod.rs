pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use ser::Serializer as _;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;
use std::time::Duration;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Duration;
	type Error = Error;

	type SerializeSeq = Impossible<Duration, Error>;
	type SerializeTuple = Impossible<Duration, Error>;
	type SerializeTupleStruct = Impossible<Duration, Error>;
	type SerializeTupleVariant = Impossible<Duration, Error>;
	type SerializeMap = Impossible<Duration, Error>;
	type SerializeStruct = SerializeDuration;
	type SerializeStructVariant = Impossible<Duration, Error>;

	const EXPECTED: &'static str = "a struct `Duration`";

	#[inline]
	fn serialize_struct(
		self,
		_name: &'static str,
		_len: usize,
	) -> Result<Self::SerializeStruct, Error> {
		Ok(SerializeDuration::default())
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
pub(super) struct SerializeDuration {
	secs: Option<u64>,
	nanos: Option<u32>,
}

impl serde::ser::SerializeStruct for SerializeDuration {
	type Ok = Duration;
	type Error = Error;

	fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
	where
		T: ?Sized + Serialize,
	{
		match key {
			"secs" => {
				self.secs = Some(value.serialize(ser::primitive::u64::Serializer.wrap())?);
			}
			"nanos" => {
				self.nanos = Some(value.serialize(ser::primitive::u32::Serializer.wrap())?);
			}
			key => {
				return Err(Error::custom(format!("unexpected field `Duration::{key}`")));
			}
		}
		Ok(())
	}

	fn end(self) -> Result<Self::Ok, Error> {
		match (self.secs, self.nanos) {
			(Some(secs), Some(nanos)) => Ok(Duration::new(secs, nanos)),
			_ => Err(Error::custom("`Duration` missing required field(s)")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::serde::serialize_internal;
	use serde::Serialize;

	#[test]
	fn default() {
		let duration = Duration::default();
		let serialized = serialize_internal(|| duration.serialize(Serializer.wrap())).unwrap();
		assert_eq!(duration, serialized);
	}
}
