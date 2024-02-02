use crate::err::Error;
use crate::sql::datetime::Datetime;
use crate::sql::statements::show::ShowSince;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = ShowSince;
	type Error = Error;

	type SerializeSeq = Impossible<ShowSince, Error>;
	type SerializeTuple = Impossible<ShowSince, Error>;
	type SerializeTupleStruct = Impossible<ShowSince, Error>;
	type SerializeTupleVariant = Impossible<ShowSince, Error>;
	type SerializeMap = Impossible<ShowSince, Error>;
	type SerializeStruct = Impossible<ShowSince, Error>;
	type SerializeStructVariant = Impossible<ShowSince, Error>;

	const EXPECTED: &'static str = "an enum `ShowSince`";

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Timestamp" => Ok(ShowSince::Timestamp(Datetime(
				value.serialize(ser::datetime::Serializer.wrap())?,
			))),
			"Versionstamp" => Ok(ShowSince::Versionstamp(
				value.serialize(ser::primitive::u64::Serializer.wrap())?,
			)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn timestamp() {
		let stmt = ShowSince::Timestamp(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}

	#[test]
	fn versionstamp() {
		let stmt = ShowSince::Versionstamp(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}
}
