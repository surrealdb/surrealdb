use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Permission;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Option<Permission>;
	type Error = Error;

	type SerializeSeq = Impossible<Option<Permission>, Error>;
	type SerializeTuple = Impossible<Option<Permission>, Error>;
	type SerializeTupleStruct = Impossible<Option<Permission>, Error>;
	type SerializeTupleVariant = Impossible<Option<Permission>, Error>;
	type SerializeMap = Impossible<Option<Permission>, Error>;
	type SerializeStruct = Impossible<Option<Permission>, Error>;
	type SerializeStructVariant = Impossible<Option<Permission>, Error>;

	const EXPECTED: &'static str = "an `Option<Permission>`";

	#[inline]
	fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
		Ok(None)
	}

	#[inline]
	fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
	where
		T: ?Sized + Serialize,
	{
		Ok(Some(value.serialize(ser::permission::Serializer.wrap())?))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use ser::Serializer as _;

	#[test]
	fn none() {
		let option: Option<Permission> = None;
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}

	#[test]
	fn some() {
		let option = Some(Permission::default());
		let serialized = option.serialize(Serializer.wrap()).unwrap();
		assert_eq!(option, serialized);
	}
}
