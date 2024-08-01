use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Permission;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub mod opt;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Permission;
	type Error = Error;

	type SerializeSeq = Impossible<Permission, Error>;
	type SerializeTuple = Impossible<Permission, Error>;
	type SerializeTupleStruct = Impossible<Permission, Error>;
	type SerializeTupleVariant = Impossible<Permission, Error>;
	type SerializeMap = Impossible<Permission, Error>;
	type SerializeStruct = Impossible<Permission, Error>;
	type SerializeStructVariant = Impossible<Permission, Error>;

	const EXPECTED: &'static str = "an enum `Permission`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"None" => Ok(Permission::None),
			"Full" => Ok(Permission::Full),
			variant => Err(Error::custom(format!("unexpected unit variant `{name}::{variant}`"))),
		}
	}

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
			"Specific" => Ok(Permission::Specific(value.serialize(ser::value::Serializer.wrap())?)),
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
	fn none() {
		let perm = Permission::None;
		let serialized = perm.serialize(Serializer.wrap()).unwrap();
		assert_eq!(perm, serialized);
	}

	#[test]
	fn full() {
		let perm = Permission::Full;
		let serialized = perm.serialize(Serializer.wrap()).unwrap();
		assert_eq!(perm, serialized);
	}

	#[test]
	fn specific() {
		let perm = Permission::Specific(Default::default());
		let serialized = perm.serialize(Serializer.wrap()).unwrap();
		assert_eq!(perm, serialized);
	}
}
