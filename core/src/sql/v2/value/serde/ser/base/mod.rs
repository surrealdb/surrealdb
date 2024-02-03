pub(super) mod opt;

use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Base;
use crate::sql::Ident;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Base;
	type Error = Error;

	type SerializeSeq = Impossible<Base, Error>;
	type SerializeTuple = Impossible<Base, Error>;
	type SerializeTupleStruct = Impossible<Base, Error>;
	type SerializeTupleVariant = Impossible<Base, Error>;
	type SerializeMap = Impossible<Base, Error>;
	type SerializeStruct = Impossible<Base, Error>;
	type SerializeStructVariant = Impossible<Base, Error>;

	const EXPECTED: &'static str = "an enum `Base`";

	#[inline]
	fn serialize_unit_variant(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
	) -> Result<Self::Ok, Error> {
		match variant {
			"Root" => Ok(Base::Root),
			"Ns" => Ok(Base::Ns),
			"Db" => Ok(Base::Db),
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
			"Sc" => Ok(Base::Sc(Ident(value.serialize(ser::string::Serializer.wrap())?))),
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
	fn root() {
		let base = Base::Root;
		let serialized = base.serialize(Serializer.wrap()).unwrap();
		assert_eq!(base, serialized);
	}

	#[test]
	fn ns() {
		let base = Base::Ns;
		let serialized = base.serialize(Serializer.wrap()).unwrap();
		assert_eq!(base, serialized);
	}

	#[test]
	fn db() {
		let base = Base::Db;
		let serialized = base.serialize(Serializer.wrap()).unwrap();
		assert_eq!(base, serialized);
	}

	#[test]
	fn sc() {
		let base = Base::Sc(Default::default());
		let serialized = base.serialize(Serializer.wrap()).unwrap();
		assert_eq!(base, serialized);
	}
}
