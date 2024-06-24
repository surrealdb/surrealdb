mod index;

use crate::err::Error;
use crate::sql::statements::rebuild::RebuildStatement;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

#[non_exhaustive]
pub struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = RebuildStatement;
	type Error = Error;

	type SerializeSeq = Impossible<RebuildStatement, Error>;
	type SerializeTuple = Impossible<RebuildStatement, Error>;
	type SerializeTupleStruct = Impossible<RebuildStatement, Error>;
	type SerializeTupleVariant = Impossible<RebuildStatement, Error>;
	type SerializeMap = Impossible<RebuildStatement, Error>;
	type SerializeStruct = Impossible<RebuildStatement, Error>;
	type SerializeStructVariant = Impossible<RebuildStatement, Error>;

	const EXPECTED: &'static str = "an enum `RebuildStatement`";

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
			"Index" => Ok(RebuildStatement::Index(value.serialize(index::Serializer.wrap())?)),
			variant => {
				Err(Error::custom(format!("unexpected newtype variant `{name}::{variant}`")))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::statements::rebuild::RebuildStatement;
	use ser::Serializer as _;

	#[test]
	fn index() {
		let stmt = RebuildStatement::Index(Default::default());
		let serialized = stmt.serialize(Serializer.wrap()).unwrap();
		assert_eq!(stmt, serialized);
	}
}
