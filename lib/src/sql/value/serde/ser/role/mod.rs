pub(crate) mod vec;
use crate::err::Error;
use crate::iam::Role;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Role;
	type Error = Error;

	type SerializeSeq = Impossible<Role, Error>;
	type SerializeTuple = Impossible<Role, Error>;
	type SerializeTupleStruct = Impossible<Role, Error>;
	type SerializeTupleVariant = Impossible<Role, Error>;
	type SerializeMap = Impossible<Role, Error>;
	type SerializeStruct = Impossible<Role, Error>;
	type SerializeStructVariant = Impossible<Role, Error>;

	const EXPECTED: &'static str = "an enum `Role`";

	#[inline]
	fn serialize_newtype_variant<T>(
		self,
		name: &'static str,
		_variant_index: u32,
		variant: &'static str,
		_value: &T,
	) -> Result<Self::Ok, Error>
	where
		T: ?Sized + Serialize,
	{
		match variant {
			"Viewer" => Ok(Role::Viewer),
			"Editor" => Ok(Role::Editor),
			"Owner" => Ok(Role::Owner),
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
	use serde::Serialize;

	#[test]
	fn int() {
		let number = Number::Int(Default::default());
		let serialized = number.serialize(Serializer.wrap()).unwrap();
		assert_eq!(number, serialized);
	}

	#[test]
	fn float() {
		let number = Number::Float(Default::default());
		let serialized = number.serialize(Serializer.wrap()).unwrap();
		assert_eq!(number, serialized);
	}

	#[test]
	fn decimal() {
		let number = Number::Decimal(Default::default());
		let serialized = number.serialize(Serializer.wrap()).unwrap();
		assert_eq!(number, serialized);
	}
}
