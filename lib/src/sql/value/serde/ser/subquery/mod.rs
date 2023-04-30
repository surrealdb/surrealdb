use crate::err::Error;
use crate::sql::value::serde::ser;
use crate::sql::Subquery;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Subquery;
	type Error = Error;

	type SerializeSeq = Impossible<Subquery, Error>;
	type SerializeTuple = Impossible<Subquery, Error>;
	type SerializeTupleStruct = Impossible<Subquery, Error>;
	type SerializeTupleVariant = Impossible<Subquery, Error>;
	type SerializeMap = Impossible<Subquery, Error>;
	type SerializeStruct = Impossible<Subquery, Error>;
	type SerializeStructVariant = Impossible<Subquery, Error>;

	const EXPECTED: &'static str = "an enum `Subquery`";

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
			"Value" => Ok(Subquery::Value(value.serialize(ser::value::Serializer.wrap())?)),
			"Ifelse" => {
				Ok(Subquery::Ifelse(value.serialize(ser::statement::ifelse::Serializer.wrap())?))
			}
			"Output" => {
				Ok(Subquery::Output(value.serialize(ser::statement::output::Serializer.wrap())?))
			}
			"Select" => {
				Ok(Subquery::Select(value.serialize(ser::statement::select::Serializer.wrap())?))
			}
			"Create" => {
				Ok(Subquery::Create(value.serialize(ser::statement::create::Serializer.wrap())?))
			}
			"Update" => {
				Ok(Subquery::Update(value.serialize(ser::statement::update::Serializer.wrap())?))
			}
			"Delete" => {
				Ok(Subquery::Delete(value.serialize(ser::statement::delete::Serializer.wrap())?))
			}
			"Relate" => {
				Ok(Subquery::Relate(value.serialize(ser::statement::relate::Serializer.wrap())?))
			}
			"Insert" => {
				Ok(Subquery::Insert(value.serialize(ser::statement::insert::Serializer.wrap())?))
			}
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
	fn value() {
		let subquery = Subquery::Value(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}

	#[test]
	fn ifelse() {
		let subquery = Subquery::Ifelse(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}

	#[test]
	fn output() {
		let subquery = Subquery::Output(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}

	#[test]
	fn select() {
		let subquery = Subquery::Select(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}

	#[test]
	fn create() {
		let subquery = Subquery::Create(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}

	#[test]
	fn update() {
		let subquery = Subquery::Update(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}

	#[test]
	fn delete() {
		let subquery = Subquery::Delete(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}

	#[test]
	fn relate() {
		let subquery = Subquery::Relate(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}

	#[test]
	fn insert() {
		let subquery = Subquery::Insert(Default::default());
		let serialized = subquery.serialize(Serializer.wrap()).unwrap();
		assert_eq!(subquery, serialized);
	}
}
