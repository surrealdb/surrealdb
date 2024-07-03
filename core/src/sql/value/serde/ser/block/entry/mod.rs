pub mod vec;

use crate::err::Error;
use crate::sql::block::Entry;
use crate::sql::value::serde::ser;
use serde::ser::Error as _;
use serde::ser::Impossible;
use serde::ser::Serialize;

pub(super) struct Serializer;

impl ser::Serializer for Serializer {
	type Ok = Entry;
	type Error = Error;

	type SerializeSeq = Impossible<Entry, Error>;
	type SerializeTuple = Impossible<Entry, Error>;
	type SerializeTupleStruct = Impossible<Entry, Error>;
	type SerializeTupleVariant = Impossible<Entry, Error>;
	type SerializeMap = Impossible<Entry, Error>;
	type SerializeStruct = Impossible<Entry, Error>;
	type SerializeStructVariant = Impossible<Entry, Error>;

	const EXPECTED: &'static str = "an enum `Entry`";

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
			"Value" => Ok(Entry::Value(value.serialize(ser::value::Serializer.wrap())?)),
			"Set" => Ok(Entry::Set(value.serialize(ser::statement::set::Serializer.wrap())?)),
			"Throw" => Ok(Entry::Throw(value.serialize(ser::statement::throw::Serializer.wrap())?)),
			"Break" => {
				Ok(Entry::Break(value.serialize(ser::statement::r#break::Serializer.wrap())?))
			}
			"Ifelse" => {
				Ok(Entry::Ifelse(value.serialize(ser::statement::ifelse::Serializer.wrap())?))
			}
			"Select" => {
				Ok(Entry::Select(value.serialize(ser::statement::select::Serializer.wrap())?))
			}
			"Create" => {
				Ok(Entry::Create(value.serialize(ser::statement::create::Serializer.wrap())?))
			}
			"Upsert" => {
				Ok(Entry::Upsert(value.serialize(ser::statement::upsert::Serializer.wrap())?))
			}
			"Update" => {
				Ok(Entry::Update(value.serialize(ser::statement::update::Serializer.wrap())?))
			}
			"Delete" => {
				Ok(Entry::Delete(value.serialize(ser::statement::delete::Serializer.wrap())?))
			}
			"Relate" => {
				Ok(Entry::Relate(value.serialize(ser::statement::relate::Serializer.wrap())?))
			}
			"Insert" => {
				Ok(Entry::Insert(value.serialize(ser::statement::insert::Serializer.wrap())?))
			}
			"Output" => {
				Ok(Entry::Output(value.serialize(ser::statement::output::Serializer.wrap())?))
			}
			"Define" => {
				Ok(Entry::Define(value.serialize(ser::statement::define::Serializer.wrap())?))
			}
			"Remove" => {
				Ok(Entry::Remove(value.serialize(ser::statement::remove::Serializer.wrap())?))
			}
			"Continue" => {
				Ok(Entry::Continue(value.serialize(ser::statement::r#continue::Serializer.wrap())?))
			}
			variant => Err(Error::custom(format!("unexpected variant `{name}::{variant}`"))),
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
		let entry = Entry::Value(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn set() {
		let entry = Entry::Set(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn ifelse() {
		let entry = Entry::Ifelse(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn select() {
		let entry = Entry::Select(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn create() {
		let entry = Entry::Create(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn upsert() {
		let entry = Entry::Upsert(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn update() {
		let entry = Entry::Update(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn delete() {
		let entry = Entry::Delete(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn relate() {
		let entry = Entry::Relate(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn insert() {
		let entry = Entry::Insert(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}

	#[test]
	fn output() {
		let entry = Entry::Output(Default::default());
		let serialized = entry.serialize(Serializer.wrap()).unwrap();
		assert_eq!(entry, serialized);
	}
}
