use std::io::{Read, Write};
use std::ops::{Deref, DerefMut};

use revision::{
	BorrowedReader, DeserializeRevisioned, Error, LeafWalker, Revisioned, SerializeRevisioned,
	SkipRevisioned, WalkRevisioned,
};
use surrealdb_kvs::release::Release;

use super::{Encoder, Floor, Item, SchemaError, StoredField, TypeSchema};
use crate::val::Value;

/// A field the `revision` encoding of a frozen type leaves out.
///
/// It serializes to nothing, deserializes as `T::default()`, and skips and walks nothing, so a
/// type that is still written in `revision` for readers that predate the field loses nothing when
/// it gains one: those readers never saw the bytes. The stored grammar encodes the field as `T`,
/// absent when it equals `T::default()`.
///
/// A value that holds a non-default `Unrevisioned` field cannot be written in `revision`: see
/// [`StoredField::unrevisioned_floor`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Unrevisioned<T>(pub T);

impl<T> Deref for Unrevisioned<T> {
	type Target = T;

	fn deref(&self) -> &T {
		&self.0
	}
}

impl<T> DerefMut for Unrevisioned<T> {
	fn deref_mut(&mut self) -> &mut T {
		&mut self.0
	}
}

impl<T> Revisioned for Unrevisioned<T> {
	fn revision() -> u16 {
		1
	}
}

impl<T> SerializeRevisioned for Unrevisioned<T> {
	#[inline]
	fn serialize_revisioned<W: Write>(&self, _writer: &mut W) -> Result<(), Error> {
		Ok(())
	}
}

impl<T: Default> DeserializeRevisioned for Unrevisioned<T> {
	#[inline]
	fn deserialize_revisioned<R: Read>(_reader: &mut R) -> Result<Self, Error> {
		Ok(Self(T::default()))
	}
}

impl<T> SkipRevisioned for Unrevisioned<T> {
	#[inline]
	fn skip_revisioned<R: Read>(_reader: &mut R) -> Result<(), Error> {
		Ok(())
	}
}

impl<T> WalkRevisioned for Unrevisioned<T> {
	type Walker<'r, R: BorrowedReader + 'r> = LeafWalker<'r, Unrevisioned<T>, R>;

	#[inline]
	fn walk_revisioned<'r, R: BorrowedReader>(
		reader: &'r mut R,
	) -> Result<Self::Walker<'r, R>, Error> {
		Ok(LeafWalker::new(reader))
	}
}

impl<T: StoredField> StoredField for Unrevisioned<T> {
	const NULLABLE: bool = T::NULLABLE;

	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError> {
		self.0.encode_item(enc, prior)
	}

	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError> {
		T::decode_item(item, depth).map(Self)
	}

	fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
		self.0.floor(acc)
	}

	fn unrevisioned_floor(&self) -> Option<Release> {
		self.0.unrevisioned_floor()
	}

	fn into_vm_value(self) -> Result<Value, SchemaError> {
		self.0.into_vm_value()
	}

	fn from_vm_value(value: Value) -> Result<Self, SchemaError> {
		T::from_vm_value(value).map(Self)
	}

	fn write_type_name(out: &mut String) {
		T::write_type_name(out);
	}

	fn collect_schemas(out: &mut Vec<&'static TypeSchema>) {
		T::collect_schemas(out);
	}
}
