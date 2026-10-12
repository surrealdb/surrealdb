//! Schema types: Rust types with a declared stored form.
//!
//! A schema type is read and written as a keyed map from field number to value (the MAP container
//! of the stored-value grammar). Its attribute table, the `#[derive(Stored)]` attributes, is the
//! one declaration of its numbers, their types, the release each arrived in and each field's class.
//! The derive generates from it the encoder, the decoder, the release floor, the conversions to and
//! from the VM's values and the static [`TypeSchema`] that the schema lock renders, so none of them
//! can drift from the type.
//!
//! # Reading
//!
//! The class of a field is the low bit of its number. A reader skips an unknown odd (ignorable)
//! number and refuses an unknown even (critical) one. A retired number is reserved: always skipped,
//! never reused. An enum is a one-entry map from variant number to payload, and a reader refuses a
//! variant it does not know.
//!
//! Every read checks the bytes in hand before it indexes them, applies the generic decoder's
//! payload checks to scalars, and nests no deeper than [`MAX_SCHEMA_DEPTH`] maps. A malformed input
//! is a [`SchemaError`], never a panic and never a value read at a wrong offset.
//!
//! # Writing
//!
//! The encoder writes one byte string for a pair of (value, prior): the entries in ascending
//! number order, each field written only when it differs from its absence, and every unknown
//! ignorable entry of the prior bytes carried over unchanged, so that a build that does not know a
//! field never drops it by rewriting an entry. A prior that is not a well-formed map contributes
//! nothing. Carried entries are copied without being decoded.
//!
//! # Floors
//!
//! [`StoredField::floor`] folds the release a value needs into a [`Floor`]: the newest release
//! among the critical fields written, the variants written, and what the values use. An ignorable
//! field raises the floor only through its value, because an earlier release that does not know
//! the field skips it while one that knows it still reads its value.

pub use surrealdb_kvs::release::Release;
pub use surrealdb_stored_derive::Stored;

use super::{Encoder, Item};
use crate::val::Value;

mod describe;
mod error;
mod iam;
mod prim;
pub mod rt;
mod unrevisioned;
pub mod vm;

pub use describe::{Absent, Class, FieldSchema, Payload, Shape, TypeSchema, VariantSchema};
pub use error::SchemaError;
pub use unrevisioned::Unrevisioned;

/// The number of nested schema maps a decoder enters before it refuses.
pub const MAX_SCHEMA_DEPTH: u32 = 32;

/// The release a value needs, as the encoder writes it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Floor {
	/// The newest release among everything the value writes and uses.
	pub release: Release,
	/// A stored SurrealQL text in the value does not parse, so its release is unknown. The entry's
	/// floor then also holds the floor of the entry it replaces.
	pub unparsable_text: bool,
}

impl Floor {
	/// The floor of a value that uses nothing newer than the first release.
	pub const fn first() -> Self {
		Self {
			release: Release::FIRST_STORED,
			unparsable_text: false,
		}
	}

	/// Raises the floor to `release` when that is newer.
	pub fn raise(&mut self, release: Release) {
		if release > self.release {
			self.release = release;
		}
	}
}

/// A Rust type with a stored-grammar item form.
///
/// The generated impls of `#[derive(Stored)]` are the usual implementors. A hand-written impl keeps
/// to the same contract: an encoded item is at least one byte, a decoder checks every byte it
/// reads, and every scalar is read through the strict accessors, which apply exactly the generic
/// decoder's payload checks.
pub trait StoredField: Sized + PartialEq {
	/// Whether the VM reads an `Option` of this type as `[x]` for `Some(x)`, because the type can
	/// itself be `NONE`.
	const NULLABLE: bool = false;

	/// Prepends this value's item to `enc`. `prior` is the item this value replaces, whose unknown
	/// ignorable entries the new item carries over.
	fn encode_item(&self, enc: &mut Encoder, prior: Option<Item<'_>>) -> Result<(), SchemaError>;

	/// Reads an item. `depth` counts the schema maps entered so far.
	fn decode_item(item: Item<'_>, depth: u32) -> Result<Self, SchemaError>;

	/// Folds this value's floor into `acc`. A scalar adds nothing.
	fn floor(&self, acc: &mut Floor) -> Result<(), SchemaError> {
		let _ = acc;
		Ok(())
	}

	/// The newest release of an [`Unrevisioned`] field holding a non-default value, if any: the
	/// `revision` encoding cannot hold such a value.
	fn unrevisioned_floor(&self) -> Option<Release> {
		None
	}

	/// The value the VM holds for this, in the shape its programs build: a struct as an object, an
	/// enum as `{Variant: body}`.
	fn into_vm_value(self) -> Result<Value, SchemaError>;

	/// [`StoredField::into_vm_value`] of a copy.
	fn to_vm_value(&self) -> Result<Value, SchemaError>
	where
		Self: Clone,
	{
		self.clone().into_vm_value()
	}

	/// The inverse of [`StoredField::into_vm_value`].
	fn from_vm_value(value: Value) -> Result<Self, SchemaError>;

	/// Appends the canonical wire type name that the schema lock records. Aliases are not
	/// distinct types: only a change of what the bytes mean changes the name.
	fn write_type_name(out: &mut String);

	/// Appends the wire type name of a list of these.
	fn write_list_type_name(out: &mut String) {
		out.push_str("list<");
		Self::write_type_name(out);
		out.push('>');
	}

	/// Adds the schema of every derived type this type holds, each once.
	fn collect_schemas(out: &mut Vec<&'static TypeSchema>) {
		let _ = out;
	}

	/// Prepends a list of these: an ARRAY container of items. An element carries the prior element
	/// at its position only when the two are equal, so a reorder never moves one element's unknown
	/// entries onto another.
	fn encode_list(
		items: &[Self],
		enc: &mut Encoder,
		prior: Option<Item<'_>>,
	) -> Result<(), SchemaError> {
		let prior = match prior {
			Some(p) => p.list().ok().flatten(),
			None => None,
		};
		let mut open = enc.open()?;
		for (i, element) in items.iter().enumerate().rev() {
			let carried = prior.and_then(|list| {
				let item = list.get(i).ok()?;
				(Self::decode_item(item, 0).ok()? == *element).then_some(item)
			});
			element.encode_item(enc, carried)?;
			enc.end_item(&mut open)?;
		}
		enc.close_list(open)?;
		Ok(())
	}

	/// Reads a list of these.
	fn decode_list(item: Item<'_>, depth: u32) -> Result<Vec<Self>, SchemaError> {
		let list = item.list()?.ok_or(SchemaError::wrong_kind())?;
		// Each element is at least one byte, so the list cannot hold more elements than the bytes
		// it occupies.
		let mut out = Vec::with_capacity(list.len().min(item.bytes().len()));
		for i in 0..list.len() {
			out.push(Self::decode_item(list.get(i)?, depth)?);
		}
		Ok(out)
	}

	/// The VM value of a list of these: an array of each element's VM value.
	fn into_vm_list(items: Vec<Self>) -> Result<Value, SchemaError> {
		vm::list_into(items)
	}

	/// The inverse of [`StoredField::into_vm_list`].
	fn from_vm_list(value: Value) -> Result<Vec<Self>, SchemaError> {
		vm::list_from(value)
	}

	/// The floor of this value alone.
	fn floor_of(&self) -> Result<Floor, SchemaError> {
		let mut floor = Floor::first();
		self.floor(&mut floor)?;
		Ok(floor)
	}
}

/// A derived type: a struct's map or an enum's one-entry map, with a schema.
pub trait StoredType: StoredField {
	const SCHEMA: TypeSchema;
}
