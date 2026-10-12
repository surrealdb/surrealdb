//! The catalog's type table, for the VM's catalog codec.
//!
//! The VM holds catalog values as generic values and reads and writes their bytes through one
//! [`CatalogVTable`] per catalog type. The table's functions are the derive's own: decoding is the
//! derived decoder followed by `into_vm_value`, encoding is `from_vm_value` followed by the entry
//! encoder, so the VM's codec cannot drift from the Rust types.

use revision::{DeserializeRevisioned, SerializeRevisioned, SkipRevisioned};
use surrealdb_expr::val::Value;
use surrealdb_expr::val::stored::schema::{SchemaError, StoredType, TypeSchema};
use surrealdb_kvs::{EncodeContext, Encoded};

use super::embedded::{read_embedded, write_embedded, write_plain};
use super::entry::{
	decode_entry, decode_entry_or_revision, encode_entry, encode_stored, read_revision_prefix,
	write_error,
};
use super::error::EntryError;
use crate::node::Node;
use crate::{
	AccessGrant, AnalyzerDefinition, DatabaseDefinition, Error, NamespaceDefinition, NodeLiveQuery,
	SequenceDefinition, StoredAccessDefinition, StoredApiDefinition, StoredBucketDefinition,
	StoredConfigDefinition, StoredEventDefinition, StoredFieldDefinition, StoredFunctionDefinition,
	StoredIndexDefinition, StoredMlModelDefinition, StoredModuleDefinition, StoredParamDefinition,
	StoredSubscriptionDefinition, StoredTableDefinition, TaskLease, UserDefinition,
};

/// The VM's view of one catalog type.
#[derive(Debug)]
pub struct CatalogVTable {
	/// The Rust type name, which is the name the VM's type descriptors use.
	pub name: &'static str,
	/// Reads an entry in either grammar into the VM's value. Bounded by the input.
	pub decode: fn(&[u8]) -> anyhow::Result<Value>,
	/// Writes the VM's value as an entry at `ctx`.
	pub encode_at: fn(&Value, &EncodeContext<'_>) -> anyhow::Result<Encoded>,
	/// Reads the copy held inside a `revision` container from the front of the slice, and advances
	/// the slice past it. Its extent is proved against the bytes present first.
	pub decode_embedded: fn(&mut &[u8]) -> anyhow::Result<Value>,
	/// Appends the copy held inside a `revision` container.
	pub encode_embedded: fn(&Value, &mut Vec<u8>) -> anyhow::Result<()>,
	/// Adds the schema of this type and of every derived type it holds.
	pub schemas: fn(&mut Vec<&'static TypeSchema>),
	/// Whether the bytes are exactly one value in the `revision` encoding, checked by skipping it
	/// rather than decoding it. Always false for a type with no `revision` form.
	pub is_revision: fn(&[u8]) -> bool,
}

/// Builds the [`CatalogVTable`] of a type.
///
/// `catalog_vtable!(T)` is for a type written in both grammars; `catalog_vtable!(stored T)` is for
/// a type that has no `revision` form, whose container copies are refused.
#[macro_export]
macro_rules! catalog_vtable {
	($t:ty) => {
		$crate::stored::CatalogVTable {
			name: <$t as $crate::stored::StoredType>::SCHEMA.name,
			decode: $crate::stored::vtable::decode::<$t>,
			encode_at: $crate::stored::vtable::encode_at::<$t>,
			decode_embedded: $crate::stored::vtable::decode_embedded::<$t>,
			encode_embedded: $crate::stored::vtable::encode_embedded::<$t>,
			schemas: $crate::stored::vtable::schemas::<$t>,
			is_revision: $crate::stored::vtable::is_revision::<$t>,
		}
	};
	(stored $t:ty) => {
		$crate::stored::CatalogVTable {
			name: <$t as $crate::stored::StoredType>::SCHEMA.name,
			decode: $crate::stored::vtable::decode_stored::<$t>,
			encode_at: $crate::stored::vtable::encode_stored_at::<$t>,
			decode_embedded: $crate::stored::vtable::no_embedded_decode::<$t>,
			encode_embedded: $crate::stored::vtable::no_embedded_encode::<$t>,
			schemas: $crate::stored::vtable::schemas::<$t>,
			is_revision: $crate::stored::vtable::no_revision,
		}
	};
}

/// The catalog types a VM program reads or writes through a key route, by Rust type name.
///
/// The types only the datastore's own code reads and writes (the feature level record, layout
/// descriptors, node release records, generation marks, generation rings and conformance records)
/// are not listed: no VM program reaches them, and the schema lock reads their schemas from the
/// types themselves ([`super::lock::all_schemas`]).
pub static CATALOG_TYPES: &[CatalogVTable] = &[
	catalog_vtable!(NamespaceDefinition),
	catalog_vtable!(DatabaseDefinition),
	catalog_vtable!(StoredTableDefinition),
	catalog_vtable!(StoredFieldDefinition),
	catalog_vtable!(StoredIndexDefinition),
	catalog_vtable!(StoredEventDefinition),
	catalog_vtable!(StoredFunctionDefinition),
	catalog_vtable!(StoredParamDefinition),
	catalog_vtable!(StoredAccessDefinition),
	catalog_vtable!(UserDefinition),
	catalog_vtable!(AnalyzerDefinition),
	catalog_vtable!(SequenceDefinition),
	catalog_vtable!(StoredModuleDefinition),
	catalog_vtable!(StoredMlModelDefinition),
	catalog_vtable!(StoredBucketDefinition),
	catalog_vtable!(StoredApiDefinition),
	catalog_vtable!(StoredConfigDefinition),
	catalog_vtable!(StoredSubscriptionDefinition),
	catalog_vtable!(Node),
	catalog_vtable!(TaskLease),
	catalog_vtable!(NodeLiveQuery),
	catalog_vtable!(AccessGrant),
];

/// A value that the type's schema cannot convert, refused as [`write_error`] refuses it.
fn convert(ty: &'static str, error: SchemaError) -> anyhow::Error {
	write_error(EntryError::body(ty, error))
}

/// [`CatalogVTable::decode`] of a type with a `revision` form.
pub fn decode<T>(bytes: &[u8]) -> anyhow::Result<Value>
where
	T: StoredType + DeserializeRevisioned + SkipRevisioned,
{
	decode_entry_or_revision::<T>(bytes)?
		.into_vm_value()
		.map_err(|error| convert(T::SCHEMA.name, error))
}

/// [`CatalogVTable::decode`] of a type with no `revision` form.
pub fn decode_stored<T: StoredType>(bytes: &[u8]) -> anyhow::Result<Value> {
	decode_entry::<T>(bytes)
		.map_err(Error::Entry)?
		.into_vm_value()
		.map_err(|error| convert(T::SCHEMA.name, error))
}

/// [`CatalogVTable::encode_at`] of a type with a `revision` form.
pub fn encode_at<T>(value: &Value, ctx: &EncodeContext<'_>) -> anyhow::Result<Encoded>
where
	T: StoredType + SerializeRevisioned,
{
	let value = T::from_vm_value(value.clone()).map_err(|error| convert(T::SCHEMA.name, error))?;
	encode_entry(&value, ctx).map_err(write_error)
}

/// [`CatalogVTable::encode_at`] of a type with no `revision` form.
pub fn encode_stored_at<T: StoredType>(
	value: &Value,
	ctx: &EncodeContext<'_>,
) -> anyhow::Result<Encoded> {
	let value = T::from_vm_value(value.clone()).map_err(|error| convert(T::SCHEMA.name, error))?;
	encode_stored(&value, ctx).map_err(write_error)
}

/// [`CatalogVTable::decode_embedded`]: a type that is embedded in a `revision` container is read
/// as its `revision` bytes or its framed entry, and any other type as its `revision` bytes.
pub fn decode_embedded<T>(cursor: &mut &[u8]) -> anyhow::Result<Value>
where
	T: StoredType + DeserializeRevisioned + SkipRevisioned,
{
	let value = if T::SCHEMA.embedded_in_revision {
		read_embedded::<T>(cursor)?
	} else {
		let (value, rest) = read_revision_prefix::<T>(cursor)?;
		*cursor = rest;
		value
	};
	value.into_vm_value().map_err(|error| convert(T::SCHEMA.name, error))
}

/// [`CatalogVTable::encode_embedded`]: a type that is embedded in a `revision` container is
/// written as its `revision` bytes when they hold it exactly and as a framed entry otherwise. Any
/// other type is written as its `revision` bytes, and refused when they cannot hold it.
pub fn encode_embedded<T>(value: &Value, out: &mut Vec<u8>) -> anyhow::Result<()>
where
	T: StoredType + SerializeRevisioned,
{
	let value = T::from_vm_value(value.clone()).map_err(|error| convert(T::SCHEMA.name, error))?;
	if T::SCHEMA.embedded_in_revision {
		write_embedded(&value, out)
	} else {
		write_plain(&value, out)
	}
}

/// [`CatalogVTable::is_revision`] of a type with a `revision` form. Trailing bytes after the value
/// make it not one, as they make a read of it fail.
pub fn is_revision<T: SkipRevisioned>(bytes: &[u8]) -> bool {
	let mut rest = bytes;
	T::skip_revisioned(&mut rest).is_ok() && rest.is_empty()
}

/// [`CatalogVTable::is_revision`] of a type with no `revision` form.
pub fn no_revision(_: &[u8]) -> bool {
	false
}

/// [`CatalogVTable::decode_embedded`] of a type with no `revision` form: refused.
pub fn no_embedded_decode<T: StoredType>(_cursor: &mut &[u8]) -> anyhow::Result<Value> {
	Err(Error::Entry(EntryError::Unlevelled {
		ty: T::SCHEMA.name,
	})
	.into())
}

/// [`CatalogVTable::encode_embedded`] of a type with no `revision` form: refused.
pub fn no_embedded_encode<T: StoredType>(_value: &Value, _out: &mut Vec<u8>) -> anyhow::Result<()> {
	Err(Error::Entry(EntryError::Unlevelled {
		ty: T::SCHEMA.name,
	})
	.into())
}

/// [`CatalogVTable::schemas`].
pub fn schemas<T: StoredType>(out: &mut Vec<&'static TypeSchema>) {
	T::collect_schemas(out);
}
