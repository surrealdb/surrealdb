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
	StoredSubscriptionDefinition, StoredTableDefinition, StoredTypeDefinition, TaskLease,
	UserDefinition,
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
}

/// Builds the [`CatalogVTable`] of a type.
///
/// `catalog_vtable!(T)` is for a type written in both grammars; `catalog_vtable!(stored T)` is for
/// a type that has no `revision` form, whose container copies are refused;
/// `catalog_vtable!(read_only T)` is the row of a type a table reads as `catalog_vtable!(T)` does
/// and refuses to write with [`unwritten`]; `catalog_vtable!(unserved T)` is the row of a type a
/// table leaves out, which names the type and refuses every read and write of it with
/// [`unserved`].
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
		}
	};
	(read_only $t:ty) => {
		$crate::stored::CatalogVTable {
			name: <$t as $crate::stored::StoredType>::SCHEMA.name,
			decode: $crate::stored::vtable::decode::<$t>,
			encode_at: $crate::stored::vtable::unwritten_encode_at::<$t>,
			decode_embedded: $crate::stored::vtable::decode_embedded::<$t>,
			encode_embedded: $crate::stored::vtable::unwritten_encode_embedded::<$t>,
			schemas: $crate::stored::vtable::schemas::<$t>,
		}
	};
	(unserved $t:ty) => {
		$crate::stored::CatalogVTable {
			name: <$t as $crate::stored::StoredType>::SCHEMA.name,
			decode: $crate::stored::vtable::unserved_decode::<$t>,
			encode_at: $crate::stored::vtable::unserved_encode_at::<$t>,
			decode_embedded: $crate::stored::vtable::unserved_decode_embedded::<$t>,
			encode_embedded: $crate::stored::vtable::unserved_encode_embedded::<$t>,
			schemas: $crate::stored::vtable::unserved_schemas::<$t>,
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
		}
	};
}

/// The rows of the catalog types, each built once, which [`CATALOG_TYPES`] lists.
mod rows {
	use super::*;

	pub(super) const NAMESPACE: CatalogVTable = catalog_vtable!(NamespaceDefinition);
	pub(super) const DATABASE: CatalogVTable = catalog_vtable!(DatabaseDefinition);
	pub(super) const TABLE: CatalogVTable = catalog_vtable!(StoredTableDefinition);
	pub(super) const FIELD: CatalogVTable = catalog_vtable!(StoredFieldDefinition);
	pub(super) const INDEX: CatalogVTable = catalog_vtable!(StoredIndexDefinition);
	pub(super) const EVENT: CatalogVTable = catalog_vtable!(StoredEventDefinition);
	pub(super) const FUNCTION: CatalogVTable = catalog_vtable!(StoredFunctionDefinition);
	pub(super) const PARAM: CatalogVTable = catalog_vtable!(StoredParamDefinition);
	pub(super) const ACCESS: CatalogVTable = catalog_vtable!(StoredAccessDefinition);
	pub(super) const USER: CatalogVTable = catalog_vtable!(UserDefinition);
	pub(super) const ANALYZER: CatalogVTable = catalog_vtable!(AnalyzerDefinition);
	pub(super) const SEQUENCE: CatalogVTable = catalog_vtable!(SequenceDefinition);
	pub(super) const MODULE: CatalogVTable = catalog_vtable!(StoredModuleDefinition);
	pub(super) const ML_MODEL: CatalogVTable = catalog_vtable!(StoredMlModelDefinition);
	pub(super) const BUCKET: CatalogVTable = catalog_vtable!(StoredBucketDefinition);
	pub(super) const TYPE: CatalogVTable = catalog_vtable!(StoredTypeDefinition);
	pub(super) const API: CatalogVTable = catalog_vtable!(StoredApiDefinition);
	pub(super) const CONFIG: CatalogVTable = catalog_vtable!(StoredConfigDefinition);
	pub(super) const SUBSCRIPTION: CatalogVTable = catalog_vtable!(StoredSubscriptionDefinition);
	pub(super) const NODE: CatalogVTable = catalog_vtable!(Node);
	pub(super) const TASK_LEASE: CatalogVTable = catalog_vtable!(TaskLease);
	pub(super) const NODE_LIVE_QUERY: CatalogVTable = catalog_vtable!(NodeLiveQuery);
	pub(super) const ACCESS_GRANT: CatalogVTable = catalog_vtable!(AccessGrant);
}

/// The catalog types a VM program reads or writes through a key route, by Rust type name.
///
/// The types only the datastore's own code reads and writes (the feature level record, layout
/// descriptors, node release records, generation marks, generation rings and conformance records)
/// are not listed: no VM program reaches them, and the schema lock reads their schemas from the
/// types themselves ([`super::lock::all_schemas`]).
pub static CATALOG_TYPES: &[CatalogVTable] = &[
	rows::NAMESPACE,
	rows::DATABASE,
	rows::TABLE,
	rows::FIELD,
	rows::INDEX,
	rows::EVENT,
	rows::FUNCTION,
	rows::PARAM,
	rows::ACCESS,
	rows::USER,
	rows::ANALYZER,
	rows::SEQUENCE,
	rows::MODULE,
	rows::ML_MODEL,
	rows::BUCKET,
	rows::TYPE,
	rows::API,
	rows::CONFIG,
	rows::SUBSCRIPTION,
	rows::NODE,
	rows::TASK_LEASE,
	rows::NODE_LIVE_QUERY,
	rows::ACCESS_GRANT,
];

/// The catalog types an edge entry reads through a key route: [`CATALOG_TYPES`] with the rows of
/// the kinds the entry never meets (access methods and their grants, users, nodes, the live
/// queries of a node, buckets) replaced by [`catalog_vtable!`]`(unserved T)`, and every other row
/// by [`catalog_vtable!`]`(read_only T)`. The table names no encoder: an edge entry writes a
/// catalog entry only through the datastore's typed path (a namespace, database or table a write
/// defines), never through the VM's codec, whose catalog writes are store steps of the schema
/// statements the entry does not serve. A row stays at its position and keeps its name, so a type
/// is the same index in both tables, and a key of a kind that is left out is refused with
/// [`unserved`] rather than skipped.
pub static EDGE_CATALOG_TYPES: &[CatalogVTable] = &[
	catalog_vtable!(read_only NamespaceDefinition),
	catalog_vtable!(read_only DatabaseDefinition),
	catalog_vtable!(read_only StoredTableDefinition),
	catalog_vtable!(read_only StoredFieldDefinition),
	catalog_vtable!(read_only StoredIndexDefinition),
	catalog_vtable!(read_only StoredEventDefinition),
	catalog_vtable!(read_only StoredFunctionDefinition),
	catalog_vtable!(read_only StoredParamDefinition),
	catalog_vtable!(unserved StoredAccessDefinition),
	catalog_vtable!(unserved UserDefinition),
	catalog_vtable!(read_only AnalyzerDefinition),
	catalog_vtable!(read_only SequenceDefinition),
	catalog_vtable!(read_only StoredModuleDefinition),
	catalog_vtable!(read_only StoredMlModelDefinition),
	catalog_vtable!(unserved StoredBucketDefinition),
	catalog_vtable!(read_only StoredTypeDefinition),
	catalog_vtable!(read_only StoredApiDefinition),
	catalog_vtable!(read_only StoredConfigDefinition),
	catalog_vtable!(read_only StoredSubscriptionDefinition),
	catalog_vtable!(unserved Node),
	catalog_vtable!(read_only TaskLease),
	catalog_vtable!(unserved NodeLiveQuery),
	catalog_vtable!(unserved AccessGrant),
];

/// The catalog types a morsel leaf reads: none. Every row of [`CATALOG_TYPES`] is replaced by
/// [`catalog_vtable!`]`(unserved T)`: a leaf runs a unit over the record keys of its range and
/// reads no catalog entry, so a key of any catalog kind is refused with [`unserved`], and the table
/// names no decoder of a definition. A row stays at its position and keeps its name, so a type is
/// the same index here as in the other tables.
pub static LEAF_CATALOG_TYPES: &[CatalogVTable] = &[
	catalog_vtable!(unserved NamespaceDefinition),
	catalog_vtable!(unserved DatabaseDefinition),
	catalog_vtable!(unserved StoredTableDefinition),
	catalog_vtable!(unserved StoredFieldDefinition),
	catalog_vtable!(unserved StoredIndexDefinition),
	catalog_vtable!(unserved StoredEventDefinition),
	catalog_vtable!(unserved StoredFunctionDefinition),
	catalog_vtable!(unserved StoredParamDefinition),
	catalog_vtable!(unserved StoredAccessDefinition),
	catalog_vtable!(unserved UserDefinition),
	catalog_vtable!(unserved AnalyzerDefinition),
	catalog_vtable!(unserved SequenceDefinition),
	catalog_vtable!(unserved StoredModuleDefinition),
	catalog_vtable!(unserved StoredMlModelDefinition),
	catalog_vtable!(unserved StoredBucketDefinition),
	catalog_vtable!(unserved StoredTypeDefinition),
	catalog_vtable!(unserved StoredApiDefinition),
	catalog_vtable!(unserved StoredConfigDefinition),
	catalog_vtable!(unserved StoredSubscriptionDefinition),
	catalog_vtable!(unserved Node),
	catalog_vtable!(unserved TaskLease),
	catalog_vtable!(unserved NodeLiveQuery),
	catalog_vtable!(unserved AccessGrant),
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

/// The refusal of a read or write of the catalog type `T`, whose row a table leaves out.
pub fn unserved<T: StoredType>() -> anyhow::Error {
	anyhow::anyhow!("catalog kind {} is not served by this entry", T::SCHEMA.name)
}

/// The refusal of a write of the catalog type `T`, whose row a table reads only.
pub fn unwritten<T: StoredType>() -> anyhow::Error {
	anyhow::anyhow!("catalog kind {} is not written by this entry", T::SCHEMA.name)
}

/// [`CatalogVTable::encode_at`] of a type a table reads only: refused.
pub fn unwritten_encode_at<T: StoredType>(
	_value: &Value,
	_ctx: &EncodeContext<'_>,
) -> anyhow::Result<Encoded> {
	Err(unwritten::<T>())
}

/// [`CatalogVTable::encode_embedded`] of a type a table reads only: refused.
pub fn unwritten_encode_embedded<T: StoredType>(
	_value: &Value,
	_out: &mut Vec<u8>,
) -> anyhow::Result<()> {
	Err(unwritten::<T>())
}

/// [`CatalogVTable::decode`] of a type a table leaves out: refused.
pub fn unserved_decode<T: StoredType>(_bytes: &[u8]) -> anyhow::Result<Value> {
	Err(unserved::<T>())
}

/// [`CatalogVTable::encode_at`] of a type a table leaves out: refused.
pub fn unserved_encode_at<T: StoredType>(
	_value: &Value,
	_ctx: &EncodeContext<'_>,
) -> anyhow::Result<Encoded> {
	Err(unserved::<T>())
}

/// [`CatalogVTable::decode_embedded`] of a type a table leaves out: refused.
pub fn unserved_decode_embedded<T: StoredType>(_cursor: &mut &[u8]) -> anyhow::Result<Value> {
	Err(unserved::<T>())
}

/// [`CatalogVTable::encode_embedded`] of a type a table leaves out: refused.
pub fn unserved_encode_embedded<T: StoredType>(
	_value: &Value,
	_out: &mut Vec<u8>,
) -> anyhow::Result<()> {
	Err(unserved::<T>())
}

/// [`CatalogVTable::schemas`] of a type a table leaves out: adds nothing, as the table serves no
/// value of the type.
pub fn unserved_schemas<T: StoredType>(_out: &mut Vec<&'static TypeSchema>) {}
