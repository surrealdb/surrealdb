//! The descriptors of the revisioned types the VM reads and writes other than the catalog's, each
//! a transcription of the type's `#[revisioned]` history, and the table that holds them beside the
//! catalog types' generated codecs.
//!
//! A catalog type is not described here: its entry in `catalog::stored::CATALOG_TYPES`
//! reads and writes its bytes through the schema its derive generates, so its codec cannot drift
//! from its Rust type. The descriptors below are of the types no schema covers: the ids, a
//! record and its metadata, the datastore's values.

use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Result, anyhow};

use super::desc::{Field, Missing, Ty, TypeDesc};
use crate::catalog::stored::CATALOG_TYPES;
use crate::val::{Number, Object, Value};

pub(super) fn opt(ty: Ty) -> Ty {
	Ty::Option(Box::new(ty))
}

pub(super) fn named(name: &'static str) -> Ty {
	Ty::Named(name)
}

/// Every descriptor written by hand.
pub(super) fn kept() -> Vec<TypeDesc> {
	ids().into_iter().chain(record_types()).chain(native_types()).chain(datastore_types()).collect()
}

/// The descriptors of the catalog types, each delegating to the codec its derive generates.
fn catalog_types() -> impl Iterator<Item = TypeDesc> {
	CATALOG_TYPES.iter().map(TypeDesc::catalog)
}

fn ids() -> Vec<TypeDesc> {
	vec![
		// Ids: a bare `u32`, with no revision header.
		TypeDesc::transparent("NamespaceId", Ty::Uint),
		TypeDesc::transparent("DatabaseId", Ty::Uint),
		TypeDesc::transparent("TableId", Ty::Uint),
		TypeDesc::transparent("IndexId", Ty::Uint),
		// `datastore/src/sequences.rs`
		TypeDesc::structure(
			"BatchValue",
			1,
			vec![Field::new("to", Ty::Int), Field::new("owner", Ty::Uuid)],
		),
		TypeDesc::structure("SequenceState", 1, vec![Field::new("next", Ty::Int)]),
	]
}

/// The types of the `Value` family that are stored as themselves.
fn native_types() -> Vec<TypeDesc> {
	use super::desc::Native;
	vec![
		TypeDesc::transparent("RecordId", Ty::Native(Native::RecordId)),
		TypeDesc::transparent("RecordIdKey", Ty::Native(Native::RecordIdKey)),
	]
}

fn record_types() -> Vec<TypeDesc> {
	use super::desc::{Native, Shape, Variant};
	let number = || Ty::Native(Native::Number);
	let arg = || Field::new("arg", Ty::Uint);
	let count = || Field::new("count", Ty::Int);
	let shift = || {
		Field::new("shift", number())
			.start(2)
			.missing(Missing::Value(|| Value::Number(Number::Float(0.0))))
	};
	vec![
		// `catalog/src/record.rs`
		TypeDesc::structure(
			"Record",
			2,
			vec![
				Field::new("metadata", opt(named("Metadata"))),
				Field::new("data", Ty::Native(Native::Value)),
			],
		)
		.indexed_from(2),
		TypeDesc::structure(
			"Metadata",
			1,
			vec![
				Field::new("record_type", named("RecordType")),
				Field::new("aggregation_stats", Ty::Vec(Box::new(named("AggregationStat")))),
			],
		),
		TypeDesc::enumeration(
			"RecordType",
			2,
			vec![
				Variant::new("Table", Shape::Unit),
				// Revision 1's unit edge predates the layout marker, and was
				// always the original adjacency-key layout.
				Variant::new("Edge", Shape::Unit)
					.end(2, |_| Ok(variant("Edge", object(vec![("variant", Value::from(1))])))),
				Variant::new("Edge", Shape::Struct(vec![Field::new("variant", Ty::Uint)])).start(2),
			],
		),
		// `catalog/src/aggregation.rs`
		TypeDesc::enumeration(
			"AggregationStat",
			2,
			vec![
				Variant::new("Count", Shape::Struct(vec![count()])),
				Variant::new("CountValue", Shape::Struct(vec![arg(), count()])),
				Variant::new("NumberMax", Shape::Struct(vec![arg(), Field::new("max", number())])),
				Variant::new("NumberMin", Shape::Struct(vec![arg(), Field::new("min", number())])),
				Variant::new("Sum", Shape::Struct(vec![arg(), Field::new("sum", number())])),
				Variant::new(
					"Mean",
					Shape::Struct(vec![arg(), Field::new("sum", number()), count()]),
				),
				Variant::new(
					"StdDev",
					Shape::Struct(vec![
						arg(),
						shift(),
						Field::new("sum", number()),
						Field::new("sum_of_squares", number()),
						count(),
					]),
				),
				Variant::new(
					"Variance",
					Shape::Struct(vec![
						arg(),
						shift(),
						Field::new("sum", number()),
						Field::new("sum_of_squares", number()),
						count(),
					]),
				),
				Variant::new(
					"TimeMax",
					Shape::Struct(vec![arg(), Field::new("max", Ty::Native(Native::Datetime))]),
				),
				Variant::new(
					"TimeMin",
					Shape::Struct(vec![arg(), Field::new("min", Ty::Native(Native::Datetime))]),
				),
				Variant::new(
					"Accumulate",
					Shape::Struct(vec![
						arg(),
						Field::new("values", Ty::Vec(Box::new(Ty::Native(Native::Value)))),
					]),
				),
			],
		),
	]
}

fn datastore_types() -> Vec<TypeDesc> {
	use super::desc::{Native, Shape, Variant};
	let value = || Ty::Native(Native::Value);
	let record = || Ty::Native(Native::RecordId);
	let path = || Ty::Vec(Box::new(Ty::String));
	let op = |name, fields: Vec<Field>| Variant::new(name, Shape::Struct(fields));
	vec![
		// `datastore/src/values/index_build.rs`
		TypeDesc::structure(
			"Appending",
			2,
			vec![
				Field::new("old_values", opt(Ty::Vec(Box::new(value())))),
				Field::new("new_values", opt(Ty::Vec(Box::new(value())))),
				Field::new("id", Ty::Native(Native::RecordIdKey)),
				Field::new("count_cond_match", opt(Ty::Tuple(vec![Ty::Bool, Ty::Bool]))).start(2),
			],
		),
		TypeDesc::structure(
			"PrimaryAppending",
			2,
			vec![Field::new("0", Ty::Uint), Field::new("1", Ty::Uint).start(2)],
		),
		// `datastore/src/values/catalog_version.rs`
		TypeDesc::structure("CatalogVersion", 1, vec![Field::new("0", Ty::Uuid)]),
		// `datastore/src/key/reclaim.rs`
		TypeDesc::structure(
			"ReclaimState",
			2,
			vec![
				Field::new("observed_ms", Ty::Uint),
				Field::new("cursor", opt(Ty::Bytes)).start(2),
			],
		),
		// `datastore/src/values/fulltext.rs`
		TypeDesc::structure(
			"DocLengthAndCount",
			1,
			vec![Field::new("total_docs_length", Ty::Int), Field::new("doc_count", Ty::Int)],
		),
		TypeDesc::structure(
			"TermDocument",
			1,
			vec![Field::new("f", Ty::Uint), Field::new("o", Ty::Vec(Box::new(named("Offset"))))],
		),
		TypeDesc::structure(
			"Offset",
			1,
			vec![
				Field::new("index", Ty::Uint),
				Field::new("start", Ty::Uint),
				Field::new("gen_start", Ty::Uint),
				Field::new("end", Ty::Uint),
			],
		),
		// `datastore/src/values/changefeed.rs`, `expr/src/expr/operation.rs`
		TypeDesc::structure(
			"TableMutations",
			1,
			vec![
				Field::new("0", Ty::String),
				Field::new("1", Ty::Vec(Box::new(named("TableMutation")))),
			],
		),
		TypeDesc::enumeration(
			"TableMutation",
			1,
			vec![
				Variant::new("Set", Shape::Tuple(vec![record(), value()])),
				Variant::new("Del", Shape::Tuple(vec![record()])),
				Variant::new("Def", Shape::Tuple(vec![named("StoredTableDefinition")])),
				Variant::new(
					"SetWithDiff",
					Shape::Tuple(vec![record(), value(), Ty::Vec(Box::new(named("Operation")))]),
				),
				Variant::new("DelWithOriginal", Shape::Tuple(vec![record(), value()])),
			],
		),
		TypeDesc::enumeration(
			"Operation",
			1,
			vec![
				op("Add", vec![Field::new("path", path()), Field::new("value", value())]),
				op("Remove", vec![Field::new("path", path())]),
				op("Replace", vec![Field::new("path", path()), Field::new("value", value())]),
				op("Change", vec![Field::new("path", path()), Field::new("value", value())]),
				op("Copy", vec![Field::new("path", path()), Field::new("from", path())]),
				op("Move", vec![Field::new("path", path()), Field::new("from", path())]),
				op("Test", vec![Field::new("path", path()), Field::new("value", value())]),
			],
		),
	]
}

pub(super) fn object(fields: Vec<(&str, Value)>) -> Value {
	let mut object = Object::default();
	for (k, v) in fields {
		object.insert(k.to_owned(), v);
	}
	Value::Object(object)
}

pub(super) fn variant(name: &str, body: Value) -> Value {
	object(vec![(name, body)])
}

/// A revisioned type, as its index into the descriptor table: plain data a
/// program stores in place of the type's name. [`TypeId::UTF8`] names the
/// unframed string type, which has no descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TypeId(pub(crate) u16);

impl TypeId {
	/// The type of a value stored as its UTF-8 bytes.
	pub(crate) const UTF8: Self = Self(u16::MAX);

	/// The id of type `name`. The name is a literal the lowering writes; one
	/// the VM holds no descriptor for is a bug in the lowering, not a runtime
	/// condition.
	pub(crate) fn named(name: &str) -> Self {
		if name == super::UTF8 {
			return Self::UTF8;
		}
		match table().1.get(name) {
			Some(id) => Self(*id),
			None => panic!("no codec descriptor for `{name}`"),
		}
	}

	/// The descriptor this id names; `None` for [`TypeId::UTF8`].
	pub(super) fn desc(self) -> Result<Option<&'static TypeDesc>> {
		if self == Self::UTF8 {
			return Ok(None);
		}
		table()
			.0
			.get(self.0 as usize)
			.map(Some)
			.ok_or_else(|| anyhow!("no codec descriptor at index {}", self.0))
	}
}

type Table = (Vec<TypeDesc>, HashMap<&'static str, u16>);

fn table() -> &'static Table {
	static TABLE: OnceLock<Table> = OnceLock::new();
	TABLE.get_or_init(|| {
		let descs: Vec<TypeDesc> = kept().into_iter().chain(catalog_types()).collect();
		let ids = descs
			.iter()
			.enumerate()
			.map(|(index, desc)| {
				let id = u16::try_from(index).expect("fewer than 65535 descriptors");
				(desc.name, id)
			})
			.collect();
		(descs, ids)
	})
}

pub(super) fn lookup(name: &str) -> Result<&'static TypeDesc> {
	let table = table();
	let id = table.1.get(name).ok_or_else(|| anyhow!("no codec descriptor for `{name}`"))?;
	Ok(&table.0[*id as usize])
}
