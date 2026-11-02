//! The descriptors of the revisioned types the VM reads and writes other than the catalog's, each
//! a transcription of the type's `#[revisioned]` history, and the table that holds them beside the
//! catalog types' generated codecs.
//!
//! A catalog type is not described here: its row in a catalog table
//! (`catalog::stored::CATALOG_TYPES`, `EDGE_CATALOG_TYPES` or `LEAF_CATALOG_TYPES`) reads and
//! writes its bytes through the schema its derive generates, so its codec cannot drift from its
//! Rust type. The descriptors below are of the types no schema covers: the ids, a record and its
//! metadata, the datastore's values.
//!
//! A [`Codec`] pairs the hand-written descriptors with one catalog table. The entry that starts a
//! query chooses its codec through its surface, so an entry that serves fewer catalog kinds names
//! none of the code of the kinds it leaves out. Both tables list the kinds in the same order, so
//! a [`TypeId`] is the same index in every codec.

use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Result, anyhow};

use super::desc::{Field, Missing, Ty, TypeDesc};
use crate::catalog::stored::{
	CATALOG_TYPES, CatalogVTable, EDGE_CATALOG_TYPES, LEAF_CATALOG_TYPES,
};
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
				Variant::new(
					"TemporalMax",
					Shape::Struct(vec![arg(), Field::new("max", Ty::Native(Native::Value))]),
				),
				Variant::new(
					"TemporalMin",
					Shape::Struct(vec![arg(), Field::new("min", Ty::Native(Native::Value))]),
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

/// The descriptors of the types the VM reads and writes: those written by hand, then one for each
/// row of a catalog table, each delegating to the codec its derive generates.
pub(crate) struct Codec {
	/// The catalog table the descriptors of the catalog types come from.
	catalog: &'static [CatalogVTable],
	table: OnceLock<Table>,
}

/// The codec of an entry that serves every catalog kind.
pub(crate) static FULL_CODEC: Codec = Codec::over(CATALOG_TYPES);

/// The codec of an entry that serves the catalog kinds of [`EDGE_CATALOG_TYPES`] alone.
pub(crate) static EDGE_CODEC: Codec = Codec::over(EDGE_CATALOG_TYPES);

/// The codec of a morsel leaf, which serves no catalog kind ([`LEAF_CATALOG_TYPES`]).
pub(crate) static LEAF_CODEC: Codec = Codec::over(LEAF_CATALOG_TYPES);

type Table = (Vec<TypeDesc>, HashMap<&'static str, u16>);

impl Codec {
	const fn over(catalog: &'static [CatalogVTable]) -> Self {
		Self {
			catalog,
			table: OnceLock::new(),
		}
	}

	fn table(&self) -> &Table {
		self.table.get_or_init(|| {
			let catalog = self.catalog.iter().map(TypeDesc::catalog);
			let descs: Vec<TypeDesc> = kept().into_iter().chain(catalog).collect();
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

	/// The descriptor of type `name`.
	pub(super) fn lookup(&self, name: &str) -> Result<&TypeDesc> {
		let table = self.table();
		let id = table.1.get(name).ok_or_else(|| anyhow!("no codec descriptor for `{name}`"))?;
		Ok(&table.0[*id as usize])
	}
}

/// A revisioned type, as its index into the descriptor table: plain data a
/// program stores in place of the type's name. [`TypeId::UTF8`] names the
/// unframed string type, which has no descriptor.
///
/// The index is the same in every [`Codec`], so a program lowered for one entry names the same
/// type to another.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TypeId(pub(crate) u16);

impl TypeId {
	/// The type of a value stored as its UTF-8 bytes.
	pub(crate) const UTF8: Self = Self(u16::MAX);

	/// The id of type `name`. The name is a literal the lowering writes; one
	/// the VM holds no descriptor for is a bug in the lowering, not a runtime
	/// condition. Reads the index from [`FULL_CODEC`], so only the lowering of
	/// the statements that name catalog types, which an entry that serves them
	/// alone lowers, reaches the full catalog table through it.
	pub(crate) fn named(name: &str) -> Self {
		if name == super::UTF8 {
			return Self::UTF8;
		}
		match FULL_CODEC.table().1.get(name) {
			Some(id) => Self(*id),
			None => panic!("no codec descriptor for `{name}`"),
		}
	}

	/// The descriptor this id names in `codec`; `None` for [`TypeId::UTF8`].
	pub(super) fn desc(self, codec: &Codec) -> Result<Option<&TypeDesc>> {
		if self == Self::UTF8 {
			return Ok(None);
		}
		codec
			.table()
			.0
			.get(self.0 as usize)
			.map(Some)
			.ok_or_else(|| anyhow!("no codec descriptor at index {}", self.0))
	}
}
