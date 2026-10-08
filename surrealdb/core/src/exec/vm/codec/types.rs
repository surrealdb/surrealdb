//! The descriptors of the revisioned types the VM reads and writes, each a
//! transcription of the type's `#[revisioned]` history.

use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Result, anyhow};

use super::desc::{Field, Missing, Ty, TypeDesc};
use crate::val::{Number, Object, Value};

fn opt(ty: Ty) -> Ty {
	Ty::Option(Box::new(ty))
}

fn named(name: &'static str) -> Ty {
	Ty::Named(name)
}

fn all() -> Vec<TypeDesc> {
	vec![
		// Ids: a bare `u32`, with no revision header.
		TypeDesc::transparent("NamespaceId", Ty::Uint),
		TypeDesc::transparent("DatabaseId", Ty::Uint),
		TypeDesc::transparent("TableId", Ty::Uint),
		TypeDesc::transparent("IndexId", Ty::Uint),
		// `catalog/src/namespace.rs`
		TypeDesc::structure(
			"NamespaceDefinition",
			1,
			vec![
				Field::new("namespace_id", named("NamespaceId")),
				Field::new("name", Ty::String),
				Field::new("comment", opt(Ty::String)),
			],
		),
		// `catalog/src/database.rs`
		TypeDesc::structure(
			"DatabaseDefinition",
			1,
			vec![
				Field::new("namespace_id", named("NamespaceId")),
				Field::new("database_id", named("DatabaseId")),
				Field::new("name", Ty::String),
				Field::new("comment", opt(Ty::String)),
				Field::new("changefeed", opt(named("ChangeFeed"))),
				Field::new("strict", Ty::Bool),
			],
		),
		// `expr/src/expr/changefeed.rs`
		TypeDesc::structure(
			"ChangeFeed",
			1,
			vec![Field::new("expiry", Ty::Duration), Field::new("store_diff", Ty::Bool)],
		),
		// `catalog/src/schema/sequence.rs`
		TypeDesc::structure(
			"SequenceDefinition",
			1,
			vec![
				Field::new("name", Ty::String),
				Field::new("batch", Ty::Uint),
				Field::new("start", Ty::Int),
				Field::new("timeout", opt(Ty::Duration)),
			],
		),
		// `catalog/src/task.rs`
		TypeDesc::structure(
			"TaskLease",
			1,
			vec![Field::new("owner", Ty::Uuid), Field::new("expiration", Ty::DateTime)],
		),
		// `catalog/src/node.rs`
		TypeDesc::structure(
			"Node",
			2,
			vec![
				Field::new("id", Ty::Uuid),
				Field::new("heartbeat", named("Timestamp")),
				Field::new("gc", Ty::Bool),
				Field::new("http_endpoint", opt(Ty::String)).start(2),
			],
		),
		TypeDesc::structure("Timestamp", 1, vec![Field::new("value", Ty::Uint)]),
		// `datastore/src/sequences.rs`
		TypeDesc::structure(
			"BatchValue",
			1,
			vec![Field::new("to", Ty::Int), Field::new("owner", Ty::Uuid)],
		),
		TypeDesc::structure("SequenceState", 1, vec![Field::new("next", Ty::Int)]),
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

fn schema_types() -> Vec<TypeDesc> {
	use super::desc::{Shape, Variant};
	let text = || Ty::String;
	let permission = || named("StoredPermission");
	let auth_limit = || {
		Field::new("auth_limit", named("AuthLimit")).start(2).missing(Missing::Value(|| {
			object(vec![
				("level", variant("Root", Value::None)),
				("role", Value::String("Owner".into())),
			])
		}))
	};
	let tables = || Ty::Vec(Box::new(Ty::String));
	vec![
		// `catalog/src/table.rs`
		TypeDesc::structure(
			"StoredTableDefinition",
			4,
			vec![
				Field::new("namespace_id", named("NamespaceId")),
				Field::new("database_id", named("DatabaseId")),
				Field::new("table_id", named("TableId")),
				Field::new("name", Ty::String),
				Field::new("drop", Ty::Bool),
				Field::new("schemafull", Ty::Bool),
				Field::new("view", opt(named("StoredViewDefinition"))),
				Field::new("permissions", named("StoredPermissions")),
				Field::new("changefeed", opt(named("ChangeFeed"))),
				Field::new("comment", opt(Ty::String)),
				Field::new("table_type", named("TableType")),
				Field::new("cache_fields_ts", Ty::Uuid),
				Field::new("cache_events_ts", Ty::Uuid),
				Field::new("cache_tables_ts", Ty::Uuid),
				Field::new("cache_indexes_ts", Ty::Uuid),
				Field::new("cache_lives_ts", Ty::Uuid).start(3),
				Field::new("graphql_alias", opt(Ty::String)).start(2),
				Field::new("graphql_deprecated", opt(Ty::String)).start(2),
				Field::new("graph_folded", Ty::Bool).start(4),
				Field::new("graph_doc_ids", Ty::Bool).start(4),
				Field::new("inline_edges_cap", opt(Ty::Uint)).start(4),
				Field::new("inline_refs_cap", opt(Ty::Uint)).start(4),
				Field::new("graph_inline_gen", Ty::Uint).start(4),
			],
		),
		// `catalog/src/schema/mod.rs`
		TypeDesc::enumeration(
			"StoredPermission",
			1,
			vec![
				Variant::new("None", Shape::Unit),
				Variant::new("Full", Shape::Unit),
				Variant::new("Specific", Shape::Tuple(vec![text()])),
			],
		),
		TypeDesc::structure(
			"StoredPermissions",
			1,
			vec![
				Field::new("select", permission()),
				Field::new("create", permission()),
				Field::new("update", permission()),
				Field::new("delete", permission()),
			],
		),
		// `catalog/src/view.rs`. The first three variants are the older,
		// AST-shaped forms; `Clauses` is the one written today.
		TypeDesc::enumeration(
			"StoredViewDefinition",
			1,
			vec![
				Variant::new(
					"Materialized",
					Shape::Struct(vec![
						Field::new("fields", named("Fields")),
						Field::new("tables", tables()),
						Field::new("condition", opt(text())),
					]),
				),
				Variant::new(
					"Aggregated",
					Shape::Struct(vec![
						Field::new("analysis", named("AggregationAnalysis")),
						Field::new("condition", opt(text())),
						Field::new("tables", tables()),
						Field::new("groups", named("Groups")),
						Field::new("fields", named("Fields")),
					]),
				),
				Variant::new(
					"Select",
					Shape::Struct(vec![
						Field::new("fields", named("Fields")),
						Field::new("tables", tables()),
						Field::new("condition", opt(text())),
						Field::new("groups", opt(named("Groups"))),
					]),
				),
				Variant::new(
					"Clauses",
					Shape::Struct(vec![
						Field::new("fields", text()),
						Field::new("tables", tables()),
						Field::new("condition", opt(text())),
						Field::new("groups", opt(Ty::Vec(Box::new(text())))),
					]),
				),
			],
		),
		// `catalog/src/aggregation.rs`: the analysis an `Aggregated` view
		// stores; an expression and an idiom are stored as their text.
		TypeDesc::structure(
			"AggregationAnalysis",
			1,
			vec![
				Field::new("aggregate_arguments", Ty::Vec(Box::new(text()))),
				Field::new("aggregations", Ty::Vec(Box::new(named("Aggregation")))),
				Field::new("group_expressions", Ty::Vec(Box::new(text()))),
				Field::new("fields", named("AggregateFields")),
			],
		),
		TypeDesc::enumeration(
			"Aggregation",
			1,
			std::iter::once(Variant::new("Count", Shape::Unit))
				.chain(
					[
						"CountValue",
						"NumberMax",
						"NumberMin",
						"Sum",
						"Mean",
						"StdDev",
						"Variance",
						"DatetimeMax",
						"DatetimeMin",
						"Accumulate",
					]
					.into_iter()
					.map(|name| Variant::new(name, Shape::Tuple(vec![Ty::Uint]))),
				)
				.collect(),
		),
		TypeDesc::enumeration(
			"AggregateFields",
			1,
			vec![
				Variant::new("Value", Shape::Tuple(vec![text()])),
				Variant::new(
					"Fields",
					Shape::Tuple(vec![Ty::Vec(Box::new(Ty::Tuple(vec![text(), text()])))]),
				),
			],
		),
		// `expr/src/expr/field.rs`, `group.rs`: an expression and an idiom
		// are stored as their SurrealQL text.
		TypeDesc::enumeration(
			"Fields",
			1,
			vec![
				Variant::new("Value", Shape::Tuple(vec![named("Selector")])),
				Variant::new("Select", Shape::Tuple(vec![Ty::Vec(Box::new(named("Field")))])),
			],
		),
		TypeDesc::enumeration(
			"Field",
			1,
			vec![
				Variant::new("All", Shape::Unit),
				Variant::new("Single", Shape::Tuple(vec![named("Selector")])),
			],
		),
		TypeDesc::structure(
			"Selector",
			1,
			vec![Field::new("expr", text()), Field::new("alias", opt(text()))],
		),
		TypeDesc::structure("Groups", 1, vec![Field::new("0", Ty::Vec(Box::new(named("Group"))))]),
		TypeDesc::structure("Group", 1, vec![Field::new("0", text())]),
		// `expr/src/expr/table_type.rs`
		TypeDesc::enumeration(
			"TableType",
			1,
			vec![
				Variant::new("Any", Shape::Unit),
				Variant::new("Normal", Shape::Unit),
				Variant::new("Relation", Shape::Tuple(vec![named("Relation")])),
			],
		),
		TypeDesc::structure(
			"Relation",
			3,
			vec![
				Field::new("old_from", opt(named("Kind"))).end(2),
				Field::new("from", tables()).start(2).missing(Missing::Convert {
					from: "old_from",
					convert: record_tables,
				}),
				Field::new("old_to", opt(named("Kind"))).end(2),
				Field::new("to", tables()).start(2).missing(Missing::Convert {
					from: "old_to",
					convert: record_tables,
				}),
				Field::new("enforced", Ty::Bool),
				Field::new("lightweight", Ty::Bool).start(3),
			],
		),
		// `catalog/src/schema/field.rs`
		TypeDesc::enumeration(
			"StoredDefineDefault",
			1,
			vec![
				Variant::new("None", Shape::Unit),
				Variant::new("Always", Shape::Tuple(vec![text()])),
				Variant::new("Set", Shape::Tuple(vec![text()])),
			],
		),
		TypeDesc::structure(
			"ComputedDeps",
			1,
			vec![
				Field::new("fields", Ty::Vec(Box::new(Ty::String))),
				Field::new("is_complete", Ty::Bool),
			],
		),
		TypeDesc::structure(
			"StoredFieldDefinition",
			6,
			vec![
				Field::new("name", text()),
				Field::new("table", Ty::String),
				Field::new("old_field_kind", opt(named("Kind"))).end(5),
				Field::new("field_kind", opt(text())).start(5).missing(Missing::Convert {
					from: "old_field_kind",
					convert: opt_kind_text,
				}),
				Field::new("flexible", Ty::Bool),
				Field::new("readonly", Ty::Bool),
				Field::new("value", opt(text())),
				Field::new("assert", opt(text())),
				Field::new("computed", opt(text())),
				Field::new("default", named("StoredDefineDefault")),
				Field::new("select_permission", permission()),
				Field::new("create_permission", permission()),
				Field::new("update_permission", permission()),
				Field::new("comment", opt(Ty::String)),
				Field::new("reference", opt(named("StoredReference"))),
				auth_limit(),
				Field::new("computed_deps", opt(named("ComputedDeps"))).start(3).end(5),
				Field::new("graphql_alias", opt(Ty::String)).start(4),
				Field::new("graphql_deprecated", opt(Ty::String)).start(4),
				Field::new("inline", Ty::Bool).start(6),
			],
		),
		// `catalog/src/schema/function.rs`
		TypeDesc::structure(
			"StoredFunctionDefinition",
			4,
			vec![
				Field::new("name", Ty::String),
				Field::new(
					"old_args",
					Ty::Vec(Box::new(Ty::Tuple(vec![Ty::String, named("Kind")]))),
				)
				.end(4),
				Field::new("args", Ty::Vec(Box::new(Ty::Tuple(vec![Ty::String, text()]))))
					.start(4)
					.missing(Missing::Convert {
						from: "old_args",
						convert: arg_kind_texts,
					}),
				Field::new("block", text()),
				Field::new("comment", opt(Ty::String)),
				Field::new("permissions", permission()),
				Field::new("old_returns", opt(named("Kind"))).end(4),
				Field::new("returns", opt(text())).start(4).missing(Missing::Convert {
					from: "old_returns",
					convert: opt_kind_text,
				}),
				auth_limit(),
				Field::new("graphql_alias", opt(Ty::String)).start(3),
				Field::new("graphql_deprecated", opt(Ty::String)).start(3),
			],
		),
		// `catalog/src/schema/event.rs`
		TypeDesc::enumeration(
			"EventKind",
			1,
			vec![
				Variant::new("Sync", Shape::Unit),
				Variant::new(
					"Async",
					Shape::Struct(vec![
						Field::new("retry", Ty::Uint),
						Field::new("max_depth", Ty::Uint),
					]),
				),
			],
		),
		TypeDesc::structure(
			"StoredEventDefinition",
			3,
			vec![
				Field::new("name", Ty::String),
				Field::new("target_table", Ty::String),
				Field::new("when", text()),
				Field::new("then", Ty::Vec(Box::new(text()))),
				Field::new("comment", opt(Ty::String)),
				auth_limit(),
				Field::new("kind", named("EventKind"))
					.start(3)
					.missing(Missing::Value(|| variant("Sync", Value::None))),
			],
		),
		// `catalog/src/schema/reference.rs`
		TypeDesc::structure(
			"StoredReference",
			1,
			vec![Field::new("on_delete", named("StoredReferenceDeleteStrategy"))],
		),
		TypeDesc::enumeration(
			"StoredReferenceDeleteStrategy",
			1,
			vec![
				Variant::new("Reject", Shape::Unit),
				Variant::new("Ignore", Shape::Unit),
				Variant::new("Cascade", Shape::Unit),
				Variant::new("Unset", Shape::Unit),
				Variant::new("Custom", Shape::Tuple(vec![text()])),
			],
		),
		// `catalog/src/auth.rs`
		TypeDesc::enumeration(
			"AuthLevel",
			1,
			vec![
				Variant::new("No", Shape::Unit),
				Variant::new("Root", Shape::Unit),
				Variant::new("Namespace", Shape::Tuple(vec![Ty::String])),
				Variant::new("Database", Shape::Tuple(vec![Ty::String, Ty::String])),
				Variant::new("Record", Shape::Tuple(vec![Ty::String, Ty::String, Ty::String])),
			],
		),
		TypeDesc::structure(
			"AuthLimit",
			1,
			vec![Field::new("level", named("AuthLevel")), Field::new("role", opt(Ty::String))],
		),
	]
}

fn kind_types() -> Vec<TypeDesc> {
	use super::desc::{Native, Shape, Variant};
	let kind = || named("Kind");
	let kinds = || Ty::Vec(Box::new(named("Kind")));
	let unit = |name| Variant::new(name, Shape::Unit);
	vec![
		// `expr/src/expr/kind.rs`: read only by the conversions of revisions
		// written before kinds were stored as text.
		TypeDesc::enumeration(
			"GeometryKind",
			1,
			["Point", "Line", "Polygon", "MultiPoint", "MultiLine", "MultiPolygon", "Collection"]
				.into_iter()
				.map(unit)
				.collect(),
		),
		TypeDesc::enumeration(
			"Kind",
			1,
			vec![
				unit("Any"),
				unit("None"),
				unit("Null"),
				unit("Bool"),
				unit("Bytes"),
				unit("Datetime"),
				unit("Decimal"),
				unit("Duration"),
				unit("Float"),
				unit("Int"),
				unit("Number"),
				unit("Object"),
				unit("String"),
				unit("Uuid"),
				unit("Regex"),
				Variant::new("Table", Shape::Tuple(vec![Ty::Vec(Box::new(Ty::String))])),
				Variant::new("Record", Shape::Tuple(vec![Ty::Vec(Box::new(Ty::String))])),
				Variant::new(
					"Geometry",
					Shape::Tuple(vec![Ty::Vec(Box::new(named("GeometryKind")))]),
				),
				Variant::new("Either", Shape::Tuple(vec![kinds()])),
				Variant::new("Set", Shape::Tuple(vec![kind(), opt(Ty::Uint)])),
				Variant::new("Array", Shape::Tuple(vec![kind(), opt(Ty::Uint)])),
				Variant::new("Function", Shape::Tuple(vec![opt(kinds()), opt(kind())])),
				unit("Range"),
				Variant::new("Literal", Shape::Tuple(vec![named("KindLiteral")])),
				Variant::new("File", Shape::Tuple(vec![Ty::Vec(Box::new(Ty::String))])),
			],
		),
		TypeDesc::enumeration(
			"KindLiteral",
			1,
			vec![
				Variant::new("String", Shape::Tuple(vec![Ty::String])),
				Variant::new("Integer", Shape::Tuple(vec![Ty::Int])),
				Variant::new("Float", Shape::Tuple(vec![Ty::F64])),
				Variant::new("Decimal", Shape::Tuple(vec![Ty::Decimal])),
				Variant::new("Duration", Shape::Tuple(vec![Ty::Native(Native::Duration)])),
				Variant::new("Array", Shape::Tuple(vec![kinds()])),
				Variant::new(
					"Object",
					Shape::Tuple(vec![Ty::Map(Box::new(Ty::String), Box::new(kind()))]),
				),
				Variant::new("Bool", Shape::Tuple(vec![Ty::Bool])),
			],
		),
	]
}

fn definition_types() -> Vec<TypeDesc> {
	use super::desc::{Native, Shape, Variant};
	let text = || Ty::String;
	let unit = |name| Variant::new(name, Shape::Unit);
	let units = |names: &[&'static str]| names.iter().map(|n| unit(*n)).collect::<Vec<_>>();
	let tuple = |name, tys| Variant::new(name, Shape::Tuple(tys));
	let permission = || named("StoredPermission");
	let durations = || opt(Ty::Duration);
	let number = || Ty::Native(Native::Number);
	let auth_limit = || {
		Field::new("auth_limit", named("AuthLimit")).start(2).missing(Missing::Value(|| {
			object(vec![
				("level", variant("Root", Value::None)),
				("role", Value::String("Owner".into())),
			])
		}))
	};
	vec![
		TypeDesc::transparent("RecordId", Ty::Native(Native::RecordId)),
		TypeDesc::transparent("RecordIdKey", Ty::Native(Native::RecordIdKey)),
		// `catalog/src/schema/base.rs`
		TypeDesc::enumeration("Base", 1, units(&["Root", "Ns", "Db"])),
		// `catalog/src/schema/index.rs`
		TypeDesc::structure(
			"StoredIndexDefinition",
			2,
			vec![
				Field::new("index_id", named("IndexId")),
				Field::new("name", Ty::String),
				Field::new("table_name", Ty::String),
				Field::new("cols", Ty::Vec(Box::new(text()))),
				Field::new("index", named("Index")),
				Field::new("comment", opt(Ty::String)),
				Field::new("prepare_remove", Ty::Bool),
				Field::new("format_version", Ty::Uint).start(2),
			],
		),
		TypeDesc::structure("CondText", 1, vec![Field::new("0", text())]),
		TypeDesc::enumeration(
			"Index",
			2,
			vec![
				unit("Idx"),
				unit("Uniq"),
				tuple("Hnsw", vec![named("HnswParams")]),
				tuple("FullText", vec![named("FullTextParams")]),
				tuple("Count", vec![opt(named("CondText"))]),
				tuple("DiskAnn", vec![named("DiskAnnParams")]).start(2),
			],
		),
		TypeDesc::structure(
			"FullTextParams",
			1,
			vec![
				Field::new("analyzer", Ty::String),
				Field::new("highlight", Ty::Bool),
				Field::new("scoring", named("Scoring")),
			],
		),
		TypeDesc::enumeration(
			"Scoring",
			1,
			vec![
				Variant::new(
					"Bm",
					Shape::Struct(vec![Field::new("k1", Ty::F32), Field::new("b", Ty::F32)]),
				),
				unit("Vs"),
			],
		),
		TypeDesc::enumeration(
			"Distance",
			2,
			vec![
				unit("Chebyshev"),
				unit("Cosine"),
				unit("Euclidean"),
				unit("Hamming"),
				unit("Jaccard"),
				unit("Manhattan"),
				tuple("Minkowski", vec![number()]),
				unit("Pearson"),
				unit("CosineNormalized").start(2),
				unit("InnerProduct").start(2),
			],
		),
		TypeDesc::enumeration(
			"VectorType",
			2,
			vec![
				unit("F64"),
				unit("F32"),
				unit("I64"),
				unit("I32"),
				unit("I16"),
				unit("F16").start(2),
				unit("I8").start(2),
				unit("U8").start(2),
			],
		),
		TypeDesc::structure(
			"HnswParams",
			2,
			vec![
				Field::new("dimension", Ty::Uint),
				Field::new("distance", named("Distance")),
				Field::new("vector_type", named("VectorType")),
				Field::new("m", Ty::U8),
				Field::new("m0", Ty::U8),
				Field::new("ml", number()),
				Field::new("ef_construction", Ty::Uint),
				Field::new("extend_candidates", Ty::Bool),
				Field::new("keep_pruned_connections", Ty::Bool),
				Field::new("use_hashed_vector", Ty::Bool).start(2),
			],
		),
		TypeDesc::structure(
			"DiskAnnParams",
			1,
			vec![
				Field::new("dimension", Ty::Uint),
				Field::new("distance", named("Distance")),
				Field::new("vector_type", named("VectorType")),
				Field::new("degree", Ty::Uint),
				Field::new("l_build", Ty::Uint),
				Field::new("alpha", number()),
				Field::new("use_hashed_vector", Ty::Bool),
			],
		),
		// `catalog/src/schema/access.rs`
		TypeDesc::enumeration(
			"AccessType",
			1,
			vec![
				tuple("Record", vec![named("RecordAccess")]),
				tuple("Jwt", vec![named("JwtAccess")]),
				tuple("Bearer", vec![named("BearerAccess")]),
			],
		),
		TypeDesc::structure(
			"RecordAccess",
			1,
			vec![
				Field::new("signup", opt(text())),
				Field::new("signin", opt(text())),
				Field::new("jwt", named("JwtAccess")),
				Field::new("bearer", opt(named("BearerAccess"))),
			],
		),
		TypeDesc::structure(
			"BearerAccess",
			1,
			vec![
				Field::new("kind", named("BearerAccessType")),
				Field::new("subject", named("BearerAccessSubject")),
				Field::new("jwt", named("JwtAccess")),
			],
		),
		TypeDesc::enumeration("BearerAccessType", 1, units(&["Bearer", "Refresh"])),
		TypeDesc::enumeration("BearerAccessSubject", 1, units(&["Record", "User"])),
		TypeDesc::structure(
			"JwtAccess",
			2,
			vec![
				Field::new("verify", named("JwtAccessVerify")),
				Field::new("issue", opt(named("JwtAccessIssue"))),
				Field::new("audience", opt(Ty::Vec(Box::new(Ty::String)))).start(2),
			],
		),
		TypeDesc::enumeration(
			"JwtAccessVerify",
			1,
			vec![
				tuple("Key", vec![named("JwtAccessVerifyKey")]),
				tuple("Jwks", vec![named("JwtAccessVerifyJwks")]),
			],
		),
		TypeDesc::structure(
			"JwtAccessVerifyKey",
			1,
			vec![Field::new("alg", named("Algorithm")), Field::new("key", Ty::String)],
		),
		TypeDesc::structure("JwtAccessVerifyJwks", 1, vec![Field::new("url", Ty::String)]),
		TypeDesc::structure(
			"JwtAccessIssue",
			1,
			vec![Field::new("alg", named("Algorithm")), Field::new("key", Ty::String)],
		),
		TypeDesc::enumeration(
			"Algorithm",
			1,
			units(&[
				"EdDSA", "Es256", "Es384", "Es512", "Hs256", "Hs384", "Hs512", "Ps256", "Ps384",
				"Ps512", "Rs256", "Rs384", "Rs512",
			]),
		),
		TypeDesc::structure(
			"StoredAccessDefinition",
			2,
			vec![
				Field::new("name", Ty::String),
				Field::new("access_type", named("AccessType")),
				Field::new("base", named("Base")),
				Field::new("authenticate", opt(text())),
				Field::new("context", opt(text())).start(2),
				Field::new("grant_duration", durations()),
				Field::new("token_duration", durations()),
				Field::new("session_duration", durations()),
				Field::new("comment", opt(Ty::String)),
			],
		),
		// `catalog/src/schema/user.rs`, `iam/src/scram.rs`
		TypeDesc::structure(
			"UserDefinition",
			2,
			vec![
				Field::new("name", Ty::String),
				Field::new("hash", Ty::String),
				Field::new("code", Ty::String),
				Field::new("roles", Ty::Vec(Box::new(Ty::String))),
				Field::new("token_duration", durations()),
				Field::new("session_duration", durations()),
				Field::new("comment", opt(Ty::String)),
				Field::new("base", named("Base")),
				Field::new("scram", opt(named("ScramCredential"))).start(2),
			],
		),
		TypeDesc::structure(
			"ScramCredential",
			1,
			vec![
				Field::new("iterations", Ty::Uint),
				Field::new("salt", Ty::Bytes),
				Field::new("stored_key", Ty::Bytes),
				Field::new("server_key", Ty::Bytes),
			],
		),
		// `catalog/src/access.rs`
		TypeDesc::enumeration(
			"Subject",
			1,
			vec![
				tuple("Record", vec![Ty::Native(Native::RecordId)]),
				tuple("User", vec![Ty::String]),
			],
		),
		TypeDesc::enumeration(
			"Grant",
			1,
			vec![
				tuple("Jwt", vec![named("GrantJwt")]),
				tuple("Record", vec![named("GrantRecord")]),
				tuple("Bearer", vec![named("GrantBearer")]),
			],
		),
		TypeDesc::structure(
			"GrantJwt",
			1,
			vec![Field::new("jti", Ty::Uuid), Field::new("token", opt(Ty::String))],
		),
		TypeDesc::structure(
			"GrantRecord",
			1,
			vec![
				Field::new("rid", Ty::Uuid),
				Field::new("jti", Ty::Uuid),
				Field::new("token", opt(Ty::String)),
			],
		),
		TypeDesc::structure(
			"GrantBearer",
			1,
			vec![Field::new("id", Ty::String), Field::new("key", Ty::String)],
		),
		TypeDesc::structure(
			"AccessGrant",
			1,
			vec![
				Field::new("id", Ty::String),
				Field::new("ac", Ty::String),
				Field::new("creation", Ty::Native(Native::Datetime)),
				Field::new("expiration", opt(Ty::Native(Native::Datetime))),
				Field::new("revocation", opt(Ty::Native(Native::Datetime))),
				Field::new("subject", named("Subject")),
				Field::new("grant", named("Grant")),
			],
		),
		// `catalog/src/schema/config.rs`, `expr/src/expr/graphql_config.rs`
		TypeDesc::enumeration(
			"StoredConfigDefinition",
			1,
			vec![
				tuple("Default", vec![named("DefaultConfig")]),
				tuple("GraphQL", vec![named("GraphQLConfig")]),
				tuple("Api", vec![named("StoredApiConfigDefinition")]),
			],
		),
		TypeDesc::structure(
			"DefaultConfig",
			1,
			vec![Field::new("namespace", opt(Ty::String)), Field::new("database", opt(Ty::String))],
		),
		TypeDesc::structure(
			"GraphQLConfig",
			3,
			vec![
				Field::new("tables", named("GraphQLTablesConfig")),
				Field::new("functions", named("GraphQLFunctionsConfig")),
				Field::new("depth_limit", opt(Ty::Uint)).start(2),
				Field::new("complexity_limit", opt(Ty::Uint)).start(2),
				Field::new("introspection", named("GraphQLIntrospectionConfig"))
					.start(3)
					.missing(Missing::Value(|| variant("Auto", Value::None))),
			],
		),
		TypeDesc::enumeration(
			"GraphQLTablesConfig",
			1,
			vec![
				unit("None"),
				unit("Auto"),
				tuple("Include", vec![Ty::Vec(Box::new(Ty::String))]),
				tuple("Exclude", vec![Ty::Vec(Box::new(Ty::String))]),
			],
		),
		TypeDesc::enumeration(
			"GraphQLFunctionsConfig",
			1,
			vec![
				unit("None"),
				unit("Auto"),
				tuple("Include", vec![Ty::Vec(Box::new(Ty::String))]),
				tuple("Exclude", vec![Ty::Vec(Box::new(Ty::String))]),
			],
		),
		TypeDesc::enumeration("GraphQLIntrospectionConfig", 1, units(&["Auto", "None"])),
		// `catalog/src/schema/module.rs`, `catalog/src/module.rs`
		TypeDesc::structure(
			"StoredModuleDefinition",
			2,
			vec![
				Field::new("name", opt(Ty::String)),
				Field::new("comment", opt(Ty::String)),
				Field::new("permissions", permission()),
				Field::new("executable", named("ModuleExecutable")),
				Field::new("unsigned", Ty::Bool)
					.start(2)
					.missing(Missing::Value(|| Value::Bool(true))),
			],
		),
		TypeDesc::enumeration(
			"ModuleExecutable",
			1,
			vec![
				tuple("Surrealism", vec![named("SurrealismExecutable")]),
				tuple("Silo", vec![named("SiloExecutable")]),
			],
		),
		TypeDesc::structure(
			"SurrealismExecutable",
			1,
			vec![Field::new("bucket", Ty::String), Field::new("key", Ty::String)],
		),
		TypeDesc::structure(
			"SiloExecutable",
			1,
			vec![
				Field::new("organisation", Ty::String),
				Field::new("package", Ty::String),
				Field::new("major", Ty::Uint),
				Field::new("minor", Ty::Uint),
				Field::new("patch", Ty::Uint),
			],
		),
		// `catalog/src/schema/api.rs`
		TypeDesc::enumeration(
			"ApiMethod",
			1,
			units(&["Delete", "Get", "Patch", "Post", "Put", "Trace"]),
		),
		TypeDesc::structure(
			"StoredApiDefinition",
			2,
			vec![
				Field::new("path", text()),
				Field::new("actions", Ty::Vec(Box::new(named("StoredApiActionDefinition")))),
				Field::new("fallback", opt(text())),
				Field::new("config", named("StoredApiConfigDefinition")),
				Field::new("comment", opt(Ty::String)),
				auth_limit(),
			],
		),
		TypeDesc::structure(
			"StoredApiActionDefinition",
			1,
			vec![
				Field::new("methods", Ty::Vec(Box::new(named("ApiMethod")))),
				Field::new("action", text()),
				Field::new("config", named("StoredApiConfigDefinition")),
			],
		),
		TypeDesc::structure(
			"StoredApiConfigDefinition",
			1,
			vec![
				Field::new("middleware", Ty::Vec(Box::new(named("MiddlewareDefinition")))),
				Field::new("permissions", permission()),
			],
		),
		TypeDesc::structure(
			"MiddlewareDefinition",
			1,
			vec![
				Field::new("name", Ty::String),
				Field::new("args", Ty::Vec(Box::new(Ty::Native(Native::Value)))),
			],
		),
		// `catalog/src/schema/bucket.rs`
		TypeDesc::structure("BucketId", 1, vec![Field::new("0", Ty::Uint)]),
		TypeDesc::structure(
			"StoredBucketDefinition",
			1,
			vec![
				Field::new("id", opt(named("BucketId"))),
				Field::new("name", Ty::String),
				Field::new("backend", opt(Ty::String)),
				Field::new("permissions", permission()),
				Field::new("readonly", Ty::Bool),
				Field::new("comment", opt(Ty::String)),
			],
		),
		// `catalog/src/schema/param.rs`
		TypeDesc::structure(
			"StoredParamDefinition",
			1,
			vec![
				Field::new("name", Ty::String),
				Field::new("value", Ty::Native(Native::Value)),
				Field::new("comment", opt(Ty::String)),
				Field::new("permissions", permission()),
			],
		),
		// `catalog/src/schema/analyzer.rs`, `expr/src/expr/{tokenizer,filter,language}.rs`
		TypeDesc::structure(
			"AnalyzerDefinition",
			1,
			vec![
				Field::new("name", Ty::String),
				Field::new("function", opt(Ty::String)),
				Field::new("tokenizers", opt(Ty::Vec(Box::new(named("Tokenizer"))))),
				Field::new("filters", opt(Ty::Vec(Box::new(named("Filter"))))),
				Field::new("comment", opt(Ty::String)),
			],
		),
		TypeDesc::enumeration("SegmentLanguage", 1, units(&["Chinese", "Japanese", "Korean"])),
		TypeDesc::enumeration(
			"Tokenizer",
			2,
			vec![
				unit("Blank"),
				unit("Camel"),
				unit("Class"),
				unit("Punct"),
				tuple("Segment", vec![named("SegmentLanguage")]).start(2),
			],
		),
		TypeDesc::enumeration(
			"Filter",
			1,
			vec![
				unit("Ascii"),
				tuple("EdgeNgram", vec![Ty::Uint, Ty::Uint]),
				unit("Lowercase"),
				tuple("Ngram", vec![Ty::Uint, Ty::Uint]),
				tuple("Snowball", vec![named("Language")]),
				unit("Uppercase"),
				tuple("Mapper", vec![Ty::String]),
			],
		),
		TypeDesc::enumeration(
			"Language",
			1,
			units(&[
				"Arabic",
				"Danish",
				"Dutch",
				"English",
				"Finnish",
				"French",
				"German",
				"Greek",
				"Hungarian",
				"Italian",
				"Norwegian",
				"Portuguese",
				"Romanian",
				"Russian",
				"Spanish",
				"Swedish",
				"Tamil",
				"Turkish",
			]),
		),
		// `catalog/src/schema/ml/model.rs`
		TypeDesc::structure(
			"StoredMlModelDefinition",
			1,
			vec![
				Field::new("hash", Ty::String),
				Field::new("name", Ty::String),
				Field::new("version", Ty::String),
				Field::new("comment", opt(Ty::String)),
				Field::new("permissions", permission()),
			],
		),
		// `catalog/src/subscription.rs`. Revision 1 stored the field list and
		// fetch clauses as an AST.
		TypeDesc::enumeration(
			"StoredSubscriptionFields",
			2,
			vec![
				unit("Diff"),
				tuple("Select", vec![named("Fields")]).end(2, select_text),
				tuple("Select", vec![text()]).start(2),
			],
		),
		TypeDesc::structure(
			"StoredSubscriptionDefinition",
			2,
			vec![
				Field::new("id", Ty::Uuid),
				Field::new("node", Ty::Uuid),
				Field::new("fields", named("StoredSubscriptionFields")),
				Field::new("what", text()),
				Field::new("cond", opt(text())),
				Field::new("old_fetch", opt(named("Fetchs"))).end(2),
				Field::new("fetch", opt(Ty::Vec(Box::new(text())))).start(2).missing(
					Missing::Convert {
						from: "old_fetch",
						convert: fetch_texts,
					},
				),
				Field::new("auth", opt(named("Auth"))),
				Field::new("session", opt(Ty::Native(Native::Value))),
				Field::new(
					"vars",
					Ty::Map(Box::new(Ty::String), Box::new(Ty::Native(Native::Value))),
				),
			],
		),
		TypeDesc::structure("Fetchs", 1, vec![Field::new("0", Ty::Vec(Box::new(named("Fetch"))))]),
		TypeDesc::structure("Fetch", 1, vec![Field::new("0", text())]),
		TypeDesc::structure(
			"NodeLiveQuery",
			1,
			vec![
				Field::new("ns", named("NamespaceId")),
				Field::new("db", named("DatabaseId")),
				Field::new("tb", Ty::String),
			],
		),
		// `iam/src/auth.rs`, `iam/src/entities`
		TypeDesc::structure("Auth", 1, vec![Field::new("actor", named("Actor"))]),
		TypeDesc::structure(
			"Actor",
			1,
			vec![
				Field::new("res", named("Resource")),
				Field::new("roles", Ty::Vec(Box::new(named("Role")))),
			],
		),
		TypeDesc::enumeration("Role", 1, units(&["Viewer", "Editor", "Owner"])),
		TypeDesc::structure(
			"Resource",
			1,
			vec![
				Field::new("id", Ty::String),
				Field::new("kind", named("ResourceKind")),
				Field::new("level", named("Level")),
			],
		),
		TypeDesc::enumeration(
			"Level",
			1,
			vec![
				unit("No"),
				unit("Root"),
				tuple("Namespace", vec![Ty::String]),
				tuple("Database", vec![Ty::String, Ty::String]),
				tuple("Record", vec![Ty::String, Ty::String, Ty::String]),
			],
		),
		TypeDesc::enumeration(
			"ResourceKind",
			5,
			vec![
				unit("Any"),
				unit("Namespace"),
				unit("Database"),
				unit("Record"),
				unit("Table"),
				unit("Document"),
				unit("Option"),
				unit("Function"),
				unit("Module"),
				unit("Analyzer"),
				unit("Parameter"),
				unit("Model"),
				unit("Event"),
				unit("Field"),
				unit("Index"),
				unit("Access"),
				tuple("Config", vec![named("ConfigKind")]).start(2),
				unit("Api").start(3),
				unit("Bucket").start(4),
				unit("Sequence").start(5),
				unit("Actor"),
			],
		),
		TypeDesc::enumeration("ConfigKind", 1, units(&["GraphQL", "Api", "Default"])),
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

/// `Relation`'s revision-1 endpoints, an optional `record<…>` kind, as the
/// tables it names.
fn record_tables(kind: Value) -> Result<Value> {
	match kind {
		Value::None => Ok(Value::Array(Default::default())),
		Value::Object(o) => match o.get("Record") {
			Some(tables) => Ok(tables.clone()),
			None => anyhow::bail!("a relation endpoint kind that is not a record kind"),
		},
		other => anyhow::bail!("a relation endpoint kind {other:?}"),
	}
}

/// `value`, decoded by its descriptor as type `name`, as the Rust type the
/// descriptor transcribes: the type a revision's `convert_fn` renders.
fn as_rust<T: revision::DeserializeRevisioned>(name: &str, value: &Value) -> Result<T> {
	let bytes = super::encode(name, value)?;
	revision::from_slice(&bytes).map_err(|e| anyhow!("re-reading a {name}: {e}"))
}

/// A stored `Kind` as the canonical kind-grammar text the current revision
/// stores, rendered as its `convert_fn` renders it.
// A conversion takes the decoded value by value (`Missing::Convert`).
#[allow(clippy::needless_pass_by_value)]
fn kind_text(kind: Value) -> Result<Value> {
	let kind: crate::expr::Kind = as_rust("Kind", &kind)?;
	Ok(Value::String(crate::catalog::KindText::new(&kind).as_str().into()))
}

/// An optional stored `Kind` as optional kind-grammar text.
fn opt_kind_text(kind: Value) -> Result<Value> {
	match kind {
		Value::None => Ok(Value::None),
		kind => kind_text(kind),
	}
}

/// A function's `(name, Kind)` arguments as `(name, kind text)`.
fn arg_kind_texts(args: Value) -> Result<Value> {
	let Value::Array(args) = args else {
		anyhow::bail!("function arguments that are not an array: {args:?}");
	};
	let mut out = Vec::with_capacity(args.len());
	for arg in args.0 {
		let Value::Array(pair) = arg else {
			anyhow::bail!("a function argument that is not a pair: {arg:?}");
		};
		let [name, kind] = <[Value; 2]>::try_from(pair.0)
			.map_err(|pair| anyhow!("a function argument of {} parts", pair.len()))?;
		out.push(Value::Array(vec![name, kind_text(kind)?].into()));
	}
	Ok(Value::Array(out.into()))
}

/// A subscription's optional stored `Fetchs` as each target's canonical text.
// A conversion takes the decoded value by value (`Missing::Convert`).
#[allow(clippy::needless_pass_by_value)]
fn fetch_texts(fetchs: Value) -> Result<Value> {
	if fetchs.is_none() {
		return Ok(Value::None);
	}
	let fetchs: crate::expr::Fetchs = as_rust("Fetchs", &fetchs)?;
	Ok(Value::Array(
		fetchs
			.into_iter()
			.map(|f| Value::String(crate::catalog::ExprText::new(&f.0).as_str().into()))
			.collect::<Vec<_>>()
			.into(),
	))
}

/// A subscription's stored `Fields` selection as the `Select` of its
/// canonical text.
// A conversion takes the decoded value by value (`Missing::Convert`).
#[allow(clippy::needless_pass_by_value)]
fn select_text(fields: Value) -> Result<Value> {
	let fields: crate::expr::Fields = as_rust("Fields", &fields)?;
	Ok(variant("Select", Value::String(crate::catalog::FieldsText::new(&fields).as_str().into())))
}

fn object(fields: Vec<(&str, Value)>) -> Value {
	let mut object = Object::default();
	for (k, v) in fields {
		object.insert(k.to_owned(), v);
	}
	Value::Object(object)
}

fn variant(name: &str, body: Value) -> Value {
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
		let descs: Vec<TypeDesc> = all()
			.into_iter()
			.chain(record_types())
			.chain(schema_types())
			.chain(kind_types())
			.chain(definition_types())
			.chain(datastore_types())
			.collect();
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
