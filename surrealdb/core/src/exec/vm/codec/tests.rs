use super::*;

// Which types must have a descriptor is decided by the compat tests: every
// revisioned type a fixture covers (`kvs/compat/tests.rs`).

/// Every revisioned type the compat fixtures cover and the VM describes:
/// decoding each fixture of it and re-encoding writes exactly what the Rust
/// codec writes for the same fixture. The compat tests run the check
/// themselves (`kvs/compat/tests.rs`); this names the types that must pass
/// it, so a descriptor that stops matching cannot be dropped silently.
#[test]
fn integers_round_trip_at_every_varint_width() {
	for v in
		[0u128, 250, 251, 65_535, 65_536, u32::MAX as u128, u32::MAX as u128 + 1, u64::MAX as u128]
	{
		let mut w = wire::Writer::default();
		w.varint(v);
		let bytes = w.into_bytes();
		assert_eq!(wire::Reader::new(&bytes).varint().unwrap(), v);
	}
	for v in [0i128, -1, 1, i64::MIN as i128, i64::MAX as i128] {
		let mut w = wire::Writer::default();
		w.zigzag(v);
		let bytes = w.into_bytes();
		assert_eq!(wire::Reader::new(&bytes).zigzag().unwrap(), v);
	}
}

mod keys {
	use std::borrow::Cow;

	use surrealdb_kvs::key::KeyStep;

	use super::key;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::KVKey;
	use crate::key::schema::{
		DatabaseKey, EventKey, FieldKey, FunctionKey, NamespaceKey, ParamKey, ReclaimKey, TableKey,
	};
	use crate::val::{Number, Object, TableName, Value};

	fn fields(pairs: &[(&str, Value)]) -> Value {
		let mut object = Object::default();
		for (k, v) in pairs {
			object.insert((*k).to_owned(), v.clone());
		}
		Value::Object(object)
	}

	fn int(v: i64) -> Value {
		Value::Number(Number::Int(v))
	}

	fn string(s: &str) -> Value {
		Value::String(s.into())
	}

	/// A name with every byte the key encoding has to escape.
	const AWKWARD: &str = "a\u{0}b\u{1}c";

	#[test]
	fn the_vm_encodes_catalog_keys_as_the_schema_does() {
		let (ns, db) = (NamespaceId(7), DatabaseId(300));
		let tb = TableName::new(AWKWARD);
		let mut cases: Vec<(&str, Vec<u8>, Value)> = vec![
			(
				"NamespaceKey",
				NamespaceKey::new(Cow::Borrowed(AWKWARD)).encode_key().unwrap().to_vec(),
				fields(&[("ns", string(AWKWARD))]),
			),
			(
				"DatabaseKey",
				DatabaseKey::new(ns, Cow::Borrowed("db")).encode_key().unwrap().to_vec(),
				fields(&[("ns", int(7)), ("db", string("db"))]),
			),
			(
				"TableKey",
				TableKey::new(ns, db, Cow::Borrowed(&tb)).encode_key().unwrap().to_vec(),
				fields(&[("ns", int(7)), ("db", int(300)), ("tb", string(AWKWARD))]),
			),
			(
				"FieldKey",
				FieldKey::new(ns, db, Cow::Borrowed(&tb), Cow::Borrowed("a.b"))
					.encode_key()
					.unwrap()
					.to_vec(),
				fields(&[
					("ns", int(7)),
					("db", int(300)),
					("tb", string(AWKWARD)),
					("fd", string("a.b")),
				]),
			),
			(
				"EventKey",
				EventKey::new(ns, db, Cow::Borrowed(&tb), Cow::Borrowed("ev"))
					.encode_key()
					.unwrap()
					.to_vec(),
				fields(&[
					("ns", int(7)),
					("db", int(300)),
					("tb", string(AWKWARD)),
					("ev", string("ev")),
				]),
			),
			(
				"FunctionKey",
				FunctionKey::new(ns, db, Cow::Borrowed("f")).encode_key().unwrap().to_vec(),
				fields(&[("ns", int(7)), ("db", int(300)), ("fc", string("f"))]),
			),
			(
				"ParamKey",
				ParamKey::new(ns, db, Cow::Borrowed("p")).encode_key().unwrap().to_vec(),
				fields(&[("ns", int(7)), ("db", int(300)), ("pa", string("p"))]),
			),
		];
		let uid = uuid::Uuid::from_u128(0x0102_0304);
		let reclaim = |kind: i64, db: i64, expunge: i64| {
			fields(&[
				("kind", int(kind)),
				("ns", int(7)),
				("db", int(db)),
				("tb", string("")),
				("ix", int(0)),
				("expunge", int(expunge)),
				("uid", Value::Uuid(crate::val::Uuid(uid))),
			])
		};
		cases.push((
			"ReclaimKey",
			ReclaimKey::namespace(ns, true, uid).encode_key().unwrap().to_vec(),
			reclaim(0, 0, 1),
		));
		cases.push((
			"ReclaimKey",
			ReclaimKey::database(ns, db, false, uid).encode_key().unwrap().to_vec(),
			reclaim(1, 300, 0),
		));
		for (name, rust, fields) in cases {
			assert_eq!(key::encode(name, &fields).unwrap(), rust, "{name}");
			assert_eq!(key::decode(name, &rust).unwrap(), fields, "{name}");
		}
	}

	/// Every route of simple fields round-trips through the VM's encoding.
	#[test]
	fn every_route_of_simple_fields_round_trips() {
		let sample = |ty: &str| -> Option<Value> {
			Some(match ty {
				"ReclaimKind" | "Expunge" => int(1),
				"u16" | "TaskLeaseType" => int(513),
				"u32"
				| "NamespaceId"
				| "DatabaseId"
				| "IndexId"
				| "BuildTicketMutationSeq"
				| "AppendingId"
				| "BatchId" => int(70_000),
				"u64" | "DocId" | "BuildGeneration" | "BuildTicket" | "ElementId" => int(1 << 40),
				"i64" => int(-5),
				"bool" => Value::Bool(true),
				"Uuid" => Value::Uuid(crate::val::Uuid(uuid::Uuid::from_u128(0x0102))),
				"Str" | "Table" => string(AWKWARD),
				_ => return None,
			})
		};
		let mut checked = 0;
		for route in surrealdb_datastore::key::schema::KEY_ROUTES {
			let mut pairs = Vec::new();
			let simple = route.steps.iter().all(|step| match step {
				KeyStep::Lit(_) => true,
				KeyStep::Field {
					name,
					ty,
				} => sample(ty).map(|v| pairs.push((*name, v))).is_some(),
				KeyStep::List {
					..
				} => false,
			});
			if !simple {
				continue;
			}
			let fields = fields(&pairs);
			let bytes = key::encode(route.name, &fields).unwrap();
			assert_eq!(key::decode(route.name, &bytes).unwrap(), fields, "{}", route.name);
			checked += 1;
		}
		assert!(checked > 50, "only {checked} routes were simple");
	}

	/// The pending-entries marker encoded from borrowed fields spells exactly
	/// the bytes the typed key spells: the record id as an identity, under the
	/// index format.
	#[test]
	fn the_pending_marker_matches_its_typed_key() {
		use key::KeyField;

		use crate::key::schema::DocPendingKey;
		use crate::val::{Array, RecordIdKey, Strand};
		let tb = TableName::from("t\u{1}b");
		let ids = [
			RecordIdKey::Number(-4),
			RecordIdKey::String(Strand::new_static("a\u{0}b")),
			RecordIdKey::Array(Array(vec![int(1), Value::from(2.5_f64)])),
		];
		for id in &ids {
			let typed = DocPendingKey::new(
				NamespaceId(1),
				DatabaseId(2),
				Cow::Borrowed(&tb),
				crate::val::RecordIdentity(id.clone()),
			)
			.encode_key()
			.unwrap();
			let vm = key::encode_fields(
				"DocPendingKey",
				&[
					("ns", KeyField::Int(1)),
					("db", KeyField::Int(2)),
					("tb", KeyField::Str(tb.as_str())),
					("id", KeyField::Id(id)),
				],
			)
			.unwrap();
			assert_eq!(vm, typed.to_vec(), "pending {id:?}");
		}
	}
}

/// A reclaim queue entry's state, which no frozen fixture covers, reads and
/// writes as the Rust type does at its current revision.
#[test]
fn reclaim_state_matches_its_revisioned_encoding() {
	use surrealdb_datastore::key::reclaim::ReclaimState;
	let cases = [
		ReclaimState::enqueued(),
		ReclaimState {
			observed_ms: 1 << 40,
			cursor: Some(vec![0, 1, 0xff]),
		},
	];
	for state in cases {
		let rust = revision::to_vec(&state).unwrap();
		let value = super::decode(&FULL_CODEC, "ReclaimState", &rust).unwrap();
		assert_eq!(super::encode(&FULL_CODEC, "ReclaimState", &value).unwrap(), rust, "{state:?}");
	}
}

/// An aggregated view row, whose metadata carries a stat of every kind, reads
/// as the Rust `Record` does in either encoding, into one value, and writes
/// exactly the bytes the Rust record's encoding writes.
#[test]
fn a_view_rows_stats_read_in_either_encoding_and_write_the_stored_one() {
	use surrealdb_kvs::value::KVValue;

	use crate::catalog::aggregation::AggregationStat;
	use crate::catalog::{Metadata, Record, RecordType};
	use crate::val::{Datetime, Number, Object};
	let n = |v: f64| Number::Float(v);
	let stats = vec![
		AggregationStat::Count {
			count: 3,
		},
		AggregationStat::CountValue {
			arg: 0,
			count: -1,
		},
		AggregationStat::NumberMax {
			arg: 1,
			max: Number::Int(9),
		},
		AggregationStat::NumberMin {
			arg: 2,
			min: n(f64::INFINITY),
		},
		AggregationStat::Sum {
			arg: 3,
			sum: Number::Decimal(rust_decimal::Decimal::new(125, 2)),
		},
		AggregationStat::Mean {
			arg: 4,
			sum: n(7.5),
			count: 2,
		},
		AggregationStat::StdDev {
			arg: 5,
			shift: n(2.0),
			sum: n(1.0),
			sum_of_squares: n(3.0),
			count: 4,
		},
		AggregationStat::Variance {
			arg: 6,
			shift: n(0.0),
			sum: Number::Int(0),
			sum_of_squares: Number::Int(0),
			count: 0,
		},
		AggregationStat::TimeMax {
			arg: 7,
			max: Datetime::MIN_UTC,
		},
		AggregationStat::TimeMin {
			arg: 8,
			min: Datetime::MAX_UTC,
		},
		AggregationStat::Accumulate {
			arg: 9,
			values: vec![Value::from(1), Value::from("a"), Value::None],
		},
		AggregationStat::TemporalMax {
			arg: 10,
			max: Value::Date(crate::val::Date(19_000)),
		},
		AggregationStat::TemporalMin {
			arg: 11,
			min: Value::Timestamp(crate::val::Date(-3).at_midnight()),
		},
	];
	let mut data = Object::default();
	data.insert("n".to_owned(), Value::from(3));
	for stats in [Vec::new(), stats] {
		let record = Record {
			metadata: Some(Metadata {
				record_type: RecordType::Table,
				aggregation_stats: stats,
			}),
			data: Value::Object(data.clone()),
		};
		let legacy = revision::to_vec(&record).unwrap();
		let stored = record.kv_encode_value().unwrap();
		let value = super::decode(&FULL_CODEC, "Record", &legacy).unwrap();
		assert_eq!(super::decode(&FULL_CODEC, "Record", &stored).unwrap(), value, "{record:?}");
		assert_eq!(super::encode(&FULL_CODEC, "Record", &value).unwrap(), stored, "{record:?}");
	}
}

/// A route or type named by id encodes and decodes exactly as the same one
/// named by its name, and every route's id resolves back to its own name.
#[test]
fn ids_name_the_routes_and_types_their_names_do() {
	for route in surrealdb_datastore::key::schema::KEY_ROUTES {
		assert_eq!(key::RouteId::named(route.name).name(), route.name);
	}
	let utf8 = encode_id(&FULL_CODEC, TypeId::UTF8, &Value::String("a".into())).unwrap();
	assert_eq!(utf8, b"a");
	assert_eq!(decode_id(&FULL_CODEC, TypeId::UTF8, &utf8).unwrap(), Value::String("a".into()));
	let record = crate::catalog::Record::new(Value::None);
	let legacy = revision::to_vec(&record).unwrap();
	let stored = surrealdb_kvs::value::KVValue::kv_encode_value(&record).unwrap();
	let value = decode(&FULL_CODEC, "Record", &legacy).unwrap();
	let id = TypeId::named("Record");
	assert_eq!(decode_id(&FULL_CODEC, id, &legacy).unwrap(), value);
	assert_eq!(decode_id(&FULL_CODEC, id, &stored).unwrap(), value);
	assert_eq!(encode_id(&FULL_CODEC, id, &value).unwrap(), stored);
}

/// A function definition as 3.2.4 wrote it, at revision 3 with its argument
/// kinds stored as `Kind` (`DEFINE FUNCTION fn::test_function_1($a: number,
/// $b: array<int>) {} COMMENT "A function comment" PERMISSIONS NONE`), reads
/// with each kind as the text the Rust `convert_fn` renders, and writes back
/// exactly what the Rust codec writes for it.
#[test]
fn a_function_with_kinds_stored_as_an_ast_reads_as_kind_text() {
	use crate::catalog::StoredFunctionDefinition;
	use crate::key::KVValue;
	const WRITTEN_BY_3_2_4: &[u8] = &[
		3, 15, 116, 101, 115, 116, 95, 102, 117, 110, 99, 116, 105, 111, 110, 95, 49, 2, 1, 97, 1,
		10, 1, 98, 1, 20, 1, 9, 0, 3, 123, 59, 125, 1, 18, 65, 32, 102, 117, 110, 99, 116, 105,
		111, 110, 32, 99, 111, 109, 109, 101, 110, 116, 1, 0, 0, 1, 1, 1, 1, 5, 79, 119, 110, 101,
		114, 0, 0,
	];
	let rust = StoredFunctionDefinition::kv_decode_value(WRITTEN_BY_3_2_4, ())
		.unwrap()
		.kv_encode_value()
		.unwrap();
	let value = decode(&FULL_CODEC, "StoredFunctionDefinition", WRITTEN_BY_3_2_4).unwrap();
	let Value::Object(fields) = &value else {
		panic!("a function decodes to an object: {value:?}");
	};
	let arg = |name: &str, kind: &str| {
		Value::Array(vec![Value::String(name.into()), Value::String(kind.into())].into())
	};
	assert_eq!(
		fields.get("args"),
		Some(&Value::Array(vec![arg("a", "number"), arg("b", "array<int>")].into()))
	);
	assert_eq!(fields.get("returns"), Some(&Value::None));
	assert_eq!(encode(&FULL_CODEC, "StoredFunctionDefinition", &value).unwrap(), rust);
}

/// A table whose view is the `Aggregated` form, which predates a view's
/// clauses being stored as text and which no frozen fixture covers, reads as
/// the Rust codec reads it and is written back with the view in its current
/// form. A view has no descriptor of its own: the VM reads it through the
/// table that holds it.
#[test]
fn an_aggregated_view_reads_as_its_revisioned_encoding_and_is_written_as_clauses() {
	use crate::catalog::aggregation::AggregationAnalysis;
	use crate::catalog::view::StoredViewDefinition;
	let ast = crate::syn::parse(
		"SELECT count() AS total, math::sum(n) AS sum, b FROM x WHERE n > 1 GROUP BY b",
	)
	.unwrap();
	let Some(surrealdb_expr::parser::TopLevelExpr::Expr(expr)) = ast.expressions.into_iter().next()
	else {
		panic!("a select statement");
	};
	let crate::expr::Expr::Select(select) = expr else {
		panic!("a select statement");
	};
	let groups = select.group.clone().unwrap();
	let view = StoredViewDefinition::Aggregated {
		analysis: AggregationAnalysis::analyze_fields_groups(&select.fields, &groups, true)
			.unwrap(),
		condition: select.cond.clone().map(|c| c.0),
		tables: vec![crate::val::TableName::new("x")],
		groups,
		fields: select.fields.clone(),
	};
	let mut table = crate::catalog::StoredTableDefinition::new(
		crate::catalog::NamespaceId(1),
		crate::catalog::DatabaseId(2),
		crate::catalog::TableId(3),
		crate::val::TableName::new("t"),
	);
	table.view = Some(view.clone());
	let rust = revision::to_vec(&table).unwrap();
	let value = decode(&FULL_CODEC, "StoredTableDefinition", &rust).unwrap();
	assert_eq!(
		value,
		crate::catalog::stored::StoredField::to_vm_value(&table).unwrap(),
		"{view:?}"
	);
	let written = encode(&FULL_CODEC, "StoredTableDefinition", &value).unwrap();
	let read = <crate::catalog::StoredTableDefinition as crate::key::KVValue>::kv_decode_value(
		&written,
		(),
	)
	.unwrap();
	assert_eq!(read.view, Some(view.current()), "{view:?}");
}

/// The kinds the edge codec leaves out refuse every read and write with the name of the kind, and
/// the kinds of the api, config, ml and module families stay in it.
#[test]
fn the_edge_codec_refuses_the_kinds_it_leaves_out_by_name() {
	let unserved = [
		"StoredAccessDefinition",
		"UserDefinition",
		"Node",
		"NodeLiveQuery",
		"AccessGrant",
		"StoredBucketDefinition",
	];
	for name in unserved {
		let message = format!("catalog kind {name} is not served by this entry");
		let error = decode(&EDGE_CODEC, name, &[]).unwrap_err();
		assert_eq!(error.to_string(), message);
		let error = encode(&EDGE_CODEC, name, &Value::None).unwrap_err();
		assert_eq!(error.to_string(), message);
		// The kind is still a name the full codec reads, at the index the edge codec holds it.
		assert!(describes(&FULL_CODEC, name), "{name}");
		let id = TypeId::named(name);
		assert_eq!(decode_id(&EDGE_CODEC, id, &[]).unwrap_err().to_string(), message);
	}
	for name in [
		"StoredApiDefinition",
		"StoredConfigDefinition",
		"StoredMlModelDefinition",
		"StoredModuleDefinition",
		"StoredTableDefinition",
		"StoredFieldDefinition",
		"StoredSubscriptionDefinition",
	] {
		let id = TypeId::named(name);
		let error = decode_id(&EDGE_CODEC, id, &[]).unwrap_err();
		assert!(!error.to_string().contains("is not served"), "{name}: {error}");
	}
}

/// Every codec lists the catalog kinds in the same order, so a program names a type by one index
/// for any of them.
#[test]
fn the_edge_leaf_and_full_codecs_index_every_type_alike() {
	use crate::catalog::stored::{CATALOG_TYPES, EDGE_CATALOG_TYPES, LEAF_CATALOG_TYPES};
	assert_eq!(CATALOG_TYPES.len(), EDGE_CATALOG_TYPES.len());
	assert_eq!(CATALOG_TYPES.len(), LEAF_CATALOG_TYPES.len());
	for ((full, edge), leaf) in CATALOG_TYPES.iter().zip(EDGE_CATALOG_TYPES).zip(LEAF_CATALOG_TYPES)
	{
		assert_eq!(full.name, edge.name);
		assert_eq!(full.name, leaf.name);
	}
	for desc in CATALOG_TYPES {
		let id = TypeId::named(desc.name);
		let full = id.desc(&FULL_CODEC).unwrap().unwrap().name;
		let edge = id.desc(&EDGE_CODEC).unwrap().unwrap().name;
		let leaf = id.desc(&LEAF_CODEC).unwrap().unwrap().name;
		assert_eq!(full, edge);
		assert_eq!(full, leaf);
	}
}

/// A leaf's codec refuses every catalog kind as unserved.
#[test]
fn the_leaf_codec_refuses_every_catalog_kind() {
	use crate::catalog::stored::CATALOG_TYPES;
	for desc in CATALOG_TYPES {
		let id = TypeId::named(desc.name);
		let error = decode_id(&LEAF_CODEC, id, &[]).unwrap_err();
		assert!(error.to_string().contains("is not served"), "{}: {error}", desc.name);
	}
}

/// The VM codec writes and reads every `Number` variant identically to its
/// `revision` implementation, including multi-byte varint payloads for sized
/// integers and `f16`, and a bigdecimal's scale.
#[test]
fn vm_value_codec_matches_revision_encoding_for_all_number_variants() {
	use crate::val::{Number, Value};

	let numbers = [
		Number::Int(-123_456_789),
		Number::Float(1234.5678),
		Number::Decimal(rust_decimal::Decimal::new(987654321, 5)),
		Number::I8(-42),
		Number::I8(i8::MIN),
		Number::U8(200),
		Number::U8(u8::MAX),
		Number::I16(-300),
		Number::I16(i16::MIN),
		Number::U16(1_000),
		Number::U16(u16::MAX),
		Number::I32(-100_000),
		Number::I32(i32::MIN),
		Number::U32(100_000),
		Number::U32(u32::MAX),
		Number::U64(1_000_000_000_000),
		Number::U64(u64::MAX),
		Number::F16(surrealdb_types::f16::from_f32(1.5)),
		Number::F16(surrealdb_types::f16::from_f32(-1024.0)),
		Number::F32(2.75),
		Number::BigDecimal(Box::new("1.50".parse().unwrap())),
		Number::BigDecimal(Box::new("0".parse().unwrap())),
		Number::BigDecimal(Box::new(
			"-123456789012345678901234567890123456789012345.6789".parse().unwrap(),
		)),
	];
	for n in numbers {
		let val = Value::Number(n.clone());
		let rust = revision::to_vec(&val).unwrap();
		let mut w = wire::Writer::default();
		value::encode(&val, &mut w).unwrap();
		let vm_bytes = w.into_bytes();
		assert_eq!(vm_bytes, rust, "VM encode mismatch for {n:?}");

		let mut r = wire::Reader::new(&rust);
		let decoded = value::decode(&mut r).unwrap();
		assert_eq!(decoded, val, "VM decode mismatch for {n:?}");
		// `==` is numeric; the debug form also holds a bigdecimal's scale.
		assert_eq!(format!("{decoded:?}"), format!("{val:?}"), "VM decode mismatch for {n:?}");
		assert!(r.is_empty(), "trailing bytes after decoding {n:?}");
	}
}

/// The VM codec writes and reads the temporal variants identically to their `revision`
/// implementation, at revision 3, and reads the variants older than them at revision 2.
#[test]
fn vm_value_codec_matches_revision_encoding_for_the_temporal_variants() {
	use crate::val::{Date, Interval, Time, Timestamp, Value};

	let values = [
		Value::Date(Date(0)),
		Value::Date(Date(-719_162)),
		Value::Date(Date(100_000)),
		Value::Time(Time(0)),
		Value::Time(Time(Time::NANOS_PER_DAY)),
		Value::Timestamp(Timestamp {
			secs: -1,
			nanos: 999_999_999,
		}),
		Value::Timestamp(Timestamp {
			secs: 8_000_000_000_000,
			nanos: 7,
		}),
		Value::Interval(Interval::new(-14, 3, i64::MIN)),
		Value::Interval(Interval::new(i32::MAX, i32::MIN, i64::MAX)),
		Value::Array(crate::val::Array(vec![Value::Date(Date(1)), Value::Null])),
		Value::Number(crate::val::Number::Int(5)),
	];
	for val in values {
		let rust = revision::to_vec(&val).unwrap();
		let mut w = wire::Writer::default();
		value::encode(&val, &mut w).unwrap();
		assert_eq!(w.into_bytes(), rust, "VM encode mismatch for {val:?}");
		let mut r = wire::Reader::new(&rust);
		let decoded = value::decode(&mut r).unwrap();
		assert!(crate::val::same_value(&decoded, &val), "VM decode mismatch for {val:?}");
		assert!(r.is_empty(), "trailing bytes after decoding {val:?}");
	}
	let mut as_rev2 = revision::to_vec(&Value::Date(Date(1))).unwrap();
	as_rev2[0] = 2;
	assert!(value::decode(&mut wire::Reader::new(&as_rev2)).is_err());
}
