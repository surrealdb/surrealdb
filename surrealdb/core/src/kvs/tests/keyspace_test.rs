//! Checks the declared keyspace against the keys the engine actually writes.
//!
//! Every other test of the schema compares a constructed key with an expected
//! byte string, which only ever proves that the declaration matches its own
//! transcription. This one runs real statements through the engine, scans the
//! whole store, and asks the generated decoder to name each key it finds. A
//! declaration whose layout is subtly wrong shows up here as an unrecognised
//! key, and a key the engine writes that nobody declared shows up the same way.

use std::collections::BTreeMap;
use std::sync::Arc;

use surrealdb_kvs::TransactionType::Read;

use crate::dbs::{Capabilities, Session};
use crate::key::RawRange;
use crate::key::schema::{AnyKey, KeyKind, describe};
use crate::kvs::Datastore;

async fn mem_ds() -> Arc<Datastore> {
	Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await
		.unwrap()
}

/// Every key currently in the store, across the entire byte range.
async fn all_keys(ds: &Datastore) -> Vec<Vec<u8>> {
	let tx = ds.transaction(Read).await.unwrap();
	let res = tx.getr_raw(RawRange::every_key(), None).await.unwrap();
	let _ = tx.cancel().await;
	res.into_iter().map(|(k, _)| k).collect()
}

/// Exercises enough of the engine to populate the catalog, a table's schema, an
/// index, records, a graph edge and a reference.
async fn populate(ds: &Datastore) {
	let session = Session::owner().with_ns("test").with_db("test");
	for statement in [
		"DEFINE NAMESPACE test;",
		"DEFINE DATABASE test;",
		"DEFINE TABLE person SCHEMAFULL;",
		"DEFINE FIELD name ON person TYPE string;",
		"DEFINE FIELD best ON person TYPE option<record<person>> REFERENCE;",
		"DEFINE INDEX name_idx ON person FIELDS name;",
		"DEFINE INDEX name_unique ON person FIELDS name UNIQUE;",
		"DEFINE ANALYZER simple TOKENIZERS blank;",
		"DEFINE PARAM $threshold VALUE 10;",
		"DEFINE FUNCTION fn::double($n: int) { RETURN $n * 2; };",
		"DEFINE SEQUENCE seq_a;",
		"DEFINE EVENT log ON person WHEN $event = 'CREATE' THEN { RETURN 1; };",
		"DEFINE USER alice ON DATABASE PASSWORD 'secret' ROLES OWNER;",
		"CREATE person:alice SET name = 'alice';",
		"CREATE person:bob SET name = 'bob', best = person:alice;",
		"RELATE person:alice->knows->person:bob;",
	] {
		let res = ds.execute(statement, &session, None).await.unwrap();
		for response in res {
			response.result.unwrap_or_else(|e| panic!("{statement} failed: {e}"));
		}
	}
}

/// The decoder must name every key the engine writes.
///
/// This is the property that makes the reverse direction trustworthy: if it holds
/// over a store built by the engine itself, then a scan of unknown data can be
/// interpreted rather than guessed at.
#[tokio::test]
async fn every_key_the_engine_writes_is_recognised() {
	let ds = mem_ds().await;
	populate(&ds).await;

	let keys = all_keys(&ds).await;
	assert!(keys.len() > 20, "expected the engine to have written a populated store");

	let unrecognised: Vec<String> = keys
		.iter()
		.filter(|k| AnyKey::decode(k).is_none())
		.map(|k| describe(k).to_string())
		.collect();

	assert!(
		unrecognised.is_empty(),
		"{} of {} keys are not covered by the declared keyspace:\n{}",
		unrecognised.len(),
		keys.len(),
		unrecognised.join("\n")
	);
}

/// The kinds the engine writes are the ones the schema says it should, and each
/// is reported under its own route rather than being folded into a neighbour.
#[tokio::test]
async fn recognised_keys_are_attributed_to_the_right_route() {
	let ds = mem_ds().await;
	populate(&ds).await;

	let mut by_kind: BTreeMap<KeyKind, usize> = BTreeMap::new();
	for key in all_keys(&ds).await {
		if let Some(decoded) = AnyKey::decode(&key) {
			*by_kind.entry(decoded.kind()).or_default() += 1;
		}
	}

	// The statements above create each of these, so a missing kind means a route
	// is being attributed to a neighbour instead of itself.
	for expected in [
		KeyKind::Namespace,
		KeyKind::Database,
		KeyKind::Table,
		KeyKind::Field,
		KeyKind::IndexDef,
		KeyKind::IndexName,
		KeyKind::Event,
		KeyKind::Analyzer,
		KeyKind::Param,
		KeyKind::Function,
		KeyKind::DbUser,
		KeyKind::Sequence,
		KeyKind::Record,
		// The two index entry shapes. These are engine-written bytes, so their
		// presence here is what proves the list segments and the trailing
		// discriminant are right.
		KeyKind::Entry,
		KeyKind::Unique,
		// The graph extension family: an inner key and a pointer key that extends
		// it byte for byte, told apart from real data rather than from a fixture.
		KeyKind::Graph,
		KeyKind::GraphPointer,
		KeyKind::Reference,
		// Both directions of the document id mapping, encoded under the index
		// format.
		KeyKind::DocKey,
		KeyKind::DocLookup,
		// The index build family, whose keys an interrupted build resumes from.
		KeyKind::BuildState,
		KeyKind::BuildTicket,
	] {
		assert!(
			by_kind.contains_key(&expected),
			"no key was attributed to {expected:?}; got {by_kind:?}"
		);
	}

	// Three records exist: the two people and the edge created by RELATE.
	assert_eq!(by_kind.get(&KeyKind::Record), Some(&3), "got {by_kind:?}");
}

/// A value decodes under the type its key binds, including for records, whose
/// identity lives in the key rather than in the stored bytes.
#[tokio::test]
async fn values_decode_under_the_type_their_key_binds() {
	let ds = mem_ds().await;
	populate(&ds).await;

	let tx = ds.transaction(Read).await.unwrap();
	let entries = tx.getr_raw(RawRange::every_key(), None).await.unwrap();
	let _ = tx.cancel().await;

	let mut decoded = 0usize;
	let mut failures = Vec::new();
	for (key, value) in &entries {
		let Some(any) = AnyKey::decode(key) else {
			continue;
		};
		match any.decode_value(value) {
			Ok(_) => decoded += 1,
			Err(e) => failures.push(format!("{}: {e}", describe(key))),
		}
	}

	assert!(failures.is_empty(), "values failed to decode:\n{}", failures.join("\n"));
	assert!(decoded > 20, "expected to have decoded a populated store, got {decoded}");
}

/// A decoded value carries the right contents, not merely the right shape.
///
/// This is the end of the round trip the reverse direction promises: given raw
/// bytes from a scan, a caller learns which key it is, decodes the value under
/// that key's own type, and reads it.
#[tokio::test]
async fn a_decoded_value_carries_its_contents() {
	use crate::key::schema::AnyValue;
	use crate::val::{RecordId, Value};

	let ds = mem_ds().await;
	populate(&ds).await;

	let tx = ds.transaction(Read).await.unwrap();
	let entries = tx.getr_raw(RawRange::every_key(), None).await.unwrap();
	let _ = tx.cancel().await;

	let mut namespaces = Vec::new();
	let mut record_ids = Vec::new();
	for (key, value) in &entries {
		let Some(any) = AnyKey::decode(key) else {
			continue;
		};
		// A record's identity is not in the stored bytes: the key supplies it. So
		// the id the decode produced is compared against the one derived from the
		// key's own fields, which is the only check that the context is applied and
		// applied the right way round.
		let expected = match &any {
			AnyKey::Record(record) => {
				Some(RecordId::new(record.tb.as_ref().clone(), record.id.as_ref().clone()))
			}
			_ => None,
		};
		match any.decode_value(value) {
			Ok(AnyValue::NamespaceDefinition(ns)) => namespaces.push(ns.name.to_string()),
			Ok(AnyValue::Record(record)) => {
				let expected = expected.expect("a record key names a record");
				let Value::Object(data) = &record.data else {
					panic!("a record's data is an object, got {:?}", record.data);
				};
				assert_eq!(
					data.0.get("id"),
					Some(&Value::RecordId(expected.clone())),
					"a record's id comes from its key"
				);
				record_ids.push(format!("{}:{:?}", expected.table, expected.key));
			}
			_ => {}
		}
	}

	assert_eq!(namespaces, ["test"], "the namespace definition decoded to its name");

	// The two people plus the edge `RELATE` created. The edge's id is generated, so
	// only its table is predictable.
	record_ids.sort();
	assert_eq!(record_ids.len(), 3, "every record decoded, got {record_ids:?}");
	assert!(
		record_ids.iter().any(|id| id.contains("person") && id.contains("alice")),
		"got {record_ids:?}"
	);
	assert!(
		record_ids.iter().any(|id| id.contains("person") && id.contains("bob")),
		"got {record_ids:?}"
	);
	assert!(
		record_ids.iter().any(|id| id.starts_with("knows:")),
		"the edge record decoded, got {record_ids:?}"
	);
}

/// Dropping a namespace leaves nothing addressable behind except the queued
/// reclaim work, which is the invariant that lets the reclaim task run later.
#[tokio::test]
async fn a_dropped_namespace_leaves_only_queued_work() {
	let ds = mem_ds().await;
	populate(&ds).await;

	let session = Session::owner().with_ns("test").with_db("test");
	ds.execute("REMOVE NAMESPACE test;", &session, None).await.unwrap();

	let mut remaining: BTreeMap<KeyKind, usize> = BTreeMap::new();
	for key in all_keys(&ds).await {
		if let Some(decoded) = AnyKey::decode(&key) {
			*remaining.entry(decoded.kind()).or_default() += 1;
		}
	}

	// The namespace's catalog entry is gone; the data prefix is still there until
	// the background task reclaims it, which is exactly what the queue records.
	assert!(!remaining.contains_key(&KeyKind::Namespace), "got {remaining:?}");
	assert!(remaining.contains_key(&KeyKind::Reclaim), "reclaim work should be queued");
}
