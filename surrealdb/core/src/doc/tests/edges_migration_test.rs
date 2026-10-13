//! Verifies the lazy variant-1 → variant-2 adjacency-key migration performed
//! by `Document::store_edges_data` when a stale-variant edge is re-related.
//!
//! A variant-1 edge stores its vertex-side adjacency keys as bare
//! [`GraphKey`]s: `(in-vertex, Out, edge)` and `(out-vertex, In, edge)`.
//! Re-relating such an edge must delete both bare keys and replace them with
//! [`GraphPointerKey`]s under the same prefixes. The pointer encoding
//! byte-extends the bare encoding, so a bare key left behind shares the scan
//! range with its replacement: every traversal over that vertex would then
//! yield the edge twice, and once the record's stamp advances to variant 2
//! the purge path deletes only the pointer form, orphaning the bare key
//! permanently. The variant-1 layout cannot be produced through SurrealQL, so
//! these tests seed it with raw key writes, mirroring the layout the
//! variant-1 arm of `Document::purge_pointers` deletes.

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};

use crate::catalog::providers::CatalogProvider;
use crate::catalog::record::{Record, RecordType};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::dbs::{Capabilities, Session};
use crate::expr::Dir;
use crate::key::schema::{AnyKey, GraphDirPrefix, GraphKey, KeyKind, RecordKey};
use crate::kvs::Datastore;
use crate::val::{Array, Object, RecordId, RecordIdKey, Strand, TableName, Value};

async fn mem_ds() -> Arc<Datastore> {
	Datastore::builder()
		.without_maintenance_tasks()
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await
		.unwrap()
}

fn rid(table: &str, key: &str) -> RecordId {
	RecordId {
		table: TableName::from(table),
		key: RecordIdKey::String(Strand::new(key)),
	}
}

/// An array value of the given record ids, as a `SELECT VALUE` result.
fn rid_array(rids: &[&RecordId]) -> Value {
	Value::Array(Array::from(
		rids.iter().map(|r| Value::RecordId((*r).clone())).collect::<Vec<_>>(),
	))
}

/// Runs each statement, panicking on the first error, and returns the result
/// of the final statement.
async fn run(ds: &Datastore, session: &Session, sql: &str) -> Value {
	let responses = ds.execute(sql, session, None).await.unwrap();
	let mut last = Value::None;
	for response in responses {
		last = response.result.unwrap_or_else(|e| panic!("{sql} failed: {e}")).into();
	}
	last
}

/// The kinds of adjacency keys stored under one (vertex, direction) prefix,
/// in key order.
async fn adjacency_kinds(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	vertex: &RecordId,
	dir: Dir,
) -> Vec<KeyKind> {
	let range = GraphDirPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir,
	}
	.range()
	.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let keys = tx.keys_raw(range, u32::MAX, 0, None).await.unwrap();
	let _ = tx.cancel().await;
	keys.iter()
		.map(|k| AnyKey::decode(k).unwrap_or_else(|| panic!("unrecognised key {k:?}")).kind())
		.collect()
}

/// Writes an edge exactly as the variant-1 layout stored it: bare
/// [`GraphKey`]s on both vertices and on the edge record, plus the record
/// itself stamped `RecordType::Edge { variant: 1 }` with its `in` / `out`
/// endpoints in the data.
async fn seed_v1_edge(
	ds: &Datastore,
	ns: NamespaceId,
	db: DatabaseId,
	edge: &RecordId,
	l: &RecordId,
	r: &RecordId,
) {
	let ltr = GraphKey {
		ns,
		db,
		tb: Cow::Borrowed(&l.table),
		id: Cow::Borrowed(&l.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&edge.table),
		foreign_key: Cow::Borrowed(&edge.key),
	};
	let etl = GraphKey {
		ns,
		db,
		tb: Cow::Borrowed(&edge.table),
		id: Cow::Borrowed(&edge.key),
		dir: Dir::In,
		foreign_table: Cow::Borrowed(&l.table),
		foreign_key: Cow::Borrowed(&l.key),
	};
	let etr = GraphKey {
		ns,
		db,
		tb: Cow::Borrowed(&edge.table),
		id: Cow::Borrowed(&edge.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&r.table),
		foreign_key: Cow::Borrowed(&r.key),
	};
	let rtl = GraphKey {
		ns,
		db,
		tb: Cow::Borrowed(&r.table),
		id: Cow::Borrowed(&r.key),
		dir: Dir::In,
		foreign_table: Cow::Borrowed(&edge.table),
		foreign_key: Cow::Borrowed(&edge.key),
	};
	let mut data = Object::default();
	data.insert("id".to_string(), Value::RecordId(edge.clone()));
	data.insert("in".to_string(), Value::RecordId(l.clone()));
	data.insert("out".to_string(), Value::RecordId(r.clone()));
	let mut record = Record::new(Value::Object(data));
	record.set_record_type(RecordType::Edge {
		variant: 1,
	});

	let tx = ds.transaction(Write).await.unwrap();
	tx.set_key(&ltr, &()).await.unwrap();
	tx.set_key(&etl, &()).await.unwrap();
	tx.set_key(&etr, &()).await.unwrap();
	tx.set_key(&rtl, &()).await.unwrap();
	tx.set_key(
		&RecordKey {
			ns,
			db,
			tb: Cow::Borrowed(&edge.table),
			id: Cow::Borrowed(&edge.key),
		},
		&record,
	)
	.await
	.unwrap();
	tx.commit().await.unwrap();
}

#[tokio::test]
async fn re_relating_a_v1_edge_replaces_both_legacy_vertex_keys() {
	let ds = mem_ds().await;
	let db_def = {
		let tx = ds.transaction(Write).await.unwrap();
		let db_def = tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
		db_def
	};
	let ns = db_def.namespace_id;
	let db = db_def.database_id;
	let session = Session::owner().with_ns("test").with_db("test");

	run(
		&ds,
		&session,
		"DEFINE TABLE person;
		 DEFINE TABLE knows TYPE RELATION IN person OUT person;
		 CREATE person:alice;
		 CREATE person:bob;",
	)
	.await;

	let alice = rid("person", "alice");
	let bob = rid("person", "bob");
	let edge = rid("knows", "k1");
	seed_v1_edge(&ds, ns, db, &edge, &alice, &bob).await;

	// The seeded store must behave as a faithful variant-1 database before
	// any migration runs.
	assert_eq!(
		run(&ds, &session, "SELECT VALUE ->knows->person FROM ONLY person:alice;").await,
		rid_array(&[&bob]),
	);
	assert_eq!(
		run(&ds, &session, "SELECT VALUE <-knows<-person FROM ONLY person:bob;").await,
		rid_array(&[&alice]),
	);

	// Re-relating the edge triggers the lazy migration to the pointer layout.
	run(&ds, &session, "RELATE OR UPDATE person:alice->knows:k1->person:bob SET since = 2020;")
		.await;

	// Both legacy vertex-side keys must be replaced by exactly one pointer
	// key each; neither vertex may hold a key in the unused direction.
	assert_eq!(adjacency_kinds(&ds, ns, db, &alice, Dir::Out).await, vec![KeyKind::GraphPointer]);
	assert_eq!(
		adjacency_kinds(&ds, ns, db, &bob, Dir::In).await,
		vec![KeyKind::GraphPointer],
		"the legacy (out-vertex, In, edge) key must not survive the migration",
	);
	assert_eq!(adjacency_kinds(&ds, ns, db, &alice, Dir::In).await, vec![]);
	assert_eq!(adjacency_kinds(&ds, ns, db, &bob, Dir::Out).await, vec![]);
	// The edge-side keys keep the bare layout across both variants.
	assert_eq!(adjacency_kinds(&ds, ns, db, &edge, Dir::In).await, vec![KeyKind::Graph]);
	assert_eq!(adjacency_kinds(&ds, ns, db, &edge, Dir::Out).await, vec![KeyKind::Graph]);

	// Traversals resolve the edge exactly once from either endpoint.
	assert_eq!(
		run(&ds, &session, "SELECT VALUE ->knows FROM ONLY person:alice;").await,
		rid_array(&[&edge]),
	);
	assert_eq!(
		run(&ds, &session, "SELECT VALUE <-knows FROM ONLY person:bob;").await,
		rid_array(&[&edge]),
		"a surviving legacy key shares the pointer key's scan range and duplicates the edge",
	);
	assert_eq!(
		run(&ds, &session, "SELECT VALUE ->knows->person FROM ONLY person:alice;").await,
		rid_array(&[&bob]),
	);
	assert_eq!(
		run(&ds, &session, "SELECT VALUE <-knows<-person FROM ONLY person:bob;").await,
		rid_array(&[&alice]),
	);

	// Deleting the migrated edge must leave no adjacency key behind: the
	// purge dispatches on the variant-2 stamp and deletes only the pointer
	// layout, so any bare key still on disk would be orphaned forever.
	run(&ds, &session, "DELETE knows:k1;").await;
	for dir in [Dir::In, Dir::Out] {
		assert_eq!(adjacency_kinds(&ds, ns, db, &alice, dir).await, vec![]);
		assert_eq!(adjacency_kinds(&ds, ns, db, &bob, dir).await, vec![]);
		assert_eq!(adjacency_kinds(&ds, ns, db, &edge, dir).await, vec![]);
	}
	assert_eq!(
		run(&ds, &session, "SELECT VALUE ->knows FROM ONLY person:alice;").await,
		rid_array(&[]),
	);
	assert_eq!(
		run(&ds, &session, "SELECT VALUE <-knows FROM ONLY person:bob;").await,
		rid_array(&[]),
	);
}

#[tokio::test]
async fn re_relating_a_v1_self_loop_edge_migrates_cleanly() {
	let ds = mem_ds().await;
	let db_def = {
		let tx = ds.transaction(Write).await.unwrap();
		let db_def = tx.ensure_ns_db(None, "test", "test").await.unwrap();
		tx.commit().await.unwrap();
		db_def
	};
	let ns = db_def.namespace_id;
	let db = db_def.database_id;
	let session = Session::owner().with_ns("test").with_db("test");

	run(
		&ds,
		&session,
		"DEFINE TABLE person;
		 DEFINE TABLE likes TYPE RELATION IN person OUT person;
		 CREATE person:alice;",
	)
	.await;

	let alice = rid("person", "alice");
	let edge = rid("likes", "k1");
	seed_v1_edge(&ds, ns, db, &edge, &alice, &alice).await;

	assert_eq!(
		run(&ds, &session, "SELECT VALUE ->likes->person FROM ONLY person:alice;").await,
		rid_array(&[&alice]),
	);

	run(&ds, &session, "RELATE OR UPDATE person:alice->likes:k1->person:alice SET since = 2020;")
		.await;

	// A self-loop holds one adjacency key per direction on its only vertex;
	// both must have been upgraded to the pointer form.
	assert_eq!(adjacency_kinds(&ds, ns, db, &alice, Dir::Out).await, vec![KeyKind::GraphPointer]);
	assert_eq!(adjacency_kinds(&ds, ns, db, &alice, Dir::In).await, vec![KeyKind::GraphPointer]);
	assert_eq!(
		run(&ds, &session, "SELECT VALUE <-likes FROM ONLY person:alice;").await,
		rid_array(&[&edge]),
	);

	run(&ds, &session, "DELETE likes:k1;").await;
	for dir in [Dir::In, Dir::Out] {
		assert_eq!(adjacency_kinds(&ds, ns, db, &alice, dir).await, vec![]);
		assert_eq!(adjacency_kinds(&ds, ns, db, &edge, dir).await, vec![]);
	}
}
