//! The v1 → v2 edge-layout migration performed by a re-RELATE of a
//! variant-1 edge. The writer must delete both stale vertex-side legacy
//! keys — the `Dir::Out` key on the `in` vertex and the `Dir::In` key on
//! the `out` vertex — before writing the pointer-format keys. A surviving
//! legacy key decodes alongside the pointer key in the same adjacency
//! range, so traversals over that vertex would yield the edge twice, and
//! the variant-2 purge path (which deletes pointer-form keys only) would
//! never reclaim it.

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_types::Value;

use crate::catalog::RecordType;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::dbs::Session;
use crate::expr::Dir;
use crate::key::schema::{GraphDirPrefix, GraphKey, GraphPointerKey};
use crate::kvs::Datastore;
use crate::val::RecordId;

async fn ds() -> Arc<Datastore> {
	Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap()
}

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> Vec<Value> {
	ds.execute(sql, ses, None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.unwrap())
		.collect()
}

#[tokio::test]
async fn v1_edge_migration_deletes_both_legacy_vertex_keys() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");

	// Seed a variant-2 edge through the normal write path.
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person;
		 DEFINE TABLE likes TYPE RELATION;
		 CREATE person:a, person:b;
		 RELATE person:a->likes:e1->person:b;",
	)
	.await;

	// Capture the known-good traversal results while the edge is in the
	// current layout; the migrated edge must reproduce them exactly.
	let baseline_out = run(&ds, &ses, "SELECT VALUE ->likes FROM person:a;").await;
	let baseline_in = run(&ds, &ses, "SELECT VALUE <-likes FROM person:b;").await;

	let a = RecordId::new("person".into(), "a".to_owned());
	let b = RecordId::new("person".into(), "b".to_owned());
	let e1 = RecordId::new("likes".into(), "e1".to_owned());

	// Resolve the namespace / database ids for key construction.
	let (ns, db) = {
		let txn = ds.transaction(Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		txn.cancel().await.unwrap();
		(db.namespace_id, db.database_id)
	};

	// The vertex-side keys of both layouts. The variant-1 writer stored a
	// plain `GraphKey` on each endpoint; variant 2 stores a
	// `GraphPointerKey` with the far vertex embedded.
	let ltr_legacy = GraphKey {
		ns,
		db,
		tb: Cow::Borrowed(&a.table),
		id: Cow::Borrowed(&a.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&e1.table),
		foreign_key: Cow::Borrowed(&e1.key),
	};
	let rtl_legacy = GraphKey {
		ns,
		db,
		tb: Cow::Borrowed(&b.table),
		id: Cow::Borrowed(&b.key),
		dir: Dir::In,
		foreign_table: Cow::Borrowed(&e1.table),
		foreign_key: Cow::Borrowed(&e1.key),
	};
	let ltr_pointer = GraphPointerKey {
		ns,
		db,
		tb: Cow::Borrowed(&a.table),
		id: Cow::Borrowed(&a.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&e1.table),
		foreign_key: Cow::Borrowed(&e1.key),
		target_table: Cow::Borrowed(&b.table),
		target_key: Cow::Borrowed(&b.key),
	};
	let rtl_pointer = GraphPointerKey {
		ns,
		db,
		tb: Cow::Borrowed(&b.table),
		id: Cow::Borrowed(&b.key),
		dir: Dir::In,
		foreign_table: Cow::Borrowed(&e1.table),
		foreign_key: Cow::Borrowed(&e1.key),
		target_table: Cow::Borrowed(&a.table),
		target_key: Cow::Borrowed(&a.key),
	};

	// Downgrade the edge to the variant-1 on-disk layout: swap the two
	// pointer keys for legacy vertex-side keys and stamp the record
	// metadata with the variant the legacy writer used.
	{
		let txn = ds.transaction(Write).await.unwrap();
		txn.del_key(&ltr_pointer).await.unwrap();
		txn.del_key(&rtl_pointer).await.unwrap();
		txn.set_key(&ltr_legacy, &()).await.unwrap();
		txn.set_key(&rtl_legacy, &()).await.unwrap();
		let record = txn.get_record(ns, db, &e1.table, &e1.key, None).await.unwrap();
		let mut record = (*record).clone();
		record.set_record_type(RecordType::Edge {
			variant: 1,
		});
		txn.set_record(ns, db, &e1.table, &e1.key, Arc::new(record)).await.unwrap();
		txn.commit().await.unwrap();
	}

	// Re-RELATE the same edge: the writer sees a variant-1 record and must
	// replace the legacy layout with the pointer layout on both vertices.
	run(&ds, &ses, "RELATE person:a->likes:e1->person:b;").await;

	{
		let txn = ds.transaction(Read).await.unwrap();
		assert!(
			!txn.exists_key(&ltr_legacy, None).await.unwrap(),
			"stale legacy key left on the `in` vertex"
		);
		assert!(
			!txn.exists_key(&rtl_legacy, None).await.unwrap(),
			"stale legacy key left on the `out` vertex"
		);
		assert!(txn.exists_key(&ltr_pointer, None).await.unwrap());
		assert!(txn.exists_key(&rtl_pointer, None).await.unwrap());
		// Each vertex direction must hold exactly the one pointer key: a
		// second key is a stale legacy survivor that traversals would
		// decode as a duplicate edge.
		let a_out = GraphDirPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&a.table),
			id: Cow::Borrowed(&a.key),
			dir: Dir::Out,
		};
		let b_in = GraphDirPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&b.table),
			id: Cow::Borrowed(&b.key),
			dir: Dir::In,
		};
		assert_eq!(txn.count(a_out.range().unwrap(), None).await.unwrap(), 1);
		assert_eq!(txn.count(b_in.range().unwrap(), None).await.unwrap(), 1);
		txn.cancel().await.unwrap();
	}

	// The user-visible oracle: traversals over the migrated edge match the
	// results the variant-2 edge produced before the downgrade.
	assert_eq!(run(&ds, &ses, "SELECT VALUE ->likes FROM person:a;").await, baseline_out);
	assert_eq!(run(&ds, &ses, "SELECT VALUE <-likes FROM person:b;").await, baseline_in);
}
