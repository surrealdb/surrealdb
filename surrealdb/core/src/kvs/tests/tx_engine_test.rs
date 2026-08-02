//! Transaction behaviour that only shows up against a running engine.
//!
//! These drive a real `Datastore` through `execute`, so they live here rather
//! than beside the transaction: the datastore is built on top of it.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use crate::catalog::DatabaseDefinition;
use crate::catalog::providers::CatalogProvider;
use crate::dbs::{Capabilities, Session};
use crate::kvs::{Datastore, TransactionType};
use crate::val::TableName;

async fn new_ds() -> Datastore {
	Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_auth(false)
		.build_with_path("memory")
		.await
		.unwrap()
}

async fn ensure_test_db(ds: &Datastore) -> Arc<DatabaseDefinition> {
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	let db = tx.ensure_ns_db(None, "test", "test").await.unwrap();
	tx.commit().await.unwrap();
	db
}

/// The reference-target summary is read off the stored catalog, so it must
/// still see a `REFERENCE` field and must not report a table that only
/// carries non-reference fields.
#[tokio::test]
async fn reference_targets_are_derived_from_the_stored_catalog() {
	let ds = new_ds().await;
	let db = ensure_test_db(&ds).await;
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = ds
		.execute(
			"DEFINE FIELD author ON comment TYPE record<person> REFERENCE;
			 DEFINE FIELD title ON comment TYPE string;",
			&ses,
			None,
		)
		.await
		.unwrap();
	for r in res.drain(..) {
		r.result.unwrap();
	}

	let tx = ds.transaction(TransactionType::Read).await.unwrap();
	let (ns, db) = (db.namespace_id, db.database_id);
	assert!(
		tx.table_may_have_incoming_references(ns, db, &TableName::from("person")).await.unwrap(),
		"`record<person> REFERENCE` can target `person`"
	);
	assert!(
		!tx.table_may_have_incoming_references(ns, db, &TableName::from("comment")).await.unwrap(),
		"no reference field can target `comment`"
	);
	tx.cancel().await.unwrap();
}

/// A `LIVE` registered part-way through a transaction must be delivered to
/// by a write later in that same transaction: bumping `cache_lives_ts`
/// leaves no cached table definition (by id, by name, or in the database's
/// table list) carrying the pre-bump timestamp, and no cached compiled
/// live-query list from before the subscription was written.
#[tokio::test]
async fn live_registered_mid_transaction_is_notified_by_a_later_write() {
	let (send, recv) = crate::channel::bounded(100);
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.with_auth(false)
		.with_notify(send)
		.build_with_path("memory")
		.await
		.unwrap();
	ensure_test_db(&ds).await;
	let ses = Session::owner().with_ns("test").with_db("test").with_rt(true);
	ds.execute("DEFINE TABLE person", &ses, None).await.unwrap().remove(0).result.unwrap();

	let mut res = ds
		.execute(
			"BEGIN; CREATE person:0; LIVE SELECT * FROM person; CREATE person:1; COMMIT;",
			&ses,
			None,
		)
		.await
		.unwrap();
	for r in res.drain(..) {
		r.result.unwrap();
	}

	let notification = tokio::time::timeout(std::time::Duration::from_secs(5), recv.recv()).await;
	let notification = notification.expect("the mid-transaction LIVE was not notified").unwrap();
	assert_eq!(
		notification.record,
		crate::types::PublicValue::RecordId(crate::types::PublicRecordId {
			table: "person".to_string().into(),
			key: crate::types::PublicRecordIdKey::Number(1),
		})
	);
}
