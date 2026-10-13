//! Degree counting (`count(->edge)`) driven end to end through a
//! `Datastore` with real SurrealQL.
//!
//! The invariant under test: the degree fast path answers exactly as the
//! materializing evaluator does, in every adjacency state a scope can be
//! in — plain per-edge keys, a live inline cache, folded blocks with a
//! clean tail (header sums), a folded scope with live/tombstoned/re-related
//! tail entries (the merge fallback), and sessions whose permission checks
//! disqualify the fast path entirely.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;
use surrealdb_types::Value;

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::expr::Dir;
use crate::idx::adjacency::fold_scope;
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

/// Folds every scope of the given vertex to completion.
async fn fold_vertex(ds: &Datastore, rid: &RecordId) {
	for dir in [Dir::In, Dir::Out] {
		loop {
			let txn = Arc::new(ds.transaction(Write).await.unwrap());
			let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
			let mut env = ds.setup_ctx().unwrap();
			env.set_transaction(Arc::clone(&txn));
			let env = env.freeze();
			let outcome =
				fold_scope(&env, db.namespace_id, db.database_id, "test", "test", rid, dir, 1024)
					.await
					.unwrap();
			txn.commit().await.unwrap();
			if !outcome.has_more {
				break;
			}
		}
	}
}

/// Asserts every degree-shaped query against its literal expectation.
async fn assert_degrees(ds: &Datastore, ses: &Session, cases: &[(&str, i64)]) {
	for (query, expected) in cases {
		let got = run(ds, ses, query).await.remove(0);
		assert_eq!(
			got,
			run(ds, ses, &format!("RETURN {{ degree: {expected} }};")).await.remove(0),
			"query `{query}`"
		);
	}
}

/// One graph, every adjacency state: the same degree questions must answer
/// identically as the storage migrates from plain keys through caches,
/// folds, tails, tombstones and re-relates.
#[tokio::test]
async fn degrees_are_exact_in_every_adjacency_state() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE cached_person SCHEMALESS INLINE EDGES 32;
		 DEFINE TABLE knows TYPE RELATION;
		 DEFINE TABLE likes TYPE RELATION;
		 CREATE |person:1..=12| RETURN NONE;
		 CREATE cached_person:1 RETURN NONE;
		 FOR $n IN 2..=8 { RELATE person:1->knows->(type::record('person', $n)) SET id = type::record('knows', $n) RETURN NONE; };
		 FOR $n IN 9..=11 { RELATE person:1->likes->(type::record('person', $n)) RETURN NONE; };
		 FOR $n IN 2..=6 { RELATE cached_person:1->knows->(type::record('person', $n)) RETURN NONE; };",
	)
	.await;

	let cases: &[(&str, i64)] = &[
		("SELECT count(->knows) AS degree FROM ONLY person:1;", 7),
		("SELECT count(->likes) AS degree FROM ONLY person:1;", 3),
		("SELECT count(->?) AS degree FROM ONLY person:1;", 10),
		("SELECT count(<-knows) AS degree FROM ONLY person:5;", 2),
		("SELECT count(<->knows) AS degree FROM ONLY person:5;", 2),
		("SELECT count(->knows) AS degree FROM ONLY person:12;", 0),
	];

	// Plain per-edge keys.
	assert_degrees(&ds, &ses, cases).await;
	// A live inline cache.
	assert_degrees(&ds, &ses, &[("SELECT count(->knows) AS degree FROM ONLY cached_person:1;", 5)])
		.await;

	// Folded blocks, clean tail: the header-sum path.
	fold_vertex(&ds, &RecordId::new("person".into(), 1)).await;
	assert_degrees(&ds, &ses, cases).await;

	// A live tail on the folded scope: the merge fallback.
	run(&ds, &ses, "RELATE person:1->knows->person:12;").await;
	assert_degrees(&ds, &ses, &[("SELECT count(->knows) AS degree FROM ONLY person:1;", 8)]).await;

	// A tombstoned tail entry subtracts from the block side.
	run(&ds, &ses, "DELETE knows:3;").await;
	assert_degrees(&ds, &ses, &[("SELECT count(->knows) AS degree FROM ONLY person:1;", 7)]).await;

	// A re-related edge coexists as a live tail key and a block entry —
	// the delta-wins dedupe must count it once.
	run(&ds, &ses, "DELETE knows:4; RELATE person:1->knows->person:4 SET id = knows:4;").await;
	assert_degrees(&ds, &ses, &[("SELECT count(->knows) AS degree FROM ONLY person:1;", 7)]).await;

	// Fold the tail away: back on the header-sum path, same answers.
	fold_vertex(&ds, &RecordId::new("person".into(), 1)).await;
	assert_degrees(&ds, &ses, &[("SELECT count(->knows) AS degree FROM ONLY person:1;", 7)]).await;
}

/// Numeric-encoded blocks: the degree fast path must answer exactly as the
/// materializing evaluator, before and after the fold, and after `REMOVE
/// TABLE` wipes the target table's doc-ID mappings — the one state where
/// nothing tombstones a foreign block's entries, so the resolve-and-drop
/// semantics alone decide the answer and both readers must apply them.
#[tokio::test]
async fn numeric_blocks_count_like_the_evaluator() {
	use surrealdb_cnf::ConfigMap;

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(ConfigMap::empty().with_key_value("graph_numeric_ids", "true"))
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE company SCHEMALESS;
		 DEFINE TABLE works_at TYPE RELATION;
		 CREATE person:1 RETURN NONE;
		 CREATE |company:1..=100| RETURN NONE;
		 FOR $n IN 1..=100 { RELATE person:1->works_at->(type::record('company', $n)) RETURN NONE; };",
	)
	.await;

	let cases: &[(&str, i64)] = &[
		("SELECT count(->works_at) AS degree FROM ONLY person:1;", 100),
		("SELECT array::len(->works_at) AS degree FROM ONLY person:1;", 100),
	];
	assert_degrees(&ds, &ses, cases).await;
	fold_vertex(&ds, &RecordId::new("person".into(), 1)).await;
	assert_degrees(&ds, &ses, cases).await;

	// `REMOVE TABLE` wipes company's doc-ID mappings while the foreign
	// blocks under person:1 survive untouched. The invariant is agreement:
	// the fast path and the materializing evaluator must report the same
	// degree for the same data.
	run(&ds, &ses, "REMOVE TABLE company;").await;
	let fast =
		run(&ds, &ses, "SELECT count(->works_at) AS degree FROM ONLY person:1;").await.remove(0);
	let slow = run(&ds, &ses, "SELECT array::len(->works_at) AS degree FROM ONLY person:1;")
		.await
		.remove(0);
	assert_eq!(fast, slow, "the degree fast path disagrees with the evaluator after REMOVE TABLE");
}

/// A session whose permission checks are live never gets the fast path:
/// its count reflects permission filtering, which raw adjacency
/// cardinality cannot — a record user counting over a SELECT-NONE edge
/// table must see zero where the owner sees the raw degree.
#[tokio::test]
async fn permission_checked_sessions_keep_the_evaluator_path() {
	let ds = ds().await;
	let owner = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&owner,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE knows TYPE RELATION PERMISSIONS NONE;
		 CREATE |person:1..=4| RETURN NONE;
		 RELATE person:1->knows->person:2;
		 RELATE person:1->knows->person:3;",
	)
	.await;
	assert_degrees(&ds, &owner, &[("SELECT count(->knows) AS degree FROM ONLY person:1;", 2)])
		.await;

	let user = Session::for_record(
		"test",
		"test",
		"user",
		Value::RecordId(surrealdb_types::RecordId::new(
			"person",
			surrealdb_types::RecordIdKey::Number(1),
		)),
	);
	assert_degrees(&ds, &user, &[("SELECT count(->knows) AS degree FROM ONLY person:1;", 0)]).await;
}
