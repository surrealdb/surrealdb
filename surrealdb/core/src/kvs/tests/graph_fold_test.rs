//! Graph traversal semantics across adjacency folds, driven end to end
//! through a `Datastore` with real SurrealQL.
//!
//! The invariant under test is the programme's correctness oracle: query
//! results must be indistinguishable before and after any sequence of
//! folds, with deletes and re-relates interleaved at fold boundaries. The
//! fold itself is invoked directly — production triggers only change
//! *when* it runs, never what it does.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_datastore::values::graph::AdjacencyBlock;
use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_types::Value;

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::expr::Dir;
use crate::idx::adjacency::fold_scope;
use crate::kvs::{Datastore, QueryRequest};
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

/// Folds every scope of the given vertex to completion, through the same
/// context-as-environment shape the background task uses.
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

fn person(n: usize) -> RecordId {
	RecordId::new("person".into(), format!("p{n}"))
}

/// The query battery whose results must be fold-invariant: id-only
/// traversal, the target-vertex fast path, reverse and bidirectional
/// traversal, full edge projection, a pushed-down edge predicate, and a
/// multi-hop chain.
const BATTERY: &[&str] = &[
	"SELECT VALUE ->likes FROM person:p0;",
	"SELECT VALUE ->likes->person FROM person:p0;",
	"SELECT VALUE <-likes FROM person:p2;",
	"SELECT VALUE <->likes FROM person:p1;",
	"SELECT VALUE ->likes.* FROM person:p0;",
	"SELECT VALUE ->(likes WHERE out = person:p2) FROM person:p0;",
	"SELECT VALUE ->likes->person->likes->person FROM person:p0;",
	"SELECT VALUE ->follows->person FROM person:p0;",
	"SELECT VALUE ->likes FROM person ORDER BY id;",
];

async fn battery(ds: &Datastore, ses: &Session) -> Vec<Vec<Value>> {
	let mut out = Vec::new();
	for query in BATTERY {
		out.push(run(ds, ses, query).await);
	}
	out
}

async fn seed(ds: &Datastore, ses: &Session) {
	run(
		ds,
		ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person;
		 DEFINE TABLE likes TYPE RELATION;
		 DEFINE TABLE follows TYPE RELATION;
		 CREATE person:p0, person:p1, person:p2, person:p3, person:p4;
		 RELATE person:p0->likes:l01->person:p1;
		 RELATE person:p0->likes:l02->person:p2;
		 RELATE person:p0->likes:l03->person:p3;
		 RELATE person:p1->likes:l12->person:p2;
		 RELATE person:p2->likes:l23->person:p3;
		 RELATE person:p3->likes:l34->person:p4;
		 RELATE person:p0->follows:f04->person:p4;
		 RELATE person:p4->likes:l40->person:p0;",
	)
	.await;
}

#[tokio::test]
async fn traversal_semantics_survive_folding() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	let before = battery(&ds, &ses).await;

	// Fold some vertices fully, one partially interleaved with the rest
	// unfolded, so the battery reads mixed block/delta/unfolded state.
	fold_vertex(&ds, &person(0)).await;
	fold_vertex(&ds, &person(2)).await;
	assert_eq!(battery(&ds, &ses).await, before, "mixed folded/unfolded state diverged");

	for n in [1, 3, 4] {
		fold_vertex(&ds, &person(n)).await;
	}
	assert_eq!(battery(&ds, &ses).await, before, "fully folded state diverged");
}

#[tokio::test]
async fn deletes_and_rerelates_are_correct_across_folds() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;
	for n in 0..5 {
		fold_vertex(&ds, &person(n)).await;
	}

	// Deleting a folded edge writes tombstones; the traversal must stop
	// seeing it immediately, before any fold consumes them.
	run(&ds, &ses, "DELETE likes:l02;").await;
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await,
		run(&ds, &ses, "RETURN [[person:p1, person:p3]];").await
	);
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE <-likes FROM person:p2;").await,
		run(&ds, &ses, "RETURN [[likes:l12]];").await
	);

	// Re-relating the same edge id over its folded-then-tombstoned state
	// must yield the edge exactly once.
	run(&ds, &ses, "RELATE person:p0->likes:l02->person:p2;").await;
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await,
		run(&ds, &ses, "RETURN [[person:p1, person:p2, person:p3]];").await
	);

	// Folding the mixture (tombstone consumed earlier, fresh delta) keeps
	// the same answer.
	fold_vertex(&ds, &person(0)).await;
	fold_vertex(&ds, &person(2)).await;
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await,
		run(&ds, &ses, "RETURN [[person:p1, person:p2, person:p3]];").await
	);
}

#[tokio::test]
async fn vertex_delete_cascades_over_folded_edges() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;
	let before_edges = run(&ds, &ses, "SELECT VALUE id FROM likes ORDER BY id;").await;
	for n in 0..5 {
		fold_vertex(&ds, &person(n)).await;
	}
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE id FROM likes ORDER BY id;").await,
		before_edges,
		"folding must not change the edge table"
	);

	// Deleting a vertex must cascade through its folded adjacency: the
	// synthesized `DELETE <rid><->` runs through the legacy collector,
	// which merges blocks — a raw key scan would find nothing to delete.
	run(&ds, &ses, "DELETE person:p0;").await;

	// Every edge incident to p0 is gone, from every endpoint's view.
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE <-likes FROM person:p1;").await,
		run(&ds, &ses, "RETURN [[]];").await
	);
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE <-likes FROM person:p2;").await,
		run(&ds, &ses, "RETURN [[likes:l12]];").await
	);
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->likes FROM person:p4;").await,
		run(&ds, &ses, "RETURN [[]];").await
	);
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE id FROM likes ORDER BY id;").await,
		run(&ds, &ses, "RETURN [likes:l12, likes:l23, likes:l34];").await
	);
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE id FROM follows;").await,
		run(&ds, &ses, "RETURN [];").await
	);

	// The surviving graph still folds and reads consistently.
	for n in 1..5 {
		fold_vertex(&ds, &person(n)).await;
	}
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p1;").await,
		run(&ds, &ses, "RETURN [[person:p2]];").await
	);
}

/// The production trigger pipeline, driven end to end on a stock datastore:
/// a traversal over a scope carrying at least the built-in
/// `graph_fold_threshold` unfolded keys observes it, an edge deletion on a
/// folded table queues its scopes durably at commit, and a
/// [`Datastore::graph_fold`] pass — the same routine the background task
/// runs — flushes the observations, drains the queue, and folds. Only the
/// *scheduling* differs from production: the pass is invoked directly
/// rather than by ticker or commit wake-up.
#[tokio::test]
async fn the_trigger_pipeline_folds_hot_scopes_and_tombstones() {
	use tokio_util::sync::CancellationToken;
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	// A hub vertex with exactly threshold-many outgoing likes edges:
	// traversing them meets the built-in threshold and observes the scope.
	let threshold = crate::idx::config::IdxConfig::default().graph_fold_threshold;
	let mut sql = String::from("CREATE person:hub;");
	for i in 0..threshold {
		sql.push_str(&format!("CREATE person:t{i}; RELATE person:hub->likes:h{i}->person:t{i};"));
	}
	run(&ds, &ses, &sql).await;
	let hub = RecordId::new("person".into(), "hub".to_string());

	let before = run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:hub;").await;
	assert_eq!(chunk_count(&ds, &hub).await, 0);

	// The fold pass flushes the observation into the queue and drains it.
	let (iterations, errors) = Datastore::graph_fold(
		Arc::clone(&ds),
		std::time::Duration::from_secs(5),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert!(iterations > 0, "the queued observation must be drained");
	assert_eq!(errors, 0);
	assert!(chunk_count(&ds, &hub).await > 0, "the hot scope must have folded");
	assert_eq!(run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:hub;").await, before);

	// Deleting a folded edge tombstones its pointer keys and queues the
	// scopes durably at commit; the next pass consumes the tombstones.
	run(&ds, &ses, "DELETE likes:h0;").await;
	assert_eq!(delta_count(&ds, &hub).await, 1, "the tombstone rides the delta layer");
	Datastore::graph_fold(
		Arc::clone(&ds),
		std::time::Duration::from_secs(5),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(delta_count(&ds, &hub).await, 0, "the queued fold must consume it");
	assert_eq!(
		run(&ds, &ses, "RETURN array::len(SELECT VALUE ->likes->person FROM ONLY person:hub);")
			.await,
		run(&ds, &ses, &format!("RETURN {};", threshold - 1)).await
	);
}

/// A zero fold threshold disables fold initiation — no new read-path
/// observations — but never debt repayment: an edge delete on a table that
/// already folded writes tombstones and queues its scopes durably whatever
/// the threshold, and the fold pass must drain that debt or the queue and
/// the per-read tombstone subtraction would grow without bound.
#[tokio::test]
async fn a_zero_threshold_still_repays_fold_debt() {
	use tokio_util::sync::CancellationToken;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_graph_fold_threshold(0)
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	// Fold one vertex directly; the fold engine itself never consults the
	// threshold, only observation does.
	fold_vertex(&ds, &person(0)).await;
	assert!(chunk_count(&ds, &person(0)).await > 0);

	// Deleting a folded edge tombstones its pointer keys and queues the
	// scopes durably at commit — fold debt already incurred.
	run(&ds, &ses, "DELETE likes:l01;").await;
	assert_eq!(delta_count(&ds, &person(0)).await, 1, "the tombstone rides the delta layer");
	assert!(fold_queue_len(&ds).await > 0, "the delete must queue its scopes");

	// The pass must repay the debt despite the zero threshold.
	let (iterations, errors) = Datastore::graph_fold(
		Arc::clone(&ds),
		std::time::Duration::from_secs(5),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert!(iterations > 0, "the queued debt must be drained");
	assert_eq!(errors, 0);
	assert_eq!(fold_queue_len(&ds).await, 0, "the queue must drain");
	assert_eq!(delta_count(&ds, &person(0)).await, 0, "the tombstone must be consumed");
}

/// A scope whose fold fails keeps its queue entries — deleting them would
/// drop the fold debt while its tombstones keep taxing every read — and
/// the pass still terminates instead of hot-looping on the poison scope.
#[tokio::test]
async fn a_failed_fold_keeps_its_queue_entries() {
	use std::borrow::Cow;

	use surrealdb_datastore::values::graph::AdjacencyValue;
	use tokio_util::sync::CancellationToken;
	use uuid::Uuid;

	use crate::key::schema::{GraphFoldKey, GraphPointerKey};

	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;
	let (ns, db) = {
		let txn = ds.transaction(Read).await.unwrap();
		let dbdef = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		let ids = (dbdef.namespace_id, dbdef.database_id);
		txn.cancel().await.unwrap();
		ids
	};

	// A raw pointer key under a table that was never defined, and a queue
	// entry naming its scope: the fold captures the key, then fails on the
	// catalog read for the missing vertex table.
	let ghost = RecordId::new("ghost".into(), "g0".to_string());
	let edge = RecordId::new("likes".into(), "gx".to_string());
	let target = RecordId::new("person".into(), "p1".to_string());
	{
		let txn = ds.transaction(Write).await.unwrap();
		let pointer = GraphPointerKey {
			ns,
			db,
			tb: Cow::Borrowed(&ghost.table),
			id: Cow::Borrowed(&ghost.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&edge.table),
			foreign_key: Cow::Borrowed(&edge.key),
			target_table: Cow::Borrowed(&target.table),
			target_key: Cow::Borrowed(&target.key),
		};
		txn.set_key(&pointer, &AdjacencyValue::live()).await.unwrap();
		let queued = GraphFoldKey {
			ns,
			db,
			tb: Cow::Borrowed(&ghost.table),
			id: Cow::Borrowed(&ghost.key),
			dir: Dir::Out,
			nid: Uuid::new_v4(),
			uid: Uuid::now_v7(),
		};
		txn.put_key(&queued, &()).await.unwrap();
		txn.commit().await.unwrap();
	}
	assert_eq!(fold_queue_len(&ds).await, 1);

	// The pass must count the failure, keep the entry, and return — the
	// timeout guards against a retained failure re-scanning forever.
	let (_, errors) = tokio::time::timeout(
		std::time::Duration::from_secs(60),
		Datastore::graph_fold(
			Arc::clone(&ds),
			std::time::Duration::from_secs(5),
			CancellationToken::new(),
		),
	)
	.await
	.expect("a retained failure must not loop the pass")
	.unwrap();
	assert_eq!(errors, 1, "the failed fold must be counted");
	assert_eq!(fold_queue_len(&ds).await, 1, "the failed scope's entry must stay queued");
}

async fn fold_queue_len(ds: &Datastore) -> usize {
	use crate::key::schema::GraphFoldPrefix;
	let txn = ds.transaction(Read).await.unwrap();
	let count = txn.count(GraphFoldPrefix {}.range().unwrap(), None).await.unwrap();
	txn.cancel().await.unwrap();
	count
}

async fn delta_count(ds: &Datastore, rid: &RecordId) -> usize {
	use std::borrow::Cow;

	use crate::key::schema::GraphDirPrefix;
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let range = GraphDirPrefix {
		ns: db.namespace_id,
		db: db.database_id,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
		dir: Dir::Out,
	}
	.range()
	.unwrap();
	let count = txn.count(range, None).await.unwrap();
	txn.cancel().await.unwrap();
	count
}

async fn chunk_count(ds: &Datastore, rid: &RecordId) -> usize {
	use std::borrow::Cow;

	use crate::key::schema::AdjacencyDirPrefix;
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let range = AdjacencyDirPrefix {
		ns: db.namespace_id,
		db: db.database_id,
		tb: Cow::Borrowed(&rid.table),
		id: Cow::Borrowed(&rid.key),
		dir: Dir::Out,
	}
	.range()
	.unwrap();
	let count = txn.count(range, None).await.unwrap();
	txn.cancel().await.unwrap();
	count
}

/// Whether `table`'s `graph_doc_ids` marker is set.
async fn graph_doc_ids(ds: &Datastore, table: &str) -> bool {
	use crate::catalog::providers::TableProvider;
	use crate::val::TableName;

	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let tb = txn
		.get_tb(db.namespace_id, db.database_id, &TableName::from(table), None)
		.await
		.unwrap()
		.unwrap();
	txn.cancel().await.unwrap();
	tb.graph_doc_ids
}

/// Numeric compression end to end: the battery is encoding-invariant,
/// and the doc-ID lifecycle holds — removing the table's last doc-ID
/// index must not reclaim the space while blocks resolve through it, a
/// record deletion removes its mapping even with no index left, and a
/// re-created record gets a fresh id rather than resurrecting entries
/// that named its predecessor.
#[tokio::test]
async fn numeric_encoding_preserves_semantics_and_the_id_lifecycle() {
	use surrealdb_cnf::ConfigMap;

	use crate::idx::docids::TableDocIds;
	use crate::val::TableName;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(ConfigMap::empty().with_key_value("graph_numeric_ids", "true"))
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	let before = battery(&ds, &ses).await;
	for n in 0..5 {
		fold_vertex(&ds, &person(n)).await;
	}
	assert_eq!(battery(&ds, &ses).await, before, "numeric folds changed query results");

	// The reclaim gate: an index consumes the same shared space; removing
	// the last index must leave the space alone while the graph marker is
	// set, or every folded numeric entry would stop resolving.
	run(&ds, &ses, "DEFINE INDEX name_idx ON person FIELDS name;").await;
	run(&ds, &ses, "REMOVE INDEX name_idx ON person;").await;
	assert_eq!(
		battery(&ds, &ses).await,
		before,
		"removing the last index must not reclaim the doc-ID space under folded blocks"
	);

	// The removal gate: with no index left, deleting a record must still
	// remove its mapping — the graph marker keeps the central removal
	// firing — so a re-created record gets a fresh id and no folded entry
	// resurrects.
	let (ns, db) = {
		let txn = ds.transaction(Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		txn.cancel().await.unwrap();
		(db.namespace_id, db.database_id)
	};
	let doc_ids = TableDocIds::new(ns, db, TableName::from("person"));
	let old_id = {
		let txn = ds.transaction(Read).await.unwrap();
		let id = doc_ids.get_doc_id(&txn, &person(4).key).await.unwrap();
		txn.cancel().await.unwrap();
		id.expect("a folded vertex has a doc-ID")
	};
	run(&ds, &ses, "DELETE person:p4;").await;
	{
		let txn = ds.transaction(Read).await.unwrap();
		assert_eq!(
			doc_ids.get_doc_id(&txn, &person(4).key).await.unwrap(),
			None,
			"record deletion must remove the mapping even with no index left"
		);
		txn.cancel().await.unwrap();
	}
	// Re-create the record and make it a fold participant again: block
	// entries compress edge and target ids (the source vertex lives in the
	// key), so it needs an incoming edge for a fresh id to be assigned.
	run(&ds, &ses, "CREATE person:p4; RELATE person:p0->likes:l98->person:p4;").await;
	fold_vertex(&ds, &person(0)).await;
	let new_id = {
		let txn = ds.transaction(Read).await.unwrap();
		let id = doc_ids.get_doc_id(&txn, &person(4).key).await.unwrap();
		txn.cancel().await.unwrap();
		id.expect("the re-created target was folded again")
	};
	assert_ne!(old_id, new_id, "a re-created record must get a fresh doc-ID");
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await,
		run(&ds, &ses, "RETURN [[person:p1, person:p2, person:p3, person:p4]];").await,
		"the folded numeric entries must resolve through the fresh mapping"
	);
}

/// How many queued reclaim entries name a shared doc-ID prefix.
async fn queued_doc_reclaims(ds: &Datastore) -> usize {
	use crate::key::KVKeyDecode;
	use crate::key::schema::{ReclaimKey, ReclaimPrefix};

	let txn = ds.transaction(Read).await.unwrap();
	let keys = txn.keysr_raw(ReclaimPrefix {}.range().unwrap(), u32::MAX, 0, None).await.unwrap();
	txn.cancel().await.unwrap();
	keys.iter()
		.filter(|key| {
			matches!(
				ReclaimKey::decode_key(key).unwrap().kind,
				kind if kind.is_doc_id()
			)
		})
		.count()
}

/// Every adjacency block of the given vertex, both directions.
async fn chunks_of(ds: &Datastore, rid: &RecordId) -> Vec<AdjacencyBlock> {
	use std::borrow::Cow;

	use crate::key::schema::AdjacencyDirPrefix;

	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let mut out = Vec::new();
	for dir in [Dir::In, Dir::Out] {
		let range = AdjacencyDirPrefix {
			ns: db.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
			dir,
		}
		.range()
		.unwrap();
		out.extend(txn.getr(range, None).await.unwrap().into_iter().map(|(_, block)| block));
	}
	txn.cancel().await.unwrap();
	out
}

/// Recording the `graph_doc_ids` marker makes the fold a consumer of the
/// shared doc-ID space, so it must go through the same reclaim-claim
/// protocol as `DEFINE INDEX`: a reclaim queued by removing the table's
/// last doc-ID index while it was unmarked is cancelled by the fold's
/// claim, and the reaper — drained here with no grace — must find nothing
/// to wipe under the folded blocks.
#[tokio::test]
async fn numeric_fold_claims_a_queued_doc_id_reclaim() {
	use surrealdb_cnf::ConfigMap;
	use tokio_util::sync::CancellationToken;

	use crate::key::reclaim::ALL_DOC_ID_KINDS;

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(ConfigMap::empty().with_key_value("graph_numeric_ids", "true"))
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;
	let before = battery(&ds, &ses).await;

	// A doc-ID index makes `person` a consumer; removing it while the
	// table is unmarked queues the shared space for background reclaim.
	run(&ds, &ses, "DEFINE INDEX name_idx ON person FIELDS name;").await;
	run(&ds, &ses, "REMOVE INDEX name_idx ON person;").await;
	assert_eq!(
		queued_doc_reclaims(&ds).await,
		ALL_DOC_ID_KINDS.len(),
		"removing the last doc-ID index must queue every shared prefix"
	);

	// The fold's markers make `person` (and `likes`) consumers again, so
	// the marker transaction must cancel the queued reclaim.
	for n in 0..5 {
		fold_vertex(&ds, &person(n)).await;
	}
	assert!(graph_doc_ids(&ds, "person").await);
	assert_eq!(
		queued_doc_reclaims(&ds).await,
		0,
		"the fold's claim must cancel the queued doc-ID reclaim"
	);

	// Drain what remains of the queue (the removed index's own data entry)
	// with no grace: nothing may touch the mappings the blocks resolve
	// through.
	for _ in 0..64 {
		let (batches, errors) = Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			std::time::Duration::from_secs(1),
			std::time::Duration::ZERO,
			CancellationToken::new(),
		)
		.await
		.unwrap();
		assert_eq!(errors, 0);
		if batches == 0 {
			break;
		}
	}
	assert_eq!(
		battery(&ds, &ses).await,
		before,
		"every folded edge must still resolve after the reclaim pass"
	);
}

/// A doc-ID space already being drained cannot be claimed, so a fold that
/// would need to mark such a table refuses numeric compression — for the
/// whole chunk — rather than bake in ids that resolve through a torn
/// space. The rewritten chunks stay plain and no table gains the marker.
#[tokio::test]
async fn a_fold_refuses_numeric_over_a_started_doc_id_reclaim() {
	use std::borrow::Cow;

	use surrealdb_cnf::ConfigMap;
	use surrealdb_datastore::values::graph::BlockEntries;
	use uuid::Uuid;

	use crate::catalog::IndexId;
	use crate::key::reclaim::{ALL_DOC_ID_KINDS, Expunge, ReclaimState};
	use crate::key::schema::ReclaimKey;
	use crate::val::TableName;

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(ConfigMap::empty().with_key_value("graph_numeric_ids", "true"))
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;
	let before = battery(&ds, &ses).await;
	let (ns, db) = {
		let txn = ds.transaction(Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		txn.cancel().await.unwrap();
		(db.namespace_id, db.database_id)
	};

	// The state a reclaim leaves mid-drain: `person`'s three queued
	// prefixes, each carrying a committed page cursor.
	let tb = TableName::from("person");
	let txn = ds.transaction(Write).await.unwrap();
	for kind in ALL_DOC_ID_KINDS {
		let rc = ReclaimKey {
			kind,
			ns,
			db,
			tb: Cow::Owned(tb.clone()),
			ix: IndexId(0),
			expunge: Expunge::Keep,
			uid: Uuid::now_v7(),
		};
		let torn = ReclaimState {
			observed_ms: 1,
			cursor: Some(vec![0]),
		};
		txn.set_key(&rc, &torn).await.unwrap();
	}
	txn.commit().await.unwrap();

	for n in 0..5 {
		fold_vertex(&ds, &person(n)).await;
	}

	// Every chunk names `person` as a target, so every chunk fell back and
	// no involved table was marked.
	assert!(!graph_doc_ids(&ds, "person").await);
	assert!(!graph_doc_ids(&ds, "likes").await);
	for n in 0..5 {
		for block in chunks_of(&ds, &person(n)).await {
			assert!(
				matches!(block.entries, BlockEntries::Plain(_)),
				"a chunk involving an unclaimable table must stay plain"
			);
		}
	}
	assert_eq!(battery(&ds, &ses).await, before);
}

/// `REMOVE TABLE` resets a table's doc-ID generation: it wipes the `!di`/
/// `!dd` mappings and the allocator along with the rest of the table's
/// keyspace, so a redefinition restarts allocation from scratch and a new
/// record can be handed an id a foreign block still names. The process-wide
/// resolve cache must not paper over that with a pre-removal hit: a stale
/// hit would resolve the foreign block's entry to the deleted record's key,
/// and — since the forward mapping for that key is gone too — assigning it
/// a doc-ID on the next fold would permanently bake a phantom mapping into
/// the rewritten block instead of correctly dropping the stale entry. The
/// cache is keyed on the table's never-reused `TableId`, so the redefined
/// table's lookups miss the warmed pre-removal entries by construction.
#[tokio::test]
async fn remove_table_resets_the_cached_doc_id_generation() {
	use surrealdb_cnf::ConfigMap;

	use crate::idx::docids::TableDocIds;
	use crate::val::{RecordIdKey, TableName};

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(ConfigMap::empty().with_key_value("graph_numeric_ids", "true"))
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	let org = RecordId::new("org".into(), "o1".to_owned());

	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person;
		 DEFINE TABLE org;
		 DEFINE TABLE owns TYPE RELATION;
		 CREATE person:old;
		 CREATE org:o1;
		 RELATE org:o1->owns:r1->person:old;",
	)
	.await;

	// Fold org:o1's edge: the block now names person:old by doc-ID, and
	// `person` carries the marker. This read also warms the process-wide
	// resolve cache with person:old's doc-ID.
	fold_vertex(&ds, &org).await;
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->owns->person FROM org:o1;").await,
		run(&ds, &ses, "RETURN [[person:old]];").await
	);

	// Remove and redefine `person`: its mapping and allocator both reset,
	// but org:o1's block — stored under org's own keyspace — still names
	// person:old's pre-removal doc-ID.
	run(&ds, &ses, "REMOVE TABLE person; DEFINE TABLE person;").await;
	run(&ds, &ses, "CREATE person:new; RELATE org:o1->owns:r2->person:new;").await;

	// Re-folding org:o1 both decodes the stale numeric entry and assigns
	// person:new's (possibly reused) doc-ID in the same pass.
	fold_vertex(&ds, &org).await;

	// Only the live record appears: the stale entry must be recognised as
	// dangling and dropped, never resolved to a phantom mapping.
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->owns->person FROM org:o1;").await,
		run(&ds, &ses, "RETURN [[person:new]];").await
	);

	// No mapping was minted for the deleted record's key in the redefined
	// table's doc-ID space.
	let (ns, db) = {
		let txn = ds.transaction(Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		txn.cancel().await.unwrap();
		(db.namespace_id, db.database_id)
	};
	let doc_ids = TableDocIds::new(ns, db, TableName::from("person"));
	let txn = ds.transaction(Read).await.unwrap();
	assert_eq!(
		doc_ids.get_doc_id(&txn, &RecordIdKey::from("old".to_owned())).await.unwrap(),
		None,
		"the deleted record must not have acquired a phantom mapping"
	);
	assert!(
		doc_ids.get_doc_id(&txn, &RecordIdKey::from("new".to_owned())).await.unwrap().is_some(),
		"the live record must have a real mapping"
	);
	txn.cancel().await.unwrap();
}

/// The write-skew window `Document::remove_doc_id` closes: reading
/// `graph_doc_ids` through the ordinary table snapshot registers no
/// commit-time conflict, so a transaction that skips doc-ID removal because
/// it observed the flag unset must not be allowed to commit that decision
/// once a concurrent fold has set the flag — the two transactions touch
/// disjoint write-sets, so no write-write check would catch it either, even
/// though the delete's read happened-before the fold's write. One of the
/// two must be forced to retry.
#[tokio::test]
async fn delete_racing_a_folds_flag_set_conflicts() {
	use surrealdb_cnf::ConfigMap;

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(ConfigMap::empty().with_key_value("graph_numeric_ids", "true"))
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;
	run(&ds, &ses, "CREATE person:p9;").await;

	// Nothing has folded yet: the marker doc-ID removal will gate on is
	// unset.
	assert!(!graph_doc_ids(&ds, "person").await);

	// Delete an unrelated record on a transaction that reads the unset
	// flag, but hold its commit back.
	let tx_del = Arc::new(ds.transaction(Write).await.unwrap());
	ds.run(QueryRequest::new("DELETE person:p9;", &ses).with_transaction(Arc::clone(&tx_del)))
		.await
		.unwrap();

	// A concurrent fold sets `graph_doc_ids` on `person` — it is the target
	// table of p0's folded edge — and commits first.
	fold_vertex(&ds, &person(0)).await;
	assert!(graph_doc_ids(&ds, "person").await);

	// The delete's decision to skip doc-ID removal rested on a flag that
	// has since changed underneath it: its commit must fail rather than
	// land quietly with a stale mapping.
	tx_del.commit().await.unwrap_err();

	// The record survives the rejected commit...
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE id FROM person:p9;").await,
		run(&ds, &ses, "RETURN [person:p9];").await
	);

	// ...and a retry, now observing the settled flag, succeeds cleanly.
	run(&ds, &ses, "DELETE person:p9;").await;
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE id FROM person:p9;").await,
		run(&ds, &ses, "RETURN [];").await
	);
}

/// The same write-skew window, closed for a delete spanning several
/// records in one transaction: the settled read behind `remove_doc_id` is
/// memoized per (transaction, table), so the delete issues the locked read
/// once — and that single registration must still invalidate the whole
/// transaction when a concurrent fold flips the flag.
#[tokio::test]
async fn multi_record_delete_racing_a_folds_flag_set_conflicts() {
	use surrealdb_cnf::ConfigMap;

	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(ConfigMap::empty().with_key_value("graph_numeric_ids", "true"))
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;
	run(&ds, &ses, "CREATE person:p7, person:p8, person:p9;").await;
	assert!(!graph_doc_ids(&ds, "person").await);

	// Delete three unrelated records on a transaction that reads the unset
	// flag, but hold its commit back.
	let tx_del = Arc::new(ds.transaction(Write).await.unwrap());
	ds.run(
		QueryRequest::new("DELETE person:p7, person:p8, person:p9;", &ses)
			.with_transaction(Arc::clone(&tx_del)),
	)
	.await
	.unwrap();

	// A concurrent fold sets `graph_doc_ids` on `person` and commits first.
	fold_vertex(&ds, &person(0)).await;
	assert!(graph_doc_ids(&ds, "person").await);

	// The delete's skip decision rested on the flag it read once for all
	// three records; its commit must fail rather than land quietly.
	tx_del.commit().await.unwrap_err();

	// The records survive the rejected commit, and a retry succeeds.
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE id FROM person:p7, person:p8, person:p9;").await,
		run(&ds, &ses, "RETURN [person:p7, person:p8, person:p9];").await
	);
	run(&ds, &ses, "DELETE person:p7, person:p8, person:p9;").await;
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE id FROM person:p7, person:p8, person:p9;").await,
		run(&ds, &ses, "RETURN [];").await
	);
}

/// The settled read is memoized per transaction — the first call's locked
/// read registers the table key for the whole transaction, and later calls
/// are served from the memo without touching the engine — and a savepoint
/// rollback clears the memo, because on rollback-scoped engines it may
/// release the registration along with the scope.
#[tokio::test]
async fn graph_doc_ids_settled_memoizes_per_transaction() {
	use std::borrow::Cow;

	use crate::catalog::providers::TableProvider;
	use crate::key::schema::TableKey;
	use crate::val::TableName;

	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	run(&ds, &ses, "DEFINE NAMESPACE test; DEFINE DATABASE test; DEFINE TABLE person;").await;
	let (ns, db) = {
		let txn = ds.transaction(Read).await.unwrap();
		let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		txn.cancel().await.unwrap();
		(db.namespace_id, db.database_id)
	};
	let tb = TableName::from("person");

	// Set the marker, so the memoized answer is distinguishable from the
	// answer an absent table gives.
	{
		let txn = ds.transaction(Write).await.unwrap();
		let def = txn.get_tb(ns, db, &tb, None).await.unwrap().unwrap();
		let mut updated = (*def).clone();
		updated.graph_doc_ids = true;
		txn.put_tb("test", "test", &updated).await.unwrap();
		txn.commit().await.unwrap();
	}

	let txn = ds.transaction(Write).await.unwrap();
	assert!(txn.graph_doc_ids_settled(ns, db, &tb).await.unwrap());
	// Delete the table key out from under the memo: a re-issued read would
	// now find no table at all, so the marker coming back proves the second
	// call never touched the engine.
	txn.del_key(&TableKey {
		ns,
		db,
		tb: Cow::Borrowed(&tb),
	})
	.await
	.unwrap();
	assert!(
		txn.graph_doc_ids_settled(ns, db, &tb).await.unwrap(),
		"the second call must be served from the memo"
	);
	// A savepoint rollback clears the memo, so the next call re-issues the
	// locked read and observes the in-transaction deletion.
	txn.new_save_point().await.unwrap();
	txn.rollback_to_save_point().await.unwrap();
	assert!(
		!txn.graph_doc_ids_settled(ns, db, &tb).await.unwrap(),
		"a savepoint rollback must clear the memo"
	);
	txn.cancel().await.unwrap();
}

/// Versioned reads across a fold, on the one backend with native
/// versioning. A fold is an ordinary transactional rewrite, so a read at
/// a pre-fold stamp merges both sides at that snapshot — where the delta
/// keys still hold everything and no blocks exist — and a read at a
/// post-fold, pre-delete stamp sees the folded blocks without the
/// tombstones written later.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn versioned_reads_see_their_snapshot_across_folds() {
	use crate::dbs::Capabilities;
	let dir = temp_dir::TempDir::new().unwrap();
	let path = format!("surrealkv://{}?versioned=true&retention=1h", dir.path().to_string_lossy());
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path(&path)
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	let before = run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await;
	let stamp = run(&ds, &ses, "RETURN time::now();").await;
	let Value::Datetime(prefold) = &stamp[0] else {
		panic!("expected a datetime stamp");
	};

	for n in 0..5 {
		fold_vertex(&ds, &person(n)).await;
	}
	let stamp = run(&ds, &ses, "RETURN time::now();").await;
	let Value::Datetime(postfold) = &stamp[0] else {
		panic!("expected a datetime stamp");
	};
	run(&ds, &ses, "DELETE likes:l02;").await;

	for stamp in [prefold, postfold] {
		let query = format!(
			"SELECT VALUE ->likes->person FROM person:p0 VERSION d'{}';",
			(*stamp).into_inner().to_rfc3339()
		);
		assert_eq!(run(&ds, &ses, &query).await, before);
	}
}

/// A catalog refresh — the read-modify-write every DDL statement performs
/// to bump one of a table's cache timestamps — writes the whole definition
/// back from its own snapshot, so a refresh whose snapshot predates a
/// concurrent fold's `graph_folded` flip carries the marker as false. The
/// locked read inside `replace_tb` makes whichever of the two commits
/// second conflict instead of silently erasing the marker; backends that
/// validate only writes at commit are covered by the `getu_write_conflict`
/// kvs contract test (and the TiKV race below).
#[tokio::test]
async fn a_stale_catalog_refresh_cannot_unset_graph_folded() {
	use crate::catalog::TableDefinition;
	use crate::catalog::providers::TableProvider;
	use crate::val::TableName;

	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	let (ns, db) = {
		let txn = ds.transaction(Read).await.unwrap();
		let dbdef = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
		let ids = (dbdef.namespace_id, dbdef.database_id);
		txn.cancel().await.unwrap();
		ids
	};
	let person_tb: TableName = "person".into();

	// The refresh pins its snapshot to the pre-fold state.
	let txn_b = ds.transaction(Write).await.unwrap();
	let before = txn_b.get_tb(ns, db, &person_tb, None).await.unwrap().unwrap();
	assert!(!before.graph_folded, "table must start unfolded");

	// A fold flips `graph_folded` and commits.
	let txn_a = Arc::new(ds.transaction(Write).await.unwrap());
	let mut env_a = ds.setup_ctx().unwrap();
	env_a.set_transaction(Arc::clone(&txn_a));
	let env_a = env_a.freeze();
	fold_scope(&env_a, ns, db, "test", "test", &person(0), Dir::Out, 1024).await.unwrap();
	txn_a.commit().await.unwrap();

	// The refresh writes the definition back from its stale snapshot — the
	// exact shape of every DDL cache-timestamp bump — and must fail rather
	// than clobber the flip.
	let refreshed = TableDefinition {
		cache_events_ts: uuid::Uuid::now_v7(),
		..(*before).clone()
	};
	let stale_refresh = match txn_b.replace_tb("test", "test", &refreshed).await {
		Ok(_) => txn_b.commit().await,
		Err(e) => {
			let _ = txn_b.cancel().await;
			Err(e)
		}
	};
	assert!(stale_refresh.is_err(), "the stale refresh must conflict with the committed flip");

	// The marker survives, a retried refresh carries it forward, and the
	// folded edges stay visible.
	let txn = ds.transaction(Write).await.unwrap();
	let tb = txn.get_tb(ns, db, &person_tb, None).await.unwrap().unwrap();
	assert!(tb.graph_folded, "the fold's flip must survive the refresh race");
	let refreshed = TableDefinition {
		cache_events_ts: uuid::Uuid::now_v7(),
		..(*tb).clone()
	};
	txn.replace_tb("test", "test", &refreshed).await.unwrap();
	txn.commit().await.unwrap();
	let txn = ds.transaction(Read).await.unwrap();
	let tb = txn.get_tb(ns, db, &person_tb, None).await.unwrap().unwrap();
	assert!(tb.graph_folded, "a refresh from a fresh read must carry the marker forward");
	txn.cancel().await.unwrap();
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p0;").await,
		run(&ds, &ses, "RETURN [[person:p1, person:p2, person:p3]];").await
	);
}

/// The `graph_folded` catalog flip's own concurrency guard, exercised
/// against a backend with no universal read validation: a plain read of
/// the table-definition key is not enough there, so the guard needs a
/// locked read (see `fold_scope`'s comment on the flip).
#[cfg(feature = "kv-tikv")]
mod tikv_concurrency {
	use uuid::Uuid;

	use super::*;
	use crate::CommunityComposer;
	use crate::catalog::providers::TableProvider;
	use crate::key::schema::{RootRoot, VersionKey};
	use crate::val::TableName;

	async fn fresh_tikv_ds() -> Arc<Datastore> {
		let ds = Datastore::builder()
			.with_id(Uuid::new_v4())
			.without_maintenance_tasks()
			.build_with_factory_path("tikv:127.0.0.1:2379", CommunityComposer())
			.await
			.unwrap();
		let tx = ds.transaction(Write).await.unwrap();
		// Both top-level regions: everything under the root, and the storage
		// version key, which sits outside it.
		tx.delr(RootRoot {}.range_subtree().unwrap()).await.unwrap();
		tx.del_key(&VersionKey {}).await.unwrap();
		tx.commit().await.unwrap();
		ds
	}

	/// Deterministic (fails before the fix, passes after): drive the exact
	/// window instead of relying on scheduler timing. B opens its
	/// transaction and reads the table definition — pinning its snapshot to
	/// the pre-fold state — before A folds a disjoint vertex and commits,
	/// flipping `graph_folded`. B then folds its own disjoint vertex on
	/// that stale snapshot: even though A's and B's folds share no
	/// adjacency key, both write the identical table-definition key to
	/// flip the same flag, so B's commit must fail.
	#[tokio::test]
	#[serial_test::serial]
	async fn concurrent_first_folds_do_not_both_flip_graph_folded() {
		let ds = fresh_tikv_ds().await;
		let ses = Session::owner().with_ns("test").with_db("test");
		seed(&ds, &ses).await;

		let (ns, db) = {
			let txn = ds.transaction(Read).await.unwrap();
			let dbdef = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
			let ids = (dbdef.namespace_id, dbdef.database_id);
			txn.cancel().await.unwrap();
			ids
		};
		let person_tb: TableName = "person".into();

		// B starts first and reads the table definition, pinning its
		// snapshot to the pre-fold state (`graph_folded` still false).
		let txn_b = Arc::new(ds.transaction(Write).await.unwrap());
		let before = txn_b.get_tb(ns, db, &person_tb, None).await.unwrap().unwrap();
		assert!(!before.graph_folded, "table must start unfolded");

		// A folds a disjoint vertex (p3's one outgoing edge) to completion
		// and commits, flipping `graph_folded`.
		let txn_a = Arc::new(ds.transaction(Write).await.unwrap());
		let mut env_a = ds.setup_ctx().unwrap();
		env_a.set_transaction(Arc::clone(&txn_a));
		let env_a = env_a.freeze();
		fold_scope(&env_a, ns, db, "test", "test", &person(3), Dir::Out, 1024).await.unwrap();
		txn_a.commit().await.unwrap();

		// B folds its own disjoint vertex (p1's one outgoing edge) on its
		// stale snapshot and tries to commit.
		let mut env_b = ds.setup_ctx().unwrap();
		env_b.set_transaction(Arc::clone(&txn_b));
		let env_b = env_b.freeze();
		fold_scope(&env_b, ns, db, "test", "test", &person(1), Dir::Out, 1024).await.unwrap();
		let b_result = txn_b.commit().await;
		assert!(
			b_result.is_err(),
			"B's commit must conflict: it flipped `graph_folded` from a snapshot \
			 that predates A's flip of the identical table-definition key"
		);

		// The flag survives set, and retrying B's fold on a fresh snapshot
		// converges cleanly.
		let txn_b2 = Arc::new(ds.transaction(Write).await.unwrap());
		let after = txn_b2.get_tb(ns, db, &person_tb, None).await.unwrap().unwrap();
		assert!(after.graph_folded, "the surviving commit must have flipped the flag");
		let mut env_b2 = ds.setup_ctx().unwrap();
		env_b2.set_transaction(Arc::clone(&txn_b2));
		let env_b2 = env_b2.freeze();
		fold_scope(&env_b2, ns, db, "test", "test", &person(1), Dir::Out, 1024).await.unwrap();
		txn_b2.commit().await.unwrap();

		// Both vertices' adjacency survived the race intact.
		assert_eq!(
			run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p1;").await,
			run(&ds, &ses, "RETURN [[person:p2]];").await
		);
		assert_eq!(
			run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p3;").await,
			run(&ds, &ses, "RETURN [[person:p4]];").await
		);
	}

	/// The DDL-refresh clobber on the backend where it is actually
	/// observable: TiKV never conflicts two blind writes to one key, so a
	/// cache refresh whose snapshot predates a concurrent fold's
	/// `graph_folded` flip would write the marker back to false and both
	/// transactions would commit — hiding every folded edge — were the
	/// refresh not routed through `replace_tb`'s locked read.
	#[tokio::test]
	#[serial_test::serial]
	async fn a_stale_catalog_refresh_cannot_unset_graph_folded_on_tikv() {
		use crate::catalog::TableDefinition;

		let ds = fresh_tikv_ds().await;
		let ses = Session::owner().with_ns("test").with_db("test");
		seed(&ds, &ses).await;

		let (ns, db) = {
			let txn = ds.transaction(Read).await.unwrap();
			let dbdef = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
			let ids = (dbdef.namespace_id, dbdef.database_id);
			txn.cancel().await.unwrap();
			ids
		};
		let person_tb: TableName = "person".into();

		// The refresh pins its snapshot to the pre-fold state.
		let txn_b = ds.transaction(Write).await.unwrap();
		let before = txn_b.get_tb(ns, db, &person_tb, None).await.unwrap().unwrap();
		assert!(!before.graph_folded, "table must start unfolded");

		// A fold flips `graph_folded` and commits.
		let txn_a = Arc::new(ds.transaction(Write).await.unwrap());
		let mut env_a = ds.setup_ctx().unwrap();
		env_a.set_transaction(Arc::clone(&txn_a));
		let env_a = env_a.freeze();
		fold_scope(&env_a, ns, db, "test", "test", &person(3), Dir::Out, 1024).await.unwrap();
		txn_a.commit().await.unwrap();

		// The refresh writes the definition back from its stale snapshot —
		// the exact shape of every DDL cache-timestamp bump.
		let refreshed = TableDefinition {
			cache_events_ts: Uuid::now_v7(),
			..(*before).clone()
		};
		let stale_refresh = match txn_b.replace_tb("test", "test", &refreshed).await {
			Ok(_) => txn_b.commit().await,
			Err(e) => {
				let _ = txn_b.cancel().await;
				Err(e)
			}
		};
		assert!(stale_refresh.is_err(), "the stale refresh must conflict with the committed flip");

		// The marker survives and the folded edge stays visible.
		let txn = ds.transaction(Read).await.unwrap();
		let after = txn.get_tb(ns, db, &person_tb, None).await.unwrap().unwrap();
		assert!(after.graph_folded, "the fold's flip must survive the refresh race");
		txn.cancel().await.unwrap();
		assert_eq!(
			run(&ds, &ses, "SELECT VALUE ->likes->person FROM person:p3;").await,
			run(&ds, &ses, "RETURN [[person:p4]];").await
		);
	}
}
