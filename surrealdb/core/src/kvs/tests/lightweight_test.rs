//! Lightweight (record-less) relation semantics, driven end to end through
//! a `Datastore` with real SurrealQL.
//!
//! A lightweight edge is its two vertex-side adjacency keys and nothing
//! else; the record every reader sees is synthesized from the canonical
//! `[in, out]` id. The invariant under test: queries against a lightweight
//! relation answer exactly as they would against a classic relation
//! holding the same edges — traversals, point reads, table scans, counts,
//! ordering, deletes and cascades — while the DDL and write paths reject
//! everything that assumes stored records.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;
use surrealdb_types::{ToSql, Value};

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

/// Runs one statement and returns its error message.
async fn run_err(ds: &Datastore, ses: &Session, sql: &str) -> String {
	let mut res = ds.execute(sql, ses, None).await.unwrap();
	assert_eq!(res.len(), 1, "expected a single statement: {sql}");
	res.remove(0).result.expect_err(&format!("expected an error from: {sql}")).to_string()
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

/// The query battery whose results must match a classic relation holding
/// the same edges: traversals in every direction and output mode, point
/// reads, full-table scans, counts, ordering, and multi-hop chains.
const BATTERY: &[&str] = &[
	"SELECT VALUE ->follows FROM person:p0;",
	"SELECT VALUE ->follows->person FROM person:p0;",
	"SELECT VALUE <-follows FROM person:p2;",
	"SELECT VALUE <->follows FROM person:p1;",
	"SELECT VALUE ->follows.* FROM person:p0;",
	"SELECT VALUE ->(follows WHERE out = person:p2) FROM person:p0;",
	"SELECT VALUE ->follows->person->follows->person FROM person:p0;",
	"SELECT * FROM follows;",
	"SELECT VALUE id FROM follows;",
	"SELECT count() FROM follows GROUP ALL;",
	"SELECT * FROM follows ORDER BY id DESC LIMIT 2;",
	"SELECT * FROM ONLY follows:[person:p0, person:p1];",
	// Batched record reads: FETCH resolves a run of edge ids through the
	// multi-get path, and a multi-id source mixes live and absent edges.
	"SELECT ->follows AS f FROM person:p0 FETCH f;",
	"SELECT * FROM follows:[person:p0, person:p1], follows:[person:p1, person:p2], \
	 follows:[person:p0, person:p3];",
	"RETURN follows:[person:p0, person:p1].out;",
	"RETURN record::exists(follows:[person:p0, person:p1]);",
	"RETURN record::exists(follows:[person:p0, person:p4]);",
];

async fn battery(ds: &Datastore, ses: &Session) -> Vec<Vec<Value>> {
	let mut out = Vec::new();
	for query in BATTERY {
		out.push(run(ds, ses, query).await);
	}
	out
}

/// Seeds an identical graph over either a lightweight or a classic
/// relation.
async fn seed(ds: &Datastore, ses: &Session, lightweight: bool) {
	let flags = if lightweight {
		"LIGHTWEIGHT"
	} else {
		"ENFORCED"
	};
	run(
		ds,
		ses,
		&format!(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE person;
			 DEFINE TABLE follows TYPE RELATION IN person OUT person {flags};
			 CREATE person:p0, person:p1, person:p2, person:p3;
			 RELATE person:p0->follows->person:p1;
			 RELATE person:p0->follows->person:p2;
			 RELATE person:p1->follows->person:p2;
			 RELATE person:p2->follows->person:p3;
			 RELATE person:p3->follows->person:p0;"
		),
	)
	.await;
}

/// Strips ids down to comparable shape: a classic RELATE generates random
/// edge ids while a lightweight one derives them, so whole-row comparisons
/// only make sense on tables seeded with explicit canonical ids. This
/// helper seeds the classic twin with the lightweight table's canonical
/// ids so every battery row compares byte for byte.
async fn seed_classic_canonical(ds: &Datastore, ses: &Session) {
	run(
		ds,
		ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person;
		 DEFINE TABLE follows TYPE RELATION IN person OUT person ENFORCED;
		 CREATE person:p0, person:p1, person:p2, person:p3;
		 RELATE person:p0->follows:[person:p0, person:p1]->person:p1;
		 RELATE person:p0->follows:[person:p0, person:p2]->person:p2;
		 RELATE person:p1->follows:[person:p1, person:p2]->person:p2;
		 RELATE person:p2->follows:[person:p2, person:p3]->person:p3;
		 RELATE person:p3->follows:[person:p3, person:p0]->person:p0;",
	)
	.await;
}

/// The headline equality: the whole battery answers identically against a
/// lightweight relation and a classic relation holding the same edges with
/// the same (canonical) ids — before and after deletes and a vertex
/// cascade.
#[tokio::test]
async fn lightweight_results_equal_classic_results() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let light = ds().await;
	let classic = ds().await;
	seed(&light, &ses, true).await;
	seed_classic_canonical(&classic, &ses).await;
	assert_eq!(battery(&light, &ses).await, battery(&classic, &ses).await);

	const MUTATIONS: &str = "DELETE follows:[person:p0, person:p2];
		 DELETE person:p3;";
	run(&light, &ses, MUTATIONS).await;
	run(&classic, &ses, MUTATIONS).await;
	assert_eq!(battery(&light, &ses).await, battery(&classic, &ses).await);

	// A traversal-addressed delete flows through the same pipeline.
	run(&light, &ses, "DELETE person:p0->follows;").await;
	run(&classic, &ses, "DELETE person:p0->follows;").await;
	assert_eq!(battery(&light, &ses).await, battery(&classic, &ses).await);
}

/// Folding the endpoint tables moves lightweight edges into packed blocks:
/// point reads probe the covering chunk, scans read through the merged
/// cursor, and deletes tombstone — the battery stays invariant throughout.
#[tokio::test]
async fn folded_lightweight_edges_stay_correct() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let light = ds().await;
	let classic = ds().await;
	seed(&light, &ses, true).await;
	seed_classic_canonical(&classic, &ses).await;

	for n in 0..4 {
		let rid = RecordId::new("person".into(), format!("p{n}"));
		fold_vertex(&light, &rid).await;
	}
	assert_eq!(battery(&light, &ses).await, battery(&classic, &ses).await);

	// Deleting a folded lightweight edge tombstones its pointer keys.
	run(&light, &ses, "DELETE follows:[person:p0, person:p1];").await;
	run(&classic, &ses, "DELETE follows:[person:p0, person:p1];").await;
	assert_eq!(battery(&light, &ses).await, battery(&classic, &ses).await);
}

/// A repeated RELATE of the same lightweight edge is idempotent, an
/// explicit canonical id is accepted, and a non-canonical id is rejected.
#[tokio::test]
async fn relate_is_idempotent_and_ids_are_canonical() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;

	run(&ds, &ses, "RELATE person:p0->follows->person:p1;").await;
	run(&ds, &ses, "RELATE person:p0->follows:[person:p0, person:p1]->person:p1;").await;
	let out = run(&ds, &ses, "SELECT VALUE ->follows FROM person:p0;").await;
	let rows = out.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	let edges = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(edges.len(), 2, "repeated RELATEs must not duplicate the edge");

	let err = run_err(&ds, &ses, "RELATE person:p0->follows:custom->person:p1;").await;
	assert!(err.contains("canonical"), "unexpected error: {err}");
	let err = run_err(&ds, &ses, "RELATE person:p0->follows->person:p1 SET weight = 1;").await;
	assert!(err.contains("data clause"), "unexpected error: {err}");
}

/// Every DDL and write shape that assumes stored records is rejected, and
/// the definitions that are accepted normalise as documented.
#[tokio::test]
async fn ddl_and_write_rejections() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	run(&ds, &ses, "DEFINE NAMESPACE test; DEFINE DATABASE test; DEFINE TABLE person;").await;

	// Accepted: LIGHTWEIGHT normalises to ENFORCED LIGHTWEIGHT in INFO.
	run(&ds, &ses, "DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;").await;
	let info = run(&ds, &ses, "INFO FOR DB;").await.remove(0).to_sql();
	assert!(info.contains("ENFORCED LIGHTWEIGHT"), "INFO must render the canonical flags: {info}");

	// Rejected definitions.
	for stmt in [
		"DEFINE TABLE bad TYPE RELATION LIGHTWEIGHT;",
		"DEFINE TABLE bad TYPE RELATION IN person LIGHTWEIGHT;",
		"DEFINE TABLE bad TYPE RELATION IN person OUT person LIGHTWEIGHT SCHEMAFULL;",
		"DEFINE TABLE bad TYPE RELATION IN person OUT person LIGHTWEIGHT CHANGEFEED 1h;",
		"DEFINE TABLE bad TYPE RELATION IN person OUT person LIGHTWEIGHT DROP;",
	] {
		let err = run_err(&ds, &ses, stmt).await;
		assert!(err.contains("LIGHTWEIGHT"), "expected a lightweight rejection from {stmt}: {err}");
	}

	// Rejected schema objects.
	for stmt in [
		"DEFINE FIELD weight ON follows TYPE number;",
		"DEFINE INDEX idx ON follows FIELDS in;",
		"DEFINE EVENT ev ON follows WHEN true THEN {};",
	] {
		let err = run_err(&ds, &ses, stmt).await;
		assert!(err.contains("LIGHTWEIGHT"), "expected a lightweight rejection from {stmt}: {err}");
	}
	// Rejected subscription, under a realtime-capable session.
	let rt = ses.clone().with_rt(true);
	let err = run_err(&ds, &rt, "LIVE SELECT * FROM follows;").await;
	assert!(err.contains("LIGHTWEIGHT"), "expected a lightweight rejection from LIVE: {err}");

	// Rejected mutations.
	run(&ds, &ses, "CREATE person:a, person:b; RELATE person:a->follows->person:b;").await;
	for stmt in [
		"UPDATE follows:[person:a, person:b] SET x = 1;",
		"UPDATE follows SET x = 1;",
		"INSERT RELATION INTO follows { in: person:a, out: person:b };",
		"UPSERT follows:[person:a, person:b];",
		"CREATE follows;",
	] {
		let err = run_err(&ds, &ses, stmt).await;
		assert!(
			err.contains("LIGHTWEIGHT") || err.contains("relation"),
			"expected a rejection from {stmt}: {err}"
		);
	}

	// A lightweight edge cannot be a graph endpoint — even of a classic
	// relation whose endpoint types would otherwise admit any record.
	run(&ds, &ses, "DEFINE TABLE likes TYPE RELATION;").await;
	let err = run_err(&ds, &ses, "RELATE follows:[person:a, person:b]->likes->person:a;").await;
	assert!(err.contains("endpoint"), "unexpected error: {err}");

	// Clearing the flag is rejected; growing IN/OUT is allowed; narrowing
	// is rejected while edges exist and allowed once empty.
	let err =
		run_err(&ds, &ses, "DEFINE TABLE OVERWRITE follows TYPE RELATION IN person OUT person;")
			.await;
	assert!(err.contains("redefined"), "unexpected error: {err}");
	run(&ds, &ses, "DEFINE TABLE other;").await;
	run(
		&ds,
		&ses,
		"DEFINE TABLE OVERWRITE follows TYPE RELATION IN person | other OUT person LIGHTWEIGHT;",
	)
	.await;
	let err = run_err(
		&ds,
		&ses,
		"DEFINE TABLE OVERWRITE follows TYPE RELATION IN other OUT person LIGHTWEIGHT;",
	)
	.await;
	assert!(err.contains("grow"), "unexpected error: {err}");

	// REMOVE TABLE refuses while edges exist, succeeds once emptied.
	let err = run_err(&ds, &ses, "REMOVE TABLE follows;").await;
	assert!(err.contains("holds edges"), "unexpected error: {err}");
	run(&ds, &ses, "DELETE follows;").await;
	run(&ds, &ses, "REMOVE TABLE follows;").await;

	// A table with records cannot become lightweight.
	run(
		&ds,
		&ses,
		"DEFINE TABLE busy TYPE RELATION IN person OUT person; RELATE person:a->busy->person:b;",
	)
	.await;
	let err = run_err(
		&ds,
		&ses,
		"DEFINE TABLE OVERWRITE busy TYPE RELATION IN person OUT person LIGHTWEIGHT;",
	)
	.await;
	assert!(err.contains("empty"), "unexpected error: {err}");
}

/// Export emits a lightweight relation as its definition plus idempotent
/// `RELATE` statements — no field lines, no `INSERT RELATION` — and the
/// round trip through a fresh datastore reproduces every battery answer.
#[tokio::test]
async fn export_reimports_equivalently() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let source = ds().await;
	seed(&source, &ses, true).await;

	let (tx, rx) = crate::channel::bounded::<Vec<u8>>(16);
	let task = source.export(&ses, tx).await.unwrap();
	let collector = tokio::spawn(async move {
		let mut out = Vec::new();
		while let Ok(chunk) = rx.recv().await {
			out.extend_from_slice(&chunk);
		}
		out
	});
	task.await.unwrap();
	let sql = String::from_utf8(collector.await.unwrap()).unwrap();
	assert!(sql.contains("RELATE person:p0 -> follows -> person:p1"), "export: {sql}");
	assert!(!sql.contains("DEFINE FIELD OVERWRITE in ON follows"), "export: {sql}");

	let target = ds().await;
	run(&target, &ses, "DEFINE NAMESPACE test; DEFINE DATABASE test;").await;
	for result in target.import(&sql, &ses).await.unwrap() {
		result.result.unwrap();
	}
	assert_eq!(battery(&source, &ses).await, battery(&target, &ses).await);
}

/// The canonical `[in, out]` id sorts in the storekey domain exactly as
/// the `(in, out)` endpoint tuple does — the ordering the record-less
/// scan emits in, and what makes exports deterministic.
#[test]
fn canonical_ids_sort_like_their_endpoints() {
	use crate::val::{Array, RecordIdKey, Value};
	let rid = |t: &str, k: &str| RecordId::new(t.into(), k.to_owned());
	let id = |l: &RecordId, r: &RecordId| {
		RecordIdKey::Array(Array(vec![Value::RecordId(l.clone()), Value::RecordId(r.clone())]))
	};
	let pairs = [
		(rid("a", "x"), rid("b", "y")),
		(rid("a", "x"), rid("b", "z")),
		(rid("a", "y"), rid("a", "a")),
		(rid("b", "a"), rid("a", "a")),
		(rid("b", "a"), rid("b", "a")),
	];
	let mut by_tuple: Vec<_> = pairs.iter().collect();
	by_tuple.sort_by_key(|(l, r)| {
		let mut key = storekey::encode_vec(&l.table).unwrap();
		key.extend(storekey::encode_vec(&l.key).unwrap());
		key.extend(storekey::encode_vec(&r.table).unwrap());
		key.extend(storekey::encode_vec(&r.key).unwrap());
		key
	});
	let mut by_id: Vec<_> = pairs.iter().collect();
	by_id.sort_by_key(|(l, r)| storekey::encode_vec(&id(l, r)).unwrap());
	assert_eq!(
		by_tuple.iter().map(|(l, r)| (l.to_sql(), r.to_sql())).collect::<Vec<_>>(),
		by_id.iter().map(|(l, r)| (l.to_sql(), r.to_sql())).collect::<Vec<_>>(),
	);
}

/// A database-level changefeed observes lightweight edges: RELATE emits an
/// update mutation carrying exactly the synthesized `{id, in, out}` shape,
/// and DELETE emits the deletion.
#[tokio::test]
async fn database_changefeed_observes_lightweight_edges() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test CHANGEFEED 1h;
		 DEFINE TABLE person;
		 DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
		 CREATE person:a, person:b;
		 RELATE person:a->follows->person:b;
		 DELETE follows:[person:a, person:b];",
	)
	.await;
	let changes = run(&ds, &ses, "SHOW CHANGES FOR TABLE follows SINCE 0;").await.remove(0);
	let text = changes.to_sql();
	assert!(
		text.contains("in: person:a") && text.contains("out: person:b"),
		"the RELATE mutation must carry the synthesized edge: {text}"
	);
	assert!(text.contains("delete"), "the DELETE must be recorded: {text}");
}

/// A lightweight edge's synthesized adjacency is gated on the edge
/// existing: traversing a deleted or never-created canonical id expands
/// to nothing, in both executors.
#[tokio::test]
async fn traversal_from_a_dead_edge_id_expands_to_nothing() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;
	run(&ds, &ses, "DELETE follows:[person:p0, person:p1];").await;
	for query in [
		"RETURN follows:[person:p0, person:p1]->person;",
		"RETURN follows:[person:p2, person:p0]->person;",
		"SELECT VALUE ->person FROM follows:[person:p0, person:p1];",
	] {
		let out = run(&ds, &ses, query).await.remove(0);
		let rows: Vec<Value> = match out {
			Value::Array(a) => a
				.into_iter()
				.flat_map(|v| match v {
					Value::Array(inner) => inner.into_iter().collect::<Vec<_>>(),
					Value::None => Vec::new(),
					other => vec![other],
				})
				.collect(),
			Value::None => Vec::new(),
			other => vec![other],
		};
		assert!(rows.is_empty(), "{query} must expand to nothing, got {rows:?}");
	}
}

/// The LIGHTWEIGHT flag clears only while the relation is empty — the
/// recoverability escape for anything that slips past the emptiness
/// probes — and DDL that would undermine the record-less contract is
/// rejected: altering or removing the auto in/out fields, and removing
/// an endpoint table while the relation holds edges.
#[tokio::test]
async fn contract_guarding_ddl() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;

	// Non-empty: clearing the flag and touching in/out are rejected.
	for stmt in [
		"DEFINE TABLE OVERWRITE follows TYPE RELATION IN person OUT person ENFORCED;",
		"ALTER FIELD in ON follows TYPE record<person>;",
		"REMOVE FIELD in ON follows;",
		"REMOVE TABLE person;",
	] {
		let err = run_err(&ds, &ses, stmt).await;
		assert!(err.contains("LIGHTWEIGHT"), "expected a lightweight rejection from {stmt}: {err}");
	}

	// Emptied: the endpoint table and the flag are both releasable.
	run(&ds, &ses, "DELETE follows;").await;
	run(&ds, &ses, "DEFINE TABLE OVERWRITE follows TYPE RELATION IN person OUT person ENFORCED;")
		.await;
	let info = run(&ds, &ses, "INFO FOR DB;").await.remove(0);
	let text = surrealdb_types::ToSql::to_sql(&info);
	assert!(!text.contains("LIGHTWEIGHT"), "the flag must clear on the empty relation: {text}");
}
