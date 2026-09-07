//! Graph reachability semi-joins as bitmap branches (issue #549), driven end
//! to end through a `Datastore` with real SurrealQL.
//!
//! The invariant under test: a `WHERE <indexed> AND ->edge->vertex CONTAINS
//! <literal>` fused into a bitmap plan answers exactly as the per-row
//! evaluation does — including on adjacency that still holds legacy-format
//! keys (no embedded target), where the branch must drop itself rather than
//! under-approximate, leaving the residual filter to answer.

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;
use surrealdb_types::Value;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::record::{Record, RecordType};
use crate::dbs::Session;
use crate::expr::Dir;
use crate::key::schema::{GraphKey, RecordKey};
use crate::kvs::Datastore;
use crate::val::{Object, RecordId, RecordIdKey, TableName};

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

async fn assert_cases(ds: &Datastore, ses: &Session, cases: &[(&str, &str)]) {
	for (query, expected) in cases {
		let got = run(ds, ses, query).await.remove(0);
		assert_eq!(got, run(ds, ses, &format!("RETURN {expected};")).await.remove(0), "`{query}`");
	}
}

/// `EXPLAIN ANALYZE` (text format) answers a plain `Value::String`; pull
/// the raw text out rather than going through `ToSql`, which would
/// re-quote it as a SQL string literal and escape its newlines, breaking
/// a per-line search.
async fn analyze_text(ds: &Datastore, ses: &Session, query: &str) -> String {
	match run(ds, ses, query).await.remove(0) {
		Value::String(text) => text,
		other => panic!("expected EXPLAIN ANALYZE to answer a string, got {other:?}"),
	}
}

/// Persons 1..=8 with `flag = true` on the even ones, an indexed `flag`
/// column (the doc-ID-capable sibling every bitmap plan needs), and
/// `person:1..=4 ->wrote-> doc:x`.
async fn seed(ds: &Datastore, ses: &Session) {
	run(
		ds,
		ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE doc SCHEMALESS;
		 DEFINE TABLE wrote TYPE RELATION;
		 DEFINE INDEX idx_flag ON person FIELDS flag;
		 DEFINE INDEX idx_kind ON doc FIELDS kind;
		 FOR $n IN 1..=8 { CREATE type::record('person', $n) SET flag = $n % 2 == 0 RETURN NONE; };
		 CREATE doc:x SET kind = 'paper' RETURN NONE;
		 CREATE doc:y SET kind = 'paper' RETURN NONE;
		 FOR $n IN 1..=4 { RELATE (type::record('person', $n))->wrote->doc:x RETURN NONE; };
		 RELATE person:1->wrote->doc:y RETURN NONE;",
	)
	.await;
}

/// The fused shapes answer exactly, the plan carries the graph branch, and
/// every shape outside the v1 contract declines it.
#[tokio::test]
async fn semijoins_fuse_and_answer_exactly() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	assert_cases(
		&ds,
		&ses,
		&[
			// The CONTAINS form: indexed sibling ∩ reachable-from-doc:x.
			(
				"SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x;",
				"[person:2, person:4]",
			),
			// The flipped INSIDE form.
			(
				"SELECT VALUE id FROM person WHERE flag = true AND doc:x INSIDE ->wrote->doc;",
				"[person:2, person:4]",
			),
			// The written `<-` direction traverses Out from the anchor.
			(
				"SELECT VALUE id FROM doc WHERE kind = 'paper' AND <-wrote<-person CONTAINS person:1;",
				"[doc:x, doc:y]",
			),
			// A row satisfying the index branch but not the traversal.
			(
				"SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:y;",
				"[]",
			),
		],
	)
	.await;

	let plan = surrealdb_types::ToSql::to_sql(
		&run(
			&ds,
			&ses,
			"EXPLAIN SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x;",
		)
		.await
		.remove(0),
	);
	assert!(plan.contains("BitmapGraphScan"), "expected the graph branch in: {plan}");
	assert!(plan.contains("BitmapAnd"), "expected an intersection in: {plan}");

	// Shapes outside the v1 contract keep today's plan: no graph branch.
	for declined in [
		// No index-backed sibling: nothing anchors the intersection.
		"EXPLAIN SELECT VALUE id FROM person WHERE ->wrote->doc CONTAINS doc:x;",
		// Multi-hop.
		"EXPLAIN SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc->wrote->doc CONTAINS doc:x;",
		// Both-direction traversal.
		"EXPLAIN SELECT VALUE id FROM person WHERE flag = true AND <->wrote<->doc CONTAINS doc:x;",
		// The anchor's table is not a vertex table the hop names.
		"EXPLAIN SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS person:1;",
	] {
		let plan = surrealdb_types::ToSql::to_sql(&run(&ds, &ses, declined).await.remove(0));
		assert!(
			!plan.contains("BitmapGraphScan"),
			"unexpected graph branch in `{declined}`: {plan}"
		);
	}
}

/// A unique-equality anchor guarantees at most one candidate row, so the
/// planner must keep the streaming plan — draining the reachable set to
/// intersect against one row can never beat the residual per-row traversal
/// — while the query still answers exactly.
#[tokio::test]
async fn a_unique_anchor_keeps_the_streaming_plan() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE doc SCHEMALESS;
		 DEFINE TABLE wrote TYPE RELATION;
		 DEFINE INDEX idx_email ON person FIELDS email UNIQUE;
		 FOR $n IN 1..=8 { CREATE type::record('person', $n) SET email = 'p' + <string>$n + '@x' RETURN NONE; };
		 CREATE doc:x RETURN NONE;
		 FOR $n IN 1..=4 { RELATE (type::record('person', $n))->wrote->doc:x RETURN NONE; };",
	)
	.await;

	assert_cases(
		&ds,
		&ses,
		&[
			(
				"SELECT VALUE id FROM person WHERE email = 'p2@x' AND ->wrote->doc CONTAINS doc:x;",
				"[person:2]",
			),
			// The anchor row exists but does not satisfy the traversal.
			(
				"SELECT VALUE id FROM person WHERE email = 'p6@x' AND ->wrote->doc CONTAINS doc:x;",
				"[]",
			),
		],
	)
	.await;

	let plan = surrealdb_types::ToSql::to_sql(
		&run(
			&ds,
			&ses,
			"EXPLAIN SELECT VALUE id FROM person WHERE email = 'p2@x' AND ->wrote->doc CONTAINS doc:x;",
		)
		.await
		.remove(0),
	);
	assert!(
		!plan.contains("BitmapGraphScan") && !plan.contains("BitmapResolve"),
		"a unique anchor must keep the streaming plan: {plan}"
	);
}

/// A graph branch whose adjacency band dwarfs the intersection accumulated
/// so far is cut off by the accumulator-proportional budget: the branch is
/// dropped (observably), the residual filter answers, and the hub's degree
/// is never drained in full against a one-row anchor.
#[tokio::test]
async fn a_hub_anchor_drops_the_graph_branch_against_a_small_intersection() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	// One indexed (non-unique) branch matching exactly one person, and a hub
	// whose in-degree (100) exceeds that row's proportional budget
	// (1 × AND_CHILD_BUDGET_FACTOR = 64).
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE doc SCHEMALESS;
		 DEFINE TABLE wrote TYPE RELATION;
		 DEFINE INDEX idx_flag ON person FIELDS flag;
		 FOR $n IN 1..=100 { CREATE type::record('person', $n) SET flag = $n == 1 RETURN NONE; };
		 CREATE doc:hub RETURN NONE;
		 FOR $n IN 1..=100 { RELATE (type::record('person', $n))->wrote->doc:hub RETURN NONE; };",
	)
	.await;

	assert_cases(
		&ds,
		&ses,
		&[(
			"SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:hub;",
			"[person:1]",
		)],
	)
	.await;

	let mut redacted = Session::owner().with_ns("test").with_db("test");
	redacted.redact_volatile_explain_attrs = true;
	let plan = analyze_text(
		&ds,
		&redacted,
		"EXPLAIN ANALYZE SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:hub;",
	)
	.await;
	let line = plan
		.lines()
		.find(|l| l.contains("BitmapGraphScan"))
		.unwrap_or_else(|| panic!("expected a BitmapGraphScan node in: {plan}"));
	assert!(
		line.contains("rows: 0") && line.contains("dropped: true"),
		"the hub branch must be cut off by the proportional budget: {line}"
	);
}

/// Writes one edge in the full variant-1 layout — bare [`GraphKey`]s, with
/// no embedded target, on both vertices and on the edge record — exactly the
/// layout `Document::store_edges_data` migrates away from. It cannot be
/// produced through SurrealQL.
async fn seed_v1_edge(ds: &Datastore, edge: &RecordId, l: &RecordId, r: &RecordId) {
	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let (ns, dbid) = (db.namespace_id, db.database_id);
	let ltr = GraphKey {
		ns,
		db: dbid,
		tb: Cow::Borrowed(&l.table),
		id: Cow::Borrowed(&l.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&edge.table),
		foreign_key: Cow::Borrowed(&edge.key),
	};
	let rtl = GraphKey {
		ns,
		db: dbid,
		tb: Cow::Borrowed(&r.table),
		id: Cow::Borrowed(&r.key),
		dir: Dir::In,
		foreign_table: Cow::Borrowed(&edge.table),
		foreign_key: Cow::Borrowed(&edge.key),
	};
	let mut data = Object::default();
	data.insert("id".to_string(), crate::val::Value::RecordId(edge.clone()));
	data.insert("in".to_string(), crate::val::Value::RecordId(l.clone()));
	data.insert("out".to_string(), crate::val::Value::RecordId(r.clone()));
	let mut record = Record::new(crate::val::Value::Object(data));
	record.set_record_type(RecordType::Edge {
		variant: 1,
	});
	let etl = GraphKey {
		ns,
		db: dbid,
		tb: Cow::Borrowed(&edge.table),
		id: Cow::Borrowed(&edge.key),
		dir: Dir::In,
		foreign_table: Cow::Borrowed(&l.table),
		foreign_key: Cow::Borrowed(&l.key),
	};
	let etr = GraphKey {
		ns,
		db: dbid,
		tb: Cow::Borrowed(&edge.table),
		id: Cow::Borrowed(&edge.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&r.table),
		foreign_key: Cow::Borrowed(&r.key),
	};
	txn.set_key(&ltr, &()).await.unwrap();
	txn.set_key(&rtl, &()).await.unwrap();
	txn.set_key(&etl, &()).await.unwrap();
	txn.set_key(&etr, &()).await.unwrap();
	txn.set_key(
		&RecordKey {
			ns,
			db: dbid,
			tb: Cow::Borrowed(&edge.table),
			id: Cow::Borrowed(&edge.key),
		},
		&record,
	)
	.await
	.unwrap();
	txn.commit().await.unwrap();
}

/// A legacy-format key in the anchor's scope carries no target, so the graph
/// branch cannot know the reachable set — it must drop itself (overflow),
/// leaving the residual filter to answer. The row reachable only through the
/// legacy edge must still be returned.
#[tokio::test]
async fn a_legacy_key_drops_the_branch_not_the_rows() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	let edge = RecordId::new(TableName::from("wrote"), RecordIdKey::String("legacy".into()));
	let l = RecordId::new(TableName::from("person"), 6);
	let r = RecordId::new(TableName::from("doc"), RecordIdKey::String("x".into()));
	seed_v1_edge(&ds, &edge, &l, &r).await;

	// person:6 (flag = true) wrote doc:x through the legacy edge: it must
	// appear even though the anchor's adjacency scope now holds a key the
	// bitmap branch cannot decode a target from.
	assert_cases(
		&ds,
		&ses,
		&[(
			"SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x;",
			"[person:2, person:4, person:6]",
		)],
	)
	.await;

	// The forced drop is observable as such: the leaf reports `dropped`,
	// not an empty reachable set.
	let mut redacted = Session::owner().with_ns("test").with_db("test");
	redacted.redact_volatile_explain_attrs = true;
	let plan = analyze_text(
		&ds,
		&redacted,
		"EXPLAIN ANALYZE SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x;",
	)
	.await;
	let line = plan
		.lines()
		.find(|l| l.contains("BitmapGraphScan"))
		.unwrap_or_else(|| panic!("expected a BitmapGraphScan node in: {plan}"));
	assert!(
		line.contains("rows: 0") && line.contains("dropped: true"),
		"a legacy key must drop the branch observably: {line}"
	);
}

/// The bitmap leaf reads the anchor's adjacency keys directly, never
/// fetching (or permission-checking) the edge record. A session without
/// `SELECT` on the edge table must not receive that record's cardinality
/// through the leaf's own `EXPLAIN ANALYZE` row count, and the query's
/// final rows must still answer correctly through the residual,
/// permission-checked traversal.
#[tokio::test]
async fn a_select_denied_edge_table_declines_the_bitmap_leaf() {
	let ds = ds().await;
	let owner = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&owner,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE wrote TYPE RELATION PERMISSIONS FOR select NONE;
		 DEFINE INDEX idx_flag ON person FIELDS flag;
		 FOR $n IN 1..=8 { CREATE type::record('person', $n) SET flag = $n % 2 == 0 RETURN NONE; };
		 CREATE doc:x SET kind = 'paper' RETURN NONE;
		 FOR $n IN 1..=4 { RELATE (type::record('person', $n))->wrote->doc:x RETURN NONE; };",
	)
	.await;

	let mut owner = owner;
	owner.redact_volatile_explain_attrs = true;
	let mut low = Session::for_record(
		"test",
		"test",
		"user",
		crate::types::PublicValue::String("person:1".to_owned()),
	);
	low.redact_volatile_explain_attrs = true;

	let query = "EXPLAIN ANALYZE SELECT VALUE id FROM person WHERE flag = true AND \
	             ->wrote->doc CONTAINS doc:x;";

	// Root sees the graph leaf's true reachable-set cardinality: all four
	// persons that wrote doc:x, before the `flag = true` intersection.
	let owner_plan = analyze_text(&ds, &owner, query).await;
	let owner_line = owner_plan
		.lines()
		.find(|l| l.contains("BitmapGraphScan"))
		.unwrap_or_else(|| panic!("expected a BitmapGraphScan node in: {owner_plan}"));
	assert!(
		owner_line.contains("rows: 4"),
		"expected the true reachable-set count in: {owner_line}"
	);

	// A select-denied session must not learn that count: the leaf must
	// decline — reported as a dropped branch, not as an empty reachable
	// set — whether or not it still appears in the plan's static shape.
	let low_plan = analyze_text(&ds, &low, query).await;
	if let Some(low_line) = low_plan.lines().find(|l| l.contains("BitmapGraphScan")) {
		assert!(
			low_line.contains("rows: 0"),
			"a select-denied session must not see the true reachable-set count: {low_line}"
		);
		assert!(
			low_line.contains("dropped: true"),
			"a declined leaf must be reported as dropped, not as empty: {low_line}"
		);
	}
	// The owner's completed leaf must not carry the dropped marker.
	assert!(
		!owner_line.contains("dropped"),
		"a completed leaf must not be reported as dropped: {owner_line}"
	);

	// Final rows are correct either way: the edge is invisible to this
	// session, so no person satisfies the traversal.
	assert_cases(
		&ds,
		&low,
		&[("SELECT VALUE id FROM person WHERE flag = true AND ->wrote->doc CONTAINS doc:x;", "[]")],
	)
	.await;
}
