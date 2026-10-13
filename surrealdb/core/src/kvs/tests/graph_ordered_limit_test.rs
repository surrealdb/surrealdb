//! `->(SELECT ... FROM edge ORDER BY id LIMIT n)` driven end to end
//! through a `Datastore` with real SurrealQL.
//!
//! The invariant under test: when the planner caps the scan beneath an
//! `ORDER BY id ASC` (see `GraphEdgeScan::limit_needs_id_order`), the
//! result is identical to the full-scan-then-sort answer, in every
//! adjacency state a scope can be in — plain per-edge keys, a live inline
//! cache (whose entries are stored in write order, not id order), folded
//! blocks (plain and numeric), a folded scope with live and tombstoned
//! tail entries, and lightweight relations. Edges are RELATEd in shuffled
//! id order throughout so that any path that caps in write order rather
//! than id order returns the wrong rows.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;
use surrealdb_types::{Value, Variables};

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::Session;
use crate::expr::Dir;
use crate::idx::adjacency::fold_scope;
use crate::kvs::Datastore;
use crate::val::RecordId;

async fn run(ds: &Datastore, ses: &Session, sql: &str) -> Vec<Value> {
	ds.execute(sql, ses, None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.unwrap())
		.collect()
}

/// Runs a single statement with bound variables and returns its result.
async fn run_with_vars(ds: &Datastore, ses: &Session, sql: &str, vars: Variables) -> Value {
	let mut res = ds.execute(sql, ses, Some(vars)).await.unwrap();
	assert_eq!(res.len(), 1, "expected a single statement: {sql}");
	res.remove(0).result.unwrap()
}

/// Runs a single statement and returns its error message.
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

/// Seeds the graph. Every RELATE names its edge id explicitly and runs in
/// shuffled id order, so storage layers that keep write order (the inline
/// cache) disagree with id order.
async fn seed(ds: &Datastore, ses: &Session) {
	run(
		ds,
		ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE cached_person SCHEMALESS INLINE EDGES 32;
		 DEFINE TABLE knows TYPE RELATION;
		 DEFINE TABLE likes TYPE RELATION;
		 DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
		 CREATE |person:1..=9| RETURN NONE;
		 CREATE person:50 RETURN NONE;
		 CREATE cached_person:1 RETURN NONE;
		 RELATE person:1->knows->person:7 SET id = knows:17 RETURN NONE;
		 RELATE person:1->knows->person:3 SET id = knows:13 RETURN NONE;
		 RELATE person:1->knows->person:9 SET id = knows:19 RETURN NONE;
		 RELATE person:1->knows->person:2 SET id = knows:12 RETURN NONE;
		 RELATE person:1->knows->person:8 SET id = knows:18 RETURN NONE;
		 RELATE person:1->knows->person:5 SET id = knows:15 RETURN NONE;
		 RELATE person:1->knows->person:4 SET id = knows:14 RETURN NONE;
		 RELATE person:1->knows->person:6 SET id = knows:16 RETURN NONE;
		 RELATE person:1->likes->person:3 SET id = likes:31 RETURN NONE;
		 RELATE person:1->likes->person:2 SET id = likes:30 RETURN NONE;
		 RELATE person:1->likes->person:4 SET id = likes:32 RETURN NONE;
		 RELATE person:2->knows->person:50 SET id = knows:41 RETURN NONE;
		 RELATE person:4->knows->person:50 SET id = knows:43 RETURN NONE;
		 RELATE person:3->knows->person:50 SET id = knows:42 RETURN NONE;
		 RELATE cached_person:1->knows->person:3 SET id = knows:23 RETURN NONE;
		 RELATE cached_person:1->knows->person:1 SET id = knows:21 RETURN NONE;
		 RELATE cached_person:1->knows->person:4 SET id = knows:24 RETURN NONE;
		 RELATE cached_person:1->knows->person:2 SET id = knows:22 RETURN NONE;
		 RELATE cached_person:1->knows->person:5 SET id = knows:25 RETURN NONE;
		 RELATE person:1->follows->person:4 RETURN NONE;
		 RELATE person:1->follows->person:2 RETURN NONE;
		 RELATE person:1->follows->person:3 RETURN NONE;",
	)
	.await;
}

/// Asserts every ordered-limit query against its literal expectation, and —
/// because a bug that miscapped could in principle also corrupt a literal
/// oracle's shape — against the same lookup with the LIMIT and START lifted
/// out of the parentheses, truncated after the full sort.
async fn assert_cases(ds: &Datastore, ses: &Session, cases: &[(&str, &str)]) {
	for (query, expected) in cases {
		let got = run(ds, ses, query).await.remove(0);
		assert_eq!(got, run(ds, ses, &format!("RETURN {expected};")).await.remove(0), "`{query}`");
	}
}

/// The core battery: shapes eligible for the ordered cap, shapes that must
/// decline it, and combinations with predicates — each against a literal
/// expectation. Run identically in every adjacency state.
fn eligible_cases() -> Vec<(&'static str, &'static str)> {
	vec![
		// Single table, ascending id: capped at the scan.
		(
			"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 3).id FROM ONLY person:1;",
			"[knows:12, knows:13, knows:14]",
		),
		// START folds into the cap.
		(
			"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 3 START 2).id FROM ONLY person:1;",
			"[knows:14, knows:15, knows:16]",
		),
		// A LIMIT past the degree returns everything, sorted.
		(
			"SELECT VALUE ->(SELECT id FROM likes ORDER BY id LIMIT 64).id FROM ONLY person:1;",
			"[likes:30, likes:31, likes:32]",
		),
		// The inverse direction is one scope too.
		(
			"SELECT VALUE <-(SELECT id FROM knows ORDER BY id LIMIT 2).id FROM ONLY person:50;",
			"[knows:41, knows:42]",
		),
		// A WHERE in the pipeline form is not pushed into the scan, so
		// the cap must decline; the filtered sort still answers.
		(
			"SELECT VALUE ->(SELECT id FROM knows WHERE id > knows:13 ORDER BY id LIMIT 2).id FROM ONLY person:1;",
			"[knows:14, knows:15]",
		),
		// DESC needs a reverse scan the storage cannot do: declined, and
		// the full sort must still answer correctly.
		(
			"SELECT VALUE ->(SELECT id FROM knows ORDER BY id DESC LIMIT 2).id FROM ONLY person:1;",
			"[knows:19, knows:18]",
		),
		// Multiple edge tables concatenate separately-sorted scopes:
		// declined.
		(
			"SELECT VALUE ->(SELECT id FROM knows, likes ORDER BY id LIMIT 9).id FROM ONLY person:1;",
			"[knows:12, knows:13, knows:14, knows:15, knows:16, knows:17, knows:18, knows:19, likes:30]",
		),
		// `<->` concatenates two directions: declined.
		(
			"SELECT VALUE <->(SELECT id FROM knows ORDER BY id LIMIT 2).id FROM ONLY person:50;",
			"[knows:41, knows:42]",
		),
		// A non-id key cannot use scan order: declined.
		(
			"SELECT VALUE ->(SELECT id, out FROM knows ORDER BY out LIMIT 2).out FROM ONLY person:1;",
			"[person:2, person:3]",
		),
	]
}

/// The inline cache stores entries in write order; the seed relates them
/// shuffled, so these answer correctly only if the cache fast path sorts
/// before the cap applies.
fn cached_cases() -> Vec<(&'static str, &'static str)> {
	vec![
		(
			"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 2).id FROM ONLY cached_person:1;",
			"[knows:21, knows:22]",
		),
		(
			"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 2 START 1).id FROM ONLY cached_person:1;",
			"[knows:22, knows:23]",
		),
	]
}

/// Lightweight edge ids are synthesized composites, so the expectation is
/// computed rather than literal: the capped lookup must equal the uncapped
/// sorted lookup's prefix.
async fn assert_lightweight(ds: &Datastore, ses: &Session) {
	let full =
		run(ds, ses, "SELECT VALUE ->(SELECT id FROM follows ORDER BY id).id FROM ONLY person:1;")
			.await;
	let capped = run(
		ds,
		ses,
		"SELECT VALUE ->(SELECT id FROM follows ORDER BY id LIMIT 2).id FROM ONLY person:1;",
	)
	.await;
	let Value::Array(full) = full.into_iter().next().unwrap() else {
		panic!("full lookup did not return an array")
	};
	let Value::Array(capped) = capped.into_iter().next().unwrap() else {
		panic!("capped lookup did not return an array")
	};
	assert_eq!(full.len(), 3, "lightweight seed degree");
	assert_eq!(capped.as_slice(), &full.as_slice()[..2], "lightweight ordered cap");
}

async fn full_battery(ds: &Datastore, ses: &Session) {
	assert_cases(ds, ses, &eligible_cases()).await;
	assert_cases(ds, ses, &cached_cases()).await;
	assert_lightweight(ds, ses).await;
}

/// One graph, every adjacency state: the same ordered-limit questions must
/// answer identically as the storage migrates from plain keys through
/// caches, folds, tails and tombstones.
#[tokio::test]
async fn ordered_limits_are_exact_in_every_adjacency_state() {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	// Plain per-edge keys (and the live inline cache).
	full_battery(&ds, &ses).await;

	// Folded blocks with a clean tail.
	fold_vertex(&ds, &RecordId::new("person".into(), 1)).await;
	fold_vertex(&ds, &RecordId::new("person".into(), 50)).await;
	full_battery(&ds, &ses).await;

	// A live tail entry sorting before every folded entry: the cap must
	// take it first, from the merge.
	run(&ds, &ses, "RELATE person:1->knows->person:9 SET id = knows:11 RETURN NONE;").await;
	assert_cases(
		&ds,
		&ses,
		&[(
			"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 3).id FROM ONLY person:1;",
			"[knows:11, knows:12, knows:13]",
		)],
	)
	.await;

	// A tombstoned entry must not consume the cap.
	run(&ds, &ses, "DELETE knows:12;").await;
	assert_cases(
		&ds,
		&ses,
		&[(
			"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 3).id FROM ONLY person:1;",
			"[knows:11, knows:13, knows:14]",
		)],
	)
	.await;
}

/// The same battery with numeric block compression: folded chunks hold
/// doc-ids assigned in fold order, but chunk entries stay in pointer-key
/// order, so the cap must still answer in id order.
#[tokio::test]
async fn ordered_limits_are_exact_with_numeric_blocks() {
	use surrealdb_cnf::ConfigMap;
	let ds = Datastore::builder()
		.without_maintenance_tasks()
		.with_config(ConfigMap::empty().with_key_value("graph_numeric_ids", "true"))
		.build_with_path("memory")
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	fold_vertex(&ds, &RecordId::new("person".into(), 1)).await;
	fold_vertex(&ds, &RecordId::new("person".into(), 50)).await;
	full_battery(&ds, &ses).await;
}

/// The plan itself: the cap and its ordering contract are visible in
/// EXPLAIN for an eligible shape, and absent for shapes that must decline.
#[tokio::test]
async fn the_ordered_cap_is_visible_in_explain() {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	let eligible = run(
		&ds,
		&ses,
		"EXPLAIN SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 3).id FROM ONLY person:1;",
	)
	.await
	.remove(0);
	let eligible = surrealdb_types::ToSql::to_sql(&eligible);
	assert!(eligible.contains("order_pushdown"), "expected the cap in: {eligible}");

	for declined in [
		"EXPLAIN SELECT VALUE ->(SELECT id FROM knows ORDER BY id DESC LIMIT 3).id FROM ONLY person:1;",
		"EXPLAIN SELECT VALUE ->(SELECT id FROM knows, likes ORDER BY id LIMIT 3).id FROM ONLY person:1;",
		"EXPLAIN SELECT VALUE <->(SELECT id FROM knows ORDER BY id LIMIT 3).id FROM ONLY person:50;",
		"EXPLAIN SELECT VALUE ->(SELECT id, out FROM knows ORDER BY out LIMIT 3).out FROM ONLY person:1;",
		// An alias renaming `out` to the literal name `id` shadows the
		// source id: declined, regardless of what the aliased values
		// happen to sort to in this seed.
		"EXPLAIN SELECT VALUE ->(SELECT out AS id FROM knows ORDER BY id LIMIT 3).id FROM ONLY person:1;",
	] {
		let plan = surrealdb_types::ToSql::to_sql(&run(&ds, &ses, declined).await.remove(0));
		assert!(!plan.contains("order_pushdown"), "unexpected cap in `{declined}`: {plan}");
	}
}

/// A parameterised START must narrow the cap by its real (runtime) value,
/// not fall back to zero: capping two rows short of a `START $skip` would
/// silently discard the rows the real skip should have produced.
#[tokio::test]
async fn ordered_limit_with_parameterised_start_matches_literal_start() {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	let mut vars = Variables::new();
	vars.insert("skip", 2i64);
	let got = run_with_vars(
		&ds,
		&ses,
		"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 3 START $skip).id FROM ONLY person:1;",
		vars,
	)
	.await;
	// Same window as the literal `START 2` case in `eligible_cases`.
	let expected = run(&ds, &ses, "RETURN [knows:14, knows:15, knows:16];").await.remove(0);
	assert_eq!(got, expected);
}

/// A `SELECT` alias that renames a different field to the literal name
/// `id` shadows the source id for `ORDER BY id` (`resolve_order_by_alias`),
/// so the cap must decline and the full sort must answer by the aliased
/// field. Edge ids run opposite to `out` here, so a cap that mistakenly
/// keeps scan (edge-id) order returns the wrong prefix.
#[tokio::test]
async fn ordered_limit_declines_when_alias_shadows_id() {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE knows TYPE RELATION;
		 CREATE |person:1..=4| RETURN NONE;
		 RELATE person:1->knows->person:2 SET id = knows:19 RETURN NONE;
		 RELATE person:1->knows->person:3 SET id = knows:18 RETURN NONE;
		 RELATE person:1->knows->person:4 SET id = knows:17 RETURN NONE;",
	)
	.await;

	// True answer: sorted by `out` ascending (person:2, person:3, person:4),
	// take 2 -> [person:2, person:3]. A cap that wrongly keeps ascending
	// edge-id order instead would keep edges 17/18 (person:4, person:3)
	// and drop edge 19 (person:2), answering [person:3, person:4].
	let got = run(
		&ds,
		&ses,
		"SELECT VALUE ->(SELECT out AS id FROM knows ORDER BY id LIMIT 2).id FROM ONLY person:1;",
	)
	.await
	.remove(0);
	let expected = run(&ds, &ses, "RETURN [person:2, person:3];").await.remove(0);
	assert_eq!(got, expected);
}

/// A multi-source lookup — the final hop of a fused chain receives every
/// mid-hop target in one scan batch — takes the concurrent fan-out, which
/// the single-source cases above never reach. The pushed cap is per source,
/// so each fan-out scan must stop at its own budget in ascending edge-id
/// order: the capped answer must equal the full-sort answer over the union
/// of every source's uncapped adjacency.
#[tokio::test]
async fn ordered_limits_are_exact_across_the_multi_source_fanout() {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	// Three mid-hop sources, each with several likes edges related in
	// shuffled id order.
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE knows TYPE RELATION;
		 DEFINE TABLE likes TYPE RELATION;
		 CREATE |person:1..=9| RETURN NONE;
		 RELATE person:1->knows->person:2 SET id = knows:12 RETURN NONE;
		 RELATE person:1->knows->person:3 SET id = knows:13 RETURN NONE;
		 RELATE person:1->knows->person:4 SET id = knows:14 RETURN NONE;
		 RELATE person:2->likes->person:5 SET id = likes:25 RETURN NONE;
		 RELATE person:2->likes->person:6 SET id = likes:21 RETURN NONE;
		 RELATE person:2->likes->person:7 SET id = likes:23 RETURN NONE;
		 RELATE person:3->likes->person:5 SET id = likes:34 RETURN NONE;
		 RELATE person:3->likes->person:6 SET id = likes:31 RETURN NONE;
		 RELATE person:3->likes->person:8 SET id = likes:32 RETURN NONE;
		 RELATE person:4->likes->person:5 SET id = likes:46 RETURN NONE;
		 RELATE person:4->likes->person:9 SET id = likes:41 RETURN NONE;
		 RELATE person:4->likes->person:6 SET id = likes:44 RETURN NONE;",
	)
	.await;

	assert_cases(
		&ds,
		&ses,
		&[
			// A cap above every per-source degree keeps all rows: the global
			// answer spans two sources, so a cap wrongly shared across the
			// fan-out (rather than per source) would drop likes:31.
			(
				"SELECT VALUE ->knows->person->(SELECT id FROM likes ORDER BY id LIMIT 4).id FROM ONLY person:1;",
				"[likes:21, likes:23, likes:25, likes:31]",
			),
			// A biting cap: every source is trimmed to its own two lowest
			// edge ids before the downstream sort keeps the global two.
			(
				"SELECT VALUE ->knows->person->(SELECT id FROM likes ORDER BY id LIMIT 2).id FROM ONLY person:1;",
				"[likes:21, likes:23]",
			),
			// START folds into each source's budget.
			(
				"SELECT VALUE ->knows->person->(SELECT id FROM likes ORDER BY id LIMIT 3 START 2).id FROM ONLY person:1;",
				"[likes:25, likes:31, likes:32]",
			),
		],
	)
	.await;

	// The same answers must hold over folded adjacency, where the fan-out
	// reads merged block entries instead of plain pointer keys.
	for n in 2..=4 {
		fold_vertex(&ds, &RecordId::new("person".into(), n)).await;
	}
	assert_cases(
		&ds,
		&ses,
		&[(
			"SELECT VALUE ->knows->person->(SELECT id FROM likes ORDER BY id LIMIT 2).id FROM ONLY person:1;",
			"[likes:21, likes:23]",
		)],
	)
	.await;
}

/// A session whose permission checks are live must not get the pushed cap
/// on an edge table with a conditional SELECT permission: without a pushed
/// predicate the scan counts rows toward the cap before the batched
/// resolve applies permissions, so a hidden edge would consume budget and
/// the lookup would return fewer than `n` visible rows. The planner
/// declines the push — ordered or plain — leaving the full scan and the
/// downstream sort/limit to answer.
#[tokio::test]
async fn permission_checked_sessions_decline_the_scan_cap() {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let owner = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&owner,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE knows TYPE RELATION PERMISSIONS FOR select WHERE public = true;
		 CREATE person:1, person:2, person:3 RETURN NONE;
		 RELATE person:1->knows->person:2 SET id = knows:1, public = false RETURN NONE;
		 RELATE person:1->knows->person:3 SET id = knows:2, public = true RETURN NONE;",
	)
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

	// The hidden lowest-id edge must not consume the cap: the visible edge
	// is the whole answer for the ordered and the plain LIMIT shape alike.
	assert_cases(
		&ds,
		&user,
		&[
			(
				"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 1).id FROM ONLY person:1;",
				"[knows:2]",
			),
			("SELECT VALUE ->(knows LIMIT 1) FROM ONLY person:1;", "[knows:2]"),
			(
				"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 2).id FROM ONLY person:1;",
				"[knows:2]",
			),
		],
	)
	.await;

	// The decline is visible in the plan: no cap, ordered or otherwise, on
	// the conditionally-visible edge table.
	let plan = surrealdb_types::ToSql::to_sql(
		&run(
			&ds,
			&user,
			"EXPLAIN SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 1).id FROM ONLY person:1;",
		)
		.await
		.remove(0),
	);
	assert!(!plan.contains("order_pushdown"), "unexpected cap under live permissions: {plan}");
}

/// A negative LIMIT or START must decline the scan cap rather than compute
/// on it: casting a negative literal to `usize` wraps to a huge value, and
/// adding the other (non-negative) bound on top overflows `usize::MAX`
/// under the overflow checks this workspace's test builds enable.
/// Declining leaves the normal Limit operator to reject the negative value
/// with its usual runtime error instead.
#[tokio::test]
async fn ordered_limit_rejects_negative_literals_without_panicking() {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("test");
	seed(&ds, &ses).await;

	for query in [
		"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT -1).id FROM ONLY person:1;",
		"SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 3 START -1).id FROM ONLY person:1;",
	] {
		let err = run_err(&ds, &ses, query).await;
		assert!(
			err.contains("non-negative"),
			"expected the non-negative LIMIT/START error for `{query}`, got `{err}`"
		);
	}
}
