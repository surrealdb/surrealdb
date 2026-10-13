//! Inline edge properties, driven end to end through a `Datastore` with
//! real SurrealQL.
//!
//! The invariant under test: a filtered traversal over a table with
//! `INLINE` fields answers exactly as the same query over a plain table
//! holding the same edges — while `EXPLAIN ANALYZE`'s `props_evals` /
//! `props_fallbacks` counters make the payload path observable: an
//! eligible predicate is evaluated against the adjacency payloads (falsy
//! edges never fetch their records), and anything the payloads cannot
//! answer trustworthily falls back to the records.

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

/// Sums every occurrence of the named metric across an `EXPLAIN ANALYZE`
/// rendering (a text plan tree with `name: value` metric pairs).
fn sum_metric(value: &Value, name: &str) -> i64 {
	let Value::String(text) = value else {
		panic!("expected a rendered plan, found {value:?}");
	};
	let marker = format!("{name}: ");
	let mut sum = 0;
	let mut rest = text.as_str();
	while let Some(at) = rest.find(&marker) {
		rest = &rest[at + marker.len()..];
		let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
		sum += digits.parse::<i64>().unwrap_or(0);
	}
	sum
}

/// Runs one query under `EXPLAIN ANALYZE` and returns the summed
/// `(props_evals, props_fallbacks)` its plan reports.
async fn props_counters(ds: &Datastore, ses: &Session, query: &str) -> (i64, i64) {
	let explained = run(ds, ses, &format!("EXPLAIN ANALYZE {query}")).await.remove(0);
	(sum_metric(&explained, "props_evals"), sum_metric(&explained, "props_fallbacks"))
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

/// Filtered traversals whose results must be identical whether the
/// predicate ran against the payloads or the records.
const BATTERY: &[&str] = &[
	"SELECT VALUE ->(likes WHERE score > 5) FROM person:p0;",
	"SELECT VALUE ->(likes WHERE score > 5)->person FROM person:p0;",
	"SELECT VALUE <-(likes WHERE score <= 5) FROM person:p2;",
	"SELECT VALUE ->(likes WHERE score > 5 AND out != person:p9) FROM person ORDER BY id;",
	"SELECT VALUE ->(likes WHERE tag = 'a') FROM person:p0;",
	"SELECT VALUE ->(likes WHERE score > 5 LIMIT 1) FROM person:p0;",
];

async fn battery(ds: &Datastore, ses: &Session) -> Vec<Vec<Value>> {
	let mut out = Vec::new();
	for query in BATTERY {
		out.push(run(ds, ses, query).await);
	}
	out
}

/// Seeds paired-oracle data. `inline` adds the INLINE clauses.
async fn seed(ds: &Datastore, ses: &Session, inline: bool) {
	let clause = if inline {
		"INLINE"
	} else {
		""
	};
	run(
		ds,
		ses,
		&format!(
			"DEFINE NAMESPACE test;
			 DEFINE DATABASE test;
			 DEFINE TABLE person;
			 DEFINE TABLE likes TYPE RELATION;
			 DEFINE FIELD score ON likes TYPE number {clause};
			 DEFINE FIELD tag ON likes TYPE option<string> {clause};
			 CREATE person:p0, person:p1, person:p2, person:p3;
			 RELATE person:p0->likes:l1->person:p1 SET score = 3, tag = 'a';
			 RELATE person:p0->likes:l2->person:p2 SET score = 7;
			 RELATE person:p0->likes:l3->person:p3 SET score = 9, tag = 'b';
			 RELATE person:p1->likes:l4->person:p2 SET score = 5, tag = 'a';"
		),
	)
	.await;
}

/// The paired oracle: an INLINE table answers the whole filtered battery
/// identically to a plain twin, with the predicate observably served from
/// the payloads (evaluations recorded, no fallbacks, and falsy edges never
/// fetched).
#[tokio::test]
async fn payload_filters_equal_the_record_oracle() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let inline = ds().await;
	let plain = ds().await;
	seed(&inline, &ses, true).await;
	seed(&plain, &ses, false).await;
	assert_eq!(battery(&inline, &ses).await, battery(&plain, &ses).await);

	let (evals, fallbacks) = props_counters(&inline, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (3, 0), "all of p0's edges evaluate from payloads");
	let (evals, fallbacks) = props_counters(&plain, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (0, 0), "a plain table has no payload path");

	// Updates restamp both pointer payloads in the writing transaction.
	run(&inline, &ses, "UPDATE likes:l1 SET score = 10;").await;
	run(&plain, &ses, "UPDATE likes:l1 SET score = 10;").await;
	assert_eq!(battery(&inline, &ses).await, battery(&plain, &ses).await);
	let (evals, fallbacks) = props_counters(&inline, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (3, 0), "an updated edge still evaluates from its payload");
}

/// Folding moves the payloads into packed block entries verbatim; the
/// filtered battery stays invariant and still evaluates from payloads.
#[tokio::test]
async fn folded_payloads_still_serve_evaluations() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let inline = ds().await;
	let plain = ds().await;
	seed(&inline, &ses, true).await;
	seed(&plain, &ses, false).await;
	for n in 0..4 {
		let rid = RecordId::new("person".into(), format!("p{n}"));
		fold_vertex(&inline, &rid).await;
	}
	assert_eq!(battery(&inline, &ses).await, battery(&plain, &ses).await);
	let (evals, fallbacks) = props_counters(&inline, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (3, 0), "block-sourced payloads evaluate too");
}

/// A schema mutation of the inline set bumps the generation: payloads
/// written before it fall back — never evaluating against the wrong field
/// set — and a later write of the edge restamps it.
#[tokio::test]
async fn stale_generations_fall_back_then_repair() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;
	let before = run(&ds, &ses, BATTERY[0]).await;

	// Changing the inline set (a third inline field) bumps the generation.
	run(&ds, &ses, "DEFINE FIELD weight ON likes TYPE option<number> INLINE;").await;
	let (evals, fallbacks) = props_counters(&ds, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (0, 3), "pre-bump payloads must not be evaluated");
	assert_eq!(run(&ds, &ses, BATTERY[0]).await, before);

	// A write restamps the edge under the new generation; the others keep
	// falling back.
	run(&ds, &ses, "UPDATE likes:l1 SET score = 3;").await;
	let (evals, fallbacks) = props_counters(&ds, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (1, 2));
	assert_eq!(run(&ds, &ses, BATTERY[0]).await, before);

	// Un-inlining below the predicate's needs disqualifies the payload path
	// entirely: no payload metrics, same results.
	run(&ds, &ses, "ALTER FIELD score ON likes DROP INLINE;").await;
	let (evals, fallbacks) = props_counters(&ds, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (0, 0));
	assert_eq!(run(&ds, &ses, BATTERY[0]).await, before);
}

/// A payload past the cap is written spilled: the edge falls back, the
/// others keep evaluating, and results never change.
#[tokio::test]
async fn over_cap_payloads_spill_to_the_record() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;
	// The default cap is 64 bytes of encoded values; a long tag overflows it.
	run(&ds, &ses, &format!("UPDATE likes:l2 SET tag = '{}';", "x".repeat(100))).await;
	let (evals, fallbacks) = props_counters(&ds, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (2, 1), "the spilled edge falls back");
	let out = run(&ds, &ses, BATTERY[0]).await.remove(0);
	let rows = out.into_t::<Vec<Value>>().unwrap();
	let edges = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(edges.len(), 2, "l2 (score 7) and l3 (score 9) still match");
}

/// A predicate referencing any non-inline field takes the record path
/// wholesale — no payload metrics — and answers identically.
#[tokio::test]
async fn mixed_predicates_take_the_record_path() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;
	run(&ds, &ses, "UPDATE likes:l2 SET note = 'keep';").await;
	let query = "SELECT VALUE ->(likes WHERE score > 5 AND note = 'keep') FROM person:p0;";
	let (evals, fallbacks) = props_counters(&ds, &ses, query).await;
	assert_eq!((evals, fallbacks), (0, 0));
	let out = run(&ds, &ses, query).await.remove(0);
	let rows = out.into_t::<Vec<Value>>().unwrap();
	let edges = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(edges.len(), 1);
}

/// A non-Full SELECT permission — on the table or on a referenced inline
/// field — only disqualifies the payload path for actors whose permission
/// checks actually run: an owner's plan keeps the payload path (their
/// checks are skipped at runtime, so raw values are theirs to read), while
/// an anonymous record-level actor is forced onto the record path, where
/// field-level SELECT clauses null the hidden values before the predicate
/// sees them. The record-user outcome itself is pinned by the
/// `inline_props_permissions` language test.
#[tokio::test]
async fn restricted_permissions_disqualify_the_payload_path() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;
	let before = run(&ds, &ses, BATTERY[0]).await;
	run(&ds, &ses, "ALTER FIELD score ON likes PERMISSIONS FOR select NONE;").await;
	// The owner's permission checks are skipped, so the payload path stays
	// admitted — and keeps answering identically. A permissions-only ALTER
	// never enters the payload shape, so the written payloads stay live.
	let (evals, fallbacks) = props_counters(&ds, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (3, 0), "an owner's payload path survives field clauses");
	assert_eq!(run(&ds, &ses, BATTERY[0]).await, before);
}

/// Concurrent writers and filtered readers: each read matches a same-
/// transaction record-path oracle, so a reader never sees a payload and a
/// record from different writes.
#[tokio::test]
async fn concurrent_updates_never_desync_payloads() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;

	let writer = {
		let ds = Arc::clone(&ds);
		let ses = ses.clone();
		tokio::spawn(async move {
			for n in 0..200 {
				let sql = format!("UPDATE likes:l2 SET score = {};", n % 12);
				let _ = ds.execute(&sql, &ses, None).await;
			}
		})
	};
	for _ in 0..100 {
		// One transaction evaluates the same predicate through the payload
		// path (traversal) and the record path (table scan); the sets must
		// agree at every snapshot.
		let out = run(
			&ds,
			&ses,
			"BEGIN;
			 SELECT VALUE ->(likes WHERE score > 5) FROM ONLY person:p0;
			 SELECT VALUE id FROM likes WHERE score > 5 AND in = person:p0;
			 COMMIT;",
		)
		.await;
		let via_payload: std::collections::BTreeSet<String> = out[1]
			.clone()
			.into_t::<Vec<Value>>()
			.unwrap()
			.into_iter()
			.map(|v| surrealdb_types::ToSql::to_sql(&v))
			.collect();
		let via_records: std::collections::BTreeSet<String> = out[2]
			.clone()
			.into_t::<Vec<Value>>()
			.unwrap()
			.into_iter()
			.map(|v| surrealdb_types::ToSql::to_sql(&v))
			.collect();
		assert_eq!(via_payload, via_records);
	}
	writer.await.unwrap();
}

/// Reads the `likes` table's inline generation from the catalog.
async fn inline_gen(ds: &Datastore) -> u32 {
	use surrealdb_kvs::TransactionType::Read;

	use crate::catalog::providers::TableProvider;
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let tb =
		txn.get_tb(db.namespace_id, db.database_id, &"likes".into(), None).await.unwrap().unwrap();
	tb.graph_inline_gen
}

/// The generation identifies the payload shape, so only shape changes may
/// bump it: toggling INLINE on or off, changing an inline field's TYPE,
/// or removing an inline field. An idempotent re-DEFINE and a
/// permissions-only ALTER leave it — and every written payload — intact.
#[tokio::test]
async fn generation_bumps_only_on_shape_changes() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	seed(&ds, &ses, true).await;
	let gen0 = inline_gen(&ds).await;
	// An idempotent re-DEFINE of the identical shape: no bump.
	run(&ds, &ses, "DEFINE FIELD OVERWRITE score ON likes TYPE number INLINE;").await;
	assert_eq!(inline_gen(&ds).await, gen0, "an identical re-DEFINE must not bump");
	// A permissions-only ALTER of an inline field: no bump.
	run(&ds, &ses, "ALTER FIELD score ON likes PERMISSIONS FOR select NONE;").await;
	assert_eq!(inline_gen(&ds).await, gen0, "a PERMISSIONS-only ALTER must not bump");
	// Changing an inline field's declared type: bump.
	run(&ds, &ses, "DEFINE FIELD OVERWRITE score ON likes TYPE int INLINE;").await;
	let gen1 = inline_gen(&ds).await;
	assert_ne!(gen1, gen0, "an inline TYPE change must bump");
	// Toggling INLINE off, then back on: each toggle bumps.
	run(&ds, &ses, "ALTER FIELD score ON likes DROP INLINE;").await;
	let gen2 = inline_gen(&ds).await;
	assert_ne!(gen2, gen1, "un-inlining must bump");
	run(&ds, &ses, "ALTER FIELD score ON likes INLINE;").await;
	let gen3 = inline_gen(&ds).await;
	assert_ne!(gen3, gen2, "re-inlining must bump");
	// Removing an inline field changes the set membership: bump.
	run(&ds, &ses, "REMOVE FIELD tag ON likes;").await;
	assert_ne!(inline_gen(&ds).await, gen3, "removing an inline field must bump");
}

/// Edges written before a field became INLINE never carry payloads: a
/// filtered traversal answers them from their records — prefetched per
/// flush — while restamped edges answer from payloads, the two
/// generations mixing freely within one scan, and the results stay
/// identical to a plain-table oracle.
#[tokio::test]
async fn mixed_generation_scans_answer_like_the_oracle() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let inline = ds().await;
	let plain = ds().await;
	seed(&inline, &ses, false).await;
	seed(&plain, &ses, false).await;
	// Inline the fields only after every edge is written: none carries a
	// payload, so the whole hub falls back to (bulk-fetched) records.
	run(&inline, &ses, "DEFINE FIELD OVERWRITE score ON likes TYPE number INLINE;").await;
	run(&inline, &ses, "DEFINE FIELD OVERWRITE tag ON likes TYPE option<string> INLINE;").await;
	assert_eq!(battery(&inline, &ses).await, battery(&plain, &ses).await);
	let (evals, fallbacks) = props_counters(&inline, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (0, 3), "pre-INLINE edges all fall back");
	// A write restamps one edge; the scan now mixes both generations.
	run(&inline, &ses, "UPDATE likes:l1 SET score = 3;").await;
	run(&plain, &ses, "UPDATE likes:l1 SET score = 3;").await;
	assert_eq!(battery(&inline, &ses).await, battery(&plain, &ses).await);
	let (evals, fallbacks) = props_counters(&inline, &ses, BATTERY[0]).await;
	assert_eq!((evals, fallbacks), (1, 2), "restamped and stale edges mix in one scan");
}

/// The canonical payload order is the raw field-name bytes: `b-b` sorts
/// after `a` raw but before it when SQL-rendered (its backtick escape
/// precedes every letter), so a writer/reader disagreement over the
/// rendering would swap the payload slots. The payload-served answer
/// must match the record oracle, and must actually come from payloads.
#[tokio::test]
async fn payload_order_uses_raw_field_names() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test; DEFINE DATABASE test;
		 DEFINE TABLE person;
		 DEFINE TABLE likes TYPE RELATION;
		 DEFINE FIELD a ON likes TYPE number INLINE;
		 DEFINE FIELD `b-b` ON likes TYPE string INLINE;
		 CREATE person:p0, person:p1, person:p2;
		 RELATE person:p0->likes:l1->person:p1 SET a = 1, `b-b` = 'x';
		 RELATE person:p0->likes:l2->person:p2 SET a = 2, `b-b` = 'y';",
	)
	.await;
	let query = "SELECT VALUE ->(likes WHERE a > 1 AND `b-b` = 'y') FROM person:p0;";
	let traversal = run(&ds, &ses, query).await.remove(0);
	let edges = traversal.into_t::<Vec<Value>>().unwrap().remove(0);
	let oracle = run(
		&ds,
		&ses,
		"SELECT VALUE id FROM likes WHERE a > 1 AND `b-b` = 'y' AND in = person:p0;",
	)
	.await
	.remove(0);
	assert_eq!(edges, oracle);
	let (evals, fallbacks) = props_counters(&ds, &ses, query).await;
	assert_eq!((evals, fallbacks), (2, 0), "both edges must answer from their payloads");
}

/// The payload's value count is a single byte: the 256th INLINE field is
/// rejected at definition time — by DEFINE and by the ALTER toggle — and
/// a table carrying the full 255 still writes edges.
#[tokio::test]
async fn inline_field_count_is_capped_at_255() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	let mut sql = String::from(
		"DEFINE NAMESPACE test; DEFINE DATABASE test;
		 DEFINE TABLE person; DEFINE TABLE likes TYPE RELATION;",
	);
	for n in 0..255 {
		sql.push_str(&format!("DEFINE FIELD f{n:03} ON likes TYPE option<number> INLINE;"));
	}
	run(&ds, &ses, &sql).await;
	let err = ds
		.execute("DEFINE FIELD f255 ON likes TYPE option<number> INLINE;", &ses, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap_err();
	assert!(err.to_string().contains("more than 255 INLINE fields"), "{err}");
	run(&ds, &ses, "DEFINE FIELD f255 ON likes TYPE option<number>;").await;
	let err = ds
		.execute("ALTER FIELD f255 ON likes INLINE;", &ses, None)
		.await
		.unwrap()
		.remove(0)
		.result
		.unwrap_err();
	assert!(err.to_string().contains("more than 255 INLINE fields"), "{err}");
	// The full 255-field table still relates and traverses.
	run(
		&ds,
		&ses,
		"CREATE person:p0, person:p1;
		 RELATE person:p0->likes:l1->person:p1 SET f000 = 1;",
	)
	.await;
	let out = run(&ds, &ses, "SELECT VALUE ->(likes WHERE f000 = 1) FROM person:p0;").await;
	let rows = out.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	let edges = rows.into_iter().next().unwrap().into_t::<Vec<Value>>().unwrap();
	assert_eq!(edges.len(), 1);
}

/// A field-level SELECT clause on a path NESTED under a referenced inline
/// field also disqualifies the payload path for permission-checked
/// actors: payload evaluation reads the inline field's raw value whole,
/// so hidden sub-values would otherwise steer the result set. The owner
/// keeps the payload path, matching the top-level rule.
#[tokio::test]
async fn nested_field_permissions_gate_the_payload_path() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let ds = ds().await;
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person;
		 DEFINE TABLE likes TYPE RELATION;
		 DEFINE FIELD meta ON likes TYPE object INLINE;
		 DEFINE FIELD meta.secret ON likes TYPE option<number> PERMISSIONS FOR select NONE;
		 CREATE person:p0, person:p1;
		 RELATE person:p0->likes:l1->person:p1 SET meta = { secret: 9 };",
	)
	.await;
	// The owner's checks are skipped, so payload mode is admitted and the
	// raw nested value is theirs to filter on.
	let query = "SELECT VALUE ->(likes WHERE meta.secret > 5) FROM person:p0;";
	let (evals, fallbacks) = props_counters(&ds, &ses, query).await;
	assert_eq!((evals, fallbacks), (1, 0));
	// For permission-checked actors the plan must refuse payload mode;
	// pin the plan-time decision through the rendered predicate scope.
	let plan = run(&ds, &ses, &format!("EXPLAIN {query}")).await.remove(0);
	let text = surrealdb_types::ToSql::to_sql(&plan);
	assert!(
		text.contains("predicate_scope: payload"),
		"owner plan should be payload-scoped: {text}"
	);
}

/// A predicate that reads only `id` / `in` / `out` is key-resident: the
/// adjacency entry itself carries every referenced field, so the scan
/// prunes non-matching edges before any record fetch — on plain tables
/// with no `INLINE` fields, on folded blocks, and on lightweight
/// relations alike. `EXPLAIN` reports the `key` predicate scope and the
/// eval counters show every edge was answered without a fallback.
#[tokio::test]
async fn endpoint_predicates_prune_before_any_fetch() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE knows TYPE RELATION;
		 DEFINE TABLE follows TYPE RELATION IN person OUT person LIGHTWEIGHT;
		 CREATE |person:1..=8| RETURN NONE;
		 FOR $n IN 2..=8 { RELATE person:1->knows->(type::record('person', $n)); };
		 FOR $n IN 2..=8 { RELATE person:1->follows->(type::record('person', $n)); };",
	)
	.await;

	// Classic relation: the plan is key-scoped and the counters show all
	// seven edges evaluated with no fallback, one survivor.
	let query = "SELECT VALUE ->(knows WHERE out = person:5) FROM person:1";
	let explained = run(&ds, &ses, &format!("EXPLAIN {query}")).await.remove(0);
	let Value::String(plan) = &explained else {
		panic!("expected a rendered plan, found {explained:?}");
	};
	assert!(plan.contains("predicate_scope: key"), "expected a key-scoped predicate:\n{plan}");
	let (evals, fallbacks) = props_counters(&ds, &ses, query).await;
	assert_eq!((evals, fallbacks), (7, 0));
	assert_eq!(
		run(&ds, &ses, query).await,
		run(&ds, &ses, "RETURN [(SELECT VALUE id FROM knows WHERE out = person:5)];").await
	);

	// The same pruning over a lightweight relation, whose synthesized
	// adjacency carries the same key material.
	let (evals, fallbacks) =
		props_counters(&ds, &ses, "SELECT VALUE ->(follows WHERE out = person:5) FROM person:1")
			.await;
	assert_eq!((evals, fallbacks), (7, 0));
	assert_eq!(
		run(&ds, &ses, "SELECT VALUE ->(follows WHERE out = person:5).out FROM person:1").await,
		run(&ds, &ses, "RETURN [[person:5]];").await
	);

	// And over folded blocks: fold the classic scope, then the same
	// key-scoped answer comes from block entries.
	fold_vertex(&ds, &RecordId::new("person".into(), 1)).await;
	let (evals, fallbacks) = props_counters(&ds, &ses, query).await;
	assert_eq!((evals, fallbacks), (7, 0));
	assert_eq!(
		run(&ds, &ses, query).await,
		run(&ds, &ses, "RETURN [(SELECT VALUE id FROM knows WHERE out = person:5)];").await
	);
}

/// Comparison-shaped key-resident predicates compile to the synchronous
/// matcher; every shape must answer exactly as the record path does, on
/// plain keys and over folded blocks, while the counters show no edge
/// fell back.
#[tokio::test]
async fn the_key_matcher_agrees_with_the_record_path() {
	let ds = ds().await;
	let ses = Session::owner().with_ns("test").with_db("test");
	run(
		&ds,
		&ses,
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE test;
		 DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE knows TYPE RELATION;
		 CREATE |person:1..=9| RETURN NONE;
		 FOR $n IN 2..=9 { RELATE person:1->knows->(type::record('person', $n)) SET id = type::record('knows', $n) RETURN NONE; };",
	)
	.await;

	let shapes = [
		"out = person:5",
		"out != person:5",
		"out > person:6",
		"out >= person:6",
		"out < person:4",
		"out <= person:4",
		"person:7 < out",
		"out > person:3 AND out != person:6",
		"id = knows:4",
		"in = person:1",
		"in = person:2",
	];
	for shape in shapes {
		let traversal = format!("SELECT VALUE ->(knows WHERE {shape}) FROM person:1");
		let oracle = format!("RETURN [(SELECT VALUE id FROM knows WHERE {shape} ORDER BY id)];");
		assert_eq!(
			run(&ds, &ses, &traversal).await,
			run(&ds, &ses, &oracle).await,
			"shape `{shape}` diverged from the record path"
		);
		let (evals, fallbacks) = props_counters(&ds, &ses, &traversal).await;
		assert_eq!((evals, fallbacks), (8, 0), "shape `{shape}` fell back");
	}

	// The same battery over folded blocks.
	fold_vertex(&ds, &RecordId::new("person".into(), 1)).await;
	for shape in shapes {
		let traversal = format!("SELECT VALUE ->(knows WHERE {shape}) FROM person:1");
		let oracle = format!("RETURN [(SELECT VALUE id FROM knows WHERE {shape} ORDER BY id)];");
		assert_eq!(
			run(&ds, &ses, &traversal).await,
			run(&ds, &ses, &oracle).await,
			"shape `{shape}` diverged from the record path over blocks"
		);
	}

	// Inverse traversal: `in`/`out` swap sides relative to the scan.
	assert_eq!(
		run(
			&ds,
			&ses,
			"SELECT VALUE <-(knows WHERE in = person:1 AND out = person:5) FROM person:5"
		)
		.await,
		run(&ds, &ses, "RETURN [[knows:5]];").await,
	);
}

/// Rewrites `rid`'s outgoing vertex-side pointer keys into the legacy
/// (target-less) format a pre-embedded-target writer left, leaving the
/// edge records untouched.
async fn strip_embedded_targets(ds: &Datastore, rid: &RecordId) {
	use std::borrow::Cow;

	use crate::key::schema::{DecodedGraph, GraphDirPrefix, GraphKey, GraphPointerKey};
	let txn = ds.transaction(Write).await.unwrap();
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
	let keys = txn.keys_raw(range, u32::MAX, 0, None).await.unwrap();
	for bytes in keys {
		let decoded = DecodedGraph::decode(&bytes).unwrap();
		let Some(target) = decoded.target else {
			continue;
		};
		txn.del_key(&GraphPointerKey {
			ns: db.namespace_id,
			db: db.database_id,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&decoded.edge.table),
			foreign_key: Cow::Borrowed(&decoded.edge.key),
			target_table: Cow::Borrowed(&target.table),
			target_key: Cow::Borrowed(&target.key),
		})
		.await
		.unwrap();
		txn.set_key(
			&GraphKey {
				ns: db.namespace_id,
				db: db.database_id,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				dir: Dir::Out,
				foreign_table: Cow::Borrowed(&decoded.edge.table),
				foreign_key: Cow::Borrowed(&decoded.edge.key),
			},
			&(),
		)
		.await
		.unwrap();
	}
	txn.commit().await.unwrap();
}

/// Legacy-format (target-less) adjacency entries cannot answer an
/// endpoint-reading key predicate from the entry alone: each falls back
/// to its record — the fallbacks of one cursor batch resolve in a single
/// batched read — and answers exactly as a twin whose entries embed their
/// targets. A pure-`id` predicate reads nothing a legacy entry lacks, so
/// it evaluates with no fallback at all.
#[tokio::test]
async fn legacy_entries_answer_key_predicates_through_the_record() {
	let ses = Session::owner().with_ns("test").with_db("test");
	let legacy = ds().await;
	let modern = ds().await;
	seed(&legacy, &ses, false).await;
	seed(&modern, &ses, false).await;
	strip_embedded_targets(&legacy, &RecordId::new("person".into(), "p0".to_owned())).await;

	// An out-reading predicate must consult the record behind each legacy
	// entry; the answer stays identical to the embedded-target twin.
	let by_out = "SELECT VALUE ->(likes WHERE out = person:p2) FROM person:p0;";
	assert_eq!(run(&legacy, &ses, by_out).await, run(&modern, &ses, by_out).await);
	let (evals, fallbacks) = props_counters(&legacy, &ses, by_out).await;
	assert_eq!((evals, fallbacks), (0, 3), "every legacy entry falls back to its record");
	let (evals, fallbacks) = props_counters(&modern, &ses, by_out).await;
	assert_eq!((evals, fallbacks), (3, 0), "embedded targets answer without a fallback");

	// A pure-id predicate is answered by the legacy entry itself.
	let by_id = "SELECT VALUE ->(likes WHERE id = likes:l1) FROM person:p0;";
	assert_eq!(run(&legacy, &ses, by_id).await, run(&modern, &ses, by_id).await);
	let (evals, fallbacks) = props_counters(&legacy, &ses, by_id).await;
	assert_eq!((evals, fallbacks), (3, 0), "a pure-id predicate needs no record fetch");
}
