//! The pre-decode prefilter of the scan that reads a write's subjects.
//!
//! An `UPDATE` or `DELETE` over a table reads its subjects with a scan whose one reader is the
//! write loop, and the loop's `WHERE` judges each record. These tests hold the scan to a
//! prefilter where that `WHERE` compares a stored field the caller sees as stored, and to none
//! where it reads a `COMPUTED` field or a field with a select permission, and hold every write
//! to the records, the effects and the events the same statement has without one.

use super::row_packed::observed;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// A table `t` of 200 records whose `a` is `0` to `9` twenty times, a table `c` whose `d` is the
/// computed `a * 2`, a table `p` whose `a` no caller may select, and a table `e` whose deletes
/// an event logs into `log`.
const SETUP: &str = "
	FOR $i IN 0..200 { CREATE type::record('t', $i) SET a = $i % 10 };
	DEFINE FIELD a ON c TYPE int;
	DEFINE FIELD d ON c COMPUTED a * 2;
	FOR $i IN 0..200 { CREATE type::record('c', $i) SET a = $i % 10 };
	DEFINE FIELD a ON p TYPE int PERMISSIONS FOR select NONE;
	FOR $i IN 0..200 { CREATE type::record('p', $i) SET a = $i % 10 };
	DEFINE EVENT gone ON e WHEN $event = 'DELETE' THEN { CREATE log SET of = $before.id };
	FOR $i IN 0..200 { CREATE type::record('e', $i) SET a = $i % 10 };
";

/// Runs `sql` as the owner and answers the result of its last statement.
async fn count(db: &TestDb, sql: &str) -> Value {
	let responses = db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
	let mut last = Value::None;
	for response in responses {
		last = response.result.expect("the statement answers").into();
	}
	last
}

/// Runs the write `sql` as the owner, and answers whether every scan it compiled holds a
/// prefilter.
async fn run(db: &TestDb, sql: &str) -> bool {
	observed::reset();
	count(db, sql).await;
	let scans: Vec<_> = observed::pipelines().into_iter().filter(|shape| shape.scan).collect();
	assert!(!scans.is_empty(), "`{sql}` scans a table: {:?}", observed::pipelines());
	scans.iter().all(|shape| shape.prefilter)
}

#[tokio::test]
async fn a_write_over_a_stored_field_prefilters_its_subjects() {
	let db = TestDb::new(SETUP).await;
	let prefiltered = run(&db, "DELETE t WHERE a = 3 RETURN NONE").await;
	assert!(prefiltered, "a DELETE over a stored field prefilters its scan");
	assert_eq!(count(&db, "count(SELECT * FROM t)").await, Value::from(180_i64));
	assert_eq!(count(&db, "count(SELECT * FROM t WHERE a = 3)").await, Value::from(0_i64));

	let prefiltered = run(&db, "UPDATE t SET b = true WHERE a = 4 RETURN NONE").await;
	assert!(prefiltered, "an UPDATE over a stored field prefilters its scan");
	assert_eq!(count(&db, "count(SELECT * FROM t WHERE b = true)").await, Value::from(20_i64));
	assert_eq!(
		count(&db, "count(SELECT * FROM t WHERE b = true AND a != 4)").await,
		Value::from(0_i64)
	);

	let prefiltered = run(&db, "DELETE t WHERE a = 5 OR a = 6 RETURN NONE").await;
	assert!(prefiltered, "a disjunction of stored comparisons prefilters its scan");
	assert_eq!(count(&db, "count(SELECT * FROM t)").await, Value::from(140_i64));
}

#[tokio::test]
async fn a_write_over_a_computed_field_reads_every_subject() {
	let db = TestDb::new(SETUP).await;
	let prefiltered = run(&db, "DELETE c WHERE d = 6 RETURN NONE").await;
	assert!(!prefiltered, "a computed field is not in the stored bytes");
	assert_eq!(count(&db, "count(SELECT * FROM c)").await, Value::from(180_i64));
	assert_eq!(count(&db, "count(SELECT * FROM c WHERE a = 3)").await, Value::from(0_i64));
}

#[tokio::test]
async fn a_write_over_a_permissioned_field_reads_every_subject() {
	let db = TestDb::new(SETUP).await;
	let prefiltered = run(&db, "DELETE p WHERE a = 3 RETURN NONE").await;
	assert!(!prefiltered, "a field with a select permission may be cut from what the WHERE sees");
	assert_eq!(count(&db, "count(SELECT * FROM p)").await, Value::from(180_i64));
}

#[tokio::test]
async fn a_prefiltered_delete_fires_the_events_of_the_records_it_deletes() {
	let db = TestDb::new(SETUP).await;
	let prefiltered = run(&db, "DELETE e WHERE a = 7 RETURN NONE").await;
	assert!(prefiltered, "an event does not stand between the scan and the WHERE");
	assert_eq!(count(&db, "count(SELECT * FROM log)").await, Value::from(20_i64));
	assert_eq!(
		count(&db, "count(SELECT * FROM log WHERE record::id(of) % 10 != 7)").await,
		Value::from(0_i64)
	);
}

/// The records the scans of this thread handed on since the last call, which a prefilter's
/// refusals never reach.
fn scanned() -> usize {
	super::host::RECORDS_SCANNED.with(|scanned| scanned.replace(0))
}

#[tokio::test]
async fn a_write_whose_where_refuses_every_record_decodes_none() {
	for (config, named) in [
		(surrealdb_cnf::ConfigMap::empty(), "the default settings"),
		(super::tests_morsel::tiny(), "morsels"),
	] {
		let db = TestDb::new_with_config(SETUP, config).await;
		for sql in
			["DELETE t WHERE a = -1 RETURN NONE", "UPDATE t SET b = true WHERE a = -1 RETURN NONE"]
		{
			scanned();
			assert!(run(&db, sql).await, "`{sql}` under {named} prefilters its scan");
			assert_eq!(scanned(), 0, "`{sql}` under {named} hands on no record");
		}
		assert_eq!(count(&db, "count(SELECT * FROM t)").await, Value::from(200_i64), "{named}");
		scanned();
		run(&db, "DELETE t WHERE a = 8 RETURN NONE").await;
		assert_eq!(scanned(), 20, "a DELETE under {named} hands on only the records it deletes");
		assert_eq!(count(&db, "count(SELECT * FROM t)").await, Value::from(180_i64), "{named}");
	}
}

#[tokio::test]
async fn an_upsert_whose_where_refuses_every_record_still_creates_its_record() {
	let db = TestDb::new(SETUP).await;
	count(&db, "UPSERT t SET a = -1, made = true WHERE a = -1 RETURN NONE").await;
	assert_eq!(count(&db, "count(SELECT * FROM t WHERE made = true)").await, Value::from(1_i64));
	count(&db, "UPSERT t SET b = true WHERE a = 9 RETURN NONE").await;
	assert_eq!(count(&db, "count(SELECT * FROM t WHERE b = true)").await, Value::from(20_i64));
}

/// A record user of a table whose `delete` and `select` permissions are predicates over `a`.
const GUARDED: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice;
	DEFINE TABLE g SCHEMALESS PERMISSIONS FOR select, delete WHERE a != 3, FOR create, update FULL;
	FOR $i IN 0..200 { CREATE type::record('g', $i) SET a = $i % 10 };
";

#[tokio::test]
async fn a_write_whose_gate_judges_a_predicate_reads_every_subject() {
	let db = TestDb::new(GUARDED).await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
	let alice = crate::dbs::Session::for_record("test", "test", "user", rid);
	observed::reset();
	let responses = db
		.datastore()
		.execute("DELETE g WHERE a = 3 OR a = 4 RETURN NONE", &alice, None)
		.await
		.expect("runs");
	for response in responses {
		response.result.expect("the statement answers");
	}
	let scans: Vec<_> = observed::pipelines().into_iter().filter(|shape| shape.scan).collect();
	assert!(!scans.is_empty() && scans.iter().all(|shape| !shape.prefilter), "{scans:?}");
	// The gate keeps the records whose `a` is 3, and the `WHERE` deletes those whose `a` is 4.
	assert_eq!(count(&db, "count(SELECT * FROM g WHERE a = 3)").await, Value::from(20_i64));
	assert_eq!(count(&db, "count(SELECT * FROM g WHERE a = 4)").await, Value::from(0_i64));
	assert_eq!(count(&db, "count(SELECT * FROM g)").await, Value::from(180_i64));
	// The owner's write over the same table plans the gate it never judges, so it reads every
	// subject too.
	assert!(!run(&db, "DELETE g WHERE a = 5 RETURN NONE").await);
	assert_eq!(count(&db, "count(SELECT * FROM g)").await, Value::from(160_i64));
}

#[tokio::test]
async fn a_write_without_a_where_is_not_judged_by_the_where_of_a_write_it_runs() {
	let db = TestDb::new(SETUP).await;
	count(&db, "UPDATE t SET b = (DELETE c WHERE a = 3 RETURN BEFORE).len() RETURN NONE").await;
	assert_eq!(count(&db, "count(SELECT * FROM t WHERE b != NONE)").await, Value::from(200_i64));
	assert_eq!(count(&db, "count(SELECT * FROM c)").await, Value::from(180_i64));
}
