//! A run-time index lookup keyed by a field of a record argument (`k = $r.k`) reads the index where
//! every fetch of the record answers that field alike, and the table whole where a fetch may answer
//! it differently: in a read, a computed field, which is derived on each fetch, or a conditional
//! `SELECT` permission a checked caller's fetch judges each time; in a write, any record, which the
//! write may change between the two fetches. A field of an object reads the index in either. An
//! event's body keys the lookup by a path into `$after` as a function body does by a path into its
//! argument.
//!
//! Each test counts the keys a transaction reads while running one statement, on two
//! datastores that differ only in how many records the looked-up table holds. A lookup reads the
//! same keys on both; a read of the table whole reads more on the larger one.

use std::sync::Arc;

use surrealdb_types::ToSql;

use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::TransactionType;
use crate::kvs::request::QueryRequest;

/// The schema: `item` is looked up by `k`; each function keys the lookup by one path into its
/// record argument, and each event by one path into `$after`.
const SCHEMA: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user WHERE id = $id);
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice;
	DEFINE TABLE item SCHEMALESS PERMISSIONS FULL;
	DEFINE INDEX item_k ON item FIELDS k;
	DEFINE TABLE plain SCHEMALESS PERMISSIONS FULL;
	CREATE plain:one SET k = 1;
	DEFINE TABLE derived SCHEMALESS PERMISSIONS FULL;
	DEFINE FIELD k ON derived COMPUTED 1;
	CREATE derived:one;
	DEFINE TABLE cfg SCHEMALESS PERMISSIONS FULL;
	DEFINE FIELD derived ON cfg COMPUTED k;
	DEFINE FIELD nested ON cfg COMPUTED { k: 1 };
	DEFINE FIELD hidden ON cfg PERMISSIONS FOR select WHERE $auth.id = user:alice;
	CREATE cfg:one SET k = 1, hidden = 1, plain = plain:one, derived_link = derived:one;
	DEFINE TABLE gated SCHEMALESS PERMISSIONS FOR select WHERE $auth.id = user:alice;
	CREATE gated:one SET k = 1;
	DEFINE TABLE keyed SCHEMALESS PERMISSIONS FULL;
	DEFINE FIELD id ON keyed TYPE [k: int];
	CREATE keyed SET k = 1;
	DEFINE FUNCTION fn::stored($r: any) { RETURN SELECT VALUE k FROM item WHERE k = $r.k; };
	DEFINE FUNCTION fn::derived($r: any) { RETURN SELECT VALUE k FROM item WHERE k = $r.derived; };
	DEFINE FUNCTION fn::nested($r: any) { RETURN SELECT VALUE k FROM item WHERE k = $r.nested.k; };
	DEFINE FUNCTION fn::hidden($r: any) { RETURN SELECT VALUE k FROM item WHERE k = $r.hidden; };
	DEFINE FUNCTION fn::linked($r: any) { RETURN SELECT VALUE k FROM item WHERE k = $r.plain.k; };
	DEFINE FUNCTION fn::linked_derived($r: any) {
		RETURN SELECT VALUE k FROM item WHERE k = $r.derived_link.k;
	};
	DEFINE FUNCTION fn::bump($r: any) { RETURN UPDATE item SET n = 1 WHERE k = $r.k RETURN VALUE k; };
	DEFINE TABLE ev_log SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE ev_read SCHEMALESS PERMISSIONS FULL;
	DEFINE EVENT read ON ev_read WHEN true THEN {
		CREATE ev_log SET k = (SELECT VALUE k FROM item WHERE k = $after.k) RETURN NONE;
	};
	DEFINE TABLE ev_when SCHEMALESS PERMISSIONS FULL;
	DEFINE EVENT read ON ev_when WHEN (SELECT VALUE k FROM item WHERE k = $after.k) = [1] THEN {
		CREATE ev_log SET k = 1 RETURN NONE;
	};
	DEFINE TABLE ev_linked SCHEMALESS PERMISSIONS FULL;
	DEFINE EVENT read ON ev_linked WHEN true THEN {
		CREATE ev_log SET k = (SELECT VALUE k FROM item WHERE k = $after.r.k) RETURN NONE;
	};
	DEFINE TABLE ev_derived SCHEMALESS PERMISSIONS FULL;
	DEFINE EVENT read ON ev_derived WHEN true THEN {
		CREATE ev_log SET k = (SELECT VALUE k FROM item WHERE k = $after.r.derived) RETURN NONE;
	};
	DEFINE TABLE ev_write SCHEMALESS PERMISSIONS FULL;
	DEFINE EVENT bump ON ev_write WHEN true THEN {
		UPDATE item SET n = 1 WHERE k = $after.k RETURN NONE;
	};
	DEFINE TABLE ev_write_linked SCHEMALESS PERMISSIONS FULL;
	DEFINE EVENT bump ON ev_write_linked WHEN true THEN {
		UPDATE item SET n = 1 WHERE k = $after.r.k RETURN NONE;
	};
";

/// The keys a transaction reads running `sql` as `session` where `item` holds `rows` records. The
/// transaction may write, and is cancelled.
async fn keys_read(rows: usize, session: &Session, sql: &str) -> u64 {
	let setup = format!(
		"{SCHEMA} INSERT INTO item (array::map(<array> 0..{rows}, |$i| {{ k: $i }})) RETURN NONE;"
	);
	let db = TestDb::quiescent(&setup, true).await;
	let txn =
		Arc::new(db.datastore().transaction(TransactionType::Write).await.expect("a transaction"));
	let request = QueryRequest::new(sql, session).with_transaction(Arc::clone(&txn));
	let mut responses = db.datastore().run(request).await.expect("the request runs");
	let answer = responses.pop().expect("a response").result.expect("the statement answers");
	assert_eq!(answer.to_sql(), "[1]", "{sql}");
	let read = txn.metrics_snapshot_for_test().keys_read;
	txn.cancel().await.expect("the transaction ends");
	read
}

/// Whether `sql` reads the index: the same keys over 100 records and over 2000.
async fn reads_the_index(session: &Session, sql: &str) -> bool {
	let small = keys_read(100, session, sql).await;
	let large = keys_read(2000, session, sql).await;
	assert!(small <= large, "{sql}: {small} keys over 100 records, {large} over 2000");
	small == large
}

fn record_user() -> Session {
	let rid = surrealdb_types::Value::RecordId(surrealdb_types::RecordId::new("user", "alice"));
	Session::for_record("test", "test", "user", rid)
}

#[tokio::test]
async fn an_unchecked_read_reads_the_index_unless_a_fetch_derives_the_field() {
	let owner = TestDb::owner();
	for sql in [
		"fn::stored(cfg:one)",
		"fn::stored(keyed:[1])",
		"fn::linked(cfg:one)",
		// An owner's fetch judges no permission.
		"fn::hidden(cfg:one)",
		"fn::stored(gated:one)",
	] {
		assert!(reads_the_index(&owner, sql).await, "{sql}: reads the table whole");
	}
	for sql in ["fn::derived(cfg:one)", "fn::nested(cfg:one)", "fn::linked_derived(cfg:one)"] {
		assert!(!reads_the_index(&owner, sql).await, "{sql}: reads the index");
	}
}

#[tokio::test]
async fn a_checked_read_reads_the_table_whole_under_a_conditional_permission() {
	let alice = record_user();
	assert!(reads_the_index(&alice, "fn::stored(cfg:one)").await, "a stored field");
	for sql in ["fn::hidden(cfg:one)", "fn::stored(gated:one)"] {
		assert!(!reads_the_index(&alice, sql).await, "{sql}: reads the index");
	}
}

#[tokio::test]
async fn a_write_reads_the_index_for_an_object_and_the_table_whole_for_a_record() {
	let owner = TestDb::owner();
	assert!(reads_the_index(&owner, "fn::bump({ k: 1 })").await, "an object's field");
	assert!(!reads_the_index(&owner, "fn::bump(plain:one)").await, "a stored field of a record");
}

#[tokio::test]
async fn an_event_keys_the_lookup_by_its_images_as_a_function_by_its_arguments() {
	let owner = TestDb::owner();
	for sql in [
		"CREATE ev_read SET k = 1 RETURN VALUE k",
		"CREATE ev_when SET k = 1 RETURN VALUE k",
		"CREATE ev_linked SET k = 1, r = cfg:one RETURN VALUE k",
		"CREATE ev_write SET k = 1 RETURN VALUE k",
	] {
		assert!(reads_the_index(&owner, sql).await, "{sql}: reads the table whole");
	}
	for sql in [
		"CREATE ev_derived SET k = 1, r = cfg:one RETURN VALUE k",
		"CREATE ev_write_linked SET k = 1, r = cfg:one RETURN VALUE k",
	] {
		assert!(!reads_the_index(&owner, sql).await, "{sql}: reads the index");
	}
}
