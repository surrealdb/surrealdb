//! Statements run in a transaction their caller owns, as an SDK's `begin` …
//! `commit` does: `QueryRequest::with_transaction`, after which the caller
//! commits whatever the statements answered.
//!
//! - `OPTION` applies to the statements after it in the request, as it does in a `BEGIN` block, and
//!   a principal that may not edit options is refused with an error of its own.
//! - A statement that fails after writing part of its change leaves the caller nothing to commit:
//!   the commit is refused, and cancels the transaction. One that fails before writing anything
//!   leaves the transaction as it was, so the caller commits what the other statements wrote.
//! - An index the caller's transaction defines is built in transactions of its own, past the
//!   caller's snapshot, for as long as the caller holds the transaction open: its reads go around
//!   the index, and a write that would fail the commit against the build's own is refused where it
//!   is made, across the requests of the one transaction and the save points it rolls back.
//! - A statement dropped while one of its records' attempts holds a save point open — a request
//!   deadline drops the statement's future — leaves the caller nothing to commit: the commit is
//!   refused.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;
use surrealdb_types::Value as PublicValue;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::kvs::{Datastore, QueryRequest, Transaction};

/// What a commit refused for a statement that failed after writing answers.
const REFUSED_COMMIT: &str = "A statement failed after writing part of its change";

async fn ds() -> Arc<Datastore> {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	let tx = ds.transaction(Write).await.unwrap();
	tx.ensure_ns_db(None, "test", "test").await.unwrap();
	tx.commit().await.unwrap();
	ds
}

fn owner() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// The answers of `sql`, one per statement, each a value or an error's message.
async fn answers(ds: &Datastore, sql: &str) -> Vec<Result<PublicValue, String>> {
	ds.execute(sql, &owner(), None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.map_err(|e| e.to_string()))
		.collect()
}

/// The answer of `sql`, a single statement that must succeed.
async fn answer(ds: &Datastore, sql: &str) -> PublicValue {
	let [answer] = <[_; 1]>::try_from(answers(ds, sql).await).unwrap();
	answer.unwrap_or_else(|e| panic!("{sql:?} failed: {e}"))
}

/// Runs `sql` and fails unless every statement succeeds.
async fn run(ds: &Datastore, sql: &str) {
	for (at, answer) in answers(ds, sql).await.into_iter().enumerate() {
		if let Err(e) = answer {
			panic!("statement {at} of {sql:?} failed: {e}");
		}
	}
}

/// The answers of `sql` run by `session` in one transaction its caller owns,
/// then the answer of the caller's commit, which follows whatever the
/// statements answered.
async fn client_answers(
	ds: &Datastore,
	session: &Session,
	sql: &str,
) -> Vec<Result<PublicValue, String>> {
	client_answers_by_request(ds, session, &[sql]).await
}

/// The answers of each of `requests`, run by `session` one after the other in
/// one transaction its caller owns, then the answer of the caller's commit.
async fn client_answers_by_request(
	ds: &Datastore,
	session: &Session,
	requests: &[&str],
) -> Vec<Result<PublicValue, String>> {
	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	let mut out = Vec::new();
	for sql in requests {
		let request = QueryRequest::new(*sql, session).with_transaction(Arc::clone(&txn));
		out.extend(
			ds.run(request)
				.await
				.unwrap()
				.into_iter()
				.map(|response| response.result.map_err(|e| e.to_string())),
		);
	}
	out.push(txn.commit().await.map(|()| PublicValue::None).map_err(|e| e.to_string()));
	out
}

fn assert_ok(answer: &Result<PublicValue, String>) {
	if let Err(e) = answer {
		panic!("expected success, got {e:?}");
	}
}

fn assert_err(answer: &Result<PublicValue, String>, contains: &str) {
	match answer {
		Err(message) => {
			assert!(message.contains(contains), "expected {contains:?}, got {message:?}")
		}
		Ok(value) => panic!("expected an error containing {contains:?}, got {value:?}"),
	}
}

async fn export_text(ds: &Datastore) -> String {
	let (tx, rx) = crate::channel::bounded::<Vec<u8>>(16);
	let session = owner();
	let task = ds.export(&session, tx).await.unwrap();
	let collector = tokio::spawn(async move {
		let mut out = Vec::new();
		while let Ok(chunk) = rx.recv().await {
			out.extend_from_slice(&chunk);
		}
		out
	});
	task.await.unwrap();
	String::from_utf8(collector.await.unwrap()).unwrap()
}

/// Exports `ds` and imports the dump into a new store, which must accept every
/// statement of it.
async fn assert_round_trips(ds: &Datastore) {
	let dump = export_text(ds).await;
	let target = self::ds().await;
	for (at, result) in target.import(&dump, &owner()).await.unwrap().into_iter().enumerate() {
		if let Err(e) = result.result {
			panic!("statement {at} of the dump was refused on import: {e}\n{dump}");
		}
	}
}

// ------------------------------------------------------------------------------------------------
// A dropped statement
// ------------------------------------------------------------------------------------------------

/// What a commit refused for a save point its opener never closed answers.
const ABANDONED_SAVE_POINT: &str = "A save point could not be closed";

/// A table whose writes of a record marked `slow` run an event that writes, then waits an hour,
/// inside the attempt the write opens a save point for.
const SLOW_EVENT: &str = "
	DEFINE TABLE item SCHEMALESS;
	DEFINE TABLE marker SCHEMALESS;
	CREATE marker:1 SET n = 0 RETURN NONE;
	CREATE item:1 SET slow = false RETURN NONE;
	DEFINE EVENT slow ON item WHEN $after.slow = true
		THEN { UPDATE marker:1 SET n += 1 RETURN NONE; sleep(1h) };
";

/// Runs `sql` in `txn` until the transaction holds a write the statement made, then drops the
/// statement's future, as a request deadline does.
async fn drop_once_written(ds: &Datastore, txn: &Arc<Transaction>, sql: &str) {
	let before = txn.bytes_written();
	let session = owner();
	let mut running =
		std::pin::pin!(ds.run(QueryRequest::new(sql, &session).with_transaction(Arc::clone(txn))));
	for _ in 0..12_000 {
		if txn.bytes_written() > before {
			return;
		}
		if let Ok(answered) =
			tokio::time::timeout(std::time::Duration::from_millis(5), running.as_mut()).await
		{
			panic!("{sql:?} answered before it was dropped: {:?}", answered.map(|_| ()));
		}
	}
	panic!("{sql:?} wrote nothing within a minute");
}

/// A statement dropped while a record's attempt holds its save point open refuses the commit:
/// the update of a record that stands, the create of one that does not, and the update an
/// `INSERT` makes of a duplicate. The same writes run to completion commit.
#[tokio::test]
async fn a_statement_dropped_inside_a_save_point_is_not_committed() {
	for sql in [
		"UPSERT item:1 SET slow = true RETURN NONE;",
		"UPSERT item:2 SET slow = true RETURN NONE;",
		"INSERT INTO item { id: item:1 } ON DUPLICATE KEY UPDATE slow = true RETURN NONE;",
	] {
		let ds = ds().await;
		run(&ds, SLOW_EVENT).await;
		let txn = Arc::new(ds.transaction(Write).await.unwrap());
		drop_once_written(&ds, &txn, sql).await;
		match txn.commit().await {
			Err(e) => assert!(
				e.to_string().contains(ABANDONED_SAVE_POINT),
				"{sql:?}: expected {ABANDONED_SAVE_POINT:?}, got {e}"
			),
			Ok(()) => {
				panic!("{sql:?}: the commit of the dropped statement's transaction succeeded")
			}
		}
		assert_eq!(
			answer(&ds, "SELECT VALUE [id, slow] FROM item ORDER BY id;").await,
			answer(&ds, "RETURN [[item:1, false]];").await,
			"{sql:?}",
		);

		let completed = sql.replace("slow = true", "slow = false");
		for answer in client_answers(&ds, &owner(), &completed).await {
			assert_ok(&answer);
		}
	}
}

// ------------------------------------------------------------------------------------------------
// OPTION
// ------------------------------------------------------------------------------------------------

/// `OPTION IMPORT` skips field processing for the statements after it, so a
/// value the field's type refuses is written as it is, in a caller's
/// transaction as in a block.
#[tokio::test]
async fn an_option_applies_to_the_statements_after_it_as_in_a_block() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE p SCHEMAFULL; DEFINE FIELD a ON p TYPE int;").await;
	let client =
		client_answers(&ds, &owner(), "OPTION IMPORT; CREATE p:1 SET a = 'x' RETURN NONE;").await;
	let block =
		answers(&ds, "BEGIN; OPTION IMPORT; CREATE p:2 SET a = 'x' RETURN NONE; COMMIT;").await;
	assert_eq!(client[..2], block[1..3], "the option and the write answer as in a block");
	assert_eq!(client[2], Ok(PublicValue::None), "the caller's commit");
	assert_eq!(
		answer(&ds, "SELECT VALUE a FROM p ORDER BY id;").await,
		answer(&ds, "RETURN ['x', 'x'];").await,
	);
}

/// The option ends with its request: a later request on the same transaction
/// runs without it.
#[tokio::test]
async fn an_option_ends_with_its_request() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE p SCHEMAFULL; DEFINE FIELD a ON p TYPE int;").await;
	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	let session = owner();
	for (sql, succeeds) in [("OPTION IMPORT;", true), ("CREATE p:1 SET a = 'x';", false)] {
		let request = QueryRequest::new(sql, &session).with_transaction(Arc::clone(&txn));
		let [response] = <[_; 1]>::try_from(ds.run(request).await.unwrap()).unwrap();
		match response.result {
			Ok(_) => assert!(succeeds, "{sql:?} succeeded"),
			Err(e) => {
				assert!(!succeeds, "{sql:?} failed: {e}");
				assert!(e.to_string().contains("Couldn't coerce value for field `a`"), "{e}");
			}
		}
	}
	txn.commit().await.unwrap();
	assert_eq!(answer(&ds, "SELECT * FROM p;").await, answer(&ds, "RETURN [];").await);
}

/// A principal that may not edit options is refused, and the statements after
/// the option run without it.
#[tokio::test]
async fn an_option_a_principal_may_not_edit_is_refused() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE p SCHEMAFULL; DEFINE FIELD a ON p TYPE int;").await;
	let viewer = Session::viewer().with_ns("test").with_db("test");
	let client = client_answers(&ds, &viewer, "OPTION IMPORT; SELECT * FROM p;").await;
	assert_err(&client[0], "Not enough permissions");
	assert_eq!(client[1], answers(&ds, "SELECT * FROM p;").await[0]);
	assert_eq!(client[2], Ok(PublicValue::None), "nothing was written, so the commit stands");
}

// ------------------------------------------------------------------------------------------------
// A statement that fails after writing
// ------------------------------------------------------------------------------------------------

/// A `UNIQUE` index whose build finds duplicates has written its definition
/// when the build is refused. The definition never commits, so it is not
/// listed, and is not exported over the rows that violate it. The record the
/// transaction writes before it is another table's: a record of the table
/// itself is a write the build cannot see, which refuses the definition before
/// it writes anything.
#[tokio::test]
async fn a_unique_definition_whose_build_fails_is_not_committed() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE u SCHEMAFULL; DEFINE FIELD v ON u TYPE string; DEFINE TABLE w SCHEMALESS;
		 CREATE u:1 SET v = 'a' RETURN NONE; CREATE u:2 SET v = 'a' RETURN NONE;",
	)
	.await;
	let client = client_answers(
		&ds,
		&owner(),
		"CREATE w:9 SET v = 'b' RETURN NONE; DEFINE INDEX uv ON u FIELDS v UNIQUE;",
	)
	.await;
	assert_ok(&client[0]);
	assert_err(&client[1], "Database index `uv` already contains 'a'");
	assert_err(&client[2], REFUSED_COMMIT);
	assert_eq!(answer(&ds, "(INFO FOR TABLE u).indexes;").await, answer(&ds, "RETURN {};").await);
	assert_eq!(
		answer(&ds, "SELECT VALUE id FROM u ORDER BY id;").await,
		answer(&ds, "RETURN [u:1, u:2];").await,
	);
	assert_eq!(
		answer(&ds, "SELECT VALUE id FROM w;").await,
		answer(&ds, "RETURN [];").await,
		"the refused commit takes the statements before the failure with it",
	);
	assert_round_trips(&ds).await;
	// Defined again once the rows admit it, the index builds and enforces.
	run(&ds, "DELETE u:2; DEFINE INDEX uv ON u FIELDS v UNIQUE;").await;
	assert_err(&answers(&ds, "CREATE u:3 SET v = 'a';").await[0], "already contains 'a'");
}

/// A record whose value a `UNIQUE` index refuses has been written when the
/// index refuses it. It never commits beside the record that holds the value.
#[tokio::test]
async fn a_record_a_unique_index_refuses_is_not_committed() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE p SCHEMAFULL; DEFINE FIELD v ON p TYPE string;
		 DEFINE INDEX pv ON p FIELDS v UNIQUE; CREATE p:0 SET v = 'taken' RETURN NONE;",
	)
	.await;
	let client = client_answers(&ds, &owner(), "CREATE p:1 SET v = 'taken' RETURN NONE;").await;
	assert_err(&client[0], "Database index `pv` already contains 'taken'");
	assert_err(&client[1], REFUSED_COMMIT);
	assert_eq!(
		answer(&ds, "SELECT VALUE id FROM p ORDER BY id;").await,
		answer(&ds, "RETURN [p:0];").await,
	);
	assert_round_trips(&ds).await;
}

/// A table event used as a guard that writes a side effect and then `THROW`s
/// has written both the triggering record and the side effect when it throws:
/// the record in its own store stage, the side effect in the event's nested
/// `CREATE`, both before the `THROW`. Catching the statement's error and
/// committing does not get past the guard: the commit is refused and neither
/// write persists (GHSA-623g-mhxq-5hmf).
#[tokio::test]
async fn an_event_that_writes_then_throws_is_not_committed() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE item SCHEMALESS; DEFINE TABLE audit SCHEMALESS;
		 DEFINE EVENT guard ON item WHEN $event = 'CREATE' AND $after.qty < 0
		     THEN { CREATE audit SET k = 'before-throw'; THROW 'qty must be >= 0' };",
	)
	.await;
	let client = client_answers(&ds, &owner(), "CREATE item:bad SET qty = -1 RETURN NONE;").await;
	assert_err(&client[0], "qty must be >= 0");
	assert_err(&client[1], REFUSED_COMMIT);
	assert_eq!(
		answer(&ds, "SELECT VALUE id FROM item, audit;").await,
		answer(&ds, "RETURN [];").await,
	);
	assert_round_trips(&ds).await;
}

/// A multi-row write refused on a later row has written the rows before it.
#[tokio::test]
async fn a_write_refused_part_way_is_not_committed() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE p SCHEMAFULL; DEFINE FIELD a ON p TYPE int;").await;
	let client = client_answers(
		&ds,
		&owner(),
		"INSERT INTO p [{ id: p:1, a: 1 }, { id: p:2, a: 'x' }] RETURN NONE;",
	)
	.await;
	assert_err(&client[0], "Couldn't coerce value for field `a` of `p:2`");
	assert_err(&client[1], REFUSED_COMMIT);
	assert_eq!(answer(&ds, "SELECT * FROM p;").await, answer(&ds, "RETURN [];").await);
}

/// A field definition refused for a table that does not exist yet has created
/// the table when it is refused.
#[tokio::test]
async fn a_definition_refused_after_creating_its_table_is_not_committed() {
	let ds = ds().await;
	let client = client_answers(&ds, &owner(), "DEFINE FIELD b ON t INLINE TYPE int;").await;
	assert_err(&client[0], "INLINE requires an existing TYPE RELATION table");
	assert_err(&client[1], REFUSED_COMMIT);
	assert_eq!(answer(&ds, "(INFO FOR DB).tables;").await, answer(&ds, "RETURN {};").await);
}

/// An index definition without `CONCURRENTLY` that the transaction's own record
/// writes refuse is refused before it writes anything, so the caller commits what
/// the other statements wrote: over a table that exists, and over a table the
/// transaction has removed with its records, which the statement would otherwise
/// create again. A strict database, which cannot create the table, says so before
/// it judges the writes.
#[tokio::test]
async fn a_blocking_definition_refused_for_the_records_written_writes_nothing() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE kept SCHEMALESS; CREATE kept:1 SET a = 1 RETURN NONE;
		 DEFINE TABLE gone SCHEMALESS; CREATE gone:1 SET a = 1 RETURN NONE;",
	)
	.await;
	let refusal = |table: &str| {
		format!(
			"Cannot define index `ix` on `{table}`: this transaction has written records of `{table}`"
		)
	};

	let client = client_answers(
		&ds,
		&owner(),
		"CREATE kept:2 SET a = 2 RETURN NONE; DEFINE INDEX ix ON kept FIELDS a;",
	)
	.await;
	assert_ok(&client[0]);
	assert_err(&client[1], &refusal("kept"));
	assert_eq!(client[2], Ok(PublicValue::None), "the caller's commit over a table that exists");
	assert_eq!(
		answer(&ds, "SELECT VALUE id FROM kept ORDER BY id;").await,
		answer(&ds, "RETURN [kept:1, kept:2];").await,
	);

	let client =
		client_answers(&ds, &owner(), "REMOVE TABLE gone; DEFINE INDEX ix ON gone FIELDS a;").await;
	assert_ok(&client[0]);
	assert_err(&client[1], &refusal("gone"));
	assert_eq!(client[2], Ok(PublicValue::None), "the caller's commit over a table it removed");
	assert_eq!(
		answer(&ds, "object::keys((INFO FOR DB).tables);").await,
		answer(&ds, "RETURN ['kept'];").await,
		"the removal commits, and the refused statement does not create the table again",
	);

	run(
		&ds,
		"DEFINE DATABASE strict STRICT; USE DB strict;
		 DEFINE TABLE gone SCHEMALESS; CREATE gone:1 SET a = 1 RETURN NONE;",
	)
	.await;
	let strict = Session::owner().with_ns("test").with_db("strict");
	let client =
		client_answers(&ds, &strict, "REMOVE TABLE gone; DEFINE INDEX ix ON gone FIELDS a;").await;
	assert_ok(&client[0]);
	assert_err(&client[1], "The table 'gone' does not exist");
	assert_eq!(client[2], Ok(PublicValue::None), "the caller's commit in a strict database");
}

/// Statements refused before they write anything leave the transaction as it
/// was, and the caller commits what the others wrote.
#[tokio::test]
async fn statements_refused_before_writing_leave_the_transaction_committable() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE p SCHEMAFULL; DEFINE FIELD a ON p TYPE int;").await;
	let client = client_answers(
		&ds,
		&owner(),
		"CREATE p:1 SET a = 1 RETURN NONE;
		 CREATE p:2 SET a = 'x' RETURN NONE;
		 UPDATE p:1 SET a = 'x' RETURN NONE;
		 SELECT * FROM nosuch;
		 THROW 'boom';
		 CREATE p:3 SET a = 3 RETURN NONE;",
	)
	.await;
	assert_ok(&client[0]);
	assert_err(&client[1], "Couldn't coerce value for field `a` of `p:2`");
	assert_err(&client[2], "Couldn't coerce value for field `a` of `p:1`");
	assert_err(&client[3], "The table 'nosuch' does not exist");
	assert_err(&client[4], "boom");
	assert_ok(&client[5]);
	assert_eq!(client[6], Ok(PublicValue::None), "the caller's commit");
	assert_eq!(
		answer(&ds, "SELECT * FROM p ORDER BY id;").await,
		answer(&ds, "RETURN [{ id: p:1, a: 1 }, { id: p:3, a: 3 }];").await,
	);
}

/// A table definition refused for a clause with an effect, or for a migration
/// step its rows are behind, is refused before it writes a relation's `in` and
/// `out`, its changefeed or itself, so the caller commits what the other
/// statements wrote and the table is left as it was.
#[tokio::test]
async fn a_table_definition_refused_for_its_clauses_or_a_step_writes_nothing() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE log SCHEMALESS;
		 DEFINE FUNCTION fn::writes() { CREATE log; RETURN true; };
		 DEFINE TABLE p SCHEMAFULL;
		 DEFINE TABLE ed TYPE ANY SCHEMAFULL;
		 DEFINE FIELD a ON ed TYPE string;
		 RELATE pt:1->ed:1->pt:2 SET a = '5' RETURN NONE;
		 ALTER FIELD a ON ed TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;",
	)
	.await;
	let table = "[(INFO FOR DB).tables.ed, (INFO FOR TABLE ed).fields];";
	let before = answer(&ds, table).await;
	for (at, (sql, refusal)) in [
		(
			"DEFINE TABLE OVERWRITE ed TYPE RELATION SCHEMAFULL CHANGEFEED 1h \
			 PERMISSIONS FOR select WHERE fn::writes();",
			"The PERMISSIONS clause must not have effects",
		),
		(
			"DEFINE TABLE OVERWRITE ed TYPE RELATION SCHEMAFULL CHANGEFEED 1h;",
			"rows are behind a migration step",
		),
	]
	.into_iter()
	.enumerate()
	{
		let client = client_answers(
			&ds,
			&owner(),
			&format!("CREATE p:{at} RETURN NONE; {sql} CREATE p:{} RETURN NONE;", at + 10),
		)
		.await;
		assert_ok(&client[0]);
		assert_err(&client[1], refusal);
		assert_ok(&client[2]);
		assert_eq!(client[3], Ok(PublicValue::None), "the caller's commit after {sql:?}");
		assert_eq!(answer(&ds, table).await, before, "{sql:?} left the table as it was");
	}
	assert_eq!(
		answer(&ds, "SELECT VALUE id FROM p ORDER BY id;").await,
		answer(&ds, "RETURN [p:0, p:1, p:10, p:11];").await,
	);
}

// ------------------------------------------------------------------------------------------------
// An index the transaction defines
// ------------------------------------------------------------------------------------------------

/// The index a caller's transaction defined holds what its build wrote, which
/// the transaction's snapshot predates: its reads answer for every row, in the
/// request that defined the index and in the requests after it, and the index
/// serves the rows it indexed once the caller commits.
#[tokio::test]
async fn an_index_the_callers_transaction_defined_is_read_around_until_it_commits() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE item SCHEMALESS;
		 CREATE item:1 SET k = 1 RETURN NONE;
		 CREATE item:2 SET k = 2 RETURN NONE;",
	)
	.await;
	let read = "SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;";
	let client = client_answers_by_request(
		&ds,
		&owner(),
		&[
			&format!("DEFINE INDEX by_k ON item FIELDS k; {read}"),
			&format!(
				"CREATE item:3 SET k = 1 RETURN NONE; {read} SELECT count() FROM item GROUP ALL;"
			),
		],
	)
	.await;
	assert_ok(&client[0]);
	assert_eq!(client[1], Ok(answer(&ds, "RETURN [item:1];").await), "the row the build indexed");
	assert_ok(&client[2]);
	assert_eq!(
		client[3],
		Ok(answer(&ds, "RETURN [item:1, item:3];").await),
		"the build's row and the caller's own, in a request after the definition",
	);
	assert_eq!(client[4], Ok(answer(&ds, "RETURN [{ count: 3 }];").await));
	assert_eq!(client[5], Ok(PublicValue::None), "the caller's commit");
	assert_eq!(
		answer(&ds, read).await,
		answer(&ds, "RETURN [item:1, item:3];").await,
		"the index serves every row it indexed once the caller has committed",
	);
}

/// A duplicate of a committed value is a uniqueness error where it is written.
/// It is not a write conflict at the caller's commit, which the caller would
/// retry into the same conflict.
#[tokio::test]
async fn a_duplicate_of_a_committed_value_is_refused_in_the_callers_transaction() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE item SCHEMALESS; CREATE item:1 SET k = 1 RETURN NONE;").await;
	let client = client_answers_by_request(
		&ds,
		&owner(),
		&["DEFINE INDEX u ON item FIELDS k UNIQUE;", "CREATE item:2 SET k = 1 RETURN NONE;"],
	)
	.await;
	assert_ok(&client[0]);
	assert_err(&client[1], "Database index `u` already contains 1, with record `item:1`");
	assert_err(&client[2], REFUSED_COMMIT);
	assert_eq!(
		answer(&ds, "SELECT VALUE id FROM item ORDER BY id;").await,
		answer(&ds, "RETURN [item:1];").await,
	);
	assert_eq!(
		answer(&ds, "(INFO FOR TABLE item).indexes;").await,
		answer(&ds, "RETURN {};").await
	);
}

/// A value no committed row holds is written, and the index enforces it, and
/// every committed value, once the caller has committed.
#[tokio::test]
async fn a_free_value_is_written_in_the_callers_transaction_and_enforced_after_it() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE item SCHEMALESS; CREATE item:1 SET k = 1 RETURN NONE;").await;
	let client = client_answers_by_request(
		&ds,
		&owner(),
		&["DEFINE INDEX u ON item FIELDS k UNIQUE;", "CREATE item:2 SET k = 2 RETURN NONE;"],
	)
	.await;
	client.iter().for_each(assert_ok);
	assert_err(
		&answers(&ds, "CREATE item:3 SET k = 1;").await[0],
		"Database index `u` already contains 1, with record `item:1`",
	);
	assert_err(
		&answers(&ds, "CREATE item:3 SET k = 2;").await[0],
		"Database index `u` already contains 2, with record `item:2`",
	);
}

/// A change that leaves the indexed value of a row alone writes nothing the
/// build wrote, so the transaction that defined the index makes it, and the
/// index serves the row once the caller has committed.
#[tokio::test]
async fn a_change_that_leaves_the_indexed_value_alone_is_made_in_the_callers_transaction() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE item SCHEMALESS; CREATE item:1 SET k = 1, other = 1 RETURN NONE;").await;
	let client = client_answers_by_request(
		&ds,
		&owner(),
		&["DEFINE INDEX by_k ON item FIELDS k;", "UPDATE item:1 SET other = 2 RETURN NONE;"],
	)
	.await;
	client.iter().for_each(assert_ok);
	assert_eq!(
		answer(&ds, "SELECT * FROM item WITH INDEX by_k WHERE k = 1;").await,
		answer(&ds, "RETURN [{ id: item:1, k: 1, other: 2 }];").await,
	);
}

/// A row the build indexed is not given another indexed value by the
/// transaction that defined the index, whichever request makes the change. The
/// refusal names the record and the index. It leaves the caller nothing to
/// commit, as does any statement that fails after writing part of its change.
#[tokio::test]
async fn a_row_the_build_indexed_is_not_given_another_value_in_the_callers_transaction() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE item SCHEMALESS; CREATE item:1 SET k = 1, other = 1 RETURN NONE;").await;
	let client = client_answers_by_request(
		&ds,
		&owner(),
		&["DEFINE INDEX by_k ON item FIELDS k;", "UPDATE item:1 SET k = 9 RETURN NONE;"],
	)
	.await;
	assert_ok(&client[0]);
	assert_err(
		&client[1],
		"Cannot change record `item:1`: index `by_k` was defined by this transaction",
	);
	assert_err(&client[2], REFUSED_COMMIT);
	assert_eq!(
		answer(&ds, "SELECT * FROM item;").await,
		answer(&ds, "RETURN [{ id: item:1, k: 1, other: 1 }];").await,
	);
}

/// The answers of `sql`, run by `session` in `txn`, which the caller keeps open
/// across requests.
async fn in_transaction(
	ds: &Datastore,
	session: &Session,
	txn: &Arc<Transaction>,
	sql: &str,
) -> Vec<Result<PublicValue, String>> {
	let request = QueryRequest::new(sql, session).with_transaction(Arc::clone(txn));
	ds.run(request)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.map_err(|e| e.to_string()))
		.collect()
}

/// A save point rolls back to a row the caller's transaction deleted, or away
/// from one it created, after it defined the index. A row the caller created
/// is its own however the save points fall: it changes the row as it likes
/// whether the rollback restored it or created it afresh, and the index serves
/// it once the caller has committed.
#[tokio::test]
async fn a_row_the_callers_save_point_restores_is_still_the_callers_own() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE item SCHEMALESS; CREATE item:1 SET k = 1 RETURN NONE;").await;
	let session = owner();
	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	let step = |sql: &'static str| in_transaction(&ds, &session, &txn, sql);

	step("DEFINE INDEX by_k ON item FIELDS k; CREATE item:2 SET k = 2 RETURN NONE;")
		.await
		.iter()
		.for_each(assert_ok);

	// The row the caller created is deleted, and the rollback brings it back.
	txn.new_save_point().await.unwrap();
	step("DELETE item:2 RETURN NONE;").await.iter().for_each(assert_ok);
	txn.rollback_to_save_point().await.unwrap();
	step("UPDATE item:2 SET k = 3 RETURN NONE;").await.iter().for_each(assert_ok);

	// The row the caller creates is rolled back, and the caller creates it again.
	txn.new_save_point().await.unwrap();
	step("CREATE item:3 SET k = 4 RETURN NONE;").await.iter().for_each(assert_ok);
	txn.rollback_to_save_point().await.unwrap();
	step("CREATE item:3 SET k = 5 RETURN NONE; UPDATE item:3 SET k = 6 RETURN NONE;")
		.await
		.iter()
		.for_each(assert_ok);

	txn.commit().await.unwrap();
	assert_eq!(
		answer(&ds, "SELECT * FROM item WITH INDEX by_k WHERE k >= 0 ORDER BY id;").await,
		answer(&ds, "RETURN [{ id: item:1, k: 1 }, { id: item:2, k: 3 }, { id: item:3, k: 6 }];")
			.await,
	);
}

/// A save point rollback does not hand the rows the build indexed to the
/// caller's transaction: the build wrote their entries, and a change of them
/// is refused after the rollback as it was before.
#[tokio::test]
async fn a_row_the_build_indexed_stays_out_of_reach_across_the_callers_save_points() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE item SCHEMALESS; CREATE item:1 SET k = 1 RETURN NONE;").await;
	let session = owner();
	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	let refused = "Cannot change record `item:1`: index `by_k` was defined by this transaction";

	in_transaction(&ds, &session, &txn, "DEFINE INDEX by_k ON item FIELDS k;")
		.await
		.iter()
		.for_each(assert_ok);
	for _ in 0..2 {
		txn.new_save_point().await.unwrap();
		let answers =
			in_transaction(&ds, &session, &txn, "UPDATE item:1 SET k = 9 RETURN NONE;").await;
		assert_err(&answers[0], refused);
		txn.rollback_to_save_point().await.unwrap();
	}
	txn.cancel().await.unwrap();
	assert_eq!(
		answer(&ds, "SELECT * FROM item;").await,
		answer(&ds, "RETURN [{ id: item:1, k: 1 }];").await,
	);
}

/// A row the caller's snapshot holds and the store no longer does, because
/// another transaction deleted it after the snapshot, is a row no build indexed.
/// The store held no record of the table, so the index is not recorded as one
/// the caller cannot read: the caller reads the index its snapshot has, which
/// lists the rows it writes after the definition. An indexed read leaves the
/// snapshot's row out where a read of the table finds it.
#[tokio::test]
async fn an_index_defined_over_rows_only_the_callers_snapshot_holds_has_no_entry_for_them() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE item SCHEMALESS; CREATE item:1 SET k = 1 RETURN NONE;").await;
	let session = owner();
	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	let step = |sql: &'static str| in_transaction(&ds, &session, &txn, sql);
	let held = answer(&ds, "RETURN [item:1];").await;
	let none = answer(&ds, "RETURN [];").await;

	assert_eq!(step("SELECT VALUE id FROM item ORDER BY id;").await, [Ok(held.clone())]);
	run(&ds, "DELETE item:1;").await;
	assert_eq!(answer(&ds, "SELECT VALUE id FROM item;").await, none);

	step("DEFINE INDEX by_k ON item FIELDS k;").await.iter().for_each(assert_ok);
	assert_eq!(
		step("SELECT VALUE id FROM item WITH NOINDEX WHERE k = 1 ORDER BY id;").await,
		[Ok(held)],
		"a read of the table finds the row the caller's snapshot holds",
	);
	assert_eq!(
		step("SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;").await,
		[Ok(none)],
		"the index has no entry for it",
	);
	step("CREATE item:2 SET k = 1 RETURN NONE;").await.iter().for_each(assert_ok);
	assert_eq!(
		step("SELECT VALUE id FROM item WITH INDEX by_k WHERE k = 1 ORDER BY id;").await,
		[Ok(answer(&ds, "RETURN [item:2];").await)],
		"the index lists the row the caller wrote after the definition",
	);
	txn.cancel().await.unwrap();
}
