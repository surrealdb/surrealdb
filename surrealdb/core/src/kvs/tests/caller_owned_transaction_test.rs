//! Statements run in a transaction their caller owns, as an SDK's `begin` …
//! `commit` does: `QueryRequest::with_transaction`, after which the caller
//! commits whatever the statements answered.
//!
//! - `OPTION` applies to the statements after it in the request, as it does in a `BEGIN` block, and
//!   a principal that may not edit options is refused with an error of its own.
//! - A statement that fails after writing part of its change leaves the caller nothing to commit:
//!   the commit is refused, and cancels the transaction. One that fails before writing anything
//!   leaves the transaction as it was, so the caller commits what the other statements wrote.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::Write;
use surrealdb_types::Value as PublicValue;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::Session;
use crate::kvs::{Datastore, QueryRequest};

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
	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	let request = QueryRequest::new(sql, session).with_transaction(Arc::clone(&txn));
	let mut out: Vec<Result<PublicValue, String>> = ds
		.run(request)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.map_err(|e| e.to_string()))
		.collect();
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
/// listed, and is not exported over the rows that violate it.
#[tokio::test]
async fn a_unique_definition_whose_build_fails_is_not_committed() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE u SCHEMAFULL; DEFINE FIELD v ON u TYPE string;
		 CREATE u:1 SET v = 'a' RETURN NONE; CREATE u:2 SET v = 'a' RETURN NONE;",
	)
	.await;
	let client = client_answers(
		&ds,
		&owner(),
		"CREATE u:9 SET v = 'b' RETURN NONE; DEFINE INDEX uv ON u FIELDS v UNIQUE;",
	)
	.await;
	assert_ok(&client[0]);
	assert_err(&client[1], "Database index `uv` already contains 'a'");
	assert_err(&client[2], REFUSED_COMMIT);
	assert_eq!(answer(&ds, "(INFO FOR TABLE u).indexes;").await, answer(&ds, "RETURN {};").await);
	assert_eq!(
		answer(&ds, "SELECT VALUE id FROM u ORDER BY id;").await,
		answer(&ds, "RETURN [u:1, u:2];").await,
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
