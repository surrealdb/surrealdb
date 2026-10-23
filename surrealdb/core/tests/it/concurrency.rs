//! Statements whose transactions overlap and write the same key.
//!
//! A statement outside a transaction block runs in a transaction of its own.
//! Two such transactions that both write one key conflict at commit: the second
//! to commit loses with a retryable `TransactionConflict`, and the caller retries
//! it. A retry starts a fresh transaction, which sees what the winner committed.

use anyhow::Result;
use surrealdb_core::dbs::Session;
use surrealdb_core::kvs::Datastore;
use surrealdb_types::{QueryError, Value};

use crate::helpers::new_ds;

/// Run `sql` twice with the two runs overlapping, and answer the one result of
/// each. On a current-thread runtime the second run starts while the first waits
/// in the `sleep` the statement evaluates before it writes, so each run's
/// transaction is open before either commits.
async fn overlap(dbs: &Datastore, sql: &str) -> Result<[Result<Value, surrealdb_types::Error>; 2]> {
	let ses = Session::owner().with_ns("test").with_db("test");
	let (a, b) = tokio::join!(dbs.execute(sql, &ses, None), dbs.execute(sql, &ses, None));
	let one = |res: Vec<surrealdb_rpc::QueryResult>| -> Result<_> {
		anyhow::ensure!(res.len() == 1, "one statement, one result: {res:?}");
		Ok(res.into_iter().next().expect("one result").result)
	};
	Ok([one(a?)?, one(b?)?])
}

/// Exactly one of `results` succeeded and the other lost a retryable conflict;
/// answers the success.
fn one_wins(results: [Result<Value, surrealdb_types::Error>; 2]) -> Value {
	let [a, b] = results;
	let (won, lost) = match (a, b) {
		(Ok(won), Err(lost)) | (Err(lost), Ok(won)) => (won, lost),
		other => panic!("expected one success and one conflict, got {other:?}"),
	};
	assert_eq!(
		lost.query_details(),
		Some(&QueryError::TransactionConflict),
		"the loser is told it can retry: {lost}"
	);
	won
}

async fn run(dbs: &Datastore, sql: &str) -> Result<Value> {
	let ses = Session::owner().with_ns("test").with_db("test");
	let mut res = dbs.execute(sql, &ses, None).await?;
	Ok(res.remove(0).result?)
}

/// Two overlapping `INSERT IGNORE`s of one new record both find it absent, so
/// both write it, and the second to commit loses with a retryable conflict.
/// Retried, it finds the record the winner stored and ignores it.
#[tokio::test(flavor = "current_thread")]
async fn overlapping_insert_ignores_of_one_record_conflict_and_a_retry_ignores_it() -> Result<()> {
	const SQL: &str = "INSERT IGNORE INTO t { id: 1, at: sleep(20ms) } RETURN id;";
	let (_, dbs) = new_ds("test", "test", true).await?;
	run(&dbs, "DEFINE TABLE t;").await?;
	let won = one_wins(overlap(&dbs, SQL).await?);
	assert_eq!(won, run(&dbs, "[{ id: t:1 }]").await?);
	assert_eq!(run(&dbs, SQL).await?, run(&dbs, "[]").await?);
	assert_eq!(run(&dbs, "SELECT VALUE id FROM t").await?, run(&dbs, "[t:1]").await?);
	Ok(())
}

/// Two overlapping first writes to a table that is not defined both define it,
/// so they write the same definition and the second to commit loses with a
/// retryable conflict. Retried, it finds the table the winner defined.
#[tokio::test(flavor = "current_thread")]
async fn overlapping_first_writes_to_an_undefined_table_conflict_and_a_retry_succeeds() -> Result<()>
{
	const SQL: &str = "CREATE |u:3| CONTENT { at: sleep(20ms) } RETURN NONE;";
	let (_, dbs) = new_ds("test", "test", true).await?;
	let _ = one_wins(overlap(&dbs, SQL).await?);
	run(&dbs, SQL).await?;
	assert_eq!(run(&dbs, "count(SELECT * FROM u)").await?, Value::from_t(6i64));
	Ok(())
}
