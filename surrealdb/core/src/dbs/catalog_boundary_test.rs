//! Catalog visibility across the transactions of one request.
//!
//! Client A's request runs its statements in auto-commit transactions. A
//! schema statement client B commits after A's earlier transaction resolves
//! and before A's later one opens is seen by the later one and not by the
//! earlier one.

use std::sync::Arc;
use std::time::Duration;

use surrealdb_rpc::QueryResult;
use surrealdb_types::Value as PublicValue;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::{QueryStreamItem, Session};
use crate::kvs::{Datastore, QueryRequest, TransactionType};

/// A datastore with the test namespace and database in place and nothing
/// defined in them.
async fn datastore() -> Arc<Datastore> {
	let ds = Datastore::new("memory").await.expect("a memory datastore");
	let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
	tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
	tx.commit().await.expect("committing the namespace");
	ds
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// Run `sql` to completion, failing the test if any statement fails.
async fn run_ok(ds: &Arc<Datastore>, sql: &str) {
	for result in ds.execute(sql, &session(), None).await.expect("the query runs") {
		result.result.expect("every statement succeeds");
	}
}

/// Run request `a` through the result stream and, once its first statement has
/// resolved but before its second has opened, run request `b` to completion.
/// Returns A's per-statement results.
///
/// The interleaving is deterministic. A's first statement is a scalar `RETURN`
/// (or a `BEGIN` block that has just resolved), so its value is sent only by
/// `finalise_results`, after its transaction resolved. It sends two items,
/// the value and `Finished`; the second cannot enter a channel of capacity 1
/// until this function receives, and the driver pulls the next statement only
/// after `finalise_results` returns. A's second statement therefore cannot
/// have opened a transaction while `b` runs.
async fn run_a_with_b_between(ds: &Arc<Datastore>, a: &str, b: &str) -> Vec<QueryResult> {
	let (tx, rx) = async_channel::bounded(1);
	let session = session();
	let job = ds.run_streaming(QueryRequest::new(a, &session), tx).expect("A parses");
	let run = tokio::spawn(job.run);

	tokio::time::timeout(Duration::from_secs(10), async {
		while rx.len() != 1 {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("A's first statement sent nothing");

	for result in ds.execute(b, &session, None).await.expect("B runs") {
		result.result.expect("B's statement succeeds");
	}

	let mut items = Vec::new();
	while let Ok(item) = rx.recv().await {
		items.push(item);
	}
	assert!(
		matches!(
			items.first(),
			Some(
				QueryStreamItem::Value {
					index: 0,
					..
				} | QueryStreamItem::Rows {
					index: 0,
					..
				}
			)
		),
		"A's first item is its first statement's value, got {:?}",
		items.first()
	);
	// A statement whose value went to the stream carries none in the job's own
	// results, so the values are reassembled from the items, as a consumer
	// demultiplexing by statement index does.
	run.await.expect("A's execution joins").expect("A's execution succeeds");
	reassemble(items)
}

/// What a consumer demultiplexing by statement index reassembles from `items`.
fn reassemble(items: Vec<QueryStreamItem>) -> Vec<QueryResult> {
	#[derive(Default)]
	struct Statement {
		rows: Vec<PublicValue>,
		single: Option<PublicValue>,
		finished: Option<QueryResult>,
	}
	let mut statements: Vec<Statement> = Vec::new();
	for item in items {
		let index = item.index();
		if index >= statements.len() {
			statements.resize_with(index + 1, Statement::default);
		}
		let statement = &mut statements[index];
		match item {
			QueryStreamItem::Rows {
				values,
				..
			} => statement.rows.extend(values),
			QueryStreamItem::Frame {
				rows,
				..
			} => statement.rows.extend(rows.to_values().expect("rows decode")),
			QueryStreamItem::Value {
				value,
				..
			} => statement.single = Some(value),
			QueryStreamItem::Finished {
				time,
				query_type,
				error,
				..
			} => {
				statement.finished = Some(QueryResult {
					time,
					result: error.map_or(Ok(PublicValue::None), Err),
					query_type,
				});
			}
			QueryStreamItem::Trace {
				..
			} => {}
		}
	}
	statements
		.into_iter()
		.map(|statement| {
			let mut result = statement.finished.expect("every statement ends with a Finished item");
			if result.result.is_ok() {
				result.result = Ok(match statement.single {
					Some(value) => value,
					None => PublicValue::Array(statement.rows.into()),
				});
			}
			result
		})
		.collect()
}

/// What a result looks like to a caller comparing it with an expectation.
fn render(results: Vec<QueryResult>) -> Vec<Result<String, String>> {
	results
		.into_iter()
		.map(|r| match r.result {
			Ok(value) => Ok(format!("{value:?}")),
			Err(e) => Err(e.to_string()),
		})
		.collect()
}

#[tokio::test]
async fn another_clients_definitions_between_auto_commit_statements_are_seen() {
	let ds = datastore().await;
	run_ok(&ds, "DEFINE TABLE t; DEFINE FUNCTION fn::f() { RETURN 1 }; DEFINE PARAM $p VALUE 1;")
		.await;
	let results = run_a_with_b_between(
		&ds,
		"RETURN [fn::f(), $p]; RETURN [fn::f(), $p]; RETURN (CREATE ONLY t:1 SET a = 1).c;",
		"DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 }; DEFINE PARAM OVERWRITE $p VALUE 2; \
		 DEFINE FIELD c ON t VALUE a + 1;",
	)
	.await;
	assert_eq!(
		render(results),
		vec![
			Ok("Array(Array([Number(Int(1)), Number(Int(1))]))".to_string()),
			Ok("Array(Array([Number(Int(2)), Number(Int(2))]))".to_string()),
			Ok("Number(Int(2))".to_string()),
		]
	);
}

#[tokio::test]
async fn another_clients_removals_between_auto_commit_statements_are_seen() {
	let ds = datastore().await;
	run_ok(&ds, "DEFINE TABLE t; DEFINE FUNCTION fn::f() { RETURN 1 }; CREATE t:1;").await;
	let results = run_a_with_b_between(
		&ds,
		"RETURN fn::f(); RETURN fn::f(); RETURN (SELECT VALUE id FROM t);",
		"REMOVE FUNCTION fn::f; REMOVE TABLE t;",
	)
	.await;
	assert_eq!(
		render(results),
		vec![
			Ok("Number(Int(1))".to_string()),
			Err("Function 'fn::f' not found: The function 'fn::f' does not exist".to_string()),
			Err("The table 't' does not exist".to_string()),
		]
	);
}

#[tokio::test]
async fn another_clients_definitions_after_a_committed_block_are_seen() {
	let ds = datastore().await;
	run_ok(&ds, "DEFINE FUNCTION fn::f() { RETURN 1 };").await;
	let results = run_a_with_b_between(
		&ds,
		"BEGIN; RETURN fn::f(); COMMIT; RETURN fn::f();",
		"DEFINE FUNCTION OVERWRITE fn::f() { RETURN 2 };",
	)
	.await;
	assert_eq!(
		render(results),
		vec![
			Ok("None".to_string()),
			Ok("Number(Int(1))".to_string()),
			Ok("None".to_string()),
			Ok("Number(Int(2))".to_string()),
		]
	);
}
