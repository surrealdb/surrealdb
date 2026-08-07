//! Differential tests for the streaming execution path.
//!
//! The contract is that streaming changes *when* a result reaches the caller,
//! never *what* it is. Each test therefore runs the same query both ways and
//! compares, rather than asserting a shape only one path produces.

use std::sync::Arc;

use surrealdb_rpc::QueryResult;
use surrealdb_types::Value as PublicValue;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::{QUERY_STREAM_BUFFER, QueryStreamItem, Session};
use crate::kvs::{Datastore, TransactionType};

/// What a consumer reassembles from one statement's items.
#[derive(Default)]
struct Statement {
	values: Vec<PublicValue>,
	/// Set when the statement's value was a single value rather than rows.
	single: Option<PublicValue>,
	finished: Option<QueryResult>,
}

impl Statement {
	fn finish(self) -> QueryResult {
		let mut result = self.finished.expect("every statement is terminated by a Finished item");
		if result.result.is_ok() {
			result.result = Ok(match self.single {
				Some(value) => value,
				None => PublicValue::Array(self.values.into()),
			});
		}
		result
	}
}

/// A datastore with the test namespace and database in place, seeded with
/// three records.
///
/// `USE` no longer creates a namespace implicitly, so the namespace has to
/// exist before any statement runs. Every seed statement's own result is
/// checked as well: `execute` reports a per-statement failure inside its `Ok`,
/// so a seed that silently did nothing would leave every test below comparing
/// two identically empty answers and passing for the wrong reason.
async fn datastore() -> Arc<Datastore> {
	let ds = Datastore::new("memory").await.expect("a memory datastore");
	let tx = ds.transaction(TransactionType::Write).await.expect("a write transaction");
	tx.ensure_ns_db(None, "test", "test").await.expect("the test namespace and database");
	tx.commit().await.expect("committing the namespace");

	for result in ds.execute(SEED, &session(), None).await.expect("the seed runs") {
		result.result.expect("every seed statement succeeds");
	}
	ds
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// Run a query through the streaming path and reassemble what a consumer would
/// see, the way a client demultiplexing by statement index does.
async fn streamed(ds: &Arc<Datastore>, sql: &str) -> Result<Vec<QueryResult>, String> {
	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let job = ds.execute_stream(sql, &session(), None, None, tx).map_err(|e| e.to_string())?;

	// The execution and the drain have to run together: the channel is bounded,
	// so an execution left to run alone would block forever on a full channel.
	let drain = async {
		let mut statements: Vec<Option<Statement>> = Vec::new();
		while let Ok(item) = rx.recv().await {
			let index = item.index();
			if index >= statements.len() {
				statements.resize_with(index + 1, || None);
			}
			let statement = statements[index].get_or_insert_with(Statement::default);
			match item {
				QueryStreamItem::Rows {
					values,
					..
				} => statement.values.extend(values),
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
						result: match error {
							Some(error) => Err(error),
							None => Ok(PublicValue::None),
						},
						query_type,
					});
				}
			}
		}
		statements
	};

	let (outcome, statements) = futures::future::join(job.run, drain).await;
	outcome.map_err(|e| e.to_string())?;
	// An index that produced no item at all is a statement control flow
	// skipped, and contributes no result -- the same as on the buffered path.
	Ok(statements.into_iter().flatten().map(Statement::finish).collect())
}

/// Compare everything about two results except how long they took.
fn same(streamed: &[QueryResult], buffered: &[QueryResult], sql: &str) {
	assert_eq!(
		streamed.len(),
		buffered.len(),
		"{sql}: streamed produced {} results, buffered {}",
		streamed.len(),
		buffered.len()
	);
	for (index, (s, b)) in streamed.iter().zip(buffered).enumerate() {
		assert_eq!(s.query_type, b.query_type, "{sql}: statement {index} query type");
		match (&s.result, &b.result) {
			(Ok(s), Ok(b)) => assert_eq!(s, b, "{sql}: statement {index} value"),
			(Err(s), Err(b)) => {
				assert_eq!(s.to_string(), b.to_string(), "{sql}: statement {index} error")
			}
			(s, b) => panic!("{sql}: statement {index} differs: streamed {s:?}, buffered {b:?}"),
		}
	}
}

/// Run a query both ways and assert the results are the same.
async fn assert_parity(sql: &str) {
	let ds = datastore().await;
	let streamed = streamed(&ds, sql).await;

	let ds = datastore().await;
	let buffered = ds.execute(sql, &session(), None).await;

	match (streamed, buffered) {
		(Ok(streamed), Ok(buffered)) => same(&streamed, &buffered, sql),
		(Err(streamed), Err(buffered)) => {
			assert_eq!(streamed, buffered.to_string(), "{sql}: both paths must fail the same way")
		}
		(s, b) => panic!("{sql}: one path failed and the other did not: {s:?} vs {b:?}"),
	}
}

const SEED: &str = "CREATE thing:1 SET n = 1; CREATE thing:2 SET n = 2; CREATE thing:3 SET n = 3;";

#[tokio::test]
async fn streaming_matches_buffered() {
	// Row-shaped, scalar-shaped, empty, multi-statement, and the statement
	// kinds that fall back to the legacy evaluator rather than streaming.
	for sql in [
		"SELECT * FROM thing",
		"SELECT * FROM thing ORDER BY n DESC",
		"SELECT * FROM thing WHERE n > 99",
		"SELECT * FROM ONLY thing:1",
		"RETURN 1 + 2",
		"SELECT n FROM thing; RETURN 'done'; SELECT * FROM thing WHERE n = 2",
		"CREATE thing:4 SET n = 4",
		"UPDATE thing:1 SET n = 10",
		"DELETE thing:1",
		"INFO FOR DB",
		"LET $x = SELECT * FROM thing; RETURN $x",
		"SELECT * FROM thing; THROW 'nope'; SELECT * FROM thing WHERE n = 1",
		"SELECT count() FROM thing GROUP ALL",
		"BEGIN; CREATE thing:5 SET n = 5; SELECT * FROM thing; COMMIT;",
		"BEGIN; CREATE thing:6 SET n = 6; CANCEL;",
		"BEGIN; SELECT * FROM thing; THROW 'nope'; SELECT * FROM thing; COMMIT;",
		"BEGIN; SELECT * FROM thing; RETURN 'early'; SELECT * FROM thing; COMMIT;",
	] {
		assert_parity(sql).await;
	}
}

/// A statement that streams rows and then fails must terminate with an error,
/// and a consumer that trusted the rows before that would be wrong to keep
/// them. This is the case the buffered path cannot exhibit, so it is asserted
/// on the items themselves rather than through a parity comparison.
#[tokio::test]
async fn a_rolled_back_block_retracts_its_rows() {
	let ds = datastore().await;

	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let job = ds
		.execute_stream(
			"BEGIN; SELECT * FROM thing; THROW 'nope'; COMMIT;",
			&session(),
			None,
			None,
			tx,
		)
		.expect("the query parses");
	let drain = async {
		let mut items = Vec::new();
		while let Ok(item) = rx.recv().await {
			items.push(item);
		}
		items
	};
	let (outcome, items) = futures::future::join(job.run, drain).await;
	outcome.expect("the execution itself succeeds; the statement is what failed");

	let rows: Vec<&QueryStreamItem> =
		items.iter().filter(|i| matches!(i, QueryStreamItem::Rows { .. })).collect();
	assert!(!rows.is_empty(), "the SELECT's rows go out before the block resolves");

	// Every row precedes every terminal item: nothing is finalised until the
	// block does, which is what lets the rollback rewrite the results.
	let first_finished = items
		.iter()
		.position(|i| matches!(i, QueryStreamItem::Finished { .. }))
		.expect("the block resolves");
	let last_row = items
		.iter()
		.rposition(|i| matches!(i, QueryStreamItem::Rows { .. }))
		.expect("rows were sent");
	assert!(last_row < first_finished, "rows are provisional until the block resolves");

	// And the SELECT's own terminal item carries the failure, so a consumer
	// that already forwarded those rows learns they are void.
	let select_finished = items
		.iter()
		.find(|i| {
			matches!(
				i,
				QueryStreamItem::Finished {
					index: 1,
					..
				}
			)
		})
		.expect("the SELECT is statement 1");
	assert!(
		matches!(
			select_finished,
			QueryStreamItem::Finished {
				error: Some(_),
				..
			}
		),
		"the rolled-back SELECT must be reported as failed, got {select_finished:?}"
	);
}

/// Every statement that produces a result produces exactly one terminal item,
/// and no item follows it for that statement.
#[tokio::test]
async fn each_statement_is_terminated_exactly_once() {
	let ds = datastore().await;

	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let job = ds
		.execute_stream(
			"SELECT * FROM thing; RETURN 1; SELECT * FROM thing",
			&session(),
			None,
			None,
			tx,
		)
		.expect("the query parses");
	assert_eq!(job.statement_count, 3, "statement_count counts what parsed");

	let drain = async {
		let mut items = Vec::new();
		while let Ok(item) = rx.recv().await {
			items.push(item);
		}
		items
	};
	let (outcome, items) = futures::future::join(job.run, drain).await;
	outcome.expect("the query succeeds");

	for index in 0..3 {
		let terminals = items
			.iter()
			.filter(|i| matches!(i, QueryStreamItem::Finished { index: i, .. } if *i == index))
			.count();
		assert_eq!(terminals, 1, "statement {index} must be terminated exactly once");

		let last =
			items.iter().rposition(|i| i.index() == index).expect("every statement produced items");
		assert!(
			matches!(items[last], QueryStreamItem::Finished { .. }),
			"statement {index}'s last item must be its terminal one"
		);

		// Which payload item a statement contributes is what tells a consumer
		// whether to rebuild an array or take the value as it stands -- and
		// asserting the SELECTs produced rows is what keeps this suite honest,
		// since a silent fall back to buffering would send a single `Value`
		// carrying the whole array and still pass every parity check.
		let payload = items
			.iter()
			.find(|i| i.index() == index && !matches!(i, QueryStreamItem::Finished { .. }))
			.expect("every statement here produces a value");
		match index {
			1 => assert!(
				matches!(payload, QueryStreamItem::Value { .. }),
				"RETURN 1 is a single value, not rows"
			),
			_ => assert!(
				matches!(payload, QueryStreamItem::Rows { .. }),
				"a SELECT streams its rows, got {payload:?}"
			),
		}
	}
}

/// A consumer that stops reading stops the query, rather than letting it run to
/// completion into a channel nobody drains.
#[tokio::test]
async fn dropping_the_consumer_stops_the_execution() {
	let ds = datastore().await;

	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let job = ds.execute_stream("SELECT * FROM thing", &session(), None, None, tx).expect("parses");
	drop(rx);

	// With nothing draining, a send-blocking execution would hang here.
	let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), job.run).await.expect(
		"a dropped consumer must end the execution rather than blocking it on a full channel",
	);
	outcome.expect("abandoning the results is not itself an error");
}

/// Abandoning the stream abandons the statements that had not run yet.
///
/// The consumer going away stops only the statement that was producing rows
/// unless the executor checks at the statement boundary too -- and a script
/// whose later statements write would otherwise perform those writes for a
/// query nobody is listening to.
#[tokio::test]
async fn a_dropped_consumer_stops_later_statements() {
	let ds = datastore().await;
	// Enough rows that the first statement is still producing -- and so the
	// execution is parked on a full channel -- when the consumer goes away. A
	// small result would finish, and the statement boundary would be passed,
	// before the drop could be observed.
	for result in ds
		.execute(
			"DEFINE TABLE marker;
			 FOR $i IN array::range(0, 5000) { CREATE type::record('big', $i) SET n = $i }",
			&session(),
			None,
		)
		.await
		.expect("the seed runs")
	{
		result.result.expect("seeding succeeds");
	}

	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let job = ds
		.execute_stream("SELECT * FROM big; CREATE marker:1", &session(), None, None, tx)
		.expect("the query parses");
	// The execution has to be driven for anything to arrive, and the point of
	// the test is to stop reading while it is still going.
	let running = tokio::spawn(job.run);

	// Take one batch, leave the rest unread, then go away.
	let first = rx.recv().await.expect("at least one item");
	assert!(matches!(first, QueryStreamItem::Rows { .. }), "the SELECT streams first");
	drop(rx);

	running
		.await
		.expect("the execution task completes")
		.expect("abandoning the results is not itself an error");

	// The statement after the abandoned one never ran, so its write is absent.
	// The table is defined by the seed, so an empty answer here means the
	// `CREATE` did not happen rather than that the table is missing.
	let mut created = ds
		.execute("SELECT VALUE id FROM marker", &session(), None)
		.await
		.expect("the follow-up query runs");
	let rows = created.remove(0).result.expect("a result");
	assert_eq!(
		rows,
		PublicValue::Array(Vec::<PublicValue>::new().into()),
		"a write after the abandoned statement must not have run"
	);
}
