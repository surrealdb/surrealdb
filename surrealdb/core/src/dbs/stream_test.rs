//! Differential tests for the result stream.
//!
//! The contract is that streaming changes *when* a result reaches the caller,
//! never *what* it is. Each test therefore runs the same query both ways and
//! compares, rather than asserting a shape only one path produces.

use std::sync::Arc;

use surrealdb_rpc::{QUERY_STREAM_BUFFER, QueryResult};
use surrealdb_types::Value as PublicValue;

use crate::catalog::providers::CatalogProvider;
use crate::dbs::{QueryStreamItem, Session};
use crate::kvs::{Datastore, QueryRequest, TransactionType};

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
/// `USE` does not create a namespace, so the namespace must exist before any statement runs.
/// Each seed statement's own result is checked as well.
/// `execute` reports a per-statement failure inside its `Ok`, so a seed that
/// silently did nothing would leave the tests comparing two empty answers and
/// pass for the wrong reason.
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

/// Run a query through the result stream and collect every item the consumer
/// receives, in arrival order. `held` asks for explicit transactions to be held
/// until they resolve.
async fn streamed_items(
	ds: &Arc<Datastore>,
	sql: &str,
	held: bool,
) -> Result<Vec<QueryStreamItem>, String> {
	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let session = session();
	let mut request = QueryRequest::new(sql, &session);
	if held {
		request = request.with_held_transactions();
	}
	let job = ds.run_streaming(request, tx).map_err(|e| e.to_string())?;

	// The execution and the drain have to run together: the channel is bounded,
	// so an execution left to run alone would block forever on a full channel.
	let drain = async {
		let mut items = Vec::new();
		while let Ok(item) = rx.recv().await {
			items.push(item);
		}
		items
	};

	let (outcome, items) = futures::future::join(job.run, drain).await;
	outcome.map_err(|e| e.to_string())?;
	Ok(items)
}

/// What a consumer demultiplexing by statement index reassembles from `items`.
fn reassemble(items: Vec<QueryStreamItem>) -> Vec<QueryResult> {
	let mut statements: Vec<Option<Statement>> = Vec::new();
	for item in items {
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
			QueryStreamItem::Frame {
				rows,
				..
			} => statement.values.extend(rows.to_values().expect("rows decode")),
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
			QueryStreamItem::Trace {
				..
			} => {}
		}
	}
	// An index that produced no item at all belongs to a statement that control flow
	// skipped. It contributes no result, the same as in a non-streaming run.
	statements.into_iter().flatten().map(Statement::finish).collect()
}

/// Run a query through the result stream and reassemble what a consumer would
/// see, the way a client demultiplexing by statement index does.
async fn streamed(ds: &Arc<Datastore>, sql: &str, held: bool) -> Result<Vec<QueryResult>, String> {
	streamed_items(ds, sql, held).await.map(reassemble)
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
	assert_parity_with(sql, false).await;
}

/// [`assert_parity`], with the streamed run holding its explicit transactions.
async fn assert_parity_held(sql: &str) {
	assert_parity_with(sql, true).await;
}

async fn assert_parity_with(sql: &str, held: bool) {
	let ds = datastore().await;
	let streamed = streamed(&ds, sql, held).await;

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

/// Row-shaped, scalar-shaped, empty, multi-statement and transaction-block
/// queries.
const PARITY_QUERIES: &[&str] = &[
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
	"BEGIN; CREATE thing:1; BEGIN; CREATE thing:2; COMMIT; SELECT * FROM thing;",
	"CREATE thing:0; BEGIN; CREATE thing:1; RETURN 2;",
	"BEGIN; COMMIT; COMMIT; CANCEL; RETURN 1;",
	"BEGIN; RETURN 1; CREATE thing:1; COMMIT; SELECT * FROM thing;",
	"BEGIN; THROW 'x'; CREATE thing:1; CANCEL; RETURN 2;",
];

#[tokio::test]
async fn streaming_matches_buffered() {
	for sql in PARITY_QUERIES {
		assert_parity(sql).await;
	}
}

#[tokio::test]
async fn held_streaming_matches_buffered() {
	for sql in PARITY_QUERIES {
		assert_parity_held(sql).await;
	}
}

/// A statement that streams rows and then fails must end with an error, and a
/// consumer must not keep rows it received before that error.
/// A buffered run cannot produce this case, so the test checks the items
/// directly instead of comparing the two paths.
#[tokio::test]
async fn a_rolled_back_block_retracts_its_rows() {
	let ds = datastore().await;

	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let job = ds
		.run_streaming(
			QueryRequest::new("BEGIN; SELECT * FROM thing; THROW 'nope'; COMMIT;", &session()),
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
		.run_streaming(
			QueryRequest::new("SELECT * FROM thing; RETURN 1; SELECT * FROM thing", &session()),
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
	let job =
		ds.run_streaming(QueryRequest::new("SELECT * FROM thing", &session()), tx).expect("parses");
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
		.run_streaming(QueryRequest::new("SELECT * FROM big; CREATE marker:1", &session()), tx)
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

fn is_rows(item: &QueryStreamItem, index: usize) -> bool {
	matches!(item, QueryStreamItem::Rows { index: i, .. } if *i == index)
}

fn is_finished(item: &QueryStreamItem, index: usize) -> bool {
	matches!(item, QueryStreamItem::Finished { index: i, .. } if *i == index)
}

/// A held block sends no row while it runs: each of its statements arrives
/// whole once the block has committed, one after another.
#[tokio::test]
async fn a_held_block_sends_each_statement_whole_after_it_commits() {
	let ds = datastore().await;
	let sql = "BEGIN; SELECT * FROM thing; SELECT * FROM thing WHERE n > 1; COMMIT;";

	let items = streamed_items(&ds, sql, true).await.expect("the held query runs");
	let first_finished = items.iter().position(|i| is_finished(i, 1)).expect("statement 1 ends");
	let first_rows = items.iter().position(|i| is_rows(i, 2)).expect("statement 2 has rows");
	assert!(first_finished < first_rows, "statement 1 is complete before statement 2 starts");
	assert!(
		items.windows(2).all(|pair| pair[0].index() <= pair[1].index()),
		"every statement's items are contiguous and ascending"
	);
	let rows: Vec<_> = items
		.iter()
		.filter_map(|i| {
			if let QueryStreamItem::Rows {
				index: 1,
				values,
			} = i
			{
				Some(values.len())
			} else {
				None
			}
		})
		.collect();
	assert_eq!(rows, vec![3], "statement 1 is sent as one batch of every row");

	// The same query streamed provisionally interleaves the statements, which
	// is what makes the ordering above a property of holding.
	let ds = datastore().await;
	let items = streamed_items(&ds, sql, false).await.expect("the streamed query runs");
	let first_finished = items.iter().position(|i| is_finished(i, 1)).expect("statement 1 ends");
	let first_rows = items.iter().position(|i| is_rows(i, 2)).expect("statement 2 has rows");
	assert!(first_rows < first_finished, "a streamed block sends rows before it resolves");
}

/// A held block that does not commit sends no payload at all: every statement
/// in it is reported failed, and nothing was sent that needs retracting.
#[tokio::test]
async fn a_held_rolled_back_block_emits_no_rows() {
	// The second element is the slot of the `CANCEL` that ends the block, which
	// reports its own success after the block's statements have failed.
	for (sql, cancel_slot) in [
		("BEGIN; SELECT * FROM thing; THROW 'nope'; COMMIT;", None),
		("BEGIN; CREATE thing:6 SET n = 6; SELECT * FROM thing; CANCEL;", Some(3)),
		("BEGIN; SELECT * FROM thing;", None),
		("BEGIN; SELECT * FROM thing; BEGIN; COMMIT;", None),
	] {
		let ds = datastore().await;
		let items = streamed_items(&ds, sql, true).await.expect("the held query runs");
		assert!(
			!items.iter().any(|i| matches!(i, QueryStreamItem::Rows { .. })),
			"{sql}: a rolled-back held block sends no rows"
		);
		for item in &items {
			// Slot 0 is the `BEGIN`, which is not part of the block's payload.
			if item.index() == 0 || Some(item.index()) == cancel_slot {
				continue;
			}
			match item {
				QueryStreamItem::Value {
					..
				} => panic!("{sql}: a rolled-back held block sends no value, got {item:?}"),
				QueryStreamItem::Finished {
					error,
					..
				} => assert!(error.is_some(), "{sql}: {item:?} must carry the failure"),
				QueryStreamItem::Rows {
					..
				}
				| QueryStreamItem::Frame {
					..
				} => unreachable!("checked above"),
				QueryStreamItem::Trace {
					..
				} => unreachable!("no trace was asked for"),
			}
		}

		let held = reassemble(items);
		let ds = datastore().await;
		let buffered = ds.execute(sql, &session(), None).await.expect("the buffered query runs");
		same(&held, &buffered, sql);
	}
}

/// Holding changes when a statement is sent, not that it is terminated exactly
/// once with that terminal item last.
#[tokio::test]
async fn held_each_statement_is_terminated_exactly_once() {
	for sql in [
		"BEGIN; SELECT * FROM thing; RETURN 1; COMMIT;",
		"BEGIN; SELECT * FROM thing; CREATE thing:9; SELECT * FROM thing; COMMIT;",
	] {
		let ds = datastore().await;
		let items = streamed_items(&ds, sql, true).await.expect("the held query runs");
		let mut indices: Vec<usize> = items.iter().map(QueryStreamItem::index).collect();
		indices.dedup();
		assert!(!indices.is_empty(), "{sql}: the query produced items");
		for index in indices {
			let terminals = items.iter().filter(|i| is_finished(i, index)).count();
			assert_eq!(terminals, 1, "{sql}: statement {index} must be terminated exactly once");
			let last = items.iter().rposition(|i| i.index() == index).expect("it has items");
			assert!(
				is_finished(&items[last], index),
				"{sql}: statement {index}'s last item must be its terminal one"
			);
		}
	}
}

/// Holding applies to explicit transactions only: a statement outside a block
/// still streams its rows in batches.
#[tokio::test]
async fn a_held_request_streams_its_auto_commit_statements() {
	let ds = datastore().await;
	for result in ds
		.execute(
			"FOR $i IN array::range(0, 5000) { CREATE type::record('big', $i) SET n = $i }",
			&session(),
			None,
		)
		.await
		.expect("the seed runs")
	{
		result.result.expect("seeding succeeds");
	}

	let items = streamed_items(&ds, "SELECT * FROM big", true).await.expect("the query runs");
	let batches = items.iter().filter(|i| is_rows(i, 0)).count();
	assert!(batches > 1, "an auto-commit statement streams in batches, got {batches}");
}

/// Dropping the consumer still rolls a held block back.
#[tokio::test]
async fn a_dropped_consumer_rolls_back_a_held_block() {
	let ds = datastore().await;
	for result in ds.execute("DEFINE TABLE marker", &session(), None).await.expect("the seed runs")
	{
		result.result.expect("seeding succeeds");
	}

	let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
	let session = session();
	let job = ds
		.run_streaming(
			QueryRequest::new("BEGIN; SLEEP 1s; CREATE marker:1; COMMIT;", &session)
				.with_held_transactions(),
			tx,
		)
		.expect("the query parses");
	let running = tokio::spawn(job.run);

	// The drop lands while the block is inside `SLEEP`, after the top-level
	// boundary check, so it is the block's own check that abandons it.
	tokio::time::sleep(std::time::Duration::from_millis(200)).await;
	drop(rx);

	running
		.await
		.expect("the execution task completes")
		.expect("abandoning the results is not itself an error");

	let mut created = ds
		.execute("SELECT VALUE id FROM marker", &session, None)
		.await
		.expect("the follow-up query runs");
	assert_eq!(
		created.remove(0).result.expect("a result"),
		PublicValue::Array(Vec::<PublicValue>::new().into()),
		"the held block's write must not have committed"
	);
}

/// What kind of item this is, and which statement it is for.
fn shape(item: &QueryStreamItem) -> (usize, &'static str) {
	let kind = match item {
		QueryStreamItem::Rows {
			..
		}
		| QueryStreamItem::Frame {
			..
		} => "rows",
		QueryStreamItem::Value {
			..
		} => "value",
		QueryStreamItem::Finished {
			..
		} => "finished",
		QueryStreamItem::Trace {
			..
		} => "trace",
	};
	(item.index(), kind)
}

/// A request that runs inside a transaction its caller holds has no explicit
/// block of its own to hold, so asking for it changes nothing.
#[tokio::test]
async fn a_session_request_ignores_hold() {
	let mut shapes = Vec::new();
	for held in [true, false] {
		let ds = datastore().await;
		let txn = Arc::new(ds.transaction(TransactionType::Write).await.expect("a transaction"));
		let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
		let session = session();
		let mut request = QueryRequest::new("CREATE thing:7; SELECT * FROM thing", &session)
			.with_transaction(Arc::clone(&txn));
		if held {
			request = request.with_held_transactions();
		}
		let job = ds.run_streaming(request, tx).expect("the query parses");
		let drain = async {
			let mut items = Vec::new();
			while let Ok(item) = rx.recv().await {
				items.push(item);
			}
			items
		};
		let (outcome, items) = futures::future::join(job.run, drain).await;
		outcome.expect("the query succeeds");
		txn.cancel().await.expect("the transaction is cancelled");
		shapes.push(items.iter().map(shape).collect::<Vec<_>>());
	}
	assert_eq!(shapes[0], shapes[1], "holding changes nothing inside a caller's transaction");
}

/// Every index below `count` is terminated by exactly one `Finished` item, that
/// item is the index's last, none is past `count`, and the terminal items
/// arrive in statement order.
fn assert_each_terminated_once(items: &[QueryStreamItem], count: usize, sql: &str) {
	assert!(items.iter().all(|i| i.index() < count), "{sql}: an item past statement {count}");
	for index in 0..count {
		let terminals = items
			.iter()
			.filter(|i| matches!(i, QueryStreamItem::Finished { index: i, .. } if *i == index))
			.count();
		assert_eq!(terminals, 1, "{sql}: statement {index} must be terminated exactly once");
		let last = items.iter().rposition(|i| i.index() == index).expect("an item per statement");
		assert!(
			matches!(items[last], QueryStreamItem::Finished { .. }),
			"{sql}: statement {index}'s last item must be its terminal one"
		);
	}
	let order: Vec<usize> = items
		.iter()
		.filter_map(|i| {
			if let QueryStreamItem::Finished {
				index,
				..
			} = i
			{
				Some(*index)
			} else {
				None
			}
		})
		.collect();
	assert_eq!(order, (0..count).collect::<Vec<_>>(), "{sql}: terminal items out of order");
}

/// The `CANCEL` of a failed block answers NONE, and its slot reaches the
/// consumer as one terminal item in its place.
#[tokio::test]
async fn a_cancel_after_a_failure_is_terminated_once() {
	let sql = "BEGIN; THROW 'x'; CREATE thing:9; CANCEL; RETURN 2;";
	for held in [false, true] {
		let ds = datastore().await;
		let items = streamed_items(&ds, sql, held).await.expect("the request runs");
		assert_each_terminated_once(&items, 5, sql);
		let results = reassemble(items);
		assert_eq!(results.len(), 5, "{sql}: held {held}");
		assert!(results[3].result.is_ok(), "{sql}: CANCEL answers NONE, held {held}");
		assert!(results[4].result.is_ok(), "{sql}: held {held}");
	}
}

/// A `RETURN` inside a block leaves the items of later statements in place: each is
/// terminated once, and the block's rows are all sent.
#[tokio::test]
async fn a_return_in_a_block_keeps_later_items() {
	let sql = "BEGIN; CREATE thing:9; RETURN 1; SELECT * FROM thing; COMMIT;";
	for held in [false, true] {
		let ds = datastore().await;
		let items = streamed_items(&ds, sql, held).await.expect("the request runs");
		assert_each_terminated_once(&items, 5, sql);
		assert!(
			items.iter().any(|i| i.index() == 3 && !matches!(i, QueryStreamItem::Finished { .. })),
			"{sql}: the SELECT after the RETURN sends its rows, held {held}"
		);
		let results = reassemble(items);
		assert!(results.iter().all(|r| r.result.is_ok()), "{sql}: held {held}");
	}
}

/// A cancellation inside a block gives the statement observed at it a slot, so
/// every statement of the request is terminated once and in order.
#[tokio::test]
async fn a_cancellation_inside_a_block_terminates_every_statement_once() {
	let sql = "BEGIN; SELECT * FROM thing; SLEEP 300ms; SELECT * FROM thing; COMMIT;";
	for held in [false, true] {
		let ds = datastore().await;
		let handle = crate::ctx::CancelHandle::new();
		let (tx, rx) = async_channel::bounded(QUERY_STREAM_BUFFER);
		let session = session();
		let mut request = QueryRequest::new(sql, &session).with_cancel(handle.clone());
		if held {
			request = request.with_held_transactions();
		}
		let job = ds.run_streaming(request, tx).expect("the query parses");
		let drain = async {
			let mut items = Vec::new();
			while let Ok(item) = rx.recv().await {
				if item.index() == 1 {
					handle.trip();
				}
				items.push(item);
			}
			items
		};
		let (outcome, items) = futures::future::join(job.run, drain).await;
		outcome.expect("the request runs");
		// A held block sends nothing until it resolves, so there the cancellation
		// can only land once the block is over; the alignment holds either way.
		assert_each_terminated_once(&items, 5, sql);
	}
}
