//! The streaming query API against an embedded engine.
//!
//! An embedded connection owns its datastore, so it streams for real rather
//! than replaying a finished result. That makes it the cheapest place to pin
//! what a caller sees: the same items, in the same order, as any other
//! connection that streams.

#![cfg(feature = "kv-mem")]

use std::time::Duration;

use futures::StreamExt;
use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::method::StreamItem;

async fn db() -> Surreal<surrealdb::engine::local::Db> {
	let db = Surreal::new::<Mem>(()).await.expect("an embedded connection");
	db.use_ns("test").use_db("test").await.expect("use");
	db
}

/// How many tasks are alive on the runtime running this test.
///
/// A streaming query occupies one: `stream_items` spawns it to drive the
/// execution against the caller's reads, and the embedded engine runs the
/// execution on that same task. It must retire once the stream is dropped, and
/// the count is how that is observed — an execution that is merely parked is
/// still alive, where one that reached its end is not.
///
/// The count is exact here because a `#[tokio::test]` runs on a current-thread
/// runtime; the metric carries no such guarantee on a multi-threaded one.
fn alive_tasks() -> usize {
	tokio::runtime::Handle::current().metrics().num_alive_tasks()
}

/// Waits for the alive-task count to fall to `target`, returning what it
/// reached. Bounded, so a task that never retires fails an assertion rather
/// than hanging the test.
async fn wait_for_tasks(target: usize) -> usize {
	let mut alive = alive_tasks();
	for _ in 0..200 {
		if alive <= target {
			break;
		}
		tokio::time::sleep(Duration::from_millis(25)).await;
		alive = alive_tasks();
	}
	alive
}

/// The alive-task count once it has stopped changing.
///
/// Every request the SDK makes occupies a task while it runs, so a count taken
/// while an earlier one is still retiring would read as a task that leaked.
async fn settled_tasks() -> usize {
	let mut last = alive_tasks();
	for _ in 0..200 {
		tokio::time::sleep(Duration::from_millis(25)).await;
		let now = alive_tasks();
		if now == last {
			return now;
		}
		last = now;
	}
	last
}

/// Rows arrive one at a time, and each statement is terminated exactly once,
/// after its own rows.
#[tokio::test]
async fn rows_and_statement_ends_arrive_in_order() {
	let db = db().await;
	db.query("FOR $i IN array::range(0, 100) { CREATE type::record('row', $i) SET n = $i }")
		.await
		.expect("send")
		.check()
		.expect("seed");

	let mut items =
		db.query("SELECT * FROM row").query("RETURN 'done'").stream_items().expect("a stream");

	let mut rows = 0usize;
	let mut ends = Vec::new();
	while let Some(item) = items.next().await {
		match item.expect("no stream-level failure") {
			StreamItem::Row {
				statement,
				..
			} => {
				assert!(
					!ends.contains(&statement),
					"statement {statement} produced a row after it finished"
				);
				rows += 1;
			}
			StreamItem::StatementEnd {
				statement,
				result,
				..
			} => {
				result.expect("both statements succeed");
				ends.push(statement);
			}
		}
	}
	assert_eq!(rows, 101, "every row, plus the second statement's one value");
	assert_eq!(ends, vec![0, 1]);
}

/// The streamed rows are the same rows the buffered API returns, so choosing
/// one over the other is a question of when results arrive, not what they are.
#[tokio::test]
async fn streaming_and_buffered_agree() {
	let db = db().await;
	db.query("FOR $i IN array::range(0, 50) { CREATE type::record('row', $i) SET n = $i }")
		.await
		.expect("send")
		.check()
		.expect("seed");

	let mut buffered = db.query("SELECT n FROM row ORDER BY n").await.expect("send");
	let buffered: Vec<surrealdb::types::Value> = buffered.take(0).expect("rows");

	let mut items = db.query("SELECT n FROM row ORDER BY n").stream_items().expect("a stream");
	let mut streamed = Vec::new();
	while let Some(item) = items.next().await {
		if let StreamItem::Row {
			value,
			..
		} = item.expect("no stream-level failure")
		{
			streamed.push(value);
		}
	}

	assert_eq!(streamed, buffered, "the same rows, in the same order");
}

/// A statement's own failure is reported on its `StatementEnd`, leaving the
/// statements around it usable.
#[tokio::test]
async fn a_failed_statement_ends_with_its_error() {
	let db = db().await;
	let mut items = db
		.query("RETURN 1")
		.query("THROW 'nope'")
		.query("RETURN 3")
		.stream_items()
		.expect("a stream");

	let mut outcomes = Vec::new();
	while let Some(item) = items.next().await {
		if let StreamItem::StatementEnd {
			statement,
			result,
			..
		} = item.expect("no stream-level failure")
		{
			outcomes.push((statement, result.is_ok()));
		}
	}
	assert_eq!(outcomes, vec![(0, true), (1, false), (2, true)]);
}

/// Abandoning the stream part-way leaves the connection usable, rather than
/// wedging it behind a query nobody is reading.
#[tokio::test]
async fn a_dropped_stream_releases_the_connection() {
	let db = db().await;
	db.query("FOR $i IN array::range(0, 500) { CREATE type::record('row', $i) SET n = $i }")
		.await
		.expect("send")
		.check()
		.expect("seed");

	let mut items = db.query("SELECT * FROM row").stream_items().expect("a stream");
	items.next().await.expect("at least one item").expect("no stream-level failure");
	drop(items);

	let mut response =
		tokio::time::timeout(std::time::Duration::from_secs(10), db.query("RETURN 'still here'"))
			.await
			.expect("the connection is usable after abandoning a stream")
			.expect("send");
	let answer: Option<String> = response.take(0).expect("a result");
	assert_eq!(answer.as_deref(), Some("still here"));
}

/// Abandoning a stream stops the execution behind it.
///
/// The execution holds an open transaction and finalises it on its own
/// completion path — committing or cancelling as its outcome requires — so an
/// execution that never completes never finalises it. Dropping the stream is
/// therefore not enough on its own: the execution has to be told to stop, and
/// then run to completion so that finalisation happens.
///
/// The observable form of "never completes" is a task that stays alive with
/// nothing left to wake it. The query's task retiring is what says the execution
/// reached its end rather than parking on a buffer no one will drain again.
///
/// The row count is what makes this a test rather than a tautology: the query
/// must be nowhere near finished when the stream is dropped. The buffers between
/// the executor and the caller hold a few thousand rows between them, so a
/// smaller result would already be produced in full and its tasks would retire
/// whether or not anything stopped them.
#[tokio::test]
async fn a_dropped_stream_stops_the_execution_behind_it() {
	let db = db().await;
	db.query("FOR $i IN array::range(0, 10000) { CREATE type::record('row', $i) SET n = $i }")
		.await
		.expect("send")
		.check()
		.expect("seed");

	// The seed's own task has to retire before the count means anything,
	// or it would be indistinguishable from a leaked one.
	let baseline = settled_tasks().await;

	let mut items = db.query("SELECT * FROM row").stream_items().expect("a stream");
	items.next().await.expect("at least one item").expect("no stream-level failure");
	assert!(
		alive_tasks() > baseline,
		"the execution is running while the stream is being read, or the count below proves nothing",
	);

	drop(items);

	let alive = wait_for_tasks(baseline).await;
	assert_eq!(
		alive, baseline,
		"the execution is still alive after its stream was dropped, holding a transaction that \
		 will never be committed or cancelled",
	);
}
