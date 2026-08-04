//! The streaming query API against an embedded engine.
//!
//! An embedded connection owns its datastore, so it streams for real rather
//! than replaying a finished result. That makes it the cheapest place to pin
//! what a caller sees: the same items, in the same order, as any other
//! connection that streams.

#![cfg(feature = "kv-mem")]

use futures::StreamExt;
use surrealdb::Surreal;
use surrealdb::engine::local::Mem;
use surrealdb::method::StreamItem;

async fn db() -> Surreal<surrealdb::engine::local::Db> {
	let db = Surreal::new::<Mem>(()).await.expect("an embedded connection");
	db.use_ns("test").use_db("test").await.expect("use");
	db
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
