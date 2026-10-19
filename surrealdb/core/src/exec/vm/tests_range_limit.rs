//! A `LIMIT` on a record-id range bounds what the scan reads from storage, not
//! only what it returns.
//!
//! Each test counts the keys a read transaction reads while running one
//! statement, on two datastores that differ only in how many records the range
//! holds. A scan that stops at its limit reads the same keys on both; a scan
//! that reads the range before cutting it to the limit reads more on the
//! larger one.

use std::sync::Arc;

use surrealdb_kvs::consts::NORMAL_BATCH_SIZE;

use crate::exec::operators::test_util::TestDb;
use crate::kvs::TransactionType;
use crate::kvs::request::QueryRequest;

/// The records of `bucket_file` whose ids are `[bytes, int, bytes]`, the
/// shape of surrealdb/surrealdb#7474, all inside the range `[b"31"]..[b"39"]`, in key order by
/// `$i`.
fn files(rows: usize) -> String {
	format!(
		"INSERT INTO bucket_file (array::map(<array> 0..{rows}, |$i| {{
			id: [<bytes> <string> (10000 + $i), 0, <bytes> 'x'], host: 'example.com'
		}})) RETURN NONE;"
	)
}

/// The keys a read transaction reads running `sql` over `rows` records.
async fn keys_read(rows: usize, sql: &str) -> u64 {
	let db = TestDb::quiescent(&files(rows), false).await;
	let txn =
		Arc::new(db.datastore().transaction(TransactionType::Read).await.expect("a transaction"));
	let session = TestDb::owner();
	let request = QueryRequest::new(sql, &session).with_transaction(Arc::clone(&txn));
	db.datastore().run(request).await.expect("the request runs");
	let read = txn.metrics_snapshot_for_test().keys_read;
	txn.cancel().await.expect("the transaction ends");
	read
}

#[tokio::test]
async fn a_limited_record_id_range_reads_no_more_of_a_larger_range() {
	for sql in [
		"SELECT * FROM bucket_file:[b\"31\"]..[b\"39\"] LIMIT 3;",
		"SELECT id FROM bucket_file:[b\"31\"]..[b\"39\"] LIMIT 3;",
		"SELECT * FROM bucket_file:[b\"31\"]..[b\"39\"] START 5 LIMIT 3;",
	] {
		let small = keys_read(100, sql).await;
		let large = keys_read(5000, sql).await;
		assert_eq!(
			small, large,
			"{sql}: read {small} keys over 100 records and {large} over 5000, so the scan read \
			 past its limit"
		);
	}
}

/// An id filter over a table scan, the report's second form, stops at its first
/// match: the table holds the match fiftieth, and the scan reads no more of a
/// table four times the size. Both tables are larger than a cursor batch, so
/// what a batch reads past the match is the same on both.
#[tokio::test]
async fn a_limited_id_filter_over_a_table_scan_reads_no_more_of_a_larger_table() {
	let batch = NORMAL_BATCH_SIZE as usize;
	let sql = "SELECT id FROM bucket_file WHERE id >= bucket_file:[b\"3130303439\", NONE] LIMIT 1;";
	let small = keys_read(2 * batch, sql).await;
	let large = keys_read(8 * batch, sql).await;
	assert_eq!(
		small,
		large,
		"read {small} keys over {} records and {large} over {}, so the scan read past its first \
		 match",
		2 * batch,
		8 * batch
	);
	// Without the limit the same filter reads the whole table, so the count
	// tells the two tables apart.
	let unlimited = "SELECT id FROM bucket_file WHERE id >= bucket_file:[b\"3130303439\", NONE];";
	let small = keys_read(2 * batch, unlimited).await;
	let large = keys_read(8 * batch, unlimited).await;
	assert!(large >= small + 6 * batch as u64 - 1, "read {small} keys and {large} without a limit");
}

/// Control for the tests above: the same range without a limit reads every
/// record, so the key count does tell the two datastores apart.
#[tokio::test]
async fn an_unlimited_record_id_range_reads_every_record() {
	let sql = "SELECT * FROM bucket_file:[b\"31\"]..[b\"39\"];";
	let small = keys_read(100, sql).await;
	let large = keys_read(5000, sql).await;
	assert!(large >= small + 4900, "read {small} keys over 100 records and {large} over 5000");
}
