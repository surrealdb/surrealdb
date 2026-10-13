//! Backend-level guarantees of the deferred data reclaim.
//!
//! `reclaim_test` covers the reclaim's shape on the memory backend, which
//! offers no out-of-transaction range destroy and so always takes the paged
//! path. These tests cover the two things a backend can get wrong that an
//! in-memory store cannot show: whether the keys are really gone at the raw
//! keyspace level rather than merely hidden from a typed read, and whether a
//! reclaim interrupted mid-prefix resumes after the store is closed and
//! reopened.
#![cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-surrealkv"))]

use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use web_time::Duration;

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::{Capabilities, Session};
use crate::key::schema::{DbRoot, ReclaimPrefix};
use crate::key::{AnyRange, Key, RawRange};
use crate::kvs::{Datastore, RECLAIM_BATCH_SIZE};

/// Open a datastore at `path` with background jobs off, so every reclaim in
/// these tests is one the test drove.
///
/// The node id is fixed rather than generated, so reopening a store models the
/// same node restarting: the maintenance-task lease its previous instance took
/// is renewable by the reopened one instead of having to expire first.
async fn open(path: &str) -> Arc<Datastore> {
	let node_id = Uuid::parse_str("6f1c9d3e-3a12-4a7b-9c4e-5b8d2f0a7e11").unwrap();
	Datastore::builder()
		.with_id(node_id)
		.with_capabilities(Capabilities::all())
		.without_maintenance_tasks()
		.build_with_path(path)
		.await
		.unwrap()
}

/// Close a datastore so an on-disk store can be reopened at the same path.
async fn close(ds: Arc<Datastore>) {
	ds.shutdown().await.unwrap();
	drop(ds);
}

/// Number of pending entries in the background reclaim queue.
async fn reclaim_queue_len(ds: &Datastore) -> usize {
	let range = ReclaimPrefix {}.range().unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let count = tx.count(range, None).await.unwrap();
	let _ = tx.cancel().await;
	count
}

/// Count the keys currently stored in a range.
async fn count_range(ds: &Datastore, range: impl AnyRange) -> usize {
	let tx = ds.transaction(Read).await.unwrap();
	let count = tx.count(range, None).await.unwrap();
	let _ = tx.cancel().await;
	count
}

/// Create the `test`/`tenant` database and return its data prefix.
async fn create_tenant(ds: &Datastore, ses: &Session) -> RawRange {
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", ses, None).await.unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let def = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
	let _ = tx.cancel().await;
	DbRoot {
		ns: def.namespace_id,
		db: def.database_id,
	}
	.range()
	.unwrap()
}

/// Write `count` synthetic keys beneath `range`, in batches so no single
/// transaction has to hold the whole seed.
async fn seed_range(ds: &Datastore, range: &RawRange, count: usize) {
	let base = range.start().as_ref().to_vec();
	for chunk in 0..count.div_ceil(500) {
		let tx = ds.transaction(Write).await.unwrap();
		for i in (chunk * 500)..(chunk * 500 + 500).min(count) {
			let mut key = base.clone();
			key.extend_from_slice(&(i as u64).to_be_bytes());
			tx.set(Key::from(key), vec![1u8]).await.unwrap();
		}
		tx.commit().await.unwrap();
	}
}

/// A reclaim leaves nothing of the prefix behind, at the raw keyspace level.
///
/// The typed count and the raw range read are both asserted because they can
/// disagree: a backend can stop serving a key through the layer a count goes
/// through while the bytes are still in the store. For an on-disk store the
/// checks are repeated after a close and reopen, which is what catches a delete
/// that only ever existed in a memory layer.
async fn reclaim_destroys_prefix(path: &str, reopen: bool) {
	let ds = open(path).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	let range = create_tenant(&ds, &ses).await;
	seed_range(&ds, &range, RECLAIM_BATCH_SIZE as usize * 2).await;
	assert!(count_range(&ds, range.clone()).await > 0);

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	let assert_empty = |ds: Arc<Datastore>, range: RawRange| async move {
		let tx = ds.transaction(Read).await.unwrap();
		assert_eq!(tx.count(range.clone(), None).await.unwrap(), 0, "the prefix must be empty");
		assert!(
			tx.getr_raw(range, None).await.unwrap().is_empty(),
			"no key of the prefix may remain in the raw keyspace"
		);
		let _ = tx.cancel().await;
	};
	assert_empty(Arc::clone(&ds), range.clone()).await;
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the reclaim queue must be drained");

	if reopen {
		close(ds).await;
		let ds = open(path).await;
		assert_empty(Arc::clone(&ds), range).await;
		assert_eq!(reclaim_queue_len(&ds).await, 0, "the drained queue must stay drained");
		close(ds).await;
	} else {
		close(ds).await;
	}
}

#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn mem_reclaim_destroys_prefix() {
	reclaim_destroys_prefix("memory", false).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test]
async fn rocksdb_reclaim_destroys_prefix() {
	use temp_dir::TempDir;

	let dir = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", dir.path().to_string_lossy());
	reclaim_destroys_prefix(&path, true).await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn surrealkv_reclaim_destroys_prefix() {
	use temp_dir::TempDir;

	let dir = TempDir::new().unwrap();
	let path = format!("surrealkv:{}", dir.path().to_string_lossy());
	reclaim_destroys_prefix(&path, true).await;
}

/// A reclaim interrupted mid-prefix resumes where it stopped, across a restart.
///
/// The resume cursor commits with the page of deletions it accounts for, so
/// closing the store between passes must cost nothing: each later pass deletes a
/// page it has not deleted before, and the queue entry survives to name the
/// remainder. Only meaningful on a store that pages — a backend with an
/// out-of-transaction range destroy empties the prefix in one call and has no
/// cursor to resume from.
#[cfg(any(feature = "kv-rocksdb", feature = "kv-surrealkv"))]
async fn reclaim_resumes_across_a_restart(path: &str) {
	use crate::key::KVKeyDecode;
	use crate::key::schema::ReclaimKey;

	let page = RECLAIM_BATCH_SIZE as u64;

	// Seed three pages, remove the database, and delete exactly one page.
	let ds = open(path).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	let range = create_tenant(&ds, &ses).await;
	let baseline = count_range(&ds, range.clone()).await;
	seed_range(&ds, &range, page as usize * 3 - baseline).await;
	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	let total = count_range(&ds, range.clone()).await as u64;
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	let after_first = count_range(&ds, range.clone()).await as u64;
	assert_eq!(after_first, total - page, "the first pass must delete exactly one page");
	close(ds).await;

	// Reopen: the queue entry and its cursor are still there.
	let ds = open(path).await;
	assert_eq!(reclaim_queue_len(&ds).await, 1, "the queue entry must survive the restart");
	let cursor = {
		let tx = ds.transaction(Read).await.unwrap();
		let items = tx.getr(ReclaimPrefix {}.range().unwrap(), None).await.unwrap();
		let _ = tx.cancel().await;
		// Decoding the key asserts the entry is still the one that was enqueued.
		ReclaimKey::decode_key(&items[0].0).unwrap();
		items[0].1.cursor.clone()
	};
	assert!(cursor.is_some(), "the resume cursor must survive the restart");

	// A second budgeted pass advances by exactly one more page rather than
	// redoing the first.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	let after_second = count_range(&ds, range.clone()).await as u64;
	assert_eq!(after_second, after_first - page, "the second pass must resume, not restart");

	// And an unbudgeted pass finishes it.
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(count_range(&ds, range).await, 0, "the prefix must end up empty");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "an emptied prefix must retire its queue entry");
	close(ds).await;
}

#[cfg(feature = "kv-rocksdb")]
#[tokio::test]
async fn rocksdb_reclaim_resumes_across_a_restart() {
	use temp_dir::TempDir;

	let dir = TempDir::new().unwrap();
	let path = format!("rocksdb:{}", dir.path().to_string_lossy());
	reclaim_resumes_across_a_restart(&path).await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn surrealkv_reclaim_resumes_across_a_restart() {
	use temp_dir::TempDir;

	let dir = TempDir::new().unwrap();
	let path = format!("surrealkv:{}", dir.path().to_string_lossy());
	reclaim_resumes_across_a_restart(&path).await;
}
