//! Backend-level guarantees of the deferred data reclaim.
//!
//! `reclaim_test` covers the reclaim's shape on the memory backend, which
//! offers no out-of-transaction range destroy and so always takes the paged
//! path. These tests cover the two things a backend can get wrong that an
//! in-memory store cannot show: whether the keys are really gone at the raw
//! keyspace level rather than merely hidden from a typed read, and whether a
//! reclaim interrupted mid-prefix resumes after the store is closed and
//! reopened.
#![cfg(any(feature = "kv-mem", feature = "kv-surrealkv"))]

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
	open_as(path, node_id).await
}

/// [`open`] as node `node_id`: a test that arms a fault injection, which is
/// keyed by node, opens under an id of its own so no other test consumes it.
async fn open_as(path: &str, node_id: Uuid) -> Arc<Datastore> {
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
#[cfg(feature = "kv-surrealkv")]
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

#[cfg(feature = "kv-surrealkv")]
#[tokio::test]
async fn surrealkv_reclaim_resumes_across_a_restart() {
	use temp_dir::TempDir;

	let dir = TempDir::new().unwrap();
	let path = format!("surrealkv:{}", dir.path().to_string_lossy());
	reclaim_resumes_across_a_restart(&path).await;
}

/// The queued doc-ID reclaim entries of table `tb` in `test`/`tenant`.
async fn doc_id_reclaim_entries(ds: &Datastore, tb: &str) -> usize {
	use std::borrow::Cow;

	use surrealdb_strand::TableName;

	use crate::key::reclaim::ALL_DOC_ID_KINDS;
	use crate::key::schema::ReclaimTbPrefix;

	let tx = ds.transaction(Read).await.unwrap();
	let def = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
	let tb = TableName::from(tb);
	let mut count = 0;
	for kind in ALL_DOC_ID_KINDS {
		let range = ReclaimTbPrefix {
			kind,
			ns: def.namespace_id,
			db: def.database_id,
			tb: Cow::Borrowed(&tb),
		}
		.range()
		.unwrap();
		count += tx.count(range, None).await.unwrap();
	}
	let _ = tx.cancel().await;
	count
}

/// Run `sql` as one statement and fail on its error, without retrying it: a
/// statement a client would have to retry is what these tests rule out.
async fn run_once(ds: &Datastore, ses: &Session, sql: &str) {
	let mut response = ds.execute(sql, ses, None).await.unwrap();
	if let Err(e) = response.remove(0).result {
		panic!("`{sql}` failed: {e}");
	}
}

/// Wait until index `ix` on `thing` reports its build ready.
async fn index_ready(ds: &Datastore, ses: &Session, ix: &str) {
	let sql = format!("INFO FOR INDEX {ix} ON thing");
	common::time::timeout(Duration::from_secs(30), async {
		loop {
			let mut response = ds.execute(&sql, ses, None).await.unwrap();
			let json = response.remove(0).result.unwrap().into_json_value();
			match json.pointer("/building/status").and_then(|s| s.as_str()).unwrap_or_default() {
				"ready" => return,
				"error" => panic!("the index build failed: {json}"),
				_ => common::time::sleep(Duration::from_millis(20)).await,
			}
		}
	})
	.await
	.expect("the index build must finish");
}

/// The number of `thing` rows whose `val` is `v`, through the index or a scan.
async fn count_val(ds: &Datastore, ses: &Session, v: i64, noindex: bool) -> i64 {
	let with = if noindex {
		" WITH NOINDEX"
	} else {
		""
	};
	let sql = format!("SELECT count() FROM thing{with} WHERE val = {v} GROUP ALL");
	let mut response = ds.execute(&sql, ses, None).await.unwrap();
	let json = response.remove(0).result.unwrap().into_json_value();
	json.pointer("/0/count").and_then(|c| c.as_i64()).unwrap_or(0)
}

/// `DEFINE INDEX` straight after `REMOVE INDEX` on the same table succeeds
/// without the client retrying, under concurrent writes to the table, both
/// before the reclaim of the shared doc-ID space has started (the claim cancels
/// it) and once a page of it has landed (the statement finishes it).
///
/// The second case is what crud-bench met: a removal followed, after the
/// reclaim grace, by a definition that was refused with "still being
/// reclaimed". The index built over the reclaimed space must then agree with a
/// scan of the table.
async fn define_index_after_remove_index(path: &str) {
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = open(path).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	create_tenant(&ds, &ses).await;
	run_once(&ds, &ses, "DEFINE TABLE thing SCHEMALESS").await;
	run_once(&ds, &ses, "DEFINE INDEX idx ON thing FIELDS val CONCURRENTLY").await;
	// Enough records that the doc-ID space spans several reclaim pages.
	for chunk in 0..3 {
		let rows: Vec<String> = (chunk * page..(chunk + 1) * page)
			.map(|i| format!("{{ id: {i}, val: {} }}", i % 10))
			.collect();
		run_once(&ds, &ses, &format!("INSERT INTO thing [{}]", rows.join(","))).await;
	}
	index_ready(&ds, &ses, "idx").await;

	// Writers that create, update and delete records of the table throughout.
	let stop = Arc::new(AtomicBool::new(false));
	let written = Arc::new(AtomicUsize::new(0));
	let writers: Vec<_> = (0..4)
		.map(|w| {
			let ds = Arc::clone(&ds);
			let ses = ses.clone();
			let stop = Arc::clone(&stop);
			let written = Arc::clone(&written);
			tokio::spawn(async move {
				let mut n = 0usize;
				while !stop.load(Ordering::Relaxed) {
					let id = (w * 7919 + n * 104_729) % (page * 4);
					let sql = match n % 3 {
						0 => format!("UPSERT thing:{id} SET val = {}", n % 10),
						1 => format!("UPDATE thing:{id} SET val = {}", (n + 3) % 10),
						_ => format!("DELETE thing:{id}"),
					};
					if let Ok(mut response) = ds.execute(&sql, &ses, None).await
						&& response.remove(0).result.is_ok()
					{
						written.fetch_add(1, Ordering::Relaxed);
					}
					n += 1;
					tokio::task::yield_now().await;
				}
			})
		})
		.collect();

	// Before any reclaim page: the definition cancels the queued reclaim.
	run_once(&ds, &ses, "REMOVE INDEX idx ON thing").await;
	assert_eq!(
		doc_id_reclaim_entries(&ds, "thing").await,
		ALL_DOC_ID_KINDS_LEN,
		"removing the last doc-ID index queues the shared space"
	);
	run_once(&ds, &ses, "DEFINE INDEX idx ON thing FIELDS val CONCURRENTLY").await;
	assert_eq!(doc_id_reclaim_entries(&ds, "thing").await, 0, "the definition claims the space");
	index_ready(&ds, &ses, "idx").await;

	// After a reclaim page has landed: the space is torn, and the definition
	// finishes the reclaim itself rather than refusing.
	run_once(&ds, &ses, "REMOVE INDEX idx ON thing").await;
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert!(
		doc_id_reclaim_entries(&ds, "thing").await > 0,
		"one page must leave the reclaim started but unfinished"
	);
	run_once(&ds, &ses, "DEFINE INDEX idx ON thing FIELDS val CONCURRENTLY").await;
	assert_eq!(
		doc_id_reclaim_entries(&ds, "thing").await,
		0,
		"the definition must have finished the reclaim"
	);
	index_ready(&ds, &ses, "idx").await;

	stop.store(true, Ordering::Relaxed);
	for writer in writers {
		writer.await.unwrap();
	}
	assert!(written.load(Ordering::Relaxed) > 0, "the writers must have written");
	for v in 0..10 {
		assert_eq!(
			count_val(&ds, &ses, v, false).await,
			count_val(&ds, &ses, v, true).await,
			"the index over the reclaimed space must agree with a scan for val = {v}"
		);
	}
	close(ds).await;
}

const ALL_DOC_ID_KINDS_LEN: usize = crate::key::reclaim::ALL_DOC_ID_KINDS.len();

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_define_index_after_remove_index() {
	define_index_after_remove_index("memory").await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn surrealkv_define_index_after_remove_index() {
	use temp_dir::TempDir;

	let dir = TempDir::new().unwrap();
	let path = format!("surrealkv:{}", dir.path().to_string_lossy());
	define_index_after_remove_index(&path).await;
}

/// Define `thing` with a doc-ID index over `pages` reclaim pages of records,
/// remove the index, and land one page of the reclaim, so the table's shared
/// doc-ID space is torn and its reclaim started but unfinished.
async fn torn_doc_id_space(ds: &Arc<Datastore>, ses: &Session, pages: usize) {
	let page = RECLAIM_BATCH_SIZE as usize;
	create_tenant(ds, ses).await;
	run_once(ds, ses, "DEFINE TABLE thing SCHEMALESS").await;
	run_once(ds, ses, "DEFINE INDEX idx ON thing FIELDS val CONCURRENTLY").await;
	for chunk in 0..pages {
		let rows: Vec<String> = (chunk * page..(chunk + 1) * page)
			.map(|i| format!("{{ id: {i}, val: {} }}", i % 10))
			.collect();
		run_once(ds, ses, &format!("INSERT INTO thing [{}]", rows.join(","))).await;
	}
	index_ready(ds, ses, "idx").await;
	run_once(ds, ses, "REMOVE INDEX idx ON thing").await;
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert!(
		doc_id_reclaim_entries(ds, "thing").await > 0,
		"one page must leave the reclaim started but unfinished"
	);
}

/// The index over a reclaimed space agrees with a scan of the table.
async fn index_agrees_with_scan(ds: &Datastore, ses: &Session) {
	for v in 0..10 {
		assert_eq!(
			count_val(ds, ses, v, false).await,
			count_val(ds, ses, v, true).await,
			"the index over the reclaimed space must agree with a scan for val = {v}"
		);
	}
}

/// `DEFINE INDEX` over a torn doc-ID space succeeds without the client retrying
/// while this node's reaper drains the same space.
///
/// The statement and the reaper page the same queue entries, and on a backend
/// that detects write-write conflicts at commit whichever commits second loses
/// with a retryable conflict. The statement's loss is the race's, not the
/// client's: it must read what the winner left and carry on.
async fn define_index_while_the_reaper_drains(path: &str) {
	use std::sync::atomic::{AtomicBool, Ordering};

	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = open_as(path, Uuid::new_v4()).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	torn_doc_id_space(&ds, &ses, 24).await;

	// Reaper passes back to back, as the background task runs them once the
	// grace has passed and a pass leaves work queued.
	let stop = Arc::new(AtomicBool::new(false));
	let reaper = {
		let ds = Arc::clone(&ds);
		let stop = Arc::clone(&stop);
		tokio::spawn(async move {
			while !stop.load(Ordering::Relaxed) {
				let _ = Datastore::reclaim_tombstones_with_budget(
					Arc::clone(&ds),
					Duration::from_secs(1),
					Duration::ZERO,
					page as u64,
					CancellationToken::new(),
				)
				.await;
				tokio::task::yield_now().await;
			}
		})
	};
	run_once(&ds, &ses, "DEFINE INDEX idx ON thing FIELDS val CONCURRENTLY").await;
	stop.store(true, Ordering::Relaxed);
	reaper.await.unwrap();
	assert_eq!(
		doc_id_reclaim_entries(&ds, "thing").await,
		0,
		"the definition must have finished the reclaim"
	);
	index_ready(&ds, &ses, "idx").await;
	index_agrees_with_scan(&ds, &ses).await;
	close(ds).await;
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_define_index_while_the_reaper_drains() {
	define_index_while_the_reaper_drains("memory").await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn surrealkv_define_index_while_the_reaper_drains() {
	use temp_dir::TempDir;

	let dir = TempDir::new().unwrap();
	let path = format!("surrealkv:{}", dir.path().to_string_lossy());
	define_index_while_the_reaper_drains(&path).await;
}

/// A page of the reclaim a `DEFINE INDEX` runs that loses its commit to a
/// concurrent page does not fail the statement: the next step reads what the
/// winner left. The loss is injected, so it lands on every backend and on the
/// statement's page rather than the reaper's.
async fn define_index_survives_a_lost_reclaim_page(path: &str) {
	use crate::kvs::testing::{
		RetryableConflictSite, inject_retryable_conflict, retryable_conflict_count,
	};

	let ds = open_as(path, Uuid::new_v4()).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	torn_doc_id_space(&ds, &ses, 3).await;
	let site = RetryableConflictSite::ReclaimPageCommit;
	let _lost = inject_retryable_conflict(site, ds.id());
	run_once(&ds, &ses, "DEFINE INDEX idx ON thing FIELDS val CONCURRENTLY").await;
	assert_eq!(retryable_conflict_count(site, ds.id()), 0, "a reclaim page must have lost");
	assert_eq!(
		doc_id_reclaim_entries(&ds, "thing").await,
		0,
		"the definition must have finished the reclaim"
	);
	index_ready(&ds, &ses, "idx").await;
	index_agrees_with_scan(&ds, &ses).await;
	close(ds).await;
}

#[cfg(feature = "kv-mem")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mem_define_index_survives_a_lost_reclaim_page() {
	define_index_survives_a_lost_reclaim_page("memory").await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn surrealkv_define_index_survives_a_lost_reclaim_page() {
	use temp_dir::TempDir;

	let dir = TempDir::new().unwrap();
	let path = format!("surrealkv:{}", dir.path().to_string_lossy());
	define_index_survives_a_lost_reclaim_page(&path).await;
}
