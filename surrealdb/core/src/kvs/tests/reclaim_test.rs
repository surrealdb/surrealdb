//! Tests for the deferred `REMOVE DATABASE/NAMESPACE/INDEX` data reclaim.
//!
//! `REMOVE` only deletes the catalog definition inside the user transaction and
//! enqueues a reclaim job (`/!rc`); the data prefix is destroyed later by the
//! background reclaim task [`Datastore::reclaim_tombstones`]. These tests verify
//! that the object becomes immediately invisible, that the reclaim task
//! physically reclaims the data and clears the queue, and that every part of it
//! stays bounded: the data delete pages behind a durable cursor, the queue is
//! walked in batches, and one pass's key budget is shared across the entries it
//! visits rather than spent on the first. So a prefix of any size is reclaimed
//! with memory proportional to one page, an interrupted reclaim resumes instead
//! of restarting, and no queued entry waits on another to finish.

use std::any::{Any, TypeId};
use std::borrow::Cow;
use std::sync::{Arc, Mutex};

use common::future::BoxFut;
use surrealdb_cnf::ConfigMap;
use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_kvs::{
	DestroyRange, DestroyRangeHandle, KeyRange, Metrics, Result as KvsResult, Transactable,
	TransactionBuilder, TransactionType,
};
use surrealdb_strand::TableName;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use web_time::{Duration, SystemTime};

use crate::CommunityComposer;
use crate::catalog::providers::{DatabaseProvider, TableProvider};
use crate::catalog::{DatabaseId, IndexId, NamespaceId};
use crate::dbs::{Capabilities, Session};
use crate::key::reclaim::{ALL_DOC_ID_KINDS, Expunge, ReclaimKind, ReclaimState};
use crate::key::schema::{
	DbRoot, DocKeyPrefix, DocLookupIdentityPrefix, DocLookupPrefix, DocPendingPrefix, IdxRoot,
	NsRoot, ReclaimKey, ReclaimPrefix, ReclaimTbPrefix,
};
use crate::key::{AnyRange, KVKeyDecode, KVSubspace, Key, RawRange};
use crate::kvs::tasks::{Pass, Progress};
use crate::kvs::test_support::{WriteTxSizeObserver, cleanup_sizes};
use crate::kvs::tx::DocIdsReclaimClaim;
use crate::kvs::{
	Datastore, RECLAIM_BATCH_SIZE, RECLAIM_PASS_KEY_BUDGET, RECLAIM_PASS_WALK_BATCHES,
	TransactionBuilderFactory, TransactionBuilderParts,
};
use crate::observe::ExecutionObserver;

async fn mem_ds() -> Arc<Datastore> {
	Datastore::builder()
		.with_capabilities(Capabilities::all())
		.build_with_path("memory")
		.await
		.unwrap()
}

/// A memory datastore whose every write transaction is recorded, and whose
/// background jobs are off so the only transactions observed are the ones the
/// test drives.
async fn observed_ds(observer: Arc<WriteTxSizeObserver>) -> Arc<Datastore> {
	Datastore::builder()
		.with_capabilities(Capabilities::all())
		.without_maintenance_tasks()
		.with_observer(observer as Arc<dyn ExecutionObserver>)
		.build_with_path("memory")
		.await
		.unwrap()
}

/// Count the keys currently stored in a range.
async fn count_range(ds: &Datastore, range: impl AnyRange) -> usize {
	let tx = ds.transaction(Read).await.unwrap();
	let count = tx.count(range, None).await.unwrap();
	let _ = tx.cancel().await;
	count
}

/// Number of pending entries in the background reclaim queue.
async fn reclaim_queue_len(ds: &Datastore) -> usize {
	let range = ReclaimPrefix {}.range().unwrap();
	count_range(ds, range).await
}

/// The kind of every queued reclaim entry, in queue order.
///
/// One removal defers more than the object it names — a cascading parent
/// removal also queues the post-builder reclaim for each index beneath it, and
/// dropping a table's last doc-ID consumer queues every shared doc-ID
/// prefixes — so an assertion about what a statement enqueued names the kinds it
/// is responsible for. The queue's total length is only meaningful where the
/// expectation is that it is empty.
async fn reclaim_queue_kinds(ds: &Datastore) -> Vec<ReclaimKind> {
	let range = ReclaimPrefix {}.range().unwrap();
	keys_in_range(ds, range)
		.await
		.iter()
		.map(|k| ReclaimKey::decode_key(k).expect("a queued entry must decode").kind)
		.collect()
}

/// How many of `kinds` are `kind`.
fn count_kind(kinds: &[ReclaimKind], kind: ReclaimKind) -> usize {
	kinds.iter().filter(|k| **k == kind).count()
}

/// How many of `kinds` name a shared doc-ID prefix.
fn count_doc_kinds(kinds: &[ReclaimKind]) -> usize {
	kinds.iter().filter(|k| k.is_doc_id()).count()
}

/// The key and value of the single pending reclaim entry (asserts there is
/// exactly one).
async fn reclaim_entry(ds: &Datastore) -> (Vec<u8>, ReclaimState) {
	let range = ReclaimPrefix {}.range().unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let items = tx.getr(range, None).await.unwrap();
	let _ = tx.cancel().await;
	assert_eq!(items.len(), 1, "expected exactly one reclaim queue entry");
	items[0].clone()
}

/// The `observed_ms` stamp of the single pending reclaim entry. `0` means the
/// reclaim task has not yet observed it.
async fn reclaim_entry_observed_ms(ds: &Datastore) -> u64 {
	reclaim_entry(ds).await.1.observed_ms
}

fn now_ms() -> u64 {
	SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64
}

/// The internal ids of a namespace and database, by name.
async fn ids(ds: &Datastore, ns: &str, db: &str) -> (NamespaceId, DatabaseId) {
	let tx = ds.transaction(Read).await.unwrap();
	let def = tx.get_db_by_name(ns, db, None).await.unwrap().unwrap();
	let _ = tx.cancel().await;
	(def.namespace_id, def.database_id)
}

/// The internal id of an index, by name.
async fn index_id(ds: &Datastore, ns: NamespaceId, db: DatabaseId, tb: &str, ix: &str) -> IndexId {
	let table = TableName::from(tb);
	let tx = ds.transaction(Read).await.unwrap();
	let def = tx.get_tb_index(ns, db, &table, ix, None).await.unwrap().unwrap();
	let _ = tx.cancel().await;
	def.index_id
}

/// Write `count` synthetic keys beneath `range`, so a test can size a prefix
/// precisely.
///
/// Seeded as raw bytes rather than through `CREATE`: a record write also lands
/// table, index and count keys under the same prefix, which would make the
/// prefix's cardinality — and so the expected page sequence — inexact. The
/// reclaim deletes and counts by bytes without decoding, so any suffix beneath
/// the prefix is indistinguishable from real data to it.
async fn seed_range(ds: &Datastore, range: &RawRange, count: usize) {
	let base = range.start().as_ref().to_vec();
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..count {
		let mut key = base.clone();
		key.extend_from_slice(&(i as u64).to_be_bytes());
		tx.set(Key::from(key), vec![1u8]).await.unwrap();
	}
	tx.commit().await.unwrap();
}

/// The keys currently stored in a range, in key order.
async fn keys_in_range(ds: &Datastore, range: impl AnyRange) -> Vec<Vec<u8>> {
	let tx = ds.transaction(Read).await.unwrap();
	let keys = tx.keys_raw(range, u32::MAX, 0, None).await.unwrap();
	let _ = tx.cancel().await;
	keys
}

/// The write-transaction sizes a paged reclaim of `total` keys must produce:
/// one committed transaction per page of at most [`RECLAIM_BATCH_SIZE`]
/// deletes, each carrying the single resume-cursor write that commits with it.
fn expected_page_sizes(total: usize) -> Vec<u64> {
	let page = RECLAIM_BATCH_SIZE as usize;
	let mut sizes = Vec::new();
	let mut left = total;
	while left > 0 {
		let deletes = left.min(page);
		sizes.push(deletes as u64 + 1);
		left -= deletes;
	}
	sizes
}

#[tokio::test]
async fn remove_database_defers_data_reclaim() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");

	// Create a tenant database with data.
	ds.execute(
		"DEFINE NAMESPACE test; DEFINE DATABASE tenant; CREATE thing:1 SET v = 1; CREATE thing:2 SET v = 2;",
		&ses,
		None,
	)
	.await
	.unwrap();

	// Resolve the internal ids and confirm data exists under the db prefix.
	let (ns_id, db_id) = {
		let tx = ds.transaction(Read).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		(db.namespace_id, db.database_id)
	};
	let db_prefix = DbRoot {
		ns: ns_id,
		db: db_id,
	};
	let range = db_prefix.range().unwrap();
	assert!(
		count_range(&ds, range.clone()).await > 0,
		"the database prefix should hold data before reclaim"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "no reclaim jobs before removal");

	// Remove the database. This must NOT delete the data prefix inline.
	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();

	// The database is immediately invisible in the catalog...
	{
		let tx = ds.transaction(Read).await.unwrap();
		let gone = tx.get_db_by_name("test", "tenant", None).await.unwrap();
		let _ = tx.cancel().await;
		assert!(gone.is_none(), "database must be invisible immediately after REMOVE");
	}
	// ...a reclaim job is queued...
	assert_eq!(reclaim_queue_len(&ds).await, 1, "REMOVE DATABASE must enqueue one reclaim job");
	// ...and the data is still physically present (reclaim is deferred).
	assert!(
		count_range(&ds, range.clone()).await > 0,
		"data must still be present before the reclaim task runs"
	);

	// Run the background reclaim task with zero grace so it reclaims immediately.
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	// The data prefix is now physically gone and the queue is drained.
	assert_eq!(
		count_range(&ds, range.clone()).await,
		0,
		"the reclaim task must destroy the database data prefix"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the reclaim task must drain the reclaim queue");

	// Recreating the same name yields a fresh, empty database with a new id.
	ds.execute("DEFINE DATABASE tenant;", &ses, None).await.unwrap();
	let new_db_id = {
		let tx = ds.transaction(Read).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		db.database_id
	};
	assert_ne!(new_db_id, db_id, "recreated database must get a fresh, never-reused id");

	// The recreated database starts physically empty — none of the removed
	// database's data is reachable under the new (disjoint) prefix.
	let new_prefix = DbRoot {
		ns: ns_id,
		db: new_db_id,
	};
	let new_range = new_prefix.range().unwrap();
	assert_eq!(
		count_range(&ds, new_range.clone()).await,
		0,
		"recreated database must start empty with no data leaked from the removed one"
	);
}

#[tokio::test]
async fn reclaim_is_idempotent_and_safe_when_empty() {
	let ds = mem_ds().await;
	// Running the reclaim task with an empty queue is a harmless no-op.
	let (iters, errors) = Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(errors, 0);
	assert_eq!(iters, 0, "an empty queue performs no reclaim iterations");
}

/// The reclaim task must NOT physically destroy a freshly-removed object's data
/// while it is still inside the snapshot-safety grace window — otherwise a read
/// transaction whose snapshot predates the `REMOVE` could have its data ripped
/// out from under it, which is exactly what an out-of-transaction range destroy
/// does, because it bypasses MVCC. Once the removal ages past the grace, reclaim
/// proceeds.
#[tokio::test]
async fn reclaim_respects_grace_period() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");

	ds.execute(
		"DEFINE NAMESPACE test; DEFINE DATABASE tenant; CREATE thing:1 SET v = 1; CREATE thing:2 SET v = 2;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = {
		let tx = ds.transaction(Read).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		(db.namespace_id, db.database_id)
	};
	let db_prefix = DbRoot {
		ns: ns_id,
		db: db_id,
	};
	let range = db_prefix.range().unwrap();

	// An in-flight reader: a transaction opened *before* the removal. It must
	// still be able to read the data afterwards, because the reclaim task must not have
	// destroyed it yet.
	let reader = ds.transaction(Read).await.unwrap();
	let reader_seen_before = reader.getr_raw(range.clone(), None).await.unwrap().len();
	assert!(reader_seen_before > 0, "reader should see the data before removal");

	// Remove the database. The freshly-enqueued entry is not yet observed.
	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	assert_eq!(
		reclaim_entry_observed_ms(&ds).await,
		0,
		"a freshly-enqueued reclaim entry must start unobserved"
	);

	// Run the reclaim task with a large grace window. The removal is brand new, so it
	// is inside the grace and must NOT be reclaimed.
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::from_secs(3600),
		CancellationToken::new(),
	)
	.await
	.unwrap();

	// The data is still physically present and the job is still queued...
	assert!(
		count_range(&ds, range.clone()).await > 0,
		"data must NOT be destroyed while inside the grace window"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 1, "the reclaim job must remain queued during grace");
	// ...the task instead recorded an observation time (aging is measured from
	// here, not from the pre-commit uid)...
	assert_ne!(
		reclaim_entry_observed_ms(&ds).await,
		0,
		"the first pass must stamp an observation time instead of reclaiming"
	);
	// ...so the in-flight reader still sees consistent data even though the
	// reclaim task ran while it was open.
	assert_eq!(
		reader.getr_raw(range.clone(), None).await.unwrap().len(),
		reader_seen_before,
		"an in-flight reader opened before REMOVE must still see its data"
	);
	let _ = reader.cancel().await;

	// Once the removal has aged past the grace (here: zero grace), reclaim runs.
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(
		count_range(&ds, range.clone()).await,
		0,
		"data must be reclaimed once past the grace window"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the reclaim job must be drained after reclaim");
}

/// Once an entry's *observation* time (not its enqueue uid) has aged past the
/// grace, a single reclaim pass destroys it under a realistic non-zero grace.
/// Backdating the observation deterministically simulates the passage of time.
#[tokio::test]
async fn reclaim_runs_once_observation_ages_past_grace() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");

	ds.execute(
		"DEFINE NAMESPACE test; DEFINE DATABASE tenant; CREATE thing:1 SET v = 1;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = {
		let tx = ds.transaction(Read).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		(db.namespace_id, db.database_id)
	};
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();

	// Backdate the queued entry's observation to one hour ago, simulating an
	// entry the reclaim task observed long before the grace elapsed.
	{
		let range = ReclaimPrefix {}.range().unwrap();
		let tx = ds.transaction(Write).await.unwrap();
		let items = tx.getr(range, None).await.unwrap();
		assert_eq!(items.len(), 1);
		let rc = ReclaimKey::decode_key(&items[0].0).unwrap();
		tx.set_key(
			&rc,
			&ReclaimState {
				observed_ms: now_ms().saturating_sub(3_600_000),
				cursor: None,
			},
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();
	}

	// A single pass under a 60s grace now reclaims it (already aged), without
	// needing a separate observe-then-wait cycle.
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::from_secs(60),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(
		count_range(&ds, range.clone()).await,
		0,
		"an entry observed longer than the grace ago must be reclaimed"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the reclaim job must be drained");
}

/// A reclaim must delete its prefix in bounded, committed pages, and must charge
/// every deleted key as an individually-accounted write.
///
/// The bound is what keeps a reclaim's memory independent of the prefix's
/// cardinality. Deleting the whole prefix in one transaction accumulates one
/// write-batch entry per key, so a prefix of millions of keys is an unbounded
/// allocation that nothing else catches: the reaper's transactions are internal
/// and carry no write-keys limit, and a range delete takes a single write slot
/// however many keys it expands to below the transaction layer.
///
/// Both halves are asserted deliberately. The per-transaction ceiling alone is
/// satisfied by a range delete, which reports zero keys written; the total lower
/// bound is what tells per-key accounting apart from that. Together they pin the
/// shape to bounded pages of per-key deletes, and the exact page sequence pins
/// the page size.
#[tokio::test]
async fn reclaim_deletes_in_bounded_committed_batches() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	// Two full pages and a partial one, so the pass must commit several times
	// and the last page is short.
	let baseline = count_range(&ds, range.clone()).await;
	seed_range(&ds, &range, page * 2 + page / 2 - baseline).await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	let total = count_range(&ds, range.clone()).await;
	assert_eq!(total, page * 2 + page / 2, "the prefix must be sized to 2.5 pages");

	// Observe only the reclaim cycle's own transactions.
	observer.clear();
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(count_range(&ds, range).await, 0, "the reclaim must empty the prefix");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "an emptied prefix must retire its queue entry");

	let sizes = observer.sizes();
	assert!(
		sizes.iter().all(|&n| n <= page as u64 + 1),
		"a reclaim write transaction carried more than one page: {sizes:?}"
	);
	assert!(
		sizes.iter().sum::<u64>() >= total as u64,
		"every reclaimed key must be charged as a write: {sizes:?}"
	);
	assert_eq!(
		cleanup_sizes(&sizes),
		expected_page_sizes(total),
		"unexpected reclaim page sequence"
	);
}

/// A queue entry is retired if and only if its prefix is empty.
///
/// A pass that stops mid-prefix has advanced a durable cursor but left keys
/// behind. Retiring the entry there would orphan the remainder permanently, with
/// the catalog entry already gone and no queue entry left to name it — data that
/// nothing can ever find, let alone delete.
#[tokio::test]
async fn reclaim_keeps_tombstone_queued_until_prefix_is_empty() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	let baseline = count_range(&ds, range.clone()).await;
	seed_range(&ds, &range, page * 2 + page / 2 - baseline).await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	let total = count_range(&ds, range.clone()).await;

	// A budget of exactly one page stops the pass after its first commit.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	let remaining = count_range(&ds, range.clone()).await;
	assert_eq!(remaining, total - page, "the pass must delete exactly its budget");
	assert_eq!(
		reclaim_queue_len(&ds).await,
		1,
		"a prefix that is not yet empty must keep its queue entry"
	);
	assert!(
		reclaim_entry(&ds).await.1.cursor.is_some(),
		"a partial pass must leave a durable resume cursor"
	);

	// An unbudgeted pass finishes the prefix and only then retires the entry.
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(count_range(&ds, range).await, 0);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "an emptied prefix must retire its queue entry");
}

/// A reclaim resumes strictly after the entry's committed cursor.
///
/// The cursor is the claim that every key at or before it is already gone, so a
/// pass must not scan back over it: intra-run that is what keeps each page off
/// the delete tombstones the retired pages left behind, and across runs it is
/// what makes progress monotonic instead of restarting the prefix.
///
/// Asserted by planting a cursor over keys that were never deleted. A reclaim
/// that honours it leaves exactly those keys behind; one that rescans from the
/// start would delete them too.
#[tokio::test]
async fn reclaim_resumes_after_the_committed_cursor() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	seed_range(&ds, &range, 16).await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	let keys = keys_in_range(&ds, range.clone()).await;
	assert!(keys.len() > 8, "the prefix must hold enough keys to split");

	// Claim the first five keys as already reclaimed.
	let already_done = 5;
	{
		let (k, state) = reclaim_entry(&ds).await;
		let rc = ReclaimKey::decode_key(&k).unwrap();
		let tx = ds.transaction(Write).await.unwrap();
		tx.set_key(
			&rc,
			&ReclaimState {
				observed_ms: state.observed_ms,
				cursor: Some(keys[already_done - 1].clone()),
			},
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();
	}

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		keys_in_range(&ds, range).await,
		keys[..already_done],
		"the reclaim must delete only the keys ordering after the cursor"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the entry is retired once the scan finds nothing");
}

/// A reclaim that commits page by page must not be visible to a reader.
///
/// A paged reclaim commits once per page, so a transaction opened before the
/// reclaim could in principle observe the prefix shrinking under it. It must
/// not: the reader's snapshot predates every page, and the whole grace-period
/// design rests on that.
#[tokio::test]
async fn reclaim_pages_are_invisible_to_an_in_flight_reader() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	let baseline = count_range(&ds, range.clone()).await;
	seed_range(&ds, &range, page * 2 - baseline).await;

	// A reader opened before the removal, held open across the whole reclaim.
	let reader = ds.transaction(Read).await.unwrap();
	let seen_before = reader.count(range.clone(), None).await.unwrap();
	assert_eq!(seen_before, page * 2);

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		reader.count(range.clone(), None).await.unwrap(),
		seen_before,
		"a reader opened before the reclaim must not see its pages commit"
	);
	let _ = reader.cancel().await;
	assert_eq!(count_range(&ds, range).await, 0, "a fresh reader sees the prefix emptied");
}

/// What a queue entry names, and how it asks for the keys to be removed.
enum Removal {
	/// `REMOVE NAMESPACE`, optionally expunging. The superset case: the
	/// namespace prefix covers every database beneath it.
	Namespace {
		expunge: bool,
	},
	/// `REMOVE DATABASE`, optionally expunging.
	Database {
		expunge: bool,
	},
	/// `REMOVE INDEX`, which always enqueues [`Expunge::Keep`].
	Index,
	/// An expunging index entry written directly. `REMOVE INDEX` has no
	/// spelling that produces one, so this is the only way the dispatcher's
	/// expunging index arm is reached at all.
	IndexExpunged,
}

/// Every reclaim kind, under both expunge modes, goes through the same bounded
/// paged delete.
///
/// The kinds resolve to different prefixes and the expunge modes to different
/// per-key primitives, so each row drives its own arm of the dispatcher and a
/// bound proven for one says nothing about the others. What every row asserts is
/// the bound and the emptied prefix: the two per-key primitives differ only in
/// whether older versions of a key survive, which the memory backend does not
/// expose.
#[tokio::test]
async fn reclaim_bounds_every_kind_and_expunge_mode() {
	for removal in [
		Removal::Namespace {
			expunge: false,
		},
		Removal::Namespace {
			expunge: true,
		},
		Removal::Database {
			expunge: false,
		},
		Removal::Database {
			expunge: true,
		},
		Removal::Index,
		Removal::IndexExpunged,
	] {
		assert_removal_reclaims_in_bounded_pages(removal).await;
	}
}

async fn assert_removal_reclaims_in_bounded_pages(removal: Removal) {
	let page = RECLAIM_BATCH_SIZE as usize;
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE DATABASE other;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let (_, other_id) = ids(&ds, "test", "other").await;
	let ix_id = index_id(&ds, ns_id, db_id, "thing", "idx").await;

	let target = match removal {
		Removal::Namespace {
			..
		} => NsRoot {
			ns: ns_id,
		}
		.range()
		.unwrap(),
		Removal::Database {
			..
		} => DbRoot {
			ns: ns_id,
			db: db_id,
		}
		.range()
		.unwrap(),
		Removal::Index | Removal::IndexExpunged => IdxRoot {
			ns: ns_id,
			db: db_id,
			tb: Cow::Owned(TableName::from("thing")),
			ix: ix_id,
		}
		.range()
		.unwrap(),
	};

	// Size the target to more than two pages so the reclaim has to commit
	// several times.
	let sized = page * 2 + page / 2;
	let baseline = count_range(&ds, target.clone()).await;
	let to_seed = sized - baseline;
	match removal {
		Removal::Namespace {
			..
		} => {
			// Split across two databases, so the namespace prefix strictly
			// contains more than either database prefix.
			let tenant = DbRoot {
				ns: ns_id,
				db: db_id,
			}
			.range()
			.unwrap();
			let other = DbRoot {
				ns: ns_id,
				db: other_id,
			}
			.range()
			.unwrap();
			seed_range(&ds, &tenant, to_seed / 2).await;
			seed_range(&ds, &other, to_seed - to_seed / 2).await;
		}
		Removal::Database {
			..
		}
		| Removal::Index
		| Removal::IndexExpunged => seed_range(&ds, &target, to_seed).await,
	}

	let statement = match removal {
		Removal::Namespace {
			expunge: false,
		} => Some("REMOVE NAMESPACE test;"),
		Removal::Namespace {
			expunge: true,
		} => Some("REMOVE NAMESPACE AND EXPUNGE test;"),
		Removal::Database {
			expunge: false,
		} => Some("REMOVE DATABASE tenant;"),
		Removal::Database {
			expunge: true,
		} => Some("REMOVE DATABASE AND EXPUNGE tenant;"),
		Removal::Index => Some("REMOVE INDEX idx ON thing;"),
		Removal::IndexExpunged => None,
	};
	match statement {
		Some(statement) => {
			ds.execute(statement, &ses, None).await.unwrap();
		}
		None => {
			// The reaper reads committed queue entries, so writing one directly
			// is the same input a `REMOVE` would have left.
			let rc = ReclaimKey {
				kind: ReclaimKind::Index,
				ns: ns_id,
				db: db_id,
				tb: Cow::Owned(TableName::from("thing")),
				ix: ix_id,
				expunge: Expunge::Expunge,
				uid: Uuid::now_v7(),
			};
			let tx = ds.transaction(Write).await.unwrap();
			tx.set_key(&rc, &ReclaimState::enqueued()).await.unwrap();
			tx.commit().await.unwrap();
		}
	}

	let kinds = reclaim_queue_kinds(&ds).await;
	let own_kind = match removal {
		Removal::Namespace {
			..
		} => ReclaimKind::Namespace,
		Removal::Database {
			..
		} => ReclaimKind::Database,
		Removal::Index | Removal::IndexExpunged => ReclaimKind::Index,
	};
	// One entry per removal, queued in the transaction that removes the
	// catalog definition. Retirement fences the build durably by deleting
	// `!bs` in that same transaction, so no later builder write can land under
	// the prefix and nothing needs a second, follow-up pass.
	assert_eq!(
		count_kind(&kinds, own_kind),
		1,
		"unexpected number of {own_kind:?} entries queued for the removal"
	);
	// `REMOVE INDEX` drops the table's last doc-ID consumer, so it enqueues the
	// shared doc-ID prefixes alongside the index's own. A parent removal
	// takes the whole prefix and has nothing to defer separately.
	let expected_doc_entries = match removal {
		Removal::Index => ALL_DOC_ID_KINDS.len(),
		Removal::Namespace {
			..
		}
		| Removal::Database {
			..
		}
		| Removal::IndexExpunged => 0,
	};
	assert_eq!(
		count_doc_kinds(&kinds),
		expected_doc_entries,
		"unexpected number of shared doc-ID entries queued for the removal"
	);
	let total = count_range(&ds, target.clone()).await;
	assert!(total > 2 * page, "the target must hold more than two pages, holds {total}");

	observer.clear();
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(count_range(&ds, target).await, 0, "the reclaim must empty the target prefix");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "an emptied prefix must retire its queue entry");

	let sizes = observer.sizes();
	assert!(
		sizes.iter().all(|&n| n <= page as u64 + 1),
		"a reclaim write transaction carried more than one page: {sizes:?}"
	);
	assert!(
		sizes.iter().sum::<u64>() >= total as u64,
		"every reclaimed key must be charged as a write: {sizes:?}"
	);
	// Retiring the entries is one transaction per batch of the queue, so an
	// index drop's three empty doc-ID entries are retired together with the
	// index's own once its last page lands.
	let mut expected = expected_page_sizes(total);
	if kinds.len() > 1 {
		expected.push(kinds.len() as u64);
	}
	assert_eq!(cleanup_sizes(&sizes), expected, "unexpected reclaim page sequence");
}

/// A pass's key budget bounds the pass, not each entry it visits.
///
/// The queue is walked in key order, so an entry that spent the whole budget on
/// its own prefix would leave every entry behind it untouched until it finished
/// — however many passes that takes. Each entry instead takes its share and
/// hands the rest on, so the entries behind it advance in the same pass.
#[tokio::test]
async fn reclaim_shares_its_budget_across_queued_entries() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("one");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE one; DEFINE DATABASE two;", &ses, None)
		.await
		.unwrap();

	let (ns_id, one_id) = ids(&ds, "test", "one").await;
	let (_, two_id) = ids(&ds, "test", "two").await;
	let one = DbRoot {
		ns: ns_id,
		db: one_id,
	}
	.range()
	.unwrap();
	let two = DbRoot {
		ns: ns_id,
		db: two_id,
	}
	.range()
	.unwrap();
	// Each prefix holds more than the whole budget below, so neither can finish
	// inside the pass and what each one deleted is what it was granted.
	for range in [&one, &two] {
		let baseline = count_range(&ds, range.clone()).await;
		seed_range(&ds, range, page * 2 - baseline).await;
	}

	ds.execute("REMOVE DATABASE one; REMOVE DATABASE two;", &ses, None).await.unwrap();
	assert_eq!(reclaim_queue_len(&ds).await, 2, "both removals must enqueue an entry");

	// Which entry the pass reaches first is the queue's key order, which for two
	// databases of one namespace is the order of their two data prefixes.
	let (first, second) = match one.start().as_ref() < two.start().as_ref() {
		true => (one, two),
		false => (two, one),
	};
	let before_first = count_range(&ds, first.clone()).await;
	let before_second = count_range(&ds, second.clone()).await;

	// One and a half pages, so neither share is a whole number of pages: what an
	// entry may delete is its slice of the budget, not a page count.
	let budget = page + page / 2;
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		budget as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	let deleted_first = before_first - count_range(&ds, first.clone()).await;
	let deleted_second = before_second - count_range(&ds, second.clone()).await;
	assert_eq!(
		deleted_first + deleted_second,
		budget,
		"the pass must spend its whole budget and no more, across both entries"
	);
	// Two entries, each granted its slice of what the budget has left when the
	// pass reaches it: half to the first, and to the second all that remains.
	assert_eq!(
		deleted_first,
		budget / 2,
		"the leading entry takes its share, not the whole budget"
	);
	assert_eq!(deleted_second, budget - budget / 2, "the entry behind it must get what is left");
	assert_eq!(reclaim_queue_len(&ds).await, 2, "neither prefix is empty yet, so both stay queued");

	// Both prefixes finish, and only then does either entry retire.
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(count_range(&ds, first).await, 0, "the leading prefix must end up empty");
	assert_eq!(count_range(&ds, second).await, 0, "the trailing prefix must end up empty");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "both entries retire once their prefixes are gone");
}

/// A pass's queue update is bounded by the batch it read, not by the queue.
///
/// The queue holds one entry per pending removal, so reading it whole would put
/// every entry in one snapshot and every retirement in one write transaction —
/// the same unbounded accumulation the paged data delete exists to avoid, one
/// level up. Sized past one batch so the bound is a real ceiling rather than a
/// count the queue happens to fall under.
#[tokio::test]
async fn reclaim_queue_updates_are_bounded_by_the_batch() {
	let batch = RECLAIM_BATCH_SIZE as usize;
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;

	// Entries naming prefixes that hold nothing: the pass retires every one of
	// them, and spends no budget doing it, so what it commits is queue writes
	// alone. The setup commits the stored-format ledger entry, which a store
	// records once, with its first stored-grammar write: otherwise the pass's
	// lease write would carry it.
	let queued = batch + batch / 2;
	{
		let tx = ds.transaction(Write).await.unwrap();
		tx.admit_stored_format().await.unwrap();
		for i in 0..queued {
			let rc = ReclaimKey::database(
				NamespaceId(1),
				DatabaseId(i as u32 + 1),
				false,
				Uuid::now_v7(),
			);
			tx.set_key(&rc, &ReclaimState::enqueued()).await.unwrap();
		}
		tx.commit().await.unwrap();
	}
	assert_eq!(reclaim_queue_len(&ds).await, queued);

	observer.clear();
	// A pass divides its budget between a bounded set of the longest-waiting
	// entries, so a queue longer than that set is retired over consecutive
	// passes. Retiring costs no budget here — every prefix is already empty — so
	// what bounds each pass is the candidate set, and what bounds each write
	// transaction is the batch.
	const MAX_PASSES: usize = 8;
	let mut passes = 0;
	while reclaim_queue_len(&ds).await > 0 {
		passes += 1;
		assert!(passes <= MAX_PASSES, "the queue must drain within {MAX_PASSES} passes");
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			Duration::ZERO,
			CancellationToken::new(),
		)
		.await
		.unwrap();
	}

	let sizes = observer.sizes();
	assert!(
		sizes.iter().all(|&n| n <= batch as u64),
		"a reclaim write transaction carried more than one batch: {sizes:?}"
	);
	// One transaction per pass, each retiring what that pass served, and between
	// them every entry queued.
	let updates = cleanup_sizes(&sizes);
	assert_eq!(
		updates,
		vec![batch as u64, (queued - batch) as u64],
		"unexpected queue update sequence"
	);
	assert_eq!(
		updates.iter().sum::<u64>(),
		queued as u64,
		"every queued entry must be accounted for by a queue update"
	);
}

/// The datastore-level range destroy reports "unsupported"; it never reports a
/// success that deleted nothing.
///
/// A backend publishes its [`surrealdb_kvs::DestroyRange`] capability only while
/// it can perform the operation, so on a backend publishing none this entry point
/// has nothing to delegate to. Answering `Ok` there would tell a caller holding
/// no fallback that a prefix had been destroyed while every key of it is still
/// present.
#[tokio::test]
async fn unsafe_destroy_range_reports_unsupported_rather_than_succeeding() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	seed_range(&ds, &range, 8).await;
	let seeded = count_range(&ds, range.clone()).await;
	assert!(seeded > 0, "the range must hold keys for the assertion below to mean anything");

	let err = ds.unsafe_destroy_range(range.clone().into_key_range()).await.unwrap_err();
	assert!(
		matches!(
			err.downcast_ref::<crate::kvs::Error>(),
			Some(crate::kvs::Error::RangeDestroyNotSupported)
		),
		"a backend offering no range destroy must report it: {err}"
	);
	assert_eq!(
		count_range(&ds, range).await,
		seeded,
		"a destroy reported as unsupported must leave every key in place"
	);
}

/// One range a [`RecordingDestroyRange`] was handed, as its `(start, end)` bytes.
type DestroyedRange = (Vec<u8>, Vec<u8>);

/// The ranges a [`RecordingDestroyRange`] was asked to destroy, shared with the
/// test that installed it.
#[derive(Clone, Default)]
struct DestroyedRanges(Arc<Mutex<Vec<DestroyedRange>>>);

impl DestroyedRanges {
	/// The recorded ranges, in call order.
	fn ranges(&self) -> Vec<DestroyedRange> {
		self.0.lock().unwrap().clone()
	}
}

/// An out-of-transaction range destroy that records the range it was handed and
/// then empties it.
///
/// Stands in for the primitive a distributed backend offers, so the reclaim's
/// range-destroy path can be driven on a backend that has no such primitive of
/// its own. Emptying the range through a transaction of the wrapped backend is
/// not what a real implementation does — the point of the capability is to skip
/// the transaction layer — but it is indistinguishable to the caller, which is
/// all this stands in for.
struct RecordingDestroyRange {
	inner: Arc<dyn TransactionBuilder>,
	destroyed: DestroyedRanges,
}

impl DestroyRange for RecordingDestroyRange {
	fn mechanism(&self) -> &'static str {
		"recording"
	}

	fn destroy_range(&self, range: KeyRange<'static>) -> BoxFut<'_, KvsResult<()>> {
		Box::pin(async move {
			self.destroyed
				.0
				.lock()
				.unwrap()
				.push((range.start.as_ref().to_vec(), range.end.as_ref().to_vec()));
			let (txn, _) = self.inner.new_transaction(TransactionType::Write).await?;
			txn.delr(range).await?;
			txn.commit().await
		})
	}
}

/// A backend that behaves exactly like the one it wraps, plus a published
/// [`DestroyRangeHandle`].
struct DestroyingBuilder {
	inner: Arc<dyn TransactionBuilder>,
	handle: Arc<DestroyRangeHandle>,
}

impl TransactionBuilder for DestroyingBuilder {
	fn name(&self) -> &'static str {
		self.inner.name()
	}

	fn new_transaction(
		&self,
		write: TransactionType,
	) -> BoxFut<'_, KvsResult<(Box<dyn Transactable>, bool)>> {
		self.inner.new_transaction(write)
	}

	fn shutdown(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.shutdown()
	}

	fn register_metrics(&self) -> Option<Metrics> {
		self.inner.register_metrics()
	}

	fn collect_u64_metric(&self, metric: &str) -> Option<u64> {
		self.inner.collect_u64_metric(metric)
	}

	fn wait_until_serve_ready(&self) -> BoxFut<'_, KvsResult<()>> {
		self.inner.wait_until_serve_ready()
	}

	fn extension(&self, id: TypeId) -> Option<Arc<dyn Any + Send + Sync>> {
		match id == TypeId::of::<DestroyRangeHandle>() {
			true => Some(Arc::clone(&self.handle) as Arc<dyn Any + Send + Sync>),
			false => self.inner.extension(id),
		}
	}
}

/// Builds the datastore of the wrapped path with a range-destroy capability
/// published on top of it.
struct DestroyRangeComposer(DestroyedRanges);

impl TransactionBuilderFactory for DestroyRangeComposer {
	type RouterState = ();

	async fn new_transaction_builder(
		&self,
		path: &str,
		canceller: CancellationToken,
		config: ConfigMap,
	) -> anyhow::Result<TransactionBuilderParts<Self::RouterState>> {
		let parts = CommunityComposer().new_transaction_builder(path, canceller, config).await?;
		let inner: Arc<dyn TransactionBuilder> = Arc::from(parts.builder);
		let handle = Arc::new(DestroyRangeHandle(Arc::new(RecordingDestroyRange {
			inner: Arc::clone(&inner),
			destroyed: self.0.clone(),
		})));
		Ok(TransactionBuilderParts::without_router_state(Box::new(DestroyingBuilder {
			inner,
			handle,
		})))
	}

	fn path_valid(&self, v: &str) -> anyhow::Result<String> {
		CommunityComposer().path_valid(v)
	}
}

/// A backend offering an out-of-transaction range destroy has the whole prefix
/// destroyed in one call: no pages, no cursor, no second pass.
///
/// The capability is resolved by type rather than by backend name, which is what
/// lets this drive the branch on a backend that has no such primitive of its own.
/// The budget is set to a single key — orders of magnitude below the prefix — so
/// a paged delete could not empty it, and the recorded range is asserted to be
/// exactly the entry's prefix, since the destroy bypasses MVCC and an
/// overreaching range would take data nothing asked to remove.
#[tokio::test]
async fn reclaim_destroys_the_prefix_in_one_call_where_the_backend_offers_it() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let destroyed = DestroyedRanges::default();
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.without_maintenance_tasks()
		.build_with_factory_path("memory", DestroyRangeComposer(destroyed.clone()))
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	let baseline = count_range(&ds, range.clone()).await;
	seed_range(&ds, &range, page * 2 - baseline).await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		1,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		count_range(&ds, range.clone()).await,
		0,
		"the whole prefix must go, however small the pass budget"
	);
	assert_eq!(
		reclaim_queue_len(&ds).await,
		0,
		"a prefix destroyed in one call retires its entry in the same pass"
	);
	assert_eq!(
		destroyed.ranges(),
		vec![(range.start().as_ref().to_vec(), range.end().as_ref().to_vec())],
		"the reclaim must hand over exactly the entry's prefix, once"
	);
}

/// A table's shared doc-ID space is never destroyed out-of-transaction, even
/// where the backend offers it.
///
/// The claim on an untouched space is revocable by `DEFINE INDEX`; after paging
/// starts, the DDL must wait for cleanup. An out-of-transaction destroy cannot
/// participate in either decision, so doc-ID kinds always take the paged path,
/// whose cursor update conditionally verifies the claim in every transaction.
#[tokio::test]
async fn a_doc_id_reclaim_never_takes_the_out_of_transaction_destroy() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let destroyed = DestroyedRanges::default();
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.without_maintenance_tasks()
		.build_with_factory_path("memory", DestroyRangeComposer(destroyed.clone()))
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(&ds, &dd, page).await;
	seed_range(&ds, &di, page).await;

	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();
	// One key for the whole pass, so a paged delete can barely start while an
	// out-of-transaction destroy would take each prefix whole regardless.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		1,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	for (name, range) in [("!dd", &dd), ("!di", &di)] {
		let bounds = (range.start().as_ref().to_vec(), range.end().as_ref().to_vec());
		assert!(
			!destroyed.ranges().contains(&bounds),
			"{name} must not be handed to the out-of-transaction destroy"
		);
		assert!(
			count_range(&ds, range.clone()).await > 0,
			"{name} must be deleted in bounded pages, not taken whole"
		);
	}

	// A started claim cannot be withdrawn or adopted. The DDL that meets it
	// finishes the remainder in the same bounded pages, never through the
	// out-of-transaction destroy, and then defines its index.
	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
	assert_eq!(count_range(&ds, dd.clone()).await + count_range(&ds, di.clone()).await, 0);
	for (name, range) in [("!dd", &dd), ("!di", &di)] {
		let bounds = (range.start().as_ref().to_vec(), range.end().as_ref().to_vec());
		assert!(
			!destroyed.ranges().contains(&bounds),
			"{name} must not be handed to the out-of-transaction destroy by the DDL either"
		);
	}
	assert_eq!(count_doc_kinds(&reclaim_queue_kinds(&ds).await), 0);
}

/// A prefix emptied by the very page that spends the budget is retired there.
///
/// The page that empties a prefix and the page that exhausts the budget can be
/// the same page. Deciding completeness from the budget before looking at the
/// range reports such a prefix unfinished, which leaves a queue entry naming
/// nothing until a later pass reads it, scans one empty page and retires it.
#[tokio::test]
async fn reclaim_retires_a_prefix_the_budget_ran_out_on_emptying() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	// Exactly one page, so the page that empties the prefix is also the page that
	// takes the last of the budget.
	let baseline = count_range(&ds, range.clone()).await;
	seed_range(&ds, &range, page - baseline).await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	let total = count_range(&ds, range.clone()).await;
	assert_eq!(total, page, "the prefix must hold exactly one page");

	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		total as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(count_range(&ds, range).await, 0, "the pass must empty the prefix");
	assert_eq!(
		reclaim_queue_len(&ds).await,
		0,
		"a prefix emptied as the budget ran out must still retire its queue entry"
	);
}

/// An entry whose stored state cannot be read is restarted, not skipped.
///
/// The queue entry is the only remaining name for the data, because the catalog
/// definition is already gone. Skipping the entry therefore strands the prefix
/// permanently: nothing else can find it, and no grace setting brings it back.
#[tokio::test]
async fn reclaim_restarts_an_entry_whose_state_cannot_be_read() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	let baseline = count_range(&ds, range.clone()).await;
	seed_range(&ds, &range, 32 - baseline).await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	assert!(count_range(&ds, range.clone()).await > 0, "the prefix must hold data to reclaim");

	// Replace the queue value with bytes no revision of the state decodes to,
	// which is the shape a truncated write or a value from an unknown revision
	// presents.
	{
		let (k, _) = reclaim_entry(&ds).await;
		let tx = ds.transaction(Write).await.unwrap();
		tx.set(Key::from(&k), vec![0xde, 0xad, 0xbe, 0xef]).await.unwrap();
		tx.commit().await.unwrap();
	}

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		count_range(&ds, range).await,
		0,
		"an entry whose state cannot be read must still have its prefix reclaimed"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "and must still be retired");
}

/// The budget is shared between the entries that can spend it.
///
/// Entries still inside their grace window delete nothing, so counting them when
/// dividing the budget hands the one entry that can spend a single page and
/// leaves the rest of the budget unspent — which is the steady state, since every
/// fresh removal sits queued for the whole grace window.
#[tokio::test]
async fn reclaim_shares_its_budget_only_with_entries_past_the_grace() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("big");
	ds.execute(
		"DEFINE NAMESPACE test; DEFINE DATABASE big;
		 DEFINE DATABASE f1; DEFINE DATABASE f2; DEFINE DATABASE f3; DEFINE DATABASE f4;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, big_id) = ids(&ds, "test", "big").await;
	let big = DbRoot {
		ns: ns_id,
		db: big_id,
	}
	.range()
	.unwrap();
	// Three pages, so a share of one page cannot finish it and the shortfall is
	// visible in what survives.
	let baseline = count_range(&ds, big.clone()).await;
	seed_range(&ds, &big, page * 3 - baseline).await;

	ds.execute(
		"REMOVE DATABASE big; REMOVE DATABASE f1; REMOVE DATABASE f2;
		 REMOVE DATABASE f3; REMOVE DATABASE f4;",
		&ses,
		None,
	)
	.await
	.unwrap();
	assert_eq!(reclaim_queue_len(&ds).await, 5, "every removal must enqueue an entry");

	// Age only the large entry past the grace. The other four stay unobserved, so
	// they are stamped rather than reclaimed and spend nothing.
	{
		let tx = ds.transaction(Write).await.unwrap();
		let items = tx.getr(ReclaimPrefix {}.range().unwrap(), None).await.unwrap();
		for (k, state) in &items {
			let rc = ReclaimKey::decode_key(k).unwrap();
			if rc.db == big_id {
				tx.set_key(
					&rc,
					&ReclaimState {
						observed_ms: now_ms().saturating_sub(3_600_000),
						cursor: state.cursor.clone(),
					},
				)
				.await
				.unwrap();
			}
		}
		tx.commit().await.unwrap();
	}

	// Enough pages for all of them, if the whole budget reaches the one entry that
	// can spend it.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::from_secs(60),
		(page * 3) as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		count_range(&ds, big).await,
		0,
		"the entry past its grace must receive the budget the ageing entries cannot spend"
	);
}

/// Dropping the last doc-ID-consuming index defers the table's shared doc-ID
/// space, so the statement's own transaction stays bounded however many records
/// the table holds.
///
/// The space is table-scoped and shared by every doc-ID-consuming index, so it
/// is not covered by the removed index's own data prefix: it needs queue entries
/// of its own. Deleting it inline would put one write per record into the user's
/// transaction.
#[tokio::test]
async fn remove_index_defers_the_shared_doc_id_reclaim() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dp = DocPendingPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();

	// Size each direction to more than two pages, so a bounded reclaim has to
	// commit several times.
	let sized = page * 2 + page / 2;
	seed_range(&ds, &di, sized).await;
	seed_range(&ds, &dd, sized).await;
	let seeded = count_range(&ds, di.clone()).await + count_range(&ds, dd.clone()).await;
	assert!(seeded > 4 * page, "the shared space must hold more than four pages, holds {seeded}");

	observer.clear();
	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();

	// The statement leaves the shared space alone: every write transaction it
	// ran is bounded, and the keys are still there to be reclaimed.
	let sizes = observer.sizes();
	assert!(
		sizes.iter().all(|&n| n <= page as u64 + 1),
		"a REMOVE INDEX write transaction carried more than one page: {sizes:?}"
	);
	assert_eq!(
		count_range(&ds, di.clone()).await + count_range(&ds, dd.clone()).await,
		seeded,
		"REMOVE INDEX must defer the shared doc-ID space, not delete it inline"
	);
	// The index's own data prefix, queued with the catalog removal, plus one
	// entry per shared doc-ID prefix.
	let kinds = reclaim_queue_kinds(&ds).await;
	assert_eq!(
		count_kind(&kinds, ReclaimKind::Index),
		1,
		"the removal must enqueue the index prefix"
	);
	for kind in ALL_DOC_ID_KINDS {
		assert_eq!(
			count_kind(&kinds, kind),
			1,
			"the removal must enqueue the {kind:?} shared doc-ID prefix"
		);
	}

	observer.clear();
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	for (name, range) in [("!di", di), ("!dd", dd), ("!dp", dp)] {
		assert_eq!(count_range(&ds, range).await, 0, "the reclaim must empty {name}");
	}
	assert_eq!(reclaim_queue_len(&ds).await, 0, "an emptied prefix must retire its queue entry");
	let sizes = observer.sizes();
	assert!(
		sizes.iter().all(|&n| n <= page as u64 + 1),
		"a reclaim write transaction carried more than one page: {sizes:?}"
	);
	assert!(
		sizes.iter().sum::<u64>() >= seeded as u64,
		"every reclaimed key must be charged as a write: {sizes:?}"
	);
}

/// Defining a doc-ID consumer cancels a queued reclaim of the table's shared
/// doc-ID space, so the reclaim cannot delete mappings the new index has
/// adopted.
///
/// The new index reads existing ids through `TableDocIds::resolve_or_assign`'s
/// fast path, so an untouched space is adopted rather than purged: the mappings
/// in it are internally consistent, and the ids records already hold are the
/// ids they keep.
#[tokio::test]
async fn defining_a_doc_id_index_cancels_a_queued_doc_id_reclaim() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;
		 CREATE thing:1 SET val = 'a';
		 CREATE thing:2 SET val = 'b';",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let dj = DocLookupIdentityPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let mapped = keys_in_range(&ds, dj.clone()).await;
	assert!(!mapped.is_empty(), "the index must have mapped the records it covers");

	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();
	assert_eq!(
		keys_in_range(&ds, dj.clone()).await,
		mapped,
		"the drop must leave the shared space for the background reclaim"
	);

	// Redefine a consumer before the reclaim runs.
	ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	let kinds = reclaim_queue_kinds(&ds).await;
	assert_eq!(
		count_doc_kinds(&kinds),
		0,
		"defining a consumer must cancel every queued shared doc-ID reclaim"
	);
	assert_eq!(
		count_kind(&kinds, ReclaimKind::Index),
		kinds.len(),
		"only the dropped index's own prefix may stay queued"
	);

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		keys_in_range(&ds, dj).await,
		mapped,
		"the reclaim must not delete mappings the redefined index has adopted"
	);
	assert!(
		!keys_in_range(&ds, dd).await.is_empty(),
		"the reverse direction must survive with the forward one"
	);
	// The adopted ids are still resolvable, so the index answers from them.
	let mut res = ds.execute("SELECT val FROM thing WHERE val = 'a';", &ses, None).await.unwrap();
	assert_eq!(
		format!("{:?}", res.remove(0).result.unwrap()),
		r#"Array(Array([Object(Object({"val": String("a")}))]))"#,
		"the redefined index must resolve the records it adopted"
	);
}

/// A reclaim that has already begun leaves the shared doc-ID space torn, so
/// defining a consumer finishes the bounded reclaim, in its pages, before it
/// adopts the space, rather than adopting it torn or purging the remainder in
/// the DDL transaction.
///
/// The prefixes are reclaimed independently and page by page, so a
/// surviving `!dj` entry may have lost the `!dd` twin that resolves its id.
/// Adopting one would give the new index entries pointing at a doc-ID that
/// resolves to no record.
#[tokio::test]
async fn defining_a_doc_id_index_finishes_a_partly_reclaimed_doc_id_space() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(&ds, &di, page * 2).await;
	seed_range(&ds, &dd, page * 2).await;

	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();

	// One page's worth of budget, so the pass commits a cursor and stops with
	// the space torn.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	let left = count_range(&ds, di.clone()).await + count_range(&ds, dd.clone()).await;
	assert!(
		left > 0 && left < page * 4,
		"the pass must have torn the space, not finished or skipped it: {left} left"
	);

	// The DDL finishes the torn space's reclaim and runs again, so the caller
	// sees it succeed rather than being asked to retry.
	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
	assert_eq!(
		count_range(&ds, di).await + count_range(&ds, dd).await,
		0,
		"the DDL must finish the torn space before adopting it"
	);
	assert_eq!(count_doc_kinds(&reclaim_queue_kinds(&ds).await), 0);
	let _ = index_id(&ds, ns_id, db_id, "thing", "idx").await;
}

/// A sibling prefix the reaper drained and retired leaves the space torn, so
/// defining a consumer finishes the surviving bounded reclaim entries first.
///
/// Retiring an entry destroys the cursor that recorded what it had deleted, so a
/// torn space can be left with no survivor carrying a cursor. What still says the
/// space is torn is the count: the entries are enqueued together, so a
/// strict subset of them surviving means the rest were drained and retired.
#[tokio::test]
async fn defining_a_doc_id_index_finishes_when_a_sibling_entry_has_retired() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(&ds, &dd, page).await;
	seed_range(&ds, &di, page).await;

	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();
	let kinds = reclaim_queue_kinds(&ds).await;
	assert_eq!(
		count_kind(&kinds, ReclaimKind::Index),
		1,
		"the removal must enqueue the index's own prefix"
	);
	assert_eq!(
		count_doc_kinds(&kinds),
		ALL_DOC_ID_KINDS.len(),
		"the removal must enqueue every shared doc-ID prefix"
	);

	// Exactly the state retiring `!dd`'s entry leaves: its prefix drained, its
	// entry gone along with the cursor that recorded the draining, and its two
	// siblings still queued having never been reached. Staged directly rather than
	// reached through a pass budget, because which entries a pass reaches follows
	// from the share arithmetic and not from the rule under test.
	let drained = keys_in_range(&ds, dd.clone()).await;
	let retired = keys_in_range(
		&ds,
		ReclaimTbPrefix {
			kind: ReclaimKind::DocKey,
			ns: ns_id,
			db: db_id,
			tb: Cow::Borrowed(&tb),
		}
		.range()
		.unwrap(),
	)
	.await;
	assert_eq!(retired.len(), 1, "`!dd` must have exactly one queue entry to retire");
	let txn = ds.transaction(Write).await.unwrap();
	for key in drained.into_iter().chain(retired) {
		txn.del(Key::from(key)).await.unwrap();
	}
	txn.commit().await.unwrap();

	assert_eq!(count_range(&ds, dd).await, 0, "`!dd` must be drained");
	assert_eq!(count_range(&ds, di.clone()).await, page, "`!di` must be untouched");
	let kinds = reclaim_queue_kinds(&ds).await;
	assert_eq!(count_kind(&kinds, ReclaimKind::DocKey), 0, "`!dd`'s entry must be retired");
	assert_eq!(
		count_doc_kinds(&kinds),
		ALL_DOC_ID_KINDS.len() - 1,
		"`!dd`'s siblings must be left queued, never having been reached"
	);

	// The missing sibling says the space is torn, so the DDL finishes the
	// surviving entries before it adopts the space.
	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
	assert_eq!(count_range(&ds, di).await, 0, "the surviving mappings must be reclaimed");
	let kinds = reclaim_queue_kinds(&ds).await;
	assert_eq!(count_doc_kinds(&kinds), 0, "the bounded reclaim must finish");
	assert_eq!(
		count_kind(&kinds, ReclaimKind::Index),
		kinds.len(),
		"only unrelated index-prefix work may remain"
	);
	let _ = index_id(&ds, ns_id, db_id, "thing", "idx").await;
}

/// A doc-ID reclaim whose entry already carries a cursor deletes nothing further
/// once the table has a consumer again.
///
/// The cursor says part of the space is already gone, so what the consumer
/// adopted is torn. Deleting the rest would strip mappings a live index is
/// resolving through, so the obligation is retired with the space left as it
/// stands and the tear reported for an operator to rebuild.
#[tokio::test]
async fn a_partly_reclaimed_doc_id_space_is_left_whole_when_a_consumer_reappears() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;
		 CREATE thing:1 SET val = 'a';",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let dj = DocLookupIdentityPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let mapped = keys_in_range(&ds, dj.clone()).await;
	assert!(!mapped.is_empty(), "the index must have mapped the record it covers");

	// The input a reclaim that committed a page and then had a consumer defined
	// behind its back would have left: a cursor naming what it deleted.
	let tx = ds.transaction(Write).await.unwrap();
	for kind in ALL_DOC_ID_KINDS {
		let rc = ReclaimKey {
			kind,
			ns: ns_id,
			db: db_id,
			tb: Cow::Owned(tb.clone()),
			ix: IndexId(0),
			expunge: Expunge::Keep,
			uid: Uuid::now_v7(),
		};
		let torn = ReclaimState {
			observed_ms: 1,
			cursor: Some(mapped[0].clone()),
		};
		tx.set_key(&rc, &torn).await.unwrap();
	}
	tx.commit().await.unwrap();

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		keys_in_range(&ds, dj).await,
		mapped,
		"the reclaim must leave a space its table still consumes whole"
	);
	assert_eq!(
		reclaim_queue_len(&ds).await,
		0,
		"a lapsed obligation must be retired rather than left to requeue forever"
	);
}

/// A queue entry cannot be claimed by a new consumer once its reclaim started.
///
/// A cursor proves the prefix may already be torn. Withdrawing its entry would
/// either strand that partial state or force the caller back to an unbounded
/// purge, so the claim attempt leaves it queued for the reaper to finish.
#[tokio::test]
async fn a_started_doc_id_reclaim_cannot_be_cancelled() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(&ds, &di, page * 2).await;

	let rc = ReclaimKey {
		kind: ReclaimKind::DocLookup,
		ns: ns_id,
		db: db_id,
		tb: Cow::Owned(tb.clone()),
		ix: IndexId(0),
		expunge: Expunge::Keep,
		uid: Uuid::now_v7(),
	};
	let tx = ds.transaction(Write).await.unwrap();
	tx.set_key(&rc, &ReclaimState::enqueued()).await.unwrap();
	tx.commit().await.unwrap();

	// Drain one page, leaving the entry queued with a durable cursor.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	let after_first_pass = count_range(&ds, di.clone()).await;
	assert!(
		after_first_pass > 0 && after_first_pass < page * 2,
		"the first pass must stop mid-prefix: {after_first_pass} left"
	);

	// A new consumer may only cancel entries that are all untouched. This started,
	// partial set must remain queued for bounded cleanup.
	let tx = ds.transaction(Write).await.unwrap();
	let claim = tx.claim_tb_doc_ids_reclaim(ns_id, db_id, &tb).await.unwrap();
	tx.commit().await.unwrap();
	assert_eq!(claim, DocIdsReclaimClaim::InProgress);
	assert_eq!(reclaim_queue_len(&ds).await, 1, "the started entry must remain queued");

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		count_range(&ds, di).await,
		0,
		"the bounded reclaim must finish after the claim attempt is rejected"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the completed entry must retire");
}

/// A claim made inside the grace is not refused by the reclaim task.
///
/// The task stamps every other kind of entry on first sight, but leaves a
/// doc-ID entry unwritten until its grace has passed: a `DEFINE INDEX` that
/// claims the space back compare-deletes the entries, and a stamp committed
/// while it is open would refuse it with a write conflict though nothing had
/// been deleted.
#[tokio::test]
async fn a_claim_inside_the_grace_meets_no_stamp() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;
		 CREATE thing:1 SET val = 1;",
		&ses,
		None,
	)
	.await
	.unwrap();
	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();
	assert_eq!(count_doc_kinds(&reclaim_queue_kinds(&ds).await), ALL_DOC_ID_KINDS.len());
	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");

	// The claim's transaction stays open across a whole pass inside the grace.
	let tx = ds.transaction(Write).await.unwrap();
	let claim = tx.claim_tb_doc_ids_reclaim(ns_id, db_id, &tb).await.unwrap();
	assert_eq!(claim, DocIdsReclaimClaim::Available);
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::from_secs(3600),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	tx.commit().await.expect("a pass inside the grace must not refuse the claim");

	// The claim withdrew every doc-ID entry. The index's own entry stays queued,
	// stamped by the pass as every other kind is.
	let tx = ds.transaction(Read).await.unwrap();
	let items = tx.getr(ReclaimPrefix {}.range().unwrap(), None).await.unwrap();
	let _ = tx.cancel().await;
	let left: Vec<(ReclaimKind, u64)> = items
		.iter()
		.map(|(k, state)| (ReclaimKey::decode_key(k).unwrap().kind, state.observed_ms))
		.collect();
	assert_eq!(left.len(), 1, "only the index's own entry may remain: {left:?}");
	assert_eq!(left[0].0, ReclaimKind::Index);
	assert_ne!(left[0].1, 0, "the index's own entry must be stamped");
}

/// The queue key of a doc-ID reclaim entry of `kind` for table `tb`.
fn doc_id_entry(
	kind: ReclaimKind,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
) -> ReclaimKey<'static> {
	ReclaimKey {
		kind,
		ns,
		db,
		tb: Cow::Owned(tb.clone()),
		ix: IndexId(0),
		expunge: Expunge::Keep,
		uid: Uuid::now_v7(),
	}
}

/// Queue `rcs` with `state`, in one transaction.
async fn queue_entries(ds: &Datastore, rcs: &[ReclaimKey<'_>], state: &ReclaimState) {
	let tx = ds.transaction(Write).await.unwrap();
	for rc in rcs {
		tx.set_key(rc, state).await.unwrap();
	}
	tx.commit().await.unwrap();
}

/// The state of the queue entry `rc`, which must be queued.
async fn entry_state(ds: &Datastore, rc: &ReclaimKey<'_>) -> ReclaimState {
	let tx = ds.transaction(Read).await.unwrap();
	let state = tx.get_key(rc, None).await.unwrap().expect("the entry must be queued");
	let _ = tx.cancel().await;
	state
}

/// A table `thing` in a fresh datastore, with `keys` keys seeded under its
/// `!di` prefix: the ids, the table, and the prefix.
async fn doc_id_space(
	keys: usize,
) -> (Arc<Datastore>, NamespaceId, DatabaseId, TableName, RawRange) {
	let ds = mem_ds().await;
	let (ns_id, db_id, tb, di) = seed_doc_id_space(&ds, keys).await;
	(ds, ns_id, db_id, tb, di)
}

/// A table `thing` in `ds`, with `keys` keys seeded under its `!di` prefix:
/// the ids, the table, and the prefix.
async fn seed_doc_id_space(
	ds: &Datastore,
	keys: usize,
) -> (NamespaceId, DatabaseId, TableName, RawRange) {
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;",
		&ses,
		None,
	)
	.await
	.unwrap();
	let (ns_id, db_id) = ids(ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(ds, &di, keys).await;
	(ns_id, db_id, tb, di)
}

/// An unstamped doc-ID entry ages from the reclaim task's first sighting of it,
/// held in memory: the pass that first sees it writes nothing and deletes
/// nothing, and a pass once the grace has passed since reclaims it.
#[tokio::test]
async fn a_doc_id_entry_ages_from_its_first_sighting_unwritten() {
	let (ds, ns_id, db_id, tb, di) = doc_id_space(RECLAIM_BATCH_SIZE as usize / 2).await;
	let rc = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb);
	queue_entries(&ds, std::slice::from_ref(&rc), &ReclaimState::enqueued()).await;
	let grace = Duration::from_millis(300);
	let pass = || {
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			grace,
			CancellationToken::new(),
		)
	};

	pass().await.unwrap();
	assert_eq!(
		entry_state(&ds, &rc).await,
		ReclaimState::enqueued(),
		"the pass that first sees a doc-ID entry must not write it"
	);
	assert_eq!(count_range(&ds, di.clone()).await, RECLAIM_BATCH_SIZE as usize / 2);

	tokio::time::sleep(grace + Duration::from_millis(100)).await;
	pass().await.unwrap();
	assert_eq!(count_range(&ds, di).await, 0, "an entry seen a grace ago must be reclaimed");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the completed entry must retire");
}

/// A sighting is forgotten on a restart or a lease move, so an untouched doc-ID
/// entry queued for twice the grace that the task holds no sighting of is
/// stamped instead, and ages durably from the stamp: a lease holder that
/// churns faster than the grace cannot defer the reclaim for good.
#[tokio::test]
async fn a_doc_id_entry_queued_past_the_backstop_is_stamped() {
	backstop_stamps_an_entry_queued_at(|now, grace| now - grace * 3).await;
}

/// A `uid` dated past the backstop ahead of the reclaiming node's clock came
/// from a clock that cannot date the entry, so it is stamped on sight rather
/// than holding the backstop off until that clock's time.
#[tokio::test]
async fn a_doc_id_entry_queued_far_in_the_future_is_stamped() {
	backstop_stamps_an_entry_queued_at(|now, grace| now + grace * 3).await;
}

/// Queue an untouched doc-ID entry whose `uid` is dated `queued(now, grace)`
/// and assert that the first pass stamps it, and that it is reclaimed a grace
/// after the stamp.
async fn backstop_stamps_an_entry_queued_at(queued: impl Fn(Duration, Duration) -> Duration) {
	let (ds, ns_id, db_id, tb, di) = doc_id_space(RECLAIM_BATCH_SIZE as usize / 2).await;
	let grace = Duration::from_millis(300);
	let rc = ReclaimKey {
		uid: uid_at(queued(Duration::from_millis(now_ms()), grace)),
		..doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb)
	};
	queue_entries(&ds, std::slice::from_ref(&rc), &ReclaimState::enqueued()).await;
	let pass = || {
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			grace,
			CancellationToken::new(),
		)
	};

	let before = now_ms();
	pass().await.unwrap();
	let after = now_ms();
	let state = entry_state(&ds, &rc).await;
	assert!(
		(before..=after).contains(&state.observed_ms),
		"an entry queued past the backstop must be stamped on sight, got {state:?}"
	);
	assert_eq!(state.cursor, None);
	assert_eq!(ds.reclaim_sightings_len(), 0, "a stamped entry needs no sighting");
	assert_eq!(
		count_range(&ds, di.clone()).await,
		RECLAIM_BATCH_SIZE as usize / 2,
		"a stamped entry still waits out its grace"
	);

	tokio::time::sleep(grace + Duration::from_millis(100)).await;
	pass().await.unwrap();
	assert_eq!(count_range(&ds, di).await, 0, "an entry stamped a grace ago must be reclaimed");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the completed entry must retire");
}

/// An entry queued after this node's sightings began that holds no sighting
/// has not been seen yet rather than seen and forgotten, so it is sighted, not
/// stamped, even when its `uid` is past the backstop: a `REMOVE` that stayed
/// open for twice the grace before it committed still leaves a claim its
/// conflict-free window.
#[tokio::test]
async fn a_doc_id_entry_queued_after_the_sightings_began_is_sighted() {
	let (ds, ns_id, db_id, tb, di) = doc_id_space(RECLAIM_BATCH_SIZE as usize / 2).await;
	let grace = Duration::from_millis(300);
	let pass = || {
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			grace,
			CancellationToken::new(),
		)
	};
	// The sightings begin with this pass, before the removal's `uid`.
	pass().await.unwrap();
	let rc = ReclaimKey {
		uid: uid_at(Duration::from_millis(now_ms())),
		..doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb)
	};
	// The removal commits only once its `uid` is past the backstop.
	tokio::time::sleep(grace * 2 + Duration::from_millis(100)).await;
	queue_entries(&ds, std::slice::from_ref(&rc), &ReclaimState::enqueued()).await;

	pass().await.unwrap();
	assert_eq!(
		entry_state(&ds, &rc).await,
		ReclaimState::enqueued(),
		"an entry queued after the sightings began must be sighted, not stamped"
	);
	assert_eq!(ds.reclaim_sightings_len(), 1);

	tokio::time::sleep(grace + Duration::from_millis(100)).await;
	pass().await.unwrap();
	assert_eq!(count_range(&ds, di).await, 0, "an entry seen a grace ago must be reclaimed");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the completed entry must retire");
}

/// A version-7 `uid` dated `at` after the unix epoch.
fn uid_at(at: Duration) -> Uuid {
	Uuid::new_v7(uuid::Timestamp::from_unix(uuid::NoContext, at.as_secs(), at.subsec_nanos()))
}

/// The first page of an unstamped doc-ID entry records the task's sighting of
/// it with the cursor, so a restart or a lease move resumes a started entry
/// against the time it was first seen rather than starting its grace again.
#[tokio::test]
async fn a_doc_id_entry_records_its_sighting_with_its_first_page() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let (ds, ns_id, db_id, tb, di) = doc_id_space(page * 3).await;
	let rc = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb);
	queue_entries(&ds, std::slice::from_ref(&rc), &ReclaimState::enqueued()).await;
	let grace = Duration::from_millis(300);

	let before = now_ms();
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		grace,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	let after = now_ms();
	tokio::time::sleep(grace + Duration::from_millis(100)).await;
	// One page of budget, so the entry stays queued with its cursor.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		grace,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert!(count_range(&ds, di).await > 0, "one page must leave the prefix part-reclaimed");
	let state = entry_state(&ds, &rc).await;
	assert!(state.cursor.is_some(), "the page must record its cursor");
	assert!(
		(before..=after).contains(&state.observed_ms),
		"the page must record the first sighting, {} not in {before}..={after}",
		state.observed_ms
	);
	assert_eq!(ds.reclaim_sightings_len(), 0, "a recorded sighting is not held in memory too");
}

/// A reclaim that fails before its first page lands keeps the entry's
/// sighting, so the next pass retries it at once rather than starting its grace
/// again.
#[tokio::test]
async fn a_failed_first_page_keeps_the_sighting() {
	use crate::kvs::testing::{RetryableConflictSite, inject_retryable_conflict};

	let (ds, ns_id, db_id, tb, di) = doc_id_space(RECLAIM_BATCH_SIZE as usize / 2).await;
	let rc = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb);
	queue_entries(&ds, std::slice::from_ref(&rc), &ReclaimState::enqueued()).await;
	let grace = Duration::from_millis(300);
	let pass = || {
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			grace,
			CancellationToken::new(),
		)
	};
	pass().await.unwrap();
	tokio::time::sleep(grace + Duration::from_millis(100)).await;

	// The entry is eligible, and its first page loses its commit.
	{
		let _lost = inject_retryable_conflict(RetryableConflictSite::ReclaimPageCommit, ds.id());
		pass().await.unwrap();
	}
	assert_eq!(entry_state(&ds, &rc).await, ReclaimState::enqueued(), "the entry is untouched");
	assert_eq!(ds.reclaim_sightings_len(), 1, "a failed first page must keep the sighting");

	pass().await.unwrap();
	assert_eq!(count_range(&ds, di).await, 0, "the next pass must reclaim without a new grace");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the completed entry must retire");
}

/// An entry whose prefix is already empty completes without a page, and keeps
/// its sighting until its retirement commits: a retirement that fails leaves the
/// entry to the next pass, which retires it at once rather than starting its
/// grace again.
#[tokio::test]
async fn a_failed_retirement_keeps_the_sighting() {
	use crate::kvs::testing::{RetryableConflictSite, inject_retryable_conflict};

	let (ds, ns_id, db_id, tb, _) = doc_id_space(0).await;
	let rc = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb);
	queue_entries(&ds, std::slice::from_ref(&rc), &ReclaimState::enqueued()).await;
	let grace = Duration::from_millis(300);
	let pass = || {
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			grace,
			CancellationToken::new(),
		)
	};
	pass().await.unwrap();
	tokio::time::sleep(grace + Duration::from_millis(100)).await;

	// The entry is eligible and its empty prefix completes, but its retirement
	// loses its commit.
	{
		let _lost = inject_retryable_conflict(RetryableConflictSite::ReclaimQueueRetire, ds.id());
		pass().await.unwrap();
	}
	assert_eq!(entry_state(&ds, &rc).await, ReclaimState::enqueued(), "the entry is untouched");
	assert_eq!(ds.reclaim_sightings_len(), 1, "a failed retirement must keep the sighting");

	pass().await.unwrap();
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the next pass must retire without a new grace");
	assert_eq!(ds.reclaim_sightings_len(), 0, "a retired entry leaves no sighting");
}

/// An entry a retirement could not delete stays queued and keeps its sighting,
/// while the entries retired beside it in the same transaction forget theirs.
#[tokio::test]
async fn an_entry_left_by_a_retirement_keeps_its_sighting() {
	use crate::kvs::testing::{NonRetryableErrorSite, inject_non_retryable_error};

	let (ds, ns_id, db_id, tb, _) = doc_id_space(0).await;
	let rcs = [
		doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb),
		doc_id_entry(ReclaimKind::DocLookupIdentity, ns_id, db_id, &tb),
	];
	queue_entries(&ds, &rcs, &ReclaimState::enqueued()).await;
	let grace = Duration::from_millis(300);
	let pass = || {
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			grace,
			CancellationToken::new(),
		)
	};
	pass().await.unwrap();
	assert_eq!(ds.reclaim_sightings_len(), 2);
	tokio::time::sleep(grace + Duration::from_millis(100)).await;

	// Both prefixes are empty and both entries complete; one entry's delete fails.
	{
		let _failed =
			inject_non_retryable_error(NonRetryableErrorSite::ReclaimQueueRetireEntry, ds.id());
		pass().await.unwrap();
	}
	assert_eq!(reclaim_queue_len(&ds).await, 1, "one entry must be retired and one left");
	assert_eq!(ds.reclaim_sightings_len(), 1, "the entry left must keep its sighting");

	pass().await.unwrap();
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the next pass must retire without a new grace");
}

/// A doc-ID entry with a cursor but no stamp has started, and is stamped
/// rather than held back as untouched by an in-memory sighting.
#[tokio::test]
async fn a_started_doc_id_entry_without_a_stamp_is_stamped() {
	let (ds, ns_id, db_id, tb, di) = doc_id_space(0).await;
	let rc = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb);
	let started = ReclaimState {
		observed_ms: 0,
		cursor: Some(di.start().as_ref().to_vec()),
	};
	queue_entries(&ds, std::slice::from_ref(&rc), &started).await;

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::from_secs(3600),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(ds.reclaim_sightings_len(), 0, "a started entry must not be sighted");
	assert_ne!(entry_state(&ds, &rc).await.observed_ms, 0, "a started entry must be stamped");
}

/// The doc-ID drain records an observation with every cursor it writes, so an
/// entry it leaves part-drained is never taken for an untouched one.
#[tokio::test]
async fn the_doc_id_drain_records_an_observation_with_its_cursor() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let (ds, ns_id, db_id, tb, di) = doc_id_space(page * 3).await;
	// Two kinds of the four, so the set reads as started.
	let rcs = [
		doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb),
		doc_id_entry(ReclaimKind::DocLookupIdentity, ns_id, db_id, &tb),
	];
	queue_entries(&ds, &rcs, &ReclaimState::enqueued()).await;

	ds.finish_tb_doc_ids_reclaim_step(ns_id, db_id, &tb, page as u64, &CancellationToken::new())
		.await
		.unwrap();
	assert!(count_range(&ds, di).await > 0, "one page must leave the prefix part-drained");
	let state = entry_state(&ds, &rcs[0]).await;
	assert!(state.cursor.is_some(), "the drain must record its cursor");
	assert_ne!(state.observed_ms, 0, "the drain must record an observation with it");
}

/// A pass that finds the lease held by another node forgets its sightings: the
/// node holding the lease now ages entries from its own, and a sighting kept
/// across a lease that moved away and back would date an entry from before.
#[tokio::test]
async fn a_pass_without_the_lease_forgets_its_sightings() {
	use crate::catalog::TaskLease;
	use crate::key::schema::TaskLeaseKey;
	use crate::kvs::tasklease::TaskLeaseType;

	let (ds, ns_id, db_id, tb, _) = doc_id_space(0).await;
	let rc = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb);
	queue_entries(&ds, &[rc], &ReclaimState::enqueued()).await;
	let pass = || {
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			Duration::from_secs(3600),
			CancellationToken::new(),
		)
	};
	pass().await.unwrap();
	assert_eq!(ds.reclaim_sightings_len(), 1);

	// Another node takes the lease.
	let tx = ds.transaction(Write).await.unwrap();
	tx.set_key(
		&TaskLeaseKey {
			task_id: TaskLeaseType::ReclaimTombstones,
		},
		&TaskLease::new(Uuid::from_u128(2), chrono::Utc::now() + chrono::Duration::hours(1)),
	)
	.await
	.unwrap();
	tx.commit().await.unwrap();

	pass().await.unwrap();
	assert_eq!(ds.reclaim_sightings_len(), 0, "a pass without the lease must forget its sightings");
}

/// A doc-ID entry that carries a stamp, from an older node or a repair, ages
/// from that stamp.
#[tokio::test]
async fn a_stamped_doc_id_entry_ages_from_its_stamp() {
	let (ds, ns_id, db_id, tb, di) = doc_id_space(RECLAIM_BATCH_SIZE as usize / 2).await;
	let grace = Duration::from_secs(60);
	let rc = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb);
	let stamped = ReclaimState {
		observed_ms: now_ms() - 2 * grace.as_millis() as u64,
		cursor: None,
	};
	queue_entries(&ds, &[rc], &stamped).await;

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		grace,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(count_range(&ds, di).await, 0, "an entry stamped a grace ago must be reclaimed");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the completed entry must retire");
}

/// The task forgets the sighting of a doc-ID entry once the entry has gone,
/// wherever its key sorts: past every entry still queued, or with nothing
/// queued at all.
#[tokio::test]
async fn sightings_of_gone_entries_are_forgotten() {
	let (ds, ns_id, db_id, _, _) = doc_id_space(0).await;
	let pass = || {
		Datastore::reclaim_tombstones(
			Arc::clone(&ds),
			Duration::from_secs(1),
			Duration::from_secs(3600),
			CancellationToken::new(),
		)
	};
	let dequeue = |rc: ReclaimKey<'static>| {
		let ds = Arc::clone(&ds);
		async move {
			let tx = ds.transaction(Write).await.unwrap();
			tx.del_key(&rc).await.unwrap();
			tx.commit().await.unwrap();
		}
	};

	let b = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &TableName::from("b"));
	queue_entries(&ds, std::slice::from_ref(&b), &ReclaimState::enqueued()).await;
	pass().await.unwrap();
	assert_eq!(ds.reclaim_sightings_len(), 1);

	// `b` is claimed, and an entry whose key sorts below it is queued.
	dequeue(b).await;
	let a = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &TableName::from("a"));
	queue_entries(&ds, std::slice::from_ref(&a), &ReclaimState::enqueued()).await;
	pass().await.unwrap();
	assert_eq!(ds.reclaim_sightings_len(), 1, "only the entry still queued may be sighted");

	// `a` is claimed too, leaving nothing queued.
	dequeue(a).await;
	pass().await.unwrap();
	assert_eq!(ds.reclaim_sightings_len(), 0, "an empty queue leaves no sighting");
}

/// A doc-ID entry whose state could not be read is repaired as started, with
/// an empty cursor: the state it lost may have held a cursor, so the space may
/// be torn, and a claim must not adopt it.
#[tokio::test]
async fn a_repaired_doc_id_entry_reads_as_started() {
	let (ds, ns_id, db_id, tb, _) = doc_id_space(0).await;
	let rcs: Vec<_> =
		ALL_DOC_ID_KINDS.into_iter().map(|kind| doc_id_entry(kind, ns_id, db_id, &tb)).collect();
	queue_entries(&ds, &rcs, &ReclaimState::enqueued()).await;
	// Bytes no revision of the state decodes to.
	{
		let tx = ds.transaction(Write).await.unwrap();
		let key = crate::key::KVKey::encode_key(&rcs[0]).unwrap();
		tx.set(key, vec![0xde, 0xad, 0xbe, 0xef]).await.unwrap();
		tx.commit().await.unwrap();
	}

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::from_secs(3600),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(entry_state(&ds, &rcs[0]).await.cursor, Some(Vec::new()));

	let tx = ds.transaction(Write).await.unwrap();
	let claim = tx.claim_tb_doc_ids_reclaim(ns_id, db_id, &tb).await.unwrap();
	tx.cancel().await.unwrap();
	assert_eq!(claim, DocIdsReclaimClaim::InProgress, "a repaired entry must read as started");
}

/// A queued doc-ID reclaim is retired without deleting anything once the table
/// has a doc-ID consumer again.
///
/// The queue entry claims the space is orphaned, which only holds while no index
/// consumes it. A consumer present when the reaper looks means one was defined
/// without cancelling the entry — a node too old to decode these kinds is the
/// case that matters — so the space is live and must be left whole.
#[tokio::test]
async fn a_queued_doc_id_reclaim_is_retired_once_the_table_has_a_consumer_again() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;
		 CREATE thing:1 SET val = 'a';",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let dj = DocLookupIdentityPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let mapped = keys_in_range(&ds, dj.clone()).await;
	assert!(!mapped.is_empty(), "the index must have mapped the record it covers");

	// The input a node that queued the space and then had a consumer defined
	// behind its back would have left committed.
	let tx = ds.transaction(Write).await.unwrap();
	for kind in ALL_DOC_ID_KINDS {
		let rc = ReclaimKey {
			kind,
			ns: ns_id,
			db: db_id,
			tb: Cow::Owned(tb.clone()),
			ix: IndexId(0),
			expunge: Expunge::Keep,
			uid: Uuid::now_v7(),
		};
		tx.set_key(&rc, &ReclaimState::enqueued()).await.unwrap();
	}
	tx.commit().await.unwrap();

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(
		keys_in_range(&ds, dj).await,
		mapped,
		"the reclaim must leave a space its table still consumes whole"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "a lapsed obligation must be retired");
}

/// Data orphaned by a reclaim that reported success without destroying anything
/// is recoverable by naming its prefix in the queue again.
#[tokio::test]
async fn an_orphaned_namespace_prefix_can_be_requeued_and_reclaimed() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 CREATE thing:1 SET val = 'a';
		 CREATE thing:2 SET val = 'b';",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, _) = ids(&ds, "test", "tenant").await;
	let ns_root = NsRoot::new(ns_id).raw_range().unwrap();
	assert!(count_range(&ds, ns_root.clone()).await > 0, "the namespace must hold data");

	ds.execute("REMOVE NAMESPACE test;", &ses, None).await.unwrap();

	// The state a reclaim that reported success without destroying anything
	// leaves behind: catalog entry committed-removed, queue entry retired, data
	// prefix untouched with nothing naming it.
	let tx = ds.transaction(Write).await.unwrap();
	for k in keys_in_range(&ds, ReclaimPrefix {}.range().unwrap()).await {
		tx.del(Key::from(k)).await.unwrap();
	}
	tx.commit().await.unwrap();
	assert_eq!(reclaim_queue_len(&ds).await, 0, "nothing may name the prefix");
	assert!(count_range(&ds, ns_root.clone()).await > 0, "the data must outlive its catalog");

	// Recovery: name the prefix again. Namespace ids come from a monotonic
	// root-level sequence with no free list, so an id holding data while absent
	// from the catalog can only be this orphan.
	let tx = ds.transaction(Write).await.unwrap();
	tx.set_key(&ReclaimKey::namespace(ns_id, false, Uuid::now_v7()), &ReclaimState::enqueued())
		.await
		.unwrap();
	tx.commit().await.unwrap();

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	assert_eq!(count_range(&ds, ns_root).await, 0, "the requeued prefix must be destroyed");
	assert_eq!(reclaim_queue_len(&ds).await, 0, "and its entry retired");
}

/// Rewrites the stored `format_version` of an index, simulating one persisted by
/// a binary that predates the shared doc-ID space. The table definition is
/// rewritten in the same transaction so cached index lists are refreshed.
async fn downgrade_index_format(ds: &Datastore, tb: &TableName, ix: &str, format_version: u16) {
	let txn = ds.transaction(Write).await.unwrap();
	let (ns, db) = ids(ds, "test", "tenant").await;
	let def = txn.get_tb_index(ns, db, tb, ix, None).await.unwrap().expect("index should exist");
	let mut old = (*def).clone();
	old.format_version = format_version;
	txn.put_tb_index(ns, db, tb, &old).await.unwrap();
	let tb_def = txn.expect_tb(ns, db, tb).await.unwrap();
	txn.put_tb("test", "tenant", &tb_def).await.unwrap();
	txn.commit().await.unwrap();
}

/// `REBUILD INDEX` upgrading a legacy index cancels the queued reclaim of the
/// shared doc-ID space.
///
/// A table can keep a legacy-format index after its last shared-doc-ID consumer
/// is removed, which queues the shared space for paged reclaim. Stamping the
/// current format makes that legacy index a consumer, adopting whatever the space
/// still holds through `resolve_or_assign`'s fast path — so the rebuild has to
/// withdraw the claim in the same transaction that stamps the format, or the
/// reaper goes on to delete mappings the rebuilt index is resolving through.
#[tokio::test]
async fn rebuilding_a_legacy_index_cancels_a_queued_doc_id_reclaim() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX legacy ON thing FIELDS a;",
		&ses,
		None,
	)
	.await
	.unwrap();

	// Downgraded while empty: only the definition changes, so data written in
	// another layout would otherwise be left unreachable.
	let tb = TableName::from("thing");
	downgrade_index_format(&ds, &tb, "legacy", 0).await;
	ds.execute(
		"DEFINE INDEX current ON thing FIELDS b;
		 CREATE thing:1 SET a = 'x', b = 'y';
		 CREATE thing:2 SET a = 'p', b = 'q';",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let dj = DocLookupIdentityPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let mapped = keys_in_range(&ds, dj.clone()).await;
	assert!(!mapped.is_empty(), "the current-format index must have mapped its records");

	// The legacy index is not a consumer, so dropping the only current-format
	// one drops the last consumer and queues the shared space.
	ds.execute("REMOVE INDEX current ON thing;", &ses, None).await.unwrap();
	let kinds = reclaim_queue_kinds(&ds).await;
	assert_eq!(
		count_doc_kinds(&kinds),
		ALL_DOC_ID_KINDS.len(),
		"dropping the last consumer must queue every shared doc-ID prefix"
	);

	// Stamping the current format makes the legacy index a consumer.
	ds.execute("REBUILD INDEX legacy ON thing;", &ses, None).await.unwrap();
	let kinds = reclaim_queue_kinds(&ds).await;
	assert_eq!(
		count_doc_kinds(&kinds),
		0,
		"the rebuild must withdraw every queued reclaim of the space it now consumes"
	);

	// And the reaper must then leave the space alone.
	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert!(
		!keys_in_range(&ds, dj).await.is_empty(),
		"the reclaim must not delete mappings the rebuilt index resolves through"
	);
}

/// A legacy-index rebuild cannot adopt a shared doc-ID space after its bounded
/// reclaim has started, so it finishes the reclaim and then stamps the current
/// format.
#[tokio::test]
async fn rebuilding_a_legacy_index_finishes_a_partly_reclaimed_doc_id_space() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX legacy ON thing FIELDS a;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let tb = TableName::from("thing");
	downgrade_index_format(&ds, &tb, "legacy", 0).await;
	ds.execute(
		"DEFINE INDEX current ON thing FIELDS b;
		 CREATE thing:1 SET a = 'x', b = 'y';
		 CREATE thing:2 SET a = 'p', b = 'q';",
		&ses,
		None,
	)
	.await
	.unwrap();
	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(&ds, &di, page * 2).await;
	seed_range(&ds, &dd, page * 2).await;
	ds.execute("REMOVE INDEX current ON thing;", &ses, None).await.unwrap();

	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	let left = count_range(&ds, di.clone()).await + count_range(&ds, dd.clone()).await;
	assert!(left > 0, "the pass must have left the space torn");

	let mut response = ds.execute("REBUILD INDEX legacy ON thing;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
	// What is left is the rebuilt index's own mappings for the table's two
	// records, not the seeded remainder.
	let after = count_range(&ds, di).await + count_range(&ds, dd).await;
	assert!(after <= 4, "the rebuild must finish the reclaim before adopting the space: {after}");
	assert_eq!(count_doc_kinds(&reclaim_queue_kinds(&ds).await), 0);
	let txn = ds.transaction(Read).await.unwrap();
	let legacy = txn.get_tb_index(ns_id, db_id, &tb, "legacy", None).await.unwrap().unwrap();
	assert!(legacy.uses_shared_doc_ids(), "the rebuild must stamp the current format");
	let _ = txn.cancel().await;
}

/// Inside a transaction block, which the executor cannot run again, a
/// definition over a torn doc-ID space fails with a retryable transaction
/// conflict, and the reclaim is finished before the block's result returns, so
/// the client's retry succeeds at once.
#[tokio::test]
async fn a_block_defining_a_doc_id_index_over_a_torn_space_can_be_retried_at_once() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();
	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(&ds, &di, page * 2).await;
	seed_range(&ds, &dd, page * 2).await;
	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert!(count_range(&ds, di.clone()).await + count_range(&ds, dd.clone()).await > 0);

	let block = "BEGIN; DEFINE INDEX idx ON thing FIELDS val; COMMIT;";
	let response = ds.execute(block, &ses, None).await.unwrap();
	let error = response
		.into_iter()
		.find_map(|result| result.result.err())
		.expect("the block's definition must fail");
	assert_eq!(
		error.query_details(),
		Some(&surrealdb_types::QueryError::TransactionConflict),
		"the refusal must be a conflict an SDK retries: {error}"
	);
	assert!(error.to_string().contains("still being reclaimed"), "{error}");
	assert_eq!(
		count_range(&ds, di).await + count_range(&ds, dd).await,
		0,
		"the reclaim must be finished before the block's result returns"
	);

	for result in ds.execute(block, &ses, None).await.unwrap() {
		result.result.unwrap();
	}
	let _ = index_id(&ds, ns_id, db_id, "thing", "idx").await;
}

/// Queue one database-reclaim entry with its observation stamp staged, and seed
/// `keys` data keys under the prefix it names. Returns the prefix.
async fn queue_database_entry(ds: &Datastore, db: u32, observed_ms: u64, keys: usize) -> RawRange {
	let range = DbRoot {
		ns: NamespaceId(1),
		db: DatabaseId(db),
	}
	.range()
	.unwrap();
	if keys > 0 {
		seed_range(ds, &range, keys).await;
	}
	let rc = ReclaimKey::database(NamespaceId(1), DatabaseId(db), false, Uuid::now_v7());
	let tx = ds.transaction(Write).await.unwrap();
	tx.set_key(
		&rc,
		&ReclaimState {
			observed_ms,
			cursor: None,
		},
	)
	.await
	.unwrap();
	tx.commit().await.unwrap();
	range
}

/// Every queued entry's observation stamp, in queue key order.
async fn queue_observations(ds: &Datastore) -> Vec<u64> {
	let txn = ds.transaction(Read).await.unwrap();
	let items: Vec<(Vec<u8>, ReclaimState)> =
		txn.getr(ReclaimPrefix {}.range().unwrap(), None).await.unwrap();
	let _ = txn.cancel().await;
	items.iter().map(|(_, state)| state.observed_ms).collect()
}

/// A pass stamps every entry it walks past, whatever its key budget.
///
/// Stamping is what starts an entry's grace, and an entry that has never been
/// stamped is not yet a candidate for reclaim at all. Gating the walk on the
/// budget therefore does not merely delay the entries it cannot reach — it stops
/// their clocks from starting, so they are still ineligible however many passes
/// run. The walk costs at most one write per entry, which scales with the number
/// of pending removals rather than with the data they name, so it is not what the
/// key budget bounds.
#[tokio::test]
async fn the_walk_stamps_entries_the_budget_cannot_reach() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let grace = Duration::from_secs(600);
	// Aged entries holding more data than one pass may delete, so the budget is
	// gone before the walk would have reached the entries behind them.
	let aged = now_ms().saturating_sub(2 * grace.as_millis() as u64);
	for db in 1..=2u32 {
		queue_database_entry(&ds, db, aged, page * 2).await;
	}
	// Never-observed entries behind them in queue order: higher database ids sort
	// later, and the queue is keyed by kind then namespace then database.
	for db in 10..=12u32 {
		queue_database_entry(&ds, db, 0, page).await;
	}
	assert_eq!(reclaim_queue_len(&ds).await, 5);

	// A budget of one page: the two aged entries cannot finish, let alone leave
	// anything for the three behind them.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		grace,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	let observed = queue_observations(&ds).await;
	assert_eq!(observed.len(), 5, "no entry may be retired: none of the prefixes is empty");
	assert!(
		observed.iter().all(|&ms| ms != 0),
		"every entry the pass walked past must be stamped, budget or not: {observed:?}"
	);
}

/// The key budget goes to the longest-waiting entries, not to whichever sort
/// lowest.
///
/// Queue order is a byte order over namespace and database ids, which says
/// nothing about how long a removal has gone unreclaimed. Spending in that order
/// serves the same leading entries every pass while they hold data; spending
/// oldest-first is the wait an operator is actually asking about.
#[tokio::test]
async fn the_budget_goes_to_the_longest_waiting_entry() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let grace = Duration::from_secs(600);
	let base = now_ms().saturating_sub(2 * grace.as_millis() as u64);
	// Age runs against key order: the lowest database id was observed most
	// recently, so a pass spending in key order would serve it first.
	let newest = queue_database_entry(&ds, 1, base + 60_000, page * 2).await;
	let middle = queue_database_entry(&ds, 2, base + 30_000, page * 2).await;
	let oldest = queue_database_entry(&ds, 3, base, page * 2).await;

	let before = [
		count_range(&ds, newest.clone()).await,
		count_range(&ds, middle.clone()).await,
		count_range(&ds, oldest.clone()).await,
	];

	// A budget of one key: every candidate is granted at least one key, so a
	// budget smaller than the number of candidates is spent before the pass
	// reaches them all — which is what makes the order it serves them in
	// observable. A budget that covers every candidate is shared between them,
	// which `reclaim_shares_one_passs_budget_across_entries` already pins.
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		grace,
		1,
		CancellationToken::new(),
	)
	.await
	.unwrap();

	let deleted = [
		before[0] - count_range(&ds, newest).await,
		before[1] - count_range(&ds, middle).await,
		before[2] - count_range(&ds, oldest).await,
	];
	assert_eq!(
		deleted.iter().sum::<usize>(),
		1,
		"the pass must spend its whole budget and no more: {deleted:?}"
	);
	assert_eq!(
		deleted,
		[0, 0, 1],
		"the budget must go to the oldest observation, not the lowest key: {deleted:?}"
	);
}

/// A batch that comes back short ends the scan even when its last key is below
/// the end the scan began with: the entry at that end has gone since (claimed or
/// retired), and nothing lies past the batch. The pass reports its walk done
/// rather than saving a position past the end of the queue.
#[tokio::test]
async fn a_short_batch_ends_the_scan() {
	let ds = mem_ds().await;
	let grace = Duration::from_secs(600);
	let batch = RECLAIM_BATCH_SIZE as usize;
	let slice = RECLAIM_PASS_WALK_BATCHES * batch;

	// A first pass walks `slice` entries. The second resumes with one batch
	// fewer than a pass walks still full, then one entry, then the scan's end.
	let total = slice + (RECLAIM_PASS_WALK_BATCHES - 1) * batch + 2;
	queue_database_entries(&ds, 1..=(total as u32)).await;
	let (_, first) = walk_pass(&ds, grace).await;
	assert_eq!(first.progress, Progress::More);

	// The entry at the end the scan began with goes, so the second pass's last
	// batch is short and ends below that end.
	let end = keys_in_range(&ds, ReclaimPrefix {}.range().unwrap()).await.pop().unwrap();
	let tx = ds.transaction(Write).await.unwrap();
	tx.del(Key::from(&end)).await.unwrap();
	tx.commit().await.unwrap();

	let (_, second) = walk_pass(&ds, grace).await;
	assert_eq!(second.progress, Progress::Done, "a short batch must end the walk");
}

/// Queue one database-reclaim entry per id in `dbs`, none yet observed and each
/// naming an empty prefix, in one transaction.
async fn queue_database_entries(ds: &Datastore, dbs: std::ops::RangeInclusive<u32>) {
	let tx = ds.transaction(Write).await.unwrap();
	for db in dbs {
		let rc = ReclaimKey::database(NamespaceId(1), DatabaseId(db), false, Uuid::now_v7());
		tx.set_key(
			&rc,
			&ReclaimState {
				observed_ms: 0,
				cursor: None,
			},
		)
		.await
		.unwrap();
	}
	tx.commit().await.unwrap();
}

/// One reclaim pass with the full key budget: the batches it walked, and what
/// it reported.
async fn walk_pass(ds: &Arc<Datastore>, grace: Duration) -> (usize, Pass) {
	let (batches, errors, pass) = Datastore::reclaim_tombstones_with_budget(
		Arc::clone(ds),
		Duration::from_secs(1),
		grace,
		RECLAIM_PASS_KEY_BUDGET,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(errors, 0);
	(batches, pass)
}

/// How many of `observed` are stamped.
fn stamped(observed: &[u64]) -> usize {
	observed.iter().filter(|&&ms| ms != 0).count()
}

/// A pass walks at most `RECLAIM_PASS_WALK_BATCHES` batches of the queue, and
/// the scan it continues ends at the last entry there was when it began.
///
/// The walk reads every entry it passes and stamps or ages it, which is bounded
/// by the number of pending removals rather than by the key budget, so a queue
/// that has grown long would otherwise be read and stamped whole in one pass, in
/// a slice the sweep's other jobs and shutdown wait on. A pass that stops with
/// entries still to walk keeps its position and reports `More`, so the scan
/// continues across passes; it ends at the entry that was last when it began, so
/// entries queued past that while it runs are the next scan's and a scan ends
/// however fast removals are queued.
#[tokio::test]
async fn the_walk_is_paged_across_passes() {
	let ds = mem_ds().await;
	let grace = Duration::from_secs(600);
	let batch = RECLAIM_BATCH_SIZE as usize;
	let slice = RECLAIM_PASS_WALK_BATCHES * batch;

	// One entry more than a pass walks, none yet observed.
	queue_database_entries(&ds, 1..=(slice as u32 + 1)).await;
	let (batches, first) = walk_pass(&ds, grace).await;
	assert_eq!(first.progress, Progress::More, "the walk stopped with an entry still to walk");
	assert_eq!(batches, RECLAIM_PASS_WALK_BATCHES);
	assert_eq!(
		stamped(&queue_observations(&ds).await),
		slice,
		"the pass stamps what it walked, and no more"
	);

	// More than a pass walks, queued past the end of the scan while it is
	// stopped.
	let queued = slice + batch;
	let from = slice as u32 + 2;
	queue_database_entries(&ds, from..=(from + queued as u32 - 1)).await;
	let (batches, second) = walk_pass(&ds, grace).await;
	assert_eq!(second.progress, Progress::Done, "the scan ends at the batch that reaches its end");
	assert_eq!(batches, 1);
	assert_eq!(
		stamped(&queue_observations(&ds).await),
		slice + batch,
		"having stamped that batch, and no more"
	);

	// The next scan begins at the head, over everything queued: two passes of
	// entries already stamped, then the batch that reaches its end.
	let (_, third) = walk_pass(&ds, grace).await;
	assert_eq!(third.progress, Progress::More);
	let (_, fourth) = walk_pass(&ds, grace).await;
	assert_eq!(fourth.progress, Progress::More);
	let (batches, fifth) = walk_pass(&ds, grace).await;
	assert_eq!(fifth.progress, Progress::Done);
	assert_eq!(batches, 2);
	let total = slice + 1 + queued;
	assert_eq!(stamped(&queue_observations(&ds).await), total, "every entry is stamped");
	assert_eq!(reclaim_queue_len(&ds).await, total, "none within the grace is retired");
}

/// A step of a refused statement's doc-ID reclaim stops at once when its
/// canceller has tripped, deleting nothing, so a query cancelled or timed out
/// while it waits is not held by the reclaim.
#[tokio::test]
async fn a_cancelled_doc_id_reclaim_step_stops_without_deleting() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();
	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(&ds, &di, page * 2).await;
	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();
	Datastore::reclaim_tombstones_with_budget(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		page as u64,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	let left = count_range(&ds, di.clone()).await;
	assert!(left > 0, "the pass must have left the space torn");

	let canceller = CancellationToken::new();
	canceller.cancel();
	let step = ds
		.finish_tb_doc_ids_reclaim_step(ns_id, db_id, &tb, RECLAIM_PASS_KEY_BUDGET, &canceller)
		.await;
	assert!(step.is_err(), "a cancelled step must stop: {step:?}");
	assert_eq!(count_range(&ds, di).await, left, "a cancelled step must delete nothing");
}

/// A memory datastore with no background job, whose reclaim grace is judged
/// against `clock`: the shape of a host that drives the passes itself.
async fn hosted_ds(clock: Arc<surrealdb_datastore::clock::ManualClock>) -> Arc<Datastore> {
	Datastore::builder()
		.with_capabilities(Capabilities::all())
		.without_maintenance_tasks()
		.with_wall_clock(clock as Arc<dyn surrealdb_datastore::clock::WallClock>)
		.build_with_path("memory")
		.await
		.unwrap()
}

fn manual_clock() -> Arc<surrealdb_datastore::clock::ManualClock> {
	Arc::new(surrealdb_datastore::clock::ManualClock::new(chrono::Utc::now()))
}

/// Whether the reclaim task's lease key is stored.
async fn reclaim_lease_held(ds: &Datastore) -> bool {
	let tx = ds.transaction(Read).await.unwrap();
	let lease = tx
		.get_key(
			&crate::key::schema::TaskLeaseKey {
				task_id: crate::kvs::tasklease::TaskLeaseType::ReclaimTombstones,
			},
			None,
		)
		.await
		.unwrap();
	let _ = tx.cancel().await;
	lease.is_some()
}

/// A hosted pass deletes a removed database's data and retires its entry,
/// without taking the reclaim lease.
#[tokio::test]
async fn a_hosted_reclaim_pass_drains_a_removed_database_unleased() {
	let ds = hosted_ds(manual_clock()).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();
	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	seed_range(&ds, &range, 100).await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	assert_eq!(reclaim_queue_len(&ds).await, 1);

	let run = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, Duration::ZERO).await.unwrap();
	assert!(run.keys >= 100, "the pass must delete the seeded keys: {run:?}");
	assert!(!run.more && run.retry_after.is_none(), "nothing is left: {run:?}");
	assert_eq!(count_range(&ds, range).await, 0);
	assert_eq!(reclaim_queue_len(&ds).await, 0);
	assert!(!reclaim_lease_held(&ds).await, "a hosted pass takes no lease");
}

/// A prefix larger than the pass budget is finished over several passes, each
/// deleting at most its budget and resuming at the cursor the last committed.
#[tokio::test]
async fn a_hosted_reclaim_pass_finishes_a_large_prefix_over_several_passes() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = hosted_ds(manual_clock()).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();
	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	let baseline = count_range(&ds, range.clone()).await;
	seed_range(&ds, &range, page * 5 / 2 - baseline).await;
	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	let total = count_range(&ds, range.clone()).await;
	assert!(total > page * 2 && total <= page * 3, "the prefix must take three pages: {total}");

	let budget = page as u64;
	let mut passes = 0;
	let mut deleted = 0;
	loop {
		let run = ds.run_reclaim(budget, Duration::ZERO).await.unwrap();
		passes += 1;
		assert!(run.keys <= budget, "a pass must stay within its budget: {run:?}");
		deleted += run.keys;
		let left = count_range(&ds, range.clone()).await;
		assert_eq!(left as u64, total as u64 - deleted, "every deleted key is committed");
		if !run.more {
			break;
		}
		assert_eq!(reclaim_queue_len(&ds).await, 1, "a partly reclaimed entry stays queued");
		assert!(passes < 8, "the reclaim must finish");
	}
	assert_eq!(passes, 3, "two full passes and one that empties the prefix");
	assert_eq!(count_range(&ds, range).await, 0);
	assert_eq!(reclaim_queue_len(&ds).await, 0);
}

/// The grace is measured on the datastore's wall clock: an entry is stamped
/// when first seen, reported as due when its grace lapses, and reclaimed by
/// the first pass after that.
#[tokio::test]
async fn a_hosted_reclaim_pass_judges_the_grace_on_the_injected_clock() {
	let clock = manual_clock();
	let ds = hosted_ds(Arc::clone(&clock)).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();
	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	seed_range(&ds, &range, 10).await;
	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();

	let grace = Duration::from_secs(10);
	let first = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, grace).await.unwrap();
	assert_eq!(first.keys, 0, "an entry first seen now is inside its grace");
	assert!(!first.more);
	assert_eq!(first.retry_after, Some(grace), "the entry is due a whole grace from now");
	assert_eq!(
		reclaim_entry_observed_ms(&ds).await,
		surrealdb_datastore::clock::WallClock::now(&*clock).timestamp_millis() as u64,
		"the stamp is the injected clock's reading"
	);

	clock.advance(chrono::Duration::seconds(9));
	let early = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, grace).await.unwrap();
	assert_eq!(early.keys, 0, "the grace has not lapsed on the injected clock");
	assert_eq!(early.retry_after, Some(Duration::from_secs(1)));
	assert!(count_range(&ds, range.clone()).await >= 10);

	clock.advance(chrono::Duration::seconds(1));
	let due = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, grace).await.unwrap();
	assert!(due.keys >= 10, "the lapsed entry is reclaimed: {due:?}");
	assert!(!due.more && due.retry_after.is_none());
	assert_eq!(count_range(&ds, range).await, 0);
	assert_eq!(reclaim_queue_len(&ds).await, 0);
}

/// A hosted pass ages an untouched doc-ID entry from its sighting, held in
/// memory and read on the datastore's wall clock: the pass that sights it
/// writes nothing and reports it due a whole grace later, and the first pass
/// after the grace lapses reclaims it.
#[tokio::test]
async fn a_hosted_reclaim_pass_reports_a_sighted_doc_id_entry_due() {
	let clock = manual_clock();
	let ds = hosted_ds(Arc::clone(&clock)).await;
	let keys = RECLAIM_BATCH_SIZE as usize / 2;
	let (ns_id, db_id, tb, di) = seed_doc_id_space(&ds, keys).await;
	let rc = doc_id_entry(ReclaimKind::DocLookup, ns_id, db_id, &tb);
	queue_entries(&ds, std::slice::from_ref(&rc), &ReclaimState::enqueued()).await;

	let grace = Duration::from_secs(10);
	let first = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, grace).await.unwrap();
	assert_eq!(first.keys, 0, "an entry first sighted now is inside its grace");
	assert!(!first.more);
	assert_eq!(first.retry_after, Some(grace), "the entry is due a whole grace from now");
	let state = entry_state(&ds, &rc).await;
	assert_eq!((state.observed_ms, state.cursor), (0, None), "a sighting writes nothing");
	assert_eq!(ds.reclaim_sightings_len(), 1);

	clock.advance(chrono::Duration::seconds(9));
	let early = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, grace).await.unwrap();
	assert_eq!(early.keys, 0, "the grace has not lapsed on the injected clock");
	assert_eq!(early.retry_after, Some(Duration::from_secs(1)));
	assert_eq!(count_range(&ds, di.clone()).await, keys);

	clock.advance(chrono::Duration::seconds(1));
	let due = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, grace).await.unwrap();
	assert_eq!(due.keys, keys as u64, "the lapsed entry is reclaimed: {due:?}");
	assert!(!due.more && due.retry_after.is_none());
	assert_eq!(count_range(&ds, di).await, 0);
	assert_eq!(reclaim_queue_len(&ds).await, 0);
	assert_eq!(ds.reclaim_sightings_len(), 0, "a retired entry's sighting is forgotten");
}

/// A hosted pass pages every prefix through transactions, even where the
/// backend can destroy a range outside one.
#[tokio::test]
async fn a_hosted_reclaim_pass_never_destroys_out_of_transaction() {
	let destroyed = DestroyedRanges::default();
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.without_maintenance_tasks()
		.build_with_factory_path("memory", DestroyRangeComposer(destroyed.clone()))
		.await
		.unwrap();
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();
	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let range = DbRoot {
		ns: ns_id,
		db: db_id,
	}
	.range()
	.unwrap();
	seed_range(&ds, &range, 10).await;
	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();

	let run = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, Duration::ZERO).await.unwrap();
	assert!(run.keys >= 10);
	assert_eq!(count_range(&ds, range).await, 0);
	assert!(destroyed.ranges().is_empty(), "a hosted pass must not destroy out-of-transaction");
}

/// A hosted pass that finds nothing queued commits nothing, so it leaves no
/// work behind for the host to be woken by.
#[tokio::test]
async fn an_idle_hosted_reclaim_pass_commits_nothing() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	observer.clear();
	let run = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, Duration::from_secs(10)).await.unwrap();
	assert_eq!(run.keys, 0);
	assert!(!run.more && run.retry_after.is_none());
	assert!(observer.sizes().is_empty(), "an idle pass committed: {:?}", observer.sizes());
}

/// A removal's commit puts a key the reclaim predicate matches, within the
/// queue's bounds, and notifies the reclaim trigger.
#[tokio::test]
async fn a_removal_notifies_the_reclaim_trigger_and_queues_a_matching_key() {
	let ds = hosted_ds(manual_clock()).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test; DEFINE DATABASE tenant; DEFINE TABLE thing; \
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();
	let (start, end) = Datastore::reclaim_queue_bounds().unwrap();
	for removal in ["REMOVE INDEX idx ON thing;", "REMOVE DATABASE tenant;"] {
		ds.execute(removal, &ses, None).await.unwrap();
		tokio::time::timeout(Duration::from_secs(1), ds.commit_triggers().reclaim.notified())
			.await
			.unwrap_or_else(|_| panic!("`{removal}` did not notify the reclaim trigger"));
		let queued = keys_in_range(&ds, ReclaimPrefix {}.range().unwrap()).await;
		assert!(!queued.is_empty(), "`{removal}` queued nothing");
		for key in queued {
			assert!(Datastore::is_reclaim_key(&key));
			assert!(key >= start && key < end);
		}
		ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, Duration::ZERO).await.unwrap();
	}
	assert!(!Datastore::is_reclaim_key(&end));
	assert!(!Datastore::is_reclaim_key(b"/!tq"));
}

/// A hosted pass shares its budget among as many entries as it has pages of
/// budget, so the pages it commits are bounded by the budget however many
/// queued entries hold data.
#[tokio::test]
async fn a_hosted_reclaim_pass_commits_pages_bounded_by_its_budget() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	let entries = 20;
	{
		let tx = ds.transaction(Write).await.unwrap();
		tx.admit_stored_format().await.unwrap();
		tx.commit().await.unwrap();
	}
	for i in 0..entries {
		let db = DatabaseId(i as u32 + 1);
		let range = DbRoot {
			ns: NamespaceId(1),
			db,
		}
		.range()
		.unwrap();
		seed_range(&ds, &range, page).await;
		let tx = ds.transaction(Write).await.unwrap();
		tx.set_key(
			&ReclaimKey::database(NamespaceId(1), db, false, Uuid::now_v7()),
			&ReclaimState::enqueued(),
		)
		.await
		.unwrap();
		tx.commit().await.unwrap();
	}

	let pages = 2;
	let budget = (pages * page) as u64;
	observer.clear();
	let run = ds.run_reclaim(budget, Duration::ZERO).await.unwrap();
	assert_eq!(run.keys, budget, "the pass spends its whole budget: {run:?}");
	assert!(run.more);
	let sizes = observer.sizes();
	assert!(
		sizes.len() <= pages + 1,
		"{pages} pages and one retirement at most, not a page per entry: {sizes:?}"
	);
	assert_eq!(reclaim_queue_len(&ds).await, entries - pages, "each served entry was emptied");
}

/// A `DEFINE INDEX` waiting on a doc-ID space a hosted pass left torn finishes
/// the space itself and completes; the hosted pass and the statement's own
/// steps share the entries through their compare-and-set, so a later pass
/// finds nothing left.
#[tokio::test]
async fn defining_a_doc_id_index_finishes_a_space_a_hosted_pass_tore() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = hosted_ds(manual_clock()).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute(
		"DEFINE NAMESPACE test;
		 DEFINE DATABASE tenant;
		 DEFINE TABLE thing SCHEMALESS;
		 DEFINE INDEX idx ON thing FIELDS val;",
		&ses,
		None,
	)
	.await
	.unwrap();

	let (ns_id, db_id) = ids(&ds, "test", "tenant").await;
	let tb = TableName::from("thing");
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	seed_range(&ds, &di, page * 2).await;
	seed_range(&ds, &dd, page * 2).await;

	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();
	let run = ds.run_reclaim(page as u64, Duration::ZERO).await.unwrap();
	assert!(run.more, "one page of budget leaves the space torn: {run:?}");
	let left = count_range(&ds, di.clone()).await + count_range(&ds, dd.clone()).await;
	assert!(left > 0 && left < page * 4, "the pass must have torn the space: {left} left");

	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
	assert_eq!(count_range(&ds, di).await + count_range(&ds, dd).await, 0);
	assert_eq!(count_doc_kinds(&reclaim_queue_kinds(&ds).await), 0);
	let _ = index_id(&ds, ns_id, db_id, "thing", "idx").await;

	let after = ds.run_reclaim(RECLAIM_PASS_KEY_BUDGET, Duration::ZERO).await.unwrap();
	assert!(!after.more, "nothing is left for a pass: {after:?}");
}

/// A hosted pass spends its whole budget on one entry when it serves only
/// that one, and reports work left whenever an entry it served still holds
/// data, even with budget unspent.
#[tokio::test]
async fn a_hosted_reclaim_pass_reports_an_entry_it_left_holding_data() {
	let page = RECLAIM_BATCH_SIZE as usize;
	let ds = hosted_ds(manual_clock()).await;
	{
		let tx = ds.transaction(Write).await.unwrap();
		tx.admit_stored_format().await.unwrap();
		tx.commit().await.unwrap();
	}
	let queue = |db: u32, keys: usize| {
		let ds = Arc::clone(&ds);
		async move {
			let range = DbRoot {
				ns: NamespaceId(1),
				db: DatabaseId(db),
			}
			.range()
			.unwrap();
			seed_range(&ds, &range, keys).await;
			let tx = ds.transaction(Write).await.unwrap();
			let rc = ReclaimKey::database(NamespaceId(1), DatabaseId(db), false, Uuid::now_v7());
			tx.set_key(&rc, &ReclaimState::enqueued()).await.unwrap();
			tx.commit().await.unwrap();
			range
		}
	};

	// One entry: the pass spends its whole budget on it.
	let big = queue(1, page * 3).await;
	let run = ds.run_reclaim((page * 4) as u64, Duration::ZERO).await.unwrap();
	assert_eq!(run.keys, (page * 3) as u64, "{run:?}");
	assert!(!run.more, "{run:?}");
	assert_eq!(count_range(&ds, big).await, 0);

	// A large entry and a small one: the large one's share runs out with data
	// left while the small one leaves budget unspent.
	let large = queue(2, page * 5).await;
	let _small = queue(3, 10).await;
	let run = ds.run_reclaim((page * 4) as u64, Duration::ZERO).await.unwrap();
	assert!(run.keys < (page * 4) as u64, "budget is left unspent: {run:?}");
	assert!(run.more, "an entry left holding data is work left: {run:?}");
	while ds.run_reclaim((page * 4) as u64, Duration::ZERO).await.unwrap().more {}
	assert_eq!(count_range(&ds, large).await, 0);
	assert_eq!(reclaim_queue_len(&ds).await, 0);
}
