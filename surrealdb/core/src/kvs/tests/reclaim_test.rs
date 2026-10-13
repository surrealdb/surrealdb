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
	DbRoot, DocKeyPrefix, DocLookupPrefix, DocPendingPrefix, IdxRoot, NsRoot, ReclaimKey,
	ReclaimPrefix, ReclaimTbPrefix,
};
use crate::key::{AnyRange, KVKeyDecode, KVSubspace, Key, RawRange};
use crate::kvs::test_support::{WriteTxSizeObserver, cleanup_sizes};
use crate::kvs::tx::DocIdsReclaimClaim;
use crate::kvs::{
	Datastore, RECLAIM_BATCH_SIZE, TransactionBuilderFactory, TransactionBuilderParts,
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
		_ => seed_range(&ds, &target, to_seed).await,
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
		_ => 0,
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
	// alone.
	let queued = batch + batch / 2;
	{
		let tx = ds.transaction(Write).await.unwrap();
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

	// A started claim cannot be withdrawn or adopted, and the rejected DDL does
	// not turn the remainder into one unbounded transaction.
	let left = count_range(&ds, dd.clone()).await + count_range(&ds, di.clone()).await;
	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	let error = response.remove(0).result.unwrap_err().to_string();
	assert!(error.contains("still being reclaimed"));
	assert_eq!(
		count_range(&ds, dd.clone()).await + count_range(&ds, di.clone()).await,
		left,
		"the rejected DDL must not synchronously purge the remainder"
	);

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(count_range(&ds, dd).await + count_range(&ds, di).await, 0);
	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
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
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let dd = DocKeyPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let mapped = keys_in_range(&ds, di.clone()).await;
	assert!(!mapped.is_empty(), "the index must have mapped the records it covers");

	ds.execute("REMOVE INDEX idx ON thing;", &ses, None).await.unwrap();
	assert_eq!(
		keys_in_range(&ds, di.clone()).await,
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
		keys_in_range(&ds, di).await,
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
/// defining a consumer waits for the bounded reclaim rather than adopting it or
/// purging the remainder in the DDL transaction.
///
/// The prefixes are reclaimed independently and page by page, so a
/// surviving `!di` entry may have lost the `!dd` twin that resolves its id.
/// Adopting one would give the new index entries pointing at a doc-ID that
/// resolves to no record.
#[tokio::test]
async fn defining_a_doc_id_index_waits_for_a_partly_reclaimed_doc_id_space() {
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

	let queue_before = reclaim_queue_len(&ds).await;
	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	let error = response.remove(0).result.unwrap_err().to_string();
	assert!(
		error.contains("still being reclaimed"),
		"the DDL must ask the caller to retry while bounded cleanup continues: {error}"
	);

	assert_eq!(
		count_range(&ds, di.clone()).await + count_range(&ds, dd.clone()).await,
		left,
		"the rejected DDL must not synchronously purge the torn space"
	);
	assert_eq!(
		reclaim_queue_len(&ds).await,
		queue_before,
		"the rejected DDL must leave the bounded reclaim resumable"
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
		count_range(&ds, di).await + count_range(&ds, dd).await,
		0,
		"the background reaper must finish the torn space"
	);
	assert_eq!(count_doc_kinds(&reclaim_queue_kinds(&ds).await), 0);

	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
	let _ = index_id(&ds, ns_id, db_id, "thing", "idx").await;
}

/// A sibling prefix the reaper drained and retired leaves the space torn, so
/// defining a consumer waits for the surviving bounded reclaim entries.
///
/// Retiring an entry destroys the cursor that recorded what it had deleted, so a
/// torn space can be left with no survivor carrying a cursor. What still says the
/// space is torn is the count: the entries are enqueued together, so a
/// strict subset of them surviving means the rest were drained and retired.
#[tokio::test]
async fn defining_a_doc_id_index_waits_when_a_sibling_entry_has_retired() {
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

	let queue_before = reclaim_queue_len(&ds).await;
	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	let error = response.remove(0).result.unwrap_err().to_string();
	assert!(
		error.contains("still being reclaimed"),
		"the missing sibling must make the DDL wait for cleanup: {error}"
	);

	assert_eq!(
		count_range(&ds, di.clone()).await,
		page,
		"the rejected DDL must not synchronously purge surviving mappings"
	);
	assert_eq!(reclaim_queue_len(&ds).await, queue_before);

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(count_range(&ds, di).await, 0);
	let kinds = reclaim_queue_kinds(&ds).await;
	assert_eq!(count_doc_kinds(&kinds), 0, "the bounded reclaim must finish");
	assert_eq!(
		count_kind(&kinds, ReclaimKind::Index),
		kinds.len(),
		"only unrelated index-prefix work may remain"
	);

	let mut response =
		ds.execute("DEFINE INDEX idx ON thing FIELDS val;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
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
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let mapped = keys_in_range(&ds, di.clone()).await;
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
		keys_in_range(&ds, di).await,
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
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let mapped = keys_in_range(&ds, di.clone()).await;
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
		keys_in_range(&ds, di).await,
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
	let di = DocLookupPrefix::new(ns_id, db_id, Cow::Borrowed(&tb)).raw_range().unwrap();
	let mapped = keys_in_range(&ds, di.clone()).await;
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
		!keys_in_range(&ds, di).await.is_empty(),
		"the reclaim must not delete mappings the rebuilt index resolves through"
	);
}

/// A legacy-index rebuild cannot adopt a shared doc-ID space after its bounded
/// reclaim has started. It leaves both the legacy format and queue intact, then
/// succeeds after the background task finishes the cleanup.
#[tokio::test]
async fn rebuilding_a_legacy_index_waits_for_a_partly_reclaimed_doc_id_space() {
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

	let mut response = ds.execute("REBUILD INDEX legacy ON thing;", &ses, None).await.unwrap();
	let error = response.remove(0).result.unwrap_err().to_string();
	assert!(error.contains("still being reclaimed"));
	assert_eq!(
		count_range(&ds, di.clone()).await + count_range(&ds, dd.clone()).await,
		left,
		"the rejected rebuild must not synchronously purge the remainder"
	);
	let txn = ds.transaction(Read).await.unwrap();
	let legacy = txn.get_tb_index(ns_id, db_id, &tb, "legacy", None).await.unwrap().unwrap();
	assert!(!legacy.uses_shared_doc_ids(), "the rejected rebuild must not stamp the new format");
	let _ = txn.cancel().await;

	Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::ZERO,
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(count_range(&ds, di).await + count_range(&ds, dd).await, 0);

	let mut response = ds.execute("REBUILD INDEX legacy ON thing;", &ses, None).await.unwrap();
	response.remove(0).result.unwrap();
	let txn = ds.transaction(Read).await.unwrap();
	let legacy = txn.get_tb_index(ns_id, db_id, &tb, "legacy", None).await.unwrap().unwrap();
	assert!(legacy.uses_shared_doc_ids(), "the retry must stamp the current format");
	let _ = txn.cancel().await;
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
