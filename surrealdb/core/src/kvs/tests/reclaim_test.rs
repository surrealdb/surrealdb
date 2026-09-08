//! Tests for the deferred `REMOVE DATABASE/NAMESPACE/INDEX` data reclaim.
//!
//! `REMOVE` only deletes the catalog definition inside the user transaction and
//! enqueues a reclaim job (`/!rc`); the data prefix is destroyed later by the
//! background reclaim task [`Datastore::reclaim_tombstones`]. These tests verify both
//! halves: the object becomes immediately invisible, and the reclaim task physically
//! reclaims the data and clears the queue.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use web_time::{Duration, SystemTime};

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{DatabaseId, NamespaceId};
use crate::dbs::{Capabilities, Session};
use crate::key::root::rc::{ReclaimKey, ReclaimKind, ReclaimState};
use crate::kvs::LockType::Optimistic;
use crate::kvs::TransactionType::{Read, Write};
use crate::kvs::{Datastore, KVValue};

async fn mem_ds() -> Arc<Datastore> {
	Arc::new(
		Datastore::builder()
			.with_capabilities(Capabilities::all())
			.build_with_path("memory")
			.await
			.unwrap(),
	)
}

/// Count the keys currently stored under a half-open byte range.
async fn count_range(ds: &Datastore, beg: Vec<u8>, end: Vec<u8>) -> usize {
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let res = tx.getr(beg..end, None).await.unwrap();
	let _ = tx.cancel().await;
	res.len()
}

/// Number of pending entries in the background reclaim queue.
async fn reclaim_queue_len(ds: &Datastore) -> usize {
	let (beg, end) = ReclaimKey::range();
	count_range(ds, beg, end).await
}

/// The `observed_ms` stamp of the single pending reclaim entry (asserts there is
/// exactly one). `0` means the reclaim task has not yet observed it.
async fn reclaim_entry_observed_ms(ds: &Datastore) -> u64 {
	let (beg, end) = ReclaimKey::range();
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let items = tx.getr(beg..end, None).await.unwrap();
	let _ = tx.cancel().await;
	assert_eq!(items.len(), 1, "expected exactly one reclaim queue entry");
	ReclaimState::kv_decode_value(&items[0].1, ()).unwrap().observed_ms
}

/// Every key currently stored under a half-open byte range, for asserting a
/// prefix is empty and naming what survived when it is not.
async fn keys_in_range(ds: &Datastore, beg: Vec<u8>, end: Vec<u8>) -> Vec<Vec<u8>> {
	let tx = ds.transaction(Read, Optimistic).await.unwrap();
	let res = tx.keysr(beg..end, 1000, 0, None).await.unwrap();
	let _ = tx.cancel().await;
	res
}

/// Drive reclaim passes until one reads no queue entry, the only state that
/// leaves nothing behind. Bounded so an entry this version cannot act on ends
/// the loop instead of hanging it.
///
/// `budget` is the per-pass key allowance. A budget below the window's key
/// count is what forces the delete to page and resume, so a caller asserting a
/// prefix is emptied should pass one well under it.
async fn drain_reclaim(ds: &Arc<Datastore>, budget: u64) {
	for _ in 0..256 {
		let (iterations, errors) = Datastore::reclaim_tombstones_with_budget(
			Arc::clone(ds),
			Duration::from_secs(1),
			Duration::ZERO,
			budget,
			CancellationToken::new(),
		)
		.await
		.unwrap();
		assert_eq!(errors, 0, "a reclaim pass must not error");
		if iterations == 0 {
			break;
		}
	}
}

fn now_ms() -> u64 {
	SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u64
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
		let tx = ds.transaction(Read, Optimistic).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		(db.namespace_id, db.database_id)
	};
	let db_prefix = crate::key::database::all::new(ns_id, db_id);
	let range = crate::kvs::util::to_prefix_range(&db_prefix).unwrap();
	assert!(
		count_range(&ds, range.start.clone(), range.end.clone()).await > 0,
		"the database prefix should hold data before reclaim"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "no reclaim jobs before removal");

	// Remove the database. This must NOT delete the data prefix inline.
	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();

	// The database is immediately invisible in the catalog...
	{
		let tx = ds.transaction(Read, Optimistic).await.unwrap();
		let gone = tx.get_db_by_name("test", "tenant", None).await.unwrap();
		let _ = tx.cancel().await;
		assert!(gone.is_none(), "database must be invisible immediately after REMOVE");
	}
	// ...a reclaim job is queued...
	assert_eq!(reclaim_queue_len(&ds).await, 1, "REMOVE DATABASE must enqueue one reclaim job");
	// ...and the data is still physically present (reclaim is deferred).
	assert!(
		count_range(&ds, range.start.clone(), range.end.clone()).await > 0,
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
		count_range(&ds, range.start.clone(), range.end.clone()).await,
		0,
		"the reclaim task must destroy the database data prefix"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the reclaim task must drain the reclaim queue");

	// Recreating the same name yields a fresh, empty database with a new id.
	ds.execute("DEFINE DATABASE tenant;", &ses, None).await.unwrap();
	let new_db_id = {
		let tx = ds.transaction(Read, Optimistic).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		db.database_id
	};
	assert_ne!(new_db_id, db_id, "recreated database must get a fresh, never-reused id");

	// The recreated database starts physically empty — none of the removed
	// database's data is reachable under the new (disjoint) prefix.
	let new_prefix = crate::key::database::all::new(ns_id, new_db_id);
	let new_range = crate::kvs::util::to_prefix_range(&new_prefix).unwrap();
	assert_eq!(
		count_range(&ds, new_range.start, new_range.end).await,
		0,
		"recreated database must start empty with no data leaked from the removed one"
	);
}

#[tokio::test]
async fn remove_namespace_reclaims_every_key_under_the_namespace() {
	let ds = mem_ds().await;
	let ses = Session::owner().with_ns("test").with_db("tenant");

	// A database inside the namespace, so the namespace holds more than the
	// database subtree: allocating the database id also writes the namespace's
	// database-id sequence keys, which sort *below* the database prefix (`!` <
	// `*`). Enough records that the window cannot be emptied in one page, so the
	// delete has to resume from its cursor — a resume that skips the low end of
	// the window strands exactly those sequence keys.
	ds.execute("DEFINE NAMESPACE test; DEFINE DATABASE tenant;", &ses, None).await.unwrap();
	for i in 0..40 {
		ds.execute(&format!("CREATE thing:{i} SET v = {i};"), &ses, None).await.unwrap();
	}

	let ns_id = {
		let tx = ds.transaction(Read, Optimistic).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		db.namespace_id
	};
	let range = crate::kvs::util::to_prefix_range(&crate::key::namespace::all::new(ns_id)).unwrap();
	assert!(
		count_range(&ds, range.start.clone(), range.end.clone()).await > 0,
		"the namespace prefix should hold keys before reclaim"
	);

	// Remove the database and then its namespace, the order a caller tearing a
	// tenant down uses. Both removals defer, so the queue holds two entries whose
	// prefixes nest: the namespace's window contains the database's entirely.
	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();
	ds.execute("REMOVE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	// Well under the ~45 keys the namespace holds, so the delete pages several times.
	drain_reclaim(&ds, 4).await;

	// Every key beneath the namespace, not merely the database subtree: a
	// namespace removal names the whole prefix, so anything left under it is
	// unreachable data no later statement can address.
	let leftover = keys_in_range(&ds, range.start.clone(), range.end.clone()).await;
	assert!(
		leftover.is_empty(),
		"the reclaim task must destroy every key under the namespace prefix, left {} behind: {:?}",
		leftover.len(),
		leftover.iter().map(crate::key::debug::Sprintable::sprint).collect::<Vec<_>>(),
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the reclaim task must drain the reclaim queue");
}

#[tokio::test]
async fn a_doc_id_entry_is_left_queued_and_takes_no_candidate_slot() {
	let ds = mem_ds().await;

	// A kind only a node with a newer layout could have written: 3.2 has no
	// shared per-table doc-ID space, so this version cannot know which keys the
	// entry names.
	let rc = ReclaimKey::of_kind(
		ReclaimKind::DocKey,
		NamespaceId(9),
		DatabaseId(9),
		uuid::Uuid::from_u128(1),
	);
	{
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		tx.set(&rc, &ReclaimState::enqueued()).await.unwrap();
		tx.commit().await.unwrap();
	}

	// A pass with a real grace, which stamps every entry it takes as a
	// candidate. Nothing here should be taken.
	let (_, errors) = Datastore::reclaim_tombstones(
		Arc::clone(&ds),
		Duration::from_secs(1),
		Duration::from_secs(3600),
		CancellationToken::new(),
	)
	.await
	.unwrap();
	assert_eq!(errors, 0, "an entry this version cannot act on is not an error");

	assert_eq!(
		reclaim_queue_len(&ds).await,
		1,
		"the entry must stay queued for a node that understands its layout"
	);
	// Unstamped is the observable form of "took no candidate slot": an entry
	// that reached selection would have had its first sighting recorded. Slots
	// are bounded, so an entry that can only ever be refused must not hold one
	// — it is older than anything actionable and would crowd it out on every
	// pass.
	assert_eq!(
		reclaim_entry_observed_ms(&ds).await,
		0,
		"it must not be observed, so it cannot occupy a bounded candidate slot"
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

/// The reclaim task must NOT physically destroy a freshly-removed object's data while
/// it is still inside the snapshot-safety grace window — otherwise a read
/// transaction whose snapshot predates the `REMOVE` could have its data ripped
/// out from under it (TiKV's `unsafe_destroy_range` bypasses MVCC). Once the
/// removal ages past the grace, reclaim proceeds.
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
		let tx = ds.transaction(Read, Optimistic).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		(db.namespace_id, db.database_id)
	};
	let db_prefix = crate::key::database::all::new(ns_id, db_id);
	let range = crate::kvs::util::to_prefix_range(&db_prefix).unwrap();

	// An in-flight reader: a transaction opened *before* the removal. It must
	// still be able to read the data afterwards, because the reclaim task must not have
	// destroyed it yet.
	let reader = ds.transaction(Read, Optimistic).await.unwrap();
	let reader_seen_before =
		reader.getr(range.start.clone()..range.end.clone(), None).await.unwrap().len();
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
		count_range(&ds, range.start.clone(), range.end.clone()).await > 0,
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
		reader.getr(range.start.clone()..range.end.clone(), None).await.unwrap().len(),
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
		count_range(&ds, range.start.clone(), range.end.clone()).await,
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
		let tx = ds.transaction(Read, Optimistic).await.unwrap();
		let db = tx.get_db_by_name("test", "tenant", None).await.unwrap().unwrap();
		let _ = tx.cancel().await;
		(db.namespace_id, db.database_id)
	};
	let range =
		crate::kvs::util::to_prefix_range(&crate::key::database::all::new(ns_id, db_id)).unwrap();

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap();

	// Backdate the queued entry's observation to one hour ago, simulating an
	// entry the reclaim task observed long before the grace elapsed.
	{
		let (beg, end) = ReclaimKey::range();
		let tx = ds.transaction(Write, Optimistic).await.unwrap();
		let items = tx.getr(beg..end, None).await.unwrap();
		assert_eq!(items.len(), 1);
		let rc = ReclaimKey::decode_key(&items[0].0).unwrap();
		tx.set(
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
		count_range(&ds, range.start.clone(), range.end.clone()).await,
		0,
		"an entry observed longer than the grace ago must be reclaimed"
	);
	assert_eq!(reclaim_queue_len(&ds).await, 0, "the reclaim job must be drained");
}
