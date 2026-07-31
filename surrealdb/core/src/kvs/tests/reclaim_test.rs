//! Tests for the deferred `REMOVE DATABASE/NAMESPACE/INDEX` data reclaim.
//!
//! `REMOVE` only deletes the catalog definition inside the user transaction and
//! enqueues a reclaim job (`/!rc`); the data prefix is destroyed later by the
//! background reclaim task [`Datastore::reclaim_tombstones`]. These tests verify both
//! halves: the object becomes immediately invisible, and the reclaim task physically
//! reclaims the data and clears the queue.

use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};
use tokio_util::sync::CancellationToken;
use web_time::{Duration, SystemTime};

use crate::catalog::providers::DatabaseProvider;
use crate::dbs::{Capabilities, Session};
use crate::key::reclaim::ReclaimState;
use crate::key::schema::{DbRoot, ReclaimKey, ReclaimPrefix};
use crate::key::{AnyRange, KVKeyDecode};
use crate::kvs::Datastore;

async fn mem_ds() -> Arc<Datastore> {
	Arc::new(
		Datastore::builder()
			.with_capabilities(Capabilities::all())
			.build_with_path("memory")
			.await
			.unwrap(),
	)
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

/// The `observed_ms` stamp of the single pending reclaim entry (asserts there is
/// exactly one). `0` means the reclaim task has not yet observed it.
async fn reclaim_entry_observed_ms(ds: &Datastore) -> u64 {
	let range = ReclaimPrefix {}.range().unwrap();
	let tx = ds.transaction(Read).await.unwrap();
	let items = tx.getr(range, None).await.unwrap();
	let _ = tx.cancel().await;
	assert_eq!(items.len(), 1, "expected exactly one reclaim queue entry");
	items[0].1.observed_ms
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
