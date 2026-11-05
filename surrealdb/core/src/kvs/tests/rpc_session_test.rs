//! Tests for durable RPC session storage (`/!se`).
//!
//! Client-attached RPC sessions can be mirrored to the KV store so they
//! survive the process that attached them ([`Datastore::create_rpc_session`]
//! and friends). These tests verify the storage lifecycle: create/load round
//! trips, lazy expiry on load, the conditional replace and refresh writes,
//! deletion, the liveness check, and the background purge task
//! [`Datastore::purge_expired_rpc_sessions`].

use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};
use uuid::Uuid;
use web_time::Duration;

use crate::dbs::{Capabilities, DurableSession, Session, durable_session};
use crate::key::schema::{SessionKey, SessionPrefix};
use crate::key::{AnyRange, KVKey};
use crate::kvs::tasks::Progress;
use crate::kvs::{Datastore, NORMAL_BATCH_SIZE, RPC_SESSION_GC_SLICE_BATCHES};

async fn mem_ds() -> Arc<Datastore> {
	Datastore::builder()
		.with_capabilities(Capabilities::all())
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

/// Number of durable session entries currently stored.
async fn durable_session_count(ds: &Datastore) -> usize {
	let range = SessionPrefix {}.range().unwrap();
	count_range(ds, range).await
}

/// The raw stored entry for a session id, if any.
async fn durable_session_entry(ds: &Datastore, id: Uuid) -> Option<DurableSession> {
	let tx = ds.transaction(Read).await.unwrap();
	let res = tx
		.get_key(
			&SessionKey {
				id,
			},
			None,
		)
		.await
		.unwrap();
	let _ = tx.cancel().await;
	res
}

/// Store an entry with an explicit expiry, bypassing the TTL computation.
async fn put_entry_with_expiry(ds: &Datastore, id: Uuid, session: &Session, expires_at: u64) {
	let value = durable_session(session, expires_at);
	let tx = ds.transaction(Write).await.unwrap();
	tx.set_key(
		&SessionKey {
			id,
		},
		&value,
	)
	.await
	.unwrap();
	tx.commit().await.unwrap();
}

fn sample_session(id: Uuid) -> Session {
	let mut session = Session::owner().with_ns("app").with_db("app").with_rt(true);
	session.id = Some(id);
	session
}

#[tokio::test]
async fn persist_and_load_round_trip() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	let session = sample_session(id);

	ds.persist_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap();
	assert_eq!(durable_session_count(&ds).await, 1);

	let (loaded, _stored) = ds.load_rpc_session(id).await.unwrap().expect("session should load");
	assert_eq!(loaded, session);

	// A different id misses without touching the stored entry.
	assert!(ds.load_rpc_session(Uuid::new_v4()).await.unwrap().is_none());
	assert_eq!(durable_session_count(&ds).await, 1);
}

#[tokio::test]
async fn expired_entry_is_deleted_on_load() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	let session = sample_session(id);

	// An entry whose expiry has already passed is a miss, and the miss
	// removes the entry (lazy expiry).
	put_entry_with_expiry(&ds, id, &session, 1).await;
	assert!(ds.load_rpc_session(id).await.unwrap().is_none());
	assert_eq!(durable_session_count(&ds).await, 0);
}

#[tokio::test]
async fn load_does_not_modify_the_expiry() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	let session = sample_session(id);

	// A live entry is a pure read: loading must not rewrite the stored
	// expiry. Sliding the TTL is the job of `refresh_rpc_session`, which
	// callers invoke only after authorizing a request — never on the load
	// path, which also runs for unauthenticated lookups.
	ds.persist_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap();
	let before = durable_session_entry(&ds, id).await.unwrap().expires_at;

	let (loaded, stored) = ds.load_rpc_session(id).await.unwrap().expect("session should load");
	assert_eq!(loaded, session);
	assert_eq!(stored.expires_at(), before, "load should report the stored expiry it read");
	assert_eq!(
		durable_session_entry(&ds, id).await.unwrap().expires_at,
		before,
		"load must not touch the stored expiry"
	);
}

#[tokio::test]
async fn delete_removes_the_entry_and_is_idempotent() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	let session = sample_session(id);

	ds.persist_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap();
	ds.delete_rpc_session(id).await.unwrap();
	assert_eq!(durable_session_count(&ds).await, 0);
	assert!(ds.load_rpc_session(id).await.unwrap().is_none());

	// Deleting an absent entry is not an error.
	ds.delete_rpc_session(id).await.unwrap();
}

#[tokio::test]
async fn create_is_conditional_on_absence() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	let session = sample_session(id);

	// The first conditional create succeeds; a second for the same id is
	// refused rather than overwriting (the invariant that keeps two concurrent
	// attaches from both creating the session on a last-writer-wins backend).
	let created = ds.create_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap();
	assert!(created.is_some());
	assert!(ds.create_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap().is_none());
	assert_eq!(durable_session_count(&ds).await, 1);
	// The create reports exactly what it stored.
	let (_, stored) = ds.load_rpc_session(id).await.unwrap().unwrap();
	assert_eq!(stored.expires_at(), created.unwrap().expires_at());
}

#[tokio::test]
async fn create_replaces_an_expired_entry() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	// An expired entry the purge has not yet removed does not hold the id.
	put_entry_with_expiry(&ds, id, &sample_session(id), 1).await;
	let fresh = Session::default();
	let created = ds.create_rpc_session(id, &fresh, Duration::from_secs(60)).await.unwrap();
	assert!(created.is_some(), "an expired session must not block a new one");
	let (loaded, _) = ds.load_rpc_session(id).await.unwrap().unwrap();
	assert!(!loaded.au.is_root(), "the expired session's principal is gone");
	assert_eq!(durable_session_count(&ds).await, 1);
}

#[tokio::test]
async fn replace_writes_only_over_the_expected_copy() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	let session = sample_session(id);
	let ttl = Duration::from_secs(60);
	let first = ds.create_rpc_session(id, &session, ttl).await.unwrap().unwrap();

	// A replace naming the stored copy writes the new value.
	let changed = session.clone().with_db("other");
	let second = ds.replace_rpc_session(id, &first, &changed, ttl).await.unwrap();
	let second = second.expect("a replace naming the stored copy must write");
	let (loaded, _) = ds.load_rpc_session(id).await.unwrap().unwrap();
	assert_eq!(loaded.db.as_deref(), Some("other"));

	// A replace naming a copy that has since been overwritten writes nothing:
	// the change made in between survives.
	let stale = session.clone().with_db("stale");
	assert!(ds.replace_rpc_session(id, &first, &stale, ttl).await.unwrap().is_none());
	let (loaded, _) = ds.load_rpc_session(id).await.unwrap().unwrap();
	assert_eq!(loaded.db.as_deref(), Some("other"), "a stale replace must not clobber");

	// Once deleted, a replace never recreates the session.
	ds.delete_rpc_session(id).await.unwrap();
	assert!(ds.replace_rpc_session(id, &second, &changed, ttl).await.unwrap().is_none());
	assert_eq!(durable_session_count(&ds).await, 0, "replace must not resurrect a deleted session");
}

#[tokio::test]
async fn refresh_extends_the_stored_contents_only() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	let session = sample_session(id);
	put_entry_with_expiry(&ds, id, &session, u64::MAX / 2).await;
	let (_, stored) = ds.load_rpc_session(id).await.unwrap().unwrap();

	// A refresh rewrites the expiry and keeps the stored contents.
	let refreshed = ds.refresh_rpc_session(id, &stored, Duration::from_secs(60)).await.unwrap();
	let refreshed = refreshed.expect("a refresh naming the stored copy must write");
	assert!(refreshed.expires_at() < stored.expires_at(), "the expiry is now + ttl");
	let (loaded, reloaded) = ds.load_rpc_session(id).await.unwrap().unwrap();
	assert_eq!(loaded, session);
	assert_eq!(reloaded.expires_at(), refreshed.expires_at());

	// A refresh naming a superseded copy writes nothing.
	assert!(ds.refresh_rpc_session(id, &stored, Duration::from_secs(60)).await.unwrap().is_none());
}

#[tokio::test]
async fn a_read_only_store_still_serves_sessions() {
	let ds = mem_ds().await;
	let expired = Uuid::new_v4();
	put_entry_with_expiry(&ds, expired, &sample_session(expired), 1).await;
	let abandoned = Uuid::new_v4();
	put_entry_with_expiry(&ds, abandoned, &sample_session(abandoned), 1).await;
	ds.open_read_only("test".to_owned());
	assert!(ds.transaction(Write).await.is_err(), "the store refuses other writes");

	// A session is the connection's state, so it is created, changed,
	// refreshed, expired and removed as on a writable store.
	let id = Uuid::new_v4();
	let session = sample_session(id);
	let ttl = Duration::from_secs(60);
	let created = ds.create_rpc_session(id, &session, ttl).await.unwrap().unwrap();
	let changed = session.clone().with_db("other");
	let replaced = ds.replace_rpc_session(id, &created, &changed, ttl).await.unwrap().unwrap();
	ds.refresh_rpc_session(id, &replaced, ttl).await.unwrap().unwrap();
	let (loaded, _) = ds.load_rpc_session(id).await.unwrap().unwrap();
	assert_eq!(loaded.db.as_deref(), Some("other"));
	assert!(ds.load_rpc_session(expired).await.unwrap().is_none());
	ds.delete_rpc_session(id).await.unwrap();

	// An expired session no request loads again is purged.
	assert_eq!(durable_session_count(&ds).await, 1);
	ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();
	assert_eq!(durable_session_count(&ds).await, 0);
}

#[tokio::test]
async fn live_rpc_sessions_reports_stored_unexpired_entries() {
	let ds = mem_ds().await;
	let live = Uuid::new_v4();
	let expired = Uuid::new_v4();
	let missing = Uuid::new_v4();
	ds.persist_rpc_session(live, &sample_session(live), Duration::from_secs(60)).await.unwrap();
	put_entry_with_expiry(&ds, expired, &sample_session(expired), 1).await;

	let found = ds.live_rpc_sessions(&[live, expired, missing]).await.unwrap();
	assert_eq!(found, vec![true, false, false]);
	// A pure read: the expired entry is left for lazy expiry and the purge.
	assert_eq!(durable_session_count(&ds).await, 2);

	// An entry this node cannot decode, as a newer node may write, is
	// reported live without failing the others.
	let unreadable = Uuid::new_v4();
	let tx = ds.transaction(Write).await.unwrap();
	let key = SessionKey {
		id: unreadable,
	}
	.encode_key()
	.unwrap();
	tx.set(key, vec![0xff, 0xff, 0xff, 0xff]).await.unwrap();
	tx.commit().await.unwrap();
	let found = ds.live_rpc_sessions(&[unreadable, expired, live]).await.unwrap();
	assert_eq!(found, vec![true, false, true]);
}

#[tokio::test]
async fn purge_deletes_only_expired_entries() {
	let ds = mem_ds().await;
	let expired_a = Uuid::new_v4();
	let expired_b = Uuid::new_v4();
	let live = Uuid::new_v4();

	put_entry_with_expiry(&ds, expired_a, &sample_session(expired_a), 1).await;
	put_entry_with_expiry(&ds, expired_b, &sample_session(expired_b), 2).await;
	ds.persist_rpc_session(live, &sample_session(live), Duration::from_secs(3600)).await.unwrap();
	assert_eq!(durable_session_count(&ds).await, 3);

	ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();

	assert_eq!(durable_session_count(&ds).await, 1);
	assert!(durable_session_entry(&ds, live).await.is_some());
	assert!(durable_session_entry(&ds, expired_a).await.is_none());
	assert!(durable_session_entry(&ds, expired_b).await.is_none());
}

/// A purge scans a slice of pages per pass, and a pass that stops with pages
/// still to scan keeps its position and reports more: the next pass continues
/// after the page it stopped at rather than from the head, so an entry behind
/// the position waits for the scan after this one, and the scan ends at the
/// last page however many passes it spans.
#[tokio::test]
async fn purge_is_paged_across_passes() {
	let ds = mem_ds().await;
	let slice = RPC_SESSION_GC_SLICE_BATCHES as u64 * NORMAL_BATCH_SIZE as u64;
	// A slice's worth of expired sessions and one more, in key order, then a
	// live one sorting after them all.
	for n in 0..=slice {
		let id = Uuid::from_u128(1_000_000 + u128::from(n));
		put_entry_with_expiry(&ds, id, &sample_session(id), 1).await;
	}
	let live = Uuid::from_u128(u128::MAX);
	ds.persist_rpc_session(live, &sample_session(live), Duration::from_secs(3600)).await.unwrap();
	assert_eq!(durable_session_count(&ds).await as u64, slice + 2);

	let first = ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();
	assert_eq!(first.progress, Progress::More, "the pass stopped at its slice");
	assert_eq!(first.items, slice, "having purged the slice's worth");
	assert_eq!(durable_session_count(&ds).await as u64, 2);

	// An expired session sorting before the position: behind the scan, so
	// the pass that continues it leaves the entry for the next scan.
	let behind = Uuid::from_u128(5);
	put_entry_with_expiry(&ds, behind, &sample_session(behind), 1).await;

	let second = ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();
	assert_eq!(second.progress, Progress::Done, "the scan reached its last page");
	assert_eq!(second.items, 1, "the one expired session past the position");
	assert!(durable_session_entry(&ds, behind).await.is_some(), "the one behind it waits");
	assert!(durable_session_entry(&ds, live).await.is_some());

	let third = ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();
	assert_eq!(third.progress, Progress::Done);
	assert_eq!(third.items, 1, "the next scan starts from the head");
	assert_eq!(durable_session_count(&ds).await, 1);
	assert!(durable_session_entry(&ds, live).await.is_some());
}

/// A scan ends at the last session there was when it began: sessions created
/// past that key while the scan runs are the next scan's, so a scan ends
/// however fast sessions are created, and the sessions behind its position —
/// scanned while live, expired since — are reached by the next scan rather
/// than left behind a suffix that keeps growing.
#[tokio::test]
async fn purge_scan_ends_at_the_sessions_there_were_when_it_began() {
	let ds = mem_ds().await;
	let slice = RPC_SESSION_GC_SLICE_BATCHES as u64 * NORMAL_BATCH_SIZE as u64;
	let page = u64::from(NORMAL_BATCH_SIZE);
	for n in 0..=slice {
		let id = Uuid::from_u128(1_000_000 + u128::from(n));
		put_entry_with_expiry(&ds, id, &sample_session(id), 1).await;
	}
	let first = ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();
	assert_eq!(first.progress, Progress::More, "the scan stopped with one session past its slice");
	assert_eq!(first.items, slice);

	// More than a slice's worth of expired sessions, created past the end
	// of the scan while it is stopped.
	let created = slice + page;
	for n in 0..created {
		let id = Uuid::from_u128(2_000_000 + u128::from(n));
		put_entry_with_expiry(&ds, id, &sample_session(id), 1).await;
	}

	let second = ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();
	assert_eq!(second.progress, Progress::Done, "the scan ends at the page that reaches its end");
	assert_eq!(second.items, page, "having purged that page, and no more");
	assert_eq!(durable_session_count(&ds).await as u64, created + 1 - page);

	// The next scan begins at the head, over what was created.
	let third = ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();
	assert_eq!(third.progress, Progress::More);
	assert_eq!(third.items, slice);
	let fourth = ds.purge_expired_rpc_sessions(&Duration::from_secs(60)).await.unwrap();
	assert_eq!(fourth.progress, Progress::Done);
	assert_eq!(durable_session_count(&ds).await, 0);
}

/// A purge that finds its lease passed to another node stops there, purging
/// nothing further, and reports the lease elsewhere, so the former holder
/// does not purge alongside the new one; its scan keeps its place for this
/// node's next pass, which continues after it once the lease is back.
#[tokio::test]
async fn a_purge_that_finds_its_lease_taken_stops_and_keeps_its_place() {
	use surrealdb_datastore::clock::{ManualClock, WallClock};
	use surrealdb_observe::TaskLeaseState;

	use crate::kvs::tasklease::TaskLeaseType;

	// Without the datastore's own purge task, which would hold the lease.
	let ds = Datastore::builder()
		.with_capabilities(Capabilities::all())
		.without_maintenance_tasks()
		.build_with_path("memory")
		.await
		.unwrap();
	let slice = RPC_SESSION_GC_SLICE_BATCHES as u64 * NORMAL_BATCH_SIZE as u64;
	for n in 0..=slice {
		let id = Uuid::from_u128(1_000_000 + u128::from(n));
		put_entry_with_expiry(&ds, id, &sample_session(id), 1).await;
	}
	// Every handler reads one clock, which a lease lapses on when advanced
	// past its duration.
	let clock = Arc::new(ManualClock::new(chrono::Utc::now()));
	let lease = |ds: &Datastore| {
		ds.lease_handler(TaskLeaseType::RpcSessionCleanup, Duration::from_secs(60), None)
			.unwrap()
			.with_clock(Arc::clone(&clock) as Arc<dyn WallClock>)
	};
	let lapse = || clock.advance(chrono::Duration::seconds(61));

	let first = ds.purge_rpc_session_slice(&lease(&ds)).await.unwrap();
	assert_eq!(first.progress, Progress::More, "the scan stopped at its slice");
	assert_eq!(first.items, slice);
	// An expired session sorting before the scan's position.
	let behind = Uuid::from_u128(5);
	put_entry_with_expiry(&ds, behind, &sample_session(behind), 1).await;

	// Another node takes the lapsed lease.
	lapse();
	let other = ds.fork_for_test_with_node_id(Uuid::from_u128(2));
	assert!(lease(&other).has_lease().await.unwrap(), "the other node takes the lease");

	// A handler's first check reads the lease rather than answering from its
	// throttle, and finds it taken.
	let second = ds.purge_rpc_session_slice(&lease(&ds)).await.unwrap();
	assert_eq!(second.lease, TaskLeaseState::Elsewhere, "the pass reports the lease lost");
	assert_eq!(second.progress, Progress::Done, "the next pass is the interval's");
	assert_eq!(second.items, 0);
	assert_eq!(durable_session_count(&ds).await, 2, "nothing is purged without the lease");

	// Once the lease is back, the scan continues after its place: the one
	// session past it is purged, and the one behind it waits.
	lapse();
	let third = ds.purge_rpc_session_slice(&lease(&ds)).await.unwrap();
	assert_eq!(third.lease, TaskLeaseState::Held);
	assert_eq!(third.items, 1, "the session past the place the scan kept");
	assert!(durable_session_entry(&ds, behind).await.is_some(), "the one behind it waits");
}
