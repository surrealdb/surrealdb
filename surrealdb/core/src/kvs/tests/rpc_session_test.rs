//! Tests for durable RPC session storage (`/!se`).
//!
//! Client-attached RPC sessions can be mirrored to the KV store so they
//! survive the process that attached them ([`Datastore::persist_rpc_session`]
//! and friends). These tests verify the storage lifecycle: persist/load round
//! trips, lazy expiry on load, the sliding-TTL refresh, deletion, and the
//! background purge task [`Datastore::purge_expired_rpc_sessions`].

use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};
use uuid::Uuid;
use web_time::Duration;

use crate::dbs::{Capabilities, DurableSession, Session};
use crate::key::root::se::{Se, SePrefix};
use crate::key::{KVRange, KeyRange};
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

/// Count the keys currently stored under a half-open byte range.
async fn count_range(ds: &Datastore, range: KeyRange<'_>) -> usize {
	let tx = ds.transaction(Read).await.unwrap();
	let res = tx.getr(range, None).await.unwrap();
	let _ = tx.cancel().await;
	res.len()
}

/// Number of durable session entries currently stored.
async fn durable_session_count(ds: &Datastore) -> usize {
	let range = SePrefix {}.encode_range().unwrap();
	count_range(ds, range).await
}

/// The raw stored entry for a session id, if any.
async fn durable_session_entry(ds: &Datastore, id: Uuid) -> Option<DurableSession> {
	let tx = ds.transaction(Read).await.unwrap();
	let res = tx
		.get_key(
			&Se {
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
	let value = DurableSession::from_session(session, expires_at);
	let tx = ds.transaction(Write).await.unwrap();
	tx.set_key(
		&Se {
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

	let (loaded, _expires_at) =
		ds.load_rpc_session(id).await.unwrap().expect("session should load");
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
	// expiry. Refreshing the TTL is the job of `persist_rpc_session`, which
	// callers invoke only after authorizing a request — never on the load
	// path, which also runs for unauthenticated lookups.
	ds.persist_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap();
	let before = durable_session_entry(&ds, id).await.unwrap().expires_at;

	let (loaded, reported) = ds.load_rpc_session(id).await.unwrap().expect("session should load");
	assert_eq!(loaded, session);
	assert_eq!(reported, before, "load should report the stored expiry it read");
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
	assert!(ds.create_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap());
	assert!(!ds.create_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap());
	assert_eq!(durable_session_count(&ds).await, 1);
}

#[tokio::test]
async fn update_only_refreshes_a_present_entry_and_never_resurrects() {
	let ds = mem_ds().await;
	let id = Uuid::new_v4();
	let session = sample_session(id);

	// No entry yet: update reports the session gone and creates nothing.
	assert!(!ds.update_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap());
	assert_eq!(durable_session_count(&ds).await, 0);

	// Once created, update refreshes it.
	ds.create_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap();
	assert!(ds.update_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap());

	// After deletion, update must not recreate it — it reports gone.
	ds.delete_rpc_session(id).await.unwrap();
	assert!(!ds.update_rpc_session(id, &session, Duration::from_secs(60)).await.unwrap());
	assert_eq!(durable_session_count(&ds).await, 0, "update must not resurrect a deleted session");
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
