//! Cross-node concurrency regression tests. These exercise the last-writer-wins
//! batch-claim race (race #1) that only manifests on TiKV, so they are gated to
//! the `kv-tikv` backend and require a running cluster at 127.0.0.1:2379 (the
//! same one the `kvs/tests` suite uses in the `tikv` CI lane).

#![cfg(feature = "kv-tikv")]

use std::collections::HashSet;
use std::sync::Arc;

use surrealdb_catalog::{DatabaseId, NamespaceId};
use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::CommunityComposer;
use crate::key::schema::{RootRoot, VersionKey};
use crate::kvs::sequences::Sequences;
use crate::kvs::{Datastore, TransactionFactory, TransactionType};

/// Build a datastore against the local TiKV cluster, clear it so reruns are
/// deterministic, and hand back its transaction factory.
///
/// Two operations, because the keyspace has two top-level regions: everything
/// under the root, and the storage version key, which sits outside it so a
/// version probe can read it before any data exists. The version key has to go
/// too, or a version left by a differently-built binary survives the wipe and
/// the next run cannot open the store.
async fn fresh_tikv_tf() -> TransactionFactory {
	let ds = Datastore::builder()
		.with_id(Uuid::new_v4())
		.build_with_factory_path("tikv:127.0.0.1:2379", CommunityComposer())
		.await
		.unwrap();
	let tx = ds.transaction(TransactionType::Write).await.unwrap();
	tx.delr(RootRoot {}.range_subtree().unwrap()).await.unwrap();
	tx.del_key(&VersionKey {}).await.unwrap();
	tx.commit().await.unwrap();
	ds.transaction_factory().clone()
}

/// Race #1: many nodes (distinct node-ids) allocating from one shared
/// sequence domain must never hand out the same id twice. `BATCH == 1` makes
/// every id a fresh cross-node batch claim, and a `Barrier` releases all
/// nodes at once, so they hammer the same batch key and drive the `putc`
/// claim + `find_batch_allocation` retry loop hard. This is an end-to-end
/// uniqueness invariant over the real allocation path; the precise mechanism
/// the fix relies on — a `putc` create serializes where a blind `set` is
/// last-writer-wins — is pinned deterministically by the primitive
/// `kvs::tests::*::multiwriter_same_keys_putc` / `multiwriter_same_keys_allow`
/// pair (a blind `set` only loses to last-writer-wins in the non-overlapping
/// commit window, which barrier-synchronised contention here does not hit).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn table_doc_ids_unique_across_nodes() {
	use tokio::sync::Barrier;

	const NODES: usize = 4;
	const PER_NODE: usize = 100;
	const BATCH: u32 = 1;
	let tf = fresh_tikv_tf().await;
	let ns = NamespaceId(1);
	let db = DatabaseId(1);
	let tb: TableName = "t".into();
	let barrier = Arc::new(Barrier::new(NODES));

	let mut handles = Vec::with_capacity(NODES);
	for _ in 0..NODES {
		// Each task is a distinct node with its own node-id, so their
		// in-process allocators hold independent mutexes and genuinely race
		// in the KV store.
		let seqs = Sequences::new(tf.clone(), Uuid::new_v4());
		let tb = tb.clone();
		let barrier = Arc::clone(&barrier);
		handles.push(tokio::spawn(async move {
			// Start every node's allocation loop at the same instant to
			// maximise contention on the shared batch key.
			barrier.wait().await;
			let mut ids = Vec::with_capacity(PER_NODE);
			for _ in 0..PER_NODE {
				let id = seqs.next_table_doc_id(None, ns, db, tb.clone(), BATCH).await.unwrap();
				ids.push(id);
			}
			ids
		}));
	}

	let mut all = Vec::with_capacity(NODES * PER_NODE);
	for h in handles {
		all.extend(h.await.unwrap());
	}

	let unique: HashSet<_> = all.iter().copied().collect();
	assert_eq!(
		unique.len(),
		all.len(),
		"table doc-IDs must be globally unique across nodes; {} duplicate(s) out of {}",
		all.len() - unique.len(),
		all.len(),
	);
}
