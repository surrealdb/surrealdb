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

/// Writes a sequence definition the way `DEFINE SEQUENCE` leaves it, clearing
/// the allocator rows the statement clears.
async fn define_sequence(
	tf: &TransactionFactory,
	sqs: &Sequences,
	ns: NamespaceId,
	db: DatabaseId,
	name: &str,
	start: i64,
	batch: u32,
) -> anyhow::Result<()> {
	use std::borrow::Cow;

	use crate::key::schema::{SeqBatchPrefix, SeqStatePrefix, SequenceKey};

	let tx = tf.transaction(TransactionType::Write, sqs.clone()).await?;
	let res = async {
		tx.set_key(
			&SequenceKey {
				ns,
				db,
				sq: Cow::Borrowed(name),
			},
			&surrealdb_catalog::SequenceDefinition {
				name: name.into(),
				batch,
				start,
				timeout: None,
			},
		)
		.await?;
		tx.delr(
			SeqBatchPrefix {
				ns,
				db,
				sq: Cow::Borrowed(name),
			}
			.range()?,
		)
		.await?;
		tx.delr(
			SeqStatePrefix {
				ns,
				db,
				sq: Cow::Borrowed(name),
			}
			.range()?,
		)
		.await?;
		Ok::<(), anyhow::Error>(())
	}
	.await;
	match res {
		Ok(()) => tx.commit().await,
		Err(e) => {
			tx.cancel().await?;
			Err(e)
		}
	}
}

/// Repositioning a sequence while it is being allocated from issues no id
/// twice, driven against TiKV.
///
/// TiKV is where a reset is most likely to reach the allocator as a conflict
/// rather than as a failed comparison. The comparisons themselves — the claim's
/// put-if-absent and the cursor's compare-and-set — report a miss from the call
/// on every backend. What differs here is the other shape: writes that got
/// through are validated against concurrent commits at commit time, so a reset
/// landing between a write and its commit is reported to whichever of the two
/// loses. The allocator has to treat that as the reset it is, which is worth
/// running against a backend that produces it readily, even though the
/// invariant is the same one the `mem` suite asserts.
///
/// It is a smoke test of that path, not a regression test for the reissue
/// defect: with the definition guards removed it still passes here, while the
/// equivalent `mem` test fails reliably. What reproduces the defect on this
/// backend has not been found, so the defect stays pinned on `mem` and this
/// covers gross breakage of the reposition path on TiKV.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial_test::serial]
async fn allocation_never_reissues_across_repositions_on_tikv() {
	use std::time::Duration;

	let tf = fresh_tikv_tf().await;
	let ns = NamespaceId(1);
	let db = DatabaseId(1);
	let name = "sq";
	const BATCH: u32 = 10;
	const NODES: usize = 3;
	const TASKS_PER_NODE: usize = 2;
	const ALLOCATIONS: usize = 500;
	const STRIDE: i64 = 10_000;
	const MAX_EPOCHS: i64 = 40;

	let seed = Sequences::new(tf.clone(), Uuid::new_v4());
	define_sequence(&tf, &seed, ns, db, name, 1, BATCH).await.unwrap();

	// The repositioner runs until the workers stop rather than on a schedule:
	// how long allocation takes is a property of the cluster, and a fixed sleep
	// silently stops overlapping the moment it changes.
	let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

	let mut workers = Vec::new();
	for _ in 0..NODES {
		let seqs = Sequences::new(tf.clone(), Uuid::new_v4());
		for _ in 0..TASKS_PER_NODE {
			let seqs = seqs.clone();
			let tf = tf.clone();
			workers.push(tokio::spawn(async move {
				let mut got = Vec::new();
				for _ in 0..ALLOCATIONS {
					let tx = tf.transaction(TransactionType::Read, seqs.clone()).await.unwrap();
					let res = seqs.next_user_sequence_id(None, &tx, ns, db, name).await;
					let _ = tx.cancel().await;
					// A reset landing on a freshly rebuilt allocator, and a
					// write that loses its race, both issue nothing.
					if let Ok(v) = res {
						got.push(v);
					}
				}
				got
			}));
		}
	}

	let repositioner = {
		let tf = tf.clone();
		let done = Arc::clone(&done);
		tokio::spawn(async move {
			let sqs = Sequences::new(tf.clone(), Uuid::new_v4());
			let mut epoch = 0i64;
			while !done.load(std::sync::atomic::Ordering::Relaxed) && epoch < MAX_EPOCHS {
				epoch += 1;
				while define_sequence(&tf, &sqs, ns, db, name, epoch * STRIDE, BATCH).await.is_err()
				{
				}
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
			epoch
		})
	};

	let mut issued = Vec::new();
	for w in workers {
		issued.extend(
			tokio::time::timeout(Duration::from_secs(120), w)
				.await
				.expect("allocation stalled")
				.expect("a worker panicked"),
		);
	}
	done.store(true, std::sync::atomic::Ordering::Relaxed);
	let repositions = tokio::time::timeout(Duration::from_secs(120), repositioner)
		.await
		.expect("repositioning stalled")
		.expect("the repositioner panicked");

	// The fixture only means anything if repositioning ran while allocation
	// did, so that is asserted rather than assumed. It counts what the
	// repositioner committed before the workers finished, not what the workers
	// were served: a defect that pins allocators to their old windows would
	// suppress the latter and quietly disarm this check.
	assert!(
		repositions > 1,
		"allocation finished before repositioning got going, so nothing was exercised: \
		 {repositions} reposition(s)"
	);

	assert!(!issued.is_empty(), "the fixture issued nothing, so it asserts nothing");
	let unique: HashSet<i64> = issued.iter().copied().collect();
	assert_eq!(
		unique.len(),
		issued.len(),
		"an id was issued twice across a reposition on TiKV ({} of {} distinct)",
		unique.len(),
		issued.len(),
	);
}
