//! Fold/merge round-trips against an in-memory store.
//!
//! The oracle throughout is the pre-fold keys-only scan: whatever a scope
//! looked like before folding, the merged cursor must reproduce edge for
//! edge, byte for byte, after any sequence of folds, tombstones and
//! re-writes.

use std::borrow::Cow;
use std::ops::Bound;

use surrealdb_datastore::values::graph::AdjacencyValue;
use surrealdb_kvs::TransactionType::{Read, Write};

use super::fold::{fold_scope, fold_scope_bounded};
use super::merge::{AdjacencyScope, MergedAdjacencyCursor, MergedAdjacencyEdge, scope_degree};
use crate::catalog::providers::{BoxProviderFut, CancellationProbe, TableProvider};
use crate::catalog::{
	DatabaseId, FromStored, NamespaceId, StoredTableDefinition, TableDefinition, TableId,
};
use crate::expr::Dir;
use crate::key::schema::{
	AdjacencyDirPrefix, GraphDirPrefix, GraphForeignTablePrefix, GraphKey, GraphPointerKey,
};
use crate::test_env::TestIndexStore;
use crate::val::{RecordId, RecordIdKey, TableName};

const NS: NamespaceId = NamespaceId(1);
const DB: DatabaseId = DatabaseId(1);
const NS_NAME: &str = "test";
const DB_NAME: &str = "test";

fn vertex() -> RecordId {
	RecordId::new("person".into(), "v".to_owned())
}

fn edge(id: &str) -> RecordId {
	RecordId::new("likes".into(), id.to_owned())
}

fn target(id: &str) -> RecordId {
	RecordId::new("person".into(), id.to_owned())
}

fn pointer_key<'a>(
	vertex: &'a RecordId,
	edge: &'a RecordId,
	target: &'a RecordId,
) -> GraphPointerKey<'a> {
	GraphPointerKey {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&edge.table),
		foreign_key: Cow::Borrowed(&edge.key),
		target_table: Cow::Borrowed(&target.table),
		target_key: Cow::Borrowed(&target.key),
	}
}

/// Defines the vertex table and writes one live pointer key per edge.
async fn seed(store: &TestIndexStore, edges: &[(RecordId, RecordId)]) {
	let txn = store.transaction(Write).await.unwrap();
	let vertex = vertex();
	let table = TableDefinition::from_stored(&StoredTableDefinition::new(
		NS,
		DB,
		TableId(1),
		TableName::from("person"),
	))
	.unwrap();
	txn.put_tb(NS_NAME, DB_NAME, &table).await.unwrap();
	for (edge, target) in edges {
		txn.set_key(&pointer_key(&vertex, edge, target), &AdjacencyValue::live()).await.unwrap();
	}
	txn.commit().await.unwrap();
}

/// Reads the whole scope through the merged cursor.
async fn read_all(store: &TestIndexStore, folded: bool) -> Vec<MergedAdjacencyEdge> {
	read_scope(store, folded, None).await
}

/// Reads the scope through the merged cursor, optionally bounded on the
/// edge key.
async fn read_scope(
	store: &TestIndexStore,
	folded: bool,
	bounds: Option<(&RecordIdKey, &RecordIdKey)>,
) -> Vec<MergedAdjacencyEdge> {
	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let edge_table = TableName::from("likes");
	let (delta_range, edge_table_ref, fk_lower) = match bounds {
		None => (
			GraphDirPrefix {
				ns: NS,
				db: DB,
				tb: Cow::Borrowed(&vertex.table),
				id: Cow::Borrowed(&vertex.key),
				dir: Dir::Out,
			}
			.range()
			.unwrap(),
			None,
			None,
		),
		Some((low, high)) => (
			GraphForeignTablePrefix {
				ns: NS,
				db: DB,
				tb: Cow::Borrowed(&vertex.table),
				id: Cow::Borrowed(&vertex.key),
				dir: Dir::Out,
				foreign_table: Cow::Borrowed(&edge_table),
			}
			.range_where((
				Bound::Included(Cow::Borrowed(low)),
				Bound::Included(Cow::Borrowed(high)),
			))
			.unwrap(),
			Some(&edge_table),
			Some(low),
		),
	};
	let scope = AdjacencyScope {
		ns: NS,
		db: DB,
		vertex: &vertex,
		dir: Some(Dir::Out),
		edge_table: edge_table_ref,
		fk_lower,
		delta_range,
	};
	let mut cursor = MergedAdjacencyCursor::open(&txn, scope, folded, None, None).await.unwrap();
	let mut out = Vec::new();
	loop {
		let batch = cursor.next_batch(97).await.unwrap();
		if batch.is_empty() {
			break;
		}
		out.extend(batch);
	}
	txn.cancel().await.unwrap();
	out
}

/// The whole outgoing-direction scope of the test vertex.
fn out_scope(vertex: &RecordId) -> AdjacencyScope<'_> {
	AdjacencyScope {
		ns: NS,
		db: DB,
		vertex,
		dir: Some(Dir::Out),
		edge_table: None,
		fk_lower: None,
		delta_range: GraphDirPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
		}
		.range()
		.unwrap(),
	}
}

/// Reads the whole scope through the keys-only merge.
async fn read_all_keys(store: &TestIndexStore, folded: bool) -> Vec<Vec<u8>> {
	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let mut cursor =
		MergedAdjacencyCursor::open(&txn, out_scope(&vertex), folded, None, None).await.unwrap();
	let mut out = Vec::new();
	loop {
		let batch = cursor.next_batch_keys(7).await.unwrap();
		if batch.is_empty() {
			break;
		}
		out.extend(batch);
	}
	txn.cancel().await.unwrap();
	out
}

fn keys_of(items: Vec<MergedAdjacencyEdge>) -> Vec<Vec<u8>> {
	items.into_iter().map(|item| item.key).collect()
}

/// Folds the scope to completion, one bounded pass per transaction.
async fn fold_all(store: &TestIndexStore, max_deltas: usize) -> usize {
	let vertex = vertex();
	let mut total = 0;
	loop {
		let env = store.env(Write).await;
		let outcome = fold_scope(&env, NS, DB, NS_NAME, DB_NAME, &vertex, Dir::Out, max_deltas)
			.await
			.unwrap();
		env.tx().commit().await.unwrap();
		total += outcome.folded;
		if !outcome.has_more {
			return total;
		}
	}
}

async fn count_delta(store: &TestIndexStore) -> usize {
	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let range = GraphDirPrefix {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir: Dir::Out,
	}
	.range()
	.unwrap();
	let count = txn.count(range, None).await.unwrap();
	txn.cancel().await.unwrap();
	count
}

async fn count_chunks(store: &TestIndexStore) -> usize {
	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let range = AdjacencyDirPrefix {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir: Dir::Out,
	}
	.range()
	.unwrap();
	let count = txn.count(range, None).await.unwrap();
	txn.cancel().await.unwrap();
	count
}

fn edges(n: usize) -> Vec<(RecordId, RecordId)> {
	(0..n).map(|i| (edge(&format!("e{i:05}")), target(&format!("t{i:05}")))).collect()
}

#[tokio::test]
async fn fold_then_merge_matches_the_prefold_scan() {
	let store = TestIndexStore::new().await;
	seed(&store, &edges(5)).await;

	let oracle = read_all(&store, false).await;
	assert_eq!(oracle.len(), 5);

	assert_eq!(fold_all(&store, 1024).await, 5);
	assert_eq!(count_delta(&store).await, 0, "folded pointer keys must be consumed");
	assert_eq!(count_chunks(&store).await, 1);

	// The table's marker was set transactionally with the fold.
	let txn = store.transaction(Read).await.unwrap();
	let tb = txn.get_tb(NS, DB, &TableName::from("person"), None).await.unwrap().unwrap();
	assert!(tb.graph_folded);
	txn.cancel().await.unwrap();

	let merged = read_all(&store, true).await;
	assert_eq!(merged, oracle, "the merged read must reproduce the pre-fold scan exactly");
}

#[tokio::test]
async fn tombstones_subtract_until_a_fold_consumes_them() {
	let store = TestIndexStore::new().await;
	let all = edges(5);
	seed(&store, &all).await;
	let oracle = read_all(&store, false).await;
	fold_all(&store, 1024).await;

	// Delete edge 2 the way `purge_pointers` does on a folded table.
	let (e, t) = &all[2];
	let txn = store.transaction(Write).await.unwrap();
	txn.set_key(&pointer_key(&vertex(), e, t), &AdjacencyValue::tombstone()).await.unwrap();
	txn.commit().await.unwrap();

	let merged = read_all(&store, true).await;
	let expected: Vec<_> = oracle.iter().filter(|edge| edge.edge != all[2].0).cloned().collect();
	assert_eq!(merged, expected, "a tombstone must subtract its folded entry");

	// The next fold consumes the tombstone and its block entry together.
	assert_eq!(fold_all(&store, 1024).await, 1);
	assert_eq!(count_delta(&store).await, 0);
	assert_eq!(read_all(&store, true).await, expected);
}

#[tokio::test]
async fn a_live_delta_wins_over_its_block_entry() {
	let store = TestIndexStore::new().await;
	let all = edges(5);
	seed(&store, &all).await;
	let oracle = read_all(&store, false).await;
	fold_all(&store, 1024).await;

	// Re-relate edge 2: the write path blindly re-writes its pointer key.
	let (e, t) = &all[2];
	let txn = store.transaction(Write).await.unwrap();
	txn.set_key(&pointer_key(&vertex(), e, t), &AdjacencyValue::live()).await.unwrap();
	txn.commit().await.unwrap();

	let merged = read_all(&store, true).await;
	assert_eq!(merged, oracle, "a re-related edge must be emitted once, not twice");
}

#[tokio::test]
async fn scopes_split_into_chunks_and_preserve_order() {
	let store = TestIndexStore::new().await;
	seed(&store, &edges(1200)).await;
	let oracle = read_all(&store, false).await;

	assert_eq!(fold_all(&store, 10_000).await, 1200);
	assert!(count_chunks(&store).await > 1, "1200 entries must split across chunks");
	assert_eq!(read_all(&store, true).await, oracle);
}

#[tokio::test]
async fn bounded_passes_drain_a_scope() {
	let store = TestIndexStore::new().await;
	seed(&store, &edges(10)).await;
	let oracle = read_all(&store, false).await;

	assert_eq!(fold_all(&store, 3).await, 10);
	assert_eq!(count_delta(&store).await, 0);
	assert_eq!(read_all(&store, true).await, oracle);
}

/// The chunk-rewrite bound: deltas scattered across a scope whose span
/// covers many chunks must fold over several bounded passes — each pass
/// loading at most `max_chunks` existing chunks and consuming only the
/// deltas those chunks cover — rather than one pass rewriting the whole
/// scope. Unloaded chunk boundaries are never shifted, so the merged read
/// stays exact at every intermediate state.
#[tokio::test]
async fn scattered_deltas_fold_over_bounded_chunk_passes() {
	let store = TestIndexStore::new().await;
	seed(&store, &edges(1200)).await;
	fold_all(&store, 10_000).await;
	let chunks = count_chunks(&store).await;
	assert!(chunks >= 3, "the seed must span several chunks, got {chunks}");

	// Scatter fresh live edges across the whole key space, plus tombstones
	// over folded entries near both ends of the scope.
	let all = edges(1200);
	let vertex = vertex();
	let scattered: Vec<(RecordId, RecordId)> = [50usize, 300, 600, 900, 1150]
		.iter()
		.map(|n| (edge(&format!("e{n:05}x")), target(&format!("t{n:05}x"))))
		.collect();
	{
		let txn = store.transaction(Write).await.unwrap();
		for (e, t) in &scattered {
			txn.set_key(&pointer_key(&vertex, e, t), &AdjacencyValue::live()).await.unwrap();
		}
		for n in [10usize, 620, 1190] {
			let (e, t) = &all[n];
			txn.set_key(&pointer_key(&vertex, e, t), &AdjacencyValue::tombstone()).await.unwrap();
		}
		txn.commit().await.unwrap();
	}

	// The merged read is already exact before any fold consumes the deltas.
	let expected = read_all(&store, true).await;
	assert_eq!(expected.len(), 1200 + scattered.len() - 3);

	// Drain with a one-chunk rewrite bound: the scattered span covers every
	// chunk, so the first pass must stop at its chunk boundary and report
	// more work.
	let mut passes = 0;
	loop {
		let env = store.env(Write).await;
		let outcome =
			fold_scope_bounded(&env, NS, DB, NS_NAME, DB_NAME, &vertex, Dir::Out, 1024, 1)
				.await
				.unwrap();
		env.tx().commit().await.unwrap();
		passes += 1;
		if passes == 1 {
			assert!(outcome.has_more, "one bounded pass must not rewrite the whole scope");
		}
		assert_eq!(read_all(&store, true).await, expected, "diverged after pass {passes}");
		if !outcome.has_more {
			break;
		}
	}
	assert!(passes > 1, "the drain must take several bounded passes");
	assert_eq!(count_delta(&store).await, 0, "the drain must consume every delta");
	assert_eq!(read_all(&store, true).await, expected);
}

#[tokio::test]
async fn bounded_windows_route_to_the_covering_chunk() {
	let store = TestIndexStore::new().await;
	seed(&store, &edges(1200)).await;
	let low = RecordIdKey::from("e00100".to_owned());
	let high = RecordIdKey::from("e01100".to_owned());
	let oracle = read_scope(&store, false, Some((&low, &high))).await;
	assert!(!oracle.is_empty());

	fold_all(&store, 10_000).await;
	assert_eq!(read_scope(&store, true, Some((&low, &high))).await, oracle);
}

/// Randomized RELATE / DELETE / re-RELATE / fold interleavings against a
/// model: after every operation, the merged read must equal the model
/// exactly, in order. Deletes mirror the write path's dispatch — a key
/// deletion while the vertex table has never folded, a tombstone once it
/// has — and folds run with a small pass bound so chunk rewrites, splits
/// and multi-pass drains are all exercised.
///
/// Seeds are fixed so a failure replays; the assertion message carries
/// the seed and step.
#[tokio::test]
async fn randomized_operations_match_the_model() {
	use rand::rngs::StdRng;
	use rand::{Rng, SeedableRng};

	for (seed, numeric) in [(1u64, false), (7, false), (42, false), (1, true), (7, true)] {
		let mut rng = StdRng::seed_from_u64(seed);
		let store = if numeric {
			TestIndexStore::with_idx_config(|config| config.graph_numeric_ids = true).await
		} else {
			TestIndexStore::new().await
		};
		seed_table(&store).await;
		let vertex = vertex();
		// The model: edge id -> target, in the same order the storekey
		// encoding sorts (zero-padded string ids sort identically).
		let mut model: std::collections::BTreeMap<String, RecordId> =
			std::collections::BTreeMap::new();

		for step in 0..300 {
			let op = rng.random_range(0..10u8);
			let n: usize = rng.random_range(0..40);
			let id = format!("e{n:03}");
			let e = edge(&id);
			// An edge's endpoints are immutable while it exists (the write
			// path rejects overrides), so a re-relate reuses the live
			// target; only a create — including a re-create after delete —
			// picks a fresh one.
			let t = model
				.get(&id)
				.cloned()
				.unwrap_or_else(|| target(&format!("t{:03}", rng.random_range(0..8usize))));
			match op {
				// Relate (and re-relate): a blind live pointer write.
				0..=4 => {
					let txn = store.transaction(Write).await.unwrap();
					txn.set_key(&pointer_key(&vertex, &e, &t), &AdjacencyValue::live())
						.await
						.unwrap();
					txn.commit().await.unwrap();
					model.insert(id, t);
				}
				// Delete: dispatched exactly as `purge_pointers` dispatches —
				// the pointer key is rebuilt blindly from the record's
				// endpoints, then tombstoned when the vertex table has
				// folded (the entry may live in a block the deletion cannot
				// reach) and deleted otherwise.
				5..=7 => {
					let Some(live_target) = model.remove(&id) else {
						continue;
					};
					let txn = store.transaction(Write).await.unwrap();
					let folded = txn
						.get_tb(NS, DB, &TableName::from("person"), None)
						.await
						.unwrap()
						.is_some_and(|tb| tb.graph_folded);
					let key = pointer_key(&vertex, &e, &live_target);
					if folded {
						txn.set_key(&key, &AdjacencyValue::tombstone()).await.unwrap();
					} else {
						txn.del_key(&key).await.unwrap();
					}
					txn.commit().await.unwrap();
				}
				// Fold one bounded pass.
				_ => {
					let env = store.env(Write).await;
					fold_scope(&env, NS, DB, NS_NAME, DB_NAME, &vertex, Dir::Out, 16)
						.await
						.unwrap();
					env.tx().commit().await.unwrap();
				}
			}

			let folded = {
				let txn = store.transaction(Read).await.unwrap();
				let folded = txn
					.get_tb(NS, DB, &TableName::from("person"), None)
					.await
					.unwrap()
					.is_some_and(|tb| tb.graph_folded);
				txn.cancel().await.unwrap();
				folded
			};
			let read: Vec<(String, RecordId)> = read_all(&store, folded)
				.await
				.into_iter()
				.map(|item| {
					let RecordIdKey::String(id) = &item.edge.key else {
						panic!("test edges use string ids");
					};
					(id.to_string(), item.target.expect("test edges embed targets"))
				})
				.collect();
			let expected: Vec<(String, RecordId)> =
				model.iter().map(|(id, t)| (id.clone(), t.clone())).collect();
			assert_eq!(read, expected, "seed {seed} (numeric: {numeric}) diverged at step {step}");
		}
	}
}

/// Defines the vertex table without seeding edges.
async fn seed_table(store: &TestIndexStore) {
	seed(store, &[]).await;
}

/// Counts the whole out-direction scope through `scope_degree`, with an
/// optional cancellation probe.
async fn degree_all(
	store: &TestIndexStore,
	folded: bool,
	cancel: Option<&dyn CancellationProbe>,
) -> anyhow::Result<u64> {
	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let scope = AdjacencyScope {
		ns: NS,
		db: DB,
		vertex: &vertex,
		dir: Some(Dir::Out),
		edge_table: None,
		fk_lower: None,
		delta_range: GraphDirPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
		}
		.range()
		.unwrap(),
	};
	let result = scope_degree(&txn, scope, folded, None, None, cancel).await;
	txn.cancel().await.unwrap();
	result
}

/// A probe that permits its first poll and fails every later one, so a
/// count that polls per batch errors mid-scan.
struct FailingProbe(std::sync::atomic::AtomicUsize);

impl CancellationProbe for FailingProbe {
	fn expect_not_timedout(&self) -> BoxProviderFut<'_, anyhow::Result<()>> {
		let calls = self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		Box::pin(async move {
			anyhow::ensure!(calls == 0, "cancelled");
			Ok(())
		})
	}
}

/// Every counting strategy polls the cancellation probe per batch: the
/// unfolded key count, the clean-tail header sum, and the merge drain must
/// all stop when the probe fails instead of draining the scope.
#[tokio::test]
async fn scope_degree_polls_cancellation_on_every_strategy() {
	let store = TestIndexStore::new().await;
	seed(&store, &edges(40)).await;

	// Unfolded: the keys-only count.
	let probe = FailingProbe(Default::default());
	assert!(degree_all(&store, false, Some(&probe)).await.is_err());
	assert_eq!(degree_all(&store, false, None).await.unwrap(), 40);

	// Folded with a clean tail: the header sum.
	fold_all(&store, 1024).await;
	let probe = FailingProbe(Default::default());
	assert!(degree_all(&store, true, Some(&probe)).await.is_err());
	assert_eq!(degree_all(&store, true, None).await.unwrap(), 40);

	// A live tail: the merge drain.
	let (e, t) = (edge("e90000"), target("t90000"));
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), &e, &t), &AdjacencyValue::live()).await.unwrap();
		txn.commit().await.unwrap();
	}
	let probe = FailingProbe(Default::default());
	assert!(degree_all(&store, true, Some(&probe)).await.is_err());
	assert_eq!(degree_all(&store, true, None).await.unwrap(), 41);
}

/// A bounded scope — a foreign-key window, with or without the `fk_lower`
/// hint — must count through the merge, which window-clips, never through
/// the whole-chunk header sums.
#[tokio::test]
async fn a_bounded_scope_counts_through_the_merge() {
	let store = TestIndexStore::new().await;
	seed(&store, &edges(10)).await;
	fold_all(&store, 1024).await;

	let low = RecordIdKey::from("e00002".to_owned());
	let high = RecordIdKey::from("e00006".to_owned());
	let oracle = read_scope(&store, true, Some((&low, &high))).await.len() as u64;
	assert!(oracle > 0 && oracle < 10, "the window must be a strict subset of the scope");

	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let edge_table = TableName::from("likes");
	let delta_range = GraphForeignTablePrefix {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir: Dir::Out,
		foreign_table: Cow::Borrowed(&edge_table),
	}
	.range_where((Bound::Included(Cow::Borrowed(&low)), Bound::Included(Cow::Borrowed(&high))))
	.unwrap();
	for fk_lower in [Some(&low), None] {
		let scope = AdjacencyScope {
			ns: NS,
			db: DB,
			vertex: &vertex,
			dir: Some(Dir::Out),
			edge_table: Some(&edge_table),
			fk_lower,
			delta_range: delta_range.clone(),
		};
		assert_eq!(
			scope_degree(&txn, scope, true, None, None, None).await.unwrap(),
			oracle,
			"a bounded scope must window-clip (fk_lower given: {})",
			fk_lower.is_some()
		);
	}
	txn.cancel().await.unwrap();
}

/// A numeric chunk's header counts entries whose doc-IDs may no longer
/// resolve, so the degree must come from the merge: removing a target
/// record's mapping with no masking tombstone — the `REMOVE TABLE` shape —
/// must subtract from the count exactly as it subtracts from a read.
#[tokio::test]
async fn numeric_chunks_count_through_the_merge() {
	use crate::docids::TableDocIds;

	let store = TestIndexStore::with_idx_config(|config| config.graph_numeric_ids = true).await;
	// The edge table must be defined for its ids to be compressible.
	{
		let txn = store.transaction(Write).await.unwrap();
		let table = TableDefinition::from_stored(&StoredTableDefinition::new(
			NS,
			DB,
			TableId(2),
			TableName::from("likes"),
		))
		.unwrap();
		txn.put_tb(NS_NAME, DB_NAME, &table).await.unwrap();
		txn.commit().await.unwrap();
	}
	let all = edges(5);
	seed(&store, &all).await;
	fold_all(&store, 1024).await;
	assert_eq!(degree_all(&store, true, None).await.unwrap(), 5);

	// Remove one TARGET record's mapping, with no tombstone anywhere.
	let (_, t2) = &all[2];
	{
		let txn = store.transaction(Write).await.unwrap();
		TableDocIds::new(NS, DB, t2.table.clone()).remove(&txn, &t2.key).await.unwrap();
		txn.commit().await.unwrap();
	}
	assert_eq!(read_all(&store, true).await.len(), 4);
	assert_eq!(degree_all(&store, true, None).await.unwrap(), 4);
}

/// Numeric folding: entries compress into doc-IDs, both involved tables
/// gain the `graph_doc_ids` marker, reads resolve back to the pre-fold
/// oracle, an id whose record mapping is removed (a deleted record)
/// disappears from reads and is collected by the next fold, and turning
/// the knob off decompresses chunks on their next fold.
#[tokio::test]
async fn numeric_folds_resolve_and_collect_deleted_ids() {
	use surrealdb_datastore::values::graph::BlockEntries;

	use crate::docids::TableDocIds;
	use crate::key::KVKey;

	let store = TestIndexStore::with_idx_config(|config| config.graph_numeric_ids = true).await;
	// The edge table must be defined for its ids to be compressible.
	{
		let txn = store.transaction(Write).await.unwrap();
		let table = TableDefinition::from_stored(&StoredTableDefinition::new(
			NS,
			DB,
			TableId(2),
			TableName::from("likes"),
		))
		.unwrap();
		txn.put_tb(NS_NAME, DB_NAME, &table).await.unwrap();
		txn.commit().await.unwrap();
	}
	let all = edges(5);
	seed(&store, &all).await;
	let oracle = read_all(&store, false).await;

	assert_eq!(fold_all(&store, 1024).await, 5);

	// The chunk is numeric and both tables carry the marker.
	{
		let txn = store.transaction(Read).await.unwrap();
		let vertex = vertex();
		let range = AdjacencyDirPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
		}
		.range()
		.unwrap();
		let chunks = txn.getr(range, None).await.unwrap();
		assert_eq!(chunks.len(), 1);
		assert!(
			matches!(chunks[0].1.entries, BlockEntries::Numeric(_)),
			"the fold must emit the numeric encoding"
		);
		for table in ["person", "likes"] {
			let tb = txn.get_tb(NS, DB, &TableName::from(table), None).await.unwrap().unwrap();
			assert!(tb.graph_doc_ids, "{table} must carry the doc-ID consumer marker");
		}
		txn.cancel().await.unwrap();
	}
	assert_eq!(read_all(&store, true).await, oracle);

	// Deleting a record removes its mapping; entries naming its id vanish
	// from reads and the next fold collects them. The tombstone the
	// deletion writes covers the edge either way — removing the TARGET
	// record's mapping alone must already hide the entry.
	let (e2, t2) = &all[2];
	{
		let txn = store.transaction(Write).await.unwrap();
		TableDocIds::new(NS, DB, t2.table.clone()).remove(&txn, &t2.key).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), e2, t2), &AdjacencyValue::tombstone()).await.unwrap();
		txn.commit().await.unwrap();
	}
	let expected: Vec<_> = oracle.iter().filter(|item| item.edge != *e2).cloned().collect();
	assert_eq!(read_all(&store, true).await, expected);
	fold_all(&store, 1024).await;
	assert_eq!(read_all(&store, true).await, expected);

	// With the knob off, the next fold rewrites touched chunks as plain.
	let plain_store = store;
	let mut config = (**plain_store.config()).clone();
	config.graph_numeric_ids = false;
	// A fresh delta forces the chunk rewrite.
	let (e9, t9) = (edge("e90009"), target("t90009"));
	{
		let txn = plain_store.transaction(Write).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), &e9, &t9), &AdjacencyValue::live()).await.unwrap();
		txn.commit().await.unwrap();
	}
	let plain_store = TestIndexStore::rewire_config(plain_store, config);
	fold_all(&plain_store, 1024).await;
	{
		let txn = plain_store.transaction(Read).await.unwrap();
		let vertex = vertex();
		let range = AdjacencyDirPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
		}
		.range()
		.unwrap();
		let chunks = txn.getr(range, None).await.unwrap();
		assert!(
			chunks.iter().all(|(_, b)| matches!(b.entries, BlockEntries::Plain(_))),
			"folds with the knob off must decompress the chunks they rewrite"
		);
		txn.cancel().await.unwrap();
	}
	let mut expected = expected;
	expected.push(MergedAdjacencyEdge {
		key: pointer_key(&vertex(), &e9, &t9).encode_key().unwrap().as_ref().to_vec(),
		edge: e9,
		target: Some(t9),
		props: None,
	});
	assert_eq!(read_all(&plain_store, true).await, expected);
}

/// The numeric writer sizes entries at their exact encoded width, so
/// small-id propless chunks fill to the entry cap rather than closing
/// early on an inflated byte estimate — and the encoding they produce
/// still fits the byte threshold.
#[tokio::test]
async fn numeric_chunks_fill_to_the_entry_cap() {
	use surrealdb_datastore::values::graph::BlockEntries;
	use surrealdb_kvs::value::KVValue;

	use super::fold::{CHUNK_MAX_BYTES, CHUNK_MAX_ENTRIES};

	let store = TestIndexStore::with_idx_config(|config| config.graph_numeric_ids = true).await;
	{
		let txn = store.transaction(Write).await.unwrap();
		let table = TableDefinition::from_stored(&StoredTableDefinition::new(
			NS,
			DB,
			TableId(2),
			TableName::from("likes"),
		))
		.unwrap();
		txn.put_tb(NS_NAME, DB_NAME, &table).await.unwrap();
		txn.commit().await.unwrap();
	}
	let total = CHUNK_MAX_ENTRIES + 88;
	seed(&store, &edges(total)).await;
	let oracle = read_all(&store, false).await;
	assert_eq!(fold_all(&store, 10_000).await, total);

	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let range = AdjacencyDirPrefix {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir: Dir::Out,
	}
	.range()
	.unwrap();
	let chunks = txn.getr(range, None).await.unwrap();
	txn.cancel().await.unwrap();
	let lens: Vec<usize> = chunks.iter().map(|(_, block)| block.len()).collect();
	assert_eq!(lens, vec![CHUNK_MAX_ENTRIES, total - CHUNK_MAX_ENTRIES]);
	for (_, block) in &chunks {
		assert!(matches!(block.entries, BlockEntries::Numeric(_)));
		let encoded = block.kv_encode_value().unwrap().len();
		assert!(
			encoded <= CHUNK_MAX_BYTES,
			"a {}-entry chunk encoded to {encoded} bytes",
			block.len()
		);
	}
	assert_eq!(read_all(&store, true).await, oracle);
}

/// Refolding an already-numeric scope reuses the ids carried through the
/// merge verbatim: no `!di`/`!dd` mapping appears or changes in either
/// involved table's space, and the rewritten chunk still resolves to the
/// same edges.
#[tokio::test]
async fn refolding_a_numeric_scope_assigns_no_new_ids() {
	use crate::key::schema::{DocKeyPrefix, DocLookupPrefix};

	let store = TestIndexStore::with_idx_config(|config| config.graph_numeric_ids = true).await;
	{
		let txn = store.transaction(Write).await.unwrap();
		let table = TableDefinition::from_stored(&StoredTableDefinition::new(
			NS,
			DB,
			TableId(2),
			TableName::from("likes"),
		))
		.unwrap();
		txn.put_tb(NS_NAME, DB_NAME, &table).await.unwrap();
		txn.commit().await.unwrap();
	}
	let all = edges(5);
	seed(&store, &all).await;
	let oracle = read_all(&store, false).await;
	assert_eq!(fold_all(&store, 1024).await, 5);

	/// Every `!di`/`!dd` mapping of both involved tables, raw.
	async fn mappings(store: &TestIndexStore) -> Vec<(Vec<u8>, Vec<u8>)> {
		let txn = store.transaction(Read).await.unwrap();
		let mut out = Vec::new();
		for table in ["person", "likes"] {
			let tb = TableName::from(table);
			let ranges: [crate::key::RawRange; 2] = [
				DocLookupPrefix::new(NS, DB, Cow::Borrowed(&tb)).range().unwrap().into(),
				DocKeyPrefix::new(NS, DB, Cow::Borrowed(&tb)).range().unwrap().into(),
			];
			for range in ranges {
				for (key, val) in txn.scan_raw(range, u32::MAX, 0, None).await.unwrap() {
					out.push((key.clone(), val.clone()));
				}
			}
		}
		txn.cancel().await.unwrap();
		out
	}
	let before = mappings(&store).await;
	assert!(!before.is_empty(), "the numeric fold must have assigned ids");

	// A tombstone forces the chunk rewrite; the surviving entries' ids ride
	// through the merge and back into the rewritten chunk untouched.
	let (e2, t2) = &all[2];
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), e2, t2), &AdjacencyValue::tombstone()).await.unwrap();
		txn.commit().await.unwrap();
	}
	assert_eq!(fold_all(&store, 1024).await, 1);

	assert_eq!(mappings(&store).await, before, "a refold must not touch the id mappings");
	let expected: Vec<_> = oracle.into_iter().filter(|item| item.edge != *e2).collect();
	assert_eq!(read_all(&store, true).await, expected);
}

/// A fold deletes exactly the keys it captured, conditioned on their
/// captured values: a deletion that lands between the fold's capture and
/// its commit rewrites a captured key, so the fold's commit must fail
/// rather than erase the newer tombstone — the lost edge would otherwise
/// resurrect from the block.
#[tokio::test]
async fn a_fold_racing_a_delete_loses_instead_of_losing_the_delete() {
	let store = TestIndexStore::new().await;
	let all = edges(5);
	seed(&store, &all).await;
	// Fold once so the table is marked and deletions tombstone.
	fold_all(&store, 1024).await;
	// Two fresh edges land in the delta layer.
	let (e5, t5) = (edge("e90005"), target("t90005"));
	let (e6, t6) = (edge("e90006"), target("t90006"));
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), &e5, &t5), &AdjacencyValue::live()).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), &e6, &t6), &AdjacencyValue::live()).await.unwrap();
		txn.commit().await.unwrap();
	}

	// The fold captures both live keys...
	let fold_env = store.env(Write).await;
	let outcome =
		fold_scope(&fold_env, NS, DB, NS_NAME, DB_NAME, &vertex(), Dir::Out, 1024).await.unwrap();
	assert_eq!(outcome.folded, 2);

	// ...then a concurrent deletion tombstones one of them and commits
	// first.
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), &e5, &t5), &AdjacencyValue::tombstone()).await.unwrap();
		txn.commit().await.unwrap();
	}

	// The fold must not win: committing it would erase the tombstone and
	// resurrect the deleted edge from the block it just wrote.
	assert!(
		fold_env.tx().commit().await.is_err(),
		"the fold must fail against the newer tombstone"
	);

	// The deletion survives, and a retried fold converges.
	fold_all(&store, 1024).await;
	let read = read_all(&store, true).await;
	assert!(read.iter().any(|item| item.edge == e6), "the untouched edge must survive");
	assert!(!read.iter().any(|item| item.edge == e5), "the deleted edge must stay deleted");
}

/// The keys-only stream must be exactly the `key` field of the edge
/// stream — same merge, same tombstone subtraction, same order — across
/// delta-only, block-only and mixed scopes.
#[tokio::test]
async fn next_batch_keys_matches_the_edge_stream() {
	use surrealdb_datastore::values::graph::BlockEntries;

	let store = TestIndexStore::new().await;
	let all = edges(20);
	seed(&store, &all).await;

	// Delta-only: the table has never folded.
	assert_eq!(read_all_keys(&store, false).await, keys_of(read_all(&store, false).await));

	// Block-only.
	fold_all(&store, 1024).await;
	assert_eq!(read_all_keys(&store, true).await, keys_of(read_all(&store, true).await));

	// Mixed: a fresh live delta plus a tombstone subtracting a folded entry.
	let (e, t) = (edge("e90000"), target("t90000"));
	let (e3, t3) = &all[3];
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), &e, &t), &AdjacencyValue::live()).await.unwrap();
		txn.set_key(&pointer_key(&vertex(), e3, t3), &AdjacencyValue::tombstone()).await.unwrap();
		txn.commit().await.unwrap();
	}
	let merged = read_all(&store, true).await;
	assert!(merged.iter().any(|item| item.edge == e));
	assert!(!merged.iter().any(|item| item.edge == *e3));
	assert_eq!(read_all_keys(&store, true).await, keys_of(merged));

	// Numeric blocks: the keys-only synthesis resolves the chunk's doc-IDs
	// exactly as the edge stream does, since the pointer-key domain both
	// emit in is built from the resolved record keys.
	let numeric = TestIndexStore::with_idx_config(|config| config.graph_numeric_ids = true).await;
	{
		// The edge table must be defined for its ids to be compressible.
		let txn = numeric.transaction(Write).await.unwrap();
		let table = TableDefinition::from_stored(&StoredTableDefinition::new(
			NS,
			DB,
			TableId(2),
			TableName::from("likes"),
		))
		.unwrap();
		txn.put_tb(NS_NAME, DB_NAME, &table).await.unwrap();
		txn.commit().await.unwrap();
	}
	seed(&numeric, &all).await;
	fold_all(&numeric, 1024).await;
	{
		let txn = numeric.transaction(Read).await.unwrap();
		let vertex = vertex();
		let range = AdjacencyDirPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
		}
		.range()
		.unwrap();
		let chunks = txn.getr(range, None).await.unwrap();
		assert_eq!(chunks.len(), 1);
		assert!(
			matches!(chunks[0].1.entries, BlockEntries::Numeric(_)),
			"the fold must emit the numeric encoding"
		);
		txn.cancel().await.unwrap();
	}
	assert_eq!(read_all_keys(&numeric, true).await, keys_of(read_all(&numeric, true).await));
}

/// A bounded read on a folded scope materializes block entries on demand:
/// a one-entry batch decodes the chunk value once and synthesizes a single
/// entry, keeping the rest of the chunk unmaterialized, and repeated
/// one-entry batches drain the chunk exactly across the resume boundary.
#[tokio::test]
async fn bounded_reads_materialize_block_entries_on_demand() {
	let store = TestIndexStore::new().await;
	seed(&store, &edges(300)).await;
	let oracle = read_all(&store, false).await;
	fold_all(&store, 1024).await;
	assert_eq!(count_chunks(&store).await, 1, "300 entries must fold into one chunk");

	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let mut cursor =
		MergedAdjacencyCursor::open(&txn, out_scope(&vertex), true, None, None).await.unwrap();
	let mut drained = cursor.next_batch(1).await.unwrap();
	assert_eq!(drained.len(), 1);
	assert_eq!(cursor.buffered_block_entries(), 0, "nothing may be buffered past the need");
	assert_eq!(cursor.open_chunk_remaining(), 299, "the chunk must stay open, unmaterialized");

	loop {
		let batch = cursor.next_batch(1).await.unwrap();
		if batch.is_empty() {
			break;
		}
		assert_eq!(batch.len(), 1);
		drained.extend(batch);
	}
	txn.cancel().await.unwrap();
	assert_eq!(drained, oracle, "one-entry batches must drain the scope exactly");
}

/// A small-limit batch escalates its delta fill size across a run of
/// tombstones instead of fetching one key per storage round trip; a peek
/// whose first item is live still costs exactly one fill.
#[tokio::test]
async fn tombstone_runs_escalate_the_delta_fill() {
	let store = TestIndexStore::new().await;
	let all = edges(300);
	seed(&store, &all).await;
	fold_all(&store, 1024).await;

	// Tombstone every folded edge, as deleting them all would.
	{
		let txn = store.transaction(Write).await.unwrap();
		for (e, t) in &all {
			txn.set_key(&pointer_key(&vertex(), e, t), &AdjacencyValue::tombstone()).await.unwrap();
		}
		txn.commit().await.unwrap();
	}

	// The peek walks all 300 tombstones (each also suppressing its block
	// entry) before concluding the scope is empty; escalation keeps the
	// fill count logarithmic.
	let vertex = vertex();
	{
		let txn = store.transaction(Read).await.unwrap();
		let mut cursor =
			MergedAdjacencyCursor::open(&txn, out_scope(&vertex), true, None, None).await.unwrap();
		let batch = cursor.next_batch_keys(1).await.unwrap();
		assert!(batch.is_empty());
		assert!(
			cursor.delta_fill_count() <= 12,
			"expected escalating fills, got {}",
			cursor.delta_fill_count()
		);
		txn.cancel().await.unwrap();
	}

	// Re-relate the first edge so the delta head is live: the peek must
	// cost exactly one fill, at the caller's one-key granularity.
	{
		let txn = store.transaction(Write).await.unwrap();
		let (e, t) = &all[0];
		txn.set_key(&pointer_key(&vertex, e, t), &AdjacencyValue::live()).await.unwrap();
		txn.commit().await.unwrap();
	}
	let txn = store.transaction(Read).await.unwrap();
	let mut cursor =
		MergedAdjacencyCursor::open(&txn, out_scope(&vertex), true, None, None).await.unwrap();
	let batch = cursor.next_batch_keys(1).await.unwrap();
	assert_eq!(batch.len(), 1);
	assert_eq!(cursor.delta_fill_count(), 1, "a live-head peek fills exactly once");
	txn.cancel().await.unwrap();
}

/// A pointer value carrying a section this build does not interpret cannot
/// be packed without discarding it — block entries hold only the props
/// section — so the fold must leave the key in the delta layer with its
/// value intact, keep serving the edge through the merge, and create no
/// block entry for it.
#[tokio::test]
async fn uninterpreted_sections_stay_in_the_delta_layer() {
	use surrealdb_datastore::values::graph::AdjacencySection;

	let store = TestIndexStore::new().await;
	let all = edges(3);
	seed(&store, &all).await;

	// A live pointer whose value carries an opaque bit-2 section, as a
	// future build interpreting that bit would write it.
	let vertex = vertex();
	let (e, t) = (edge("e-sectioned"), target("t-sectioned"));
	let opaque = AdjacencyValue {
		tombstone: false,
		sections: vec![AdjacencySection {
			flag_bit: 2,
			body: b"opaque".to_vec(),
		}],
	};
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(&pointer_key(&vertex, &e, &t), &opaque).await.unwrap();
		txn.commit().await.unwrap();
	}

	let oracle = read_all(&store, false).await;
	assert_eq!(oracle.len(), 4);

	fold_all(&store, 1024).await;

	// The sectioned key survives in place, its value intact, and no block
	// entry was created for it — only the three plain edges packed.
	assert_eq!(count_delta(&store).await, 1);
	let txn = store.transaction(Read).await.unwrap();
	let survived = txn.get_key(&pointer_key(&vertex, &e, &t), None).await.unwrap();
	assert_eq!(survived, Some(opaque), "the fold must not touch the sectioned value");
	let range = AdjacencyDirPrefix {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir: Dir::Out,
	}
	.range()
	.unwrap();
	let chunks = txn.scan(range, u32::MAX, 0, None).await.unwrap();
	txn.cancel().await.unwrap();
	let entries: usize = chunks.iter().map(|(_, block)| block.len()).sum();
	assert_eq!(entries, 3, "no block entry may be created for the sectioned edge");

	// The merge keeps serving the edge from the delta side.
	assert_eq!(read_all(&store, true).await, oracle);
}

#[tokio::test]
async fn legacy_keys_stay_in_the_delta_layer() {
	let store = TestIndexStore::new().await;
	let all = edges(3);
	seed(&store, &all).await;

	// Two legacy-format keys, as a pre-#180 writer left them.
	let vertex = vertex();
	let legacy: Vec<RecordId> = vec![edge("a-legacy"), edge("b-legacy")];
	let txn = store.transaction(Write).await.unwrap();
	for e in &legacy {
		let key = GraphKey {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&e.table),
			foreign_key: Cow::Borrowed(&e.key),
		};
		txn.set_key(&key, &()).await.unwrap();
	}
	txn.commit().await.unwrap();

	let oracle = read_all(&store, false).await;
	assert_eq!(oracle.len(), 5);

	fold_all(&store, 1024).await;
	// The legacy keys cannot be packed and must survive in place.
	assert_eq!(count_delta(&store).await, 2);
	let merged = read_all(&store, true).await;
	assert_eq!(merged, oracle);
	assert!(
		merged.iter().filter(|e| e.target.is_none()).count() == 2,
		"legacy entries carry no target"
	);
}

/// A chunk whose `involved` tables include one undefined table always falls
/// back to Plain, and — regardless of which of the *defined* tables the
/// (unspecified-order) scan happens to visit first — none of them gets
/// `graph_doc_ids` set: the marker is written only once every involved
/// table is confirmed defined, never speculatively per table. Runs several
/// independent trials because each fold builds its own freshly, randomly
/// seeded `HashSet` for `involved`; a single trial could not rule out an
/// iteration order that happens to visit the undefined table first, which
/// would hide a partial-marking regression.
#[tokio::test]
async fn undefined_target_table_marks_no_table_numeric() {
	use surrealdb_datastore::values::graph::BlockEntries;

	for trial in 0..25 {
		let store = TestIndexStore::with_idx_config(|config| config.graph_numeric_ids = true).await;
		let vertex = vertex();
		let txn = store.transaction(Write).await.unwrap();
		for (id, name) in [(1, "person"), (2, "likes"), (3, "friend")] {
			let table = TableDefinition::from_stored(&StoredTableDefinition::new(
				NS,
				DB,
				TableId(id),
				TableName::from(name),
			))
			.unwrap();
			txn.put_tb(NS_NAME, DB_NAME, &table).await.unwrap();
		}
		// "ghost" is deliberately left undefined.
		let e1 = edge("e00001");
		let e2 = edge("e00002");
		let e3 = edge("e00003");
		let friend_target = RecordId::new("friend".into(), "t00002".to_owned());
		let ghost_target = RecordId::new("ghost".into(), "t00003".to_owned());
		txn.set_key(&pointer_key(&vertex, &e1, &target("t00001")), &AdjacencyValue::live())
			.await
			.unwrap();
		txn.set_key(&pointer_key(&vertex, &e2, &friend_target), &AdjacencyValue::live())
			.await
			.unwrap();
		txn.set_key(&pointer_key(&vertex, &e3, &ghost_target), &AdjacencyValue::live())
			.await
			.unwrap();
		txn.commit().await.unwrap();

		let env = store.env(Write).await;
		fold_scope(&env, NS, DB, NS_NAME, DB_NAME, &vertex, Dir::Out, 1024).await.unwrap();
		env.tx().commit().await.unwrap();

		let txn = store.transaction(Read).await.unwrap();
		let range = AdjacencyDirPrefix {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
		}
		.range()
		.unwrap();
		let chunks = txn.getr(range, None).await.unwrap();
		assert_eq!(chunks.len(), 1, "trial {trial}");
		assert!(
			matches!(chunks[0].1.entries, BlockEntries::Plain(_)),
			"trial {trial}: a chunk naming an undefined table must fall back to Plain"
		);
		for name in ["person", "likes", "friend"] {
			let tb = txn.get_tb(NS, DB, &TableName::from(name), None).await.unwrap().unwrap();
			assert!(
				!tb.graph_doc_ids,
				"trial {trial}: {name} must not be marked when the chunk fell back to Plain"
			);
		}
		txn.cancel().await.unwrap();
	}
}
