//! The maintenance protocol against an in-memory store.
//!
//! The oracle throughout is the scan a cache replaces: a live cache must
//! equal it as a set and in byte order after any sequence of writes,
//! deletes, folds and tombstones, and must spill — stickily — whenever it
//! cannot.

use std::borrow::Cow;

use surrealdb_datastore::values::graph::AdjacencyValue;
use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_kvs::key::KVKey;

use super::*;
use crate::adjacency::fold_scope;
use crate::catalog::{FromStored, StoredTableDefinition, TableId};
use crate::key::schema::{GraphKey, GraphPointerKey, ReferenceKey};
use crate::test_env::TestIndexStore;
use crate::val::{RecordIdKey, Value};

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

/// Defines the vertex table with the given caps.
async fn define_table(store: &TestIndexStore, edges_cap: Option<u32>, refs_cap: Option<u32>) {
	let txn = store.transaction(Write).await.unwrap();
	let mut table = TableDefinition::from_stored(&StoredTableDefinition::new(
		NS,
		DB,
		TableId(1),
		TableName::from("person"),
	))
	.unwrap();
	table.inline_edges_cap = edges_cap;
	table.inline_refs_cap = refs_cap;
	txn.put_tb(NS_NAME, DB_NAME, &table).await.unwrap();
	txn.commit().await.unwrap();
}

/// Writes one edge's vertex-side pointer key and maintains the cache, the
/// way `store_edges_data` does, in one transaction.
async fn relate(store: &TestIndexStore, edge: &RecordId, target: &RecordId) {
	let txn = store.transaction(Write).await.unwrap();
	let vertex = vertex();
	txn.set_key(&pointer_key(&vertex, edge, target), &AdjacencyValue::live()).await.unwrap();
	record_edge_write(&txn, NS, DB, &vertex, Dir::Out, edge, target, None).await.unwrap();
	txn.commit().await.unwrap();
}

/// Deletes one edge's vertex-side pointer key (or tombstones it on a folded
/// table) and maintains the cache, the way `purge_pointers` does.
async fn unrelate(store: &TestIndexStore, edge: &RecordId, target: &RecordId, folded: bool) {
	let txn = store.transaction(Write).await.unwrap();
	let vertex = vertex();
	let key = pointer_key(&vertex, edge, target);
	if folded {
		txn.set_key(&key, &AdjacencyValue::tombstone()).await.unwrap();
	} else {
		txn.del_key(&key).await.unwrap();
	}
	record_edge_delete(&txn, NS, DB, &vertex, Dir::Out, edge).await.unwrap();
	txn.commit().await.unwrap();
}

async fn read_cache(store: &TestIndexStore) -> Option<EdgeCacheValue> {
	let txn = store.transaction(Read).await.unwrap();
	let vertex = vertex();
	let key = EdgeCacheKey {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir: Dir::Out,
	};
	let value = txn.get_key(&key, None).await.unwrap();
	txn.cancel().await.unwrap();
	value
}

fn entry(edge_rid: &RecordId, target_rid: &RecordId) -> EdgeCacheEntry {
	EdgeCacheEntry {
		edge: record_bytes(edge_rid).unwrap(),
		target: record_bytes(target_rid).unwrap(),
	}
}

/// An entry's identity and payload bytes are exactly the pointer key's
/// bytes after its `(vertex, dir)` prefix — the property that makes cache
/// emission order equal key-scan order, for record ids of every shape.
#[test]
fn edge_entry_bytes_are_the_pointer_key_tail() {
	let vertex = vertex();
	let targets = [
		target("plain"),
		RecordId::new("person".into(), RecordIdKey::Number(42)),
		RecordId::new(
			"person".into(),
			RecordIdKey::Array(vec![Value::from("a"), Value::from(7)].into()),
		),
	];
	let edges = [
		edge("e\u{0}awkward"),
		RecordId::new("likes".into(), RecordIdKey::Number(-3)),
		edge("plain"),
	];
	let prefix_len = GraphDirPrefix {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir: Dir::Out,
	}
	.range_subtree()
	.unwrap()
	.start()
	.len();
	for (edge, target) in edges.iter().zip(&targets) {
		let key = pointer_key(&vertex, edge, target).encode_key().unwrap();
		let mut tail = record_bytes(edge).unwrap();
		tail.extend(record_bytes(target).unwrap());
		assert_eq!(&key[prefix_len..], tail.as_slice());
	}
}

/// The reference-entry twin: identity bytes equal the reference key's bytes
/// after its record prefix.
#[test]
fn ref_entry_bytes_are_the_reference_key_tail() {
	let target = vertex();
	let foreign_table = TableName::from("post");
	let foreign_field = "author[0].`weird field`";
	let foreign_key = RecordIdKey::from("p1".to_owned());
	let key = ReferenceKey {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&target.table),
		id: Cow::Borrowed(&target.key),
		foreign_table: Cow::Borrowed(&foreign_table),
		foreign_field: Cow::Borrowed(foreign_field),
		foreign_key: Cow::Borrowed(&foreign_key),
	}
	.encode_key()
	.unwrap();
	let prefix_len = ReferenceIdPrefix {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&target.table),
		id: Cow::Borrowed(&target.key),
	}
	.range_subtree()
	.map(RawRange::from)
	.unwrap()
	.start()
	.len();
	let tail = reference_bytes(&foreign_table, foreign_field, &foreign_key).unwrap();
	assert_eq!(&key[prefix_len..], tail.as_slice());
}

/// First eligible write backfills the complete pre-existing set; later
/// writes and deletes maintain it entry by entry, all visible in the
/// writing transaction.
#[tokio::test]
async fn write_backfills_then_maintains() {
	let store = TestIndexStore::new().await;
	define_table(&store, Some(8), None).await;

	// Two edges written before any cache exists — as if the cap predated
	// them but no write has materialised the cache yet.
	{
		let txn = store.transaction(Write).await.unwrap();
		let vertex = vertex();
		for id in ["a", "b"] {
			txn.set_key(&pointer_key(&vertex, &edge(id), &target(id)), &AdjacencyValue::live())
				.await
				.unwrap();
		}
		txn.commit().await.unwrap();
	}
	assert_eq!(read_cache(&store).await, None);

	// The first maintained write backfills everything, itself included.
	relate(&store, &edge("c"), &target("c")).await;
	assert_eq!(
		read_cache(&store).await,
		Some(EdgeCacheValue::Live(vec![
			entry(&edge("a"), &target("a")),
			entry(&edge("b"), &target("b")),
			entry(&edge("c"), &target("c")),
		]))
	);

	// A delete maintains the set; deleting everything leaves an
	// authoritative empty cache, not a missing one.
	unrelate(&store, &edge("b"), &target("b"), false).await;
	assert_eq!(
		read_cache(&store).await,
		Some(EdgeCacheValue::Live(vec![
			entry(&edge("a"), &target("a")),
			entry(&edge("c"), &target("c")),
		]))
	);
	unrelate(&store, &edge("a"), &target("a"), false).await;
	unrelate(&store, &edge("c"), &target("c"), false).await;
	assert_eq!(read_cache(&store).await, Some(EdgeCacheValue::Live(Vec::new())));
}

/// The cap bounds the cache, not the data: the write over the cap goes
/// through and flips the cache to spilled, and the spill outlives the
/// count dropping back under the cap.
#[tokio::test]
async fn spills_over_cap_and_stays_spilled() {
	let store = TestIndexStore::new().await;
	define_table(&store, Some(2), None).await;

	relate(&store, &edge("a"), &target("a")).await;
	relate(&store, &edge("b"), &target("b")).await;
	assert!(matches!(read_cache(&store).await, Some(EdgeCacheValue::Live(_))));

	relate(&store, &edge("c"), &target("c")).await;
	assert_eq!(read_cache(&store).await, Some(EdgeCacheValue::Spilled));

	unrelate(&store, &edge("a"), &target("a"), false).await;
	unrelate(&store, &edge("b"), &target("b"), false).await;
	assert_eq!(read_cache(&store).await, Some(EdgeCacheValue::Spilled));

	// A cap-sized re-backfill only happens after the key is deleted (a cap
	// change or record purge); emulate the record purge.
	{
		let txn = store.transaction(Write).await.unwrap();
		purge_record_caches(&txn, NS, DB, &vertex()).await.unwrap();
		txn.commit().await.unwrap();
	}
	assert_eq!(read_cache(&store).await, None);
	relate(&store, &edge("d"), &target("d")).await;
	assert_eq!(
		read_cache(&store).await,
		Some(EdgeCacheValue::Live(vec![
			entry(&edge("c"), &target("c")),
			entry(&edge("d"), &target("d")),
		]))
	);
}

/// A backfill reads through the merged cursor, so folded block entries are
/// included and tombstoned edges subtracted — the cache equals the merged
/// truth, not the delta keys.
#[tokio::test]
async fn backfill_reads_blocks_and_subtracts_tombstones() {
	let store = TestIndexStore::new().await;
	define_table(&store, Some(8), None).await;

	{
		let txn = store.transaction(Write).await.unwrap();
		let vertex = vertex();
		for id in ["a", "b", "c"] {
			txn.set_key(&pointer_key(&vertex, &edge(id), &target(id)), &AdjacencyValue::live())
				.await
				.unwrap();
		}
		txn.commit().await.unwrap();
	}
	// Fold the scope into packed blocks.
	let env = store.env(Write).await;
	let outcome =
		fold_scope(&env, NS, DB, NS_NAME, DB_NAME, &vertex(), Dir::Out, 1024).await.unwrap();
	env.tx().commit().await.unwrap();
	assert_eq!(outcome.folded, 3);

	// Tombstone one folded edge, as the delete path does on a folded table.
	unrelate(&store, &edge("b"), &target("b"), true).await;
	assert_eq!(read_cache(&store).await, None);

	// The next write backfills: blocks (a, c) plus the new delta edge.
	relate(&store, &edge("d"), &target("d")).await;
	assert_eq!(
		read_cache(&store).await,
		Some(EdgeCacheValue::Live(vec![
			entry(&edge("a"), &target("a")),
			entry(&edge("c"), &target("c")),
			entry(&edge("d"), &target("d")),
		]))
	);
}

/// A legacy-format pointer key has no embedded target and therefore no
/// cache representation: a backfill that observes one spills rather than
/// serving an incomplete set.
#[tokio::test]
async fn backfill_spills_on_legacy_keys() {
	let store = TestIndexStore::new().await;
	define_table(&store, Some(8), None).await;

	{
		let txn = store.transaction(Write).await.unwrap();
		let vertex = vertex();
		let legacy = edge("old");
		let key = GraphKey {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&legacy.table),
			foreign_key: Cow::Borrowed(&legacy.key),
		};
		txn.set_key(&key, &()).await.unwrap();
		txn.commit().await.unwrap();
	}
	relate(&store, &edge("new"), &target("new")).await;
	assert_eq!(read_cache(&store).await, Some(EdgeCacheValue::Spilled));
}

/// The reference cache follows the same protocol: backfill from the key
/// range on first write, per-entry maintenance, spill over cap.
#[tokio::test]
async fn ref_cache_backfills_maintains_and_spills() {
	let store = TestIndexStore::new().await;
	define_table(&store, None, Some(2)).await;
	let target = vertex();
	let post = |id: &str| RecordIdKey::from(id.to_owned());
	let post_table = TableName::from("post");
	let field = "author";

	let write_ref = |id: String, maintain: bool| {
		let target = target.clone();
		let post_table = post_table.clone();
		let store = &store;
		async move {
			let txn = store.transaction(Write).await.unwrap();
			let fk = post(&id);
			let key = ReferenceKey {
				ns: NS,
				db: DB,
				tb: Cow::Borrowed(&target.table),
				id: Cow::Borrowed(&target.key),
				foreign_table: Cow::Borrowed(&post_table),
				foreign_field: Cow::Borrowed(field),
				foreign_key: Cow::Borrowed(&fk),
			};
			txn.set_key(&key, &()).await.unwrap();
			if maintain {
				record_ref_write(&txn, NS, DB, &target, &post_table, field, &fk).await.unwrap();
			}
			txn.commit().await.unwrap();
		}
	};
	let read = || async {
		let txn = store.transaction(Read).await.unwrap();
		let key = RefCacheKey {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&target.table),
			id: Cow::Borrowed(&target.key),
		};
		let value = txn.get_key(&key, None).await.unwrap();
		txn.cancel().await.unwrap();
		value
	};

	// One pre-existing unmaintained reference, then a maintained write:
	// the backfill picks both up.
	write_ref("p1".to_owned(), false).await;
	assert_eq!(read().await, None);
	write_ref("p2".to_owned(), true).await;
	let expect = |ids: &[&str]| {
		Some(RefCacheValue::Live(
			ids.iter()
				.map(|id| RefCacheEntry {
					reference: reference_bytes(&post_table, field, &post(id)).unwrap(),
				})
				.collect(),
		))
	};
	assert_eq!(read().await, expect(&["p1", "p2"]));

	// Over the cap: spilled, stickily.
	write_ref("p3".to_owned(), true).await;
	assert_eq!(read().await, Some(RefCacheValue::Spilled));
	{
		let txn = store.transaction(Write).await.unwrap();
		let fk = post("p1");
		record_ref_delete(&txn, NS, DB, &target, &post_table, field, &fk).await.unwrap();
		txn.commit().await.unwrap();
	}
	assert_eq!(read().await, Some(RefCacheValue::Spilled));
}

/// A record purge touches the reference cache key only when some field in
/// the database can reference the table: with no such field the key cannot
/// exist and no backfill can race the purge, so the purge spends no
/// read-for-update and no delete on it — while still clearing both edge
/// directions. The planted raw key stands in for that unreachable state to
/// make the skip observable.
#[tokio::test]
async fn purge_skips_the_ref_cache_when_nothing_can_reference_the_table() {
	let store = TestIndexStore::new().await;
	define_table(&store, Some(8), Some(8)).await;
	let vertex = vertex();
	relate(&store, &edge("a"), &target("a")).await;
	assert!(matches!(read_cache(&store).await, Some(EdgeCacheValue::Live(_))));

	let ref_key = || RefCacheKey {
		ns: NS,
		db: DB,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
	};
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(&ref_key(), &RefCacheValue::Live(Vec::new())).await.unwrap();
		txn.commit().await.unwrap();
	}

	{
		let txn = store.transaction(Write).await.unwrap();
		purge_record_caches(&txn, NS, DB, &vertex).await.unwrap();
		txn.commit().await.unwrap();
	}
	assert_eq!(read_cache(&store).await, None, "both edge directions are purged");
	let txn = store.transaction(Read).await.unwrap();
	let untouched = txn.get_key(&ref_key(), None).await.unwrap();
	txn.cancel().await.unwrap();
	assert_eq!(
		untouched,
		Some(RefCacheValue::Live(Vec::new())),
		"the reference gate is closed, so the purge must not touch the key"
	);
}

/// With no operative cap every helper is a no-op: nothing is written, read
/// or maintained, and `Some(0)` disables exactly like `None`.
#[tokio::test]
async fn disabled_caps_are_inert() {
	let store = TestIndexStore::new().await;
	define_table(&store, Some(0), None).await;
	relate(&store, &edge("a"), &target("a")).await;
	assert_eq!(read_cache(&store).await, None);
}

/// The delete-vs-backfill race: a delete that finds no cache writes a
/// blind delete of the cache key purely to conflict with a concurrent
/// first-eligible-write backfill, whose snapshot still holds the edge
/// being deleted. Without that armed conflict both transactions commit
/// with disjoint write sets and the deleted edge is frozen into a live
/// cache forever.
#[tokio::test]
async fn missing_cache_delete_conflicts_with_a_concurrent_backfill() {
	let store = TestIndexStore::new().await;
	define_table(&store, Some(8), None).await;
	let vertex = vertex();

	// One edge, written before any cache exists.
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(&pointer_key(&vertex, &edge("a"), &target("a")), &AdjacencyValue::live())
			.await
			.unwrap();
		txn.commit().await.unwrap();
	}

	// T1 deletes edge `a`; the cache key is missing, so the maintenance
	// writes only the armed blind delete.
	let t_del = store.transaction(Write).await.unwrap();
	t_del.del_key(&pointer_key(&vertex, &edge("a"), &target("a"))).await.unwrap();
	record_edge_delete(&t_del, NS, DB, &vertex, Dir::Out, &edge("a")).await.unwrap();

	// T2 relates edge `b`; its first-eligible-write backfill scans at a
	// snapshot that still holds `a` and writes the cache key.
	let t_fill = store.transaction(Write).await.unwrap();
	t_fill
		.set_key(&pointer_key(&vertex, &edge("b"), &target("b")), &AdjacencyValue::live())
		.await
		.unwrap();
	record_edge_write(&t_fill, NS, DB, &vertex, Dir::Out, &edge("b"), &target("b"), None)
		.await
		.unwrap();

	// First committer wins the cache key; the stale backfill must not land.
	t_del.commit().await.unwrap();
	assert!(t_fill.commit().await.is_err(), "the backfill must conflict with the armed delete");

	// The retried write backfills from a post-delete snapshot.
	relate(&store, &edge("b"), &target("b")).await;
	assert_eq!(
		read_cache(&store).await,
		Some(EdgeCacheValue::Live(vec![entry(&edge("b"), &target("b"))]))
	);
}

/// A vertex-side pointer key embeds both the edge's own identity and its
/// target, so a RELATE that retargets an existing edge id — accepted by the
/// base graph layer without deleting the pointer key for the previous
/// target — leaves two live pointer keys with the same edge identity. A
/// backfill that scans them while both are still present must collapse them
/// to one cache entry, or a later delete of that edge — which removes by
/// edge identity alone — leaves the other stuck in the cache forever, since
/// nothing addresses it again once the edge it names is gone.
#[tokio::test]
async fn backfill_collapses_a_retargeted_edge_to_one_entry() {
	let store = TestIndexStore::new().await;
	define_table(&store, Some(8), None).await;
	let vertex = vertex();

	// Edge `shared` has two live pointer keys, as if it had been RELATEd
	// first to `old` and then retargeted to `new` without the previous
	// pointer key being removed. Neither write goes through the cache
	// maintenance call, so the cache stays missing.
	{
		let txn = store.transaction(Write).await.unwrap();
		txn.set_key(
			&pointer_key(&vertex, &edge("shared"), &target("old")),
			&AdjacencyValue::live(),
		)
		.await
		.unwrap();
		txn.set_key(
			&pointer_key(&vertex, &edge("shared"), &target("new")),
			&AdjacencyValue::live(),
		)
		.await
		.unwrap();
		txn.commit().await.unwrap();
	}
	assert_eq!(read_cache(&store).await, None);

	// The first maintained write backfills from scratch, through both
	// stale pointer keys.
	relate(&store, &edge("other"), &target("other")).await;
	let Some(EdgeCacheValue::Live(entries)) = read_cache(&store).await else {
		panic!("expected a live cache");
	};
	let shared_bytes = record_bytes(&edge("shared")).unwrap();
	assert_eq!(
		entries.iter().filter(|e| e.edge == shared_bytes).count(),
		1,
		"a retargeted edge id must collapse to exactly one cache entry, got {entries:?}"
	);

	// Deleting `shared` must remove it completely — no phantom entry for
	// the target that lost the collapse survives to outlive the edge.
	unrelate(&store, &edge("shared"), &target("new"), false).await;
	assert_eq!(
		read_cache(&store).await,
		Some(EdgeCacheValue::Live(vec![entry(&edge("other"), &target("other"))])),
		"the retargeted edge must not leave a phantom entry after its delete"
	);
}

/// Concurrency regressions that only manifest on backends whose OCC
/// validates just the read set, where a write with no read of its own key
/// never registers a conflict (see
/// `surrealdb-kvs-test::multi::multiwriter_same_keys_allow`). `mem://`
/// additionally conflicts on a blind write with no read at all, which is
/// why these races cannot be reproduced above: they need a backend that
/// does not. Gated to the `kv-tikv` backend and requiring a running cluster
/// at 127.0.0.1:2379 (the `tikv` CI lane), the same way `docids`'s own
/// cross-index concurrency regressions are.
#[cfg(test)]
#[cfg(feature = "kv-tikv")]
mod tikv_concurrency {
	use super::*;
	use crate::key::schema::{RootRoot, VersionKey};

	/// Wipes the shared cluster's keyspace and returns a fresh store, the
	/// way `docids::tikv_concurrency::fresh_tikv_ds` does; tests here run
	/// `#[serial_test::serial]` for the same reason those do.
	async fn fresh_tikv_store() -> TestIndexStore {
		let store = TestIndexStore::with_path("tikv:127.0.0.1:2379").await;
		let txn = store.transaction(Write).await.unwrap();
		txn.delr(RootRoot {}.range_subtree().unwrap()).await.unwrap();
		txn.del_key(&VersionKey {}).await.unwrap();
		txn.commit().await.unwrap();
		store
	}

	fn cache_key(vertex: &RecordId) -> EdgeCacheKey<'_> {
		EdgeCacheKey {
			ns: NS,
			db: DB,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir: Dir::Out,
		}
	}

	/// Two first-eligible-write backfills of the same missing cache key
	/// must not both commit. T2 opens and reads the missing cache before T1
	/// commits its own backfill, pinning T2's snapshot to the pre-T1 state
	/// even though T2's own maintenance call runs after — reproducing the
	/// exact interleaving a blind read-then-set loses: both transactions
	/// would compute a complete-looking backfill from their own snapshot,
	/// and the later blind set would overwrite the earlier one's, dropping
	/// its edge from the cache with neither commit failing.
	#[tokio::test]
	#[serial_test::serial]
	async fn concurrent_backfills_of_the_same_missing_key_conflict() {
		let store = fresh_tikv_store().await;
		define_table(&store, Some(8), None).await;
		let vertex = vertex();

		// One edge, written before any cache exists.
		{
			let txn = store.transaction(Write).await.unwrap();
			txn.set_key(&pointer_key(&vertex, &edge("a"), &target("a")), &AdjacencyValue::live())
				.await
				.unwrap();
			txn.commit().await.unwrap();
		}

		// T2 pins its snapshot on the missing-cache state before T1 commits.
		let t2 = store.transaction(Write).await.unwrap();
		assert_eq!(t2.get_key::<EdgeCacheKey>(&cache_key(&vertex), None).await.unwrap(), None);

		// T1 backfills edge `b` and commits first.
		let t1 = store.transaction(Write).await.unwrap();
		t1.set_key(&pointer_key(&vertex, &edge("b"), &target("b")), &AdjacencyValue::live())
			.await
			.unwrap();
		record_edge_write(&t1, NS, DB, &vertex, Dir::Out, &edge("b"), &target("b"), None)
			.await
			.unwrap();
		t1.commit().await.unwrap();

		// T2 backfills edge `c` against its pinned pre-T1 snapshot, which
		// still sees the cache as missing.
		t2.set_key(&pointer_key(&vertex, &edge("c"), &target("c")), &AdjacencyValue::live())
			.await
			.unwrap();
		record_edge_write(&t2, NS, DB, &vertex, Dir::Out, &edge("c"), &target("c"), None)
			.await
			.unwrap();
		assert!(t2.commit().await.is_err(), "the second backfill must conflict with the first");

		// The retried write backfills from a post-T1 snapshot and carries
		// every edge forward — nothing was silently dropped.
		relate(&store, &edge("c"), &target("c")).await;
		assert_eq!(
			read_cache(&store).await,
			Some(EdgeCacheValue::Live(vec![
				entry(&edge("a"), &target("a")),
				entry(&edge("b"), &target("b")),
				entry(&edge("c"), &target("c")),
			]))
		);
	}

	/// A record purge's unconditional cache-key delete must conflict with a
	/// concurrent, content-bearing maintenance write to the same key.
	/// T_purge reads the live cache, pinning its snapshot, before T_update
	/// commits its own edge into the same key — a blind delete racing a
	/// blind set would otherwise let either one silently overwrite the
	/// other with neither commit failing.
	#[tokio::test]
	#[serial_test::serial]
	async fn purge_conflicts_with_a_concurrent_incremental_update() {
		let store = fresh_tikv_store().await;
		define_table(&store, Some(8), None).await;
		let vertex = vertex();
		relate(&store, &edge("a"), &target("a")).await;

		// T_purge pins its snapshot on the live cache before T_update
		// commits.
		let t_purge = store.transaction(Write).await.unwrap();
		assert!(
			t_purge.get_key::<EdgeCacheKey>(&cache_key(&vertex), None).await.unwrap().is_some()
		);

		// T_update adds a second edge and commits, maintaining the same
		// cache key incrementally.
		{
			let txn = store.transaction(Write).await.unwrap();
			txn.set_key(&pointer_key(&vertex, &edge("b"), &target("b")), &AdjacencyValue::live())
				.await
				.unwrap();
			record_edge_write(&txn, NS, DB, &vertex, Dir::Out, &edge("b"), &target("b"), None)
				.await
				.unwrap();
			txn.commit().await.unwrap();
		}

		// T_purge, unaware of T_update, purges the vertex's own cache keys
		// against its earlier, pinned snapshot.
		purge_record_caches(&t_purge, NS, DB, &vertex).await.unwrap();
		assert!(
			t_purge.commit().await.is_err(),
			"the purge must conflict with the concurrent update to the same key"
		);
	}
}
