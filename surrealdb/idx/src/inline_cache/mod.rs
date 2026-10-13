//! Inline sidecar caches: one key holding a record's complete adjacency in
//! one direction (`!ce`), or its complete incoming reference set (`!cr`).
//!
//! A fan-out over many source records normally opens a cursor per source;
//! with a cache present it becomes one batched multi-get. The value codec
//! and its three states — missing, live, spilled — live with the keyspace
//! in [`surrealdb_datastore::values::inline_cache`]; this module owns the
//! maintenance protocol that makes a live cache trustworthy.
//!
//! # The invariant readers rely on
//!
//! For a table whose cap has been continuously set since a cache key was
//! written, a present, non-spilled cache equals — as a set, and in storekey
//! byte order — the merged adjacency truth (blocks ∪ delta − tombstones)
//! for its `(record, direction)` at the same snapshot; a reference cache
//! likewise equals the record's reference-key range. This holds because:
//!
//! * every write path that adds or removes an entry updates the cache in the **same transaction**
//!   (the helpers below are called right after the underlying key writes, so a backfill's scan
//!   already sees them);
//! * a cache that cannot be maintained — the set outgrew the cap, or an entry has no representable
//!   form (a legacy pointer key with no embedded target) — flips to spilled, which is sticky until
//!   the key is deleted;
//! * every cap change is durable DDL and deletes, in the DDL transaction, every cache key its
//!   invalidation scan observes — which is every key already written at that transaction's
//!   snapshot, but not a range lock: a first-eligible-write backfill concurrently in flight under
//!   the old cap can still land a new key after the scan has passed it by, so a cache entry can
//!   survive into a cap regime it was not built under (see the cap-change invalidation in
//!   `alter/table.rs` for the gap this leaves open);
//! * the fold engine never touches cache keys — folding moves entries between the two merged tiers
//!   below the cache without changing the set.
//!
//! Entries are stored as the storekey bytes of their identity tuples, which
//! are exactly the corresponding key's bytes after its `(record, dir)`
//! prefix — so a cache emits entries in exactly the order the key scan it
//! replaces would have.
//!
//! Concurrent writers to one `(record, direction)` conflict on the one
//! small cache key and retry under MVCC; a hot record spills once and then
//! stops being maintained, which bounds the conflict window.

#[cfg(test)]
mod tests;

use std::borrow::Cow;

use anyhow::Result;
use surrealdb_datastore::Transaction;
use surrealdb_datastore::values::inline_cache::{
	CacheValue, EdgeCacheEntry, EdgeCacheValue, RefCacheEntry, RefCacheValue,
};
use surrealdb_kvs::Direction;
use surrealdb_kvs::key::Error as KeyError;

use crate::adjacency::{AdjacencyResolveCache, AdjacencyScope, MergedAdjacencyCursor};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition};
use crate::expr::Dir;
use crate::key::RawRange;
use crate::key::schema::{EdgeCacheKey, GraphDirPrefix, RefCacheKey, ReferenceIdPrefix};
use crate::val::{RecordId, TableName};

/// The hard upper bound on either cap. Every entry write rewrites the whole
/// cache value, so maintaining a cache of N entries costs O(N²) bytes over
/// its fill; the bound keeps that quadratic term small enough to stay
/// cheaper than the cursor it replaces.
pub const MAX_INLINE_CACHE_CAP: u32 = 256;

/// The table's operative `INLINE EDGES` cap: unset, or set to zero, means
/// the feature is off for the table — nothing is read or maintained. A
/// table absent from the catalog has never been defined and can carry no
/// cap.
pub fn effective_edges_cap(tb: Option<&TableDefinition>) -> Option<u32> {
	tb.and_then(|tb| tb.inline_edges_cap).filter(|cap| *cap > 0)
}

/// The table's operative `INLINE REFERENCES` cap; same contract as
/// [`effective_edges_cap`].
pub fn effective_refs_cap(tb: Option<&TableDefinition>) -> Option<u32> {
	tb.and_then(|tb| tb.inline_refs_cap).filter(|cap| *cap > 0)
}

/// The storekey bytes of a `(table, key)` pair — a cache entry's identity
/// segment, byte-identical to the same pair's segment inside a physical
/// graph or reference key.
fn record_bytes(rid: &RecordId) -> Result<Vec<u8>> {
	let mut buf = Vec::new();
	storekey::encode(&mut buf, &rid.table)?;
	storekey::encode(&mut buf, &rid.key)?;
	Ok(buf)
}

/// Folds one just-written edge into the vertex's adjacency cache,
/// materialising the cache if this is the first eligible write. Must be
/// called in the transaction that wrote the edge's graph keys, after them.
///
/// `vertex` is the record whose cache this maintains, `dir` the direction
/// the edge leaves it in, `edge` the edge record and `target` the far
/// vertex. No-op unless the vertex's table has an operative cap.
#[allow(clippy::too_many_arguments)]
pub async fn record_edge_write(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	vertex: &RecordId,
	dir: Dir,
	edge: &RecordId,
	target: &RecordId,
	resolve: Option<&AdjacencyResolveCache>,
) -> Result<()> {
	let tb = txn.get_tb(ns, db, &vertex.table, None).await?;
	let Some(cap) = effective_edges_cap(tb.as_deref()) else {
		return Ok(());
	};
	let key = EdgeCacheKey {
		ns,
		db,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir,
	};
	// A read-for-update on the cache key gives every writer to this
	// (vertex, dir) key a read-then-write pair on it, which is what
	// conflicts two concurrent writers on backends whose OCC validates
	// only the read set: without it, two first-eligible-write backfills
	// of the same missing key, or two incremental upserts, would each
	// compute their result from a plain read and last-writer-wins on the
	// blind set, silently losing whichever wrote first.
	match txn.getu_key(&key).await? {
		// Spill is sticky: the scan is authoritative and stays so.
		Some(CacheValue::Spilled) => Ok(()),
		Some(mut cache) => {
			let entry = EdgeCacheEntry {
				edge: record_bytes(edge)?,
				target: record_bytes(target)?,
			};
			let count = cache.upsert(entry).expect("a non-spilled cache accepts upserts") as u32;
			if count > cap {
				txn.set_key(&key, &EdgeCacheValue::Spilled).await
			} else {
				txn.set_key(&key, &cache).await
			}
		}
		None => {
			// First eligible write: build the complete set from the merged
			// truth. The scan runs in the writing transaction, so it already
			// includes the keys written just before this call.
			let folded = tb.as_deref().is_some_and(|tb| tb.graph_folded);
			let cache = backfill_edges(txn, ns, db, vertex, dir, folded, cap, resolve).await?;
			txn.set_key(&key, &cache).await
		}
	}
}

/// Builds a vertex's complete `(record, dir)` adjacency set through the
/// merged cursor, spilling when it exceeds the cap or contains an edge with
/// no embedded target (a legacy-format pointer key, which has no cache
/// representation).
#[allow(clippy::too_many_arguments)]
async fn backfill_edges(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	vertex: &RecordId,
	dir: Dir,
	folded: bool,
	cap: u32,
	resolve: Option<&AdjacencyResolveCache>,
) -> Result<EdgeCacheValue> {
	let scope = AdjacencyScope {
		ns,
		db,
		vertex,
		dir: Some(dir),
		edge_table: None,
		fk_lower: None,
		delta_range: GraphDirPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&vertex.table),
			id: Cow::Borrowed(&vertex.key),
			dir,
		}
		.range()?,
	};
	let mut cursor = MergedAdjacencyCursor::open(txn, scope, folded, None, resolve).await?;
	let mut entries: Vec<EdgeCacheEntry> = Vec::new();
	loop {
		let want = cap as usize + 1 - entries.len();
		let batch = cursor.next_batch(want as u32).await?;
		if batch.is_empty() {
			break;
		}
		for edge in batch {
			let Some(target) = edge.target else {
				return Ok(EdgeCacheValue::Spilled);
			};
			let candidate = EdgeCacheEntry {
				edge: record_bytes(&edge.edge)?,
				target: record_bytes(&target)?,
			};
			// A RELATE that retargets an existing edge id is accepted by the
			// base graph layer without deleting the pointer key for the
			// edge's previous target, so the vertex-side scan this cursor
			// replaces can hold two live pointer keys for the same edge
			// identity. They differ only in the target segment that trails
			// the identity in the key's byte order, so both sort adjacent to
			// each other here; a cache holds at most one entry per edge
			// identity (`upsert`/`remove` assume it), so a run sharing an
			// identity collapses to the one encountered last.
			match entries.last_mut() {
				Some(last) if last.edge == candidate.edge => *last = candidate,
				_ => entries.push(candidate),
			}
		}
		if entries.len() > cap as usize {
			return Ok(EdgeCacheValue::Spilled);
		}
	}
	Ok(EdgeCacheValue::Live(entries))
}

/// Removes one edge from the vertex's adjacency cache, in the transaction
/// that removed the edge's graph keys. A missing cache stays missing (this
/// never materialises one) and a spilled cache stays spilled.
pub async fn record_edge_delete(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	vertex: &RecordId,
	dir: Dir,
	edge: &RecordId,
) -> Result<()> {
	let tb = txn.get_tb(ns, db, &vertex.table, None).await?;
	if effective_edges_cap(tb.as_deref()).is_none() {
		return Ok(());
	}
	let key = EdgeCacheKey {
		ns,
		db,
		tb: Cow::Borrowed(&vertex.table),
		id: Cow::Borrowed(&vertex.key),
		dir,
	};
	let Some(mut cache) = txn.getu_key::<EdgeCacheKey>(&key).await? else {
		// A missing cache stays missing — but this transaction must still
		// WRITE the key, or a concurrent first-eligible-write backfill
		// (whose snapshot still holds the edge being deleted here) commits
		// beside this delete with disjoint write sets, freezing the deleted
		// edge into a live cache. The delete is a no-op on the data; paired
		// with the read-for-update above, it gives this transaction and
		// that backfill a read-then-write pair on the same key, which is
		// what conflicts them on every backend, not only the ones that
		// already detect a blind write-write conflict with no read at all.
		txn.del_key(&key).await?;
		return Ok(());
	};
	if cache.is_spilled() {
		return Ok(());
	}
	cache.remove(&record_bytes(edge)?);
	txn.set_key(&key, &cache).await
}

/// Folds one just-written reference back-link into the referenced record's
/// cache, materialising it on first eligible write. Must be called in the
/// transaction that wrote the reference key, after it. `target` is the
/// record being referenced (whose cache this is); the other three arguments
/// are the reference's identity, matching the reference key's own fields.
pub async fn record_ref_write(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	target: &RecordId,
	foreign_table: &TableName,
	foreign_field: &str,
	foreign_key: &crate::val::RecordIdKey,
) -> Result<()> {
	let tb = txn.get_tb(ns, db, &target.table, None).await?;
	let Some(cap) = effective_refs_cap(tb.as_deref()) else {
		return Ok(());
	};
	let key = RefCacheKey {
		ns,
		db,
		tb: Cow::Borrowed(&target.table),
		id: Cow::Borrowed(&target.key),
	};
	// See the identical read-for-update on the edge-cache twin above: it is
	// what conflicts two concurrent writers to this key on backends whose
	// OCC validates only the read set.
	match txn.getu_key(&key).await? {
		Some(CacheValue::Spilled) => Ok(()),
		Some(mut cache) => {
			let entry = RefCacheEntry {
				reference: reference_bytes(foreign_table, foreign_field, foreign_key)?,
			};
			let count = cache.upsert(entry).expect("a non-spilled cache accepts upserts") as u32;
			if count > cap {
				txn.set_key(&key, &RefCacheValue::Spilled).await
			} else {
				txn.set_key(&key, &cache).await
			}
		}
		None => {
			let cache = backfill_refs(txn, ns, db, target, cap).await?;
			txn.set_key(&key, &cache).await
		}
	}
}

/// The storekey bytes of a reference's `(foreign_table, foreign_field,
/// foreign_key)` identity — byte-identical to the reference key's bytes
/// after its `(record)` prefix.
fn reference_bytes(
	foreign_table: &TableName,
	foreign_field: &str,
	foreign_key: &crate::val::RecordIdKey,
) -> Result<Vec<u8>> {
	let mut buf = Vec::new();
	storekey::encode(&mut buf, foreign_table)?;
	storekey::encode(&mut buf, foreign_field)?;
	storekey::encode(&mut buf, foreign_key)?;
	Ok(buf)
}

/// Builds a record's complete reference set by scanning its reference-key
/// range, spilling past the cap. Entry bytes are the physical keys' tails,
/// so parity with scan order is by construction.
async fn backfill_refs(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	target: &RecordId,
	cap: u32,
) -> Result<RefCacheValue> {
	let prefix = ReferenceIdPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&target.table),
		id: Cow::Borrowed(&target.key),
	};
	// Entry bytes are each key's tail after the record's own prefix bytes —
	// which is `range_subtree`'s start, not `range`'s (a children range
	// starts one sentinel byte past the prefix).
	let skip = RawRange::from(prefix.range_subtree()?).start().len();
	let range: RawRange = prefix.range()?.into();
	let mut cursor = txn.open_keys_cursor_raw(range, Direction::Forward, 0, None).await?;
	let mut entries: Vec<RefCacheEntry> = Vec::new();
	loop {
		let want = cap as usize + 1 - entries.len();
		let batch = cursor.next_batch(want as u32).await?;
		if batch.is_empty() {
			break;
		}
		for key in &batch {
			// Every key the range yields extends the prefix (the range
			// starts one sentinel byte past it), so a shorter key can only
			// come from a corrupted keyspace — surface that as a decode
			// error rather than slicing out of bounds.
			let reference = key.get(skip..).ok_or_else(|| {
				anyhow::anyhow!(KeyError::Corrupted(
					"a reference key is shorter than its record prefix"
				))
			})?;
			entries.push(RefCacheEntry {
				reference: reference.to_vec(),
			});
		}
		if entries.len() > cap as usize {
			return Ok(RefCacheValue::Spilled);
		}
	}
	Ok(RefCacheValue::Live(entries))
}

/// Removes one reference from the referenced record's cache, in the
/// transaction that removed the reference key. Never materialises a
/// missing cache; leaves a spilled one spilled.
pub async fn record_ref_delete(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	target: &RecordId,
	foreign_table: &TableName,
	foreign_field: &str,
	foreign_key: &crate::val::RecordIdKey,
) -> Result<()> {
	let tb = txn.get_tb(ns, db, &target.table, None).await?;
	if effective_refs_cap(tb.as_deref()).is_none() {
		return Ok(());
	}
	let key = RefCacheKey {
		ns,
		db,
		tb: Cow::Borrowed(&target.table),
		id: Cow::Borrowed(&target.key),
	};
	let Some(mut cache) = txn.getu_key::<RefCacheKey>(&key).await? else {
		// The read-for-update above, paired with the delete below, arms the
		// conflict against a concurrent backfill on every backend; see the
		// adjacency twin above.
		txn.del_key(&key).await?;
		return Ok(());
	};
	if cache.is_spilled() {
		return Ok(());
	}
	cache.remove(&reference_bytes(foreign_table, foreign_field, foreign_key)?);
	txn.set_key(&key, &cache).await
}

/// Deletes a record's own cache keys when the record itself is purged.
/// Gated on the operative caps: while a cap is unset its cache subspace is
/// empty (the cap change that unset it deleted the subspace), so there is
/// nothing to remove.
///
/// The reference key carries a second gate: whether any `REFERENCE` field
/// in the database can target the table — the same gate the reference
/// purge itself applies. While the gate is closed no reference key can be
/// written, so no concurrent backfill can race this purge, and skipping
/// the read+delete pair is safe. At most an empty or spilled cache key
/// left behind by a field removal can outlive the record — both read
/// identically to the truth if the record id is ever re-created (an empty
/// set is authoritative precisely because no reference key can exist, and
/// a spilled cache falls back to the scan).
pub async fn purge_record_caches(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	rid: &RecordId,
) -> Result<()> {
	let tb = txn.get_tb(ns, db, &rid.table, None).await?;
	if effective_edges_cap(tb.as_deref()).is_some() {
		for dir in [Dir::In, Dir::Out] {
			let key = EdgeCacheKey {
				ns,
				db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
				dir,
			};
			// The read-for-update arms this delete against a concurrent
			// maintenance write to the same key — an edge write or delete
			// for this same record racing the purge — so the two conflict
			// on every backend instead of leaving whichever lands last to
			// silently decide the key's fate.
			let _ = txn.getu_key(&key).await?;
			txn.del_key(&key).await?;
		}
	}
	if effective_refs_cap(tb.as_deref()).is_some()
		&& txn.table_may_have_incoming_references(ns, db, &rid.table).await?
	{
		let key = RefCacheKey {
			ns,
			db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
		};
		let _ = txn.getu_key(&key).await?;
		txn.del_key(&key).await?;
	}
	Ok(())
}
