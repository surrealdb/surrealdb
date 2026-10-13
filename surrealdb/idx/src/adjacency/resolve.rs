//! Doc-ID resolution for numeric adjacency entries.
//!
//! Numeric block entries hold table-level doc-IDs; readers resolve them
//! back to record keys through the `!dd` mapping, batched per chunk and
//! short-circuited by a process-wide cache. Within one table generation, a
//! mapping, once written, is immutable — the id space neither recycles nor
//! remaps an id — so a cached positive needs no per-entry invalidation:
//! after a record is deleted its mapping is removed, but every id still
//! reachable from a block was mapped before that block committed, and the
//! deletion that removed the mapping also tombstoned the entries naming
//! it, so a stale positive is always masked by the merge.
//!
//! `REMOVE TABLE` ends a generation: it wipes the table's whole keyspace,
//! including the `!di`/`!dd` mappings and the allocator that hands out
//! ids, so a redefinition restarts id allocation from scratch. A numeric
//! block naming one of the removed table's ids can still exist — a block
//! lives under its *vertex*'s keyspace, which may be a different,
//! untouched table — so the merge's usual mask (the removed mapping's own
//! tombstoned entries) does not apply here: nothing tombstones a foreign
//! block's entries when the table it points into is removed. The cache is
//! therefore keyed on the table's [`TableId`], never its name: table ids
//! come from a monotonic sequence and are never reused, so a redefined
//! table is a fresh key space by construction — on every node, however
//! stale its view — and a removed generation's entries are unreachable
//! garbage that ages out. Callers supply the id from the table definition
//! in their own snapshot; a table with no definition there resolves
//! uncached.
//!
//! Time-travel reads resolve through the mapping state their snapshot
//! saw. Cache hits stay valid there (immutability again), but a miss read
//! at a historical version must not populate the cache: the mapping may
//! since have been removed, and a current-time read finding the cached
//! value would resurrect it.

use std::sync::Arc;

use anyhow::Result;
use quick_cache::Weighter;
use quick_cache::sync::Cache;
use surrealdb_datastore::Transaction;

use crate::catalog::{DatabaseId, NamespaceId, TableId};
use crate::docids::{DocId, TableDocIds};
use crate::val::{RecordIdKey, TableName};

type CacheKey = (NamespaceId, DatabaseId, TableId, DocId);

/// One cached resolution: the record key plus its weight, measured once
/// when the entry is created.
struct CachedKey {
	key: RecordIdKey,
	/// The record key's storekey-encoded byte length — a faithful proxy
	/// for its heap footprint whatever its shape, so composite (array /
	/// object) keys are charged what they actually hold rather than a
	/// flat guess quick_cache would evict by.
	weight: u32,
}

#[derive(Clone)]
struct ResolveWeighter;

impl Weighter<CacheKey, Arc<CachedKey>> for ResolveWeighter {
	fn weight(&self, _: &CacheKey, value: &Arc<CachedKey>) -> u64 {
		// The fixed tuple and entry fields plus the measured record key.
		(std::mem::size_of::<CacheKey>() + std::mem::size_of::<RecordIdKey>() + 32) as u64
			+ u64::from(value.weight)
	}
}

/// The process-wide doc-ID → record-key cache for adjacency resolution.
pub struct AdjacencyResolveCache {
	cache: Cache<CacheKey, Arc<CachedKey>, ResolveWeighter>,
}

impl AdjacencyResolveCache {
	/// A cache bounded to roughly `size` bytes.
	pub fn new(size: u64) -> Self {
		let items = (size / 64).clamp(16, u64::MAX) as usize;
		Self {
			cache: Cache::with_weighter(items, size, ResolveWeighter),
		}
	}

	/// Serves a cached resolution.
	fn get(&self, key: &CacheKey) -> Option<RecordIdKey> {
		self.cache.get(key).map(|cached| cached.key.clone())
	}

	/// Caches one resolution, measuring the record key's encoded length
	/// as its weight. Called only on resolution misses, so the encode
	/// costs nothing on the hit path.
	fn insert(&self, key: CacheKey, record: &RecordIdKey) -> Result<()> {
		let weight = storekey::encode_vec(record).map_err(anyhow::Error::from_boxed)?.len();
		self.cache.insert(
			key,
			Arc::new(CachedKey {
				key: record.clone(),
				weight: u32::try_from(weight).unwrap_or(u32::MAX),
			}),
		);
		Ok(())
	}
}

/// Resolves `ids` in `table`'s shared doc-ID space, preserving order;
/// unmapped ids — deleted records — yield `None` in their slot.
///
/// Cache hits are served without a read. Misses go to the mapping at
/// `version`, and populate the cache only for current-time reads (see the
/// module contract). `table_id` keys the cache to the table's generation
/// and must come from the table definition in the caller's snapshot;
/// `None` — the table is not defined there — resolves uncached.
#[allow(clippy::too_many_arguments)]
pub async fn resolve_doc_ids(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	table_id: Option<TableId>,
	ids: &[DocId],
	version: Option<u64>,
	cache: Option<&AdjacencyResolveCache>,
) -> Result<Vec<Option<RecordIdKey>>> {
	let cache = match (cache, table_id) {
		(Some(cache), Some(table_id)) => Some((cache, table_id)),
		_ => None,
	};
	let mut out: Vec<Option<RecordIdKey>> = vec![None; ids.len()];
	let mut missing: Vec<DocId> = Vec::new();
	let mut missing_slots: Vec<usize> = Vec::new();
	for (slot, &id) in ids.iter().enumerate() {
		let cached = cache.and_then(|(c, table_id)| c.get(&(ns, db, table_id, id)));
		match cached {
			Some(key) => out[slot] = Some(key),
			None => {
				missing.push(id);
				missing_slots.push(slot);
			}
		}
	}
	if missing.is_empty() {
		return Ok(out);
	}
	let resolved = TableDocIds::new(ns, db, table.clone())
		.get_record_ids_batch_at(txn, &missing, version)
		.await?;
	for ((slot, id), key) in missing_slots.into_iter().zip(missing).zip(resolved) {
		if let Some(key) = key {
			if version.is_none()
				&& let Some((cache, table_id)) = cache
			{
				cache.insert((ns, db, table_id, id), &key)?;
			}
			out[slot] = Some(key);
		}
	}
	Ok(out)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::Array;

	/// The cache evicts strictly by weight, so every entry must be charged
	/// what it actually holds: a composite (array) key weighed as a flat
	/// constant would let a nominally small cache retain orders of
	/// magnitude more live bytes than its budget.
	#[test]
	fn composite_keys_are_weighed_by_their_real_size() {
		const BUDGET: u64 = 64 * 1024;
		let cache = AdjacencyResolveCache::new(BUDGET);
		// Each key holds ~4 KiB, so the budget fits only a handful; without
		// proportional weighing, all of them would be retained.
		for id in 0..200u64 {
			let record = RecordIdKey::Array(Array::from(vec![
				format!("{id:04}").repeat(1024 + id as usize),
			]));
			cache.insert((NamespaceId(1), DatabaseId(1), TableId(1), id), &record).unwrap();
		}
		assert!(
			cache.cache.weight() <= BUDGET,
			"the cache holds {} weighted bytes against a budget of {BUDGET}",
			cache.cache.weight(),
		);
		assert!(
			cache.cache.len() < 32,
			"a 64 KiB cache cannot hold {} ~4 KiB keys",
			cache.cache.len(),
		);
	}
}
