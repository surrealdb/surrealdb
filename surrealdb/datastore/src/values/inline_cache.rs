//! The values stored under inline adjacency and reference cache keys.
//!
//! A cache holds the **complete** set of one record's adjacency in one
//! direction (`!ce`), or of its incoming references (`!cr`), so a fan-out
//! across many source records becomes one batched multi-get instead of a
//! cursor per source. Completeness is the entire contract: a present,
//! non-spilled cache is authoritative — readers may emit its entries and
//! skip the scan — so every write path that adds or removes an entry
//! maintains the cache in the same transaction, and a cache that stops
//! being maintainable flips to [`Spilled`](CacheValue::Spilled) instead.
//!
//! Three states, told apart by presence and the flags byte:
//!
//! * **missing** — never materialised: readers fall back to the scan, the first eligible write
//!   backfills;
//! * **live** — the complete entry set, possibly empty (an authoritative "nothing here", which
//!   skips the scan outright);
//! * **spilled** — the record outgrew its cap, or an entry could not be represented: opaque
//!   forever, readers fall back, writers stop maintaining it. Spill is sticky — it is cleared only
//!   by deletion of the key (record purge or a cap change's invalidation), never by the count
//!   dropping back under the cap — so a hot record settles into one state instead of thrashing
//!   across the boundary.
//!
//! Entries are held as the storekey bytes of their identity tuples, kept
//! sorted, so a cache emits its entries in exactly the byte order the
//! corresponding key scan would — the correctness oracle for the whole
//! feature.

use anyhow::{Result, bail, ensure};
use surrealdb_kvs::key::Error as KeyError;
use surrealdb_kvs::value::KVValue;

/// Flags byte: the cache is opaque and readers must fall back.
const CACHE_FLAG_SPILLED: u8 = 1 << 0;

/// One cached set, generic over the entry type.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheValue<E> {
	/// The record outgrew the cache; the scan is authoritative.
	Spilled,
	/// The complete, sorted entry set.
	Live(Vec<E>),
}

impl<E> CacheValue<E> {
	pub fn is_spilled(&self) -> bool {
		matches!(self, CacheValue::Spilled)
	}
}

/// One cached adjacency entry, mirroring the payload of a vertex-side
/// pointer key: the edge record and the far vertex, each as the storekey
/// bytes of its `(TableName, RecordIdKey)` pair.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct EdgeCacheEntry {
	/// Storekey bytes of `(edge_table, edge_key)` — the entry's identity.
	pub edge: Vec<u8>,
	/// Storekey bytes of `(target_table, target_key)`.
	pub target: Vec<u8>,
}

/// One cached reference entry, mirroring the payload of a reference
/// back-link key.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RefCacheEntry {
	/// Storekey bytes of `(foreign_table, foreign_field, foreign_key)` —
	/// the entry's identity, in the reference keys' own field order.
	pub reference: Vec<u8>,
}

/// The value of an `!ce` inline adjacency cache key.
pub type EdgeCacheValue = CacheValue<EdgeCacheEntry>;

/// The value of a `!cr` inline reference cache key.
pub type RefCacheValue = CacheValue<RefCacheEntry>;

impl EdgeCacheValue {
	/// Insert-or-replace by identity, keeping the set sorted. Returns the
	/// live count, or `None` when the cache is spilled.
	pub fn upsert(&mut self, entry: EdgeCacheEntry) -> Option<usize> {
		let CacheValue::Live(entries) = self else {
			return None;
		};
		match entries.binary_search_by(|e| e.edge.cmp(&entry.edge)) {
			Ok(i) => entries[i] = entry,
			Err(i) => entries.insert(i, entry),
		}
		Some(entries.len())
	}

	/// Remove by identity. Spilled caches are untouched.
	pub fn remove(&mut self, edge: &[u8]) {
		if let CacheValue::Live(entries) = self
			&& let Ok(i) = entries.binary_search_by(|e| e.edge.as_slice().cmp(edge))
		{
			entries.remove(i);
		}
	}
}

impl RefCacheValue {
	/// Insert by identity, keeping the set sorted. Returns the live
	/// count, or `None` when the cache is spilled.
	pub fn upsert(&mut self, entry: RefCacheEntry) -> Option<usize> {
		let CacheValue::Live(entries) = self else {
			return None;
		};
		match entries.binary_search(&entry) {
			Ok(_) => {}
			Err(i) => entries.insert(i, entry),
		}
		Some(entries.len())
	}

	/// Remove by identity. Spilled caches are untouched.
	pub fn remove(&mut self, reference: &[u8]) {
		if let CacheValue::Live(entries) = self
			&& let Ok(i) = entries.binary_search_by(|e| e.reference.as_slice().cmp(reference))
		{
			entries.remove(i);
		}
	}
}

/// Encodes the flags byte followed by each segment, varint-length-prefixed,
/// borrowing every segment straight from the caller's entries. `size_hint`
/// pre-sizes the buffer: pass the summed segment lengths plus a small
/// per-segment allowance for the length prefixes (over-reservation is
/// harmless, a reallocation mid-encode is what it avoids).
fn encode_segments<'a>(
	spilled: bool,
	segments: impl Iterator<Item = &'a [u8]>,
	size_hint: usize,
) -> Vec<u8> {
	if spilled {
		return vec![CACHE_FLAG_SPILLED];
	}
	let mut buf = Vec::with_capacity(1 + size_hint);
	buf.push(0);
	for segment in segments {
		write_len(&mut buf, segment.len());
		buf.extend_from_slice(segment);
	}
	buf
}

fn write_len(buf: &mut Vec<u8>, mut value: usize) {
	loop {
		let byte = (value & 0x7f) as u8;
		value >>= 7;
		if value == 0 {
			buf.push(byte);
			return;
		}
		buf.push(byte | 0x80);
	}
}

fn read_segment<'a>(bytes: &mut &'a [u8]) -> Option<&'a [u8]> {
	let mut len = 0usize;
	let mut shift = 0u32;
	loop {
		let (&byte, rest) = bytes.split_first()?;
		*bytes = rest;
		len |= ((byte & 0x7f) as usize) << shift;
		if byte & 0x80 == 0 {
			break;
		}
		shift += 7;
		if shift > 28 {
			return None;
		}
	}
	if bytes.len() < len {
		return None;
	}
	let (segment, rest) = bytes.split_at(len);
	*bytes = rest;
	Some(segment)
}

fn decode_flags(bytes: &[u8]) -> Result<Option<&[u8]>> {
	let Some((&flags, rest)) = bytes.split_first() else {
		bail!(KeyError::Corrupted("an inline cache value cannot be empty"));
	};
	ensure!(
		flags & !CACHE_FLAG_SPILLED == 0,
		KeyError::Corrupted("inline cache value carries unknown flags")
	);
	if flags & CACHE_FLAG_SPILLED != 0 {
		ensure!(
			rest.is_empty(),
			KeyError::Corrupted("a spilled inline cache must carry no entries")
		);
		return Ok(None);
	}
	Ok(Some(rest))
}

impl KVValue for EdgeCacheValue {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(match self {
			CacheValue::Spilled => encode_segments(true, std::iter::empty(), 0),
			CacheValue::Live(entries) => encode_segments(
				false,
				entries.iter().flat_map(|e| [e.edge.as_slice(), e.target.as_slice()]),
				entries.iter().map(|e| e.edge.len() + e.target.len() + 6).sum(),
			),
		})
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		let Some(mut rest) = decode_flags(bytes)? else {
			return Ok(CacheValue::Spilled);
		};
		let corrupted = || KeyError::Corrupted("an inline adjacency cache value is truncated");
		let mut entries = Vec::new();
		while !rest.is_empty() {
			let edge = read_segment(&mut rest).ok_or_else(corrupted)?.to_vec();
			let target = read_segment(&mut rest).ok_or_else(corrupted)?.to_vec();
			entries.push(EdgeCacheEntry {
				edge,
				target,
			});
		}
		Ok(CacheValue::Live(entries))
	}
}

impl KVValue for RefCacheValue {
	type KeyContext = ();

	fn kv_encode_value(&self) -> Result<Vec<u8>> {
		Ok(match self {
			CacheValue::Spilled => encode_segments(true, std::iter::empty(), 0),
			CacheValue::Live(entries) => encode_segments(
				false,
				entries.iter().map(|e| e.reference.as_slice()),
				entries.iter().map(|e| e.reference.len() + 3).sum(),
			),
		})
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> Result<Self> {
		let Some(mut rest) = decode_flags(bytes)? else {
			return Ok(CacheValue::Spilled);
		};
		let corrupted = || KeyError::Corrupted("an inline reference cache value is truncated");
		let mut entries = Vec::new();
		while !rest.is_empty() {
			let reference = read_segment(&mut rest).ok_or_else(corrupted)?.to_vec();
			entries.push(RefCacheEntry {
				reference,
			});
		}
		Ok(CacheValue::Live(entries))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn edge(edge: &[u8], target: &[u8]) -> EdgeCacheEntry {
		EdgeCacheEntry {
			edge: edge.to_vec(),
			target: target.to_vec(),
		}
	}

	#[test]
	fn edge_cache_round_trips() {
		for value in [
			EdgeCacheValue::Spilled,
			EdgeCacheValue::Live(Vec::new()),
			EdgeCacheValue::Live(vec![edge(b"a", b"x"), edge(b"b", b"")]),
		] {
			let bytes = value.kv_encode_value().unwrap();
			assert_eq!(EdgeCacheValue::kv_decode_value(&bytes, ()).unwrap(), value);
		}
	}

	#[test]
	fn ref_cache_round_trips() {
		for value in [
			RefCacheValue::Spilled,
			RefCacheValue::Live(Vec::new()),
			RefCacheValue::Live(vec![
				RefCacheEntry {
					reference: b"r1".to_vec(),
				},
				RefCacheEntry {
					reference: b"r2".to_vec(),
				},
			]),
		] {
			let bytes = value.kv_encode_value().unwrap();
			assert_eq!(RefCacheValue::kv_decode_value(&bytes, ()).unwrap(), value);
		}
	}

	/// The wire format, pinned byte for byte: a zero flags byte, then each
	/// entry's segments in order, each prefixed by its varint length — and
	/// for a spilled value, the spill flag alone. Any encode change that
	/// alters the bytes must fail here, because values already on disk
	/// decode under this exact layout.
	#[test]
	fn encoded_bytes_are_flag_then_length_prefixed_segments() {
		assert_eq!(
			EdgeCacheValue::Live(vec![edge(b"ab", b"xyz")]).kv_encode_value().unwrap(),
			vec![0, 2, b'a', b'b', 3, b'x', b'y', b'z'],
		);
		assert_eq!(
			RefCacheValue::Live(vec![RefCacheEntry {
				reference: b"r1".to_vec(),
			}])
			.kv_encode_value()
			.unwrap(),
			vec![0, 2, b'r', b'1'],
		);
		assert_eq!(EdgeCacheValue::Spilled.kv_encode_value().unwrap(), vec![CACHE_FLAG_SPILLED]);
		assert_eq!(RefCacheValue::Spilled.kv_encode_value().unwrap(), vec![CACHE_FLAG_SPILLED]);
	}

	/// Missing and empty are different states: an empty live cache is an
	/// authoritative "no entries", so it must encode to something a
	/// decoder can tell apart from an absent key — and zero bytes is what
	/// an absent key looks like to a multi-get.
	#[test]
	fn empty_live_is_not_empty_bytes() {
		let bytes = EdgeCacheValue::Live(Vec::new()).kv_encode_value().unwrap();
		assert!(!bytes.is_empty());
		assert!(EdgeCacheValue::kv_decode_value(&[], ()).is_err());
	}

	#[test]
	fn upsert_keeps_order_and_replaces_by_identity() {
		let mut cache = EdgeCacheValue::Live(Vec::new());
		assert_eq!(cache.upsert(edge(b"b", b"x")), Some(1));
		assert_eq!(cache.upsert(edge(b"a", b"y")), Some(2));
		// Same identity, new target: replaced, not duplicated.
		assert_eq!(cache.upsert(edge(b"b", b"z")), Some(2));
		let CacheValue::Live(entries) = &cache else {
			panic!("still live");
		};
		assert_eq!(entries, &[edge(b"a", b"y"), edge(b"b", b"z")]);
		cache.remove(b"a");
		let CacheValue::Live(entries) = &cache else {
			panic!("still live");
		};
		assert_eq!(entries, &[edge(b"b", b"z")]);
		// Spilled caches ignore maintenance.
		let mut spilled = EdgeCacheValue::Spilled;
		assert_eq!(spilled.upsert(edge(b"a", b"y")), None);
		spilled.remove(b"a");
		assert!(spilled.is_spilled());
	}

	#[test]
	fn malformed_values_are_rejected() {
		// Unknown flag bits.
		assert!(EdgeCacheValue::kv_decode_value(&[1 << 3], ()).is_err());
		// A spilled value carrying trailing bytes.
		assert!(EdgeCacheValue::kv_decode_value(&[CACHE_FLAG_SPILLED, 1], ()).is_err());
		// A segment length pointing past the end.
		assert!(EdgeCacheValue::kv_decode_value(&[0, 200, 1, 2], ()).is_err());
		// An edge segment with its target segment missing.
		assert!(EdgeCacheValue::kv_decode_value(&[0, 1, b'a'], ()).is_err());
	}
}
