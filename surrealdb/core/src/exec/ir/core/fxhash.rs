//! A fast, non-cryptographic hasher for maps and sets keyed by internal IR ids.
//!
//! This is the rustc "Fx" hash: one rotate, one xor and one multiply per word. It has no random
//! seed and no resistance to adversarial keys.
//!
//! **Use it only** for maps and sets keyed by internal IR/arena ids (`OpId`, `ValueId`,
//! `RegionId`, `BlockId`, `FrameId`, `SnapshotId`, slot ids) and small tuples of those, whose
//! values the planner allocates and a user cannot choose. **Never** use it for a key derived from
//! user input (strings, `Value`s, field or table names): those stay on std's SipHash, which is
//! what makes hash-flooding them impractical.
//!
//! The hash is deterministic across runs, but no caller may depend on iteration order: a map's
//! order depends on the table layout, not on the keys.

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};

const SEED: u64 = 0x517c_c1b7_2722_0a95;

/// The Fx hasher state.
#[derive(Clone, Copy, Default)]
pub(crate) struct FxHasher {
	hash: u64,
}

impl FxHasher {
	#[inline]
	fn add(&mut self, word: u64) {
		self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(SEED);
	}
}

impl Hasher for FxHasher {
	#[inline]
	fn write(&mut self, bytes: &[u8]) {
		let mut chunks = bytes.chunks_exact(8);
		for chunk in &mut chunks {
			let mut word = [0u8; 8];
			word.copy_from_slice(chunk);
			self.add(u64::from_le_bytes(word));
		}
		// The tail is added a byte at a time, so slices that differ only by trailing zeros hash
		// apart.
		for &byte in chunks.remainder() {
			self.add(u64::from(byte));
		}
	}

	#[inline]
	fn write_u8(&mut self, i: u8) {
		self.add(u64::from(i));
	}

	#[inline]
	fn write_u16(&mut self, i: u16) {
		self.add(u64::from(i));
	}

	#[inline]
	fn write_u32(&mut self, i: u32) {
		self.add(u64::from(i));
	}

	#[inline]
	fn write_u64(&mut self, i: u64) {
		self.add(i);
	}

	#[inline]
	fn write_usize(&mut self, i: usize) {
		self.add(i as u64);
	}

	#[inline]
	fn finish(&self) -> u64 {
		self.hash
	}
}

/// The builder for [`FxHasher`]; stateless, so every map built with it hashes alike.
pub(crate) type FxBuildHasher = BuildHasherDefault<FxHasher>;

/// A `HashMap` keyed by internal ids. See the module rule on which keys are allowed.
pub(crate) type FxHashMap<K, V> = HashMap<K, V, FxBuildHasher>;

/// A `HashSet` of internal ids. See the module rule on which keys are allowed.
pub(crate) type FxHashSet<T> = HashSet<T, FxBuildHasher>;

/// An empty [`FxHashMap`] with room for `capacity` entries.
#[inline]
pub(crate) fn map_with_capacity<K, V>(capacity: usize) -> FxHashMap<K, V> {
	HashMap::with_capacity_and_hasher(capacity, FxBuildHasher::default())
}

/// An empty [`FxHashSet`] with room for `capacity` entries.
#[inline]
pub(crate) fn set_with_capacity<T>(capacity: usize) -> FxHashSet<T> {
	HashSet::with_capacity_and_hasher(capacity, FxBuildHasher::default())
}

#[cfg(test)]
mod tests {
	use std::hash::{BuildHasher, Hash};

	use super::*;

	fn hash_of<T: Hash>(value: &T) -> u64 {
		FxBuildHasher::default().hash_one(value)
	}

	#[test]
	fn hashing_is_deterministic() {
		assert_eq!(hash_of(&(7u32, 3u16)), hash_of(&(7u32, 3u16)));
		let mut a = FxHasher::default();
		let mut b = FxHasher::default();
		a.write(b"abcdefghijk");
		b.write(b"abcdefghijk");
		assert_eq!(a.finish(), b.finish());
	}

	#[test]
	fn distinct_small_ids_hash_apart() {
		let mut seen = HashSet::new();
		for id in 0u32..4096 {
			assert!(seen.insert(hash_of(&id)), "collision at {id}");
		}
		assert_ne!(hash_of(&(1u32, 2u16)), hash_of(&(2u32, 1u16)));
		assert_ne!(hash_of(&0u32), hash_of(&1u32));
	}

	#[test]
	fn write_covers_chunks_and_tail() {
		let mut a = FxHasher::default();
		let mut b = FxHasher::default();
		a.write(b"123456789");
		b.write(b"12345678a");
		assert_ne!(a.finish(), b.finish());
		let mut short = FxHasher::default();
		let mut padded = FxHasher::default();
		short.write(b"A");
		padded.write(b"A\0");
		assert_ne!(short.finish(), padded.finish());
	}

	#[test]
	fn map_round_trip() {
		let mut map: FxHashMap<u32, u32> = map_with_capacity(8);
		for id in 0..1000 {
			map.insert(id, id * 2);
		}
		for id in 0..1000 {
			assert_eq!(map.get(&id), Some(&(id * 2)));
		}
		assert_eq!(map.remove(&5), Some(10));
		assert!(!map.contains_key(&5));
		let mut set: FxHashSet<(u32, u16)> = set_with_capacity(4);
		assert!(set.insert((1, 2)));
		assert!(!set.insert((1, 2)));
		assert!(set.insert((2, 1)));
	}
}
