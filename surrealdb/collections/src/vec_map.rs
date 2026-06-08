//! Ordered map backed by a sorted `Vec` of `(K, V)` entries.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::iter::FromIterator;
use std::ops::Index;

use storekey::{BorrowDecode, BorrowReader, DecodeError, Encode, EncodeError, Writer};

use crate::search::search_sorted_by;

/// Map with unique keys in ascending `Ord` order, stored in a `Vec`.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct VecMap<K, V> {
	entries: Vec<(K, V)>,
}

impl<K, V> Default for VecMap<K, V> {
	fn default() -> Self {
		Self {
			entries: Vec::new(),
		}
	}
}

impl<K: PartialEq, V: PartialEq> PartialEq for VecMap<K, V> {
	fn eq(&self, other: &Self) -> bool {
		self.entries == other.entries
	}
}

impl<K: Eq, V: Eq> Eq for VecMap<K, V> {}

impl<K: Hash, V: Hash> Hash for VecMap<K, V> {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.entries.hash(state);
	}
}

impl<K: Ord, V: Ord> PartialOrd for VecMap<K, V> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl<K: Ord, V: Ord> Ord for VecMap<K, V> {
	fn cmp(&self, other: &Self) -> Ordering {
		self.entries.cmp(&other.entries)
	}
}

impl<K, V> VecMap<K, V> {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	/// Creates a new `VecMap` with space preallocated for `capacity` entries.
	#[must_use]
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			entries: Vec::with_capacity(capacity),
		}
	}

	/// Build a `VecMap` from a `Vec` whose entries are already sorted by key
	/// and contain no duplicate keys.
	///
	/// The invariant is checked with a `debug_assert!` and is **not** enforced
	/// in release builds; callers must guarantee it. Use this when converting
	/// from an already-ordered source (e.g. `BTreeMap`, revision-decoded
	/// payloads, `PublicObject`) to avoid re-sorting.
	#[must_use]
	pub fn from_sorted_vec_unchecked(entries: Vec<(K, V)>) -> Self
	where
		K: Ord,
	{
		debug_assert!(
			entries.windows(2).all(|w| w[0].0 < w[1].0),
			"VecMap::from_sorted_vec_unchecked: entries not strictly sorted by key"
		);
		Self {
			entries,
		}
	}

	#[must_use]
	pub fn len(&self) -> usize {
		self.entries.len()
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn clear(&mut self) {
		self.entries.clear();
	}

	#[must_use]
	pub fn get<Q: ?Sized + Ord>(&self, key: &Q) -> Option<&V>
	where
		K: Borrow<Q>,
	{
		let i = search_sorted_by(&self.entries, |(k, _)| k.borrow().cmp(key)).ok()?;
		Some(&self.entries[i].1)
	}

	#[must_use]
	pub fn get_mut<Q: ?Sized + Ord>(&mut self, key: &Q) -> Option<&mut V>
	where
		K: Borrow<Q>,
	{
		let i = search_sorted_by(&self.entries, |(k, _)| k.borrow().cmp(key)).ok()?;
		Some(&mut self.entries[i].1)
	}

	#[must_use]
	pub fn contains_key<Q: ?Sized + Ord>(&self, key: &Q) -> bool
	where
		K: Borrow<Q>,
	{
		self.get(key).is_some()
	}

	pub fn insert(&mut self, key: K, value: V) -> Option<V>
	where
		K: Ord,
	{
		match search_sorted_by(&self.entries, |(k, _)| k.cmp(&key)) {
			Ok(i) => Some(std::mem::replace(&mut self.entries[i].1, value)),
			Err(i) => {
				self.entries.insert(i, (key, value));
				None
			}
		}
	}

	pub fn remove<Q: ?Sized + Ord>(&mut self, key: &Q) -> Option<V>
	where
		K: Borrow<Q>,
	{
		let i = search_sorted_by(&self.entries, |(k, _)| k.borrow().cmp(key)).ok()?;
		Some(self.entries.remove(i).1)
	}

	pub fn retain<F>(&mut self, mut f: F)
	where
		F: FnMut(&K, &mut V) -> bool,
	{
		self.entries.retain_mut(|(k, v)| f(k, v));
	}

	/// Moves all elements from `other` into `self`, leaving `other` empty.
	///
	/// This matches [`BTreeMap::append`]: both maps must already be sorted by key with no
	/// duplicates within each map. If the same key appears in both, the value from `other`
	/// replaces the value in `self` (the key slot from `self` is dropped, as with
	/// [`BTreeMap::insert`]).
	pub fn append(&mut self, other: &mut Self)
	where
		K: Ord,
	{
		if other.is_empty() {
			return;
		}
		if self.is_empty() {
			std::mem::swap(self, other);
			return;
		}
		let can_concat =
			self.entries.last().zip(other.entries.first()).is_some_and(|(a, b)| a.0 < b.0);
		if can_concat {
			self.entries.append(&mut other.entries);
			return;
		}
		*self = Self::merge_sorted_prefer_rhs(
			Self {
				entries: std::mem::take(&mut self.entries),
			},
			Self {
				entries: std::mem::take(&mut other.entries),
			},
		);
	}

	/// Linear merge of two maps whose keys are sorted ascending in each.
	///
	/// If the same key appears in both maps, the value from `rhs` is kept (same as
	/// repeatedly inserting each entry from `rhs` into a copy of `lhs`).
	#[must_use]
	pub fn merge_sorted_prefer_rhs(lhs: Self, rhs: Self) -> Self
	where
		K: Ord,
	{
		let reserve = lhs.entries.len() + rhs.entries.len();
		let mut a = lhs.entries.into_iter().peekable();
		let mut b = rhs.entries.into_iter().peekable();
		let mut out = Vec::with_capacity(reserve);
		loop {
			match (a.peek(), b.peek()) {
				(None, None) => break,
				(Some(_), None) => {
					out.extend(a);
					break;
				}
				(None, Some(_)) => {
					out.extend(b);
					break;
				}
				(Some((ka, _)), Some((kb, _))) => match ka.cmp(kb) {
					Ordering::Less => {
						out.push(
							a.next().expect("merge_sorted_prefer_rhs: iterator lagged behind peek"),
						);
					}
					Ordering::Greater => {
						out.push(
							b.next().expect("merge_sorted_prefer_rhs: iterator lagged behind peek"),
						);
					}
					Ordering::Equal => {
						a.next()
							.expect("merge_sorted_prefer_rhs: iterator lagged behind peek (lhs)");
						out.push(
							b.next().expect(
								"merge_sorted_prefer_rhs: iterator lagged behind peek (rhs)",
							),
						);
					}
				},
			}
		}
		Self::from_sorted_vec_unchecked(out)
	}

	/// Appends `(key, value)` in **O(1)** amortized time without searching.
	///
	/// # Ordering
	///
	/// `key` must be **strictly greater** than every existing key (or the map
	/// must be empty). Violating this breaks sorted-map invariants; it is checked
	/// with [`debug_assert!`] only (see [`append`](Self::append)).
	pub fn push(&mut self, key: K, value: V)
	where
		K: Ord,
	{
		debug_assert!(
			self.entries.last().map(|(k, _)| k < &key).unwrap_or(true),
			"VecMap::push: key must be strictly greater than the current maximum key"
		);
		self.entries.push((key, value));
	}

	pub fn iter(&self) -> Iter<'_, K, V> {
		Iter {
			inner: self.entries.iter(),
		}
	}

	pub fn iter_mut(&mut self) -> IterMut<'_, K, V> {
		IterMut {
			inner: self.entries.iter_mut(),
		}
	}

	pub fn keys(&self) -> impl Iterator<Item = &K> + '_ {
		self.entries.iter().map(|(k, _)| k)
	}

	pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
		self.entries.iter().map(|(_, v)| v)
	}

	pub fn values_mut(&mut self) -> impl Iterator<Item = &mut V> + '_ {
		self.entries.iter_mut().map(|(_, v)| v)
	}

	#[must_use]
	pub fn first_key_value(&self) -> Option<(&K, &V)> {
		self.entries.first().map(|(k, v)| (k, v))
	}

	#[must_use]
	pub fn last_key_value(&self) -> Option<(&K, &V)> {
		self.entries.last().map(|(k, v)| (k, v))
	}

	pub fn entry(&mut self, key: K) -> Entry<'_, K, V>
	where
		K: Ord,
	{
		match search_sorted_by(&self.entries, |(k, _)| k.cmp(&key)) {
			Ok(i) => Entry::Occupied(OccupiedEntry {
				entries: &mut self.entries,
				index: i,
			}),
			Err(i) => Entry::Vacant(VacantEntry {
				entries: &mut self.entries,
				key,
				index: i,
			}),
		}
	}
}

impl<K: Ord, V> Index<&K> for VecMap<K, V> {
	type Output = V;

	fn index(&self, key: &K) -> &Self::Output {
		self.get(key).expect("VecMap: index out of bounds")
	}
}

impl<'a, K, V> IntoIterator for &'a VecMap<K, V> {
	type Item = (&'a K, &'a V);
	type IntoIter = Iter<'a, K, V>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

pub struct Iter<'a, K, V> {
	inner: std::slice::Iter<'a, (K, V)>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
	type Item = (&'a K, &'a V);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|(k, v)| (k, v))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.inner.size_hint()
	}
}

impl<'a, K, V> ExactSizeIterator for Iter<'a, K, V> {
	fn len(&self) -> usize {
		self.inner.len()
	}
}

impl<'a, K, V> Clone for Iter<'a, K, V> {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

pub struct IterMut<'a, K, V> {
	inner: std::slice::IterMut<'a, (K, V)>,
}

impl<'a, K, V> Iterator for IterMut<'a, K, V> {
	type Item = (&'a K, &'a mut V);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next().map(|(k, v)| (&*k, v))
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.inner.size_hint()
	}
}

pub struct IntoIter<K, V> {
	inner: std::vec::IntoIter<(K, V)>,
}

impl<K, V> Iterator for IntoIter<K, V> {
	type Item = (K, V);

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next()
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.inner.size_hint()
	}
}

impl<K, V> ExactSizeIterator for IntoIter<K, V> {
	fn len(&self) -> usize {
		self.inner.len()
	}
}

impl<K, V> VecMap<K, V> {
	/// Consumes the map and returns an iterator over the values in key order.
	pub fn into_values(self) -> impl Iterator<Item = V> {
		self.entries.into_iter().map(|(_, v)| v)
	}
}

impl<K, V> IntoIterator for VecMap<K, V> {
	type Item = (K, V);
	type IntoIter = IntoIter<K, V>;

	fn into_iter(self) -> Self::IntoIter {
		IntoIter {
			inner: self.entries.into_iter(),
		}
	}
}

impl<K: Ord, V> FromIterator<(K, V)> for VecMap<K, V> {
	fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
		let mut v: Vec<(K, V)> = iter.into_iter().collect();
		if v.is_empty() {
			return Self::default();
		}
		v.sort_by(|a, b| a.0.cmp(&b.0));
		// Stable sort preserves iterator order among equal keys. Reverse so the last
		// occurrence per key is first, dedup keeps the first of consecutive equals, then
		// reverse back to ascending order — matching repeated `insert` (last wins).
		v.reverse();
		v.dedup_by(|a, b| a.0 == b.0);
		v.reverse();
		Self::from_sorted_vec_unchecked(v)
	}
}

impl<K: Ord, V> Extend<(K, V)> for VecMap<K, V> {
	fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, iter: I) {
		if self.is_empty() {
			*self = iter.into_iter().collect();
			return;
		}
		for (k, v) in iter {
			self.insert(k, v);
		}
	}
}

impl<K: Ord, V> From<BTreeMap<K, V>> for VecMap<K, V> {
	fn from(map: BTreeMap<K, V>) -> Self {
		let mut entries = Vec::with_capacity(map.len());
		entries.extend(map);
		Self::from_sorted_vec_unchecked(entries)
	}
}

pub enum Entry<'a, K, V> {
	Occupied(OccupiedEntry<'a, K, V>),
	Vacant(VacantEntry<'a, K, V>),
}

impl<'a, K: Ord, V> Entry<'a, K, V> {
	pub fn or_insert_with<F: FnOnce() -> V>(self, default: F) -> &'a mut V {
		match self {
			Entry::Occupied(o) => o.into_mut(),
			Entry::Vacant(v) => v.insert(default()),
		}
	}
}

pub struct OccupiedEntry<'a, K, V> {
	entries: &'a mut Vec<(K, V)>,
	index: usize,
}

impl<'a, K, V> OccupiedEntry<'a, K, V> {
	pub fn into_mut(self) -> &'a mut V {
		&mut self.entries[self.index].1
	}
}

pub struct VacantEntry<'a, K, V> {
	entries: &'a mut Vec<(K, V)>,
	key: K,
	index: usize,
}

impl<'a, K, V> VacantEntry<'a, K, V> {
	pub fn insert(self, value: V) -> &'a mut V {
		self.entries.insert(self.index, (self.key, value));
		&mut self.entries[self.index].1
	}
}

impl<F, K: Encode<F>, V: Encode<F>> Encode<F> for VecMap<K, V> {
	fn encode<W: Write>(&self, w: &mut Writer<W>) -> Result<(), EncodeError> {
		for (k, v) in self.iter() {
			w.mark_terminator();
			k.encode(w)?;
			v.encode(w)?;
		}
		w.write_terminator()
	}
}

impl<'de, F, K: BorrowDecode<'de, F> + Ord, V: BorrowDecode<'de, F>> BorrowDecode<'de, F>
	for VecMap<K, V>
{
	/// Deserialises a storekey map as a sequence of key-value pairs, appending each with
	/// [`push`](VecMap::push).
	///
	/// **Wire contract:** Keys must appear in **strictly ascending** order (matching
	/// [`Encode`] for `VecMap` / `BTreeMap`). If a decoded key is less than or equal to the
	/// previous key, decoding fails with [`DecodeError::InvalidFormat`] in all builds.
	fn borrow_decode(r: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		let mut map = VecMap::new();
		while !r.read_terminal()? {
			let k = K::borrow_decode(r)?;
			if let Some((prev_k, _)) = map.last_key_value()
				&& k <= *prev_k
			{
				return Err(DecodeError::InvalidFormat);
			}
			let v = V::borrow_decode(r)?;
			map.push(k, v);
		}
		Ok(map)
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;
	use std::collections::BTreeMap;
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};

	use super::*;

	/// Asserts that `m`'s keys are in strictly ascending order.
	fn assert_sorted<K: Ord + std::fmt::Debug, V>(m: &VecMap<K, V>) {
		for (a, b) in m.keys().zip(m.keys().skip(1)) {
			assert!(a < b, "VecMap keys not strictly sorted: {a:?} >= {b:?}");
		}
	}

	fn hash_of<T: Hash>(t: &T) -> u64 {
		let mut h = DefaultHasher::new();
		t.hash(&mut h);
		h.finish()
	}

	#[test]
	fn new_and_default_are_empty() {
		let m: VecMap<i32, i32> = VecMap::new();
		assert!(m.is_empty());
		assert_eq!(m.len(), 0);
		let d: VecMap<i32, i32> = VecMap::default();
		assert_eq!(m, d);
	}

	#[test]
	fn with_capacity_starts_empty_and_works() {
		let mut m: VecMap<i32, i32> = VecMap::with_capacity(8);
		assert!(m.is_empty());
		for i in 0..8 {
			m.insert(i, i);
		}
		assert_eq!(m.len(), 8);
		assert_sorted(&m);
	}

	#[test]
	fn from_sorted_vec_unchecked_builds_correct_map() {
		let m: VecMap<i32, &str> =
			VecMap::from_sorted_vec_unchecked(vec![(1, "a"), (2, "b"), (3, "c")]);
		assert_eq!(m.len(), 3);
		assert_eq!(m.get(&2), Some(&"b"));
	}

	#[test]
	fn from_sorted_vec_unchecked_empty_is_ok() {
		let m: VecMap<i32, i32> = VecMap::from_sorted_vec_unchecked(vec![]);
		assert!(m.is_empty());
	}

	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "VecMap::from_sorted_vec_unchecked")]
	fn from_sorted_vec_unchecked_unsorted_panics_in_debug() {
		let _: VecMap<i32, i32> = VecMap::from_sorted_vec_unchecked(vec![(2, 0), (1, 0)]);
	}

	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "VecMap::from_sorted_vec_unchecked")]
	fn from_sorted_vec_unchecked_duplicate_keys_panics_in_debug() {
		let _: VecMap<i32, i32> = VecMap::from_sorted_vec_unchecked(vec![(1, 0), (1, 0)]);
	}

	#[test]
	fn clear_resets_to_empty() {
		let mut m: VecMap<i32, i32> = [(1, 10), (2, 20)].into_iter().collect();
		m.clear();
		assert!(m.is_empty());
		assert_eq!(m.len(), 0);
	}

	#[test]
	fn insert_returns_none_for_new_and_old_for_replace() {
		let mut m = VecMap::new();
		assert_eq!(m.insert("a", 1), None);
		assert_eq!(m.insert("b", 2), None);
		assert_eq!(m.insert("a", 10), Some(1));
		assert_eq!(m.get("a"), Some(&10));
		assert_eq!(m.len(), 2);
	}

	#[test]
	fn insert_keeps_entries_sorted_for_arbitrary_order() {
		let mut m = VecMap::new();
		for &k in &[5, 1, 4, 2, 3] {
			m.insert(k, k * 10);
		}
		assert_sorted(&m);
		let keys: Vec<_> = m.keys().copied().collect();
		assert_eq!(keys, vec![1, 2, 3, 4, 5]);
	}

	/// Cross the `LINEAR_SEARCH_THRESHOLD` (64) boundary so that
	/// `search_sorted_by` exercises the `binary_search_by` fallback during
	/// inserts in the middle and end of the map.
	#[test]
	fn insert_above_linear_threshold_keeps_sorted_and_replaces() {
		let mut m = VecMap::new();
		for i in 0..128u32 {
			m.insert(i * 2, i * 2);
		}
		for i in 0..128u32 {
			m.insert(i * 2 + 1, i * 2 + 1);
		}
		assert_eq!(m.len(), 256);
		assert_sorted(&m);
		for i in 0..256u32 {
			assert_eq!(m.get(&i), Some(&i));
		}
		assert_eq!(m.insert(150, 9_999), Some(150));
		assert_eq!(m.get(&150), Some(&9_999));
	}

	#[test]
	fn get_and_contains_key_with_borrow() {
		let m: VecMap<String, i32> =
			[("apple".to_string(), 1), ("banana".to_string(), 2)].into_iter().collect();
		assert_eq!(m.get("apple"), Some(&1));
		assert_eq!(m.get("missing"), None);
		assert!(m.contains_key("banana"));
		assert!(!m.contains_key("cherry"));
	}

	#[test]
	fn get_mut_allows_mutation_and_returns_none_for_missing() {
		let mut m: VecMap<i32, String> = [(1, "a".into()), (2, "b".into())].into_iter().collect();
		if let Some(v) = m.get_mut(&1) {
			v.push('!');
		}
		assert_eq!(m.get(&1), Some(&"a!".to_string()));
		assert!(m.get_mut(&99).is_none());
	}

	#[test]
	fn remove_returns_value_then_none_and_keeps_order() {
		let mut m: VecMap<i32, i32> = (0..10).map(|i| (i, i * 100)).collect();
		assert_eq!(m.remove(&3), Some(300));
		assert_eq!(m.remove(&3), None);
		assert_sorted(&m);
		let keys: Vec<_> = m.keys().copied().collect();
		assert_eq!(keys, vec![0, 1, 2, 4, 5, 6, 7, 8, 9]);
	}

	#[test]
	fn remove_with_borrow() {
		let mut m: VecMap<String, i32> =
			[("a".to_string(), 1), ("b".to_string(), 2)].into_iter().collect();
		assert_eq!(m.remove("a"), Some(1));
		assert_eq!(m.remove("missing"), None);
		assert_eq!(m.len(), 1);
	}

	#[test]
	fn retain_keeps_subset_in_order() {
		let mut m: VecMap<i32, i32> = (0..6).map(|i| (i, i * 10)).collect();
		m.retain(|&k, _| k % 2 == 0);
		assert_sorted(&m);
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(0, 0), (2, 20), (4, 40)]);
	}

	#[test]
	fn retain_can_mutate_values_in_place() {
		let mut m: VecMap<i32, i32> = (0..3).map(|i| (i, i)).collect();
		m.retain(|_, v| {
			*v += 100;
			true
		});
		let vs: Vec<_> = m.values().copied().collect();
		assert_eq!(vs, vec![100, 101, 102]);
	}

	#[test]
	fn retain_remove_all_or_keep_all() {
		let mut m: VecMap<i32, i32> = (0..5).map(|i| (i, i)).collect();
		let mut copy = m.clone();
		m.retain(|_, _| false);
		assert!(m.is_empty());
		copy.retain(|_, _| true);
		assert_eq!(copy.len(), 5);
	}

	#[test]
	fn append_moves_entries_and_empties_other() {
		let mut a: VecMap<i32, i32> = (0..3).map(|i| (i, i)).collect();
		let mut b: VecMap<i32, i32> = (3..6).map(|i| (i, i)).collect();
		a.append(&mut b);
		assert!(b.is_empty());
		assert_eq!(a.len(), 6);
		assert_sorted(&a);
	}

	#[test]
	fn append_with_one_or_both_empty() {
		let mut a: VecMap<i32, i32> = (0..3).map(|i| (i, i)).collect();
		let mut empty: VecMap<i32, i32> = VecMap::new();
		a.append(&mut empty);
		assert_eq!(a.len(), 3);
		assert!(empty.is_empty());

		let mut a: VecMap<i32, i32> = VecMap::new();
		let mut b: VecMap<i32, i32> = (0..3).map(|i| (i, i)).collect();
		a.append(&mut b);
		assert_eq!(a.len(), 3);
		assert!(b.is_empty());

		let mut a: VecMap<i32, i32> = VecMap::new();
		let mut b: VecMap<i32, i32> = VecMap::new();
		a.append(&mut b);
		assert!(a.is_empty());
		assert!(b.is_empty());
	}

	#[test]
	fn append_merges_when_other_is_entirely_before_self() {
		let mut a: VecMap<i32, i32> = (5..8).map(|i| (i, i)).collect();
		let mut b: VecMap<i32, i32> = (0..3).map(|i| (i, i)).collect();
		a.append(&mut b);
		assert!(b.is_empty());
		assert_eq!(a.len(), 6);
		assert_sorted(&a);
		let pairs: Vec<_> = a.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(0, 0), (1, 1), (2, 2), (5, 5), (6, 6), (7, 7)]);
	}

	#[test]
	fn append_overlapping_prefers_values_from_other_like_btreemap() {
		let mut a: VecMap<i32, i32> = [(1, 1), (2, 2), (3, 3)].into_iter().collect();
		let mut b: VecMap<i32, i32> = [(3, 30), (4, 4), (5, 5)].into_iter().collect();
		a.append(&mut b);
		assert!(b.is_empty());
		let pairs: Vec<_> = a.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, 1), (2, 2), (3, 30), (4, 4), (5, 5)]);
	}

	#[test]
	fn append_interleaved_keys_merges_sorted() {
		let mut a: VecMap<i32, i32> = [(1, 10), (3, 30), (5, 50)].into_iter().collect();
		let mut b: VecMap<i32, i32> = [(2, 20), (4, 40), (6, 60)].into_iter().collect();
		a.append(&mut b);
		assert!(b.is_empty());
		let pairs: Vec<_> = a.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)]);
	}

	#[test]
	fn append_full_overlap_all_keys_match() {
		let mut a: VecMap<i32, i32> = [(1, 1), (2, 2), (3, 3)].into_iter().collect();
		let mut b: VecMap<i32, i32> = [(1, 100), (2, 200), (3, 300)].into_iter().collect();
		a.append(&mut b);
		assert!(b.is_empty());
		let pairs: Vec<_> = a.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, 100), (2, 200), (3, 300)]);
	}

	#[test]
	fn append_other_is_subset_of_self() {
		let mut a: VecMap<i32, i32> =
			[(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)].into_iter().collect();
		let mut b: VecMap<i32, i32> = [(2, 200), (4, 400)].into_iter().collect();
		a.append(&mut b);
		assert!(b.is_empty());
		let pairs: Vec<_> = a.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, 1), (2, 200), (3, 3), (4, 400), (5, 5)]);
	}

	#[test]
	fn append_self_is_subset_of_other() {
		let mut a: VecMap<i32, i32> = [(2, 2), (4, 4)].into_iter().collect();
		let mut b: VecMap<i32, i32> =
			[(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)].into_iter().collect();
		a.append(&mut b);
		assert!(b.is_empty());
		let pairs: Vec<_> = a.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]);
	}

	/// Differential test against `BTreeMap::append`: across a battery of
	/// disjoint, overlapping, and subset scenarios — including inputs large
	/// enough to cross the linear/binary-search threshold — `VecMap::append`
	/// must produce the same key/value pairs in the same order as
	/// `BTreeMap::append`, and leave `other` empty in both cases.
	#[test]
	fn append_matches_btreemap_append_for_sample_inputs() {
		type Scenario = (Vec<(i32, i32)>, Vec<(i32, i32)>);
		let scenarios: Vec<Scenario> = vec![
			// disjoint, self < other (concat path)
			(vec![(1, 1), (2, 2)], vec![(3, 3), (4, 4)]),
			// disjoint, self > other (merge path)
			(vec![(5, 5), (6, 6)], vec![(1, 1), (2, 2)]),
			// interleaved disjoint
			(vec![(1, 10), (3, 30), (5, 50)], vec![(2, 20), (4, 40), (6, 60)]),
			// single-key overlap
			(vec![(1, 1), (2, 2), (3, 3)], vec![(3, 30), (4, 4), (5, 5)]),
			// fully overlapping
			(vec![(1, 1), (2, 2), (3, 3)], vec![(1, 100), (2, 200), (3, 300)]),
			// other is subset of self
			(vec![(1, 1), (2, 2), (3, 3), (4, 4), (5, 5)], vec![(2, 200), (4, 400)]),
			// self is subset of other
			(vec![(2, 2), (4, 4)], vec![(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]),
			// empty cases
			(vec![], vec![(1, 1), (2, 2)]),
			(vec![(1, 1), (2, 2)], vec![]),
			(vec![], vec![]),
			// large inputs that exceed LINEAR_SEARCH_THRESHOLD (64) on both sides;
			// every other key in `b` overlaps `a`, every alternate key extends past
			(
				(0..100i32).map(|i| (i * 2, i)).collect(),
				(0..100i32).map(|i| (i * 2 + 1, i + 10_000)).collect(),
			),
			(
				(0..100i32).map(|i| (i, i)).collect(),
				(50..150i32).map(|i| (i, i + 1_000_000)).collect(),
			),
		];

		for (i, (a_pairs, b_pairs)) in scenarios.into_iter().enumerate() {
			let mut vm_a: VecMap<i32, i32> = a_pairs.iter().copied().collect();
			let mut vm_b: VecMap<i32, i32> = b_pairs.iter().copied().collect();
			let mut bt_a: BTreeMap<i32, i32> = a_pairs.iter().copied().collect();
			let mut bt_b: BTreeMap<i32, i32> = b_pairs.iter().copied().collect();

			vm_a.append(&mut vm_b);
			bt_a.append(&mut bt_b);

			assert!(vm_b.is_empty(), "scenario {i}: VecMap other not emptied");
			assert!(bt_b.is_empty(), "scenario {i}: BTreeMap other not emptied");

			let vm_pairs: Vec<_> = vm_a.iter().map(|(k, v)| (*k, *v)).collect();
			let bt_pairs: Vec<_> = bt_a.iter().map(|(k, v)| (*k, *v)).collect();
			assert_eq!(vm_pairs, bt_pairs, "scenario {i}: VecMap diverges from BTreeMap");
			assert_sorted(&vm_a);
		}
	}

	#[test]
	fn merge_sorted_prefer_rhs_with_empty_inputs() {
		let empty: VecMap<i32, i32> = VecMap::new();
		assert!(VecMap::merge_sorted_prefer_rhs(empty.clone(), empty.clone()).is_empty());

		let m: VecMap<i32, i32> = (0..3).map(|i| (i, i)).collect();
		assert_eq!(VecMap::merge_sorted_prefer_rhs(m.clone(), empty.clone()), m);
		assert_eq!(VecMap::merge_sorted_prefer_rhs(empty, m.clone()), m);
	}

	#[test]
	fn merge_sorted_prefer_rhs_overlapping_uses_rhs_value() {
		let a: VecMap<&str, i32> =
			VecMap::from_sorted_vec_unchecked(vec![("a", 1), ("b", 2), ("c", 3)]);
		let b: VecMap<&str, i32> =
			VecMap::from_sorted_vec_unchecked(vec![("b", 22), ("c", 33), ("d", 4)]);
		let m = VecMap::merge_sorted_prefer_rhs(a, b);
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![("a", 1), ("b", 22), ("c", 33), ("d", 4)]);
	}

	/// Drains the longer side into `out` once the shorter side is exhausted.
	#[test]
	fn merge_sorted_prefer_rhs_lhs_longer_then_rhs_longer() {
		let a: VecMap<&str, i32> =
			VecMap::from_sorted_vec_unchecked(vec![("a", 1), ("b", 2), ("c", 3)]);
		let b: VecMap<&str, i32> = VecMap::from_sorted_vec_unchecked(vec![("a", 11)]);
		let m = VecMap::merge_sorted_prefer_rhs(a, b);
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![("a", 11), ("b", 2), ("c", 3)]);

		let a: VecMap<&str, i32> = VecMap::from_sorted_vec_unchecked(vec![("a", 1)]);
		let b: VecMap<&str, i32> =
			VecMap::from_sorted_vec_unchecked(vec![("b", 2), ("c", 3), ("d", 4)]);
		let m = VecMap::merge_sorted_prefer_rhs(a, b);
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![("a", 1), ("b", 2), ("c", 3), ("d", 4)]);
	}

	#[test]
	fn push_appends_in_order() {
		let mut m = VecMap::new();
		m.push(1, "a");
		m.push(2, "b");
		m.push(3, "c");
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, "a"), (2, "b"), (3, "c")]);
	}

	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "VecMap::push")]
	fn push_out_of_order_panics_in_debug() {
		let mut m = VecMap::new();
		m.push(2, ());
		m.push(1, ());
	}

	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "VecMap::push")]
	fn push_equal_key_panics_in_debug() {
		let mut m = VecMap::new();
		m.push(1, ());
		m.push(1, ());
	}

	#[test]
	fn iter_yields_keys_in_order() {
		let m: VecMap<i32, i32> = [(2, 20), (1, 10), (3, 30)].into_iter().collect();
		let keys: Vec<_> = m.iter().map(|(k, _)| *k).collect();
		assert_eq!(keys, vec![1, 2, 3]);
	}

	#[test]
	fn iter_size_hint_and_len() {
		let m: VecMap<i32, i32> = (0..5).map(|i| (i, i)).collect();
		let mut it = m.iter();
		assert_eq!(it.size_hint(), (5, Some(5)));
		assert_eq!(it.len(), 5);
		it.next();
		assert_eq!(it.size_hint(), (4, Some(4)));
		assert_eq!(it.len(), 4);
	}

	#[test]
	fn iter_clone_is_independent() {
		let m: VecMap<i32, i32> = (0..3).map(|i| (i, i)).collect();
		let mut a = m.iter();
		let mut b = a.clone();
		assert_eq!(a.next().map(|(k, _)| *k), Some(0));
		assert_eq!(b.next().map(|(k, _)| *k), Some(0));
		assert_eq!(a.next().map(|(k, _)| *k), Some(1));
		assert_eq!(b.next().map(|(k, _)| *k), Some(1));
	}

	#[test]
	fn iter_mut_allows_value_mutation_and_reports_size() {
		let mut m: VecMap<i32, i32> = (0..3).map(|i| (i, i)).collect();
		{
			let it = m.iter_mut();
			assert_eq!(it.size_hint(), (3, Some(3)));
		}
		for (_, v) in m.iter_mut() {
			*v += 100;
		}
		let vs: Vec<_> = m.values().copied().collect();
		assert_eq!(vs, vec![100, 101, 102]);
	}

	#[test]
	fn keys_values_values_mut_in_order() {
		let mut m: VecMap<&str, i32> = [("a", 1), ("b", 2)].into_iter().collect();
		assert_eq!(m.keys().copied().collect::<Vec<_>>(), vec!["a", "b"]);
		assert_eq!(m.values().copied().collect::<Vec<_>>(), vec![1, 2]);
		for v in m.values_mut() {
			*v *= 10;
		}
		assert_eq!(m.values().copied().collect::<Vec<_>>(), vec![10, 20]);
	}

	#[test]
	fn first_and_last_key_value() {
		let m: VecMap<i32, &str> = VecMap::new();
		assert_eq!(m.first_key_value(), None);
		assert_eq!(m.last_key_value(), None);

		let m: VecMap<i32, &str> =
			VecMap::from_sorted_vec_unchecked(vec![(1, "a"), (2, "b"), (3, "c")]);
		assert_eq!(m.first_key_value(), Some((&1, &"a")));
		assert_eq!(m.last_key_value(), Some((&3, &"c")));
	}

	#[test]
	fn entry_or_insert_with_for_vacant_then_occupied() {
		let mut m: VecMap<&str, i32> = VecMap::new();
		*m.entry("a").or_insert_with(|| 1) += 10;
		*m.entry("a").or_insert_with(|| 999) += 1;
		assert_eq!(m.get("a"), Some(&12));
		m.entry("c").or_insert_with(|| 3);
		m.entry("b").or_insert_with(|| 2);
		assert_sorted(&m);
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![("a", 12), ("b", 2), ("c", 3)]);
	}

	#[test]
	fn index_returns_value_for_present_key() {
		let m: VecMap<i32, &str> = VecMap::from_sorted_vec_unchecked(vec![(1, "a"), (2, "b")]);
		assert_eq!(m[&1], "a");
		assert_eq!(m[&2], "b");
	}

	#[test]
	#[should_panic(expected = "VecMap: index out of bounds")]
	fn index_missing_key_panics() {
		let m: VecMap<i32, i32> = VecMap::new();
		let _ = m[&1];
	}

	#[test]
	fn into_iter_owned_yields_in_order() {
		let m: VecMap<i32, i32> = (0..3).map(|i| (i, i * 10)).collect();
		let pairs: Vec<_> = m.into_iter().collect();
		assert_eq!(pairs, vec![(0, 0), (1, 10), (2, 20)]);
	}

	#[test]
	fn into_iter_owned_size_hint_and_len() {
		let m: VecMap<i32, i32> = (0..4).map(|i| (i, i)).collect();
		let mut it = m.into_iter();
		assert_eq!(it.size_hint(), (4, Some(4)));
		assert_eq!(it.len(), 4);
		it.next();
		assert_eq!(it.len(), 3);
	}

	#[test]
	fn into_iter_borrowed_yields_in_order() {
		let m: VecMap<i32, i32> = (0..3).map(|i| (i, i * 10)).collect();
		let pairs: Vec<_> = (&m).into_iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(0, 0), (1, 10), (2, 20)]);
	}

	#[test]
	fn into_values_yields_in_key_order() {
		let m: VecMap<i32, &str> =
			VecMap::from_sorted_vec_unchecked(vec![(1, "a"), (2, "b"), (3, "c")]);
		let values: Vec<_> = m.into_values().collect();
		assert_eq!(values, vec!["a", "b", "c"]);
	}

	#[test]
	fn from_iter_dedups_unsorted_input_keeping_last() {
		let m: VecMap<i32, &str> =
			vec![(2, "x"), (1, "a"), (2, "b"), (3, "c"), (2, "z")].into_iter().collect();
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, "a"), (2, "z"), (3, "c")]);
	}

	#[test]
	fn from_iter_empty_returns_default() {
		let m: VecMap<i32, i32> = std::iter::empty().collect();
		assert!(m.is_empty());
	}

	/// `extend` into an empty map goes through the `FromIterator` fast path.
	#[test]
	fn extend_into_empty_uses_from_iter_path_and_dedups() {
		let mut m: VecMap<i32, i32> = VecMap::new();
		m.extend([(2, 20), (1, 10), (2, 200)]);
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, 10), (2, 200)]);
	}

	/// `extend` into a non-empty map uses repeated `insert`.
	#[test]
	fn extend_into_non_empty_uses_insert_path() {
		let mut m: VecMap<i32, i32> = [(1, 1)].into_iter().collect();
		m.extend([(0, 0), (1, 99), (2, 2)]);
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(0, 0), (1, 99), (2, 2)]);
		assert_sorted(&m);
	}

	#[test]
	fn from_btreemap_preserves_order() {
		let mut bt = BTreeMap::new();
		bt.insert(1, "a");
		bt.insert(3, "c");
		bt.insert(2, "b");
		let m: VecMap<i32, &str> = VecMap::from(bt);
		let pairs: Vec<_> = m.iter().map(|(k, v)| (*k, *v)).collect();
		assert_eq!(pairs, vec![(1, "a"), (2, "b"), (3, "c")]);
	}

	#[test]
	fn equality_and_ord_are_lexicographic() {
		let a: VecMap<i32, i32> = [(1, 1), (2, 2)].into_iter().collect();
		let b: VecMap<i32, i32> = [(1, 1), (2, 3)].into_iter().collect();
		let c: VecMap<i32, i32> = [(1, 1), (2, 2)].into_iter().collect();
		assert_eq!(a, c);
		assert!(a < b);
		assert_eq!(a.cmp(&c), Ordering::Equal);
		assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));
	}

	#[test]
	fn hash_matches_for_equal_maps_built_in_different_order() {
		let a: VecMap<i32, i32> = [(1, 1), (2, 2), (3, 3)].into_iter().collect();
		let b: VecMap<i32, i32> = [(3, 3), (1, 1), (2, 2)].into_iter().collect();
		assert_eq!(a, b);
		assert_eq!(hash_of(&a), hash_of(&b));
	}
}
