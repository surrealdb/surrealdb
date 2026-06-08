//! Ordered set backed by a sorted unique `Vec`.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::iter::FromIterator;

use storekey::{BorrowDecode, BorrowReader, DecodeError, Encode, EncodeError, Writer};

use crate::search::search_sorted_by;

/// Set of unique values in ascending `Ord` order, stored in a `Vec`.
#[derive(Clone, Debug)]
#[repr(transparent)]
pub struct VecSet<T> {
	entries: Vec<T>,
}

impl<T> Default for VecSet<T> {
	fn default() -> Self {
		Self {
			entries: Vec::new(),
		}
	}
}

impl<T> VecSet<T> {
	pub fn iter(&self) -> std::slice::Iter<'_, T> {
		self.entries.iter()
	}
}

impl<T: PartialEq> PartialEq for VecSet<T> {
	fn eq(&self, other: &Self) -> bool {
		self.entries == other.entries
	}
}

impl<T: Eq> Eq for VecSet<T> {}

impl<T: Hash> Hash for VecSet<T> {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.entries.hash(state);
	}
}

impl<T: Ord> PartialOrd for VecSet<T> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl<T: Ord> Ord for VecSet<T> {
	fn cmp(&self, other: &Self) -> Ordering {
		self.entries.cmp(&other.entries)
	}
}

impl<T: Ord> VecSet<T> {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	/// Creates a new `VecSet` with space preallocated for `capacity` entries.
	#[must_use]
	pub fn with_capacity(capacity: usize) -> Self {
		Self {
			entries: Vec::with_capacity(capacity),
		}
	}

	/// Build a `VecSet` from a `Vec` whose entries are already sorted and
	/// unique per the `Ord` implementation.
	///
	/// The invariant is checked with a `debug_assert!` and is **not** enforced
	/// in release builds; callers must guarantee it. Use this when converting
	/// from an already-ordered source (e.g. `BTreeSet`, revision-decoded
	/// payloads) to avoid re-sorting and deduplicating.
	#[must_use]
	pub fn from_sorted_vec_unchecked(entries: Vec<T>) -> Self {
		debug_assert!(
			entries.windows(2).all(|w| w[0] < w[1]),
			"VecSet::from_sorted_vec_unchecked: entries not strictly sorted"
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

	/// Appends `value` in **O(1)** amortized time without searching.
	///
	/// # Ordering
	///
	/// `value` must be **strictly greater** than every existing element (or the
	/// set must be empty). Violating this breaks sorted-set invariants; it is
	/// checked with [`debug_assert!`] only (see [`from_sorted_vec_unchecked`]).
	pub fn push(&mut self, value: T) {
		debug_assert!(
			self.entries.last().map(|last| last < &value).unwrap_or(true),
			"VecSet::push: value must be strictly greater than the current maximum"
		);
		self.entries.push(value);
	}

	pub fn insert(&mut self, value: T) -> bool {
		match search_sorted_by(&self.entries, |x| x.cmp(&value)) {
			Ok(_) => false,
			Err(i) => {
				self.entries.insert(i, value);
				true
			}
		}
	}

	pub fn remove<Q: ?Sized + Ord>(&mut self, value: &Q) -> bool
	where
		T: Borrow<Q>,
	{
		if let Ok(i) = search_sorted_by(&self.entries, |x| x.borrow().cmp(value)) {
			self.entries.remove(i);
			true
		} else {
			false
		}
	}

	#[must_use]
	pub fn contains<Q: ?Sized + Ord>(&self, value: &Q) -> bool
	where
		T: Borrow<Q>,
	{
		search_sorted_by(&self.entries, |x| x.borrow().cmp(value)).is_ok()
	}

	#[must_use]
	pub fn first(&self) -> Option<&T> {
		self.entries.first()
	}

	#[must_use]
	pub fn last(&self) -> Option<&T> {
		self.entries.last()
	}

	pub fn retain<F>(&mut self, mut f: F)
	where
		F: FnMut(&T) -> bool,
	{
		self.entries.retain(|x| f(x));
	}

	#[must_use]
	pub fn union(&self, other: &Self) -> Self
	where
		T: Clone,
	{
		let mut i = 0;
		let mut j = 0;
		let mut out = Vec::new();
		while i < self.entries.len() && j < other.entries.len() {
			match self.entries[i].cmp(&other.entries[j]) {
				Ordering::Less => {
					out.push(self.entries[i].clone());
					i += 1;
				}
				Ordering::Greater => {
					out.push(other.entries[j].clone());
					j += 1;
				}
				Ordering::Equal => {
					out.push(self.entries[i].clone());
					i += 1;
					j += 1;
				}
			}
		}
		out.extend_from_slice(&self.entries[i..]);
		out.extend_from_slice(&other.entries[j..]);
		Self {
			entries: out,
		}
	}

	#[must_use]
	pub fn intersection(&self, other: &Self) -> Self
	where
		T: Clone,
	{
		let mut i = 0;
		let mut j = 0;
		let mut out = Vec::new();
		while i < self.entries.len() && j < other.entries.len() {
			match self.entries[i].cmp(&other.entries[j]) {
				Ordering::Less => i += 1,
				Ordering::Greater => j += 1,
				Ordering::Equal => {
					out.push(self.entries[i].clone());
					i += 1;
					j += 1;
				}
			}
		}
		Self {
			entries: out,
		}
	}

	#[must_use]
	pub fn difference(&self, other: &Self) -> Self
	where
		T: Clone,
	{
		let mut i = 0;
		let mut j = 0;
		let mut out = Vec::new();
		while i < self.entries.len() && j < other.entries.len() {
			match self.entries[i].cmp(&other.entries[j]) {
				Ordering::Less => {
					out.push(self.entries[i].clone());
					i += 1;
				}
				Ordering::Greater => j += 1,
				Ordering::Equal => {
					i += 1;
					j += 1;
				}
			}
		}
		out.extend(self.entries[i..].iter().cloned());
		Self {
			entries: out,
		}
	}

	#[must_use]
	pub fn symmetric_difference(&self, other: &Self) -> Self
	where
		T: Clone,
	{
		let mut i = 0;
		let mut j = 0;
		let mut out = Vec::new();
		while i < self.entries.len() && j < other.entries.len() {
			match self.entries[i].cmp(&other.entries[j]) {
				Ordering::Less => {
					out.push(self.entries[i].clone());
					i += 1;
				}
				Ordering::Greater => {
					out.push(other.entries[j].clone());
					j += 1;
				}
				Ordering::Equal => {
					i += 1;
					j += 1;
				}
			}
		}
		out.extend(self.entries[i..].iter().cloned());
		out.extend(other.entries[j..].iter().cloned());
		Self {
			entries: out,
		}
	}
}

impl<'a, T> IntoIterator for &'a VecSet<T> {
	type Item = &'a T;
	type IntoIter = std::slice::Iter<'a, T>;

	fn into_iter(self) -> Self::IntoIter {
		self.entries.iter()
	}
}

pub struct IntoIter<T> {
	inner: std::vec::IntoIter<T>,
}

impl<T> Iterator for IntoIter<T> {
	type Item = T;

	fn next(&mut self) -> Option<Self::Item> {
		self.inner.next()
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.inner.size_hint()
	}
}

impl<T> ExactSizeIterator for IntoIter<T> {
	fn len(&self) -> usize {
		self.inner.len()
	}
}

impl<T: Ord> IntoIterator for VecSet<T> {
	type Item = T;
	type IntoIter = IntoIter<T>;

	fn into_iter(self) -> Self::IntoIter {
		IntoIter {
			inner: self.entries.into_iter(),
		}
	}
}

impl<T: Ord> FromIterator<T> for VecSet<T> {
	fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
		let mut v: Vec<T> = iter.into_iter().collect();
		v.sort();
		v.dedup();
		Self {
			entries: v,
		}
	}
}

impl<T: Ord> Extend<T> for VecSet<T> {
	fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
		for x in iter {
			self.insert(x);
		}
	}
}

impl<T: Ord> From<BTreeSet<T>> for VecSet<T> {
	fn from(set: BTreeSet<T>) -> Self {
		let mut entries = Vec::with_capacity(set.len());
		entries.extend(set);
		Self::from_sorted_vec_unchecked(entries)
	}
}

impl<F, T: Encode<F>> Encode<F> for VecSet<T> {
	fn encode<W: Write>(&self, w: &mut Writer<W>) -> Result<(), EncodeError> {
		for item in &self.entries {
			w.mark_terminator();
			item.encode(w)?;
		}
		w.write_terminator()
	}
}

impl<'de, F, T: BorrowDecode<'de, F> + Ord> BorrowDecode<'de, F> for VecSet<T> {
	/// Deserialises a storekey set as a sequence of items, appending each with
	/// [`push`](VecSet::push).
	///
	/// **Wire contract:** Elements must appear in **strictly ascending** order (matching
	/// [`Encode`] for `VecSet` / `BTreeSet`). If a decoded element is less than or equal to
	/// the previous one, decoding fails with [`DecodeError::InvalidFormat`] in all builds.
	fn borrow_decode(r: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		let mut set = VecSet::new();
		while !r.read_terminal()? {
			let item = T::borrow_decode(r)?;
			if let Some(prev) = set.last()
				&& item <= *prev
			{
				return Err(DecodeError::InvalidFormat);
			}
			set.push(item);
		}
		Ok(set)
	}
}

#[cfg(test)]
mod tests {
	use std::cmp::Ordering;
	use std::collections::BTreeSet;
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};

	use super::*;

	/// Asserts that `s`'s entries are in strictly ascending order.
	fn assert_sorted<T: Ord + std::fmt::Debug>(s: &VecSet<T>) {
		for (a, b) in s.iter().zip(s.iter().skip(1)) {
			assert!(a < b, "VecSet entries not strictly sorted: {a:?} >= {b:?}");
		}
	}

	fn hash_of<T: Hash>(t: &T) -> u64 {
		let mut h = DefaultHasher::new();
		t.hash(&mut h);
		h.finish()
	}

	fn set_of<I: IntoIterator<Item = i32>>(iter: I) -> VecSet<i32> {
		iter.into_iter().collect()
	}

	#[test]
	fn new_and_default_are_empty() {
		let s: VecSet<i32> = VecSet::new();
		assert!(s.is_empty());
		assert_eq!(s.len(), 0);
		let d: VecSet<i32> = VecSet::default();
		assert_eq!(s, d);
	}

	#[test]
	fn with_capacity_starts_empty_and_works() {
		let mut s: VecSet<i32> = VecSet::with_capacity(8);
		assert!(s.is_empty());
		for i in 0..8 {
			s.insert(i);
		}
		assert_eq!(s.len(), 8);
		assert_sorted(&s);
	}

	#[test]
	fn from_sorted_vec_unchecked_builds_correct_set() {
		let s: VecSet<i32> = VecSet::from_sorted_vec_unchecked(vec![1, 2, 3]);
		assert_eq!(s.len(), 3);
		assert!(s.contains(&2));
	}

	#[test]
	fn from_sorted_vec_unchecked_empty_is_ok() {
		let s: VecSet<i32> = VecSet::from_sorted_vec_unchecked(vec![]);
		assert!(s.is_empty());
	}

	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "VecSet::from_sorted_vec_unchecked")]
	fn from_sorted_vec_unchecked_unsorted_panics_in_debug() {
		let _: VecSet<i32> = VecSet::from_sorted_vec_unchecked(vec![2, 1]);
	}

	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "VecSet::from_sorted_vec_unchecked")]
	fn from_sorted_vec_unchecked_duplicate_panics_in_debug() {
		let _: VecSet<i32> = VecSet::from_sorted_vec_unchecked(vec![1, 1]);
	}

	#[test]
	fn clear_resets_to_empty() {
		let mut s = set_of([1, 2, 3]);
		s.clear();
		assert!(s.is_empty());
		assert_eq!(s.len(), 0);
	}

	#[test]
	fn push_appends_in_order() {
		let mut s: VecSet<i32> = VecSet::new();
		s.push(1);
		s.push(2);
		s.push(3);
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
	}

	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "VecSet::push")]
	fn push_out_of_order_panics_in_debug() {
		let mut s = VecSet::new();
		s.push(2);
		s.push(1);
	}

	#[test]
	#[cfg(debug_assertions)]
	#[should_panic(expected = "VecSet::push")]
	fn push_equal_value_panics_in_debug() {
		let mut s = VecSet::new();
		s.push(1);
		s.push(1);
	}

	#[test]
	fn insert_returns_true_for_new_false_for_existing() {
		let mut s = VecSet::new();
		assert!(s.insert(1));
		assert!(s.insert(2));
		assert!(!s.insert(1));
		assert_eq!(s.len(), 2);
	}

	#[test]
	fn insert_arbitrary_order_keeps_sorted() {
		let mut s = VecSet::new();
		for &v in &[5, 1, 4, 2, 3] {
			assert!(s.insert(v));
		}
		assert_sorted(&s);
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
	}

	/// Cross the `LINEAR_SEARCH_THRESHOLD` (64) boundary so that
	/// `search_sorted_by` exercises the `binary_search_by` fallback.
	#[test]
	fn insert_above_linear_threshold_works() {
		let mut s = VecSet::new();
		for i in 0..128u32 {
			assert!(s.insert(i * 2));
		}
		for i in 0..128u32 {
			assert!(s.insert(i * 2 + 1));
		}
		assert_eq!(s.len(), 256);
		assert_sorted(&s);
		for i in 0..256u32 {
			assert!(s.contains(&i));
		}
		assert!(!s.insert(150));
	}

	#[test]
	fn remove_returns_true_when_present_then_false() {
		let mut s = set_of([1, 2, 3]);
		assert!(s.remove(&2));
		assert!(!s.remove(&2));
		assert_sorted(&s);
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![1, 3]);
	}

	#[test]
	fn remove_uses_borrow() {
		let mut s: VecSet<String> = ["apple".into(), "banana".into()].into_iter().collect();
		assert!(s.remove("apple"));
		assert!(!s.remove("missing"));
		assert_eq!(s.len(), 1);
	}

	#[test]
	fn contains_uses_borrow() {
		let s: VecSet<String> = ["apple".into(), "banana".into()].into_iter().collect();
		assert!(s.contains("apple"));
		assert!(!s.contains("cherry"));
	}

	#[test]
	fn first_and_last_min_and_max() {
		let s: VecSet<i32> = VecSet::new();
		assert_eq!(s.first(), None);
		assert_eq!(s.last(), None);
		let s = set_of([3, 1, 2]);
		assert_eq!(s.first(), Some(&1));
		assert_eq!(s.last(), Some(&3));
	}

	#[test]
	fn retain_keeps_subset_in_order() {
		let mut s = set_of([1, 2, 3, 4, 5]);
		s.retain(|&x| x % 2 == 0);
		assert_sorted(&s);
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![2, 4]);
	}

	#[test]
	fn retain_remove_all_or_keep_all() {
		let mut s = set_of([1, 2, 3]);
		let mut copy = s.clone();
		s.retain(|_| false);
		assert!(s.is_empty());
		copy.retain(|_| true);
		assert_eq!(copy.len(), 3);
	}

	#[test]
	fn union_disjoint_overlapping_identical_and_empty() {
		// All `Less` from a, then drain b.
		let a = set_of([1, 3, 5]);
		let b = set_of([2, 4, 6]);
		let u = a.union(&b);
		assert_sorted(&u);
		assert_eq!(u.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5, 6]);

		let a = set_of([1, 2, 3]);
		let b = set_of([2, 3, 4]);
		assert_eq!(a.union(&b).iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4]);

		assert_eq!(a.union(&a), a);

		let empty: VecSet<i32> = VecSet::new();
		assert_eq!(empty.union(&empty), empty);
		assert_eq!(a.union(&empty), a);
		assert_eq!(empty.union(&a), a);
	}

	#[test]
	fn intersection_disjoint_overlapping_identical_and_empty() {
		let a = set_of([1, 3, 5]);
		let b = set_of([2, 4, 6]);
		assert!(a.intersection(&b).is_empty());

		let a = set_of([1, 2, 3, 4]);
		let b = set_of([3, 4, 5]);
		let inter = a.intersection(&b);
		assert_sorted(&inter);
		assert_eq!(inter.iter().copied().collect::<Vec<_>>(), vec![3, 4]);

		assert_eq!(a.intersection(&a), a);

		let empty: VecSet<i32> = VecSet::new();
		assert!(a.intersection(&empty).is_empty());
		assert!(empty.intersection(&a).is_empty());
	}

	#[test]
	fn difference_disjoint_overlapping_identical_and_empty() {
		let a = set_of([1, 2, 3, 4]);
		let b = set_of([2, 4]);
		let d = a.difference(&b);
		assert_sorted(&d);
		assert_eq!(d.iter().copied().collect::<Vec<_>>(), vec![1, 3]);

		// Items in `a` that overflow past the end of `b` must be carried over.
		let a = set_of([1, 2, 5, 7]);
		let b = set_of([2, 5]);
		assert_eq!(a.difference(&b).iter().copied().collect::<Vec<_>>(), vec![1, 7]);

		// All elements removed.
		let a = set_of([1, 2, 3]);
		assert!(a.difference(&a).is_empty());

		let empty: VecSet<i32> = VecSet::new();
		assert_eq!(a.difference(&empty), a);
		assert!(empty.difference(&a).is_empty());
	}

	#[test]
	fn symmetric_difference_disjoint_overlapping_identical_and_empty() {
		let a = set_of([1, 2, 3, 4]);
		let b = set_of([3, 4, 5, 6]);
		let sd = a.symmetric_difference(&b);
		assert_sorted(&sd);
		assert_eq!(sd.iter().copied().collect::<Vec<_>>(), vec![1, 2, 5, 6]);

		assert!(a.symmetric_difference(&a).is_empty());

		let empty: VecSet<i32> = VecSet::new();
		assert_eq!(a.symmetric_difference(&empty), a);
		assert_eq!(empty.symmetric_difference(&a), a);
	}

	#[test]
	fn iter_yields_in_order() {
		let s = set_of([3, 1, 2]);
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
	}

	#[test]
	fn into_iter_owned_yields_in_order() {
		let s = set_of([3, 1, 2]);
		assert_eq!(s.into_iter().collect::<Vec<_>>(), vec![1, 2, 3]);
	}

	#[test]
	fn into_iter_owned_size_hint_and_len() {
		let s = set_of([1, 2, 3, 4]);
		let mut it = s.into_iter();
		assert_eq!(it.size_hint(), (4, Some(4)));
		assert_eq!(it.len(), 4);
		it.next();
		assert_eq!(it.len(), 3);
	}

	#[test]
	fn into_iter_borrowed_yields_in_order() {
		let s = set_of([3, 1, 2]);
		let v: Vec<_> = (&s).into_iter().copied().collect();
		assert_eq!(v, vec![1, 2, 3]);
	}

	#[test]
	fn from_iter_dedups_unsorted_input() {
		let s: VecSet<i32> = vec![5, 3, 5, 1, 3, 2].into_iter().collect();
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 5]);
	}

	#[test]
	fn from_iter_empty_returns_default() {
		let s: VecSet<i32> = std::iter::empty().collect();
		assert!(s.is_empty());
	}

	#[test]
	fn extend_inserts_and_dedups() {
		let mut s = set_of([1, 2, 3]);
		s.extend([2, 4, 5, 4]);
		assert_sorted(&s);
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
	}

	#[test]
	fn from_btreeset_preserves_order() {
		let mut b = BTreeSet::new();
		b.insert(3);
		b.insert(1);
		b.insert(2);
		let s: VecSet<i32> = VecSet::from(b);
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
	}

	#[test]
	fn equality_and_ord_are_lexicographic() {
		let a = set_of([1, 2]);
		let b = set_of([1, 3]);
		let c = set_of([1, 2]);
		assert_eq!(a, c);
		assert!(a < b);
		assert_eq!(a.cmp(&c), Ordering::Equal);
		assert_eq!(a.partial_cmp(&b), Some(Ordering::Less));
	}

	#[test]
	fn hash_matches_for_equal_sets_built_in_different_order() {
		let a = set_of([1, 2, 3]);
		let b = set_of([3, 1, 2]);
		assert_eq!(a, b);
		assert_eq!(hash_of(&a), hash_of(&b));
	}
}
