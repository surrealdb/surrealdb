//! Ordered set backed by a sorted unique `Vec`.

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

#[cfg(feature = "storekey")]
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

/// Whether `v` is in strictly ascending `Ord` order (sorted, no equal neighbours).
fn is_strictly_ascending<T: Ord>(v: &[T]) -> bool {
	v.iter().zip(v.iter().skip(1)).all(|(a, b)| a < b)
}

/// Sorts `v` by `cmp`, stably, without any path that can panic whatever `cmp` returns.
///
/// This is a bottom-up merge sort over a permutation of indices. `std`'s sorts may panic, or abort,
/// when the comparison they are given is not a total order, and the values reaching a decode path
/// come from stored bytes. A comparison that is not a total order here yields some permutation of
/// the input, never a panic. Input already in non-descending order is returned as it is, with no
/// allocation. Otherwise the cost is O(n log n) comparisons and two `Vec<usize>` buffers of length
/// `n`, reused across every merge level.
pub(crate) fn sort_by_cmp<T>(v: Vec<T>, mut cmp: impl FnMut(&T, &T) -> Ordering) -> Vec<T> {
	if v.iter().zip(v.iter().skip(1)).all(|(a, b)| cmp(a, b) != Ordering::Greater) {
		return v;
	}
	let n = v.len();
	let mut src: Vec<usize> = (0..n).collect();
	let mut dst: Vec<usize> = vec![0; n];
	let mut width = 1usize;
	while width < n {
		let run = width.saturating_mul(2);
		for (from, to) in src.chunks(run).zip(dst.chunks_mut(run)) {
			let (left, right) = from.split_at(width.min(from.len()));
			merge_runs(&v, left, right, to, &mut cmp);
		}
		std::mem::swap(&mut src, &mut dst);
		width = run;
	}
	// `src` is a permutation of `0..n`: each merge places every index it was given exactly once.
	let mut slots: Vec<Option<T>> = v.into_iter().map(Some).collect();
	src.iter().filter_map(|&i| slots.get_mut(i).and_then(Option::take)).collect()
}

/// Merges the index runs `left` and `right` into `out`, taking from `left` on ties (stable).
fn merge_runs<T>(
	v: &[T],
	left: &[usize],
	right: &[usize],
	out: &mut [usize],
	cmp: &mut impl FnMut(&T, &T) -> Ordering,
) {
	let mut l = left.iter().copied().peekable();
	let mut r = right.iter().copied().peekable();
	for slot in out.iter_mut() {
		let next = match (l.peek().copied(), r.peek().copied()) {
			(Some(a), Some(b)) => {
				let right_first = match (v.get(b), v.get(a)) {
					(Some(rv), Some(lv)) => cmp(rv, lv) == Ordering::Less,
					_ => false,
				};
				if right_first {
					r.next();
					b
				} else {
					l.next();
					a
				}
			}
			(Some(a), None) => {
				l.next();
				a
			}
			(None, Some(b)) => {
				r.next();
				b
			}
			(None, None) => break,
		};
		*slot = next;
	}
}

/// Sorts `v` by `Ord`, stably, and cannot panic whatever `Ord` does.
///
/// `Ord` implementations that are not total orders (such as an approximate cross-type numeric
/// comparison) make `std`'s sorts unsafe to call on decoded data. Elements the order cannot
/// separate keep their input order. See [`sort_by_cmp`].
pub(crate) fn sort_by_ord<T: Ord>(v: Vec<T>) -> Vec<T> {
	sort_by_cmp(v, |a, b| a.cmp(b))
}

impl<T: Ord> VecSet<T> {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	/// Builds a set from elements in any order, with or without duplicates.
	///
	/// Strictly ascending input is taken as it is, with no allocation. Otherwise the elements are
	/// sorted by `Ord` and elements that compare equal are merged, keeping the first of each run.
	/// Returns `None` if the result is still not strictly ascending, which only an `Ord` that is
	/// not a total order can cause; the caller decides how to refuse. Cannot panic whatever `Ord`
	/// does.
	#[must_use]
	pub fn try_from_vec(entries: Vec<T>) -> Option<Self> {
		if is_strictly_ascending(&entries) {
			return Some(Self {
				entries,
			});
		}
		let mut entries = sort_by_ord(entries);
		entries.dedup_by(|a, b| Ord::cmp(&*a, &*b) == Ordering::Equal);
		is_strictly_ascending(&entries).then_some(Self {
			entries,
		})
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
	/// from an already-ordered source (e.g. `BTreeSet`) to avoid re-sorting and
	/// deduplicating. For data of unknown order use [`try_from_vec`](Self::try_from_vec).
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

	/// Restore the set's invariant — distinct elements in ascending order —
	/// after elements were changed in place through [`get_mut`](Self::get_mut),
	/// [`first_mut`](Self::first_mut) or [`last_mut`](Self::last_mut).
	///
	/// Elements that compare equal under `Ord` collapse to the first in sorted
	/// order, as [`FromIterator`] collapses them, and the sort cannot panic
	/// whatever `Ord` does ([`sort_by_ord`]). A set whose elements are already
	/// distinct and ascending is checked in one pass and left as it is.
	pub fn normalize(&mut self) {
		if !is_strictly_ascending(&self.entries) {
			let mut entries = sort_by_ord(std::mem::take(&mut self.entries));
			entries.dedup_by(|a, b| Ord::cmp(&*a, &*b) == Ordering::Equal);
			self.entries = entries;
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

	pub fn first_mut(&mut self) -> Option<&mut T> {
		self.entries.first_mut()
	}

	pub fn last_mut(&mut self) -> Option<&mut T> {
		self.entries.last_mut()
	}

	pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
		self.entries.get_mut(index)
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
		let mut entries = sort_by_ord(iter.into_iter().collect());
		entries.dedup_by(|a, b| Ord::cmp(&*a, &*b) == Ordering::Equal);
		Self {
			entries,
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

#[cfg(feature = "storekey")]
impl<F, T: Encode<F>> Encode<F> for VecSet<T> {
	fn encode<W: std::io::Write>(&self, w: &mut Writer<W>) -> Result<(), EncodeError> {
		for item in &self.entries {
			w.mark_terminator();
			item.encode(w)?;
		}
		w.write_terminator()
	}
}

#[cfg(feature = "storekey")]
impl<'de, F, T: BorrowDecode<'de, F> + Ord> BorrowDecode<'de, F> for VecSet<T> {
	/// Deserialises a storekey set as a sequence of items, then builds the set with
	/// [`try_from_vec`](VecSet::try_from_vec).
	///
	/// **Wire contract:** [`Encode`] writes elements in ascending `Ord` order, and strictly
	/// ascending input is taken as it is. Elements are read in any order and taken in `Ord` order,
	/// with elements that compare equal merged, so a key written under an earlier order of `T`
	/// still decodes; it re-encodes in the current order. A sequence that no order can sort into a
	/// strictly ascending run fails with [`DecodeError::InvalidFormat`] in all builds.
	fn borrow_decode(r: &mut BorrowReader<'de>) -> Result<Self, DecodeError> {
		let mut items = Vec::new();
		while !r.read_terminal()? {
			items.push(T::borrow_decode(r)?);
		}
		VecSet::try_from_vec(items).ok_or(DecodeError::InvalidFormat)
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

	/// An element whose `Ord` answers `Greater` for every pair, so no order sorts it.
	#[derive(Debug, PartialEq, Eq)]
	struct Contrary;

	impl PartialOrd for Contrary {
		fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
			Some(self.cmp(other))
		}
	}

	impl Ord for Contrary {
		fn cmp(&self, _: &Self) -> Ordering {
			Ordering::Greater
		}
	}

	#[test]
	fn try_from_vec_refuses_elements_no_order_sorts() {
		assert!(VecSet::try_from_vec(vec![Contrary, Contrary]).is_none());
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
	fn normalize_restores_order_and_distinctness_in_place() {
		let mut s = set_of([1, 2, 3, 4]);
		*s.get_mut(0).unwrap() = 4;
		*s.get_mut(1).unwrap() = 9;
		s.normalize();
		assert_sorted(&s);
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![3, 4, 9]);
		// A set already in order keeps its elements and its buffer.
		let mut s = set_of([1, 2, 3]);
		let buffer = s.iter().as_slice().as_ptr();
		s.normalize();
		assert_eq!(s.iter().copied().collect::<Vec<_>>(), vec![1, 2, 3]);
		assert_eq!(s.iter().as_slice().as_ptr(), buffer);
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
