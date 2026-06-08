//! Sorted-slice search helper used by [`crate::VecMap`] and [`crate::VecSet`].
//!
//! The ordered containers store their entries in a sorted `Vec`. For small
//! sizes (up to [`LINEAR_SEARCH_THRESHOLD`]) a plain forward scan is faster and
//! more cache-friendly than a binary search; above that we fall back to
//! `Vec::binary_search_by`.

use std::cmp::Ordering;

/// Slices at or below this length use a linear search; larger slices use
/// `binary_search_by`.
pub(crate) const LINEAR_SEARCH_THRESHOLD: usize = 64;

/// Semantically equivalent to [`<[T]>::binary_search_by`], but uses a linear
/// scan for slices at or below [`LINEAR_SEARCH_THRESHOLD`] items.
///
/// The slice **must** be sorted per `cmp`: `cmp` should return `Less` if the
/// given element is less than the search target, `Equal` on a match, and
/// `Greater` once the element has passed the target.
#[inline]
pub(crate) fn search_sorted_by<T>(
	slice: &[T],
	mut cmp: impl FnMut(&T) -> Ordering,
) -> Result<usize, usize> {
	if slice.len() <= LINEAR_SEARCH_THRESHOLD {
		for (i, item) in slice.iter().enumerate() {
			match cmp(item) {
				Ordering::Less => {}
				Ordering::Equal => return Ok(i),
				Ordering::Greater => return Err(i),
			}
		}
		Err(slice.len())
	} else {
		slice.binary_search_by(cmp)
	}
}
