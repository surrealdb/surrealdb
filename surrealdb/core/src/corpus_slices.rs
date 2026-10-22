//! A test that walks a corpus, split into slices that run as tests of their own.
//!
//! A test that walks every item of a corpus holds one runner slot for as long as the walk takes,
//! which in a debug build is minutes. Split into [`SLICES`] tests that each walk the items whose
//! position is congruent to their number, the walk runs in parallel and no one test spans the
//! corpus.
//!
//! [`sliced_tests!`] defines the slice tests of one walk and does not compile unless it names
//! every slice once. [`assert_slices_cover`] holds, over the list a walk takes, that the slices
//! together hold every item once and that none is empty.

/// The number of slices a walk is split into.
pub(crate) const SLICES: usize = 12;

/// The items whose position is congruent to `k` modulo [`SLICES`]. For `k` in `0..SLICES` the
/// slices of one list are disjoint and together hold every item, in the list's order.
pub(crate) fn slice<T>(items: Vec<T>, k: usize) -> Vec<T> {
	items
		.into_iter()
		.enumerate()
		.filter_map(|(position, item)| (position % SLICES == k).then_some(item))
		.collect()
}

/// Whether `listed` is `0..SLICES` in order: the slices a [`sliced_tests!`] names are all of them.
pub(crate) const fn lists_every_slice(listed: &[usize]) -> bool {
	let mut at = 0;
	while at < SLICES {
		if at >= listed.len() || listed[at] != at {
			return false;
		}
		at += 1;
	}
	listed.len() == SLICES
}

/// Defines one `#[test]` per slice, each calling `$run` with its slice number, and fails to
/// compile unless the slices named are exactly `0..SLICES`.
macro_rules! sliced_tests {
	($run:ident, $listed:ident; $($name:ident => $k:literal),+ $(,)?) => {
		const $listed: &[usize] = &[$($k),+];
		const _: () = assert!(
			$crate::corpus_slices::lists_every_slice($listed),
			"name each slice 0..SLICES once"
		);
		$(
			#[test]
			fn $name() {
				$run($k)
			}
		)+
	};
}
pub(crate) use sliced_tests;

/// Requires that the slices of `items` hold every item once between them and that none is empty.
/// `what` names the list in the failure.
pub(crate) fn assert_slices_cover(what: &str, items: &[String]) {
	let mut seen = Vec::new();
	for k in 0..SLICES {
		let part = slice(items.to_vec(), k);
		assert!(!part.is_empty(), "slice {k} of the {what} is empty: {} items", items.len());
		seen.extend(part);
	}
	let mut expected = items.to_vec();
	seen.sort();
	expected.sort();
	assert_eq!(seen, expected, "the slices of the {what}");
}

/// The slices of any list are disjoint and together hold every item.
#[test]
fn slices_partition_a_list() {
	for len in [0, 1, SLICES - 1, SLICES, 5 * SLICES + 3] {
		let items: Vec<usize> = (0..len).collect();
		let mut seen: Vec<usize> = (0..SLICES).flat_map(|k| slice(items.clone(), k)).collect();
		seen.sort_unstable();
		assert_eq!(seen, items, "{len} items");
	}
}

/// A slice keeps the order of the list it was taken from.
#[test]
fn a_slice_keeps_the_order_of_its_list() {
	let items: Vec<usize> = (0..3 * SLICES).collect();
	assert_eq!(slice(items, 2), vec![2, 2 + SLICES, 2 + 2 * SLICES]);
}

#[test]
fn only_the_whole_range_of_slices_is_listed() {
	let all: Vec<usize> = (0..SLICES).collect();
	assert!(lists_every_slice(&all));
	assert!(!lists_every_slice(&all[1..]));
	assert!(!lists_every_slice(&all[..SLICES - 1]));
	let mut shuffled = all.clone();
	shuffled.swap(0, 1);
	assert!(!lists_every_slice(&shuffled));
	let mut longer = all;
	longer.push(SLICES);
	assert!(!lists_every_slice(&longer));
}

/// A list with fewer items than there are slices leaves a slice empty, which the check reports.
#[test]
#[should_panic(expected = "is empty")]
fn a_list_shorter_than_the_slices_is_refused() {
	let items: Vec<String> = (0..SLICES - 1).map(|n| n.to_string()).collect();
	assert_slices_cover("short list", &items);
}
