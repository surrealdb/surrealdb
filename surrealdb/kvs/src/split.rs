//! Dividing a key range into parts that are read independently, and estimating what a range holds.
//!
//! [`split_range`] divides `[lo, hi)` into disjoint extents covering exactly the original range, in
//! ascending key order. The extents share no key: the high bound of one is the low bound of the
//! next. Every backend answers [`Transactable::split`](crate::Transactable::split) and
//! [`Transactable::estimate`](crate::Transactable::estimate) correctly through the helpers here, so
//! a backend with nothing better to offer inherits valid answers.
//!
//! The division is numeric over the first bytes that distinguish the bounds. Record keys of one
//! table share a prefix and differ in a generated id, which for the default generator is a ULID
//! whose leading bytes are a timestamp and whose trailing bytes are random; over a table written
//! in one load the ids spread near-uniformly, so even numeric division of the differing bytes
//! divides the rows nearly evenly. A distribution that defeats this -- ids assigned in a narrow
//! band, or a table whose keys cluster -- costs balance and never correctness: an empty extent
//! finishes at once and the rows it did not hold are read by whichever extent does hold them.
//! [`position_fraction`] measures a key's place in a range on the same numeric window, so an
//! estimate extrapolated from a prefix of the range is as good as the division is. The estimate
//! measures that place between the first and last keys the range holds, as the division does,
//! not between its bounds: keys that cluster in a corner of their bounds (integer ids under a
//! table's prefix) would otherwise read as a sliver of the range and scale to far too many.

use crate::types::{Key, KeyRange};

/// Keys a default estimate reads before it extrapolates.
pub const ESTIMATE_BUDGET: u32 = 1024;

/// Most parts one split answers, whatever it is asked.
pub const MAX_SPLIT_PARTS: u32 = 10_000;

/// What a range holds, as an estimate answers it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Estimate {
	/// Keys in the range; `None` when the answer measured bytes without counting keys.
	pub keys: Option<u64>,
	/// Key bytes plus value bytes in the range. An answer read from a backend's metadata measures
	/// the bytes the backend stores, which compression makes fewer than the entries hold.
	pub bytes: u64,
	/// Whether `keys` and `bytes` were counted rather than extrapolated or read from metadata.
	pub exact: bool,
}

/// How finely a split divides a range.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SplitTarget {
	/// About this many parts.
	Parts(u32),
	/// Parts of about this many key and value bytes each.
	Bytes(u64),
}

/// The number of leading bytes shared by both bounds.
fn common_prefix(lo: &[u8], hi: &[u8]) -> usize {
	lo.iter().zip(hi.iter()).take_while(|(a, b)| a == b).count()
}

/// The `width` bytes of `key` at `from`, big-endian, zero-padded past its end.
fn window(key: &[u8], from: usize, width: usize) -> u64 {
	let mut acc = 0u64;
	for i in 0..width {
		acc = (acc << 8) | u64::from(key.get(from + i).copied().unwrap_or(0));
	}
	acc
}

/// The key `prefix` followed by `value` in `width` big-endian bytes.
fn boundary(prefix: &[u8], value: u64, width: usize) -> Vec<u8> {
	let mut key = Vec::with_capacity(prefix.len() + width);
	key.extend_from_slice(prefix);
	for i in (0..width).rev() {
		key.push(((value >> (i * 8)) & 0xFF) as u8);
	}
	key
}

/// The bytes of the bounds that [`split_range`] divides on: where they begin and how many, at most
/// eight, which is far more resolution than any worker count needs.
fn division_window(lo: &[u8], hi: &[u8]) -> (usize, usize) {
	let at = common_prefix(lo, hi);
	(at, 8.min(hi.len().max(lo.len()).saturating_sub(at)))
}

/// Divide `[lo, hi)` into at most `parts` disjoint extents in ascending key order.
///
/// Returns a single `[lo, hi)` when the range cannot be divided `parts` ways -- `parts` is 0 or 1,
/// the bounds are equal or inverted, or the bytes distinguishing them span fewer values than
/// `parts`. The result is never empty, so a caller may always scan what it returns.
///
/// The extents cover `[lo, hi)` exactly: the first begins at `lo`, the last ends at `hi`, and each
/// other bound is strictly between them and strictly ascending.
pub fn split_range(lo: &[u8], hi: &[u8], parts: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
	let whole = vec![(lo.to_vec(), hi.to_vec())];
	if parts <= 1 || lo >= hi {
		return whole;
	}
	// Divide on the first bytes that differ.
	let (at, width) = division_window(lo, hi);
	if width == 0 {
		return whole;
	}
	let (low, high) = (window(lo, at, width), window(hi, at, width));
	let span = high.saturating_sub(low);
	// Fewer distinct values than extents asked for: dividing would produce empty or inverted
	// extents, so the range is left whole.
	if span < parts as u64 {
		return whole;
	}
	let prefix = &lo[..at];
	let mut extents = Vec::with_capacity(parts);
	let mut start = lo.to_vec();
	for i in 1..parts {
		// Multiply in `u128`: `span` can reach `u64::MAX` when the bounds differ in their first
		// byte, and a `u64` product with the extent index would wrap. A wrapped cut could land
		// below the previous bound or past the range, and the extents would stop covering it.
		let offset =
			u64::try_from(u128::from(span) * i as u128 / parts as u128).unwrap_or(u64::MAX);
		let cut = boundary(prefix, low.saturating_add(offset), width);
		// The numeric cut can land on the previous bound once `span` is close to `parts`; an
		// extent that would be empty is skipped rather than emitted inverted.
		if cut.as_slice() <= start.as_slice() || cut.as_slice() >= hi {
			continue;
		}
		extents.push((start, cut.clone()));
		start = cut;
	}
	extents.push((start, hi.to_vec()));
	extents
}

/// Where `at` lies within `[lo, hi)`, in `[0.0, 1.0]`, on the numeric window of the first
/// differing bytes that [`split_range`] divides on. A key at or before `lo` is at `0.0` and one at
/// or after `hi` at `1.0`. `0.0` when the range is empty or that window is.
pub fn position_fraction(lo: &[u8], hi: &[u8], at: &[u8]) -> f64 {
	if lo >= hi || at <= lo {
		return 0.0;
	}
	if at >= hi {
		return 1.0;
	}
	let (from, width) = division_window(lo, hi);
	if width == 0 {
		return 0.0;
	}
	let (low, high) = (window(lo, from, width), window(hi, from, width));
	let span = high.saturating_sub(low);
	if span == 0 {
		return 0.0;
	}
	let offset = window(at, from, width).saturating_sub(low);
	(offset as f64 / span as f64).clamp(0.0, 1.0)
}

/// What a range holds, from the `read` entries and their `bytes` read from its start: `first`
/// and `last` the first and final keys read, `far` the last key the range holds.
///
/// `read` fewer than [`ESTIMATE_BUDGET`] entries means the range ended inside the budget and the
/// answer is exact; `far` is not read then. Otherwise the entries read are scaled by the share of
/// the keys from `first` to `far` they span, measured with [`position_fraction`]; a share that
/// cannot be measured leaves them unscaled. A caller seeks `far` only on that path.
pub fn estimate_from_prefix(
	read: u64,
	bytes: u64,
	first: Option<&[u8]>,
	last: Option<&[u8]>,
	far: Option<&[u8]>,
) -> Estimate {
	if read < u64::from(ESTIMATE_BUDGET) {
		return Estimate {
			keys: Some(read),
			bytes,
			exact: true,
		};
	}
	let fraction = match (first, last, far) {
		(Some(first), Some(last), Some(far)) => {
			let mut end = far.to_vec();
			end.push(0);
			position_fraction(first, &end, last)
		}
		_ => 0.0,
	};
	if fraction <= 0.0 {
		return Estimate {
			keys: Some(read),
			bytes,
			exact: false,
		};
	}
	Estimate {
		keys: Some(((read as f64 / fraction).round() as u64).max(read)),
		bytes: ((bytes as f64 / fraction).round() as u64).max(bytes),
		exact: false,
	}
}

/// The number of parts a split into `target` asks for, given the range's `estimated_bytes` (read
/// only for [`SplitTarget::Bytes`]), clamped to `[1, MAX_SPLIT_PARTS]`.
pub fn parts_for(target: SplitTarget, estimated_bytes: impl FnOnce() -> u64) -> u32 {
	let parts = match target {
		SplitTarget::Parts(parts) => u64::from(parts),
		SplitTarget::Bytes(per_part) => estimated_bytes().div_ceil(per_part.max(1)),
	};
	parts.clamp(1, u64::from(MAX_SPLIT_PARTS)) as u32
}

/// The boundaries that divide `rng` into `parts`, given its `first` and `last` keys.
///
/// The boundaries are the interior bounds of [`split_range`] over `[first, last.next())`, kept when
/// strictly inside `rng`, ascending and without duplicates. The parts they cut tile `rng` exactly.
/// Empty when `first == last` -- the range holds no more than one key -- or when the keys span too
/// few values to divide `parts` ways.
pub fn default_split(
	first: &[u8],
	last: &[u8],
	parts: u32,
	rng: &KeyRange<'_>,
) -> Vec<Key<'static>> {
	if first >= last {
		return Vec::new();
	}
	let mut end = last.to_vec();
	end.push(0);
	let mut bounds: Vec<Key<'static>> = split_range(first, &end, parts as usize)
		.into_iter()
		.skip(1)
		.map(|(start, _)| Key::from(start))
		.filter(|key| rng.start < *key && *key < rng.end)
		.collect();
	bounds.dedup();
	bounds
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Every extent is non-empty and ascending, and together they tile `[lo, hi)` exactly.
	fn assert_tiles(lo: &[u8], hi: &[u8], extents: &[(Vec<u8>, Vec<u8>)]) {
		assert!(!extents.is_empty(), "a split always yields a scannable range");
		assert_eq!(extents[0].0.as_slice(), lo, "first extent begins at lo");
		assert_eq!(extents[extents.len() - 1].1.as_slice(), hi, "last extent ends at hi");
		for (start, end) in extents {
			assert!(start.as_slice() < end.as_slice(), "extent {start:?}..{end:?} is non-empty");
		}
		for pair in extents.windows(2) {
			assert_eq!(pair[0].1, pair[1].0, "extents meet with no gap and no overlap");
		}
	}

	#[test]
	fn splits_a_wide_range_into_the_requested_parts() {
		let (lo, hi) = (b"tb\x00".as_slice(), b"tb\xff".as_slice());
		let extents = split_range(lo, hi, 8);
		assert_eq!(extents.len(), 8);
		assert_tiles(lo, hi, &extents);
	}

	#[test]
	fn covers_the_range_exactly_for_every_worker_count() {
		let (lo, hi) = (b"/*ns*db*person*\x00".as_slice(), b"/*ns*db*person*\xff".as_slice());
		for parts in 1..=32 {
			assert_tiles(lo, hi, &split_range(lo, hi, parts));
		}
	}

	#[test]
	fn leaves_an_indivisible_range_whole() {
		// Adjacent keys: nothing to divide.
		assert_eq!(
			split_range(b"k\x01", b"k\x02", 8),
			vec![(b"k\x01".to_vec(), b"k\x02".to_vec())]
		);
		// One part is the whole range.
		assert_eq!(split_range(b"a", b"z", 1), vec![(b"a".to_vec(), b"z".to_vec())]);
		// Identical and inverted bounds are returned untouched rather than producing extents that
		// scan keys outside the caller's range.
		assert_eq!(split_range(b"a", b"a", 4), vec![(b"a".to_vec(), b"a".to_vec())]);
		assert_eq!(split_range(b"z", b"a", 4), vec![(b"z".to_vec(), b"a".to_vec())]);
	}

	#[test]
	fn divides_long_shared_prefixes_on_the_bytes_that_differ() {
		let lo = b"/*ns*db*person*\x01\x00\x00\x00".as_slice();
		let hi = b"/*ns*db*person*\x01\xff\xff\xff".as_slice();
		let extents = split_range(lo, hi, 4);
		assert_eq!(extents.len(), 4);
		assert_tiles(lo, hi, &extents);
		// The shared prefix survives on every bound, so no extent escapes the table.
		for (start, end) in &extents {
			assert!(start.starts_with(b"/*ns*db*person*"));
			assert!(end.starts_with(b"/*ns*db*person*"));
		}
	}

	#[test]
	fn balances_uniform_keys_across_extents() {
		// 4096 evenly-spread two-byte ids, as a ULID's random tail would spread.
		let keys: Vec<Vec<u8>> =
			(0..4096u32).map(|i| vec![b'k', (i >> 4) as u8, (i & 0xF) as u8]).collect();
		let (lo, hi) = (b"k\x00\x00".as_slice(), b"k\xff\xff".as_slice());
		let extents = split_range(lo, hi, 8);
		assert_tiles(lo, hi, &extents);
		let counts: Vec<usize> = extents
			.iter()
			.map(|(s, e)| {
				keys.iter()
					.filter(|k| k.as_slice() >= s.as_slice() && k.as_slice() < e.as_slice())
					.count()
			})
			.collect();
		assert_eq!(
			counts.iter().sum::<usize>(),
			keys.len(),
			"every key lands in exactly one extent"
		);
		let (min, max) = (counts.iter().min().unwrap(), counts.iter().max().unwrap());
		assert!(max - min <= keys.len() / extents.len() / 2, "extents are balanced: {counts:?}");
	}

	#[test]
	fn position_fraction_is_monotonic_and_bounded() {
		let (lo, hi) = (b"k\x00\x00".as_slice(), b"k\xff\xff".as_slice());
		let mut previous = 0.0;
		for i in 0..=255u8 {
			let at = [b'k', i, 0x80];
			let f = position_fraction(lo, hi, &at);
			assert!((0.0..=1.0).contains(&f), "{f} is outside [0, 1]");
			assert!(f >= previous, "{f} fell below {previous} at {at:?}");
			previous = f;
		}
		// Outside the range the position clamps to its ends.
		assert_eq!(position_fraction(lo, hi, b"a"), 0.0);
		assert_eq!(position_fraction(lo, hi, b"z"), 1.0);
		assert_eq!(position_fraction(lo, hi, lo), 0.0);
		// Midway on the window is about one half.
		assert!((position_fraction(lo, hi, b"k\x80\x00") - 0.5).abs() < 0.01);
		// A range with nothing to measure on answers 0.
		assert_eq!(position_fraction(b"a", b"a", b"a"), 0.0);
		assert_eq!(position_fraction(b"z", b"a", b"m"), 0.0);
	}

	#[test]
	fn default_split_cuts_strictly_inside_the_range() {
		let rng = KeyRange::from(&b"k"[..]..&b"l"[..]);
		let (first, last) = (b"k\x00\x01".as_slice(), b"k\x03\xe7".as_slice());
		let bounds = default_split(first, last, 8, &rng);
		assert!(!bounds.is_empty() && bounds.len() <= 7);
		for pair in bounds.windows(2) {
			assert!(pair[0] < pair[1], "boundaries ascend");
		}
		for key in &bounds {
			assert!(rng.start < *key && *key < rng.end);
		}
		// One key, or one part: nothing to cut.
		assert!(default_split(first, first, 8, &rng).is_empty());
		assert!(default_split(first, last, 1, &rng).is_empty());
	}

	#[test]
	fn ten_thousand_parts_tile_the_range() {
		let lo = b"k\x00\x00\x00\x00";
		let hi = b"k\xff\xff\xff\xff";
		let extents = split_range(lo, hi, parts_for(SplitTarget::Parts(10_000), || 0) as usize);
		assert_eq!(extents.len(), 10_000);
		assert_tiles(lo, hi, &extents);
	}

	#[test]
	fn parts_are_clamped_to_what_a_split_answers() {
		assert_eq!(parts_for(SplitTarget::Parts(0), || 0), 1);
		assert_eq!(parts_for(SplitTarget::Parts(8), || 0), 8);
		assert_eq!(parts_for(SplitTarget::Parts(u32::MAX), || 0), MAX_SPLIT_PARTS);
		assert_eq!(parts_for(SplitTarget::Bytes(100), || 250), 3);
		assert_eq!(parts_for(SplitTarget::Bytes(0), || 5), 5);
		assert_eq!(parts_for(SplitTarget::Bytes(1), || u64::MAX), MAX_SPLIT_PARTS);
	}

	#[test]
	fn an_estimate_within_the_budget_is_exact() {
		let est = estimate_from_prefix(100, 4000, Some(b"k\x01"), Some(b"k\x10"), None);
		assert_eq!(
			est,
			Estimate {
				keys: Some(100),
				bytes: 4000,
				exact: true
			}
		);
	}

	#[test]
	fn an_estimate_past_the_budget_scales_by_the_share_read() {
		let (first, far) = (&b"k\x00\x00"[..], &b"k\x3f\xff\xff"[..]);
		// A quarter of the keys from the first to the last read, in `ESTIMATE_BUDGET` keys.
		let est = estimate_from_prefix(1024, 10_240, Some(first), Some(b"k\x10\x00"), Some(far));
		assert!(!est.exact);
		assert_eq!(est.keys, Some(4096));
		assert_eq!(est.bytes, 40_960);
		// A share that cannot be measured leaves the counts as read.
		let est = estimate_from_prefix(1024, 10_240, Some(first), Some(first), Some(far));
		assert_eq!((est.keys, est.bytes, est.exact), (Some(1024), 10_240, false));
		let est = estimate_from_prefix(1024, 10_240, Some(first), Some(b"k\x10\x00"), None);
		assert_eq!(est.keys, Some(1024));
	}

	#[test]
	fn an_estimate_measures_between_the_keys_held_not_the_bounds() {
		// Ids 0..3000 under a table's prefix, two bytes each: the bounds span 0x00..0xff, the
		// keys a sliver at the bottom. Reading the first 1024 is about a third of what is held.
		let key = |id: u16| [&b"t"[..], &id.to_be_bytes()].concat();
		let est =
			estimate_from_prefix(1024, 10_240, Some(&key(0)), Some(&key(1023)), Some(&key(2999)));
		let keys = est.keys.expect("counted");
		assert!((2_900..=3_100).contains(&keys), "{keys} estimated for 3000 held");
	}
}
