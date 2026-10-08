//! Splitting one key range into extents that are read concurrently.
//!
//! A count of a large range on a backend that counts off the calling thread divides `[lo, hi)`
//! into disjoint extents covering exactly the original range, in ascending key order, and counts
//! each on its own; the extents' counts sum to the range's. Every extent is half-open and the
//! extents share no key: the high bound of one is the low bound of the next.
//!
//! The split is numeric over the first bytes that distinguish the bounds. Record keys of one
//! table share a prefix and differ in a generated id, which for the default generator is a ULID
//! whose leading bytes are a timestamp and whose trailing bytes are random; over a table written
//! in one load the ids spread near-uniformly, so even numeric division of the differing bytes
//! divides the rows nearly evenly. A distribution that defeats this -- ids assigned in a narrow
//! band, or a table whose keys cluster -- costs balance and never correctness: an empty extent
//! finishes at once and the rows it did not hold are read by whichever extent does hold them.

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

/// Divide `[lo, hi)` into at most `parts` disjoint extents in ascending key order.
///
/// Returns a single `[lo, hi)` when the range cannot be divided `parts` ways -- `parts` is 0 or 1,
/// the bounds are equal or inverted, or the bytes distinguishing them span fewer values than
/// `parts`. The result is never empty, so a caller may always scan what it returns.
///
/// The extents cover `[lo, hi)` exactly: the first begins at `lo`, the last ends at `hi`, and each
/// other bound is strictly between them and strictly ascending.
pub(crate) fn split_range(lo: &[u8], hi: &[u8], parts: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
	let whole = vec![(lo.to_vec(), hi.to_vec())];
	if parts <= 1 || lo >= hi {
		return whole;
	}
	// Divide on the first bytes that differ; up to 8 of them, which is far more resolution than
	// any worker count needs.
	let at = common_prefix(lo, hi);
	let width = 8.min(hi.len().max(lo.len()).saturating_sub(at));
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
		// Widened: `span` reaches the whole of `u64` on a range whose bounds differ in their first
		// byte, and multiplying that by the extent's index overflows before the division brings it
		// back. Wrapped, the cut lands anywhere -- below the previous bound, or past the range --
		// and the extents stop covering what they were asked to.
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
}

#[cfg(test)]
mod range_shape {
	use super::split_range;
	use crate::key::KVKey;
	use crate::key::schema::RecordPrefix;

	/// A table's own record range divides into the extents a parallel count asks for.
	///
	/// The splitter divides on the bytes that distinguish the bounds, so what it can do depends on
	/// the shape of the range the scan is given. A range whose bounds differ only in a final marker
	/// byte offers one value to divide, and the count that asked for many extents would get one.
	/// The extents of a table's range hold comparable numbers of its records.
	///
	/// Dividing the range evenly divides the rows evenly only if the keys are spread evenly
	/// through it. A generated record id is base32 text, so its bytes occupy a narrow band of the
	/// range the splitter divides; extents outside that band hold nothing and the count is as slow
	/// as whichever extent holds the rows.
	#[test]
	fn extents_hold_comparable_numbers_of_real_record_keys() {
		let range = RecordPrefix {
			ns: crate::catalog::NamespaceId(1),
			db: crate::catalog::DatabaseId(1),
			tb: std::borrow::Cow::Owned(crate::val::TableName::from("person")),
		}
		.range()
		.expect("record range");
		let (lo, hi) = range.bounds();

		// Keys as the default generator makes them: 20 characters of base32 text.
		const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
		let mut seed = 0x2545_F491_4F6C_DD1Du64;
		let keys: Vec<Vec<u8>> = (0..5000)
			.map(|_| {
				let id: String = (0..20)
					.map(|_| {
						seed ^= seed << 13;
						seed ^= seed >> 7;
						seed ^= seed << 17;
						ALPHABET[(seed % ALPHABET.len() as u64) as usize] as char
					})
					.collect();
				crate::key::schema::RecordKey {
					ns: crate::catalog::NamespaceId(1),
					db: crate::catalog::DatabaseId(1),
					tb: std::borrow::Cow::Owned(crate::val::TableName::from("person")),
					id: std::borrow::Cow::Owned(crate::val::RecordIdKey::String(id.into())),
				}
				.encode_key()
				.expect("encode")
				.as_slice()
				.to_vec()
			})
			.collect();

		// What the scan divides: the band the keys occupy, as its two-seek probe reports it, not
		// the range's own bounds.
		let mut sorted = keys.clone();
		sorted.sort();
		let first = sorted.first().expect("keys").clone();
		let mut band_end = sorted.last().expect("keys").clone();
		band_end.push(0);
		assert!(
			first.as_slice() >= lo && band_end.as_slice() <= hi,
			"the band sits inside the range"
		);
		let extents = split_range(&first, &band_end, 12);
		let counts: Vec<usize> = extents
			.iter()
			.map(|(s, e)| {
				keys.iter()
					.filter(|k| k.as_slice() >= s.as_slice() && k.as_slice() < e.as_slice())
					.count()
			})
			.collect();
		let occupied = counts.iter().filter(|&&c| c > 0).count();
		let biggest = counts.iter().max().copied().unwrap_or(0);
		assert_eq!(counts.iter().sum::<usize>(), keys.len(), "every key lands in one extent");
		assert!(
			occupied >= extents.len() / 2,
			"only {occupied} of {} extents hold any rows: {counts:?}",
			extents.len()
		);
		assert!(
			biggest <= keys.len() / extents.len() * 3,
			"one extent holds {biggest} of {} rows; a parallel count is as slow as that extent: \
			 {counts:?}",
			keys.len()
		);
	}

	#[test]
	fn a_tables_record_range_divides_into_extents() {
		let range = RecordPrefix {
			ns: crate::catalog::NamespaceId(1),
			db: crate::catalog::DatabaseId(1),
			tb: std::borrow::Cow::Owned(crate::val::TableName::from("person")),
		}
		.range()
		.expect("record range");
		let (lo, hi) = range.bounds();
		let extents = split_range(lo, hi, 16);
		assert!(
			extents.len() > 1,
			"a table's record range must divide, or a parallel count silently runs one extent; \
			 lo={lo:?} hi={hi:?} gave {} extent(s)",
			extents.len()
		);
	}
}
