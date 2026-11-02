//! The binary forms of PostgreSQL range and multirange values.
//!
//! The codecs are generic over the element type. Each takes the element codec as a function, so
//! the caller decides how a bound is written and read. They do not canonicalise: a decoded range
//! keeps the bounds and inclusivity it was sent with, and the value layer applies the canonical
//! form of the declared type. The range itself and its text forms are the declared types' own, in
//! `surrealdb_sqlplan::sql_types::text_form::range`.
//!
//! A range is one flags byte, then each bounded side as an `i32` length and the element bytes.
//! The flags are 0x01 empty, 0x02 lower inclusive, 0x04 upper inclusive, 0x08 lower unbounded,
//! and 0x10 upper unbounded. An empty range is the flags byte alone. A multirange is an `i32`
//! count, then each range as an `i32` length and its binary form.

use std::ops::Bound;

pub use surrealdb_sqlplan::sql_types::text_form::range::Range;
use surrealdb_sqlplan::sql_types::text_form::range::bound;

const FLAG_EMPTY: u8 = 0x01;
const FLAG_LOWER_INCLUSIVE: u8 = 0x02;
const FLAG_UPPER_INCLUSIVE: u8 = 0x04;
const FLAG_LOWER_UNBOUNDED: u8 = 0x08;
const FLAG_UPPER_UNBOUNDED: u8 = 0x10;
const KNOWN_FLAGS: u8 = FLAG_EMPTY
	| FLAG_LOWER_INCLUSIVE
	| FLAG_UPPER_INCLUSIVE
	| FLAG_LOWER_UNBOUNDED
	| FLAG_UPPER_UNBOUNDED;

/// A range or multirange that does not decode, with the reason.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeCodecError {
	pub message: String,
}

pub type CodecResult<T> = Result<T, RangeCodecError>;

fn error(message: impl Into<String>) -> RangeCodecError {
	RangeCodecError {
		message: message.into(),
	}
}

/// The binary form of a range. `element` writes a bounded side's value.
pub fn encode_binary<T>(range: &Range<T>, element: impl Fn(&T) -> Vec<u8>) -> CodecResult<Vec<u8>> {
	match range {
		Range::Empty => Ok(vec![FLAG_EMPTY]),
		Range::Span {
			lower,
			upper,
		} => {
			let mut flags = 0u8;
			let mut body = Vec::new();
			match lower {
				Bound::Included(value) => {
					flags |= FLAG_LOWER_INCLUSIVE;
					push_prefixed(&mut body, &element(value))?;
				}
				Bound::Excluded(value) => push_prefixed(&mut body, &element(value))?,
				Bound::Unbounded => flags |= FLAG_LOWER_UNBOUNDED,
			}
			match upper {
				Bound::Included(value) => {
					flags |= FLAG_UPPER_INCLUSIVE;
					push_prefixed(&mut body, &element(value))?;
				}
				Bound::Excluded(value) => push_prefixed(&mut body, &element(value))?,
				Bound::Unbounded => flags |= FLAG_UPPER_UNBOUNDED,
			}
			let mut out = Vec::with_capacity(1 + body.len());
			out.push(flags);
			out.extend_from_slice(&body);
			Ok(out)
		}
	}
}

/// The binary form of a multirange: a count, then each range length-prefixed.
pub fn encode_multirange_binary<T>(
	ranges: &[Range<T>],
	element: impl Fn(&T) -> Vec<u8>,
) -> CodecResult<Vec<u8>> {
	let count = i32::try_from(ranges.len())
		.map_err(|_| error("a multirange holds more ranges than the wire allows"))?;
	let mut out = count.to_be_bytes().to_vec();
	for range in ranges {
		push_prefixed(&mut out, &encode_binary(range, &element)?)?;
	}
	Ok(out)
}

/// Read the binary form of a range. `parse` reads a bounded side's bytes.
pub fn decode_binary<T>(bytes: &[u8], parse: impl Fn(&[u8]) -> Option<T>) -> CodecResult<Range<T>> {
	let Some((&flags, mut rest)) = bytes.split_first() else {
		return Err(error("a range has no flags byte"));
	};
	if flags & !KNOWN_FLAGS != 0 {
		return Err(error(format!("a range has unknown flag bits {flags:#04x}")));
	}
	if flags & FLAG_EMPTY != 0 {
		return if rest.is_empty() {
			Ok(Range::Empty)
		} else {
			Err(error("an empty range has bytes after its flags"))
		};
	}
	let lower = if flags & FLAG_LOWER_UNBOUNDED != 0 {
		Bound::Unbounded
	} else {
		let (chunk, next) = take_prefixed(rest)?;
		rest = next;
		bound(read_element(chunk, &parse)?, flags & FLAG_LOWER_INCLUSIVE != 0)
	};
	let upper = if flags & FLAG_UPPER_UNBOUNDED != 0 {
		Bound::Unbounded
	} else {
		let (chunk, next) = take_prefixed(rest)?;
		rest = next;
		bound(read_element(chunk, &parse)?, flags & FLAG_UPPER_INCLUSIVE != 0)
	};
	if !rest.is_empty() {
		return Err(error("a range has bytes after its bounds"));
	}
	Ok(Range::Span {
		lower,
		upper,
	})
}

/// Read the binary form of a multirange.
pub fn decode_multirange_binary<T>(
	bytes: &[u8],
	parse: impl Fn(&[u8]) -> Option<T>,
) -> CodecResult<Vec<Range<T>>> {
	let (count, mut rest) = take_i32(bytes)?;
	let count = usize::try_from(count).map_err(|_| error("a multirange has a negative count"))?;
	let mut ranges = Vec::new();
	for _ in 0..count {
		let (chunk, next) = take_prefixed(rest)?;
		ranges.push(decode_binary(chunk, &parse)?);
		rest = next;
	}
	if !rest.is_empty() {
		return Err(error("a multirange has bytes after its last range"));
	}
	Ok(ranges)
}

fn read_element<T>(bytes: &[u8], parse: &impl Fn(&[u8]) -> Option<T>) -> CodecResult<T> {
	parse(bytes).ok_or_else(|| error("a range bound is not a value of the element type"))
}

fn push_prefixed(out: &mut Vec<u8>, bytes: &[u8]) -> CodecResult<()> {
	let len =
		i32::try_from(bytes.len()).map_err(|_| error("a range bound is too long for the wire"))?;
	out.extend_from_slice(&len.to_be_bytes());
	out.extend_from_slice(bytes);
	Ok(())
}

fn take_i32(bytes: &[u8]) -> CodecResult<(i32, &[u8])> {
	let Some((head, rest)) = bytes.split_first_chunk::<4>() else {
		return Err(error("a binary count or length is cut short"));
	};
	Ok((i32::from_be_bytes(*head), rest))
}

/// Split off an `i32`-length-prefixed chunk, returning the chunk and the bytes after it.
fn take_prefixed(bytes: &[u8]) -> CodecResult<(&[u8], &[u8])> {
	let (len, rest) = take_i32(bytes)?;
	let len = usize::try_from(len).map_err(|_| error("a binary length is negative"))?;
	if rest.len() < len {
		return Err(error("a binary length runs past the end of the value"));
	}
	Ok(rest.split_at(len))
}

#[cfg(test)]
mod tests {
	use surrealdb_sqlplan::sql_types::text_form::range::{decode_text, encode_text};

	use super::*;

	fn int_text(text: &str) -> Option<i32> {
		text.parse().ok()
	}

	fn int_bytes(bytes: &[u8]) -> Option<i32> {
		<[u8; 4]>::try_from(bytes).ok().map(i32::from_be_bytes)
	}

	#[expect(
		clippy::trivially_copy_pass_by_ref,
		reason = "the codec hands each bound by reference"
	)]
	fn int_to_text(value: &i32) -> String {
		value.to_string()
	}

	#[expect(
		clippy::trivially_copy_pass_by_ref,
		reason = "the codec hands each bound by reference"
	)]
	fn int_to_bytes(value: &i32) -> Vec<u8> {
		value.to_be_bytes().to_vec()
	}

	fn span(lower: Bound<i32>, upper: Bound<i32>) -> Range<i32> {
		Range::Span {
			lower,
			upper,
		}
	}

	#[test]
	fn binary_range_flags_and_bounds_match_the_postgresql_layout() {
		let bytes =
			encode_binary(&span(Bound::Included(1), Bound::Excluded(5)), int_to_bytes).unwrap();
		assert_eq!(bytes, [0x02, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 5]);
		let unbounded_lower =
			encode_binary(&span(Bound::Unbounded, Bound::Included(3)), int_to_bytes).unwrap();
		assert_eq!(unbounded_lower, [0x0c, 0, 0, 0, 4, 0, 0, 0, 3]);
		let unbounded_both =
			encode_binary(&span(Bound::Unbounded, Bound::Unbounded), int_to_bytes).unwrap();
		assert_eq!(unbounded_both, [0x18]);
		assert_eq!(encode_binary(&Range::<i32>::Empty, int_to_bytes).unwrap(), [0x01]);
	}

	#[test]
	fn binary_range_reads_back_every_shape() {
		let shapes = [
			span(Bound::Included(1), Bound::Excluded(5)),
			span(Bound::Excluded(-2), Bound::Included(9)),
			span(Bound::Unbounded, Bound::Included(3)),
			span(Bound::Excluded(7), Bound::Unbounded),
			span(Bound::Unbounded, Bound::Unbounded),
			Range::Empty,
		];
		for shape in shapes {
			let bytes = encode_binary(&shape, int_to_bytes).unwrap();
			assert_eq!(decode_binary(&bytes, int_bytes).unwrap(), shape);
		}
	}

	#[test]
	fn binary_range_refuses_unknown_flags_and_bad_lengths() {
		assert!(decode_binary(&[0x20], int_bytes).is_err());
		assert!(decode_binary(&[], int_bytes).is_err());
		assert!(decode_binary(&[0x01, 0x00], int_bytes).is_err());
		assert!(decode_binary(&[0x02, 0, 0, 0, 9, 0, 0, 0, 1], int_bytes).is_err());
		assert!(decode_binary(&[0x02, 0, 0, 0, 4, 0, 0, 0, 1, 0xff], int_bytes).is_err());
		assert!(decode_binary(&[0x0c, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 0], int_bytes).is_err());
		assert!(decode_binary(&[0x02, 0, 0, 0, 3, 0, 0, 0], int_bytes).is_err());
	}

	#[test]
	fn binary_multirange_is_a_count_then_length_prefixed_ranges() {
		let ranges = vec![
			span(Bound::Included(1), Bound::Excluded(3)),
			span(Bound::Unbounded, Bound::Included(9)),
		];
		let bytes = encode_multirange_binary(&ranges, int_to_bytes).unwrap();
		assert_eq!(
			bytes,
			[
				0, 0, 0, 2, // count
				0, 0, 0, 17, // length of the first range
				0x02, 0, 0, 0, 4, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 3, 0, 0, 0,
				9, // length of the second range
				0x0c, 0, 0, 0, 4, 0, 0, 0, 9,
			]
		);
		assert_eq!(decode_multirange_binary(&bytes, int_bytes).unwrap(), ranges);
		assert_eq!(
			decode_multirange_binary(&[0, 0, 0, 0], int_bytes).unwrap(),
			Vec::<Range<i32>>::new()
		);
	}

	#[test]
	fn binary_multirange_refuses_a_count_that_does_not_match_its_bytes() {
		assert!(decode_multirange_binary(&[0, 0, 0, 2, 0, 0, 0, 1, 0x01], int_bytes).is_err());
		assert!(decode_multirange_binary(&[0, 0, 0, 0, 0xff], int_bytes).is_err());
		assert!(decode_multirange_binary(&[0xff, 0xff, 0xff, 0xff], int_bytes).is_err());
		assert!(decode_multirange_binary(&[0, 0], int_bytes).is_err());
	}

	#[test]
	fn text_and_binary_agree_on_the_same_value() {
		let range = span(Bound::Excluded(10), Bound::Included(20));
		assert_eq!(encode_text(&range, int_to_text), "(10,20]");
		let bytes = encode_binary(&range, int_to_bytes).unwrap();
		assert_eq!(
			decode_binary(&bytes, int_bytes).unwrap(),
			decode_text("(10,20]", int_text).unwrap()
		);
	}
}
