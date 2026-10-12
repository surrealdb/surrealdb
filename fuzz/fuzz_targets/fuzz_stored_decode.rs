//! Fuzzing of the stored-value decoder and in-place reader on arbitrary bytes.
//!
//! Neither reader may panic, overflow, or allocate out of proportion to its
//! input, whatever the bytes. Bytes the decoder accepts must read the same in
//! place: every accessor, field lookup and element read agrees with the decoded
//! value, at every level. They must also write back: the decoded value encodes,
//! its encoding decodes to exactly that value (float bits, decimal scale and
//! all), and encoding it again gives the same bytes.

#![no_main]

use libfuzzer_sys::fuzz_target;
use surrealdb_expr::val::stored::{self, ValueRef, tag};

mod stored_value;

use stored_value::{Counting, hex, holds_regex, identical, peak_allocation, read_loosely, walk};

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// The most bytes a decode may hold allocated at once, per byte of its input.
///
/// The smallest element of any encoding is one byte, an element of an `I8` array, with no tag or
/// offset of its own, and it decodes to one `Value`. An element of a generic container takes at
/// least two bytes, an offset and a tag. So a decode holds at most `size_of::<Value>()` bytes per
/// input byte for its elements, and the rest of the bound is headroom for container overhead; a
/// decode past it has allocated for elements its input does not hold.
const ALLOCATED_PER_BYTE: usize = 256;

const _: () = assert!(
	std::mem::size_of::<surrealdb_expr::val::Value>() <= ALLOCATED_PER_BYTE / 2,
	"a one-byte typed array element must decode within half of the per-byte allocation bound"
);

fuzz_target!(|data: &[u8]| {
	let (decoded, allocated) = peak_allocation(|| stored::decode(data));
	assert_eq!(
		stored::validate(data).is_ok(),
		decoded.is_ok(),
		"validate and decode disagree on {}",
		hex(data)
	);
	// Compiling a regex allocates out of proportion to its pattern, within the
	// regex engine's own size limit, so a decode that may have compiled one is
	// not held to the bound: one whose value holds a regex, or one refused from
	// bytes that hold the regex tag anywhere.
	let compiled = match &decoded {
		Ok(v) => holds_regex(v),
		Err(_) => data.contains(&tag::REGEX),
	};
	if !compiled {
		assert!(
			allocated <= ALLOCATED_PER_BYTE * data.len() + 4096,
			"decoding {} bytes held {allocated} bytes allocated at once\n{}",
			data.len(),
			hex(data)
		);
	}
	if let Ok(r) = ValueRef::new(data) {
		match &decoded {
			Ok(v) => walk(r, v, false),
			Err(_) => read_loosely(r, 0),
		}
	}
	// A record id key is a value too.
	if let Ok(key) = stored::decode_key(data) {
		let v = decoded
			.as_ref()
			.unwrap_or_else(|e| panic!("{key:?} decodes as a key but not as a value: {e}"));
		assert!(identical(v, &key.clone().into_value()), "as a key {key:?}\nas a value {v:?}");
	}
	let Ok(v) = decoded else {
		return;
	};
	let canonical = stored::encode(&v)
		.unwrap_or_else(|e| panic!("{v:?}, decoded from {}, does not encode: {e}", hex(data)));
	let back = stored::decode(&canonical).unwrap_or_else(|e| {
		panic!("{v:?} encodes to {}, which does not decode: {e}", hex(&canonical))
	});
	assert!(
		identical(&back, &v),
		"decoded  {v:?}\nwritten  {}\nread as  {back:?}",
		hex(&canonical)
	);
	let again = stored::encode(&back).expect("a value read back encodes");
	assert_eq!(again, canonical, "{v:?} encodes two ways");
	walk(ValueRef::new(&canonical).expect("an encoding has a tag"), &v, true);
});
