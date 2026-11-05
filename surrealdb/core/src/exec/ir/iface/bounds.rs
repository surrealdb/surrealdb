//! Size and effort bounds the IR guarantees to its consumers.
//!
//! Each constant is a contract: a producer stays within it, and a consumer may rely on it.

use crate::val::Value;

/// The most bytes one op's inline data occupies on a 64-bit target: the op's kind (a dialect op
/// of at most 32 bytes under a tag), its operand, result and region lists, its location and its
/// provenance. Payloads larger than 24 bytes are boxed in their variant, so a container's op
/// storage stays dense. The bound is the measured layout: growing `OpData` past it fails the
/// build.
pub(crate) const OP_DATA_MAX_BYTES: usize = 208;

/// The most bytes of encoded result a plan-time fold produces. A larger result is not folded.
pub(crate) const FOLD_MAX_BYTES: usize = 1 << 20;

/// The instruction budget of the restricted host a plan-time fold runs in. A fold that exhausts
/// it is not folded.
pub(crate) const FOLD_FUEL: u64 = 100_000;

/// The most iterations a loop may have and still be unrolled. Past it the loop stays a loop.
pub(crate) const UNROLL_MAX_ITERATIONS: u32 = 64;

/// The most ops unrolling one loop may create. Past it the loop stays a loop.
pub(crate) const UNROLL_MAX_OPS: u32 = 4_096;

/// The most bytes, by [`value_exceeds`], a literal may measure and still be keyed for value
/// numbering. A larger literal is never merged with an equal one: building its key encodes the
/// whole payload, work that grows with the literal while two equal literals of that size are rare.
pub(crate) const KEYED_LITERAL_MAX_BYTES: usize = 1024;

/// Whether `value` is larger than `max_bytes`. The measure counts what a string or byte string
/// holds, 16 bytes for a number, duration, datetime or uuid, 64 for any other scalar, and one byte
/// per none, null, bool and container element. It stops at the first overrun, so it never walks
/// more of `value` than `max_bytes` admits, and it allocates nothing.
pub(crate) fn value_exceeds(value: &Value, max_bytes: usize) -> bool {
	let mut left = max_bytes;
	!fits(value, &mut left)
}

/// Spends `bytes` of `left`; answers `false` once the budget is exhausted.
fn spend(left: &mut usize, bytes: usize) -> bool {
	match left.checked_sub(bytes) {
		Some(rest) => {
			*left = rest;
			true
		}
		None => false,
	}
}

/// Whether `value` fits in the bytes `left` still allows.
fn fits(value: &Value, left: &mut usize) -> bool {
	match value {
		Value::None | Value::Null | Value::Bool(_) => spend(left, 1),
		Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_) => spend(left, 16),
		Value::String(s) => spend(left, s.len()),
		Value::Bytes(b) => spend(left, b.len()),
		Value::Array(items) => spend(left, 1) && items.iter().all(|item| fits(item, left)),
		Value::Set(items) => spend(left, 1) && items.0.iter().all(|item| fits(item, left)),
		Value::Object(fields) => {
			spend(left, 1)
				&& fields.iter().all(|(key, item)| spend(left, key.len()) && fits(item, left))
		}
		Value::Geometry(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => spend(left, 64),
	}
}
