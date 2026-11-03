//! What a value held in memory weighs, for a breaker's account.
//!
//! The weight is an estimate: the value's own size, and what its strings,
//! byte buffers and collections hold on the heap, walked to a fixed depth and
//! counted by length beyond it. It is close enough to drive a breaker's spill
//! and cheap enough to take once per row a breaker holds.

use crate::val::{Closure, RecordIdKey, Value};

/// How deep the walk goes before nested collections are counted by length.
const DEPTH: usize = 4;

/// The bytes `value` holds, its own size included.
pub(crate) fn value_bytes(value: &Value) -> usize {
	std::mem::size_of::<Value>() + heap_bytes(value, DEPTH)
}

/// The bytes a program keeps by holding `value`: nothing for a value with no
/// heap, which touches no ledger, and [`value_bytes`] otherwise.
pub(crate) fn kept_bytes(value: &Value) -> usize {
	match value {
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_) => 0,
		Value::String(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::File(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => value_bytes(value),
	}
}

/// The bytes record id `id` holds, its own size included: its table's name and
/// its key, however long or structured.
pub(crate) fn record_id_bytes(id: &crate::val::RecordId) -> usize {
	std::mem::size_of::<crate::val::RecordId>()
		+ id.table.as_str().len()
		+ key_heap_bytes(&id.key, DEPTH)
}

/// The bytes `values` hold, their own sizes included.
pub(crate) fn values_bytes(values: &[Value]) -> usize {
	values.iter().map(value_bytes).sum()
}

fn bound_heap_bytes(bound: &std::ops::Bound<Value>, depth: usize) -> usize {
	match bound {
		std::ops::Bound::Included(v) | std::ops::Bound::Excluded(v) => {
			if depth == 0 {
				0
			} else {
				heap_bytes(v, depth - 1)
			}
		}
		std::ops::Bound::Unbounded => 0,
	}
}

fn key_bound_heap_bytes(bound: &std::ops::Bound<RecordIdKey>, depth: usize) -> usize {
	match bound {
		std::ops::Bound::Included(k) | std::ops::Bound::Excluded(k) => {
			if depth == 0 {
				0
			} else {
				key_heap_bytes(k, depth - 1)
			}
		}
		std::ops::Bound::Unbounded => 0,
	}
}

fn key_heap_bytes(key: &RecordIdKey, depth: usize) -> usize {
	const SLOT: usize = std::mem::size_of::<Value>();
	match key {
		RecordIdKey::String(s) => s.len(),
		RecordIdKey::Array(a) if depth == 0 => a.len() * SLOT,
		RecordIdKey::Array(a) => a.iter().map(|v| SLOT + heap_bytes(v, depth - 1)).sum(),
		RecordIdKey::Object(o) if depth == 0 => o.len() * (SLOT + 32),
		RecordIdKey::Object(o) => {
			o.iter().map(|(k, v)| SLOT + 24 + k.len() + heap_bytes(v, depth - 1)).sum()
		}
		RecordIdKey::Range(r) => {
			std::mem::size_of::<crate::val::RecordIdKeyRange>()
				+ key_bound_heap_bytes(&r.start, depth)
				+ key_bound_heap_bytes(&r.end, depth)
		}
		RecordIdKey::Number(_) | RecordIdKey::Uuid(_) => 0,
	}
}

fn heap_bytes(value: &Value, depth: usize) -> usize {
	const SLOT: usize = std::mem::size_of::<Value>();
	match value {
		Value::String(s) => s.len(),
		Value::Bytes(b) => b.len(),
		Value::Array(array) if depth == 0 => array.len() * SLOT,
		Value::Array(array) => array.iter().map(|v| SLOT + heap_bytes(v, depth - 1)).sum(),
		Value::Set(set) if depth == 0 => set.len() * SLOT * 2,
		Value::Set(set) => set.iter().map(|v| SLOT * 2 + heap_bytes(v, depth - 1)).sum(),
		Value::Object(object) if depth == 0 => object.len() * (SLOT + 32),
		Value::Object(object) => {
			object.iter().map(|(k, v)| SLOT + 24 + k.len() + heap_bytes(v, depth - 1)).sum()
		}
		Value::RecordId(rid) => rid.table.as_str().len() + key_heap_bytes(&rid.key, depth),
		Value::Geometry(geometry) => geometry.heap_bytes(),
		Value::Table(t) => t.as_str().len(),
		Value::File(f) => f.bucket.capacity() + f.key.capacity(),
		Value::Regex(r) => r.0.as_str().len(),
		Value::Range(r) => {
			std::mem::size_of::<crate::val::Range>()
				+ bound_heap_bytes(&r.start, depth)
				+ bound_heap_bytes(&r.end, depth)
		}
		Value::Closure(closure) => {
			std::mem::size_of::<Closure>()
				+ match &**closure {
					Closure::Expr {
						captures,
						..
					} if depth == 0 => captures.0.len() * (SLOT + 32),
					Closure::Expr {
						captures,
						..
					} => captures
						.0
						.iter()
						.map(|(k, v)| SLOT + 24 + k.len() + heap_bytes(v, depth - 1))
						.sum(),
					Closure::Builtin(object) => object
						.captured_values()
						.into_iter()
						.map(|v| SLOT + heap_bytes(v, depth.saturating_sub(1)))
						.sum(),
				}
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_) => 0,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A value with no heap weighs nothing to a program that keeps it.
	#[test]
	fn a_scalar_is_kept_for_free() {
		for value in [Value::None, Value::Null, Value::Bool(true), Value::from(3_i64)] {
			assert_eq!(kept_bytes(&value), 0);
		}
		assert!(kept_bytes(&Value::from("a string")) > 0);
	}

	/// A closure weighs what it captured, so one that captures a large array
	/// does not escape the charge.
	#[test]
	fn a_closure_counts_its_captures() {
		let large = Value::Array(crate::val::Array((0..1000).map(Value::from).collect()));
		let mut captures = crate::expr::variables::Variables::new();
		captures.insert("big", large.clone());
		let closure = Value::Closure(Box::new(Closure::Expr {
			args: Vec::new(),
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::None),
			captures,
		}));
		let empty = Value::Closure(Box::new(Closure::Expr {
			args: Vec::new(),
			returns: None,
			body: crate::expr::Expr::Literal(crate::expr::Literal::None),
			captures: crate::expr::variables::Variables::new(),
		}));
		assert!(value_bytes(&closure) >= value_bytes(&large));
		assert!(value_bytes(&closure) > value_bytes(&empty));
	}

	#[test]
	fn set_range_file_and_regex_account_for_heap_bytes() {
		let big_str = Value::from("x".repeat(4096));
		let mut set = crate::val::Set::new();
		set.insert(big_str.clone());
		let set_val = Value::Set(set);
		assert!(value_bytes(&set_val) > 4096);

		let range_val = Value::Range(Box::new(crate::val::Range {
			start: std::ops::Bound::Included(big_str),
			end: std::ops::Bound::Unbounded,
		}));
		assert!(value_bytes(&range_val) > 4096);

		let file_val = Value::File(crate::val::File::new("bucket_name".into(), "a".repeat(1024)));
		assert!(value_bytes(&file_val) > 1024);
	}
}
