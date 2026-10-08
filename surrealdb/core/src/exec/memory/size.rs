//! What a value held in memory weighs, for a breaker's account.
//!
//! The weight is an estimate: the value's own size, and what its strings,
//! byte buffers and collections hold on the heap, walked to a fixed depth and
//! counted by length beyond it. It is close enough to drive a breaker's spill
//! and cheap enough to take once per row a breaker holds.

use crate::val::{RecordIdKey, Value};

/// How deep the walk goes before nested collections are counted by length.
const DEPTH: usize = 4;

/// The bytes `value` holds, its own size included.
pub(crate) fn value_bytes(value: &Value) -> usize {
	std::mem::size_of::<Value>() + heap_bytes(value, DEPTH)
}

/// The bytes `values` hold, their own sizes included.
pub(crate) fn values_bytes(values: &[Value]) -> usize {
	values.iter().map(value_bytes).sum()
}

fn heap_bytes(value: &Value, depth: usize) -> usize {
	const SLOT: usize = std::mem::size_of::<Value>();
	match value {
		Value::String(s) => s.len(),
		Value::Bytes(b) => b.len(),
		Value::Array(array) if depth == 0 => array.len() * SLOT,
		Value::Array(array) => array.iter().map(|v| SLOT + heap_bytes(v, depth - 1)).sum(),
		Value::Set(set) => set.len() * SLOT * 2,
		Value::Object(object) if depth == 0 => object.len() * (SLOT + 32),
		Value::Object(object) => {
			object.iter().map(|(k, v)| SLOT + 24 + k.len() + heap_bytes(v, depth - 1)).sum()
		}
		Value::RecordId(rid) => {
			rid.table.as_str().len()
				+ match &rid.key {
					RecordIdKey::String(s) => s.len(),
					RecordIdKey::Array(a) => a.len() * SLOT,
					RecordIdKey::Object(o) => o.len() * (SLOT + 32),
					_ => 0,
				}
		}
		Value::Geometry(_) => 64,
		_ => 0,
	}
}
