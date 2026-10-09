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
		| Value::Uuid(_) => 0,
		_ => value_bytes(value),
	}
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
		Value::Closure(closure) => match &**closure {
			Closure::Expr {
				captures,
				..
			} if depth == 0 => captures.0.len() * (SLOT + 32),
			Closure::Expr {
				captures,
				..
			} => {
				captures.0.iter().map(|(k, v)| SLOT + 24 + k.len() + heap_bytes(v, depth - 1)).sum()
			}
			Closure::Builtin(object) => object
				.captured_values()
				.into_iter()
				.map(|v| SLOT + heap_bytes(v, depth.saturating_sub(1)))
				.sum(),
		},
		_ => 0,
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
}
