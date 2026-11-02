//! Non-recursive teardown of deeply nested values.
//!
//! The derived `Drop` of [`Value`] and the types it contains recurses one stack
//! frame per nesting level, so a value nested hundreds of thousands of levels
//! deep aborts the process when it is dropped. Such a value can only come from
//! storage written before [`MAX_VALUE_DEPTH`](super::MAX_VALUE_DEPTH) was
//! enforced, and is only ever dropped by a path that rejects it.
//!
//! [`discard_value`] is that path's drop: it takes the value apart on an
//! explicit worklist instead of the call stack. No container type implements
//! `Drop` itself, so ordinary drops pay nothing; code that owns a value it is
//! about to reject calls this instead of letting it fall out of scope.
//!
//! A `Closure::Builtin` holds its captures behind a shared trait object that
//! cannot be taken apart, so it is dropped in place. Closures are never
//! persisted, so a stored value cannot carry one.

use std::ops::Bound;

use surrealdb_strand::Strand;

use crate::val::{Closure, Geometry, RecordIdKey, Value};

/// A subtree awaiting teardown. Nesting reaches a [`Value`] through wrappers
/// that are not themselves values, so the worklist carries each of them.
pub(super) enum Node {
	Value(Value),
	Key(RecordIdKey),
	Geo(Geometry),
}

/// An owned item that can be handed to the worklist.
pub(super) trait Discard {
	fn into_node(self) -> Node;
}

impl Discard for Value {
	fn into_node(self) -> Node {
		Node::Value(self)
	}
}

impl Discard for (Strand, Value) {
	fn into_node(self) -> Node {
		Node::Value(self.1)
	}
}

impl Discard for RecordIdKey {
	fn into_node(self) -> Node {
		Node::Key(self)
	}
}

impl Discard for Geometry {
	fn into_node(self) -> Node {
		Node::Geo(self)
	}
}

impl<T: Discard> Discard for Bound<T> {
	fn into_node(self) -> Node {
		match self {
			Bound::Included(v) | Bound::Excluded(v) => v.into_node(),
			// An unbounded end owns nothing; an empty value is the cheapest node
			// that says so.
			Bound::Unbounded => Node::Value(Value::None),
		}
	}
}

/// Drops `value` without recursing into its nesting.
///
/// Stack use is constant in the depth of `value`; the worklist is heap-allocated
/// and holds at most the unexplored siblings along the walk.
pub fn discard_value(value: Value) {
	discard_node(value.into_node());
}

/// Drops every item of `items` without recursing into its nesting.
pub(super) fn discard_all<T: Discard>(items: impl IntoIterator<Item = T>) {
	let mut work: Vec<Node> = items.into_iter().map(Discard::into_node).collect();
	drain(&mut work);
}

pub(super) fn discard_node(node: Node) {
	let mut work = vec![node];
	drain(&mut work);
}

/// Takes each node on `work` apart, pushing the children that nest further and
/// dropping the rest where they stand. Every node is deconstructed by move
/// before it is dropped, so what remains of it is shallow.
fn drain(work: &mut Vec<Node>) {
	while let Some(node) = work.pop() {
		match node {
			Node::Value(value) => match value {
				Value::Array(a) => work.extend(a.0.into_iter().map(Node::Value)),
				Value::Set(s) => work.extend(s.0.into_iter().map(Node::Value)),
				Value::Object(o) => work.extend(o.0.into_iter().map(|(_, v)| Node::Value(v))),
				Value::Range(r) => {
					let crate::val::Range {
						start,
						end,
					} = *r;
					work.push(start.into_node());
					work.push(end.into_node());
				}
				Value::RecordId(r) => work.push(Node::Key(r.key)),
				Value::Geometry(g) => work.push(Node::Geo(g)),
				Value::Closure(c) => {
					if let Closure::Expr {
						captures,
						..
					} = *c
					{
						work.extend(captures.0.into_values().map(Node::Value));
					}
				}
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::File(_)
				| Value::Regex(_) => {}
			},
			Node::Key(key) => match key {
				RecordIdKey::Array(a) => work.extend(a.0.into_iter().map(Node::Value)),
				RecordIdKey::Object(o) => work.extend(o.0.into_iter().map(|(_, v)| Node::Value(v))),
				RecordIdKey::Range(r) => {
					let crate::val::record_id::RecordIdKeyRange {
						start,
						end,
					} = *r;
					work.push(start.into_node());
					work.push(end.into_node());
				}
				RecordIdKey::Number(_) | RecordIdKey::String(_) | RecordIdKey::Uuid(_) => {}
			},
			Node::Geo(geo) => {
				if let Geometry::Collection(geoms) = geo {
					work.extend(geoms.into_iter().map(Node::Geo));
				}
			}
		}
	}
}

// Every value built here is nested far past any thread stack's tolerance for a
// recursive drop, and none is wrapped in `ManuallyDrop`: a regression to
// recursion aborts the test process rather than failing an assertion.
#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use super::discard_value;
	use crate::val::record_id::RecordIdKeyRange;
	use crate::val::{
		Array, Geometry, Object, Range, RecordId, RecordIdKey, Set, Value,
		convert_value_to_public_value,
	};

	const DEEP: usize = 200_000;

	type Shape = (&'static str, fn(usize) -> Value);

	fn nested_array(depth: usize) -> Value {
		let mut value = Value::from(1i64);
		for _ in 1..depth {
			value = Value::Array(Array::from(vec![value]));
		}
		value
	}

	fn nested_object(depth: usize) -> Value {
		let mut value = Value::from(1i64);
		for _ in 1..depth {
			let mut obj = Object::default();
			obj.insert("v".to_owned(), value);
			value = Value::Object(obj);
		}
		value
	}

	fn nested_set(depth: usize) -> Value {
		let mut value = Value::from(1i64);
		for _ in 1..depth {
			value = Value::Set(Set::from(vec![value]));
		}
		value
	}

	fn nested_geometry(depth: usize) -> Value {
		let mut geo = Geometry::Collection(vec![]);
		for _ in 1..depth {
			geo = Geometry::Collection(vec![geo]);
		}
		Value::Geometry(geo)
	}

	fn nested_range(depth: usize) -> Value {
		let mut value = Value::from(1i64);
		for _ in 1..depth {
			value = Value::Range(Box::new(Range {
				start: Bound::Included(value),
				end: Bound::Unbounded,
			}));
		}
		value
	}

	fn nested_key_range(depth: usize) -> Value {
		let mut key = RecordIdKey::Number(1);
		for _ in 1..depth {
			key = RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Included(key),
				end: Bound::Unbounded,
			}));
		}
		Value::RecordId(RecordId {
			table: "t".into(),
			key,
		})
	}

	fn nested_key_array(depth: usize) -> Value {
		Value::RecordId(RecordId {
			table: "t".into(),
			key: RecordIdKey::Array(Array::from(vec![nested_array(depth)])),
		})
	}

	fn shapes() -> Vec<Shape> {
		vec![
			("array", nested_array),
			("object", nested_object),
			("set", nested_set),
			("geometry collection", nested_geometry),
			("range", nested_range),
			("record id key range", nested_key_range),
			("record id key array", nested_key_array),
		]
	}

	#[test]
	fn discarding_a_deep_value_does_not_overflow() {
		for (name, build) in shapes() {
			let _ = name;
			discard_value(build(DEEP));
		}
	}

	#[test]
	fn discarding_a_wide_and_deep_value_releases_every_branch() {
		// Two deep branches under one root, so the worklist holds the unexplored
		// sibling while the first is torn down.
		discard_value(Value::Array(Array::from(vec![nested_array(DEEP), nested_object(DEEP)])));
	}

	#[test]
	fn rejected_conversion_of_a_deep_value_does_not_overflow() {
		for (name, build) in shapes() {
			let err = convert_value_to_public_value(build(DEEP))
				.expect_err("a value this deep must be rejected");
			assert!(
				err.to_string().contains("nested too deeply"),
				"{name}: unexpected error: {err}"
			);
		}
	}

	#[test]
	fn rejected_conversion_discards_the_siblings_it_never_reached() {
		// The first element fails, leaving a second deep element unconverted in
		// the iterator that is unwound with the error.
		let siblings = Value::Array(Array::from(vec![nested_array(DEEP), nested_array(DEEP)]));
		assert!(convert_value_to_public_value(siblings).is_err());

		let mut obj = Object::default();
		obj.insert("a".to_owned(), nested_object(DEEP));
		obj.insert("b".to_owned(), nested_object(DEEP));
		assert!(convert_value_to_public_value(Value::Object(obj)).is_err());

		// A range whose start fails leaves its end bound unconverted.
		let range = Value::Range(Box::new(Range {
			start: Bound::Included(nested_array(DEEP)),
			end: Bound::Excluded(nested_array(DEEP)),
		}));
		assert!(convert_value_to_public_value(range).is_err());

		let key_range = Value::RecordId(RecordId {
			table: "t".into(),
			key: RecordIdKey::Range(Box::new(RecordIdKeyRange {
				start: Bound::Included(RecordIdKey::Array(Array::from(vec![nested_array(DEEP)]))),
				end: Bound::Included(RecordIdKey::Array(Array::from(vec![nested_array(DEEP)]))),
			})),
		});
		assert!(convert_value_to_public_value(key_range).is_err());
	}

	#[test]
	fn discarding_preserves_conversion_of_values_within_the_cap() {
		let value = Value::Array(Array::from(vec![nested_array(8), nested_object(8)]));
		convert_value_to_public_value(value).expect("a shallow value must still convert");
	}
}
