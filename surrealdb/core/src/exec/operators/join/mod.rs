mod hash;
mod index_nested_loop;
mod nested_loop;
mod sort_merge;

use std::collections::BTreeMap;

pub use hash::HashJoin;
pub use index_nested_loop::IndexNestedLoopJoin;
pub use nested_loop::NestedLoopJoin;
pub use sort_merge::SortMergeJoin;

use crate::val::{Object, Value};

pub(crate) fn merge_records(
	left: &Value,
	right: &Value,
	left_alias: &str,
	right_alias: &str,
) -> Value {
	let mut map = BTreeMap::new();
	if !left_alias.is_empty() {
		map.insert(left_alias.to_string(), left.clone());
	}
	if !right_alias.is_empty() {
		map.insert(right_alias.to_string(), right.clone());
	}
	if let Value::Object(obj) = left {
		for (k, v) in obj.iter() {
			map.entry(k.clone()).or_insert_with(|| v.clone());
		}
	}
	if let Value::Object(obj) = right {
		for (k, v) in obj.iter() {
			map.entry(k.clone()).or_insert_with(|| v.clone());
		}
	}
	Value::Object(Object(map))
}

pub(crate) fn merge_left_null(left: &Value, left_alias: &str, right_alias: &str) -> Value {
	let mut map = BTreeMap::new();
	map.insert(left_alias.to_string(), left.clone());
	map.insert(right_alias.to_string(), Value::Null);
	if let Value::Object(obj) = left {
		for (k, v) in obj.iter() {
			map.entry(k.clone()).or_insert_with(|| v.clone());
		}
	}
	Value::Object(Object(map))
}

pub(crate) fn merge_right_null(right: &Value, left_alias: &str, right_alias: &str) -> Value {
	let mut map = BTreeMap::new();
	map.insert(left_alias.to_string(), Value::Null);
	map.insert(right_alias.to_string(), right.clone());
	if let Value::Object(obj) = right {
		for (k, v) in obj.iter() {
			map.entry(k.clone()).or_insert_with(|| v.clone());
		}
	}
	Value::Object(Object(map))
}
