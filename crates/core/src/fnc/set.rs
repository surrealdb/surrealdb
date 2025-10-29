use anyhow::Result;
use reblessive::tree::Stk;

use super::args::Optional;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::{Set, Value};

/// Add value(s) to a set
pub fn add((mut set, value): (Set, Value)) -> Result<Value> {
	match value {
		Value::Array(arr) => {
			for v in arr.0 {
				set.insert(v);
			}
			Ok(set.into())
		}
		Value::Set(other) => {
			for v in other.0 {
				set.insert(v);
			}
			Ok(set.into())
		}
		value => {
			set.insert(value);
			Ok(set.into())
		}
	}
}

/// Remove value(s) from a set
pub fn remove((mut set, value): (Set, Value)) -> Result<Value> {
	match value {
		Value::Array(arr) => {
			for v in arr.0 {
				set.remove(&v);
			}
			Ok(set.into())
		}
		Value::Set(other) => {
			for v in other.0 {
				set.remove(&v);
			}
			Ok(set.into())
		}
		value => {
			set.remove(&value);
			Ok(set.into())
		}
	}
}

/// Return the union of two sets (A ∪ B)
pub fn union((set1, set2): (Set, Set)) -> Result<Value> {
	Ok(set1.union(set2).into())
}

/// Return the intersection of two sets (A ∩ B)
pub fn intersect((set1, set2): (Set, Set)) -> Result<Value> {
	Ok(set1.intersection(&set2).into())
}

/// Return the symmetric difference of two sets (A △ B)
pub fn difference((set1, set2): (Set, Set)) -> Result<Value> {
	Ok(set1.symmetric_difference(set2).into())
}

/// Return the relative complement (A \ B)
pub fn complement((set1, set2): (Set, Set)) -> Result<Value> {
	Ok(set1.complement(set2).into())
}

/// Get the number of elements in the set
pub fn len((set,): (Set,)) -> Result<Value> {
	Ok(set.len().into())
}

/// Check if the set is empty
pub fn is_empty((set,): (Set,)) -> Result<Value> {
	Ok(set.is_empty().into())
}

/// Check if the set contains a value
pub fn contains((set, value): (Set, Value)) -> Result<Value> {
	Ok(set.contains(&value).into())
}

/// Check if all elements in the set match a condition
pub async fn all(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(set, Optional(check)): (Set, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(opt) = opt {
				for arg in set.into_iter() {
					if closure.invoke(stk, ctx, opt, doc, vec![arg]).await?.is_truthy() {
						continue;
					} else {
						return Ok(Value::Bool(false));
					}
				}
				Value::Bool(true)
			} else {
				Value::None
			}
		}
		Some(value) => set.iter().all(|v: &Value| *v == value).into(),
		None => set.iter().all(Value::is_truthy).into(),
	})
}

/// Check if any element in the set matches a condition
pub async fn any(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(set, Optional(check)): (Set, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(opt) = opt {
				for arg in set.into_iter() {
					if closure.invoke(stk, ctx, opt, doc, vec![arg]).await?.is_truthy() {
						return Ok(Value::Bool(true));
					} else {
						continue;
					}
				}
				Value::Bool(false)
			} else {
				Value::None
			}
		}
		Some(value) => set.contains(&value).into(),
		None => set.iter().any(Value::is_truthy).into(),
	})
}
