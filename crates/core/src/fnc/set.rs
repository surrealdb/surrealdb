use std::ops::Bound;

use anyhow::Result;
use reblessive::tree::Stk;

use super::args::Optional;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::val::range::TypedRange;
use crate::val::{Closure, Set, Value};

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
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(set, Optional(check)): (Set, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(opt) = opt {
				for arg in set {
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
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(set, Optional(check)): (Set, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(opt) = opt {
				for arg in set {
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

/// Access element at a specific position in the set (in BTree order)
pub fn at((set, i): (Set, i64)) -> Result<Value> {
	let mut idx = i;
	if idx < 0 {
		idx += set.len() as i64;
	}
	if idx < 0 {
		return Ok(Value::None);
	}
	Ok(set.iter().nth(idx as usize).cloned().unwrap_or(Value::None))
}

/// Filter elements in the set that match a condition
pub async fn filter(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(set, check): (Set, Value),
) -> Result<Value> {
	Ok(match check {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				let mut res = Set::new();
				for arg in set {
					if closure.invoke(stk, ctx, opt, doc, vec![arg.clone()]).await?.is_truthy() {
						res.insert(arg);
					}
				}
				res.into()
			} else {
				Value::None
			}
		}
		value => set.into_iter().filter(|v: &Value| *v == value).collect::<Set>().into(),
	})
}

/// Find the first element in the set matching a condition (in BTree order)
pub async fn find(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(set, value): (Set, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				for arg in set {
					if closure.invoke(stk, ctx, opt, doc, vec![arg.clone()]).await?.is_truthy() {
						return Ok(arg);
					}
				}
				Value::None
			} else {
				Value::None
			}
		}
		value => set.into_iter().find(|v: &Value| *v == value).unwrap_or(Value::None),
	})
}

/// Get the first element in the set (minimum in BTree order)
pub fn first((set,): (Set,)) -> Result<Value> {
	Ok(set.iter().next().cloned().unwrap_or(Value::None))
}

/// Flatten nested sets and arrays into a single set
pub fn flatten((set,): (Set,)) -> Result<Value> {
	Ok(set.flatten().into())
}

/// Fold over the set with an accumulator
pub async fn fold(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(set, init, mapper): (Set, Value, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		let mut accum = init;
		for val in set {
			accum = mapper.invoke(stk, ctx, opt, doc, vec![accum, val]).await?
		}
		Ok(accum)
	} else {
		Ok(Value::None)
	}
}

/// Join set elements into a string with a separator
pub fn join((set, sep): (Set, String)) -> Result<Value> {
	Ok(set.into_iter().map(Value::into_raw_string).collect::<Vec<_>>().join(&sep).into())
}

/// Get the last element in the set (maximum in BTree order)
pub fn last((set,): (Set,)) -> Result<Value> {
	Ok(set.iter().last().cloned().unwrap_or(Value::None))
}

/// Map over the set elements, returning a new set
pub async fn map(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(set, mapper): (Set, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		let mut res = Set::new();
		for arg in set {
			res.insert(mapper.invoke(stk, ctx, opt, doc, vec![arg]).await?);
		}
		Ok(res.into())
	} else {
		Ok(Value::None)
	}
}

/// Get the maximum value in the set
pub fn max((set,): (Set,)) -> Result<Value> {
	Ok(set.into_iter().max().unwrap_or(Value::None))
}

/// Get the minimum value in the set
pub fn min((set,): (Set,)) -> Result<Value> {
	Ok(set.into_iter().min().unwrap_or(Value::None))
}

/// Reduce the set using a closure (uses first element as initial value)
pub async fn reduce(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(set, mapper): (Set, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		match set.len() {
			0 => Ok(Value::None),
			1 => {
				let Some(val) = set.into_iter().next() else {
					return Err(Error::InvalidArguments {
						name: String::from("set::reduce"),
						message: String::from("Iterator should have an item at this point"),
					}
					.into());
				};
				Ok(val)
			}
			_ => {
				// Get the first item
				let mut iter = set.into_iter();
				let Some(mut accum) = iter.next() else {
					return Ok(Value::None);
				};
				for val in iter {
					accum = mapper.invoke(stk, ctx, opt, doc, vec![accum, val]).await?;
				}
				Ok(accum)
			}
		}
	} else {
		Ok(Value::None)
	}
}

/// Extract a range of elements from the set by position (in BTree order)
pub fn slice(
	(set, Optional(range_start), Optional(end)): (Set, Optional<Value>, Optional<i64>),
) -> Result<Value> {
	let Some(range_start) = range_start else {
		return Ok(set.into());
	};

	let range = if let Some(end) = end {
		let start = range_start.coerce_to::<i64>().map_err(|e| Error::InvalidArguments {
			name: String::from("set::slice"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?;

		TypedRange {
			start: Bound::Included(start),
			end: Bound::Excluded(end),
		}
	} else if let Value::Range(range) = range_start {
		range.coerce_to_typed::<i64>().map_err(|e| Error::InvalidArguments {
			name: String::from("set::slice"),
			message: format!("Range was the wrong type. {e}"),
		})?
	} else {
		let start = range_start.coerce_to::<i64>().map_err(|e| Error::InvalidArguments {
			name: String::from("set::slice"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?;
		TypedRange {
			start: Bound::Included(start),
			end: Bound::Unbounded,
		}
	};

	let set_len = set.len() as i64;

	let start = match range.start {
		Bound::Included(x) => {
			if x < 0 {
				set_len.saturating_add(x).max(0) as usize
			} else {
				x as usize
			}
		}
		Bound::Excluded(x) => {
			if x < 0 {
				set_len.saturating_add(x).saturating_add(1).max(0) as usize
			} else {
				x.saturating_add(1) as usize
			}
		}
		Bound::Unbounded => 0,
	};

	if start >= set.len() {
		return Ok(Value::Set(Set::new()));
	}

	let end = match range.end {
		Bound::Included(x) => {
			if x < 0 {
				set_len.saturating_add(x).max(0) as usize
			} else {
				x as usize
			}
		}
		Bound::Excluded(x) => {
			if x < 0 {
				let end = set_len.saturating_add(x).saturating_sub(1);
				if end < start as i64 {
					return Ok(Value::Set(Set::new()));
				}
				end as usize
			} else {
				if x <= start as i64 {
					return Ok(Value::Set(Set::new()));
				}
				x.saturating_sub(1) as usize
			}
		}
		Bound::Unbounded => usize::MAX,
	};

	if end < start {
		return Ok(Value::Set(Set::new()));
	}

	let mut result = Set::new();
	for (i, value) in set.into_iter().enumerate() {
		if i >= start && i <= end {
			result.insert(value);
		} else if i > end {
			break;
		}
	}

	Ok(result.into())
}
