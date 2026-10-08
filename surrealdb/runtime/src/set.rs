use std::ops::{Bound, ControlFlow};

use anyhow::Result;
use common::range::TypedRange;
use reblessive::tree::Stk;
use surrealdb_expr::expr::Error;
use surrealdb_expr::val::{Closure, ClosureEvaluator, Set, Value, check_value_depth};

use crate::args::Optional;
use crate::fuel;

/// Continue a walk.
const ON: ControlFlow<()> = ControlFlow::Continue(());

/// Define each synchronous function as its walk driven to its end at once
/// ([`fuel::now`]).
macro_rules! now {
	($($name:ident = $walk:ident($args:ty);)+) => {
		$(
			pub fn $name(args: $args) -> Result<Value> {
				fuel::now($walk(args))
			}
		)+
	};
}

now! {
	add = add_walk((Set, Value));
	remove = remove_walk((Set, Value));
	union = union_walk((Set, Set));
	intersect = intersect_walk((Set, Set));
	difference = difference_walk((Set, Set));
	complement = complement_walk((Set, Set));
	at = at_walk((Set, i64));
	flatten = flatten_walk((Set,));
	join = join_walk((Set, String));
	max = max_walk((Set,));
	min = min_walk((Set,));
	slice = slice_walk((Set, Optional<Value>, Optional<i64>));
}

/// The elements of `value`: those of an array or a set, or the value itself.
fn elements(value: Value) -> Vec<Value> {
	match value {
		Value::Array(values) => values.0,
		Value::Set(values) => values.into_iter().collect(),
		value => vec![value],
	}
}

/// The elements of `set` that `other` holds, or does not hold when `held`
/// is false.
async fn kept(set: Set, other: &Set, held: bool) -> Set {
	let mut out = Set::new();
	fuel::walk(set, |value| {
		if other.contains(&value) == held {
			out.insert(value);
		}
		ON
	})
	.await;
	out
}

/// Add value(s) to a set
pub async fn add_walk((mut set, value): (Set, Value)) -> Result<Value> {
	fuel::walk(elements(value), |value| {
		set.insert(value);
		ON
	})
	.await;
	Ok(set.into())
}

/// Remove value(s) from a set
pub async fn remove_walk((mut set, value): (Set, Value)) -> Result<Value> {
	fuel::walk(elements(value), |value| {
		set.remove(&value);
		ON
	})
	.await;
	Ok(set.into())
}

/// Return the union of two sets (A ∪ B)
pub async fn union_walk((mut set1, set2): (Set, Set)) -> Result<Value> {
	fuel::walk(set2, |value| {
		set1.insert(value);
		ON
	})
	.await;
	Ok(set1.into())
}

/// Return the intersection of two sets (A ∩ B)
pub async fn intersect_walk((set1, set2): (Set, Set)) -> Result<Value> {
	Ok(kept(set1, &set2, true).await.into())
}

/// Return the symmetric difference of two sets (A △ B)
pub async fn difference_walk((set1, set2): (Set, Set)) -> Result<Value> {
	let mut out = Set::new();
	fuel::walk(set1.iter(), |value| {
		if !set2.contains(value) {
			out.insert(value.clone());
		}
		ON
	})
	.await;
	fuel::walk(set2, |value| {
		if !set1.contains(&value) {
			out.insert(value);
		}
		ON
	})
	.await;
	Ok(out.into())
}

/// Return the relative complement (A \ B)
pub async fn complement_walk((set1, set2): (Set, Set)) -> Result<Value> {
	Ok(kept(set1, &set2, false).await.into())
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
	(stk, ev): (&mut Stk, Option<&dyn ClosureEvaluator>),
	(set, Optional(check)): (Set, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(ev) = ev {
				for arg in set {
					if ev.invoke(stk, &closure, vec![arg]).await?.is_truthy() {
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
		Some(value) => (!fuel::any(set.iter(), |v| *v != value).await).into(),
		None => (!fuel::any(set.iter(), |v| !v.is_truthy()).await).into(),
	})
}

/// Check if any element in the set matches a condition
pub async fn any(
	(stk, ev): (&mut Stk, Option<&dyn ClosureEvaluator>),
	(set, Optional(check)): (Set, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(ev) = ev {
				for arg in set {
					if ev.invoke(stk, &closure, vec![arg]).await?.is_truthy() {
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
		None => fuel::any(set.iter(), Value::is_truthy).await.into(),
	})
}

/// Access element at a specific position in the set (in BTree order)
pub async fn at_walk((set, i): (Set, i64)) -> Result<Value> {
	let mut idx = i;
	if idx < 0 {
		idx += set.len() as i64;
	}
	if idx < 0 {
		return Ok(Value::None);
	}
	let idx = idx as usize;
	let found = fuel::find(set.iter().enumerate(), |&(at, _)| at == idx).await;
	Ok(found.map(|(_, value)| value.clone()).unwrap_or(Value::None))
}

/// Filter elements in the set that match a condition
pub async fn filter(
	(stk, ev): (&mut Stk, Option<&dyn ClosureEvaluator>),
	(set, check): (Set, Value),
) -> Result<Value> {
	Ok(match check {
		Value::Closure(closure) => {
			if let Some(ev) = ev {
				let mut res = Set::new();
				for arg in set {
					if ev.invoke(stk, &closure, vec![arg.clone()]).await?.is_truthy() {
						res.insert(arg);
					}
				}
				res.into()
			} else {
				Value::None
			}
		}
		value => {
			let mut out = Set::new();
			fuel::walk(set, |v| {
				if v == value {
					out.insert(v);
				}
				ON
			})
			.await;
			out.into()
		}
	})
}

/// Find the first element in the set matching a condition (in BTree order)
pub async fn find(
	(stk, ev): (&mut Stk, Option<&dyn ClosureEvaluator>),
	(set, value): (Set, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(ev) = ev {
				for arg in set {
					if ev.invoke(stk, &closure, vec![arg.clone()]).await?.is_truthy() {
						return Ok(arg);
					}
				}
				Value::None
			} else {
				Value::None
			}
		}
		value => fuel::find(set, |v| *v == value).await.unwrap_or(Value::None),
	})
}

/// Get the first element in the set (minimum in BTree order)
pub fn first((set,): (Set,)) -> Result<Value> {
	Ok(set.iter().next().cloned().unwrap_or(Value::None))
}

/// Flatten nested sets and arrays into a single set
pub async fn flatten_walk((set,): (Set,)) -> Result<Value> {
	let mut out = Set::new();
	for value in set {
		match value {
			Value::Array(_) | Value::Set(_) => {
				let inner = elements(value);
				fuel::walk(inner, |item| {
					out.insert(item);
					ON
				})
				.await;
			}
			value => {
				out.insert(value);
			}
		}
		fuel::tick(1).await;
	}
	Ok(out.into())
}

/// Fold over the set with an accumulator
pub async fn fold(
	(stk, ev): (&mut Stk, Option<&dyn ClosureEvaluator>),
	(set, init, mapper): (Set, Value, Box<Closure>),
) -> Result<Value> {
	if let Some(ev) = ev {
		let mut accum = init;
		for val in set {
			accum = ev.invoke(stk, &mapper, vec![accum, val]).await?;
			// SECURITY: bound the accumulator's nesting depth. Like `array::fold`,
			// this threads a caller-controlled accumulator through a flat loop, so
			// a wrapping closure grows its depth without bound and overflows the
			// stack on the value's recursive drop/clone/encode. See
			// `crate::array::fold` for the full rationale.
			check_value_depth(&accum)?;
		}
		Ok(accum)
	} else {
		Ok(Value::None)
	}
}

/// Join set elements into a string with a separator
pub async fn join_walk((set, sep): (Set, String)) -> Result<Value> {
	let mut out = String::new();
	fuel::walk(set.into_iter().enumerate(), |(at, value)| {
		if at > 0 {
			out.push_str(&sep);
		}
		out.push_str(&value.into_raw_string());
		ON
	})
	.await;
	Ok(out.into())
}

/// Get the last element in the set (maximum in BTree order)
pub fn last((set,): (Set,)) -> Result<Value> {
	Ok(set.iter().last().cloned().unwrap_or(Value::None))
}

/// Map over the set elements, returning a new set
pub async fn map(
	(stk, ev): (&mut Stk, Option<&dyn ClosureEvaluator>),
	(set, mapper): (Set, Box<Closure>),
) -> Result<Value> {
	if let Some(ev) = ev {
		let mut res = Set::new();
		for arg in set {
			res.insert(ev.invoke(stk, &mapper, vec![arg]).await?);
		}
		Ok(res.into())
	} else {
		Ok(Value::None)
	}
}

/// Get the maximum value in the set
pub async fn max_walk((set,): (Set,)) -> Result<Value> {
	let max = fuel::fold(set, None, |max: Option<Value>, value| match max {
		Some(max) if max > value => Some(max),
		_ => Some(value),
	})
	.await;
	Ok(max.unwrap_or(Value::None))
}

/// Get the minimum value in the set
pub async fn min_walk((set,): (Set,)) -> Result<Value> {
	let min = fuel::fold(set, None, |min: Option<Value>, value| match min {
		Some(min) if min <= value => Some(min),
		_ => Some(value),
	})
	.await;
	Ok(min.unwrap_or(Value::None))
}

/// Reduce the set using a closure (uses first element as initial value)
pub async fn reduce(
	(stk, ev): (&mut Stk, Option<&dyn ClosureEvaluator>),
	(set, mapper): (Set, Box<Closure>),
) -> Result<Value> {
	if let Some(ev) = ev {
		match set.len() {
			0 => Ok(Value::None),
			1 => {
				let Some(val) = set.into_iter().next() else {
					return Err(Error::InvalidFunctionArguments {
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
					accum = ev.invoke(stk, &mapper, vec![accum, val]).await?;
					// SECURITY: bound the accumulator's nesting depth, as in
					// `crate::array::fold`.
					check_value_depth(&accum)?;
				}
				Ok(accum)
			}
		}
	} else {
		Ok(Value::None)
	}
}

/// Extract a range of elements from the set by position (in BTree order)
pub async fn slice_walk(
	(set, Optional(range_start), Optional(end)): (Set, Optional<Value>, Optional<i64>),
) -> Result<Value> {
	let Some(range_start) = range_start else {
		return Ok(set.into());
	};

	let range = if let Some(end) = end {
		let start =
			range_start.coerce_to::<i64>().map_err(|e| Error::InvalidFunctionArguments {
				name: String::from("set::slice"),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?;

		TypedRange {
			start: Bound::Included(start),
			end: Bound::Excluded(end),
		}
	} else if let Value::Range(range) = range_start {
		range.coerce_to_typed::<i64>().map_err(|e| Error::InvalidFunctionArguments {
			name: String::from("set::slice"),
			message: format!("Range was the wrong type. {e}"),
		})?
	} else {
		let start =
			range_start.coerce_to::<i64>().map_err(|e| Error::InvalidFunctionArguments {
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
	fuel::walk(set.into_iter().enumerate(), |(i, value)| {
		if i >= start && i <= end {
			result.insert(value);
			ON
		} else if i > end {
			ControlFlow::Break(())
		} else {
			ON
		}
	})
	.await;

	Ok(result.into())
}
