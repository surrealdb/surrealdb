use std::cmp::Ordering;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::mem::{self};
use std::ops::{Bound, ControlFlow};

use anyhow::{Result, ensure};
use common::fail;
use common::range::TypedRange;
use surrealdb_cnf::GENERATION_ALLOCATION_LIMIT;
use surrealdb_expr::expr::Error;
use surrealdb_expr::val::{Array, Closure, ClosureEvaluator, Value, check_value_depth};

use crate::args::{Optional, Rest};
use crate::fuel::{self, units};

/// Continue a walk.
const ON: ControlFlow<()> = ControlFlow::Continue(());

/// Define each synchronous function as its walk driven to its end at once
/// ([`fuel::now`]): the units the walk charges stand, and the executor yields
/// at its next yield point.
macro_rules! now {
	($($(#[$attr:meta])* $name:ident = $walk:ident($args:ty);)+) => {
		$(
			$(#[$attr])*
			pub fn $name(args: $args) -> Result<Value> {
				fuel::now($walk(args))
			}
		)+
	};
}

now! {
	add = add_walk((Array, Value));
	boolean_not = boolean_not_walk((Array,));
	boolean_or = boolean_or_walk((Array, Array));
	boolean_and = boolean_and_walk((Array, Array));
	boolean_xor = boolean_xor_walk((Array, Array));
	clump = clump_walk((Array, i64));
	combine = combine_walk((Array, Array));
	complement = complement_walk((Array, Array));
	concat = concat_walk(Rest<Array>);
	difference = difference_walk((Array, Array));
	distinct = distinct_walk((Array,));
	fill = fill_walk((Array, Value, Optional<Value>, Optional<i64>));
	flatten = flatten_walk((Array,));
	group = group_walk((Array,));
	intersect = intersect_walk((Array, Array));
	join = join_walk((Array, String));
	logical_and = logical_and_walk((Array, Array));
	logical_or = logical_or_walk((Array, Array));
	logical_xor = logical_xor_walk((Array, Array));
	matches = matches_walk((Array, Value));
	max = max_walk((Array,));
	min = min_walk((Array,));
	range = range_walk((Value, Optional<i64>));
	sequence = sequence_walk((i64, Optional<i64>));
	repeat = repeat_walk((Value, i64));
	shuffle = shuffle_walk((Array,));
	sort = sort_walk((Array, Optional<Value>));
	sort_natural = sort_natural_walk((Array, Optional<Value>));
	#[cfg(feature = "collate")]
	sort_lexical = sort_lexical_walk((Array, Optional<Value>));
	#[cfg(feature = "collate")]
	sort_natural_lexical = sort_natural_lexical_walk((Array, Optional<Value>));
	transpose = transpose_walk((Array,));
	union = union_walk((Array, Array));
	windows = windows_walk((Array, i64));
}

/// The distinct elements of `values`, in the order each first appears.
async fn distinct_of(values: Vec<Value>) -> Vec<Value> {
	#[expect(clippy::mutable_key_type)]
	let mut seen = HashSet::with_capacity(values.len());
	let mut out = Vec::with_capacity(values.len());
	fuel::walk(values.iter(), |value| {
		if seen.insert(value) {
			out.push(value.clone());
		}
		ON
	})
	.await;
	out
}

/// `values` with each array among them replaced by its elements.
async fn flattened(values: Vec<Value>) -> Vec<Value> {
	let mut out = Vec::with_capacity(values.len());
	for value in values {
		match value {
			Value::Array(inner) => fuel::extend(&mut out, inner.0).await,
			value => out.push(value),
		}
		fuel::tick(1).await;
	}
	out
}

/// Where `value` first appears in `values`.
async fn position<'a>(values: impl IntoIterator<Item = &'a Value>, value: &Value) -> Option<usize> {
	fuel::find(values.into_iter().enumerate(), |&(_, held)| held == value).await.map(|(at, _)| at)
}

/// Returns an error if an array of this length is too much to allocate.
fn limit(name: &str, n: usize) -> Result<(), Error> {
	if n > *GENERATION_ALLOCATION_LIMIT {
		Err(Error::InvalidFunctionArguments {
			name: name.to_owned(),
			message: format!("Output must not exceed {} bytes.", *GENERATION_ALLOCATION_LIMIT),
		})
	} else {
		Ok(())
	}
}

pub async fn add_walk((mut array, value): (Array, Value)) -> Result<Value> {
	let values = match value {
		Value::Array(values) => values.0,
		value => vec![value],
	};
	for value in values {
		if !fuel::any(array.iter(), |held| *held == value).await {
			array.0.push(value)
		}
		fuel::tick(1).await;
	}
	Ok(array.into())
}

pub async fn all(
	ev: Option<&dyn ClosureEvaluator>,
	(array, Optional(check)): (Array, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(ev) = ev {
				for arg in array {
					if ev.invoke(&closure, vec![arg]).await?.is_truthy() {
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
		Some(value) => (!fuel::any(array.iter(), |v| *v != value).await).into(),
		None => (!fuel::any(array.iter(), |v| !v.is_truthy()).await).into(),
	})
}

pub async fn any(
	ev: Option<&dyn ClosureEvaluator>,
	(array, Optional(check)): (Array, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(ev) = ev {
				for arg in array {
					if ev.invoke(&closure, vec![arg]).await?.is_truthy() {
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
		Some(value) => fuel::any(array.iter(), |v| *v == value).await.into(),
		None => fuel::any(array.iter(), Value::is_truthy).await.into(),
	})
}

pub fn append((mut array, value): (Array, Value)) -> Result<Value> {
	array.push(value);
	Ok(array.into())
}

pub fn at((array, i): (Array, i64)) -> Result<Value> {
	let mut idx = i as usize;
	if i < 0 {
		idx = (array.len() as i64 + i) as usize;
	}
	Ok(array.get(idx).cloned().unwrap_or_default())
}

pub async fn boolean_not_walk((mut array,): (Array,)) -> Result<Value> {
	fuel::walk(array.iter_mut(), |v| {
		*v = (!v.is_truthy()).into();
		ON
	})
	.await;
	Ok(array.into())
}

pub async fn boolean_or_walk((lh, rh): (Array, Array)) -> Result<Value> {
	let (mut res, comp) = if lh.len() < rh.len() {
		(rh, lh)
	} else {
		(lh, rh)
	};

	let comp_len = comp.len();
	fuel::walk(comp.into_iter().enumerate(), |(idx, i)| {
		res[idx] = (res[idx].is_truthy() || i.is_truthy()).into();
		ON
	})
	.await;

	fuel::walk(&mut res[comp_len..], |i| {
		*i = i.is_truthy().into();
		ON
	})
	.await;

	Ok(res.into())
}

pub async fn boolean_and_walk((lh, rh): (Array, Array)) -> Result<Value> {
	let (mut res, comp) = if lh.len() < rh.len() {
		(rh, lh)
	} else {
		(lh, rh)
	};

	let comp_len = comp.len();
	fuel::walk(comp.into_iter().enumerate(), |(idx, i)| {
		res[idx] = (res[idx].is_truthy() && i.is_truthy()).into();
		ON
	})
	.await;

	fuel::walk(&mut res[comp_len..], |i| {
		*i = Value::Bool(false);
		ON
	})
	.await;

	Ok(res.into())
}

pub async fn boolean_xor_walk((lh, rh): (Array, Array)) -> Result<Value> {
	let (mut res, comp) = if lh.len() < rh.len() {
		(rh, lh)
	} else {
		(lh, rh)
	};

	let comp_len = comp.len();
	fuel::walk(comp.into_iter().enumerate(), |(idx, i)| {
		res[idx] = (res[idx].is_truthy() ^ i.is_truthy()).into();
		ON
	})
	.await;

	fuel::walk(&mut res[comp_len..], |i| {
		*i = i.is_truthy().into();
		ON
	})
	.await;

	Ok(res.into())
}

pub async fn clump_walk((array, clump_size): (Array, i64)) -> Result<Value> {
	let clump_size = clump_size.max(0) as usize;
	ensure!(
		clump_size >= 1,
		Error::InvalidFunctionArguments {
			name: "array::clump".to_string(),
			message: "The second argument must be an integer greater than 0".to_string(),
		}
	);
	let mut out = Vec::with_capacity(array.len().div_ceil(clump_size));
	for chunk in array.0.chunks(clump_size) {
		out.push(Value::from(chunk.to_vec()));
		fuel::tick(units(chunk.len())).await;
	}
	Ok(out.into())
}

pub async fn combine_walk((array, other): (Array, Array)) -> Result<Value> {
	let mut out = Vec::with_capacity(array.len().saturating_mul(other.len()));
	for a in array.iter() {
		let pairs = fuel::map(other.iter(), |b| Value::from(vec![a.clone(), b.clone()])).await;
		out.extend(pairs);
		fuel::tick(1).await;
	}
	Ok(out.into())
}

#[expect(clippy::mutable_key_type)]
pub async fn complement_walk((array, other): (Array, Array)) -> Result<Value> {
	let mut set = BTreeSet::new();
	fuel::walk(other.iter(), |value| {
		set.insert(value);
		ON
	})
	.await;
	let mut out = Vec::with_capacity(array.len());
	fuel::walk(array, |value| {
		if !set.contains(&value) {
			out.push(value);
		}
		ON
	})
	.await;
	Ok(out.into())
}

pub async fn concat_walk(Rest(arrays): Rest<Array>) -> Result<Value> {
	let len = arrays.iter().map(Array::len).sum();
	limit("array::concat", mem::size_of::<Value>().saturating_mul(len))?;
	let mut arr = Vec::with_capacity(len);
	for val in arrays {
		fuel::extend(&mut arr, val.0).await;
	}
	Ok(arr.into())
}

pub async fn difference_walk((array, other): (Array, Array)) -> Result<Value> {
	let mut out = Vec::with_capacity(array.len() + other.len());
	let mut other = VecDeque::from(other.0);
	for value in array {
		match position(&other, &value).await {
			Some(at) => {
				other.remove(at);
			}
			None => out.push(value),
		}
		fuel::tick(1).await;
	}
	out.extend(other);
	Ok(out.into())
}

pub async fn distinct_walk((array,): (Array,)) -> Result<Value> {
	Ok(distinct_of(array.0).await.into())
}

pub async fn fill_walk(
	(mut array, value, Optional(range_start), Optional(end)): (
		Array,
		Value,
		Optional<Value>,
		Optional<i64>,
	),
) -> Result<Value> {
	let Some(range_start) = range_start else {
		fuel::walk(array.iter_mut(), |held| {
			*held = value.clone();
			ON
		})
		.await;
		return Ok(array.into());
	};

	let range = if let Some(end) = end {
		let start =
			range_start.coerce_to::<i64>().map_err(|e| Error::InvalidFunctionArguments {
				name: String::from("array::fill"),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?;

		TypedRange::from_range(start..end)
	} else if range_start.is_range() {
		// Condition checked above, cannot fail
		let range = range_start.into_range().expect("is_range() check passed");
		range.coerce_to_typed::<i64>().map_err(|e| Error::InvalidFunctionArguments {
			name: String::from("array::fill"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?
	} else {
		let start =
			range_start.coerce_to::<i64>().map_err(|e| Error::InvalidFunctionArguments {
				name: String::from("array::fill"),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?;
		TypedRange::from_range(start..)
	};

	let array_len = array.len() as i64;

	let start = match range.start {
		Bound::Included(x) => {
			if x < 0 {
				array_len.saturating_add(x).max(0) as usize
			} else {
				x as usize
			}
		}
		Bound::Excluded(x) => {
			if x < 0 {
				array_len.saturating_add(x).saturating_add(1).max(0) as usize
			} else {
				x.saturating_add(1) as usize
			}
		}
		Bound::Unbounded => 0,
	};

	if start >= array.len() {
		return Ok(array.into());
	}

	let end = match range.end {
		Bound::Included(x) => {
			if x < 0 {
				array_len.saturating_add(x).clamp(0, array_len) as usize
			} else {
				x.min(array_len) as usize
			}
		}
		Bound::Excluded(x) => {
			if x < 0 {
				let end = array_len.saturating_add(x).min(array_len).saturating_sub(1);
				if end < start as i64 {
					return Ok(array.into());
				}
				end as usize
			} else {
				if x <= start as i64 {
					return Ok(array.into());
				}
				x.min(array_len).saturating_sub(1) as usize
			}
		}
		Bound::Unbounded => array.len() - 1,
	};

	if end < start {
		return Ok(array.into());
	}

	fuel::walk(&mut array[start..=end], |held| {
		*held = value.clone();
		ON
	})
	.await;

	Ok(array.into())
}

pub async fn filter(
	ev: Option<&dyn ClosureEvaluator>,
	(array, check): (Array, Value),
) -> Result<Value> {
	Ok(match check {
		Value::Closure(closure) => {
			if let Some(ev) = ev {
				let mut res = Vec::with_capacity(array.len());
				for arg in array {
					if ev.invoke(&closure, vec![arg.clone()]).await?.is_truthy() {
						res.push(arg)
					}
				}
				Value::from(res)
			} else {
				Value::None
			}
		}
		value => {
			let mut out = Vec::new();
			fuel::walk(array, |v| {
				if v == value {
					out.push(v);
				}
				ON
			})
			.await;
			out.into()
		}
	})
}

pub async fn filter_index(
	ev: Option<&dyn ClosureEvaluator>,
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(ev) = ev {
				let mut res = Vec::with_capacity(array.len());
				for (i, arg) in array.into_iter().enumerate() {
					if ev.invoke(&closure, vec![arg]).await?.is_truthy() {
						res.push(Value::from(i as i64));
					}
				}
				Value::from(res)
			} else {
				Value::None
			}
		}
		value => {
			let mut out = Vec::new();
			fuel::walk(array.iter().enumerate(), |(i, v)| {
				if *v == value {
					out.push(Value::from(i));
				}
				ON
			})
			.await;
			out.into()
		}
	})
}

pub async fn find(
	ev: Option<&dyn ClosureEvaluator>,
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(ev) = ev {
				for arg in array {
					if ev.invoke(&closure, vec![arg.clone()]).await?.is_truthy() {
						return Ok(arg);
					}
				}
				Value::None
			} else {
				Value::None
			}
		}
		value => fuel::find(array, |v| *v == value).await.unwrap_or(Value::None),
	})
}

pub async fn find_index(
	ev: Option<&dyn ClosureEvaluator>,
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(ev) = ev {
				for (i, arg) in array.into_iter().enumerate() {
					if ev.invoke(&closure, vec![arg]).await?.is_truthy() {
						return Ok(i.into());
					}
				}
				Value::None
			} else {
				Value::None
			}
		}
		value => position(array.iter(), &value).await.map(Value::from).unwrap_or(Value::None),
	})
}

pub fn first((array,): (Array,)) -> Result<Value> {
	if let [first, ..] = &array[0..] {
		Ok(first.to_owned())
	} else {
		Ok(Value::None)
	}
}

pub async fn flatten_walk((array,): (Array,)) -> Result<Value> {
	Ok(flattened(array.0).await.into())
}

pub async fn fold(
	ev: Option<&dyn ClosureEvaluator>,
	(array, init, mapper): (Array, Value, Box<Closure>),
) -> Result<Value> {
	if let Some(ev) = ev {
		let mut accum = init;
		for (i, val) in array.into_iter().enumerate() {
			accum = ev.invoke(&mapper, vec![accum, val, i.into()]).await?;
			// SECURITY: fold threads a caller-controlled accumulator through a
			// flat loop, so a closure that wraps it each turn (`|$a, $_| [$a]`)
			// grows the value's nesting depth without bound. The per-compute
			// `dive` budget resets every iteration and does not constrain this,
			// and the value need never be stored, so the storage-write guard does
			// not see it either - it is built purely in memory. Left unchecked the
			// next iteration's clone of the accumulator (reading `$a` to wrap it)
			// recurses deep enough to overflow the stack, an unconditional process
			// abort under the release profile's `panic = "abort"`. Checking each
			// iteration keeps the accumulator within one closure's nesting of the
			// cap and rejects the attack before it is ever built that deep. The
			// walk early-bails as soon as the cap is exceeded, so the deep-narrow
			// attack costs O(cap); a fold that legitimately grows a large flat
			// collection is walked in full, O(accumulator size), but that is the
			// same order as the work the fold body already does to build it, a
			// constant-factor overhead rather than a change in complexity. For the
			// common scalar accumulator (sum, product, count) the walk is O(1).
			check_value_depth(&accum)?;
		}
		Ok(accum)
	} else {
		Ok(Value::None)
	}
}

pub async fn group_walk((array,): (Array,)) -> Result<Value> {
	Ok(distinct_of(flattened(array.0).await).await.into())
}

pub fn insert((mut array, value, Optional(index)): (Array, Value, Optional<i64>)) -> Result<Value> {
	match index {
		Some(mut index) => {
			// Negative index means start from the back
			if index < 0 {
				index += array.len() as i64;
			}
			// Invalid index so return array unaltered
			if index > array.len() as i64 || index < 0 {
				return Ok(array.into());
			}
			// Insert the value into the array
			array.insert(index as usize, value);
			// Return the array
			Ok(array.into())
		}
		None => {
			array.push(value);
			Ok(array.into())
		}
	}
}

pub async fn intersect_walk((array, mut other): (Array, Array)) -> Result<Value> {
	let mut out = Vec::new();
	for value in array {
		if let Some(at) = position(other.iter(), &value).await {
			other.remove(at);
			out.push(value);
		}
		fuel::tick(1).await;
	}
	Ok(out.into())
}

pub fn is_empty((array,): (Array,)) -> Result<Value> {
	Ok(array.is_empty().into())
}

pub async fn join_walk((arr, sep): (Array, String)) -> Result<Value> {
	let mut out = String::new();
	fuel::walk(arr.into_iter().enumerate(), |(at, value)| {
		if at > 0 {
			out.push_str(&sep);
		}
		out.push_str(&value.into_raw_string());
		ON
	})
	.await;
	Ok(out.into())
}

pub fn last((array,): (Array,)) -> Result<Value> {
	if let [.., last] = &array[0..] {
		Ok(last.to_owned())
	} else {
		Ok(Value::None)
	}
}

pub fn len((array,): (Array,)) -> Result<Value> {
	Ok(array.len().into())
}

pub async fn logical_and_walk((mut lh, mut rh): (Array, Array)) -> Result<Value> {
	if lh.len() < rh.len() {
		let lh_len = lh.len();
		fuel::walk(lh.into_iter().enumerate(), |(idx, b)| {
			if !b.is_truthy() {
				rh[idx] = b;
			}
			ON
		})
		.await;
		fuel::walk(&mut rh[lh_len..], |i| {
			*i = Value::Null;
			ON
		})
		.await;
		Ok(rh.into())
	} else {
		let rh_len = rh.len();
		fuel::walk(rh.into_iter().enumerate(), |(idx, b)| {
			if lh[idx].is_truthy() {
				lh[idx] = b
			}
			ON
		})
		.await;
		fuel::walk(&mut lh[rh_len..], |i| {
			if i.is_truthy() {
				*i = Value::Null
			}
			ON
		})
		.await;
		Ok(lh.into())
	}
}

pub async fn logical_or_walk((mut lh, mut rh): (Array, Array)) -> Result<Value> {
	if lh.len() < rh.len() {
		fuel::walk(lh.into_iter().enumerate(), |(idx, b)| {
			if b.is_truthy() {
				rh[idx] = b;
			}
			ON
		})
		.await;
		Ok(rh.into())
	} else {
		let rh_len = rh.len();
		fuel::walk(rh.into_iter().enumerate(), |(idx, b)| {
			if !lh[idx].is_truthy() {
				lh[idx] = b
			}
			ON
		})
		.await;
		fuel::walk(&mut lh[rh_len..], |i| {
			if !i.is_truthy() {
				*i = Value::Null
			}
			ON
		})
		.await;
		Ok(lh.into())
	}
}

pub async fn logical_xor_walk((mut lh, mut rh): (Array, Array)) -> Result<Value> {
	if lh.len() < rh.len() {
		let lh_len = lh.len();
		fuel::walk(lh.into_iter().enumerate(), |(idx, b)| {
			let v = b.is_truthy() ^ rh[idx].is_truthy();
			if b.is_truthy() == v {
				rh[idx] = b;
			} else if rh[idx].is_truthy() != v {
				rh[idx] = v.into();
			}
			ON
		})
		.await;
		fuel::walk(&mut rh[lh_len..], |i| {
			if !i.is_truthy() {
				*i = Value::Null;
			}
			ON
		})
		.await;
		Ok(rh.into())
	} else {
		fuel::walk(rh.into_iter().enumerate(), |(idx, b)| {
			let v = b.is_truthy() ^ lh[idx].is_truthy();
			if lh[idx].is_truthy() != v {
				if b.is_truthy() == v {
					lh[idx] = b;
				} else {
					lh[idx] = v.into();
				}
			}
			ON
		})
		.await;
		Ok(lh.into())
	}
}

pub async fn map(
	ev: Option<&dyn ClosureEvaluator>,
	(array, mapper): (Array, Box<Closure>),
) -> Result<Value> {
	if let Some(ev) = ev {
		let mut res = Vec::with_capacity(array.len());
		for (i, arg) in array.into_iter().enumerate() {
			res.push(ev.invoke(&mapper, vec![arg, i.into()]).await?);
		}
		Ok(res.into())
	} else {
		Ok(Value::None)
	}
}

pub async fn matches_walk((array, compare_val): (Array, Value)) -> Result<Value> {
	Ok(fuel::map(array.iter(), |value| Value::from(*value == compare_val)).await.into())
}

pub async fn max_walk((array,): (Array,)) -> Result<Value> {
	// The last of equal greatest elements, as `Iterator::max` answers.
	let max = fuel::fold(array, None, |max: Option<Value>, value| match max {
		Some(max) if max > value => Some(max),
		_ => Some(value),
	})
	.await;
	Ok(max.unwrap_or_default())
}

pub async fn min_walk((array,): (Array,)) -> Result<Value> {
	// The first of equal least elements, as `Iterator::min` answers.
	let min = fuel::fold(array, None, |min: Option<Value>, value| match min {
		Some(min) if min <= value => Some(min),
		_ => Some(value),
	})
	.await;
	Ok(min.unwrap_or_default())
}

pub fn pop((mut array,): (Array,)) -> Result<Value> {
	Ok(array.pop().unwrap_or(Value::None))
}

pub fn prepend((mut array, value): (Array, Value)) -> Result<Value> {
	array.insert(0, value);
	Ok(array.into())
}

pub fn push((mut array, value): (Array, Value)) -> Result<Value> {
	array.push(value);
	Ok(array.into())
}

pub async fn range_walk((start_range, Optional(end)): (Value, Optional<i64>)) -> Result<Value> {
	let range = if let Some(end) = end {
		let start =
			start_range.coerce_to::<i64>().map_err(|e| Error::InvalidFunctionArguments {
				name: String::from("array::range"),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?;

		TypedRange {
			start: Bound::Included(start),
			end: Bound::Excluded(end),
		}
	} else if start_range.is_range() {
		// Condition checked above, cannot fail
		let range = start_range.into_range().expect("is_range() check passed");
		range.coerce_to_typed::<i64>().map_err(|e| Error::InvalidFunctionArguments {
			name: String::from("array::range"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?
	} else {
		let start =
			start_range.coerce_to::<i64>().map_err(|e| Error::InvalidFunctionArguments {
				name: String::from("array::range"),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?;
		TypedRange {
			start: Bound::Included(start),
			end: Bound::Unbounded,
		}
	};

	limit(
		"array::range",
		mem::size_of::<Value>().saturating_mul(range.len().unwrap_or(usize::MAX)),
	)?;

	Ok(fuel::map(range.iter(), Value::from).await.into())
}

pub async fn sequence_walk((offset_len, Optional(len)): (i64, Optional<i64>)) -> Result<Value> {
	let (offset, len) = if let Some(len) = len {
		(offset_len, len)
	} else {
		(0, offset_len)
	};

	if len <= 0 {
		return Ok(Value::Array(Array(Vec::new())));
	}

	let end = offset.saturating_add(len - 1);
	let range = TypedRange::from_range(offset..=end);

	limit(
		"array::sequence",
		mem::size_of::<Value>().saturating_mul(range.len().unwrap_or(usize::MAX)),
	)?;
	Ok(fuel::map(range.iter(), Value::from).await.into())
}

pub async fn reduce(
	ev: Option<&dyn ClosureEvaluator>,
	(array, mapper): (Array, Box<Closure>),
) -> Result<Value> {
	if let Some(ev) = ev {
		match array.len() {
			0 => Ok(Value::None),
			1 => {
				let Some(val) = array.into_iter().next() else {
					fail!("Iterator should have an item at this point")
				};
				Ok(val)
			}
			_ => {
				// Get the first item
				let mut iter = array.into_iter();
				let Some(mut accum) = iter.next() else {
					return Ok(Value::None);
				};
				for (idx, val) in iter.enumerate() {
					accum = ev.invoke(&mapper, vec![accum, val, idx.into()]).await?;
					// SECURITY: bound the accumulator's nesting depth, as in
					// `fold` above; reduce threads it through the same flat loop.
					check_value_depth(&accum)?;
				}
				Ok(accum)
			}
		}
	} else {
		Ok(Value::None)
	}
}

pub fn remove((mut array, mut index): (Array, i64)) -> Result<Value> {
	// Negative index means start from the back
	if index < 0 {
		index += array.len() as i64;
	}
	// Invalid index so return array unaltered
	if index >= array.len() as i64 || index < 0 {
		return Ok(array.into());
	}
	// Remove the value from the array
	array.remove(index as usize);
	// Return the array
	Ok(array.into())
}

pub async fn repeat_walk((value, count): (Value, i64)) -> Result<Value> {
	let count = usize::try_from(count).map_err(|_| {
		anyhow::Error::new(Error::ArithmeticNegativeOverflow(format!("array::repeat({count})")))
	})?;
	limit("array::repeat", mem::size_of::<Value>().saturating_mul(count))?;
	Ok(fuel::map(0..count, |_| value.clone()).await.into())
}

pub fn reverse((mut array,): (Array,)) -> Result<Value> {
	array.reverse();
	Ok(array.into())
}

pub async fn shuffle_walk((mut array,): (Array,)) -> Result<Value> {
	use rand::Rng as _;
	// A Fisher-Yates shuffle from the back, a stretch of swaps at a time.
	let mut at = array.len();
	while at > 1 {
		let end = at.saturating_sub(fuel::next_stretch().await).max(1);
		{
			let mut rng = rand::rng();
			for i in (end..at).rev() {
				array.swap(i, rng.random_range(0..=i));
			}
		}
		fuel::tick(units(at - end)).await;
		at = end;
	}
	Ok(array.into())
}

pub fn slice(
	(mut array, Optional(range_start), Optional(end)): (Array, Optional<Value>, Optional<i64>),
) -> Result<Value> {
	let Some(range_start) = range_start else {
		return Ok(array.into());
	};

	let range = if let Some(end) = end {
		let start =
			range_start.coerce_to::<i64>().map_err(|e| Error::InvalidFunctionArguments {
				name: String::from("array::range"),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?;

		TypedRange {
			start: Bound::Included(start),
			end: Bound::Excluded(end),
		}
	} else if range_start.is_range() {
		// Condition checked above, cannot fail
		let range = range_start.into_range().expect("is_range() check passed");
		range.coerce_to_typed::<i64>().map_err(|e| Error::InvalidFunctionArguments {
			name: String::from("array::range"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?
	} else {
		let start =
			range_start.coerce_to::<i64>().map_err(|e| Error::InvalidFunctionArguments {
				name: String::from("array::range"),
				message: format!("Argument 1 was the wrong type. {e}"),
			})?;
		TypedRange {
			start: Bound::Included(start),
			end: Bound::Unbounded,
		}
	};

	let array_len = array.len() as i64;

	let start = match range.start {
		Bound::Included(x) => {
			if x < 0 {
				array_len.saturating_add(x).max(0) as usize
			} else {
				x as usize
			}
		}
		Bound::Excluded(x) => {
			if x < 0 {
				array_len.saturating_add(x).saturating_add(1).max(0) as usize
			} else {
				x.saturating_add(1) as usize
			}
		}
		Bound::Unbounded => 0,
	};

	if start >= array.len() {
		return Ok(Value::Array(Array::new()));
	}

	let end = match range.end {
		Bound::Included(x) => {
			if x < 0 {
				array_len.saturating_add(x).max(0) as usize
			} else {
				x as usize
			}
		}
		Bound::Excluded(x) => {
			if x < 0 {
				let end = array_len.saturating_add(x).saturating_sub(1);
				if end < start as i64 {
					return Ok(Value::Array(Array::new()));
				}
				end as usize
			} else {
				if x <= start as i64 {
					return Ok(Value::Array(Array::new()));
				}
				x.saturating_sub(1) as usize
			}
		}
		Bound::Unbounded => usize::MAX,
	};

	if end < start {
		return Ok(Value::Array(Array::new()));
	}

	let mut i = 0;
	array.retain(|_| {
		let res = i >= start && i <= end;
		i += 1;
		res
	});
	array.shrink_to_fit();

	Ok(array.into())
}

fn sort_as_asc(order: &Option<Value>) -> bool {
	match order {
		Some(Value::String(s)) if s.as_str() == "asc" => true,
		Some(Value::String(s)) if s.as_str() == "desc" => false,
		Some(Value::Bool(true)) => true,
		Some(Value::Bool(false)) => false,
		_ => true,
	}
}

/// `array` sorted by `cmp`, ascending, or descending unless `order` says
/// otherwise, a quantum at a time.
async fn sorted_by(
	array: Array,
	order: &Option<Value>,
	cmp: impl Fn(&Value, &Value) -> Ordering,
) -> Result<Value> {
	let quantum = fuel::quantum();
	let sorted = if sort_as_asc(order) {
		fuel::sort_by(array.0, |a, b| cmp(a, b), usize::MAX, quantum).await
	} else {
		fuel::sort_by(array.0, |a, b| cmp(b, a), usize::MAX, quantum).await
	};
	Ok(sorted.into())
}

pub async fn sort_walk((array, Optional(order)): (Array, Optional<Value>)) -> Result<Value> {
	sorted_by(array, &order, Value::cmp).await
}

pub async fn sort_natural_walk(
	(array, Optional(order)): (Array, Optional<Value>),
) -> Result<Value> {
	sorted_by(array, &order, |a, b| a.natural_cmp(b).unwrap_or(Ordering::Equal)).await
}

/// Needs the `collate` feature; without it this function is absent and
/// `surrealdb-core` answers a call with an error naming the feature.
#[cfg(feature = "collate")]
pub async fn sort_lexical_walk(
	(array, Optional(order)): (Array, Optional<Value>),
) -> Result<Value> {
	sorted_by(array, &order, |a, b| a.lexical_cmp(b).unwrap_or(Ordering::Equal)).await
}

/// Needs the `collate` feature, as `sort_lexical_walk` does.
#[cfg(feature = "collate")]
pub async fn sort_natural_lexical_walk(
	(array, Optional(order)): (Array, Optional<Value>),
) -> Result<Value> {
	sorted_by(array, &order, |a, b| a.natural_lexical_cmp(b).unwrap_or(Ordering::Equal)).await
}

pub fn swap((mut array, from, to): (Array, i64, i64)) -> Result<Value> {
	let min = 0;
	let max = array.len();
	let negative_max = -(max as isize);
	let from = from as isize;
	let to = to as isize;

	let from = match from {
		from if from < negative_max || from >= max as isize => {
			Err(Error::InvalidFunctionArguments {
				name: String::from("array::swap"),
				message: format!(
					"Argument 1 is out of range. Expected a number between {negative_max} and {max}"
				),
			})
		}
		from if negative_max <= from && from < min => Ok((from + max as isize) as usize),
		from => Ok(from as usize),
	}?;

	let to = match to {
		to if to < negative_max || to >= max as isize => Err(Error::InvalidFunctionArguments {
			name: String::from("array::swap"),
			message: format!(
				"Argument 2 is out of range. Expected a number between {negative_max} and {max}"
			),
		}),
		to if negative_max <= to && to < min => Ok((to + max as isize) as usize),
		to => Ok(to as usize),
	}?;

	array.swap(from, to);
	Ok(array.into())
}

pub async fn transpose_walk((array,): (Array,)) -> Result<Value> {
	if array.is_empty() {
		return Ok(array.into());
	}
	let height =
		fuel::fold(array.iter(), 0, |height, x| height.max(x.as_array().map_or(1, Array::len)))
			.await;
	let width = array.len();
	let mut rows = Vec::with_capacity(height);
	for _ in 0..height {
		rows.push(vec![Value::None; width]);
		fuel::tick(units(width)).await;
	}
	for (idx, item) in array.into_iter().enumerate() {
		match item {
			Value::Array(inner) => {
				fuel::tick(units(inner.len())).await;
				for (jdx, value) in inner.into_iter().enumerate() {
					rows[jdx][idx] = value;
				}
			}
			value => {
				rows[0][idx] = value;
			}
		}
		fuel::tick(1).await;
	}
	Ok(rows.into_iter().map(Value::from).collect::<Vec<_>>().into())
}

pub async fn union_walk((mut array, other): (Array, Array)) -> Result<Value> {
	fuel::extend(&mut array.0, other.0).await;
	Ok(distinct_of(array.0).await.into())
}

pub async fn windows_walk((array, window_size): (Array, i64)) -> Result<Value> {
	let window_size = window_size.max(0) as usize;
	ensure!(
		window_size >= 1,
		Error::InvalidFunctionArguments {
			name: "array::windows".to_string(),
			message: "The second argument must be an integer greater than 0".to_string(),
		}
	);
	let mut out = Vec::with_capacity(array.len().saturating_sub(window_size - 1));
	for window in array.0.windows(window_size) {
		out.push(Value::from(window.to_vec()));
		fuel::tick(units(window.len())).await;
	}
	Ok(out.into())
}

pub mod sort {

	use anyhow::Result;
	use surrealdb_expr::val::{Array, Value};

	use crate::fuel;

	pub fn asc(args: (Array,)) -> Result<Value> {
		fuel::now(asc_walk(args))
	}

	pub fn desc(args: (Array,)) -> Result<Value> {
		fuel::now(desc_walk(args))
	}

	pub async fn asc_walk((array,): (Array,)) -> Result<Value> {
		super::sorted_by(array, &None, Value::cmp).await
	}

	pub async fn desc_walk((array,): (Array,)) -> Result<Value> {
		super::sorted_by(array, &Some(Value::Bool(false)), Value::cmp).await
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_expr::val::{Array, Value};

	use super::{at, first, join, last, slice};
	use crate::args::Optional;

	#[test]
	fn array_slice() {
		#[track_caller]
		fn test(initial: &[u8], beg: Option<i64>, end: Option<i64>, expected: &[u8]) {
			let initial_values =
				initial.iter().map(|n| Value::from(*n as i64)).collect::<Vec<_>>().into();
			let expected_values: Array =
				expected.iter().map(|n| Value::from(*n as i64)).collect::<Vec<_>>().into();
			assert_eq!(
				slice((initial_values, Optional(beg.map(Value::from)), Optional(end))).unwrap(),
				Value::from(expected_values)
			);
		}

		let array = b"abcdefg";
		test(array, None, None, array);
		test(array, Some(2), None, &array[2..]);
		test(array, Some(2), Some(3), &array[2..3]);
		test(array, Some(2), Some(-1), b"cdef");
		test(array, Some(-2), None, b"fg");
		test(array, Some(-4), Some(2), b"");
		test(array, Some(-4), Some(-1), b"def");
	}

	#[test]
	fn array_join() {
		fn test(arr: Array, sep: &str, expected: &str) {
			assert_eq!(join((arr, sep.to_string())).unwrap(), Value::from(expected));
		}

		test(Vec::<Value>::new().into(), ",", "");
		test(vec!["hello"].into(), ",", "hello");
		test(vec!["hello", "world"].into(), ",", "hello,world");
		test(vec!["again"; 512].into(), " and ", &vec!["again"; 512].join(" and "));
		test(
			vec![Value::from(true), Value::from(false), Value::from(true)].into(),
			" is ",
			"true is false is true",
		);
		test(
			vec![Value::from(3.56), Value::from(2.72), Value::from(1.61)].into(),
			" is not ",
			"3.56f is not 2.72f is not 1.61f",
		);
	}

	#[test]
	fn array_first() {
		#[allow(clippy::needless_pass_by_value)]
		fn test(arr: Array, expected: Value) {
			assert_eq!(first((arr,)).unwrap(), expected);
		}

		test(vec!["hello", "world"].into(), "hello".into());
		test(Array::new(), Value::None);
	}

	#[test]
	fn array_last() {
		#[allow(clippy::needless_pass_by_value)]
		fn test(arr: Array, expected: Value) {
			assert_eq!(last((arr,)).unwrap(), expected);
		}

		test(vec!["hello", "world"].into(), "world".into());
		test(Array::new(), Value::None);
	}

	#[test]
	fn array_at() {
		#[allow(clippy::needless_pass_by_value)]
		fn test(arr: Array, i: i64, expected: Value) {
			assert_eq!(at((arr, i)).unwrap(), expected);
		}
		test(vec!["hello", "world"].into(), -2, "hello".into());
		test(vec!["hello", "world"].into(), -3, Value::None);
	}
}
