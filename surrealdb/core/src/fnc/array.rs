use std::cmp::Ordering;
use std::mem::{self};
use std::ops::Bound;

use anyhow::{Result, ensure};
use rand::prelude::SliceRandom;
use reblessive::tree::Stk;

use super::args::{Optional, Rest};
use crate::cnf::GENERATION_ALLOCATION_LIMIT;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::val::array::{
	Clump, Combine, Complement, Difference, Flatten, Intersect, Matches, Union, Uniq, Windows,
};
use crate::val::range::TypedRange;
use crate::val::{Array, Closure, Value};

/// Returns an error if an array of this length is too much to allocate.
fn limit(name: &str, n: usize) -> Result<(), Error> {
	if n > *GENERATION_ALLOCATION_LIMIT {
		Err(Error::InvalidArguments {
			name: name.to_owned(),
			message: format!("Output must not exceed {} bytes.", *GENERATION_ALLOCATION_LIMIT),
		})
	} else {
		Ok(())
	}
}

pub fn add((mut array, value): (Array, Value)) -> Result<Value> {
	match value {
		Value::Array(value) => {
			for v in value.0 {
				if !array.0.contains(&v) {
					array.0.push(v)
				}
			}
			Ok(array.into())
		}
		value => {
			if !array.0.contains(&value) {
				array.0.push(value)
			}
			Ok(array.into())
		}
	}
}

pub async fn all(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, Optional(check)): (Array, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(opt) = opt {
				for arg in array {
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
		Some(value) => array.iter().all(|v: &Value| *v == value).into(),
		None => array.iter().all(Value::is_truthy).into(),
	})
}

pub async fn any(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, Optional(check)): (Array, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(opt) = opt {
				for arg in array {
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
		Some(value) => array.contains(&value).into(),
		None => array.iter().any(Value::is_truthy).into(),
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

pub fn boolean_not((mut array,): (Array,)) -> Result<Value> {
	array.iter_mut().for_each(|v| *v = (!v.is_truthy()).into());
	Ok(array.into())
}

pub fn boolean_or((lh, rh): (Array, Array)) -> Result<Value> {
	let (mut res, comp) = if lh.len() < rh.len() {
		(rh, lh)
	} else {
		(lh, rh)
	};

	let comp_len = comp.len();
	for (idx, i) in comp.into_iter().enumerate() {
		res[idx] = (res[idx].is_truthy() || i.is_truthy()).into()
	}

	for i in &mut res[comp_len..] {
		*i = i.is_truthy().into()
	}

	Ok(res.into())
}

pub fn boolean_and((lh, rh): (Array, Array)) -> Result<Value> {
	let (mut res, comp) = if lh.len() < rh.len() {
		(rh, lh)
	} else {
		(lh, rh)
	};

	let comp_len = comp.len();
	for (idx, i) in comp.into_iter().enumerate() {
		res[idx] = (res[idx].is_truthy() && i.is_truthy()).into()
	}

	res[comp_len..].fill(Value::Bool(false));

	Ok(res.into())
}

pub fn boolean_xor((lh, rh): (Array, Array)) -> Result<Value> {
	let (mut res, comp) = if lh.len() < rh.len() {
		(rh, lh)
	} else {
		(lh, rh)
	};

	let comp_len = comp.len();
	for (idx, i) in comp.into_iter().enumerate() {
		res[idx] = (res[idx].is_truthy() ^ i.is_truthy()).into()
	}

	for i in &mut res[comp_len..] {
		*i = i.is_truthy().into()
	}

	Ok(res.into())
}

pub fn clump((array, clump_size): (Array, i64)) -> Result<Value> {
	let clump_size = clump_size.max(0) as usize;
	Ok(array.clump(clump_size)?.into())
}

pub fn combine((array, other): (Array, Array)) -> Result<Value> {
	Ok(array.combine(other).into())
}

pub fn complement((array, other): (Array, Array)) -> Result<Value> {
	Ok(array.complement(other).into())
}

pub fn concat(Rest(arrays): Rest<Array>) -> Result<Value> {
	let len = arrays.iter().map(Array::len).sum();
	limit("array::concat", mem::size_of::<Value>().saturating_mul(len))?;
	let mut arr = Array::with_capacity(len);
	arrays.into_iter().for_each(|mut val| {
		arr.0.append(&mut val);
	});
	Ok(arr.into())
}

pub fn difference((array, other): (Array, Array)) -> Result<Value> {
	Ok(array.difference(other).into())
}

pub fn distinct((array,): (Array,)) -> Result<Value> {
	Ok(array.uniq().into())
}

pub fn fill(
	(mut array, value, Optional(range_start), Optional(end)): (
		Array,
		Value,
		Optional<Value>,
		Optional<i64>,
	),
) -> Result<Value> {
	let Some(range_start) = range_start else {
		array.fill(value);
		return Ok(array.into());
	};

	let range = if let Some(end) = end {
		let start = range_start.coerce_to::<i64>().map_err(|e| Error::InvalidArguments {
			name: String::from("array::fill"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?;

		TypedRange::from_range(start..end)
	} else if range_start.is_range() {
		// Condition checked above, cannot fail
		let range = range_start.into_range().expect("is_range() check passed");
		range.coerce_to_typed::<i64>().map_err(|e| Error::InvalidArguments {
			name: String::from("array::fill"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?
	} else {
		let start = range_start.coerce_to::<i64>().map_err(|e| Error::InvalidArguments {
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

	array[start..=end].fill(value);

	Ok(array.into())
}

pub async fn filter(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, check): (Array, Value),
) -> Result<Value> {
	Ok(match check {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				let mut res = Vec::with_capacity(array.len());
				for arg in array {
					if closure.invoke(stk, ctx, opt, doc, vec![arg.clone()]).await?.is_truthy() {
						res.push(arg)
					}
				}
				Value::from(res)
			} else {
				Value::None
			}
		}
		value => array.into_iter().filter(|v: &Value| *v == value).collect::<Vec<_>>().into(),
	})
}

pub async fn filter_index(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				let mut res = Vec::with_capacity(array.len());
				for (i, arg) in array.into_iter().enumerate() {
					if closure.invoke(stk, ctx, opt, doc, vec![arg]).await?.is_truthy() {
						res.push(Value::from(i as i64));
					}
				}
				Value::from(res)
			} else {
				Value::None
			}
		}
		value => array
			.iter()
			.enumerate()
			.filter_map(|(i, v)| {
				if *v == value {
					Some(Value::from(i))
				} else {
					None
				}
			})
			.collect::<Vec<_>>()
			.into(),
	})
}

pub async fn find(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				for arg in array {
					if closure.invoke(stk, ctx, opt, doc, vec![arg.clone()]).await?.is_truthy() {
						return Ok(arg);
					}
				}
				Value::None
			} else {
				Value::None
			}
		}
		value => array.into_iter().find(|v: &Value| *v == value).unwrap_or(Value::None),
	})
}

pub async fn find_index(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				for (i, arg) in array.into_iter().enumerate() {
					if closure.invoke(stk, ctx, opt, doc, vec![arg]).await?.is_truthy() {
						return Ok(i.into());
					}
				}
				Value::None
			} else {
				Value::None
			}
		}
		value => array
			.iter()
			.enumerate()
			.find_map(|(i, v)| {
				if *v == value {
					Some(Value::from(i))
				} else {
					None
				}
			})
			.unwrap_or(Value::None),
	})
}

pub fn first((array,): (Array,)) -> Result<Value> {
	if let [first, ..] = &array[0..] {
		Ok(first.to_owned())
	} else {
		Ok(Value::None)
	}
}

pub fn flatten((array,): (Array,)) -> Result<Value> {
	Ok(array.flatten().into())
}

pub async fn fold(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, init, mapper): (Array, Value, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		let mut accum = init;
		for (i, val) in array.into_iter().enumerate() {
			accum = mapper.invoke(stk, ctx, opt, doc, vec![accum, val, i.into()]).await?
		}
		Ok(accum)
	} else {
		Ok(Value::None)
	}
}

pub fn group((array,): (Array,)) -> Result<Value> {
	Ok(array.flatten().uniq().into())
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

pub fn intersect((array, other): (Array, Array)) -> Result<Value> {
	Ok(array.intersect(other).into())
}

pub fn is_empty((array,): (Array,)) -> Result<Value> {
	Ok(array.is_empty().into())
}

pub fn join((arr, sep): (Array, String)) -> Result<Value> {
	Ok(arr.into_iter().map(Value::into_raw_string).collect::<Vec<_>>().join(&sep).into())
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

pub fn logical_and((mut lh, mut rh): (Array, Array)) -> Result<Value> {
	if lh.len() < rh.len() {
		let lh_len = lh.len();
		for (idx, b) in lh.into_iter().enumerate() {
			if !b.is_truthy() {
				rh[idx] = b;
			}
		}

		rh[lh_len..].fill(Value::Null);
		Ok(rh.into())
	} else {
		let rh_len = rh.len();
		for (idx, b) in rh.into_iter().enumerate() {
			if lh[idx].is_truthy() {
				lh[idx] = b
			}
		}

		for i in &mut lh[rh_len..] {
			if i.is_truthy() {
				*i = Value::Null
			}
		}
		Ok(lh.into())
	}
}

pub fn logical_or((mut lh, mut rh): (Array, Array)) -> Result<Value> {
	if lh.len() < rh.len() {
		for (idx, b) in lh.into_iter().enumerate() {
			if b.is_truthy() {
				rh[idx] = b;
			}
		}

		Ok(rh.into())
	} else {
		let rh_len = rh.len();
		for (idx, b) in rh.into_iter().enumerate() {
			if !lh[idx].is_truthy() {
				lh[idx] = b
			}
		}

		for i in &mut lh[rh_len..] {
			if !i.is_truthy() {
				*i = Value::Null
			}
		}

		Ok(lh.into())
	}
}

pub fn logical_xor((mut lh, mut rh): (Array, Array)) -> Result<Value> {
	if lh.len() < rh.len() {
		let lh_len = lh.len();
		for (idx, b) in lh.into_iter().enumerate() {
			let v = b.is_truthy() ^ rh[idx].is_truthy();
			if b.is_truthy() == v {
				rh[idx] = b;
			} else if rh[idx].is_truthy() != v {
				rh[idx] = v.into();
			}
		}

		for i in &mut rh[lh_len..] {
			if !i.is_truthy() {
				*i = Value::Null;
			}
		}

		Ok(rh.into())
	} else {
		for (idx, b) in rh.into_iter().enumerate() {
			let v = b.is_truthy() ^ lh[idx].is_truthy();
			if lh[idx].is_truthy() == v {
				continue;
			}

			if b.is_truthy() == v {
				lh[idx] = b;
			} else {
				lh[idx] = v.into();
			}
		}

		Ok(lh.into())
	}
}

pub async fn map(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, mapper): (Array, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		let mut res = Vec::with_capacity(array.len());
		for (i, arg) in array.into_iter().enumerate() {
			res.push(mapper.invoke(stk, ctx, opt, doc, vec![arg, i.into()]).await?);
		}
		Ok(res.into())
	} else {
		Ok(Value::None)
	}
}

pub fn matches((array, compare_val): (Array, Value)) -> Result<Value> {
	Ok(array.matches(compare_val).into())
}

pub fn max((array,): (Array,)) -> Result<Value> {
	Ok(array.into_iter().max().unwrap_or_default())
}

pub fn min((array,): (Array,)) -> Result<Value> {
	Ok(array.into_iter().min().unwrap_or_default())
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

pub fn range((start_range, Optional(end)): (Value, Optional<i64>)) -> Result<Value> {
	let range = if let Some(end) = end {
		let start = start_range.coerce_to::<i64>().map_err(|e| Error::InvalidArguments {
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
		range.coerce_to_typed::<i64>().map_err(|e| Error::InvalidArguments {
			name: String::from("array::range"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?
	} else {
		let start = start_range.coerce_to::<i64>().map_err(|e| Error::InvalidArguments {
			name: String::from("array::range"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?;
		TypedRange {
			start: Bound::Included(start),
			end: Bound::Unbounded,
		}
	};

	limit("array::range", mem::size_of::<Value>().saturating_mul(range.len()))?;

	Ok(range.iter().map(Value::from).collect())
}

pub fn sequence((offset_len, Optional(len)): (i64, Optional<i64>)) -> Result<Value> {
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

	limit("array::sequence", mem::size_of::<Value>().saturating_mul(range.len()))?;
	Ok(range.iter().map(Value::from).collect())
}

pub async fn reduce(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(array, mapper): (Array, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
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
					accum = mapper.invoke(stk, ctx, opt, doc, vec![accum, val, idx.into()]).await?;
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

pub fn repeat((value, count): (Value, i64)) -> Result<Value> {
	ensure!(
		count >= 0,
		Error::InvalidArguments {
			name: "array::repeat".to_owned(),
			message: "Expected argument 2 to be a positive number".to_owned()
		}
	);

	// TODO: Fix signed to unsigned casting here.
	let count = count as usize;
	limit("array::repeat", mem::size_of::<Value>().saturating_mul(count))?;
	Ok(Array(std::iter::repeat_n(value, count).collect()).into())
}

pub fn reverse((mut array,): (Array,)) -> Result<Value> {
	array.reverse();
	Ok(array.into())
}

pub fn shuffle((mut array,): (Array,)) -> Result<Value> {
	let mut rng = rand::thread_rng();
	array.shuffle(&mut rng);
	Ok(array.into())
}

pub fn slice(
	(mut array, Optional(range_start), Optional(end)): (Array, Optional<Value>, Optional<i64>),
) -> Result<Value> {
	let Some(range_start) = range_start else {
		return Ok(array.into());
	};

	let range = if let Some(end) = end {
		let start = range_start.coerce_to::<i64>().map_err(|e| Error::InvalidArguments {
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
		range.coerce_to_typed::<i64>().map_err(|e| Error::InvalidArguments {
			name: String::from("array::range"),
			message: format!("Argument 1 was the wrong type. {e}"),
		})?
	} else {
		let start = range_start.coerce_to::<i64>().map_err(|e| Error::InvalidArguments {
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

pub fn sort((mut array, Optional(order)): (Array, Optional<Value>)) -> Result<Value> {
	if sort_as_asc(&order) {
		array.sort_unstable();
		Ok(array.into())
	} else {
		array.sort_unstable_by(|a, b| b.cmp(a));
		Ok(array.into())
	}
}

pub fn sort_natural((mut array, Optional(order)): (Array, Optional<Value>)) -> Result<Value> {
	if sort_as_asc(&order) {
		array.sort_unstable_by(|a, b| a.natural_cmp(b).unwrap_or(Ordering::Equal));
		Ok(array.into())
	} else {
		array.sort_unstable_by(|a, b| b.natural_cmp(a).unwrap_or(Ordering::Equal));
		Ok(array.into())
	}
}

pub fn sort_lexical((mut array, Optional(order)): (Array, Optional<Value>)) -> Result<Value> {
	if sort_as_asc(&order) {
		array.sort_unstable_by(|a, b| a.lexical_cmp(b).unwrap_or(Ordering::Equal));
		Ok(array.into())
	} else {
		array.sort_unstable_by(|a, b| b.lexical_cmp(a).unwrap_or(Ordering::Equal));
		Ok(array.into())
	}
}

pub fn sort_natural_lexical(
	(mut array, Optional(order)): (Array, Optional<Value>),
) -> Result<Value> {
	if sort_as_asc(&order) {
		array.sort_unstable_by(|a, b| a.natural_lexical_cmp(b).unwrap_or(Ordering::Equal));
		Ok(array.into())
	} else {
		array.sort_unstable_by(|a, b| b.natural_lexical_cmp(a).unwrap_or(Ordering::Equal));
		Ok(array.into())
	}
}

pub fn swap((mut array, from, to): (Array, i64, i64)) -> Result<Value> {
	let min = 0;
	let max = array.len();
	let negative_max = -(max as isize);
	let from = from as isize;
	let to = to as isize;

	let from = match from {
		from if from < negative_max || from >= max as isize => Err(Error::InvalidArguments {
			name: String::from("array::swap"),
			message: format!(
				"Argument 1 is out of range. Expected a number between {negative_max} and {max}"
			),
		}),
		from if negative_max <= from && from < min => Ok((from + max as isize) as usize),
		from => Ok(from as usize),
	}?;

	let to = match to {
		to if to < negative_max || to >= max as isize => Err(Error::InvalidArguments {
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

pub fn transpose((array,): (Array,)) -> Result<Value> {
	Ok(array.transpose().into())
}

pub fn union((array, other): (Array, Array)) -> Result<Value> {
	Ok(array.union(other).into())
}

pub fn windows((array, window_size): (Array, i64)) -> Result<Value> {
	let window_size = window_size.max(0) as usize;
	Ok(array.windows(window_size)?.into())
}

pub mod sort {

	use anyhow::Result;

	use crate::val::{Array, Value};

	pub fn asc((mut array,): (Array,)) -> Result<Value> {
		array.sort_unstable();
		Ok(array.into())
	}

	pub fn desc((mut array,): (Array,)) -> Result<Value> {
		array.sort_unstable_by(|a, b| b.cmp(a));
		Ok(array.into())
	}
}

#[cfg(test)]
mod tests {
	use super::{at, first, join, last, slice};
	use crate::fnc::args::Optional;
	use crate::val::{Array, Value};

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
		fn test(arr: Array, expected: Value) {
			assert_eq!(first((arr,)).unwrap(), expected);
		}

		test(vec!["hello", "world"].into(), "hello".into());
		test(Array::new(), Value::None);
	}

	#[test]
	fn array_last() {
		fn test(arr: Array, expected: Value) {
			assert_eq!(last((arr,)).unwrap(), expected);
		}

		test(vec!["hello", "world"].into(), "world".into());
		test(Array::new(), Value::None);
	}

	#[test]
	fn array_at() {
		fn test(arr: Array, i: i64, expected: Value) {
			assert_eq!(at((arr, i)).unwrap(), expected);
		}
		test(vec!["hello", "world"].into(), -2, "hello".into());
		test(vec!["hello", "world"].into(), -3, Value::None);
	}
}
