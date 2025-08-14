use std::cmp::Ordering;
use std::mem::size_of_val;

use anyhow::{Result, bail, ensure};
use rand::prelude::SliceRandom;
use reblessive::tree::Stk;

use super::args::{Optional, Rest};
use crate::cnf::GENERATION_ALLOCATION_LIMIT;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::val::array::{
	Clump, Combine, Complement, Difference, Flatten, Intersect, Matches, Transpose, Union, Uniq,
	Windows,
};
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
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(array, Optional(check)): (Array, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(opt) = opt {
				for arg in array.into_iter() {
					// TODO: Don't clone the closure every time the function is called.
					if closure.compute(stk, ctx, opt, doc, vec![arg]).await?.is_truthy() {
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
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(array, Optional(check)): (Array, Optional<Value>),
) -> Result<Value> {
	Ok(match check {
		Some(Value::Closure(closure)) => {
			if let Some(opt) = opt {
				for arg in array.into_iter() {
					// TODO: Don't clone the closure every time the function is called.
					if closure.compute(stk, ctx, opt, doc, vec![arg]).await?.is_truthy() {
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

pub fn boolean_and((lh, rh): (Array, Array)) -> Result<Value> {
	let longest_length = lh.len().max(rh.len());
	let mut results = Array::with_capacity(longest_length);
	for i in 0..longest_length {
		let lhv = lh.get(i);
		let rhv = rh.get(i);
		results
			.push((lhv.is_some_and(Value::is_truthy) && rhv.is_some_and(Value::is_truthy)).into());
	}
	Ok(results.into())
}

pub fn boolean_not((mut array,): (Array,)) -> Result<Value> {
	array.iter_mut().for_each(|v| *v = (!v.is_truthy()).into());
	Ok(array.into())
}

pub fn boolean_or((lh, rh): (Array, Array)) -> Result<Value> {
	let longest_length = lh.len().max(rh.len());
	let mut results = Array::with_capacity(longest_length);
	for i in 0..longest_length {
		let lhv = lh.get(i);
		let rhv = rh.get(i);
		results
			.push((lhv.is_some_and(Value::is_truthy) || rhv.is_some_and(Value::is_truthy)).into());
	}
	Ok(results.into())
}

pub fn boolean_xor((lh, rh): (Array, Array)) -> Result<Value> {
	let longest_length = lh.len().max(rh.len());
	let mut results = Array::with_capacity(longest_length);
	for i in 0..longest_length {
		let lhv = lh.get(i);
		let rhv = rh.get(i);
		results
			.push((lhv.is_some_and(Value::is_truthy) ^ rhv.is_some_and(Value::is_truthy)).into());
	}
	Ok(results.into())
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

pub fn concat(Rest(mut arrays): Rest<Array>) -> Result<Value> {
	let len = match arrays.iter().map(Array::len).reduce(|c, v| c + v) {
		None => Err(Error::InvalidArguments {
			name: String::from("array::concat"),
			message: String::from("Expected at least one argument"),
		}),
		Some(l) => Ok(l),
	}?;
	let mut arr = Array::with_capacity(len);
	arrays.iter_mut().for_each(|val| {
		arr.append(val);
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
	(mut array, value, Optional(start), Optional(end)): (
		Array,
		Value,
		Optional<i64>,
		Optional<i64>,
	),
) -> Result<Value> {
	let len = array.len();

	let start = start.unwrap_or(0);
	let start = if start < 0 {
		len.saturating_sub((-start) as usize)
	} else {
		(start as usize).min(len)
	};

	let end = if let Some(end) = end {
		if end < 0 {
			len.saturating_sub((-end) as usize)
		} else {
			(end as usize).min(len)
		}
	} else {
		len
	};

	array[start..end].fill(value);

	Ok(array.into())
}

pub async fn filter(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(array, check): (Array, Value),
) -> Result<Value> {
	Ok(match check {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				let mut res = Vec::with_capacity(array.len());
				for arg in array.into_iter() {
					if closure.compute(stk, ctx, opt, doc, vec![arg.clone()]).await?.is_truthy() {
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
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				let mut res = Vec::with_capacity(array.len());
				for (i, arg) in array.into_iter().enumerate() {
					if closure.compute(stk, ctx, opt, doc, vec![arg]).await?.is_truthy() {
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
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				for arg in array.into_iter() {
					if closure.compute(stk, ctx, opt, doc, vec![arg.clone()]).await?.is_truthy() {
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
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(array, value): (Array, Value),
) -> Result<Value> {
	Ok(match value {
		Value::Closure(closure) => {
			if let Some(opt) = opt {
				for (i, arg) in array.into_iter().enumerate() {
					// TODO: Don't clone the closure every time the function is called.
					if closure.compute(stk, ctx, opt, doc, vec![arg]).await?.is_truthy() {
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
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(array, init, mapper): (Array, Value, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		let mut accum = init;
		for (i, val) in array.into_iter().enumerate() {
			// TODO: Don't clone the closure every time the function is called.
			accum = mapper.compute(stk, ctx, opt, doc, vec![accum, val, i.into()]).await?
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
	Ok(arr.into_iter().map(Value::as_raw_string).collect::<Vec<_>>().join(&sep).into())
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

pub fn logical_and((lh, rh): (Array, Array)) -> Result<Value> {
	let mut result_arr = Array::with_capacity(lh.len().max(rh.len()));
	let mut iters = (lh.into_iter(), rh.into_iter());
	for (lhv, rhv) in std::iter::from_fn(|| {
		let r = (iters.0.next(), iters.1.next());
		if r.0.is_none() && r.1.is_none() {
			None
		} else {
			Some((r.0.unwrap_or(Value::Null), r.1.unwrap_or(Value::Null)))
		}
	}) {
		let truth = lhv.is_truthy() && rhv.is_truthy();
		let r = if lhv.is_truthy() == truth {
			lhv
		} else if rhv.is_truthy() == truth {
			rhv
		} else {
			truth.into()
		};
		result_arr.push(r);
	}
	Ok(result_arr.into())
}

pub fn logical_or((lh, rh): (Array, Array)) -> Result<Value> {
	let mut result_arr = Array::with_capacity(lh.len().max(rh.len()));
	let mut iters = (lh.into_iter(), rh.into_iter());
	for (lhv, rhv) in std::iter::from_fn(|| {
		let r = (iters.0.next(), iters.1.next());
		if r.0.is_none() && r.1.is_none() {
			None
		} else {
			Some((r.0.unwrap_or(Value::Null), r.1.unwrap_or(Value::Null)))
		}
	}) {
		let truth = lhv.is_truthy() || rhv.is_truthy();
		let r = if lhv.is_truthy() == truth {
			lhv
		} else if rhv.is_truthy() == truth {
			rhv
		} else {
			truth.into()
		};
		result_arr.push(r);
	}
	Ok(result_arr.into())
}

pub fn logical_xor((lh, rh): (Array, Array)) -> Result<Value> {
	let mut result_arr = Array::with_capacity(lh.len().max(rh.len()));
	let mut iters = (lh.into_iter(), rh.into_iter());
	for (lhv, rhv) in std::iter::from_fn(|| {
		let r = (iters.0.next(), iters.1.next());
		if r.0.is_none() && r.1.is_none() {
			None
		} else {
			Some((r.0.unwrap_or(Value::Null), r.1.unwrap_or(Value::Null)))
		}
	}) {
		let truth = lhv.is_truthy() ^ rhv.is_truthy();
		let r = if lhv.is_truthy() == truth {
			lhv
		} else if rhv.is_truthy() == truth {
			rhv
		} else {
			truth.into()
		};
		result_arr.push(r);
	}
	Ok(result_arr.into())
}

pub async fn map(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(array, mapper): (Array, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		let mut res = Vec::with_capacity(array.len());
		for (i, arg) in array.into_iter().enumerate() {
			// TODO: Don't clone the closure every time the function is called.
			res.push(mapper.compute(stk, ctx, opt, doc, vec![arg, i.into()]).await?);
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

pub fn range((start, count): (i64, i64)) -> Result<Value> {
	ensure!(
		count >= 0,
		Error::InvalidArguments {
			name: String::from("array::range"),
			message: format!(
				"Argument 1 was the wrong type. Expected a positive number but found {count}"
			),
		}
	);

	if let Some(end) = start.checked_add(count - 1) {
		Ok(Array((start..=end).map(Value::from).collect::<Vec<_>>()).into())
	} else {
		bail!(Error::InvalidArguments {
			name: String::from("array::range"),
			message: String::from("The range overflowed the maximum value for an integer"),
		})
	}
}

pub async fn reduce(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
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
					accum =
						mapper.compute(stk, ctx, opt, doc, vec![accum, val, idx.into()]).await?;
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
	// TODO: Fix signed to unsigned casting here.
	let count = count as usize;
	limit("array::repeat", size_of_val(&value).saturating_mul(count))?;
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
	(array, Optional(beg), Optional(lim)): (Array, Optional<i64>, Optional<i64>),
) -> Result<Value> {
	let skip = match beg {
		Some(v) if v < 0 => array.len().saturating_sub(v.unsigned_abs() as usize),
		Some(v) => v as usize,
		None => 0,
	};

	let take = match lim {
		Some(v) if v < 0 => {
			array.len().saturating_sub(skip).saturating_sub(v.unsigned_abs() as usize)
		}
		Some(v) => v as usize,
		None => usize::MAX,
	};

	Ok(if skip > 0 || take < usize::MAX {
		array.into_iter().skip(skip).take(take).collect::<Vec<_>>().into()
	} else {
		array
	}
	.into())
}

fn sort_as_asc(order: &Option<Value>) -> bool {
	match order {
		Some(Value::Strand(s)) if s.as_str() == "asc" => true,
		Some(Value::Strand(s)) if s.as_str() == "desc" => false,
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
		fn test(initial: &[u8], beg: Option<i64>, lim: Option<i64>, expected: &[u8]) {
			let initial_values =
				initial.iter().map(|n| Value::from(*n as i64)).collect::<Vec<_>>().into();
			let expected_values: Array =
				expected.iter().map(|n| Value::from(*n as i64)).collect::<Vec<_>>().into();
			assert_eq!(
				slice((initial_values, Optional(beg), Optional(lim))).unwrap(),
				Value::from(expected_values)
			);
		}

		let array = b"abcdefg";
		test(array, None, None, array);
		test(array, Some(2), None, &array[2..]);
		test(array, Some(2), Some(3), &array[2..5]);
		test(array, Some(2), Some(-1), b"cdef");
		test(array, Some(-2), None, b"fg");
		test(array, Some(-4), Some(2), b"de");
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
