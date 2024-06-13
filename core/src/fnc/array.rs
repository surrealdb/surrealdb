use crate::err::Error;
use crate::sql::array::Array;
use crate::sql::array::Clump;
use crate::sql::array::Combine;
use crate::sql::array::Complement;
use crate::sql::array::Difference;
use crate::sql::array::Flatten;
use crate::sql::array::Intersect;
use crate::sql::array::Matches;
use crate::sql::array::Transpose;
use crate::sql::array::Union;
use crate::sql::array::Uniq;
use crate::sql::array::Windows;
use crate::sql::value::Value;

use rand::prelude::SliceRandom;

pub fn add((mut array, value): (Array, Value)) -> Result<Value, Error> {
	match value {
		Value::Array(value) => {
			for v in value.0 {
				if !array.0.iter().any(|x| *x == v) {
					array.0.push(v)
				}
			}
			Ok(array.into())
		}
		value => {
			if !array.0.iter().any(|x| *x == value) {
				array.0.push(value)
			}
			Ok(array.into())
		}
	}
}

pub fn all((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.iter().all(Value::is_truthy).into())
}

pub fn any((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.iter().any(Value::is_truthy).into())
}

pub fn append((mut array, value): (Array, Value)) -> Result<Value, Error> {
	array.push(value);
	Ok(array.into())
}

pub fn at((array, i): (Array, i64)) -> Result<Value, Error> {
	let mut idx = i as usize;
	if i < 0 {
		idx = (array.len() as i64 + i) as usize;
	}
	Ok(array.get(idx).cloned().unwrap_or_default())
}

pub fn boolean_and((lh, rh): (Array, Array)) -> Result<Value, Error> {
	let longest_length = lh.len().max(rh.len());
	let mut results = Array::with_capacity(longest_length);
	for i in 0..longest_length {
		let lhv = lh.get(i);
		let rhv = rh.get(i);
		results.push(
			(lhv.map_or(false, |v| v.is_truthy()) && rhv.map_or(false, |v| v.is_truthy())).into(),
		);
	}
	Ok(results.into())
}

pub fn boolean_not((mut array,): (Array,)) -> Result<Value, Error> {
	array.iter_mut().for_each(|v| *v = (!v.is_truthy()).into());
	Ok(array.into())
}

pub fn boolean_or((lh, rh): (Array, Array)) -> Result<Value, Error> {
	let longest_length = lh.len().max(rh.len());
	let mut results = Array::with_capacity(longest_length);
	for i in 0..longest_length {
		let lhv = lh.get(i);
		let rhv = rh.get(i);
		results.push(
			(lhv.map_or(false, |v| v.is_truthy()) || rhv.map_or(false, |v| v.is_truthy())).into(),
		);
	}
	Ok(results.into())
}

pub fn boolean_xor((lh, rh): (Array, Array)) -> Result<Value, Error> {
	let longest_length = lh.len().max(rh.len());
	let mut results = Array::with_capacity(longest_length);
	for i in 0..longest_length {
		let lhv = lh.get(i);
		let rhv = rh.get(i);
		results.push(
			(lhv.map_or(false, |v| v.is_truthy()) ^ rhv.map_or(false, |v| v.is_truthy())).into(),
		);
	}
	Ok(results.into())
}

pub fn clump((array, clump_size): (Array, i64)) -> Result<Value, Error> {
	let clump_size = clump_size.max(0) as usize;
	Ok(array.clump(clump_size)?.into())
}

pub fn combine((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.combine(other).into())
}

pub fn complement((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.complement(other).into())
}

pub fn concat(mut arrays: Vec<Array>) -> Result<Value, Error> {
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

pub fn difference((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.difference(other).into())
}

pub fn distinct((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.uniq().into())
}

pub fn filter_index((array, value): (Array, Value)) -> Result<Value, Error> {
	Ok(array
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
		.into())
}

pub fn find_index((array, value): (Array, Value)) -> Result<Value, Error> {
	Ok(array
		.iter()
		.enumerate()
		.find(|(_i, v)| **v == value)
		.map_or(Value::Null, |(i, _v)| i.into()))
}

pub fn first((array,): (Array,)) -> Result<Value, Error> {
	if let [first, ..] = &array[0..] {
		Ok(first.to_owned())
	} else {
		Ok(Value::None)
	}
}

pub fn flatten((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.flatten().into())
}

pub fn group((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.flatten().uniq().into())
}

pub fn insert((mut array, value, index): (Array, Value, Option<i64>)) -> Result<Value, Error> {
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

pub fn intersect((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.intersect(other).into())
}

pub fn join((arr, sep): (Array, String)) -> Result<Value, Error> {
	Ok(arr.into_iter().map(Value::as_raw_string).collect::<Vec<_>>().join(&sep).into())
}

pub fn last((array,): (Array,)) -> Result<Value, Error> {
	if let [.., last] = &array[0..] {
		Ok(last.to_owned())
	} else {
		Ok(Value::None)
	}
}

pub fn len((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.len().into())
}

pub fn logical_and((lh, rh): (Array, Array)) -> Result<Value, Error> {
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

pub fn logical_or((lh, rh): (Array, Array)) -> Result<Value, Error> {
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

pub fn logical_xor((lh, rh): (Array, Array)) -> Result<Value, Error> {
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

pub fn matches((array, compare_val): (Array, Value)) -> Result<Value, Error> {
	Ok(array.matches(compare_val).into())
}

pub fn max((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.into_iter().max().unwrap_or_default())
}

pub fn min((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.into_iter().min().unwrap_or_default())
}

pub fn pop((mut array,): (Array,)) -> Result<Value, Error> {
	Ok(array.pop().into())
}

pub fn prepend((mut array, value): (Array, Value)) -> Result<Value, Error> {
	array.insert(0, value);
	Ok(array.into())
}

pub fn push((mut array, value): (Array, Value)) -> Result<Value, Error> {
	array.push(value);
	Ok(array.into())
}

pub fn range((start, count): (i64, i64)) -> Result<Value, Error> {
	if count < 0 {
		return Err(Error::InvalidArguments {
			name: String::from("array::range"),
			message: String::from(format!("Argument 1 was the wrong type. Expected a positive number but found {count}")),
		});
	}
	
	if let Some(end) = start.checked_add(count - 1) {
		Ok(Array((start..=end).map(Value::from).collect::<Vec<_>>()).into())
	} else {
		Err(Error::InvalidArguments {
			name: String::from("array::range"),
			message: String::from("The range overflowed the maximum value for an integer"),
		})
	}
}

pub fn remove((mut array, mut index): (Array, i64)) -> Result<Value, Error> {
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

pub fn reverse((mut array,): (Array,)) -> Result<Value, Error> {
	array.reverse();
	Ok(array.into())
}

pub fn shuffle((mut array,): (Array,)) -> Result<Value, Error> {
	let mut rng = rand::thread_rng();
	array.0.shuffle(&mut rng);
	Ok(array.into())
}

pub fn slice((array, beg, lim): (Array, Option<isize>, Option<isize>)) -> Result<Value, Error> {
	let skip = match beg {
		Some(v) if v < 0 => array.len().saturating_sub(v.unsigned_abs()),
		Some(v) => v as usize,
		None => 0,
	};

	let take = match lim {
		Some(v) if v < 0 => array.len().saturating_sub(skip).saturating_sub(v.unsigned_abs()),
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

pub fn sort((mut array, order): (Array, Option<Value>)) -> Result<Value, Error> {
	match order {
		// If "asc", sort ascending
		Some(Value::Strand(s)) if s.as_str() == "asc" => {
			array.sort_unstable();
			Ok(array.into())
		}
		// If "desc", sort descending
		Some(Value::Strand(s)) if s.as_str() == "desc" => {
			array.sort_unstable_by(|a, b| b.cmp(a));
			Ok(array.into())
		}
		// If true, sort ascending
		Some(Value::Bool(true)) => {
			array.sort_unstable();
			Ok(array.into())
		}
		// If false, sort descending
		Some(Value::Bool(false)) => {
			array.sort_unstable_by(|a, b| b.cmp(a));
			Ok(array.into())
		}
		// Sort ascending by default
		_ => {
			array.sort_unstable();
			Ok(array.into())
		}
	}
}

pub fn transpose((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.transpose().into())
}

pub fn union((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.union(other).into())
}

pub fn windows((array, window_size): (Array, i64)) -> Result<Value, Error> {
	let window_size = window_size.max(0) as usize;
	Ok(array.windows(window_size)?.into())
}

pub mod sort {

	use crate::err::Error;
	use crate::sql::array::Array;
	use crate::sql::value::Value;

	pub fn asc((mut array,): (Array,)) -> Result<Value, Error> {
		array.sort_unstable();
		Ok(array.into())
	}

	pub fn desc((mut array,): (Array,)) -> Result<Value, Error> {
		array.sort_unstable_by(|a, b| b.cmp(a));
		Ok(array.into())
	}
}

#[cfg(test)]
mod tests {
	use super::{at, first, join, last, slice};
	use crate::sql::{Array, Value};

	#[test]
	fn array_slice() {
		fn test(initial: &[u8], beg: Option<isize>, lim: Option<isize>, expected: &[u8]) {
			let initial_values =
				initial.iter().map(|n| Value::from(*n as i64)).collect::<Vec<_>>().into();
			let expected_values: Array =
				expected.iter().map(|n| Value::from(*n as i64)).collect::<Vec<_>>().into();
			assert_eq!(slice((initial_values, beg, lim)).unwrap(), expected_values.into());
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
			assert_eq!(join((arr, sep.to_string())).unwrap(), expected.into());
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
