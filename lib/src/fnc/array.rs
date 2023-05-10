use crate::err::Error;
use crate::sql::array::Array;
use crate::sql::array::Combine;
use crate::sql::array::Complement;
use crate::sql::array::Concat;
use crate::sql::array::Difference;
use crate::sql::array::Flatten;
use crate::sql::array::Intersect;
use crate::sql::array::Union;
use crate::sql::array::Uniq;
use crate::sql::value::Value;

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

pub fn combine((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.combine(other).into())
}

pub fn complement((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.complement(other).into())
}

pub fn concat((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.concat(other).into())
}

pub fn difference((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.difference(other).into())
}

pub fn distinct((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.uniq().into())
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

pub fn len((array,): (Array,)) -> Result<Value, Error> {
	Ok(array.len().into())
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

pub fn union((array, other): (Array, Array)) -> Result<Value, Error> {
	Ok(array.union(other).into())
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
	use super::{join, slice};
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

		let array = &[b'a', b'b', b'c', b'd', b'e', b'f', b'g'];
		test(array, None, None, array);
		test(array, Some(2), None, &array[2..]);
		test(array, Some(2), Some(3), &array[2..5]);
		test(array, Some(2), Some(-1), &[b'c', b'd', b'e', b'f']);
		test(array, Some(-2), None, &[b'f', b'g']);
		test(array, Some(-4), Some(2), &[b'd', b'e']);
		test(array, Some(-4), Some(-1), &[b'd', b'e', b'f']);
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
			vec![Value::from(3.14), Value::from(2.72), Value::from(1.61)].into(),
			" is not ",
			"3.14 is not 2.72 is not 1.61",
		);
	}
}
