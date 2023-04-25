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
		Some(Value::True) => {
			array.sort_unstable();
			Ok(array.into())
		}
		// If false, sort descending
		Some(Value::False) => {
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
