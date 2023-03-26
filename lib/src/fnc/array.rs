use crate::err::Error;
use crate::sql::array::Combine;
use crate::sql::array::Complement;
use crate::sql::array::Concat;
use crate::sql::array::Difference;
use crate::sql::array::Flatten;
use crate::sql::array::Intersect;
use crate::sql::array::Union;
use crate::sql::array::Uniq;
use crate::sql::value::Value;

pub fn add((array, value): (Value, Value)) -> Result<Value, Error> {
	match (array, value) {
		(Value::Array(mut arr), Value::Array(other)) => {
			for v in other.0 {
				if !arr.0.iter().any(|x| *x == v) {
					arr.0.push(v)
				}
			}
			Ok(arr.into())
		}
		(Value::Array(mut arr), value) => {
			if !arr.0.iter().any(|x| *x == value) {
				arr.0.push(value)
			}
			Ok(arr.into())
		}
		_ => Ok(Value::None),
	}
}

pub fn all((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Array(v) => Ok(v.iter().all(Value::is_truthy).into()),
		_ => Ok(Value::False),
	}
}

pub fn any((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Array(v) => Ok(v.iter().any(Value::is_truthy).into()),
		_ => Ok(Value::False),
	}
}

pub fn append((array, value): (Value, Value)) -> Result<Value, Error> {
	match array {
		Value::Array(mut v) => {
			v.push(value);
			Ok(v.into())
		}
		_ => Ok(Value::None),
	}
}

pub fn combine(arrays: (Value, Value)) -> Result<Value, Error> {
	Ok(match arrays {
		(Value::Array(v), Value::Array(w)) => v.combine(w).into(),
		_ => Value::None,
	})
}

pub fn complement(arrays: (Value, Value)) -> Result<Value, Error> {
	Ok(match arrays {
		(Value::Array(v), Value::Array(w)) => v.complement(w).into(),
		_ => Value::None,
	})
}

pub fn concat(arrays: (Value, Value)) -> Result<Value, Error> {
	Ok(match arrays {
		(Value::Array(v), Value::Array(w)) => v.concat(w).into(),
		_ => Value::None,
	})
}

pub fn difference(arrays: (Value, Value)) -> Result<Value, Error> {
	Ok(match arrays {
		(Value::Array(v), Value::Array(w)) => v.difference(w).into(),
		_ => Value::None,
	})
}

pub fn distinct((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Array(v) => Ok(v.uniq().into()),
		_ => Ok(Value::None),
	}
}

pub fn flatten((arg,): (Value,)) -> Result<Value, Error> {
	Ok(match arg {
		Value::Array(v) => v.flatten().into(),
		_ => Value::None,
	})
}

pub fn group((arg,): (Value,)) -> Result<Value, Error> {
	Ok(match arg {
		Value::Array(v) => v.flatten().uniq().into(),
		_ => Value::None,
	})
}

pub fn insert((array, value, index): (Value, Value, Option<Value>)) -> Result<Value, Error> {
	match (array, index) {
		(Value::Array(mut v), Some(Value::Number(i))) => {
			let mut i = i.as_int();
			// Negative index means start from the back
			if i < 0 {
				i += v.len() as i64;
			}
			// Invalid index so return array unaltered
			if i > v.len() as i64 || i < 0 {
				return Ok(v.into());
			}
			// Insert the value into the array
			v.insert(i as usize, value);
			// Return the array
			Ok(v.into())
		}
		(Value::Array(mut v), None) => {
			v.push(value);
			Ok(v.into())
		}
		(_, _) => Ok(Value::None),
	}
}

pub fn intersect(arrays: (Value, Value)) -> Result<Value, Error> {
	Ok(match arrays {
		(Value::Array(v), Value::Array(w)) => v.intersect(w).into(),
		_ => Value::None,
	})
}

pub fn len((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Array(v) => Ok(v.len().into()),
		_ => Ok(Value::None),
	}
}

pub fn max((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Array(v) => Ok(v.into_iter().max().unwrap_or(Value::None)),
		_ => Ok(Value::None),
	}
}

pub fn min((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Array(v) => Ok(v.into_iter().min().unwrap_or(Value::None)),
		_ => Ok(Value::None),
	}
}

pub fn pop((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Array(mut v) => Ok(v.pop().into()),
		_ => Ok(Value::None),
	}
}

pub fn prepend((array, value): (Value, Value)) -> Result<Value, Error> {
	match array {
		Value::Array(mut v) => {
			v.insert(0, value);
			Ok(v.into())
		}
		_ => Ok(Value::None),
	}
}

pub fn push((array, value): (Value, Value)) -> Result<Value, Error> {
	match array {
		Value::Array(mut v) => {
			v.push(value);
			Ok(v.into())
		}
		_ => Ok(Value::None),
	}
}

pub fn remove((array, index): (Value, Value)) -> Result<Value, Error> {
	match (array, index) {
		(Value::Array(mut v), Value::Number(i)) => {
			let mut i = i.as_int();
			// Negative index means start from the back
			if i < 0 {
				i += v.len() as i64;
			}
			// Invalid index so return array unaltered
			if i > v.len() as i64 || i < 0 {
				return Ok(v.into());
			}
			// Remove the value from the array
			v.remove(i as usize);
			// Return the array
			Ok(v.into())
		}
		(Value::Array(v), _) => Ok(v.into()),
		(_, _) => Ok(Value::None),
	}
}

pub fn reverse((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Array(mut v) => {
			v.reverse();
			Ok(v.into())
		}
		_ => Ok(Value::None),
	}
}

pub fn sort((array, order): (Value, Option<Value>)) -> Result<Value, Error> {
	match array {
		Value::Array(mut v) => match order {
			// If "asc", sort ascending
			Some(Value::Strand(s)) if s.as_str() == "asc" => {
				v.sort_unstable();
				Ok(v.into())
			}
			// If "desc", sort descending
			Some(Value::Strand(s)) if s.as_str() == "desc" => {
				v.sort_unstable_by(|a, b| b.cmp(a));
				Ok(v.into())
			}
			// If true, sort ascending
			Some(Value::True) => {
				v.sort_unstable();
				Ok(v.into())
			}
			// If false, sort descending
			Some(Value::False) => {
				v.sort_unstable_by(|a, b| b.cmp(a));
				Ok(v.into())
			}
			// Sort ascending by default
			_ => {
				v.sort_unstable();
				Ok(v.into())
			}
		},
		v => Ok(v),
	}
}

pub fn union(arrays: (Value, Value)) -> Result<Value, Error> {
	Ok(match arrays {
		(Value::Array(v), Value::Array(w)) => v.union(w).into(),
		_ => Value::None,
	})
}

pub mod sort {

	use crate::err::Error;
	use crate::sql::value::Value;

	pub fn asc((array,): (Value,)) -> Result<Value, Error> {
		match array {
			Value::Array(mut v) => {
				v.sort_unstable();
				Ok(v.into())
			}
			v => Ok(v),
		}
	}

	pub fn desc((array,): (Value,)) -> Result<Value, Error> {
		match array {
			Value::Array(mut v) => {
				v.sort_unstable_by(|a, b| b.cmp(a));
				Ok(v.into())
			}
			v => Ok(v),
		}
	}
}
