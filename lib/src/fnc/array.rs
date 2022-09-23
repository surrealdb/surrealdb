use crate::err::Error;
use crate::sql::array::Combine;
use crate::sql::array::Concat;
use crate::sql::array::Difference;
use crate::sql::array::Intersect;
use crate::sql::array::Union;
use crate::sql::array::Uniq;
use crate::sql::value::Value;

pub fn concat(arrays: (Value, Value)) -> Result<Value, Error> {
	Ok(match arrays {
		(Value::Array(v), Value::Array(w)) => v.concat(w).into(),
		_ => Value::None,
	})
}

pub fn combine(arrays: (Value, Value)) -> Result<Value, Error> {
	Ok(match arrays {
		(Value::Array(v), Value::Array(w)) => v.combine(w).into(),
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

pub fn sort((array, order): (Value, Option<Value>)) -> Result<Value, Error> {
	match array {
		Value::Array(mut v) => match order {
			// If "asc", sort ascending
			Some(Value::Strand(s)) if s.as_str() == "asc" => {
				v.sort_unstable_by(|a, b| a.cmp(b));
				Ok(v.into())
			}
			// If "desc", sort descending
			Some(Value::Strand(s)) if s.as_str() == "desc" => {
				v.sort_unstable_by(|a, b| b.cmp(a));
				Ok(v.into())
			}
			// If true, sort ascending
			Some(Value::True) => {
				v.sort_unstable_by(|a, b| a.cmp(b));
				Ok(v.into())
			}
			// If false, sort descending
			Some(Value::False) => {
				v.sort_unstable_by(|a, b| b.cmp(a));
				Ok(v.into())
			}
			// Sort ascending by default
			_ => {
				v.sort_unstable_by(|a, b| a.cmp(b));
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
				v.sort_unstable_by(|a, b| a.cmp(b));
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
