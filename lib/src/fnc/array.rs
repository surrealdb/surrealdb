use crate::ctx::Context;
use crate::err::Error;
use crate::sql::array::Combine;
use crate::sql::array::Concat;
use crate::sql::array::Difference;
use crate::sql::array::Intersect;
use crate::sql::array::Union;
use crate::sql::array::Uniq;
use crate::sql::value::Value;

pub fn concat(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.concat(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn combine(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.combine(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn difference(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.difference(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn distinct(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.uniq().into()),
		_ => Ok(Value::None),
	}
}

pub fn intersect(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.intersect(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn len(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.len().into()),
		_ => Ok(Value::None),
	}
}

pub fn sort(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.len() {
		2 => match args.remove(0) {
			Value::Array(mut v) => match args.remove(0) {
				// If "asc", sort ascending
				Value::Strand(s) if s.as_str() == "asc" => {
					v.sort_unstable_by(|a, b| a.cmp(b));
					Ok(v.into())
				}
				// If "desc", sort descending
				Value::Strand(s) if s.as_str() == "desc" => {
					v.sort_unstable_by(|a, b| b.cmp(a));
					Ok(v.into())
				}
				// If true, sort ascending
				Value::True => {
					v.sort_unstable_by(|a, b| a.cmp(b));
					Ok(v.into())
				}
				// If false, sort descending
				Value::False => {
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
		},
		1 => match args.remove(0) {
			Value::Array(mut v) => {
				v.sort_unstable_by(|a, b| a.cmp(b));
				Ok(v.into())
			}
			v => Ok(v),
		},
		_ => unreachable!(),
	}
}

pub fn union(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.union(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub mod sort {

	use crate::ctx::Context;
	use crate::err::Error;
	use crate::sql::value::Value;

	pub fn asc(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		match args.remove(0) {
			Value::Array(mut v) => {
				v.sort_unstable_by(|a, b| a.cmp(b));
				Ok(v.into())
			}
			v => Ok(v),
		}
	}

	pub fn desc(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
		match args.remove(0) {
			Value::Array(mut v) => {
				v.sort_unstable_by(|a, b| b.cmp(a));
				Ok(v.into())
			}
			v => Ok(v),
		}
	}
}
