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

pub fn union(_: &Context, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.union(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}
