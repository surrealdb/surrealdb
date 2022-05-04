use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::array::Combine;
use crate::sql::array::Concat;
use crate::sql::array::Difference;
use crate::sql::array::Intersect;
use crate::sql::array::Union;
use crate::sql::array::Uniq;
use crate::sql::value::Value;

pub fn concat(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.concat(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn combine(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.combine(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn difference(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.difference(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn distinct(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.uniq().into()),
		_ => Ok(Value::None),
	}
}

pub fn intersect(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.intersect(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}

pub fn len(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => Ok(v.len().into()),
		_ => Ok(Value::None),
	}
}

pub fn union(_: &Runtime, mut args: Vec<Value>) -> Result<Value, Error> {
	match args.remove(0) {
		Value::Array(v) => match args.remove(0) {
			Value::Array(w) => Ok(v.union(w).into()),
			_ => Ok(Value::None),
		},
		_ => Ok(Value::None),
	}
}
