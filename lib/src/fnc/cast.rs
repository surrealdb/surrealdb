use crate::ctx::Context;
use crate::err::Error;
use crate::sql::number::Number;
use crate::sql::value::Value;

pub fn run(_: &Context, name: &str, val: Value) -> Result<Value, Error> {
	match name {
		"bool" => bool(val),
		"int" => int(val),
		"float" => float(val),
		"string" => string(val),
		"number" => number(val),
		"decimal" => decimal(val),
		"datetime" => datetime(val),
		"duration" => duration(val),
		_ => Ok(val),
	}
}

pub fn bool(val: Value) -> Result<Value, Error> {
	Ok(val.is_truthy().into())
}

pub fn int(val: Value) -> Result<Value, Error> {
	match val {
		Value::Number(Number::Int(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Int(val.as_int()))),
	}
}

pub fn float(val: Value) -> Result<Value, Error> {
	match val {
		Value::Number(Number::Float(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Float(val.as_float()))),
	}
}

pub fn number(val: Value) -> Result<Value, Error> {
	match val {
		Value::Number(Number::Decimal(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Decimal(val.as_decimal()))),
	}
}

pub fn decimal(val: Value) -> Result<Value, Error> {
	match val {
		Value::Number(Number::Decimal(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Decimal(val.as_decimal()))),
	}
}

pub fn string(val: Value) -> Result<Value, Error> {
	match val {
		Value::Strand(_) => Ok(val),
		_ => Ok(Value::Strand(val.as_strand())),
	}
}

pub fn datetime(val: Value) -> Result<Value, Error> {
	match val {
		Value::Datetime(_) => Ok(val),
		_ => Ok(Value::Datetime(val.as_datetime())),
	}
}

pub fn duration(val: Value) -> Result<Value, Error> {
	match val {
		Value::Duration(_) => Ok(val),
		_ => Ok(Value::Duration(val.as_duration())),
	}
}
