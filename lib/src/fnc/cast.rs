use crate::ctx::Context;
use crate::err::Error;
use crate::sql::number::Number;
use crate::sql::value::Value;

pub fn run(ctx: &Context, name: &str, val: Value) -> Result<Value, Error> {
	match name {
		"bool" => bool(ctx, val),
		"int" => int(ctx, val),
		"float" => float(ctx, val),
		"string" => string(ctx, val),
		"number" => number(ctx, val),
		"decimal" => decimal(ctx, val),
		"datetime" => datetime(ctx, val),
		"duration" => duration(ctx, val),
		_ => Ok(val),
	}
}

pub fn bool(_: &Context, val: Value) -> Result<Value, Error> {
	match val.is_truthy() {
		true => Ok(Value::True),
		false => Ok(Value::False),
	}
}

pub fn int(_: &Context, val: Value) -> Result<Value, Error> {
	match val {
		Value::Number(Number::Int(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Int(val.as_int()))),
	}
}

pub fn float(_: &Context, val: Value) -> Result<Value, Error> {
	match val {
		Value::Number(Number::Float(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Float(val.as_float()))),
	}
}

pub fn number(_: &Context, val: Value) -> Result<Value, Error> {
	match val {
		Value::Number(Number::Decimal(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Decimal(val.as_decimal()))),
	}
}

pub fn decimal(_: &Context, val: Value) -> Result<Value, Error> {
	match val {
		Value::Number(Number::Decimal(_)) => Ok(val),
		_ => Ok(Value::Number(Number::Decimal(val.as_decimal()))),
	}
}

pub fn string(_: &Context, val: Value) -> Result<Value, Error> {
	match val {
		Value::Strand(_) => Ok(val),
		_ => Ok(Value::Strand(val.as_strand())),
	}
}

pub fn datetime(_: &Context, val: Value) -> Result<Value, Error> {
	match val {
		Value::Datetime(_) => Ok(val),
		_ => Ok(Value::Datetime(val.as_datetime())),
	}
}

pub fn duration(_: &Context, val: Value) -> Result<Value, Error> {
	match val {
		Value::Duration(_) => Ok(val),
		_ => Ok(Value::Duration(val.as_duration())),
	}
}
