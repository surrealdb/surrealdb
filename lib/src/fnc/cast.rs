use crate::ctx::Context;
use crate::err::Error;
use crate::sql::value::Value;

/// Attempts to cast a value to another type
pub fn run(_: &Context, name: &str, val: Value) -> Result<Value, Error> {
	match name {
		"bool" => val.convert_to_bool().map(Value::from),
		"datetime" => val.convert_to_datetime().map(Value::from),
		"decimal" => val.convert_to_decimal().map(Value::from),
		"duration" => val.convert_to_duration().map(Value::from),
		"float" => val.convert_to_float().map(Value::from),
		"int" => val.convert_to_int().map(Value::from),
		"number" => val.convert_to_number().map(Value::from),
		"point" => val.convert_to_point().map(Value::from),
		"string" => val.convert_to_strand().map(Value::from),
		"uuid" => val.convert_to_uuid().map(Value::from),
		_ => unreachable!(),
	}
}
