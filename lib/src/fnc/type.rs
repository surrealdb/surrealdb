use crate::err::Error;
use crate::sql::geometry::Geometry;
use crate::sql::number::Number;
use crate::sql::table::Table;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use crate::sql::Strand;

pub fn bool((arg,): (Value,)) -> Result<Value, Error> {
	Ok(arg.is_truthy().into())
}

pub fn datetime((arg,): (Value,)) -> Result<Value, Error> {
	Ok(match arg {
		Value::Datetime(_) => arg,
		_ => Value::Datetime(arg.as_datetime()),
	})
}

pub fn decimal((arg,): (Value,)) -> Result<Value, Error> {
	Ok(match arg {
		Value::Number(Number::Decimal(_)) => arg,
		_ => Value::Number(Number::Decimal(arg.as_decimal())),
	})
}

pub fn duration((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Duration(_) => Ok(arg),
		_ => Ok(Value::Duration(arg.as_duration())),
	}
}

pub fn float((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Number(Number::Float(_)) => Ok(arg),
		_ => Ok(Value::Number(Number::Float(arg.as_float()))),
	}
}

pub fn int((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Number(Number::Int(_)) => Ok(arg),
		_ => Ok(Value::Number(Number::Int(arg.as_int()))),
	}
}

pub fn number((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Number(_) => Ok(arg),
		_ => Ok(Value::Number(arg.as_number())),
	}
}

pub fn point((arg1, arg2): (Value, Option<Value>)) -> Result<Value, Error> {
	Ok(if let Some(y) = arg2 {
		let x = arg1;
		(x.as_float(), y.as_float()).into()
	} else {
		match arg1 {
			Value::Array(v) if v.len() == 2 => v.as_point().into(),
			Value::Geometry(Geometry::Point(v)) => v.into(),
			_ => Value::None,
		}
	})
}

pub fn regex((arg,): (Value,)) -> Result<Value, Error> {
	match arg {
		Value::Strand(v) => Ok(Value::Regex(v.as_str().into())),
		_ => Ok(Value::None),
	}
}

pub fn string((arg,): (Strand,)) -> Result<Value, Error> {
	Ok(arg.into())
}

pub fn table((arg,): (Value,)) -> Result<Value, Error> {
	Ok(Value::Table(Table(match arg {
		Value::Thing(t) => t.tb,
		v => v.as_string(),
	})))
}

pub fn thing((arg1, arg2): (Value, Option<Value>)) -> Result<Value, Error> {
	Ok(if let Some(arg2) = arg2 {
		Value::Thing(Thing {
			tb: arg1.as_string(),
			id: match arg2 {
				Value::Thing(v) => v.id,
				Value::Array(v) => v.into(),
				Value::Object(v) => v.into(),
				Value::Number(Number::Int(v)) => v.into(),
				v => v.as_string().into(),
			},
		})
	} else {
		match arg1 {
			Value::Thing(v) => v.into(),
			_ => Value::None,
		}
	})
}
