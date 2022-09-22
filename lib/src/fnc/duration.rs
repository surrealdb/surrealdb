use crate::err::Error;
use crate::sql::value::Value;

pub fn secs((duration,): (Value,)) -> Result<Value, Error> {
	let duration = match duration {
		Value::Duration(d) => d,
		_ => return Ok(Value::None),
	};

	Ok(duration.secs())
}

pub fn mins((duration,): (Value,)) -> Result<Value, Error> {
	let duration = match duration {
		Value::Duration(d) => d,
		_ => return Ok(Value::None),
	};

	Ok(duration.mins())
}

pub fn hours((duration,): (Value,)) -> Result<Value, Error> {
	let duration = match duration {
		Value::Duration(d) => d,
		_ => return Ok(Value::None),
	};

	Ok(duration.hours())
}

pub fn days((duration,): (Value,)) -> Result<Value, Error> {
	let duration = match duration {
		Value::Duration(d) => d,
		_ => return Ok(Value::None),
	};

	Ok(duration.days())
}

pub fn weeks((duration,): (Value,)) -> Result<Value, Error> {
	let duration = match duration {
		Value::Duration(d) => d,
		_ => return Ok(Value::None),
	};

	Ok(duration.weeks())
}

pub fn years((duration,): (Value,)) -> Result<Value, Error> {
	let duration = match duration {
		Value::Duration(d) => d,
		_ => return Ok(Value::None),
	};

	Ok(duration.years())
}
