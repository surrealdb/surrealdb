use crate::err::Error;
use crate::sql::duration::Duration;
use crate::sql::value::Value;

pub fn secs((duration,): (Option<Value>,)) -> Result<Value, Error> {
	let duration = match duration {
		Some(Value::Duration(d)) => d,
		None => Duration::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(duration.secs())
}

pub fn mins((duration,): (Option<Value>,)) -> Result<Value, Error> {
	let duration = match duration {
		Some(Value::Duration(d)) => d,
		None => Duration::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(duration.mins())
}

pub fn hours((duration,): (Option<Value>,)) -> Result<Value, Error> {
	let duration = match duration {
		Some(Value::Duration(d)) => d,
		None => Duration::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(duration.hours())
}

pub fn days((duration,): (Option<Value>,)) -> Result<Value, Error> {
	let duration = match duration {
		Some(Value::Duration(d)) => d,
		None => Duration::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(duration.days())
}

pub fn weeks((duration,): (Option<Value>,)) -> Result<Value, Error> {
	let duration = match duration {
		Some(Value::Duration(d)) => d,
		None => Duration::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(duration.weeks())
}

pub fn years((duration,): (Option<Value>,)) -> Result<Value, Error> {
	let duration = match duration {
		Some(Value::Duration(d)) => d,
		None => Duration::default(),
		Some(_) => return Ok(Value::None),
	};

	Ok(duration.years())
}
