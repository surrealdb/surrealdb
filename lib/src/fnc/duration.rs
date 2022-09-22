use crate::err::Error;
use crate::sql::value::Value;

pub fn days((duration,): (Value,)) -> Result<Value, Error> {
	Ok(match duration {
		Value::Duration(d) => d.days(),
		_ => Value::None,
	})
}

pub fn hours((duration,): (Value,)) -> Result<Value, Error> {
	Ok(match duration {
		Value::Duration(d) => d.hours(),
		_ => Value::None,
	})
}

pub fn mins((duration,): (Value,)) -> Result<Value, Error> {
	Ok(match duration {
		Value::Duration(d) => d.mins(),
		_ => Value::None,
	})
}

pub fn secs((duration,): (Value,)) -> Result<Value, Error> {
	Ok(match duration {
		Value::Duration(d) => d.secs(),
		_ => Value::None,
	})
}

pub fn weeks((duration,): (Value,)) -> Result<Value, Error> {
	Ok(match duration {
		Value::Duration(d) => d.weeks(),
		_ => Value::None,
	})
}

pub fn years((duration,): (Value,)) -> Result<Value, Error> {
	Ok(match duration {
		Value::Duration(d) => d.years(),
		_ => Value::None,
	})
}
