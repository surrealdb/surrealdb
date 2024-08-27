use crate::err::Error;
use crate::sql::{Idiom, Value};

pub fn diff((val1, val2): (Value, Value)) -> Result<Value, Error> {
	Ok(val1.diff(&val2, Idiom::default()).into())
}

pub fn patch((mut val, diff): (Value, Value)) -> Result<Value, Error> {
	val.patch(diff)?;
	Ok(val)
}
