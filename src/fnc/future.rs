use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::value::Value;

pub fn run(_: &Runtime, expr: Value) -> Result<Value, Error> {
	Ok(expr)
}
