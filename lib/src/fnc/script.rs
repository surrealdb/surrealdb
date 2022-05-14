use crate::ctx::Context;
use crate::err::Error;
use crate::sql::script::Script;
use crate::sql::value::Value;

pub fn run(_ctx: &Context, _expr: Script) -> Result<Value, Error> {
	Err(Error::InvalidScript {
		message: String::from("Embedded functions are not yet supported."),
	})
}
