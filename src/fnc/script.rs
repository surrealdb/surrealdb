use crate::dbs::Runtime;
use crate::err::Error;
use crate::sql::script::Script;
use crate::sql::value::Value;

pub fn run(ctx: &Runtime, expr: Script) -> Result<Value, Error> {
	Err(Error::LanguageError {
		message: String::from("Embedded functions are not yet supported."),
	})
}
