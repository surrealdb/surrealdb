use crate::ctx::Context;
use crate::err::Error;
use crate::sql::Value;

/// Return the next value for a given sequence.
pub async fn nextval(_ctx: &Context, (seq,): (Value,)) -> Result<Value, Error> {
	Ok(seq.to_string().len().into())
}
