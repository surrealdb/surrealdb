use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::paths::ID;
use crate::sql::thing::Thing;
use crate::sql::value::Value;
use reblessive::tree::Stk;

pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, Option<&Options>, Option<&CursorDoc>),
	(arg,): (Thing,),
) -> Result<Value, Error> {
	if let Some(opt) = opt {
		Ok(match Value::Thing(arg).get(stk, ctx, opt, doc, ID.as_ref()).await? {
			Value::None => Value::Bool(false),
			_ => Value::Bool(true),
		})
	} else {
		Ok(Value::None)
	}
}

pub fn id((arg,): (Thing,)) -> Result<Value, Error> {
	Ok(arg.id.into())
}

pub fn tb((arg,): (Thing,)) -> Result<Value, Error> {
	Ok(arg.tb.into())
}
