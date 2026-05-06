use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Operation;
use crate::fnc::args::Optional;
use crate::val::{Closure, Value};

pub async fn chain(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(value, worker): (Value, Box<Closure>),
) -> Result<Value> {
	if let Some(opt) = opt {
		worker.invoke(stk, ctx, opt, doc, vec![value]).await
	} else {
		Ok(Value::None)
	}
}

pub async fn expect(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, Option<&Options>, Option<&CursorDoc>),
	(value, worker, Optional(message)): (Value, Box<Closure>, Optional<Value>),
) -> Result<Value> {
	if let Some(opt) = opt {
		let got = worker.invoke(stk, ctx, opt, doc, vec![value.clone()]).await?;
		match got {
			Value::Bool(true) => Ok(value),
			Value::Bool(false) => {
				if let Some(Value::String(user_message)) = message {
					bail!(Error::Thrown(format!(
						"value::expect assertion failed with message: '{user_message}'"
					)))
				} else {
					bail!(Error::Thrown("value::expect assertion failed".to_owned()))
				}
			}
			other => {
				bail!(Error::InvalidFunctionArguments {
					name: "value::expect".to_owned(),
					message: format!(
						"Assertion closure must return a bool, got {}",
						other.kind_of()
					),
				})
			}
		}
	} else {
		Ok(Value::None)
	}
}

pub async fn diff((val1, val2): (Value, Value)) -> Result<Value> {
	Ok(Operation::operations_to_value(val1.diff(&val2)))
}

pub async fn patch((mut val, diff): (Value, Value)) -> Result<Value> {
	val.patch(diff)?;
	Ok(val)
}
