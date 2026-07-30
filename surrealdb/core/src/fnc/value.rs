use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::expr::{Error as ExprError, Operation};
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
				let suffix = match message {
					None | Some(Value::None) => String::new(),
					Some(user_message) => {
						format!(" with message: {}", user_message.to_sql())
					}
				};
				bail!(ExecError::Thrown(format!("value::expect assertion failed{suffix}")))
			}
			other => {
				bail!(ExprError::InvalidFunctionArguments {
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
