use anyhow::{Result, bail};
use surrealdb_types::ToSql;

use crate::exec::Error as ExecError;
use crate::expr::{Error as ExprError, Operation};
use crate::fnc::args::Optional;
use crate::val::{Closure, ClosureEvaluator, Value};

pub async fn chain(
	ev: Option<&dyn ClosureEvaluator>,
	(value, worker): (Value, Box<Closure>),
) -> Result<Value> {
	if let Some(ev) = ev {
		ev.invoke(&worker, vec![value]).await
	} else {
		Ok(Value::None)
	}
}

pub async fn expect(
	ev: Option<&dyn ClosureEvaluator>,
	(value, worker, Optional(message)): (Value, Box<Closure>, Optional<Value>),
) -> Result<Value> {
	if let Some(ev) = ev {
		let got = ev.invoke(&worker, vec![value.clone()]).await?;
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
			other @ (Value::None
			| Value::Null
			| Value::Number(_)
			| Value::String(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_)) => {
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
