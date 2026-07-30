//! Flow-control handling for evaluator results.

use anyhow::Result;

use crate::expr::{ControlFlow, FlowResult};
use crate::val::Value;

/// Helper trait to catch controlflow return unwinding.
pub(crate) trait FlowResultExt {
	/// Function which catches `ControlFlow::Return(x)` and turns it into
	/// `Ok(x)`.
	///
	/// If the error value is either `ControlFlow::Break` or
	/// `ControlFlow::Continue` it will instead create an error that
	/// break/continue was used within an invalid location.
	fn catch_return(self) -> Result<Value, anyhow::Error>;

	/// Falls back to `Value::None` for ignorable errors (type mismatches, etc.),
	/// but propagates consequential errors (storage, timeout, permissions).
	fn or_none(self) -> FlowResult<Value>;
}

impl FlowResultExt for FlowResult<Value> {
	fn catch_return(self) -> Result<Value, anyhow::Error> {
		match self {
			Err(ControlFlow::Break) | Err(ControlFlow::Continue) => {
				Err(anyhow::Error::new(crate::exec::Error::InvalidControlFlow))
			}
			Err(ControlFlow::Return(x)) => Ok(x),
			Err(ControlFlow::Err(e)) => Err(e),
			Ok(x) => Ok(x),
		}
	}

	fn or_none(self) -> FlowResult<Value> {
		match self {
			Ok(v) => Ok(v),
			Err(cf) if cf.is_ignorable() => Ok(Value::None),
			Err(cf) => Err(cf),
		}
	}
}
