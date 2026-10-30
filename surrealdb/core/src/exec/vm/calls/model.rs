//! ML model function expressions. SurrealDB does not run models: every `ml::`
//! call fails before any argument is evaluated.

use std::convert::Infallible;

use crate::exec::{Error as ExecError, EvalContext};
use crate::expr::FlowResult;
use crate::val::Value;

/// Refuses every call: no model is read, so the call fails with the reason.
pub(crate) async fn enter_model(
	name: &str,
	version: &str,
	ctx: &EvalContext<'_>,
) -> FlowResult<Infallible> {
	let _ = (name, version, ctx);
	Err(ExecError::InvalidModel {
		message: String::from("Machine learning computation is not enabled."),
	}
	.into())
}

/// Unreachable: [`enter_model`] refuses every call before a model is read, so
/// no definition ever reaches this function.
pub(crate) async fn compute_model(
	name: &str,
	version: &str,
	definition: &Infallible,
	args: Vec<Value>,
	ctx: &EvalContext<'_>,
) -> FlowResult<Value> {
	let _ = (name, version, args, ctx);
	match *definition {}
}
