pub(crate) mod cache;
pub(crate) mod host;
use anyhow::Result;
use surrealism_runtime::controller::Controller;
use surrealism_runtime::host::InvocationContext;

use crate::expr::Kind;
use crate::val::Value;

pub(crate) trait InternalSurrealismController {
	fn args(
		&mut self,
		context: &mut dyn InvocationContext,
		sub: Option<String>,
	) -> Result<Vec<Kind>>;
	fn returns(&mut self, context: &mut dyn InvocationContext, sub: Option<String>)
	-> Result<Kind>;
	fn run(
		&mut self,
		context: &mut dyn InvocationContext,
		sub: Option<String>,
		args: Vec<Value>,
	) -> Result<Value>;
}

impl InternalSurrealismController for Controller {
	fn args(
		&mut self,
		context: &mut dyn InvocationContext,
		sub: Option<String>,
	) -> Result<Vec<Kind>> {
		self.with_context(context, |controller| {
			controller.args(sub).map(|x| x.into_iter().map(|x| x.into()).collect())
		})
	}

	fn returns(
		&mut self,
		context: &mut dyn InvocationContext,
		sub: Option<String>,
	) -> Result<Kind> {
		self.with_context(context, |controller| controller.returns(sub).map(Into::into))
	}

	fn run(
		&mut self,
		context: &mut dyn InvocationContext,
		sub: Option<String>,
		args: Vec<Value>,
	) -> Result<Value> {
		let args: Result<Vec<crate::types::PublicValue>, _> =
			args.into_iter().map(|x| x.try_into()).collect();
		let args = args?;
		self.with_context(context, |controller| controller.invoke(sub, args).map(|x| x.into()))
	}
}
