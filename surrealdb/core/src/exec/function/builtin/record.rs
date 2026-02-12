//! Record functions

use anyhow::Result;
use reblessive::tree::TreeStack;

use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::fnc::args::FromArgs;
use crate::val::Value;
use crate::{define_pure_function, register_functions};

define_pure_function!(RecordId, "record::id", (record: Any) -> Any, crate::fnc::record::id);
define_pure_function!(RecordTb, "record::tb", (record: Any) -> String, crate::fnc::record::tb);
define_pure_function!(RecordTable, "record::table", (record: Any) -> String, crate::fnc::record::tb);

// =========================================================================
// record::exists - Check if a record exists in the database
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct RecordExists;

impl ScalarFunction for RecordExists {
	fn name(&self) -> &'static str {
		"record::exists"
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("record", Kind::Any).returns(Kind::Bool)
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let args = FromArgs::from_args("record::exists", args)?;
			let frozen = ctx.exec_ctx.ctx();
			let opt = ctx.exec_ctx.options();
			// Note: CursorDoc is not available in the streaming executor context
			let doc = None;
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async move {
					crate::fnc::record::exists((stk, frozen, opt, doc), args).await
				})
				.finish()
				.await
		})
	}
}

// =========================================================================
// record::is_edge - Check if a record is an edge
// =========================================================================

#[derive(Debug, Clone, Copy, Default)]
pub struct RecordIsEdge;

impl ScalarFunction for RecordIsEdge {
	fn name(&self) -> &'static str {
		"record::is_edge"
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("record", Kind::Any).returns(Kind::Bool)
	}

	fn is_pure(&self) -> bool {
		false
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let args = FromArgs::from_args("record::is_edge", args)?;
			let frozen = ctx.exec_ctx.ctx();
			let opt = ctx.exec_ctx.options();
			// Note: CursorDoc is not available in the streaming executor context
			let doc = None;
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async move {
					crate::fnc::record::is::edge((stk, frozen, opt, doc), args).await
				})
				.finish()
				.await
		})
	}
}

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, RecordId, RecordTb, RecordTable);
	registry.register(RecordExists);
	registry.register(RecordIsEdge);
}
