//! JavaScript function expression - embedded script functions.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql};

use super::helpers::args_access_mode;
#[cfg(feature = "scripting")]
use super::helpers::evaluate_args;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::{FlowResult, Script};
use crate::val::Value;

/// JavaScript function expression - embedded script functions.
#[derive(Debug, Clone)]
pub struct JsFunctionExec {
	#[cfg_attr(not(feature = "scripting"), allow(unused_variables))]
	pub(crate) script: Script,
	pub(crate) arguments: Vec<Arc<dyn PhysicalExpr>>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for JsFunctionExec {
	fn name(&self) -> &'static str {
		"JsFunction"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Script functions access database context through the frozen context
		// when needed, so they can operate at root level. Requiring Database
		// here would cause failures when no namespace/database is selected.
		crate::exec::ContextLevel::Root
	}

	#[cfg(feature = "scripting")]
	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		use reblessive::TreeStack;

		use crate::doc::CursorDoc;
		use crate::fnc::script;

		// Get the frozen context and options
		let frozen_ctx = ctx.exec_ctx.ctx().clone();
		let opt = ctx
			.exec_ctx
			.options()
			.ok_or_else(|| anyhow::anyhow!("Script functions require Options context"))?
			.clone();

		// Check if scripting is allowed
		frozen_ctx.check_allowed_scripting()?;

		// Evaluate all arguments
		let args = evaluate_args(&self.arguments, ctx.clone()).await?;

		// Build CursorDoc from current value
		let doc = ctx.current_value.map(|v| CursorDoc::new(None, None, v.clone()));

		// Execute the script within a TreeStack context
		// This is required because JavaScript can call back into SurrealDB functions
		// via surrealdb.functions.* which need TreeStack for recursive computation
		let mut stack = TreeStack::new();
		Ok(stack
			.enter(|_stk| async {
				script::run(&frozen_ctx, &opt, doc.as_ref(), &self.script.0, args).await
			})
			.finish()
			.await?)
	}

	#[cfg(not(feature = "scripting"))]
	async fn evaluate(&self, _ctx: EvalContext<'_>) -> FlowResult<Value> {
		Err(crate::err::Error::InvalidScript {
			message: String::from("Embedded functions are not enabled."),
		}
		.into())
	}

	fn access_mode(&self) -> AccessMode {
		// Script functions are always potentially read-write
		AccessMode::ReadWrite.combine(args_access_mode(&self.arguments))
	}
}

impl ToSql for JsFunctionExec {
	fn fmt_sql(&self, f: &mut String, _fmt: SqlFormat) {
		f.push_str("function(...)");
	}
}
