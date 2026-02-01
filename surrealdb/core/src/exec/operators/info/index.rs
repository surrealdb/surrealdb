//! Index INFO operator - returns index building status.
//!
//! Implements INFO FOR INDEX name ON TABLE table [STRUCTURE] which returns
//! information about whether an index is currently being built.
//!
//! Note: The index builder status is only available in certain execution contexts.
//! When not available, an empty object is returned.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{AccessMode, FlowResult, OperatorPlan, ValueBatch, ValueBatchStream};
use crate::iam::{Action, ResourceKind};
use crate::val::{Object, TableName, Value};

/// Index INFO operator.
///
/// Returns information about whether an index is currently being built.
#[derive(Debug)]
pub struct IndexInfoPlan {
	/// Index name expression
	pub index: Arc<dyn PhysicalExpr>,
	/// Table name expression
	pub table: Arc<dyn PhysicalExpr>,
	/// Whether to return structured output (currently ignored for index info)
	pub structured: bool,
}

#[async_trait]
impl OperatorPlan for IndexInfoPlan {
	fn name(&self) -> &'static str {
		"InfoIndex"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("index".to_string(), self.index.to_sql()),
			("table".to_string(), self.table.to_sql()),
			("structured".to_string(), self.structured.to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let index = self.index.clone();
		let table = self.table.clone();
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			match execute_index_info(&ctx, &*index, &*table).await {
				Ok(value) => Ok(ValueBatch {
					values: vec![value],
				}),
				Err(e) => Err(crate::expr::ControlFlow::Err(e)),
			}
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

async fn execute_index_info(
	ctx: &ExecutionContext,
	index_expr: &dyn PhysicalExpr,
	table_expr: &dyn PhysicalExpr,
) -> Result<Value> {
	// Check permissions
	let root = ctx.root();
	let opt = root
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;

	// Allowed to run?
	opt.is_allowed(Action::View, ResourceKind::Actor, &crate::expr::Base::Db)?;

	// Evaluate the index and table name expressions
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let index_value = index_expr.evaluate(eval_ctx.clone()).await?;
	let table_value = table_expr.evaluate(eval_ctx).await?;

	let _index = index_value.coerce_to::<String>()?;
	let _table = TableName::new(table_value.coerce_to::<String>()?);

	// Note: The index builder status is only available in certain execution
	// contexts (via FrozenContext::get_index_builder()). The streaming executor's
	// ExecutionContext doesn't currently have access to the index builder.
	//
	// The original implementation returns an empty object when the index builder
	// is not available, so we do the same here.
	//
	// TODO: If index building status is needed in the streaming executor,
	// we would need to add index_builder access to ExecutionContext.

	Ok(Object::default().into())
}
