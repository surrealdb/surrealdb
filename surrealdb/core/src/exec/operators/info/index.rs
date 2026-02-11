//! Index INFO operator - returns index building status.
//!
//! Implements INFO FOR INDEX name ON TABLE table [STRUCTURE] which returns
//! information about whether an index is currently being built.
//!
//! Note: The index builder status is only available in certain execution contexts.
//! When not available, an empty object is returned.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream,
};
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
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for IndexInfoPlan {
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

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let index = self.index.clone();
		let table = self.table.clone();
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			let value = execute_index_info(&ctx, &*index, &*table).await?;
			Ok(ValueBatch {
				values: vec![value],
			})
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
) -> crate::expr::FlowResult<Value> {
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

	let index = index_value.coerce_to::<String>().map_err(|e| anyhow::anyhow!("{e}"))?;
	let table =
		TableName::new(table_value.coerce_to::<String>().map_err(|e| anyhow::anyhow!("{e}"))?);

	// Get the index builder from the frozen context
	let frozen_ctx = ctx.ctx();
	if let Some(ib) = frozen_ctx.get_index_builder() {
		// Get namespace and database IDs
		let (ns, db) = frozen_ctx.expect_ns_db_ids(opt).await?;
		// Get the transaction
		let txn = ctx.txn();
		// Obtain the index definition
		let ix = txn.expect_tb_index(ns, db, &table, &index).await?;
		// Get the building status
		let status = ib.get_status(ns, db, &ix).await;
		let mut out = Object::default();
		out.insert("building".to_string(), status.into());
		return Ok(out.into());
	}

	// Fallback: return empty object if index builder not available
	Ok(Object::default().into())
}
