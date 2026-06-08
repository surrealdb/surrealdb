//! Index INFO operator - returns index build status.
//!
//! Implements INFO FOR INDEX name ON TABLE table [STRUCTURE] which returns
//! cluster-visible index build status.
//!
//! Note: Index build status is only available in certain execution contexts.
//! When not available, an empty object is returned.

use std::sync::Arc;

use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};
use crate::iam::{Action, ResourceKind};
use crate::kvs::index::index_building_info;
use crate::val::{TableName, Value};

/// Index INFO operator.
///
/// Returns cluster-visible index build status.
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

impl IndexInfoPlan {
	pub(crate) fn new(
		index: Arc<dyn PhysicalExpr>,
		table: Arc<dyn PhysicalExpr>,
		structured: bool,
	) -> Self {
		Self {
			index,
			table,
			structured,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
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
		// Index info needs database context, combined with expression contexts
		self.index.required_context().max(self.table.required_context()).max(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
		// Info is inherently read-only, but the index/table expressions
		// could theoretically contain mutation subqueries.
		self.index.access_mode().combine(self.table.access_mode())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("index", &self.index), ("table", &self.table)]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let index = Arc::clone(&self.index);
		let table = Arc::clone(&self.table);
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
	ctx.is_allowed(Action::View, ResourceKind::Actor, crate::expr::Base::Db)?;

	// Evaluate the index and table name expressions
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let index_value = index_expr.evaluate(eval_ctx.clone()).await?;
	let table_value = table_expr.evaluate(eval_ctx).await?;

	let index = index_value.coerce_to::<String>().map_err(|e| anyhow::anyhow!("{e}"))?;
	let table =
		TableName::new(table_value.coerce_to::<String>().map_err(|e| anyhow::anyhow!("{e}"))?);

	let frozen_ctx = ctx.ctx();
	// Get namespace and database IDs
	let (ns, db) = frozen_ctx.expect_ns_db_ids(opt).await?;
	// Get the transaction
	let txn = ctx.txn();
	// Obtain the index definition
	let ix = txn.expect_tb_index(ns, db, &table, &index).await?;
	Ok(index_building_info(&txn, ns, db, &ix).await?)
}
