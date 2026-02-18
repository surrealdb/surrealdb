//! Table INFO operator - returns table-level metadata.
//!
//! Implements INFO FOR TABLE name [STRUCTURE] [VERSION timestamp] which returns information about:
//! - Events
//! - Fields
//! - Indexes
//! - Live queries
//! - Views (tables that reference this table)

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};
use crate::expr::statements::info::InfoStructure;
use crate::iam::{Action, ResourceKind};
use crate::val::{Datetime, Object, TableName, Value};

/// Table INFO operator.
///
/// Returns table-level metadata including events, fields, indexes,
/// live queries, and views.
#[derive(Debug)]
pub struct TableInfoPlan {
	/// Table name expression
	pub table: Arc<dyn PhysicalExpr>,
	/// Whether to return structured output
	pub structured: bool,
	/// Optional version timestamp to filter schema by
	pub version: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl TableInfoPlan {
	pub(crate) fn new(
		table: Arc<dyn PhysicalExpr>,
		structured: bool,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			table,
			structured,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for TableInfoPlan {
	fn name(&self) -> &'static str {
		"InfoTable"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![
			("table".to_string(), self.table.to_sql()),
			("structured".to_string(), self.structured.to_string()),
		];
		if self.version.is_some() {
			attrs.push(("version".to_string(), "<expr>".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Table info needs database context, combined with expression contexts
		let version_ctx =
			self.version.as_ref().map(|e| e.required_context()).unwrap_or(ContextLevel::Root);
		self.table.required_context().max(version_ctx).max(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
		// Info is inherently read-only, but the table/version expressions
		// could theoretically contain mutation subqueries.
		let version_mode =
			self.version.as_ref().map(|e| e.access_mode()).unwrap_or(AccessMode::ReadOnly);
		self.table.access_mode().combine(version_mode)
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		let mut exprs = vec![("table", &self.table)];
		if let Some(ref v) = self.version {
			exprs.push(("version", v));
		}
		exprs
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let table = self.table.clone();
		let structured = self.structured;
		let version = self.version.clone();
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			let value = execute_table_info(&ctx, &*table, structured, version.as_deref()).await?;
			Ok(ValueBatch {
				values: vec![value],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

async fn execute_table_info(
	ctx: &ExecutionContext,
	table_expr: &dyn PhysicalExpr,
	structured: bool,
	version: Option<&dyn PhysicalExpr>,
) -> crate::expr::FlowResult<Value> {
	// Check permissions
	let root = ctx.root();
	let opt = root
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;

	// Allowed to run?
	opt.is_allowed(Action::View, ResourceKind::Any, &crate::expr::Base::Db)?;

	// Get database context
	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	// Evaluate the table name expression
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let table_value = table_expr.evaluate(eval_ctx.clone()).await?;
	let tb = TableName::new(table_value.coerce_to::<String>().map_err(|e| anyhow::anyhow!("{e}"))?);

	// Convert the version to u64 if present
	let version = match version {
		Some(v) => {
			let value = v.evaluate(eval_ctx).await?;
			Some(
				value
					.cast_to::<Datetime>()
					.map_err(|e| anyhow::anyhow!("{e}"))?
					.to_version_stamp()?,
			)
		}
		None => None,
	};

	// Get the transaction
	let txn = ctx.txn();

	// Create the result set
	if structured {
		Ok(Value::from(map! {
			"events".to_string() => process(txn.all_tb_events(ns, db, &tb).await?),
			"fields".to_string() => process(txn.all_tb_fields(ns, db, &tb, version).await?),
			"indexes".to_string() => process(txn.all_tb_indexes(ns, db, &tb).await?),
			"lives".to_string() => process(txn.all_tb_lives(ns, db, &tb).await?),
			"tables".to_string() => process(txn.all_tb_views(ns, db, &tb).await?),
		}))
	} else {
		Ok(Value::from(map! {
			"events".to_string() => {
				let mut out = Object::default();
				for v in txn.all_tb_events(ns, db, &tb).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"fields".to_string() => {
				let mut out = Object::default();
				for v in txn.all_tb_fields(ns, db, &tb, version).await?.iter() {
					out.insert(v.name.to_raw_string(), v.to_sql().into());
				}
				out.into()
			},
			"indexes".to_string() => {
				let mut out = Object::default();
				for v in txn.all_tb_indexes(ns, db, &tb).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"lives".to_string() => {
				let mut out = Object::default();
				for v in txn.all_tb_lives(ns, db, &tb).await?.iter() {
					out.insert(v.id.to_string(), v.to_sql().into());
				}
				out.into()
			},
			"tables".to_string() => {
				let mut out = Object::default();
				for v in txn.all_tb_views(ns, db, &tb).await?.iter() {
					out.insert(v.name.clone().into_string(), v.to_sql().into());
				}
				out.into()
			},
		}))
	}
}

fn process<T>(a: Arc<[T]>) -> Value
where
	T: InfoStructure + Clone,
{
	Value::Array(a.iter().cloned().map(InfoStructure::structure).collect())
}
