use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct AlterDatabasePlan {
	pub compact: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterDatabasePlan {
	pub(crate) fn new(compact: bool) -> Self {
		Self {
			compact,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterDatabasePlan {
	ddl_operator_common!("AlterDatabase", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let compact = self.compact;
		helpers::ddl_stream(ctx, move |ctx| Box::pin(async move { execute(&ctx, compact).await }))
	}
}

async fn execute(ctx: &ExecutionContext, compact: bool) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;

	if compact {
		let db_ctx = ctx.database()?;
		let ns = db_ctx.ns_ctx.ns.namespace_id;
		let db = db_ctx.db.database_id;
		let database_root = crate::key::database::all::new(ns, db);
		ctx.txn().compact(Some(database_root)).await?;
	}

	Ok(Value::None)
}
