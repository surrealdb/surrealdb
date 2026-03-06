use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::DatabaseProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct RemoveAnalyzerPlan {
	pub name: Arc<dyn PhysicalExpr>,
	pub if_exists: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveAnalyzerPlan {
	pub(crate) fn new(name: Arc<dyn PhysicalExpr>, if_exists: bool) -> Self {
		Self {
			name,
			if_exists,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveAnalyzerPlan {
	ddl_operator_common!("RemoveAnalyzer", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, &*name, if_exists).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name_expr: &dyn PhysicalExpr,
	if_exists: bool,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	let az = match txn.get_db_analyzer(ns, db, &name).await {
		Ok(x) => x,
		Err(e) => {
			if if_exists && matches!(e.downcast_ref(), Some(Error::AzNotFound { .. })) {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	let key = crate::key::database::az::new(ns, db, &az.name);
	txn.del(&key).await?;

	let azs = txn.all_db_analyzers(ns, db).await?;
	ctx.ctx().get_index_stores().mappers().cleanup(&azs);

	txn.clear_cache();
	Ok(Value::None)
}
