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
pub struct RemoveDatabasePlan {
	pub name: Arc<dyn PhysicalExpr>,
	pub if_exists: bool,
	pub expunge: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveDatabasePlan {
	pub(crate) fn new(name: Arc<dyn PhysicalExpr>, if_exists: bool, expunge: bool) -> Self {
		Self {
			name,
			if_exists,
			expunge,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveDatabasePlan {
	ddl_operator_common!("RemoveDatabase", ContextLevel::Namespace);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let expunge = self.expunge;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, &*name, if_exists, expunge).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name_expr: &dyn PhysicalExpr,
	if_exists: bool,
	expunge: bool,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;

	let ns_ctx = ctx.namespace()?;
	let ns_name = &ns_ctx.ns.name;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	let db = match txn.get_db_by_name(ns_name, &name).await? {
		Some(x) => x,
		None => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(Error::DbNotFound {
				name,
			}
			.into());
		}
	};

	ctx.ctx()
		.get_index_stores()
		.database_removed(ctx.ctx().get_index_builder(), &txn, db.namespace_id, db.database_id)
		.await?;

	if let Some(seq) = ctx.ctx().get_sequences() {
		seq.database_removed(&txn, db.namespace_id, db.database_id).await?;
	}

	txn.del_db(ns_name, &db.name, expunge).await?;

	if let Some(cache) = ctx.ctx().get_cache() {
		cache.clear();
	}
	txn.clear_cache();
	Ok(Value::None)
}
