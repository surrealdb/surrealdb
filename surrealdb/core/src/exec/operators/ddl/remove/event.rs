use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::{TableName, Value};

#[derive(Debug)]
pub struct RemoveEventPlan {
	pub name: Arc<dyn PhysicalExpr>,
	pub table_name: Arc<dyn PhysicalExpr>,
	pub if_exists: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveEventPlan {
	pub(crate) fn new(
		name: Arc<dyn PhysicalExpr>,
		table_name: Arc<dyn PhysicalExpr>,
		if_exists: bool,
	) -> Self {
		Self {
			name,
			table_name,
			if_exists,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveEventPlan {
	ddl_operator_common!("RemoveEvent", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let table_name = self.table_name.clone();
		let if_exists = self.if_exists;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, &*name, &*table_name, if_exists).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name_expr: &dyn PhysicalExpr,
	table_name_expr: &dyn PhysicalExpr,
	if_exists: bool,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;
	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;

	let name = helpers::eval_ident(name_expr, ctx).await?;
	let table_name = TableName::new(helpers::eval_ident(table_name_expr, ctx).await?);

	let txn = ctx.txn();

	let ev = match txn.get_tb_event(ns, db, &table_name, &name).await {
		Ok(x) => x,
		Err(e) => {
			if if_exists && matches!(e.downcast_ref(), Some(Error::EvNotFound { .. })) {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	let key = crate::key::table::ev::new(ns, db, &ev.target_table, &ev.name);
	txn.del(&key).await?;

	let tb = txn.expect_tb(ns, db, &table_name).await?;
	txn.put_tb(
		ns_name,
		db_name,
		&TableDefinition {
			cache_events_ts: Uuid::now_v7(),
			..tb.as_ref().clone()
		},
	)
	.await?;

	if let Some(cache) = ctx.ctx().get_cache() {
		cache.clear_tb(ns, db, &table_name);
	}
	txn.clear_cache();
	Ok(Value::None)
}
