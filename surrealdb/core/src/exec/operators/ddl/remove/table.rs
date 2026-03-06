use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{TableDefinition, ViewDefinition};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::types::{PublicAction, PublicNotification, PublicValue};
use crate::val::{TableName, Value};

#[derive(Debug)]
pub struct RemoveTablePlan {
	pub name: Arc<dyn PhysicalExpr>,
	pub if_exists: bool,
	pub expunge: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveTablePlan {
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
impl ExecOperator for RemoveTablePlan {
	ddl_operator_common!("RemoveTable", ContextLevel::Database);

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
	opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;
	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;

	let name = TableName::new(helpers::eval_ident(name_expr, ctx).await?);

	let txn = ctx.txn();

	let Some(tb) = txn.get_tb(ns, db, &name).await? else {
		if if_exists {
			return Ok(Value::None);
		}
		return Err(Error::TbNotFound {
			name,
		}
		.into());
	};

	ctx.ctx()
		.get_index_stores()
		.table_removed(ctx.ctx().get_index_builder(), &txn, ns, db, &tb)
		.await?;

	let fts = txn.all_tb_views(ns, db, &name).await?;
	if !fts.is_empty() {
		let mut message =
			format!("Cannot delete table `{name}` on which a view is defined, table(s) `");
		for (idx, f) in fts.iter().enumerate() {
			if idx != 0 {
				message.push_str("`, `")
			}
			message.push_str(&f.name);
		}
		message.push_str("` are defined as a view on this table.");
		bail!(Error::Query {
			message
		});
	}

	let lvs = txn.all_tb_lives(ns, db, &name).await?;

	if expunge {
		txn.clr_tb(ns_name, db_name, &name).await?
	} else {
		txn.del_tb(ns_name, db_name, &name).await?
	};

	let key = crate::key::table::all::new(ns, db, &name);
	if expunge {
		txn.clrp(&key).await?
	} else {
		txn.delp(&key).await?
	};

	if let Some(view) = &tb.view {
		let (ViewDefinition::Materialized {
			tables,
			..
		}
		| ViewDefinition::Aggregated {
			tables,
			..
		}
		| ViewDefinition::Select {
			tables,
			..
		}) = &view;

		for ft in tables.iter() {
			let key = crate::key::table::ft::new(ns, db, ft, &name);
			txn.del(&key).await?;
			let foreign_tb = txn.expect_tb(ns, db, ft).await?;
			txn.put_tb(
				ns_name,
				db_name,
				&TableDefinition {
					cache_tables_ts: Uuid::now_v7(),
					..foreign_tb.as_ref().clone()
				},
			)
			.await?;
		}
	}

	if let Some(sender) = opt.broker.as_ref() {
		for lv in lvs.iter() {
			sender
				.send(PublicNotification::new(
					lv.id.into(),
					None,
					PublicAction::Killed,
					PublicValue::None,
					PublicValue::None,
				))
				.await;
		}
	}

	if let Some(cache) = ctx.ctx().get_cache() {
		cache.clear_tb(ns, db, &name);
		cache.clear();
	}
	txn.clear_cache();
	Ok(Value::None)
}
