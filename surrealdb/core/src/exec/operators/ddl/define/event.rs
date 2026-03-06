use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{EventDefinition, EventKind, TableDefinition};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::statements::define::DefineKind;
use crate::expr::{Base, Expr};
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::key::table::ev;
use crate::val::{TableName, Value};

#[derive(Debug)]
pub struct DefineEventPlan {
	pub kind: DefineKind,
	pub name: Arc<dyn PhysicalExpr>,
	pub target_table: Arc<dyn PhysicalExpr>,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Arc<dyn PhysicalExpr>,
	pub event_kind: EventKind,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineEventPlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: Arc<dyn PhysicalExpr>,
		target_table: Arc<dyn PhysicalExpr>,
		when: Expr,
		then: Vec<Expr>,
		comment: Arc<dyn PhysicalExpr>,
		event_kind: EventKind,
	) -> Self {
		Self {
			kind,
			name,
			target_table,
			when,
			then,
			comment,
			event_kind,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineEventPlan {
	ddl_operator_common!("DefineEvent", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let target_table = self.target_table.clone();
		let when = self.when.clone();
		let then = self.then.clone();
		let comment = self.comment.clone();
		let event_kind = self.event_kind.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, kind, &*name, &*target_table, when, then, &*comment, event_kind).await
			})
		})
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name_expr: &dyn PhysicalExpr,
	target_table_expr: &dyn PhysicalExpr,
	when: Expr,
	then: Vec<Expr>,
	comment_expr: &dyn PhysicalExpr,
	event_kind: EventKind,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;
	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;

	let name = helpers::eval_ident(name_expr, ctx).await?;
	let target_table = TableName::new(helpers::eval_ident(target_table_expr, ctx).await?);

	let txn = ctx.txn();

	if txn.get_tb_event(ns, db, &target_table, &name).await.is_ok() {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::EvAlreadyExists {
						name: name.clone()
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let tb = txn.get_or_add_tb(Some(ctx.ctx()), ns_name, db_name, &target_table).await?;

	let comment = helpers::eval_comment(comment_expr, ctx).await?;

	let auth_limit = AuthLimit::new_from_auth(ctx.auth()).into();

	let key = ev::new(ns, db, &target_table, &name);
	txn.set(
		&key,
		&EventDefinition {
			name: name.clone(),
			target_table: target_table.clone(),
			when,
			then,
			auth_limit,
			comment,
			kind: event_kind,
		},
		None,
	)
	.await?;

	let tb_def = TableDefinition {
		cache_events_ts: Uuid::now_v7(),
		..tb.as_ref().clone()
	};
	txn.put_tb(ns_name, db_name, &tb_def).await?;

	if let Some(cache) = ctx.ctx().get_cache() {
		cache.clear_tb(ns, db, &target_table);
	}
	txn.clear_cache();
	Ok(Value::None)
}
