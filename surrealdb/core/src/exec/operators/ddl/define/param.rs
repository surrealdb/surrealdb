use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{ParamDefinition, Permission};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct DefineParamPlan {
	pub kind: DefineKind,
	pub name: String,
	pub value: Arc<dyn PhysicalExpr>,
	pub comment: Arc<dyn PhysicalExpr>,
	pub permissions: Permission,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineParamPlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: String,
		value: Arc<dyn PhysicalExpr>,
		comment: Arc<dyn PhysicalExpr>,
		permissions: Permission,
	) -> Self {
		Self {
			kind,
			name,
			value,
			comment,
			permissions,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineParamPlan {
	ddl_operator_common!("DefineParam", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let value = self.value.clone();
		let comment = self.comment.clone();
		let permissions = self.permissions.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(
				async move { execute(&ctx, kind, name, &*value, &*comment, permissions).await },
			)
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name: String,
	value_expr: &dyn PhysicalExpr,
	comment_expr: &dyn PhysicalExpr,
	permissions: Permission,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Parameter, &Base::Db)?;

	let value = helpers::eval_value(value_expr, ctx).await?;
	let txn = ctx.txn();

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	if txn.get_db_param(ns, db, &name).await.is_ok() {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::PaAlreadyExists {
						name: name.clone()
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;
	let db_def = txn.get_or_add_db(Some(ctx.ctx()), ns_name, db_name).await?;

	let comment = helpers::eval_comment(comment_expr, ctx).await?;
	txn.put_db_param(
		db_def.namespace_id,
		db_def.database_id,
		&ParamDefinition {
			name,
			value,
			comment,
			permissions,
		},
	)
	.await?;
	txn.clear_cache();
	Ok(Value::None)
}
