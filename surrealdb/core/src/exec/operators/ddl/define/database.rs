use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::DatabaseDefinition;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::statements::define::DefineKind;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct DefineDatabasePlan {
	pub kind: DefineKind,
	pub name: Arc<dyn PhysicalExpr>,
	pub strict: bool,
	pub comment: Arc<dyn PhysicalExpr>,
	pub changefeed: Option<ChangeFeed>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineDatabasePlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: Arc<dyn PhysicalExpr>,
		strict: bool,
		comment: Arc<dyn PhysicalExpr>,
		changefeed: Option<ChangeFeed>,
	) -> Self {
		Self {
			kind,
			name,
			strict,
			comment,
			changefeed,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineDatabasePlan {
	ddl_operator_common!("DefineDatabase", ContextLevel::Namespace);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let strict = self.strict;
		let comment = self.comment.clone();
		let changefeed = self.changefeed;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(
				async move { execute(&ctx, kind, &*name, strict, &*comment, changefeed).await },
			)
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name_expr: &dyn PhysicalExpr,
	strict: bool,
	comment_expr: &dyn PhysicalExpr,
	changefeed: Option<ChangeFeed>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;

	let ns_ctx = ctx.namespace()?;
	let ns_name = &ns_ctx.ns.name;

	let txn = ctx.txn();
	let nsv = txn.get_or_add_ns(Some(ctx.ctx()), ns_name).await?;

	let name = helpers::eval_ident(name_expr, ctx).await?;

	let database_id = if let Some(db) = txn.get_db_by_name(ns_name, &name).await? {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::DbAlreadyExists {
						name: name.clone(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
		db.database_id
	} else {
		ctx.ctx().try_get_sequences()?.next_database_id(Some(ctx.ctx()), nsv.namespace_id).await?
	};

	let comment = helpers::eval_comment(comment_expr, ctx).await?;

	let db_def = DatabaseDefinition {
		namespace_id: nsv.namespace_id,
		database_id,
		name: name.clone(),
		comment,
		changefeed,
		strict,
	};
	txn.put_db(&nsv.name, db_def).await?;

	if let Some(cache) = ctx.ctx().get_cache() {
		cache.clear();
	}
	txn.clear_cache();
	Ok(Value::None)
}
