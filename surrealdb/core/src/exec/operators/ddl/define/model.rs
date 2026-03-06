use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{MlModelDefinition, Permission};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::iam::{Action, ResourceKind};
use crate::key::database::ml;
use crate::val::Value;

#[derive(Debug)]
pub struct DefineModelPlan {
	pub kind: DefineKind,
	pub hash: String,
	pub name: String,
	pub version: String,
	pub comment: Arc<dyn PhysicalExpr>,
	pub permissions: Permission,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineModelPlan {
	pub(crate) fn new(
		kind: DefineKind,
		hash: String,
		name: String,
		version: String,
		comment: Arc<dyn PhysicalExpr>,
		permissions: Permission,
	) -> Self {
		Self {
			kind,
			hash,
			name,
			version,
			comment,
			permissions,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineModelPlan {
	ddl_operator_common!("DefineModel", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let hash = self.hash.clone();
		let name = self.name.clone();
		let version = self.version.clone();
		let comment = self.comment.clone();
		let permissions = self.permissions.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, kind, hash, name, version, &*comment, permissions).await
			})
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	hash: String,
	name: String,
	version: String,
	comment_expr: &dyn PhysicalExpr,
	permissions: Permission,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Model, &Base::Db)?;

	let txn = ctx.txn();

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	if txn.get_db_model(ns, db, &name, &version).await?.is_some() {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::MlAlreadyExists {
						name: name.clone()
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let comment = helpers::eval_comment(comment_expr, ctx).await?;

	let key = ml::new(ns, db, &name, &version);
	txn.set(
		&key,
		&MlModelDefinition {
			hash,
			name: name.clone(),
			version: version.clone(),
			comment,
			permissions,
		},
		None,
	)
	.await?;
	txn.clear_cache();
	Ok(Value::None)
}
