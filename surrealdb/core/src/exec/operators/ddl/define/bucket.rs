use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::providers::BucketProvider;
use crate::catalog::{BucketDefinition, Permission};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::iam::{Action, ResourceKind};
use crate::key::database::bu;
use crate::val::Value;

#[derive(Debug)]
pub struct DefineBucketPlan {
	pub kind: DefineKind,
	pub name: Arc<dyn PhysicalExpr>,
	pub backend: Option<Arc<dyn PhysicalExpr>>,
	pub permissions: Permission,
	pub readonly: bool,
	pub comment: Arc<dyn PhysicalExpr>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineBucketPlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: Arc<dyn PhysicalExpr>,
		backend: Option<Arc<dyn PhysicalExpr>>,
		permissions: Permission,
		readonly: bool,
		comment: Arc<dyn PhysicalExpr>,
	) -> Self {
		Self {
			kind,
			name,
			backend,
			permissions,
			readonly,
			comment,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineBucketPlan {
	ddl_operator_common!("DefineBucket", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let backend = self.backend.clone();
		let permissions = self.permissions.clone();
		let readonly = self.readonly;
		let comment = self.comment.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, kind, &*name, backend.as_deref(), permissions, readonly, &*comment)
					.await
			})
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name_expr: &dyn PhysicalExpr,
	backend_expr: Option<&dyn PhysicalExpr>,
	permissions: Permission,
	readonly: bool,
	comment_expr: &dyn PhysicalExpr,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Bucket, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	if let Some(bucket) = txn.get_db_bucket(ns, db, &name).await? {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::BuAlreadyExists {
						value: bucket.name.clone(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let backend = if let Some(expr) = backend_expr {
		Some(
			helpers::eval_value(expr, ctx)
				.await?
				.coerce_to::<String>()
				.map_err(anyhow::Error::from)?,
		)
	} else {
		None
	};

	if let Some(buckets) = ctx.ctx().get_buckets() {
		buckets.new_backend(ns, db, &name, readonly, backend.as_deref()).await?;
	} else {
		bail!(Error::BucketUnavailable(name));
	}

	let comment = helpers::eval_comment(comment_expr, ctx).await?;

	let key = bu::new(ns, db, &name);
	let definition = BucketDefinition {
		id: None,
		name: name.clone(),
		backend,
		permissions,
		readonly,
		comment,
	};
	txn.set(&key, &definition, None).await?;

	txn.clear_cache();
	Ok(Value::None)
}
