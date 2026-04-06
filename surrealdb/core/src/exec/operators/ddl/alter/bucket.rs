use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::Permission;
use crate::catalog::providers::BucketProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug)]
pub struct AlterBucketPlan {
	pub name: String,
	pub if_exists: bool,
	pub backend: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub readonly: AlterKind<()>,
	pub comment: AlterKind<String>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterBucketPlan {
	pub(crate) fn new(
		name: String,
		if_exists: bool,
		backend: AlterKind<String>,
		permissions: Option<Permission>,
		readonly: AlterKind<()>,
		comment: AlterKind<String>,
	) -> Self {
		Self {
			name,
			if_exists,
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
impl ExecOperator for AlterBucketPlan {
	ddl_operator_common!("AlterBucket", ContextLevel::Database, strict);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let backend = self.backend.clone();
		let permissions = self.permissions.clone();
		let readonly = self.readonly.clone();
		let comment = self.comment.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, name, if_exists, backend, permissions, readonly, comment).await
			})
		})
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	name: String,
	if_exists: bool,
	backend: AlterKind<String>,
	permissions: Option<Permission>,
	readonly: AlterKind<()>,
	comment: AlterKind<String>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Bucket, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let mut bu = match txn.get_db_bucket(ns, db, &name).await? {
		Some(v) => v.as_ref().clone(),
		None => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(Error::BuNotFound {
				name,
			}
			.into());
		}
	};

	match backend {
		AlterKind::Set(ref v) => bu.backend = Some(v.clone()),
		AlterKind::Drop => bu.backend = None,
		AlterKind::None => {}
	}

	if let Some(ref p) = permissions {
		bu.permissions = p.clone();
	}

	match readonly {
		AlterKind::Set(_) => bu.readonly = true,
		AlterKind::Drop => bu.readonly = false,
		AlterKind::None => {}
	}

	match comment {
		AlterKind::Set(ref v) => bu.comment = Some(v.clone()),
		AlterKind::Drop => bu.comment = None,
		AlterKind::None => {}
	}

	let key = crate::key::database::bu::new(ns, db, &name);
	txn.set(&key, &bu, None).await?;
	txn.clear_cache();
	Ok(Value::None)
}
