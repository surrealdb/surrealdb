use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{ModuleName, Permission};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug)]
pub struct AlterModulePlan {
	pub name: ModuleName,
	pub if_exists: bool,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterModulePlan {
	pub(crate) fn new(
		name: ModuleName,
		if_exists: bool,
		comment: AlterKind<String>,
		permissions: Option<Permission>,
	) -> Self {
		Self {
			name,
			if_exists,
			comment,
			permissions,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterModulePlan {
	ddl_operator_common!("AlterModule", ContextLevel::Database, strict);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let comment = self.comment.clone();
		let permissions = self.permissions.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, name, if_exists, comment, permissions).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name: ModuleName,
	if_exists: bool,
	comment: AlterKind<String>,
	permissions: Option<Permission>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Module, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let storage_name = name.get_storage_name();
	let mut md = match txn.get_db_module(ns, db, &storage_name).await {
		Ok(v) => v.as_ref().clone(),
		Err(e) => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	match comment {
		AlterKind::Set(ref v) => md.comment = Some(v.clone()),
		AlterKind::Drop => md.comment = None,
		AlterKind::None => {}
	}

	if let Some(ref p) = permissions {
		md.permissions = p.clone();
	}

	txn.put_db_module(ns, db, &md).await?;
	txn.clear_cache();
	Ok(Value::None)
}
