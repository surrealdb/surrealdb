use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

pub struct AlterParamPlan {
	pub name: String,
	pub if_exists: bool,
	pub value: Option<Arc<dyn PhysicalExpr>>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl fmt::Debug for AlterParamPlan {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("AlterParamPlan")
			.field("name", &self.name)
			.field("if_exists", &self.if_exists)
			.field("value", &self.value.as_ref().map(|_| ".."))
			.field("comment", &self.comment)
			.field("permissions", &self.permissions)
			.finish()
	}
}

impl AlterParamPlan {
	pub(crate) fn new(
		name: String,
		if_exists: bool,
		value: Option<Arc<dyn PhysicalExpr>>,
		comment: AlterKind<String>,
		permissions: Option<Permission>,
	) -> Self {
		Self {
			name,
			if_exists,
			value,
			comment,
			permissions,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterParamPlan {
	ddl_operator_common!("AlterParam", ContextLevel::Database, strict);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let value = self.value.clone();
		let comment = self.comment.clone();
		let permissions = self.permissions.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, name, if_exists, value.as_deref(), comment, permissions).await
			})
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name: String,
	if_exists: bool,
	value_expr: Option<&dyn PhysicalExpr>,
	comment: AlterKind<String>,
	permissions: Option<Permission>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Parameter, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let mut pa = match txn.get_db_param(ns, db, &name).await {
		Ok(v) => v.as_ref().clone(),
		Err(e) => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	if let Some(expr) = value_expr {
		pa.value = helpers::eval_value(expr, ctx).await?;
	}

	match comment {
		AlterKind::Set(ref v) => pa.comment = Some(v.clone()),
		AlterKind::Drop => pa.comment = None,
		AlterKind::None => {}
	}

	if let Some(ref p) = permissions {
		pa.permissions = p.clone();
	}

	let key = crate::key::database::pa::new(ns, db, &name);
	txn.set(&key, &pa, None).await?;
	txn.clear_cache();
	Ok(Value::None)
}
