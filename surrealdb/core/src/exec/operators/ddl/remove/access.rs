use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::AuthorisationProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers;
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream,
};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct RemoveAccessPlan {
	pub name: Arc<dyn PhysicalExpr>,
	pub base: Base,
	pub if_exists: bool,
	pub required_context: ContextLevel,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveAccessPlan {
	pub(crate) fn new(
		name: Arc<dyn PhysicalExpr>,
		base: Base,
		if_exists: bool,
		required_context: ContextLevel,
	) -> Self {
		Self {
			name,
			base,
			if_exists,
			required_context,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveAccessPlan {
	fn name(&self) -> &'static str {
		"RemoveAccess"
	}

	fn required_context(&self) -> ContextLevel {
		self.required_context
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadWrite
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn is_scalar(&self) -> bool {
		true
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let base = self.base;
		let if_exists = self.if_exists;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, &*name, base, if_exists).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name_expr: &dyn PhysicalExpr,
	base: Base,
	if_exists: bool,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Actor, &base)?;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	match base {
		Base::Root => {
			let Some(ac) = txn.get_root_access(&name).await? else {
				if if_exists {
					return Ok(Value::None);
				} else {
					return Err(anyhow::Error::new(Error::AccessRootNotFound {
						ac: name,
					}));
				}
			};
			txn.del_root_access(&ac.name).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
		Base::Ns => {
			let ns = ctx.namespace()?.ns.namespace_id;
			let Some(ac) = txn.get_ns_access(ns, &name).await? else {
				if if_exists {
					return Ok(Value::None);
				} else {
					let ns_name = &ctx.namespace()?.ns.name;
					return Err(anyhow::Error::new(Error::AccessNsNotFound {
						ac: name,
						ns: ns_name.clone(),
					}));
				}
			};
			txn.del_ns_access(ns, &ac.name).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
		Base::Db => {
			let db_ctx = ctx.database()?;
			let ns = db_ctx.ns_ctx.ns.namespace_id;
			let db = db_ctx.db.database_id;
			let Some(ac) = txn.get_db_access(ns, db, &name).await? else {
				if if_exists {
					return Ok(Value::None);
				} else {
					let ns_name = &db_ctx.ns_ctx.ns.name;
					let db_name = &db_ctx.db.name;
					return Err(anyhow::Error::new(Error::AccessDbNotFound {
						ac: name,
						ns: ns_name.clone(),
						db: db_name.clone(),
					}));
				}
			};
			txn.del_db_access(ns, db, &ac.name).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
	}
}
