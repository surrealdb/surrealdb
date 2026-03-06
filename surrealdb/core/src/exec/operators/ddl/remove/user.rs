use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::UserProvider;
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
pub struct RemoveUserPlan {
	pub name: Arc<dyn PhysicalExpr>,
	pub base: Base,
	pub if_exists: bool,
	pub required_context: ContextLevel,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveUserPlan {
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
impl ExecOperator for RemoveUserPlan {
	fn name(&self) -> &'static str {
		"RemoveUser"
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
			let us = match txn.get_root_user(&name).await? {
				Some(x) => x,
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::UserRootNotFound {
						name,
					}
					.into());
				}
			};
			let key = crate::key::root::us::new(&us.name);
			txn.del(&key).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
		Base::Ns => {
			let ns_ctx = ctx.namespace()?;
			let ns = ns_ctx.ns.namespace_id;
			let us = match txn.get_ns_user(ns, &name).await? {
				Some(x) => x,
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::UserNsNotFound {
						ns: ns_ctx.ns.name.clone(),
						name,
					}
					.into());
				}
			};
			let key = crate::key::namespace::us::new(ns, &us.name);
			txn.del(&key).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
		Base::Db => {
			let db_ctx = ctx.database()?;
			let ns = db_ctx.ns_ctx.ns.namespace_id;
			let db = db_ctx.db.database_id;
			let us = match txn.get_db_user(ns, db, &name).await? {
				Some(x) => x,
				None => {
					if if_exists {
						return Ok(Value::None);
					}
					return Err(Error::UserDbNotFound {
						ns: db_ctx.ns_ctx.ns.name.clone(),
						db: db_ctx.db.name.clone(),
						name,
					}
					.into());
				}
			};
			let key = crate::key::database::us::new(ns, db, &us.name);
			txn.del(&key).await?;
			txn.clear_cache();
			Ok(Value::None)
		}
	}
}
