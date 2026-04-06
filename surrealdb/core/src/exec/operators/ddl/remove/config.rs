use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::base::Base as CatalogBase;
use crate::catalog::providers::{DatabaseProvider, RootProvider};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers;
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream,
};
use crate::iam::{Action, ConfigKind, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug)]
pub struct RemoveConfigPlan {
	pub kind: ConfigKind,
	pub if_exists: bool,
	pub required_context: ContextLevel,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveConfigPlan {
	pub(crate) fn new(kind: ConfigKind, if_exists: bool, required_context: ContextLevel) -> Self {
		Self {
			kind,
			if_exists,
			required_context,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveConfigPlan {
	fn name(&self) -> &'static str {
		"RemoveConfig"
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
		let kind = self.kind.clone();
		let if_exists = self.if_exists;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, kind, if_exists).await })
		})
	}
}

async fn execute(ctx: &ExecutionContext, kind: ConfigKind, if_exists: bool) -> Result<Value> {
	let base = kind.base();
	let expr_base: crate::expr::Base = base.clone().into();
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Config(kind.clone()), &expr_base)?;

	let cg = match &kind {
		ConfigKind::GraphQL => "graphql",
		ConfigKind::Api => "api",
		ConfigKind::Default => "default",
	};

	let txn = ctx.txn();

	match base {
		CatalogBase::Root => {
			if txn.get_root_config(cg).await?.is_none() {
				if if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::CgNotFound {
						name: cg.to_string(),
					}
					.into());
				}
			}
			let key = crate::key::root::root_config::new(cg);
			txn.del(&key).await?;
		}
		CatalogBase::Db => {
			let db_ctx = ctx.database()?;
			let ns = db_ctx.ns_ctx.ns.namespace_id;
			let db = db_ctx.db.database_id;
			if txn.get_db_config(ns, db, cg).await?.is_none() {
				if if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::CgNotFound {
						name: cg.to_string(),
					}
					.into());
				}
			}
			let key = crate::key::database::cg::new(ns, db, cg);
			txn.del(&key).await?;
		}
		CatalogBase::Ns => {
			anyhow::bail!("config on namespace scope is not supported");
		}
	}

	txn.clear_cache();
	Ok(Value::None)
}
