use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::DatabaseProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct RemoveModelPlan {
	pub name: String,
	pub version: String,
	pub if_exists: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveModelPlan {
	pub(crate) fn new(name: String, version: String, if_exists: bool) -> Self {
		Self {
			name,
			version,
			if_exists,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveModelPlan {
	ddl_operator_common!("RemoveModel", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let version = self.version.clone();
		let if_exists = self.if_exists;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, name, version, if_exists).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name: String,
	version: String,
	if_exists: bool,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Model, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let ml = match txn.get_db_model(ns, db, &name, &version).await? {
		Some(x) => x,
		None => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(Error::MlNotFound {
				name: format!("{}<{}>", name.clone(), version.clone()),
			}
			.into());
		}
	};

	let key = crate::key::database::ml::new(ns, db, &ml.name, &ml.version);
	txn.del(&key).await?;

	txn.clear_cache();
	Ok(Value::None)
}
