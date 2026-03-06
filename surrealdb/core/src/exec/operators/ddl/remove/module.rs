use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::DatabaseProvider;
#[cfg_attr(not(feature = "surrealism"), allow(unused_imports))]
use crate::catalog::{ModuleExecutable, ModuleName};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;
use crate::val::Value;

#[derive(Debug)]
pub struct RemoveModulePlan {
	pub name: ModuleName,
	pub if_exists: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveModulePlan {
	pub(crate) fn new(name: ModuleName, if_exists: bool) -> Self {
		Self {
			name,
			if_exists,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveModulePlan {
	ddl_operator_common!("RemoveModule", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, name, if_exists).await })
		})
	}
}

async fn execute(ctx: &ExecutionContext, name: ModuleName, if_exists: bool) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Module, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();
	let storage_name = name.get_storage_name();

	#[cfg_attr(not(feature = "surrealism"), allow(unused_variables))]
	let md = match txn.get_db_module(ns, db, &storage_name).await {
		Ok(x) => x,
		Err(e) => {
			if if_exists && matches!(e.downcast_ref(), Some(Error::MdNotFound { .. })) {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	let key = crate::key::database::md::new(ns, db, &storage_name);
	txn.del(&key).await?;

	txn.clear_cache();

	#[cfg(feature = "surrealism")]
	if let Some(cache) = ctx.ctx().get_surrealism_cache() {
		let lookup = match &md.executable {
			ModuleExecutable::Surrealism(surrealism) => {
				SurrealismCacheLookup::File(&ns, &db, &surrealism.bucket, &surrealism.key)
			}
			ModuleExecutable::Silo(silo) => SurrealismCacheLookup::Silo(
				&silo.organisation,
				&silo.package,
				silo.major,
				silo.minor,
				silo.patch,
			),
		};
		cache.remove(&lookup);
	}

	Ok(Value::None)
}
