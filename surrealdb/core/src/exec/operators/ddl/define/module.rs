use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{ModuleDefinition, Permission};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::statements::define::DefineKind;
use crate::expr::{Base, ModuleExecutable};
use crate::iam::{Action, ResourceKind};
#[cfg(feature = "surrealism")]
use crate::surrealism::cache::SurrealismCacheLookup;
use crate::val::Value;

#[derive(Debug)]
pub struct DefineModulePlan {
	pub kind: DefineKind,
	pub name: Option<String>,
	pub storage_name: String,
	pub executable: ModuleExecutable,
	pub comment: Arc<dyn PhysicalExpr>,
	pub permissions: Permission,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineModulePlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: Option<String>,
		storage_name: String,
		executable: ModuleExecutable,
		comment: Arc<dyn PhysicalExpr>,
		permissions: Permission,
	) -> Self {
		Self {
			kind,
			name,
			storage_name,
			executable,
			comment,
			permissions,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineModulePlan {
	ddl_operator_common!("DefineModule", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let storage_name = self.storage_name.clone();
		let executable = self.executable.clone();
		let comment = self.comment.clone();
		let permissions = self.permissions.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, kind, name, storage_name, executable, &*comment, permissions).await
			})
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name: Option<String>,
	storage_name: String,
	executable: ModuleExecutable,
	comment_expr: &dyn PhysicalExpr,
	permissions: Permission,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Module, &Base::Db)?;

	let txn = ctx.txn();

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	if txn.get_db_module(ns, db, &storage_name).await.is_ok() {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::MdAlreadyExists {
						name: storage_name
					});
				}
			}
			DefineKind::Overwrite =>
			{
				#[cfg(feature = "surrealism")]
				if let Some(cache) = ctx.ctx().get_surrealism_cache() {
					let lookup = match &executable {
						ModuleExecutable::Surrealism(surrealism) => SurrealismCacheLookup::File(
							&ns,
							&db,
							&surrealism.0.bucket,
							&surrealism.0.key,
						),
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
			}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;
	txn.get_or_add_db(Some(ctx.ctx()), ns_name, db_name).await?;

	let comment = helpers::eval_comment(comment_expr, ctx).await?;

	txn.put_db_module(
		ns,
		db,
		&ModuleDefinition {
			name,
			executable: executable.into(),
			comment,
			permissions,
		},
	)
	.await?;
	txn.clear_cache();
	Ok(Value::None)
}
