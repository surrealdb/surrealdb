use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{FunctionDefinition, Permission};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::statements::define::DefineKind;
use crate::expr::{Base, Block, Kind};
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct DefineFunctionPlan {
	pub kind: DefineKind,
	pub name: String,
	pub args: Vec<(String, Kind)>,
	pub block: Block,
	pub returns: Option<Kind>,
	pub comment: Arc<dyn PhysicalExpr>,
	pub permissions: Permission,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineFunctionPlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: String,
		args: Vec<(String, Kind)>,
		block: Block,
		returns: Option<Kind>,
		comment: Arc<dyn PhysicalExpr>,
		permissions: Permission,
	) -> Self {
		Self {
			kind,
			name,
			args,
			block,
			returns,
			comment,
			permissions,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineFunctionPlan {
	ddl_operator_common!("DefineFunction", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let args = self.args.clone();
		let block = self.block.clone();
		let returns = self.returns.clone();
		let comment = self.comment.clone();
		let permissions = self.permissions.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, kind, name, args, block, returns, &*comment, permissions).await
			})
		})
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name: String,
	args: Vec<(String, Kind)>,
	block: Block,
	returns: Option<Kind>,
	comment_expr: &dyn PhysicalExpr,
	permissions: Permission,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;

	let txn = ctx.txn();

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	if txn.get_db_function(ns, db, &name).await.is_ok() {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::FcAlreadyExists {
						name: name.clone()
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;
	txn.get_or_add_db(Some(ctx.ctx()), ns_name, db_name).await?;

	let comment = helpers::eval_comment(comment_expr, ctx).await?;
	let auth_limit = AuthLimit::new_from_auth(ctx.auth()).into();

	txn.put_db_function(
		ns,
		db,
		&FunctionDefinition {
			name,
			args,
			block,
			comment,
			permissions,
			returns,
			auth_limit,
		},
	)
	.await?;
	txn.clear_cache();
	Ok(Value::None)
}
