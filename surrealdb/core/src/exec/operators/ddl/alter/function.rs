use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Block, Kind};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug)]
pub struct AlterFunctionPlan {
	pub name: String,
	pub if_exists: bool,
	pub args: AlterKind<Vec<(String, Kind)>>,
	pub block: AlterKind<Block>,
	pub comment: AlterKind<String>,
	pub permissions: Option<Permission>,
	pub returns: AlterKind<Kind>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterFunctionPlan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		name: String,
		if_exists: bool,
		args: AlterKind<Vec<(String, Kind)>>,
		block: AlterKind<Block>,
		comment: AlterKind<String>,
		permissions: Option<Permission>,
		returns: AlterKind<Kind>,
	) -> Self {
		Self {
			name,
			if_exists,
			args,
			block,
			comment,
			permissions,
			returns,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterFunctionPlan {
	ddl_operator_common!("AlterFunction", ContextLevel::Database, strict);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let args = self.args.clone();
		let block = self.block.clone();
		let comment = self.comment.clone();
		let permissions = self.permissions.clone();
		let returns = self.returns.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, name, if_exists, args, block, comment, permissions, returns).await
			})
		})
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	name: String,
	if_exists: bool,
	args: AlterKind<Vec<(String, Kind)>>,
	block: AlterKind<Block>,
	comment: AlterKind<String>,
	permissions: Option<Permission>,
	returns: AlterKind<Kind>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let mut fc = match txn.get_db_function(ns, db, &name).await {
		Ok(v) => v.as_ref().clone(),
		Err(e) => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	match args {
		AlterKind::Set(ref v) => fc.args.clone_from(v),
		AlterKind::Drop => fc.args = vec![],
		AlterKind::None => {}
	}

	match block {
		AlterKind::Set(ref v) => fc.block = v.clone(),
		AlterKind::Drop | AlterKind::None => {}
	}

	match comment {
		AlterKind::Set(ref v) => fc.comment = Some(v.clone()),
		AlterKind::Drop => fc.comment = None,
		AlterKind::None => {}
	}

	if let Some(ref p) = permissions {
		fc.permissions = p.clone();
	}

	match returns {
		AlterKind::Set(ref v) => fc.returns = Some(v.clone()),
		AlterKind::Drop => fc.returns = None,
		AlterKind::None => {}
	}

	let key = crate::key::database::fc::new(ns, db, &name);
	txn.set(&key, &fc, None).await?;
	txn.clear_cache();
	Ok(Value::None)
}
