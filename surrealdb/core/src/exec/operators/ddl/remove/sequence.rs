use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::DatabaseProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::key::database::sq::Sq;
use crate::key::sequence::Prefix;
use crate::val::Value;

#[derive(Debug)]
pub struct RemoveSequencePlan {
	pub name: Arc<dyn PhysicalExpr>,
	pub if_exists: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveSequencePlan {
	pub(crate) fn new(name: Arc<dyn PhysicalExpr>, if_exists: bool) -> Self {
		Self {
			name,
			if_exists,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveSequencePlan {
	ddl_operator_common!("RemoveSequence", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, &*name, if_exists).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name_expr: &dyn PhysicalExpr,
	if_exists: bool,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	let sq = match txn.get_db_sequence(ns, db, &name).await {
		Ok(x) => x,
		Err(e) => {
			if if_exists && matches!(e.downcast_ref(), Some(Error::SeqNotFound { .. })) {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	if let Some(seq) = ctx.ctx().get_sequences() {
		seq.sequence_removed(ns, db, &name).await;
	}

	let ba_range = Prefix::new_ba_range(ns, db, &sq.name)?;
	txn.delr(ba_range).await?;
	let st_range = Prefix::new_st_range(ns, db, &sq.name)?;
	txn.delr(st_range).await?;

	let key = Sq::new(ns, db, &name);
	txn.del(&key).await?;

	txn.clear_cache();
	Ok(Value::None)
}
