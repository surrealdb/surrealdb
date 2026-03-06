use std::fmt;
use std::ops::Deref;
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
use crate::val::{Duration, Value};

pub struct AlterSequencePlan {
	pub name: String,
	pub if_exists: bool,
	pub timeout: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl fmt::Debug for AlterSequencePlan {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("AlterSequencePlan")
			.field("name", &self.name)
			.field("if_exists", &self.if_exists)
			.field("timeout", &self.timeout.as_ref().map(|_| ".."))
			.finish()
	}
}

impl AlterSequencePlan {
	pub(crate) fn new(
		name: String,
		if_exists: bool,
		timeout: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			name,
			if_exists,
			timeout,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterSequencePlan {
	ddl_operator_common!("AlterSequence", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let timeout = self.timeout.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, name, if_exists, timeout.as_deref()).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name: String,
	if_exists: bool,
	timeout_expr: Option<&dyn PhysicalExpr>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let mut sq = match txn.get_db_sequence(ns, db, &name).await {
		Ok(sq) => sq.deref().clone(),
		Err(e) => {
			if if_exists && matches!(e.downcast_ref(), Some(Error::SeqNotFound { .. })) {
				return Ok(Value::None);
			} else {
				return Err(e);
			}
		}
	};

	if let Some(timeout_expr) = timeout_expr {
		if let Some(timeout) = helpers::eval_value(timeout_expr, ctx)
			.await?
			.cast_to::<Option<Duration>>()
			.map_err(anyhow::Error::from)?
		{
			sq.timeout = Some(timeout.0);
		} else {
			sq.timeout = None;
		}
	}

	let key = Sq::new(ns, db, &name);
	txn.set(&key, &sq, None).await?;
	txn.clear_cache();
	Ok(Value::None)
}
