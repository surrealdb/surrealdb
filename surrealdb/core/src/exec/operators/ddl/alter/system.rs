use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::kvs::Key;
use crate::val::{Duration, Value};

pub struct AlterSystemPlan {
	pub query_timeout: Option<Arc<dyn PhysicalExpr>>,
	pub drop_timeout: bool,
	pub compact: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl fmt::Debug for AlterSystemPlan {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("AlterSystemPlan")
			.field("query_timeout", &self.query_timeout.as_ref().map(|_| ".."))
			.field("drop_timeout", &self.drop_timeout)
			.field("compact", &self.compact)
			.finish()
	}
}

impl AlterSystemPlan {
	pub(crate) fn new(
		query_timeout: Option<Arc<dyn PhysicalExpr>>,
		drop_timeout: bool,
		compact: bool,
	) -> Self {
		Self {
			query_timeout,
			drop_timeout,
			compact,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterSystemPlan {
	ddl_operator_common!("AlterSystem", ContextLevel::Root);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let query_timeout = self.query_timeout.clone();
		let drop_timeout = self.drop_timeout;
		let compact = self.compact;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(
				async move { execute(&ctx, query_timeout.as_deref(), drop_timeout, compact).await },
			)
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	query_timeout: Option<&dyn PhysicalExpr>,
	drop_timeout: bool,
	compact: bool,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Any, &Base::Root)?;

	if compact {
		ctx.txn().compact::<Key>(None).await?;
	}

	if let Some(timeout_expr) = query_timeout {
		let timeout = helpers::eval_value(timeout_expr, ctx)
			.await?
			.cast_to::<Duration>()
			.map_err(anyhow::Error::from)?;
		opt.dynamic_configuration().set_query_timeout(Some(timeout.0));
	} else if drop_timeout {
		opt.dynamic_configuration().set_query_timeout(None);
	}

	Ok(Value::None)
}
