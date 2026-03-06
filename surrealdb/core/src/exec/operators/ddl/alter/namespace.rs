use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct AlterNamespacePlan {
	pub compact: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterNamespacePlan {
	pub(crate) fn new(compact: bool) -> Self {
		Self {
			compact,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterNamespacePlan {
	ddl_operator_common!("AlterNamespace", ContextLevel::Namespace);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let compact = self.compact;
		helpers::ddl_stream(ctx, move |ctx| Box::pin(async move { execute(&ctx, compact).await }))
	}
}

async fn execute(ctx: &ExecutionContext, compact: bool) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;

	if compact {
		let ns_id = ctx.namespace()?.ns.namespace_id;
		let namespace_root = crate::key::namespace::all::new(ns_id);
		ctx.txn().compact(Some(namespace_root)).await?;
	}

	Ok(Value::None)
}
