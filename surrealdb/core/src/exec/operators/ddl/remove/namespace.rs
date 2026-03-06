use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::NamespaceProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct RemoveNamespacePlan {
	pub name: Arc<dyn PhysicalExpr>,
	pub if_exists: bool,
	pub expunge: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RemoveNamespacePlan {
	pub(crate) fn new(name: Arc<dyn PhysicalExpr>, if_exists: bool, expunge: bool) -> Self {
		Self {
			name,
			if_exists,
			expunge,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RemoveNamespacePlan {
	ddl_operator_common!("RemoveNamespace", ContextLevel::Root);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let expunge = self.expunge;
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, &*name, if_exists, expunge).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name_expr: &dyn PhysicalExpr,
	if_exists: bool,
	expunge: bool,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	let ns = match txn.get_ns_by_name(&name).await? {
		Some(x) => x,
		None => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(Error::NsNotFound {
				name,
			}
			.into());
		}
	};

	ctx.ctx()
		.get_index_stores()
		.namespace_removed(ctx.ctx().get_index_builder(), &txn, ns.namespace_id)
		.await?;

	if let Some(seq) = ctx.ctx().get_sequences() {
		seq.namespace_removed(&txn, ns.namespace_id).await?;
	}

	let key = crate::key::root::ns::new(&ns.name);
	let namespace_root = crate::key::namespace::all::new(ns.namespace_id);
	if expunge {
		txn.clr(&key).await?;
		txn.clrp(&namespace_root).await?;
	} else {
		txn.del(&key).await?;
		txn.delp(&namespace_root).await?;
	};

	if let Some(cache) = ctx.ctx().get_cache() {
		cache.clear();
	}
	txn.clear_cache();
	Ok(Value::None)
}
