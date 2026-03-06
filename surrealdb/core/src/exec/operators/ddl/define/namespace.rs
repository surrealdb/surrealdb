use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::NamespaceDefinition;
use crate::catalog::providers::NamespaceProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Debug)]
pub struct DefineNamespacePlan {
	pub kind: DefineKind,
	pub name: Arc<dyn PhysicalExpr>,
	pub comment: Arc<dyn PhysicalExpr>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineNamespacePlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: Arc<dyn PhysicalExpr>,
		comment: Arc<dyn PhysicalExpr>,
	) -> Self {
		Self {
			kind,
			name,
			comment,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineNamespacePlan {
	ddl_operator_common!("DefineNamespace", ContextLevel::Root);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let comment = self.comment.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, kind, &*name, &*comment).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name_expr: &dyn PhysicalExpr,
	comment_expr: &dyn PhysicalExpr,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	let namespace_id = if let Some(ns) = txn.get_ns_by_name(&name).await? {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::NsAlreadyExists {
						name: name.clone(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
		ns.namespace_id
	} else {
		ctx.ctx().try_get_sequences()?.next_namespace_id(Some(ctx.ctx())).await?
	};

	let comment = helpers::eval_comment(comment_expr, ctx).await?;

	txn.put_ns(NamespaceDefinition {
		namespace_id,
		name,
		comment,
	})
	.await?;
	txn.clear_cache();
	Ok(Value::None)
}
