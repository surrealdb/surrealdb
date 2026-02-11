//! Namespace INFO operator - returns namespace-level metadata.
//!
//! Implements INFO FOR NS [STRUCTURE] which returns information about:
//! - Namespace accesses
//! - Databases
//! - Namespace users

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::{AuthorisationProvider, DatabaseProvider, UserProvider};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream,
};
use crate::expr::statements::info::InfoStructure;
use crate::iam::{Action, ResourceKind};
use crate::val::{Object, Value};

/// Namespace INFO operator.
///
/// Returns namespace-level metadata including accesses, databases, and users.
#[derive(Debug, Clone)]
pub struct NamespaceInfoPlan {
	/// Whether to return structured output
	pub structured: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for NamespaceInfoPlan {
	fn name(&self) -> &'static str {
		"InfoNamespace"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("structured".to_string(), self.structured.to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Namespace
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let structured = self.structured;
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			match execute_namespace_info(&ctx, structured).await {
				Ok(value) => Ok(ValueBatch {
					values: vec![value],
				}),
				Err(e) => Err(crate::expr::ControlFlow::Err(e)),
			}
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

async fn execute_namespace_info(ctx: &ExecutionContext, structured: bool) -> Result<Value> {
	// Check permissions
	let root = ctx.root();
	let opt = root
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;

	// Allowed to run?
	opt.is_allowed(Action::View, ResourceKind::Any, &crate::expr::Base::Ns)?;

	// Get namespace context
	let ns_ctx = ctx.namespace()?;
	let ns = ns_ctx.ns.namespace_id;

	// Get the transaction
	let txn = ctx.txn();

	// Create the result set
	if structured {
		let object = map! {
			"accesses".to_string() => process(txn.all_ns_accesses(ns).await?),
			"databases".to_string() => process(txn.all_db(ns).await?),
			"users".to_string() => process(txn.all_ns_users(ns).await?),
		};
		Ok(Value::Object(Object(object)))
	} else {
		let object = map! {
			"accesses".to_string() => {
				let mut out = Object::default();
				for v in txn.all_ns_accesses(ns).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"databases".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db(ns).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"users".to_string() => {
				let mut out = Object::default();
				for v in txn.all_ns_users(ns).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
		};
		Ok(Value::Object(Object(object)))
	}
}

fn process<T>(a: Arc<[T]>) -> Value
where
	T: InfoStructure + Clone,
{
	Value::Array(a.iter().cloned().map(InfoStructure::structure).collect())
}
