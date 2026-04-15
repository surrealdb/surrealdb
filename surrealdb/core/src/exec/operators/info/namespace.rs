//! Namespace INFO operator - returns namespace-level metadata.
//!
//! Implements INFO FOR NS [VERSION timestamp] [STRUCTURE] which returns information about:
//! - Namespace accesses
//! - Databases
//! - Namespace users

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::{AuthorisationProvider, DatabaseProvider, UserProvider};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};
use crate::expr::statements::info::InfoStructure;
use crate::iam::{Action, ResourceKind};
use crate::val::{Datetime, Object, Value};

/// Namespace INFO operator.
///
/// Returns namespace-level metadata including accesses, databases, and users.
#[derive(Debug)]
pub struct NamespaceInfoPlan {
	/// Whether to return structured output
	pub structured: bool,
	/// Optional version timestamp to filter schema by
	pub version: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl NamespaceInfoPlan {
	pub(crate) fn new(structured: bool, version: Option<Arc<dyn PhysicalExpr>>) -> Self {
		Self {
			structured,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for NamespaceInfoPlan {
	fn name(&self) -> &'static str {
		"InfoNamespace"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("structured".to_string(), self.structured.to_string())];
		if self.version.is_some() {
			attrs.push(("version".to_string(), "<expr>".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		let version_ctx =
			self.version.as_ref().map(|e| e.required_context()).unwrap_or(ContextLevel::Root);
		version_ctx.max(ContextLevel::Namespace)
	}

	fn access_mode(&self) -> AccessMode {
		self.version.as_ref().map(|e| e.access_mode()).unwrap_or(AccessMode::ReadOnly)
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		if let Some(ref v) = self.version {
			vec![("version", v)]
		} else {
			vec![]
		}
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let structured = self.structured;
		let version = self.version.clone();
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			let value = execute_namespace_info(&ctx, structured, version.as_deref()).await?;
			Ok(ValueBatch {
				values: vec![value],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

async fn execute_namespace_info(
	ctx: &ExecutionContext,
	structured: bool,
	version: Option<&dyn PhysicalExpr>,
) -> crate::expr::FlowResult<Value> {
	let root = ctx.root();
	let opt = root
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;

	opt.is_allowed(Action::View, ResourceKind::Any, &crate::expr::Base::Ns)?;

	let ns_ctx = ctx.namespace()?;
	let ns = ns_ctx.ns.namespace_id;

	let version = match version {
		Some(v) => {
			let eval_ctx = EvalContext::from_exec_ctx(ctx);
			let value = v.evaluate(eval_ctx).await?;
			Some(
				value
					.cast_to::<Datetime>()
					.map_err(|e| anyhow::anyhow!("{e}"))?
					.to_version_stamp(ctx.txn().timestamp_impl().as_ref())?,
			)
		}
		None => None,
	};

	let txn = ctx.txn();

	if structured {
		let object = map! {
			"accesses".to_string() => process(txn.all_ns_accesses(ns, version).await?),
			"databases".to_string() => process(txn.all_db(ns, version).await?),
			"users".to_string() => process(txn.all_ns_users(ns, version).await?),
		};
		Ok(Value::Object(Object(object)))
	} else {
		let object = map! {
			"accesses".to_string() => {
				let mut out = Object::default();
				for v in txn.all_ns_accesses(ns, version).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"databases".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db(ns, version).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"users".to_string() => {
				let mut out = Object::default();
				for v in txn.all_ns_users(ns, version).await?.iter() {
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
