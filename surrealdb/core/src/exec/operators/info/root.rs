//! Root INFO operator - returns root-level metadata.
//!
//! Implements INFO FOR ROOT [VERSION timestamp] [STRUCTURE] which returns information about:
//! - Root accesses
//! - Default configuration
//! - Namespaces
//! - Nodes
//! - System information
//! - Root users
//! - Runtime configuration

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::{
	AuthorisationProvider, NamespaceProvider, NodeProvider, RootProvider, UserProvider,
};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};
use crate::expr::statements::info::InfoStructure;
use crate::iam::{Action, ResourceKind};
use crate::sys::INFORMATION;
use crate::val::{Datetime, Object, Value};

/// Root INFO operator.
///
/// Returns root-level metadata including accesses, namespaces, nodes,
/// system information, and users.
#[derive(Debug)]
pub struct RootInfoPlan {
	/// Whether to return structured output
	pub structured: bool,
	/// Optional version timestamp to filter schema by
	pub version: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RootInfoPlan {
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
impl ExecOperator for RootInfoPlan {
	fn name(&self) -> &'static str {
		"InfoRoot"
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
		version_ctx.max(ContextLevel::Root)
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
			let value = execute_root_info(&ctx, structured, version.as_deref()).await?;
			Ok(ValueBatch {
				values: vec![value],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

async fn execute_root_info(
	ctx: &ExecutionContext,
	structured: bool,
	version: Option<&dyn PhysicalExpr>,
) -> crate::expr::FlowResult<Value> {
	let root = ctx.root();
	let opt = root
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;

	opt.is_allowed(Action::View, ResourceKind::Any, &crate::expr::Base::Root)?;

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
			"accesses".to_string() => process(txn.all_root_accesses(version).await?),
			"defaults".to_string() => txn.get_default_config().await?
				.map(|x| x.as_ref().clone().structure())
				.unwrap_or_else(|| Value::Object(Default::default())),
			"namespaces".to_string() => process(txn.all_ns(version).await?),
			"nodes".to_string() => process(txn.all_nodes().await?),
			"system".to_string() => system().await,
			"users".to_string() => process(txn.all_root_users(version).await?),
			"config".to_string() => opt.dynamic_configuration().clone().structure()
		};
		Ok(Value::Object(Object(object)))
	} else {
		let object = map! {
			"accesses".to_string() => {
				let mut out = Object::default();
				for v in txn.all_root_accesses(version).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"defaults".to_string() => txn.get_default_config().await?
				.map(|x| x.as_ref().clone().structure())
				.unwrap_or_else(|| Value::Object(Default::default())),
			"namespaces".to_string() => {
				let mut out = Object::default();
				for v in txn.all_ns(version).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"nodes".to_string() => {
				let mut out = Object::default();
				for v in txn.all_nodes().await?.iter() {
					out.insert(v.id.to_string(), v.to_sql().into());
				}
				out.into()
			},
			"system".to_string() => system().await,
			"users".to_string() => {
				let mut out = Object::default();
				for v in txn.all_root_users(version).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"config".to_string() => {
				opt.dynamic_configuration().clone().structure()
			}
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

async fn system() -> Value {
	let info = INFORMATION.lock().await;
	Value::from(map! {
		"available_parallelism".to_string() => info.available_parallelism.into(),
		"cpu_usage".to_string() => info.cpu_usage.into(),
		"load_average".to_string() => info.load_average.iter().map(|x| Value::from(*x)).collect::<Vec<_>>().into(),
		"memory_usage".to_string() => info.memory_usage.into(),
		"physical_cores".to_string() => info.physical_cores.into(),
		"memory_allocated".to_string() => info.memory_allocated.into(),
	})
}
