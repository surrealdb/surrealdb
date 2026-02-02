//! Root INFO operator - returns root-level metadata.
//!
//! Implements INFO FOR ROOT [STRUCTURE] which returns information about:
//! - Root accesses
//! - Default configuration
//! - Namespaces
//! - Nodes
//! - System information
//! - Root users
//! - Runtime configuration

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::{
	AuthorisationProvider, NamespaceProvider, NodeProvider, RootProvider, UserProvider,
};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{AccessMode, ExecOperator, FlowResult, ValueBatch, ValueBatchStream};
use crate::expr::statements::info::InfoStructure;
use crate::iam::{Action, ResourceKind};
use crate::sys::INFORMATION;
use crate::val::{Object, Value};

/// Root INFO operator.
///
/// Returns root-level metadata including accesses, namespaces, nodes,
/// system information, and users.
#[derive(Debug, Clone)]
pub struct RootInfoPlan {
	/// Whether to return structured output
	pub structured: bool,
}

#[async_trait]
impl ExecOperator for RootInfoPlan {
	fn name(&self) -> &'static str {
		"InfoRoot"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("structured".to_string(), self.structured.to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let structured = self.structured;
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			match execute_root_info(&ctx, structured).await {
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

async fn execute_root_info(ctx: &ExecutionContext, structured: bool) -> Result<Value> {
	// Check permissions - root level requires appropriate access
	let root = ctx.root();
	let opt = root
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;

	// Allowed to run?
	opt.is_allowed(Action::View, ResourceKind::Any, &crate::expr::Base::Root)?;

	// Get the transaction
	let txn = ctx.txn();

	// Create the result set
	if structured {
		let object = map! {
			"accesses".to_string() => process(txn.all_root_accesses().await?),
			"defaults".to_string() => txn.get_default_config().await?
				.map(|x| x.as_ref().clone().structure())
				.unwrap_or_else(|| Value::Object(Default::default())),
			"namespaces".to_string() => process(txn.all_ns().await?),
			"nodes".to_string() => process(txn.all_nodes().await?),
			"system".to_string() => system().await,
			"users".to_string() => process(txn.all_root_users().await?),
			"config".to_string() => opt.dynamic_configuration().clone().structure()
		};
		Ok(Value::Object(Object(object)))
	} else {
		let object = map! {
			"accesses".to_string() => {
				let mut out = Object::default();
				for v in txn.all_root_accesses().await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"defaults".to_string() => txn.get_default_config().await?
				.map(|x| x.as_ref().clone().structure())
				.unwrap_or_else(|| Value::Object(Default::default())),
			"namespaces".to_string() => {
				let mut out = Object::default();
				for v in txn.all_ns().await?.iter() {
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
				for v in txn.all_root_users().await?.iter() {
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
