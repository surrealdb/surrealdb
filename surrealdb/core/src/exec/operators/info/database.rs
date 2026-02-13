//! Database INFO operator - returns database-level metadata.
//!
//! Implements INFO FOR DB [STRUCTURE] [VERSION timestamp] which returns information about:
//! - Database accesses
//! - APIs
//! - Analyzers
//! - Buckets
//! - Functions
//! - Modules
//! - Models
//! - Params
//! - Tables
//! - Users
//! - Configs
//! - Sequences

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::{
	ApiProvider, AuthorisationProvider, BucketProvider, DatabaseProvider, TableProvider,
	UserProvider,
};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream,
};
use crate::expr::statements::info::InfoStructure;
use crate::iam::{Action, ResourceKind};
use crate::val::{Datetime, Object, Value};

/// Database INFO operator.
///
/// Returns database-level metadata including accesses, APIs, analyzers,
/// tables, functions, and more.
#[derive(Debug)]
pub struct DatabaseInfoPlan {
	/// Whether to return structured output
	pub structured: bool,
	/// Optional version timestamp to filter schema by
	pub version: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DatabaseInfoPlan {
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
impl ExecOperator for DatabaseInfoPlan {
	fn name(&self) -> &'static str {
		"InfoDatabase"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("structured".to_string(), self.structured.to_string())];
		if self.version.is_some() {
			attrs.push(("version".to_string(), "<expr>".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// Database info needs database context, combined with expression contexts
		let version_ctx =
			self.version.as_ref().map(|e| e.required_context()).unwrap_or(ContextLevel::Root);
		version_ctx.max(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let structured = self.structured;
		let version = self.version.clone();
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			let value = execute_database_info(&ctx, structured, version.as_deref()).await?;
			Ok(ValueBatch {
				values: vec![value],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

async fn execute_database_info(
	ctx: &ExecutionContext,
	structured: bool,
	version: Option<&dyn PhysicalExpr>,
) -> crate::expr::FlowResult<Value> {
	// Check permissions
	let root = ctx.root();
	let opt = root
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;

	// Allowed to run?
	opt.is_allowed(Action::View, ResourceKind::Any, &crate::expr::Base::Db)?;

	// Get database context
	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	// Convert the version to u64 if present
	let version = match version {
		Some(v) => {
			let eval_ctx = EvalContext::from_exec_ctx(ctx);
			let value = v.evaluate(eval_ctx).await?;
			Some(
				value
					.cast_to::<Datetime>()
					.map_err(|e| anyhow::anyhow!("{e}"))?
					.to_version_stamp()?,
			)
		}
		None => None,
	};

	// Get the transaction
	let txn = ctx.txn();

	// Create the result set
	if structured {
		let object = map! {
			"accesses".to_string() => process(txn.all_db_accesses(ns, db).await?),
			"apis".to_string() => process(txn.all_db_apis(ns, db).await?),
			"analyzers".to_string() => process(txn.all_db_analyzers(ns, db).await?),
			"buckets".to_string() => process(txn.all_db_buckets(ns, db).await?),
			"functions".to_string() => process(txn.all_db_functions(ns, db).await?),
			"modules".to_string() => process(txn.all_db_modules(ns, db).await?),
			"models".to_string() => process(txn.all_db_models(ns, db).await?),
			"params".to_string() => process(txn.all_db_params(ns, db).await?),
			"tables".to_string() => process(txn.all_tb(ns, db, version).await?),
			"users".to_string() => process(txn.all_db_users(ns, db).await?),
			"configs".to_string() => process(txn.all_db_configs(ns, db).await?),
			"sequences".to_string() => process(txn.all_db_sequences(ns, db).await?),
		};
		Ok(Value::Object(Object(object)))
	} else {
		let object = map! {
			"accesses".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_accesses(ns, db).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"apis".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_apis(ns, db).await?.iter() {
					out.insert(v.path.to_string(), v.to_sql().into());
				}
				out.into()
			},
			"analyzers".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_analyzers(ns, db).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"buckets".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_buckets(ns, db).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"functions".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_functions(ns, db).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"modules".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_modules(ns, db).await?.iter() {
					out.insert(v.get_storage_name()?, v.to_sql().into());
				}
				out.into()
			},
			"models".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_models(ns, db).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"params".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_params(ns, db).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"tables".to_string() => {
				let mut out = Object::default();
				for v in txn.all_tb(ns, db, version).await?.iter() {
					out.insert(v.name.clone().into_string(), v.to_sql().into());
				}
				out.into()
			},
			"users".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_users(ns, db).await?.iter() {
					out.insert(v.name.clone(), v.to_sql().into());
				}
				out.into()
			},
			"configs".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_configs(ns, db).await?.iter() {
					out.insert(v.name(), v.to_sql().into());
				}
				out.into()
			},
			"sequences".to_string() => {
				let mut out = Object::default();
				for v in txn.all_db_sequences(ns, db).await?.iter() {
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
