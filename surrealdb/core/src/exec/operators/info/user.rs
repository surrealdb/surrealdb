//! User INFO operator - returns user information.
//!
//! Implements INFO FOR USER name [ON base] [STRUCTURE] which returns information
//! about a specific user at the root, namespace, or database level.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, UserProvider};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, ExecOperator, FlowResult, OperatorMetrics, ValueBatch, ValueBatchStream,
};
use crate::expr::Base;
use crate::expr::statements::info::InfoStructure;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// User INFO operator.
///
/// Returns information about a specific user at root, namespace, or database level.
#[derive(Debug)]
pub struct UserInfoPlan {
	/// User name expression
	pub user: Arc<dyn PhysicalExpr>,
	/// Base level (Root, Ns, or Db) - None means use current context
	pub base: Option<Base>,
	/// Whether to return structured output
	pub structured: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl UserInfoPlan {
	pub(crate) fn new(user: Arc<dyn PhysicalExpr>, base: Option<Base>, structured: bool) -> Self {
		Self {
			user,
			base,
			structured,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Determine the required context level based on the base parameter.
	///
	/// When base is None, we return Root as the minimum required context since
	/// the actual base will be determined at execution time using `Options::selected_base()`.
	fn context_level_for_base(&self) -> ContextLevel {
		match self.base {
			Some(Base::Root) | None => ContextLevel::Root,
			Some(Base::Ns) => ContextLevel::Namespace,
			Some(Base::Db) => ContextLevel::Database,
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for UserInfoPlan {
	fn name(&self) -> &'static str {
		"InfoUser"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("user".to_string(), self.user.to_sql())];
		if let Some(ref base) = self.base {
			attrs.push(("base".to_string(), format!("{:?}", base)));
		}
		attrs.push(("structured".to_string(), self.structured.to_string()));
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		self.context_level_for_base()
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(self.metrics.as_ref())
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("user", &self.user)]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let user = self.user.clone();
		let base = self.base;
		let structured = self.structured;
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			let value = execute_user_info(&ctx, &*user, base, structured).await?;
			Ok(ValueBatch {
				values: vec![value],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

async fn execute_user_info(
	ctx: &ExecutionContext,
	user_expr: &dyn PhysicalExpr,
	base: Option<Base>,
	structured: bool,
) -> crate::expr::FlowResult<Value> {
	// Check permissions
	let root = ctx.root();
	let opt = root
		.options
		.as_ref()
		.ok_or_else(|| anyhow::anyhow!("Options not available in execution context"))?;

	// Get the base type - default to current context level if not specified
	let base = base.unwrap_or(opt.selected_base()?);

	// Allowed to run?
	opt.is_allowed(Action::View, ResourceKind::Actor, &base)?;

	// Evaluate the user name expression
	let eval_ctx = EvalContext::from_exec_ctx(ctx);
	let user_value = user_expr.evaluate(eval_ctx).await?;
	let user = user_value.coerce_to::<String>().map_err(|e| anyhow::anyhow!("{e}"))?;

	// Get the transaction
	let txn = ctx.txn();

	// Process the user based on the base level
	let res = match base {
		Base::Root => txn.expect_root_user(&user).await?,
		Base::Ns => {
			let ns_name = opt.ns()?;
			let ns = txn.expect_ns_by_name(ns_name).await?;
			match txn.get_ns_user(ns.namespace_id, &user).await? {
				Some(user_def) => user_def,
				None => {
					return Err(Error::UserNsNotFound {
						name: user,
						ns: ns.name.clone(),
					}
					.into());
				}
			}
		}
		Base::Db => {
			let (ns_name, db_name) = opt.ns_db()?;
			let Some(db_def) = txn.get_db_by_name(ns_name, db_name).await? else {
				return Err(Error::UserDbNotFound {
					name: user,
					ns: ns_name.to_string(),
					db: db_name.to_string(),
				}
				.into());
			};
			txn.get_db_user(db_def.namespace_id, db_def.database_id, &user).await?.ok_or_else(
				|| Error::UserDbNotFound {
					name: user,
					ns: ns_name.to_string(),
					db: db_name.to_string(),
				},
			)?
		}
	};

	// Return structured or SQL format
	Ok(if structured {
		res.as_ref().clone().structure()
	} else {
		Value::from(res.as_ref().to_sql())
	})
}
