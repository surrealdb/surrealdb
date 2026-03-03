use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{DatabaseId, NamespaceId, Permission};
use crate::err::Error;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::FlowResult;
use crate::expr::mock::Mock;
use crate::iam::Action;
use crate::kvs::Transaction;
use crate::val::{Array, Value};

/// Literal value - "foo", 42, true
#[derive(Debug, Clone)]
pub struct Literal(pub(crate) Value);

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for Literal {
	fn name(&self) -> &'static str {
		"Literal"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Literals are constant values, no context needed
		crate::exec::ContextLevel::Root
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> FlowResult<Value> {
		Ok(self.0.clone())
	}

	fn access_mode(&self) -> AccessMode {
		// Literals are always read-only
		AccessMode::ReadOnly
	}

	fn try_literal(&self) -> Option<&Value> {
		Some(&self.0)
	}
}

impl ToSql for Literal {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}

/// Parameter reference - $foo
#[derive(Debug, Clone)]
pub struct Param(pub(crate) String);

impl Param {
	/// Fetch a parameter from the database and check permissions.
	async fn fetch_db_param(
		&self,
		ctx: EvalContext<'_>,
		txn: &Arc<Transaction>,
		ns_id: NamespaceId,
		db_id: DatabaseId,
	) -> anyhow::Result<Value> {
		match txn.get_db_param(ns_id, db_id, &self.0).await {
			Ok(param_def) => {
				// Check permissions
				if ctx.exec_ctx.should_check_perms(Action::View)? {
					match &param_def.permissions {
						Permission::Full => {}
						Permission::None => {
							bail!(Error::ParamPermissions {
								name: self.0.clone()
							})
						}
						Permission::Specific(perm_expr) => {
							// Plan and evaluate the permission expression
							match crate::exec::planner::expr_to_physical_expr(
								perm_expr.clone(),
								ctx.exec_ctx.ctx(),
							)
							.await
							{
								Ok(phys_expr) => {
									match phys_expr.evaluate(ctx.clone()).await {
										Ok(result) if result.is_truthy() => {
											// Permission granted
										}
										Ok(_) => {
											bail!(Error::ParamPermissions {
												name: self.0.clone()
											})
										}
										Err(crate::expr::ControlFlow::Err(e)) => {
											return Err(e);
										}
										Err(_) => {
											bail!(Error::ParamPermissions {
												name: self.0.clone()
											})
										}
									}
								}
								Err(_) => {
									// If we can't plan the expression, deny by default
									bail!(Error::ParamPermissions {
										name: self.0.clone()
									})
								}
							}
						}
					}
				}
				Ok(param_def.value.clone())
			}
			Err(e) => {
				if matches!(e.downcast_ref(), Some(Error::PaNotFound { .. })) {
					Ok(Value::None)
				} else {
					Err(e)
				}
			}
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for Param {
	fn name(&self) -> &'static str {
		"Param"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Parameters can be local, session-level, or database-defined.
		// Local and session params work at Root level; database params
		// will fail at runtime if database context is unavailable.
		// We report Root to allow simple parameter usage without DB.
		crate::exec::ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		// Handle special params $this and $self.
		// When current_value is available (per-row context), use it directly.
		// When it's None (scalar context, e.g. a subquery's FROM clause),
		// check if $this was explicitly bound as a parameter (by ScalarSubquery)
		// before defaulting to NONE. This avoids database lookups for $this.
		match self.0.as_str() {
			"this" | "self" => {
				if let Some(v) = ctx.current_value {
					return Ok(v.clone());
				}
				// Check if $this was explicitly bound as a parameter (e.g. by subquery)
				if let Some(local_params) = ctx.local_params
					&& let Some(v) = local_params.get(&self.0)
				{
					return Ok(v.clone());
				}
				if let Some(v) = ctx.exec_ctx.value(&self.0) {
					return Ok(v.clone());
				}
				return Ok(Value::None);
			}
			_ => {}
		}

		// Check block-local parameters (they shadow global params)
		if let Some(local_params) = ctx.local_params
			&& let Some(value) = local_params.get(&self.0)
		{
			return Ok(value.clone());
		}

		// FrozenContext handles scoped parameter lookup via parent-chain,
		// including protected params ($auth, $access, $token, $session)
		if let Some(v) = ctx.exec_ctx.value(&self.0) {
			return Ok(v.clone());
		}

		// Try to fetch from database
		// First check if we have database context directly
		if let Ok(db_ctx) = ctx.exec_ctx.database() {
			let txn = ctx.exec_ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;

			return Ok(self.fetch_db_param(ctx, &txn, ns_id, db_id).await?);
		}

		// If no database context but we have options with ns/db set, look up by name
		if let Some(opts) = ctx.exec_ctx.options() {
			// Check if namespace/database are set - if not, throw appropriate error
			let ns_name = match opts.ns() {
				Ok(ns) => ns,
				Err(_) => return Err(Error::NsEmpty.into()),
			};
			let db_name = match opts.db() {
				Ok(db) => db,
				Err(_) => return Err(Error::DbEmpty.into()),
			};

			let txn = ctx.exec_ctx.txn();
			// Look up database definition by name to get the IDs
			if let Ok(Some(db_def)) = txn.get_db_by_name(ns_name, db_name).await {
				let ns_id = db_def.namespace_id;
				let db_id = db_def.database_id;

				return Ok(self.fetch_db_param(ctx, &txn, ns_id, db_id).await?);
			}
			// Database doesn't exist yet, param cannot be found
			return Ok(Value::None);
		}

		// No options available and param not found locally - throw error
		Err(anyhow::anyhow!("Parameter not found: ${}", self.0).into())
	}

	fn access_mode(&self) -> AccessMode {
		// Parameter references are read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Param {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "${}", self.0)
	}
}

/// Mock expression - |table:count| or |table:range|
///
/// Generates an array of RecordIds for testing purposes. Equivalent to
/// the old executor's `Expr::Mock` compute path.
#[derive(Debug, Clone)]
pub struct MockExpr(pub(crate) Mock);

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl PhysicalExpr for MockExpr {
	fn name(&self) -> &'static str {
		"Mock"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Mock expressions produce constant test data, no context needed
		crate::exec::ContextLevel::Root
	}

	async fn evaluate(&self, ctx: EvalContext<'_>) -> FlowResult<Value> {
		let limit = ctx.exec_ctx.ctx().config().limits.generation_allocation_limit;
		let iter = self.0.clone().into_iter();
		if iter
			.size_hint()
			.1
			.map(|x| x.saturating_mul(std::mem::size_of::<Value>()) > limit)
			.unwrap_or(true)
		{
			return Err(anyhow::Error::msg("Mock range exceeds allocation limit").into());
		}
		let record_ids = iter.map(Value::RecordId).collect();
		Ok(Value::Array(Array(record_ids)))
	}

	fn access_mode(&self) -> AccessMode {
		// Mock expressions are read-only constant generators
		AccessMode::ReadOnly
	}
}

impl ToSql for MockExpr {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt);
	}
}
