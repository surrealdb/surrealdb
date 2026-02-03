use anyhow::bail;
use async_trait::async_trait;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::Permission;
use crate::catalog::providers::DatabaseProvider;
use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::err::Error;
use crate::exec::AccessMode;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::iam::Action;
use crate::val::Value;

/// Literal value - "foo", 42, true
#[derive(Debug, Clone)]
pub struct Literal(pub(crate) Value);

#[async_trait]
impl PhysicalExpr for Literal {
	fn name(&self) -> &'static str {
		"Literal"
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Literals are constant values, no context needed
		crate::exec::ContextLevel::Root
	}

	async fn evaluate(&self, _ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		Ok(self.0.clone())
	}

	fn references_current_value(&self) -> bool {
		false
	}

	fn access_mode(&self) -> AccessMode {
		// Literals are always read-only
		AccessMode::ReadOnly
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

use std::sync::Arc;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::kvs::Transaction;

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
						Permission::Specific(_) => {
							// TODO: Evaluate permission expression
							// For now, allow access (matches Permission::Full behavior)
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

#[async_trait]
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

	async fn evaluate(&self, ctx: EvalContext<'_>) -> anyhow::Result<Value> {
		// Handle special params $this and $self
		match self.0.as_str() {
			"this" | "self" => {
				return Ok(ctx.current_value.cloned().unwrap_or(Value::None));
			}
			_ => {}
		}

		// Check protected/session parameters ($auth, $access, $token, $session)
		// These are stored in the FrozenContext and are not user-modifiable
		if PROTECTED_PARAM_NAMES.contains(&self.0.as_str()) {
			if let Some(value) = ctx.exec_ctx.ctx().value(&self.0) {
				return Ok(value.clone());
			}
			return Ok(Value::None);
		}

		// Check block-local parameters (they shadow global params)
		if let Some(local_params) = ctx.local_params {
			if let Some(value) = local_params.get(&self.0) {
				return Ok(value.clone());
			}
		}

		// Check execution context parameters
		if let Some(v) = ctx.exec_ctx.params().get(self.0.as_str()) {
			return Ok((**v).clone());
		}

		// Try to fetch from database
		// First check if we have database context directly
		if let Ok(db_ctx) = ctx.exec_ctx.database() {
			let txn = ctx.exec_ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;

			return self.fetch_db_param(ctx, txn, ns_id, db_id).await;
		}

		// If no database context but we have options with ns/db set, look up by name
		if let Some(opts) = ctx.exec_ctx.options() {
			// Check if namespace/database are set - if not, throw appropriate error
			let ns_name = match opts.ns() {
				Ok(ns) => ns,
				Err(_) => bail!(Error::NsEmpty),
			};
			let db_name = match opts.db() {
				Ok(db) => db,
				Err(_) => bail!(Error::DbEmpty),
			};

			let txn = ctx.exec_ctx.txn();
			// Look up database definition by name to get the IDs
			if let Ok(Some(db_def)) = txn.get_db_by_name(ns_name, db_name).await {
				let ns_id = db_def.namespace_id;
				let db_id = db_def.database_id;

				return self.fetch_db_param(ctx, txn, ns_id, db_id).await;
			}
			// Database doesn't exist yet, param cannot be found
			return Ok(Value::None);
		}

		// No options available and param not found locally - throw error
		bail!("Parameter not found: ${}", self.0)
	}

	fn references_current_value(&self) -> bool {
		false
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
