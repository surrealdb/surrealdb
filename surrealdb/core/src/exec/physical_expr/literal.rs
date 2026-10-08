use std::sync::Arc;

use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseId, Error as CatalogError, NamespaceId, ParamDefinition};
use crate::exec::permission::CompiledGuard;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr, PlannedGuard};
use crate::exec::{AccessMode, BoxFut, Error as ExecError};
use crate::expr::FlowResult;
use crate::expr::mock::Mock;
use crate::iam::Action;
use crate::kvs::Transaction;
use crate::val::{Array, Value};

/// Literal value - "foo", 42, true
#[derive(Debug, Clone)]
pub struct Literal(pub(crate) Value);
impl PhysicalExpr for Literal {
	fn name(&self) -> &'static str {
		"Literal"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Literals are constant values, no context needed
		crate::exec::ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, _ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move { Ok(self.0.clone()) })
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
///
/// Resolution order: block-local parameters, the context's parameter chain
/// (including the protected `$auth`, `$access`, `$token`, `$session`), then
/// the `$parent` fallback below, then `DEFINE PARAM`. The row is never
/// consulted: a per-row value reaches an expression as a field of the row it
/// evaluates against, not as a parameter.
#[derive(Debug)]
pub struct Param {
	pub(crate) name: Strand,
	/// The conditional `PERMISSIONS` clause of the `DEFINE PARAM` this read
	/// last found, compiled. A parameter in a projection is read once per
	/// row, and compiling its guard is plan-time work: without this it is
	/// done again for every row. See [`CompiledGuard`] for what is kept and
	/// what is not.
	guard: CompiledGuard<ParamDefinition>,
	/// The `PERMISSIONS` clause compiled with the plan, for the definition
	/// the planner read; see [`PlannedGuard`].
	planned: PlannedGuard<ParamDefinition>,
}

impl Param {
	/// A read of `$name` whose `DEFINE PARAM` predicate the plan compiled as
	/// `planned`.
	pub(crate) fn with_planned_guard(name: Strand, planned: PlannedGuard<ParamDefinition>) -> Self {
		Self {
			name,
			guard: CompiledGuard::default(),
			planned,
		}
	}

	/// The parameter's value in `ctx`, resolved in the order the type's
	/// documentation gives.
	pub(crate) fn read<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			// Handle special params $this and $self.
			// When current_value is available (per-row context), use it directly.
			// When it's None (scalar context, e.g. a subquery's FROM clause),
			// check if $this was explicitly bound as a parameter (by ScalarSubquery)
			// before defaulting to NONE. This avoids database lookups for $this.
			match self.name.as_str() {
				"this" | "self" => {
					if let Some(v) = ctx.current_value {
						return Ok(v.clone());
					}
					// Check if $this was explicitly bound as a parameter (e.g. by subquery)
					if let Some(local_params) = ctx.local_params
						&& let Some(v) = local_params.get(self.name.as_str())
					{
						return Ok(v.clone());
					}
					if let Some(v) = ctx.exec_ctx.value(self.name.as_str()) {
						return Ok(v.clone());
					}
					return Ok(Value::None);
				}
				_ => {}
			}

			// Check block-local parameters (they shadow global params)
			if let Some(local_params) = ctx.local_params
				&& let Some(value) = local_params.get(self.name.as_str())
			{
				return Ok(value.clone());
			}

			// A write-path binding ($before, $after, $value, $input, $event,
			// $reference) sits between the two: a `LET $value = ...` in the
			// body being evaluated is an inner scope and wins, while the
			// binding shadows a session or database parameter of the same
			// name.
			if let Some(value) = ctx.write_binding(self.name.as_str()) {
				return Ok(value.clone());
			}

			// FrozenContext handles scoped parameter lookup via parent-chain,
			// including protected params ($auth, $access, $token, $session)
			if let Some(v) = ctx.exec_ctx.value(self.name.as_str()) {
				return Ok(v.clone());
			}

			// $parent falls back to document_root when not explicitly bound.
			// This allows `->edge[WHERE $parent.field]` and similar idiom-level
			// WHERE clauses to reference the enclosing row being projected/filtered.
			// Code a write reaches has no such fallback: an unbound `$parent`
			// there is `NONE`.
			if self.name.as_str() == "parent" {
				if ctx.exec_ctx.root().writing {
					return Ok(Value::None);
				}
				if let Some(v) = ctx.document_root {
					return Ok(v.clone());
				}
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
					Err(_) => return Err(ExecError::NsEmpty.into()),
				};
				let db_name = match opts.db() {
					Ok(db) => db,
					Err(_) => return Err(ExecError::DbEmpty.into()),
				};

				let txn = ctx.exec_ctx.txn();
				// Look up the database definition by name to get the IDs
				if let Ok(Some(db_def)) =
					txn.get_db_by_name(ns_name, db_name, ctx.exec_ctx.version_stamp()).await
				{
					let ns_id = db_def.namespace_id;
					let db_id = db_def.database_id;

					// A database-defined param's PERMISSIONS predicate may dereference
					// records (e.g. `$auth.admin`), whose fetch needs a database-level
					// context. Root/namespace-level execution carries none, so evaluate
					// the param under a database context built from its own ns/db, as a
					// function call already does. `with_database` clones the current
					// root, preserving auth, session, options, and the permission-
					// predicate isolation flags.
					if let Ok(Some(ns_def)) =
						txn.get_ns_by_name(ns_name, ctx.exec_ctx.version_stamp()).await
					{
						let db_level = ctx.exec_ctx.with_database(ns_def, db_def);
						let db_ctx = EvalContext {
							exec_ctx: &db_level,
							..ctx.clone()
						};
						return Ok(self.fetch_db_param(db_ctx, &txn, ns_id, db_id).await?);
					}

					return Ok(self.fetch_db_param(ctx, &txn, ns_id, db_id).await?);
				}
				// Database doesn't exist yet, param cannot be found
				return Ok(Value::None);
			}

			// No options available and param not found locally - throw error
			Err(anyhow::anyhow!("Parameter not found: ${}", self.name).into())
		})
	}

	/// Fetch a `DEFINE PARAM` value and check its `PERMISSIONS` clause.
	///
	/// A conditional permission is judged on every read — the clause reads the
	/// row the parameter is read on — through [`CompiledGuard`], which
	/// compiles it once for the definition this read found rather than once
	/// per read. The stored value is already a value; nothing about it is
	/// compiled.
	async fn fetch_db_param(
		&self,
		ctx: EvalContext<'_>,
		txn: &Arc<Transaction>,
		ns_id: NamespaceId,
		db_id: DatabaseId,
	) -> anyhow::Result<Value> {
		match txn.get_db_param(ns_id, db_id, self.name.as_str(), ctx.exec_ctx.version_stamp()).await
		{
			Ok(param_def) => {
				if !ctx.skip_fetch_perms && ctx.exec_ctx.should_check_perms(Action::View)? {
					let denied = || ExecError::ParamPermissions {
						name: self.name.to_string(),
					};
					let permission = param_def.permissions_body.logical_ref()?;
					let judged = match self.planned.matching(&param_def, permission, None) {
						Some(matched) => {
							crate::exec::guard_verdicts::judged(
								ctx.exec_ctx,
								matched.scope,
								crate::exec::guard_verdicts::GuardKind::Param,
								self.name.as_str(),
								matched.id,
								|| {
									crate::exec::permission::judge_compiled_guard(
										matched.compiled,
										&ctx,
										denied,
									)
								},
							)
							.await
						}
						None => {
							self.guard
								.check(permission, &param_def, &ctx, Some(Arc::clone(txn)), denied)
								.await
						}
					};
					judged.map_err(|flow| match flow {
						crate::expr::ControlFlow::Err(e) => e,
						// A guard raises nothing else: `check` turns every
						// other signal into a denial before it returns.
						_ => anyhow::Error::new(denied()),
					})?;
				}
				Ok(param_def.value.clone())
			}
			Err(e) => {
				if matches!(e.downcast_ref(), Some(CatalogError::PaNotFound { .. })) {
					Ok(Value::None)
				} else {
					Err(e)
				}
			}
		}
	}
}
impl PhysicalExpr for Param {
	fn name(&self) -> &'static str {
		"Param"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Parameters can be local, session-level, or database-defined.
		// Local and session params work at Root level; database params
		// will fail at runtime if database context is unavailable.
		// We report Root to allow simple parameter usage without DB.
		crate::exec::ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		self.read(ctx)
	}

	fn access_mode(&self) -> AccessMode {
		// Parameter references are read-only
		AccessMode::ReadOnly
	}
}

impl ToSql for Param {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use common::fmt::EscapeKwFreeIdent;
		write_sql!(f, fmt, "${}", EscapeKwFreeIdent(self.name.as_str()))
	}
}

/// Mock expression - |table:count| or |table:range|
///
/// Generates an array of RecordIds for testing purposes.
#[derive(Debug, Clone)]
pub struct MockExpr(pub(crate) Mock);
impl PhysicalExpr for MockExpr {
	fn name(&self) -> &'static str {
		"Mock"
	}

	fn as_any(&self) -> &dyn std::any::Any {
		self
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		// Mock expressions produce constant test data, no context needed
		crate::exec::ContextLevel::Root
	}

	fn evaluate<'a>(&'a self, ctx: EvalContext<'a>) -> BoxFut<'a, FlowResult<Value>> {
		Box::pin(async move {
			let iter = self.0.clone().into_iter();
			if iter
				.size_hint()
				.1
				.map(|x| {
					x.saturating_mul(std::mem::size_of::<Value>())
						> ctx.exec_ctx.root().ctx.config.exec.generation_allocation_limit
				})
				.unwrap_or(true)
			{
				return Err(anyhow::Error::msg("Mock range exceeds allocation limit").into());
			}
			let record_ids = iter.map(Value::RecordId).collect();
			Ok(Value::Array(Array(record_ids)))
		})
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

#[cfg(test)]
mod tests {
	use std::collections::HashMap;

	use surrealdb_strand::Strand;

	use crate::exec::operators::test_util::{physical_expr, root_ctx};
	use crate::exec::physical_expr::EvalContext;
	use crate::val::Value;

	/// `$value` is a write-path binding, but a `LET $value = ...` in the body
	/// being evaluated is an inner scope and has to win. Both are populated
	/// here, so nothing but the order the two are consulted in decides the
	/// answer.
	#[tokio::test]
	async fn a_block_local_let_shadows_the_write_value_binding() {
		let exec_ctx = root_ctx();
		let expr = physical_expr("$value", &exec_ctx).await;
		let bound = Value::from("the write binding");

		let params: HashMap<Strand, Value> =
			HashMap::from([(Strand::from("value"), Value::from(1))]);
		let mut shadowed = EvalContext::from_exec_ctx(&exec_ctx);
		shadowed.write_value = Some(&bound);
		shadowed.local_params = Some(&params);
		assert_eq!(
			expr.evaluate(shadowed).await.expect("evaluation should succeed"),
			Value::from(1),
			"a block-local LET must shadow the write binding of the same name"
		);

		// Without the LET, the same expression reaches the slot: the shadowing
		// above is the ordering at work, not an unread binding.
		let mut bare = EvalContext::from_exec_ctx(&exec_ctx);
		bare.write_value = Some(&bound);
		assert_eq!(
			expr.evaluate(bare).await.expect("evaluation should succeed"),
			bound,
			"with no LET in scope, $value must resolve to the write binding"
		);
	}
}
