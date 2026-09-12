//! Plan-time parameter resolution.
//!
//! Resolves bind parameters and `DEFINE PARAM` values to literals so that
//! constant folding and index analysis can see through them. Row-scoped
//! parameters (`$this`, `$self`, `$parent`) are intentionally NOT resolved
//! here — they're bound per-row at execution time. The
//! `super::row_scope::ROW_SCOPED_PARAMS` constant is the source of truth
//! and is re-exported as `SELECT_ITERATION_PARAMS`.
//!
//! Also includes `resolve_projection_field_idioms`, which rewrites
//! projection function calls (`type::field("name")`) into the path expression
//! the argument names so the index analyzer can match them.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::ops::ControlFlow;
use std::sync::Arc;

use super::literals::try_value_to_logical_expr;
use crate::exec::function::FunctionRegistry;
use crate::expr::Function;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, lower_expr};

/// Parameters injected per-row during SELECT document iteration.
/// These change value for every row and cannot be resolved at plan time.
///
/// - `this`/`self`: the current document being iterated
/// - `parent`: the outer document in correlated subqueries
///
/// Event/live/field params (`$before`, `$after`, `$value`, `$input`, `$event`)
/// are intentionally excluded because the exec planner only handles
/// top-level statements.  SELECTs inside event handlers, live query
/// notifications, and field evaluators run through the legacy `compute()`
/// path, where those params are resolved at runtime via `ctx.value()`.
/// If the exec planner is ever extended to those contexts, this guard
/// set would need to be expanded accordingly.
pub(crate) const SELECT_ITERATION_PARAMS: &[&str] = super::super::row_scope::ROW_SCOPED_PARAMS;

/// What a parameter is at plan time; see [`bind_param`].
#[derive(Debug)]
pub(crate) enum ParamBinding {
	/// A value fixed for the statement: a context value — a `LET` binding, a
	/// client bind parameter, a session value — or a `DEFINE PARAM` readable
	/// with `Permission::Full`.
	Value(crate::val::Value),
	/// Resolved when the statement runs: a row-scoped name, a `DEFINE PARAM`
	/// whose `PERMISSIONS` clause is judged per read, or a name the catalog
	/// cannot be asked about, for want of a transaction or a database.
	Runtime,
	/// Bound nowhere the statement can reach — not in the context, and not a
	/// `DEFINE PARAM` of the database — so it evaluates to `NONE`.
	Unbound,
}

/// Bind a parameter at plan time.
///
/// The order is the runtime's (`physical_expr::Param`): the row-scoped names
/// — `$this`, `$self`, `$parent` — are bound per row and never here, even
/// when the context holds a value under the name; then the context's value
/// chain, which holds `LET` bindings, client bind parameters and the session
/// values; then the database's `DEFINE PARAM`s. A `DEFINE PARAM` is a value
/// only when readable with `Permission::Full`; one with a `NONE` or `WHERE`
/// permission is judged per read at runtime. A name found nowhere is
/// [`ParamBinding::Unbound`] only when the catalog was consulted: without a
/// transaction and a database the name stays a runtime lookup. `txn` is the
/// planner's transaction; the context's own is consulted when there is none.
pub(crate) async fn bind_param(
	name: &str,
	ctx: &crate::ctx::FrozenContext,
	txn: Option<&Arc<crate::kvs::Transaction>>,
	ns_db: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	row_scoped: &[&str],
) -> ParamBinding {
	use crate::catalog::providers::DatabaseProvider;

	if row_scoped.contains(&name) {
		return ParamBinding::Runtime;
	}
	if let Some(value) = ctx.value(name) {
		return ParamBinding::Value(value.clone());
	}
	let (Some((ns, db)), Some(txn)) = (ns_db, txn.or_else(|| ctx.try_tx())) else {
		return ParamBinding::Runtime;
	};
	match txn.get_db_param(ns, db, name, None).await {
		Ok(param_def) if matches!(param_def.permissions, crate::catalog::Permission::Full) => {
			ParamBinding::Value(param_def.value.clone())
		}
		Ok(_) => ParamBinding::Runtime,
		Err(e) if matches!(e.downcast_ref(), Some(crate::catalog::Error::PaNotFound { .. })) => {
			ParamBinding::Unbound
		}
		Err(_) => ParamBinding::Runtime,
	}
}

/// The value a parameter has at plan time, when it has one; see
/// [`bind_param`]. An unbound name answers `None` here, as a runtime one does.
pub(crate) async fn resolve_param_value(
	name: &str,
	ctx: &crate::ctx::FrozenContext,
	txn: Option<&Arc<crate::kvs::Transaction>>,
	ns_db: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	row_scoped: &[&str],
) -> Option<crate::val::Value> {
	match bind_param(name, ctx, txn, ns_db, row_scoped).await {
		ParamBinding::Value(value) => Some(value),
		ParamBinding::Runtime | ParamBinding::Unbound => None,
	}
}

/// Resolve bind-parameter references in a `WHERE` condition to their literal
/// values. Returns a new expression with [`LogicalExpr::Param`] nodes replaced
/// by their literal form wherever the value is available.
///
/// Delegates to [`resolve_param_value`] per parameter. Row-scoped names
/// (e.g. [`SELECT_ITERATION_PARAMS`]) are never resolved; all other params
/// are looked up first in the context, then as `DEFINE PARAM` values.
///
/// Parameters that cannot be resolved, or whose literal form would evaluate
/// to a different value (see [`super::literals::substitution_is_lossy`]), are
/// left as-is: runtime parameter resolution yields the correct value, whereas
/// a lossy substitution changes results, not just the plan. A
/// [`LogicalExpr::Subquery`] is not entered; the plan inside it is bound on
/// its own.
pub(crate) async fn resolve_condition_params(
	cond: &LogicalExpr,
	ctx: &crate::ctx::FrozenContext,
	txn: Option<&Arc<crate::kvs::Transaction>>,
	ns_db: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	row_scoped: &[&str],
) -> LogicalExpr {
	// Pass 1: collect all param names referenced in the condition.
	let mut names: HashSet<String> = HashSet::new();
	let _ = cond.visit(&mut |e| {
		if let LogicalExpr::Param(param) = e {
			names.insert(param.as_str().to_string());
		}
		ControlFlow::<()>::Continue(())
	});
	if names.is_empty() {
		return cond.clone();
	}

	// Pass 2: resolve each parameter via the shared resolution path.
	let mut resolved: HashMap<String, LogicalExpr> = HashMap::with_capacity(names.len());
	for name in names {
		if let Some(value) = resolve_param_value(&name, ctx, txn, ns_db, row_scoped).await
			&& let Some(literal) = try_value_to_logical_expr(value)
		{
			resolved.insert(name, literal);
		}
	}

	if resolved.is_empty() {
		return cond.clone();
	}

	// Pass 3: apply substitutions.
	let mut expr = cond.clone();
	let Ok(()) = expr.visit_mut(&mut |e| {
		if let LogicalExpr::Param(param) = e
			&& let Some(literal) = resolved.get(param.as_str())
		{
			*e = literal.clone();
		}
		Ok::<_, Infallible>(())
	});
	expr
}

/// Rewrite projection function calls with a single string literal argument
/// (e.g. `type::field("name")`) to the path expression the argument names so
/// the index analyzer can match them against indexed columns.
///
/// Projection functions like `type::field(s)` and a bare path `s` both
/// reference the same document field, but the index analyzer only recognises
/// a path. This pass bridges the gap after param resolution and constant
/// folding have reduced the argument to a string literal.
///
/// Uses `FunctionRegistry::is_projection` so any current or future
/// projection function is handled without hardcoding names. The argument is
/// parsed as a surface idiom and lowered, so it takes the same path every
/// written idiom takes.
pub(crate) fn resolve_projection_field_idioms(expr: &mut LogicalExpr, registry: &FunctionRegistry) {
	let Ok(()) = expr.visit_mut(&mut |e| {
		if let LogicalExpr::Call(call) = e
			&& let Function::Normal(name) = &call.receiver
			&& registry.is_projection(name)
			&& let [LogicalExpr::Literal(LogicalLiteral::String(s))] = call.args.as_slice()
			&& let Ok(idiom) = crate::syn::idiom(s.as_str())
			&& let Ok(field) = lower_expr(crate::sql::Expr::Idiom(idiom))
			&& field.is_path()
		{
			*e = field;
		}
		Ok::<_, Infallible>(())
	});
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use super::*;

	pub(super) fn expr(sql: &str) -> LogicalExpr {
		let ast = crate::syn::parse(&format!("RETURN {sql}")).expect("expression should parse");
		let crate::sql::TopLevelExpr::Expr(crate::sql::Expr::Return(out)) =
			ast.expressions.into_iter().next().expect("one statement")
		else {
			panic!("expected a RETURN");
		};
		lower_expr(out.what).expect("expression should lower")
	}

	#[test]
	fn projection_functions_become_fields() {
		let registry = FunctionRegistry::with_builtins();
		let mut e = expr("type::field('a.b') = 1 AND type::field($p) = 2");
		resolve_projection_field_idioms(&mut e, &registry);
		// The literal argument is rewritten; the parameter one is left for
		// evaluation.
		assert_eq!(e.to_sql(), "a.b = 1 AND type::field($p) = 2");
	}

	#[cfg(feature = "kv-mem")]
	#[tokio::test]
	async fn statement_params_resolve_and_row_params_do_not() {
		use std::sync::Arc;

		use crate::kvs::Datastore;
		use crate::val::Value;

		let ds = Datastore::new("memory").await.expect("datastore");
		let mut ctx = ds.setup_ctx().expect("context");
		ctx.add_value("p", Arc::new(Value::Bool(true)));
		ctx.add_value("this", Arc::new(Value::Bool(false)));
		let ctx = ctx.freeze();

		let resolved = resolve_condition_params(
			&expr("$p AND a = $q AND $this.b"),
			&ctx,
			None,
			None,
			SELECT_ITERATION_PARAMS,
		)
		.await;
		assert_eq!(resolved.to_sql(), "true AND a = $q AND $this.b");
	}
}
