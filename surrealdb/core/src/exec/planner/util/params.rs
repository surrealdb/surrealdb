//! Plan-time parameter resolution.
//!
//! Resolves bind parameters and `DEFINE PARAM` values to literals so that
//! constant folding and index analysis can see through them. Row-scoped
//! parameters (`$this`, `$self`, `$parent`) are intentionally NOT resolved
//! here — they're bound per-row at execution time. The
//! `super::row_scope::ROW_SCOPED_PARAMS` constant is the source of truth
//! and is re-exported as `SELECT_ITERATION_PARAMS` for backward compat.
//!
//! Also includes `resolve_projection_field_idioms`, which rewrites
//! projection function calls (`type::field("name")`) into
//! `Expr::Idiom(...)` so the index analyzer can match them.

use crate::exec::function::FunctionRegistry;
use crate::expr::visit::{MutVisitor, Visit, VisitMut, Visitor};
use crate::expr::{Cond, Expr, Literal};

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

/// Resolve a single parameter to its value at plan time.
///
/// Resolution order:
/// 1. Row-scoped guard (skip params whose values change per-row)
/// 2. Context values (LET bindings, client bind parameters, session params)
/// 3. DEFINE PARAM values with `Permission::Full` from the transaction store
///
/// Row-scoped params (`$this`, `$self`, `$parent`) are bound per-row at
/// execution time (e.g. `$parent` by `ScalarSubquery`, `$this`/`$self` by
/// per-row evaluation). They must never be folded at plan time, even when
/// a `LET` binding shadows the name, because the runtime binding takes
/// precedence.
///
/// DEFINE PARAMs with `Permission::None` or `Permission::Specific` are left
/// for runtime resolution where the full permission machinery is available.
pub(crate) async fn resolve_param_value(
	name: &str,
	ctx: &crate::ctx::FrozenContext,
	ns_db: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	row_scoped: &[&str],
) -> Option<crate::val::Value> {
	use crate::catalog::providers::DatabaseProvider;

	if row_scoped.contains(&name) {
		return None;
	}
	if let Some(value) = ctx.value(name) {
		return Some(value.clone());
	}
	if let Some((ns, db)) = ns_db
		&& let Some(txn) = ctx.try_tx()
		&& let Ok(param_def) = txn.get_db_param(ns, db, name, None).await
		&& matches!(param_def.permissions, crate::catalog::Permission::Full)
	{
		return Some(param_def.value.clone());
	}
	None
}

/// Resolve bind-parameter references in a `WHERE` condition to their literal
/// values. Returns a new `Cond` with `Expr::Param` nodes replaced by
/// `Expr::Literal` wherever the value is available.
///
/// Delegates to [`resolve_param_value`] per parameter. Row-scoped names
/// (e.g. [`SELECT_ITERATION_PARAMS`]) are never resolved; all other params
/// are looked up first in the context, then as `DEFINE PARAM` values.
///
/// Parameters that cannot be resolved are left as-is.
pub(crate) async fn resolve_condition_params(
	cond: &Cond,
	ctx: &crate::ctx::FrozenContext,
	ns_db: Option<(crate::catalog::NamespaceId, crate::catalog::DatabaseId)>,
	row_scoped: &[&str],
) -> Cond {
	// Pass 1: collect all param names referenced in the condition.
	let mut collector = ParamCollector {
		names: std::collections::HashSet::new(),
	};
	let _ = collector.visit_expr(&cond.0);
	if collector.names.is_empty() {
		return cond.clone();
	}

	// Pass 2: resolve each parameter via the shared resolution path.
	let mut resolved = std::collections::HashMap::with_capacity(collector.names.len());
	for name in &collector.names {
		if let Some(value) = resolve_param_value(name, ctx, ns_db, row_scoped).await {
			resolved.insert(name.clone(), value);
		}
	}

	if resolved.is_empty() {
		return cond.clone();
	}

	// Pass 3: apply substitutions.
	let mut expr = cond.0.clone();
	let _ = ParamResolver {
		values: &resolved,
	}
	.visit_mut_expr(&mut expr);
	Cond(expr)
}

/// Rewrite projection function calls with a single string literal argument
/// (e.g. `type::field("name")`) to `Expr::Idiom` so the index analyzer can
/// match them against indexed columns.
///
/// Projection functions like `type::field(s)` and a bare `Idiom(s)` both
/// reference the same document field, but the index analyzer only recognises
/// `Expr::Idiom`. This pass bridges the gap after param resolution and
/// constant folding have reduced the argument to a string literal.
///
/// Uses `FunctionRegistry::is_projection` so any current or future
/// projection function is handled without hardcoding names.
pub(crate) fn resolve_projection_field_idioms(cond: &mut Cond, registry: &FunctionRegistry) {
	let mut resolver = ProjectionFieldResolver {
		registry,
	};
	let _ = resolver.visit_mut_expr(&mut cond.0);
}

/// Collects all `Expr::Param` names referenced in a condition, skipping
/// subqueries. Used as the first pass before async resolution.
struct ParamCollector {
	names: std::collections::HashSet<String>,
}

impl Visitor for ParamCollector {
	type Error = std::convert::Infallible;

	fn visit_expr(&mut self, expr: &Expr) -> Result<(), Self::Error> {
		if let Expr::Param(param) = expr {
			self.names.insert(param.as_str().to_string());
		}
		expr.visit(self)
	}

	fn visit_select(&mut self, _: &crate::expr::SelectStatement) -> Result<(), Self::Error> {
		Ok(())
	}
}

/// Replaces `Expr::Param` nodes with `Expr::Literal` using a pre-built value
/// map. Applied after async resolution has populated the map.
struct ParamResolver<'a> {
	values: &'a std::collections::HashMap<String, crate::val::Value>,
}

impl MutVisitor for ParamResolver<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		if let Expr::Param(param) = expr
			&& let Some(value) = self.values.get(param.as_str())
		{
			*expr = value.clone().into_literal();
			return Ok(());
		}
		expr.visit_mut(self)
	}

	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}

struct ProjectionFieldResolver<'a> {
	registry: &'a FunctionRegistry,
}

impl MutVisitor for ProjectionFieldResolver<'_> {
	type Error = std::convert::Infallible;

	fn visit_mut_expr(&mut self, expr: &mut Expr) -> Result<(), Self::Error> {
		use crate::expr::function::Function;

		expr.visit_mut(self)?;

		if let Expr::FunctionCall(fc) = expr
			&& let Function::Normal(name) = &fc.receiver
			&& self.registry.is_projection(name)
			&& fc.arguments.len() == 1
			&& let Expr::Literal(Literal::String(s)) = &fc.arguments[0]
			&& let Ok(idiom) = crate::syn::idiom(s)
		{
			*expr = Expr::Idiom(idiom.into());
		}
		Ok(())
	}

	fn visit_mut_select(
		&mut self,
		_: &mut crate::expr::SelectStatement,
	) -> Result<(), Self::Error> {
		Ok(())
	}
}
