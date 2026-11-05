//! Plan-time parameter resolution.
//!
//! Resolves bind parameters and `DEFINE PARAM` values to literals so that
//! constant folding and index analysis can see through them. Row-scoped
//! parameters (`$this`, `$self`, `$parent`) are never resolved here. They are
//! bound per row at execution time. `super::row_scope::ROW_SCOPED_PARAMS` is the
//! source of truth and is re-exported as `SELECT_ITERATION_PARAMS`.
//!
//! Also includes `resolve_projection_field_idioms`, which rewrites projection
//! function calls (`type::field("name")`) into the path expression the argument
//! names, so the index analyzer can match them.

use std::convert::Infallible;
use std::sync::Arc;

use crate::exec::function::FunctionRegistry;
use crate::expr::Function;
use crate::expr::logical::{LogicalExpr, LogicalLiteral, lower_expr};

/// Parameters injected per-row during SELECT document iteration.
/// These change value for every row, so they cannot be resolved at plan time.
///
/// - `this`/`self`: the current document being iterated
/// - `parent`: the outer document in correlated subqueries
///
/// Event, live and field params (`$before`, `$after`, `$value`, `$input`,
/// `$event`) are excluded because the exec planner only handles top-level
/// statements. Those params are bound by the frame an event handler, live
/// query notification or field evaluator runs in, and are read at runtime
/// via `ctx.value()`. A plan compiled in such a frame must expand this
/// guard set.
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
/// Lookup order matches the runtime. Row-scoped names (`$this`, `$self`,
/// `$parent`) are always bound per row and never here, even when the context
/// holds a value under the name. Next comes the context's value chain, which
/// holds `LET` bindings, client bind parameters and session values. Last comes
/// the database's `DEFINE PARAM`s. A `DEFINE PARAM` is a value only when it is
/// readable with `Permission::Full`. One with a `NONE` or `WHERE` permission is
/// checked per read at runtime. A name found nowhere is
/// [`ParamBinding::Unbound`] only when the catalog was consulted. Without a
/// transaction and a database, the name stays a runtime lookup. `txn` is the
/// planner's transaction. When there is none, the context's own is used.
///
/// A function body is planned in the scope a body reads
/// ([`crate::ctx::Context::new_call_scope`]). A plan made while a body runs
/// reads its context the same way ([`crate::ctx::Context::lexical_value`]).
/// So the context holds, for a name, only a request parameter, a session
/// value, a row name, an argument or a binding the body made. A `LET` or `FOR`
/// binding of the body's caller is never visible. A name only the caller binds
/// resolves to a `DEFINE PARAM` of the database or to unbound, as it does when
/// the body runs (`vm::function`).
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
	if let Some(value) = ctx.lexical_value(name) {
		return ParamBinding::Value(value.as_ref().clone());
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

/// Rewrite projection function calls with one string literal argument, such
/// as `type::field("name")`, to the path expression the argument names. The
/// index analyzer can then match them against indexed columns.
///
/// `type::field(s)` and a bare path `s` reference the same document field,
/// but the index analyzer only recognises a path. This pass runs after
/// parameter resolution and constant folding have reduced the argument to a
/// string literal.
///
/// Projection functions are found through `FunctionRegistry::is_projection`,
/// so new ones are handled without a name list. The argument is parsed as a
/// surface idiom and lowered, so it takes the same path as any written idiom.
pub(crate) fn resolve_projection_field_idioms(expr: &mut LogicalExpr, registry: &FunctionRegistry) {
	let Ok(()) = expr.visit_mut(&mut |e| {
		if let LogicalExpr::Call(call) = e
			&& let Function::Normal(name) = &call.receiver
			&& registry.is_projection(name)
			&& let [LogicalExpr::Literal(LogicalLiteral::String(s))] = call.args.as_slice()
			&& let Ok(idiom) = crate::syn::idiom(s.as_str())
			&& let Ok(field) = lower_expr(crate::expr::Expr::Idiom(idiom))
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
		let surrealdb_expr::parser::TopLevelExpr::Expr(crate::expr::Expr::Return(out)) =
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
}
