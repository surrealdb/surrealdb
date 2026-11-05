//! The callee side of a call of a user-defined function bound in place.
//!
//! The expression binder binds a call whose definition may run in the caller as the call and, in
//! a `FunctionBody` region of it, the body with the declared parameters as the region's
//! arguments. [`inlinable_body`] answers whether a definition has a body that may stand in the
//! caller at all, from the definition alone:
//!
//! - the body is `{ RETURN e }` or `{ e }`, and `e` is read-only, reads nothing of the calling row
//!   (no field, document, current value, lookup, row parameter, nested plan, or call whose body
//!   could), names no parameter but its arguments (the body runs in a context where the caller's
//!   parameters are not bound), calls no user-defined, scripted, module, silo or model function,
//!   and holds no closure, recursion, `THROW`, `BREAK`, `CONTINUE`, `MATCHES` or `KNN`;
//! - the declared kind of each argument, which the call site is checked against, comes with the
//!   definition.
//!
//! What a call site must satisfy beyond that (argument purity and kinds, the caller's authority,
//! the catalog the statement may meet) is decided by the binder's host and the inlining pass.
#![deny(clippy::wildcard_enum_match_arm)]

use crate::catalog::FunctionDefinition;
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::reads::reads_row_opaquely;
use crate::expr::logical::{LogicalEval, LogicalExpr, LogicalOp, LogicalReturn, LogicalSequence};
use crate::expr::{BinaryOperator, Function, Kind};

/// The expression `definition`'s body yields, when the body is one that may stand in the caller.
///
/// SECURITY: a definition whose declared arguments include a protected name
/// (`$auth`, `$access`, `$token`, `$session`) is never inlined, even when its
/// body otherwise qualifies. Inlining replaces the call with the body, past the
/// invoke-time refusal in [`bind_args`](crate::exec::vm::function); declining
/// here leaves the call standing, so that refusal runs and the stored definition
/// (one persisted before the define-time refusal, or on an earlier version) is
/// refused whether its body would have been inlined or not (GHSA-ffvm-mqh3-7986).
pub(crate) fn inlinable_body<'d>(
	definition: &'d FunctionDefinition,
	registry: &FunctionRegistry,
) -> Option<&'d LogicalExpr> {
	if definition
		.args
		.iter()
		.any(|(name, _)| surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&name.as_str()))
	{
		return None;
	}
	// A named type in a signature is judged against the catalog where the call binds its arguments
	// and returns its value, which a body standing in the caller never does.
	if definition
		.args
		.iter()
		.map(|(_, kind)| kind)
		.chain(definition.returns.as_ref())
		.any(Kind::has_named_type)
	{
		return None;
	}
	let expr = return_expr(definition.logical_body().ok()?)?;
	body_is_inlinable(expr, &definition.args, registry).then_some(expr)
}

/// The expression a `{ RETURN e }` or `{ e }` body yields.
fn return_expr(body: &LogicalOp) -> Option<&LogicalExpr> {
	let LogicalOp::Sequence(LogicalSequence {
		stmts,
	}) = body
	else {
		return None;
	};
	match stmts.as_slice() {
		[
			LogicalOp::Return(LogicalReturn {
				what,
				fetch,
			}),
		] if fetch.is_empty() => {
			if let LogicalOp::Eval(LogicalEval {
				expr,
			}) = what.as_ref()
			{
				Some(expr)
			} else {
				None
			}
		}
		[
			LogicalOp::Eval(LogicalEval {
				expr,
			}),
		] => Some(expr),
		_ => None,
	}
}

/// Whether `expr` mentions an index operator, `MATCHES` or `KNN`, anywhere, a nested plan and a
/// closure body included.
pub(crate) fn mentions_index_operator(expr: &LogicalExpr) -> bool {
	expr.any(|e| {
		matches!(
			e,
			LogicalExpr::Binary {
				op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_),
				..
			}
		)
	})
}

/// Whether `expr`, a body's expression, is a function of `args` alone that may be placed at a call
/// site; see the module documentation.
fn body_is_inlinable(
	expr: &LogicalExpr,
	args: &[(String, Kind)],
	registry: &FunctionRegistry,
) -> bool {
	if !expr.read_only() || reads_row_opaquely(expr, registry) || mentions_index_operator(expr) {
		return false;
	}
	!expr.any(|e| match e {
		LogicalExpr::Param(param) => !args.iter().any(|(name, _)| name == param.as_str()),
		LogicalExpr::Call(call) => match &call.receiver {
			Function::Normal(_) => false,
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => true,
		},
		LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => true,
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Access {
			..
		}
		| LogicalExpr::Index {
			..
		}
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Each(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		}
		| LogicalExpr::Broadcast {
			..
		}
		| LogicalExpr::SetField {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::If(_) => false,
	})
}

#[cfg(test)]
mod tests {
	use surrealdb_expr::parser::TopLevelExpr;

	use super::*;
	use crate::expr::statements::DefineStatement;

	/// The definition `sql`, a `DEFINE FUNCTION`, states.
	fn definition(sql: &str) -> FunctionDefinition {
		let parsed = crate::syn::parse(sql).expect("the statement parses");
		let Some(TopLevelExpr::Expr(crate::expr::Expr::Define(define))) =
			parsed.expressions.into_iter().next()
		else {
			panic!("`{sql}` is not a definition");
		};
		let DefineStatement::Function(stmt) = *define else {
			panic!("`{sql}` does not define a function");
		};
		FunctionDefinition {
			name: stmt.name.clone(),
			args: stmt.args.clone(),
			block: stmt.block.clone(),
			body: crate::catalog::LogicalBlockBody::of(&stmt.block),
			permissions: stmt.permissions.clone(),
			permissions_body: crate::catalog::LogicalPermissionBody::of(&stmt.permissions),
			returns: stmt.returns.clone(),
			comment: None,
			auth_limit: crate::catalog::auth::AuthLimit::default(),
			graphql_alias: None,
			graphql_deprecated: None,
		}
	}

	fn inlinable(sql: &str) -> bool {
		let registry = FunctionRegistry::with_builtins();
		inlinable_body(&definition(sql), &registry).is_some()
	}

	#[test]
	fn a_body_over_its_arguments_alone_may_stand_in_the_caller() {
		assert!(inlinable("DEFINE FUNCTION fn::double($x: int) { RETURN $x * 2 }"));
		assert!(inlinable("DEFINE FUNCTION fn::bare($x: int) { $x + 1 }"));
		assert!(inlinable("DEFINE FUNCTION fn::len($x: string) { RETURN string::len($x) }"));
	}

	#[test]
	fn a_body_that_reads_more_than_its_arguments_may_not() {
		for sql in [
			"DEFINE FUNCTION fn::two($x: int) { LET $y = $x; RETURN $y }",
			"DEFINE FUNCTION fn::row() { RETURN c }",
			"DEFINE FUNCTION fn::this() { RETURN $this }",
			"DEFINE FUNCTION fn::sub() { RETURN (SELECT VALUE a FROM t) }",
			"DEFINE FUNCTION fn::calls($x: int) { RETURN fn::double($x) }",
			"DEFINE FUNCTION fn::outer($x: int) { RETURN $x + $other }",
			"DEFINE FUNCTION fn::searching($x: string) { RETURN $x @@ 'y' }",
			"DEFINE FUNCTION fn::closure($x: int) { RETURN |$y| $x + $y }",
		] {
			assert!(!inlinable(sql), "`{sql}`");
		}
	}

	#[test]
	fn a_protected_argument_name_may_not_stand_in_the_caller() {
		// A definition may declare a protected argument only when it was written
		// before the define-time refusal, or on an earlier version; the parser
		// still accepts the name, so the catalog of an upgraded database can hold
		// one. Its body may be inlinable, and inlining would replace the call with
		// the body, bypassing the invoke-time refusal in `bind_args`. The
		// definition is refused inlining so the call stands and `bind_args` refuses
		// it, inlined or not (GHSA-ffvm-mqh3-7986).
		for protected in surrealdb_cnf::PROTECTED_PARAM_NAMES {
			let sql =
				format!("DEFINE FUNCTION fn::id(${protected}: any) {{ RETURN ${protected} }}");
			assert!(!inlinable(&sql), "`{sql}`");
		}
		// The same body under a free argument name still stands in the caller.
		assert!(inlinable("DEFINE FUNCTION fn::id($row: any) { RETURN $row }"));
	}
}
