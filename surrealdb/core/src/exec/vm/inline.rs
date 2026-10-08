//! Which user-defined function bodies a call may inline.
//!
//! A call runs its body as a subroutine: a program of its own, run one
//! re-entry deeper in a child of the caller's context — which reads the
//! caller's bindings, unless the caller is a privileged frame — with the
//! arguments bound as parameters. Inlining replaces only that: the body's instructions run in
//! the caller's program, reading the bound arguments from registers. The
//! call's boundary stays whole around them: the entry (capability,
//! definition, `AUTH LIMIT`, `PERMISSIONS`), the arguments evaluated in the
//! frame the entry opens, their count and coercion, the re-entry the call
//! consumes, and the coercion of what the body returns.
//!
//! So the body inlined must observe nothing the call's own context would have
//! answered differently: it reads no parameter but its arguments, no row, no
//! record, and no context, and it calls nothing but builtins that are pure
//! and synchronous. Such a body is a function of its arguments alone, and so
//! has the one value wherever it is evaluated.

use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{LogicalExpr, LogicalOp};

/// The most nodes a body inlined may have: past it, a call is the smaller
/// program.
const MAX_NODES: usize = 64;

/// The expression a call of a function whose bound body is `body` and whose
/// parameters are `params` may inline in place of running the body; `None`
/// when it must call it.
///
/// The body is one expression whose value is the function's: `{ RETURN e }`
/// or `{ e }`.
pub(super) fn inlinable<'b>(
	body: &'b LogicalOp,
	params: &[&str],
	registry: &FunctionRegistry,
) -> Option<&'b LogicalExpr> {
	let expr = single_expression(body)?;
	inlinable_expr(expr, params, registry).then_some(expr)
}

/// The one expression `body` is the value of: `{ RETURN e }` or `{ e }`.
fn single_expression(body: &LogicalOp) -> Option<&LogicalExpr> {
	let statement = match body {
		LogicalOp::Sequence(sequence) => match sequence.stmts.as_slice() {
			[statement] => statement,
			_ => return None,
		},
		statement => statement,
	};
	let expr = match statement {
		LogicalOp::Return(ret) if ret.fetch.is_empty() => match ret.what.as_ref() {
			LogicalOp::Eval(eval) => &eval.expr,
			_ => return None,
		},
		LogicalOp::Eval(eval) => &eval.expr,
		_ => return None,
	};
	Some(expr)
}

/// The expression a closure literal's body `body` is, when an invocation of
/// the literal by a builtin may run it in place: a block body being the one
/// expression it returns.
///
/// A closure's body runs in an isolated child of its caller's context, with
/// its arguments and what it captured where it was written bound, against
/// the caller's document. Where the literal is written in the call that
/// invokes it, that is the row the body is lowered over, and what it captured
/// is what the caller holds under those names, so a body may read the row,
/// its parameters and the caller's, and call builtins, besides what
/// [`inlinable_expr`] admits. It may not read `$parent`, which the isolated
/// context answers from the document rather than from the caller, nor run a
/// statement, a closure, a lookup or a user-defined function, each of which
/// the isolated context would plan or enter anew.
pub(super) fn closure_body<'b>(
	body: &'b LogicalExpr,
	params: &[&str],
	registry: &FunctionRegistry,
) -> Option<&'b LogicalExpr> {
	let expr = match body {
		LogicalExpr::Subquery(block) => single_expression(block)?,
		body => body,
	};
	let mut nodes = 0;
	let refused = expr.any(|node| {
		nodes += 1;
		nodes > MAX_NODES
			|| !(admits(node, params, registry)
				|| match node {
					LogicalExpr::Field(_)
					| LogicalExpr::Current
					| LogicalExpr::Access {
						..
					} => true,
					LogicalExpr::Param(param) => param.as_str() != "parent",
					LogicalExpr::Call(call) => match &call.receiver {
						crate::expr::Function::Normal(name) => {
							registry.get(name).is_some()
								&& !registry.is_index_function(name)
								&& !registry.is_projection(name)
								&& !crate::expr::method::is_writer_builtin(name)
						}
						_ => false,
					},
					_ => false,
				})
	});
	(!refused).then_some(expr)
}

/// Whether `expr`, a body over `params`, may run in place of the call that
/// binds them: a function of its operands alone, of at most [`MAX_NODES`]
/// nodes. A closure's body is judged by this alone.
pub(super) fn inlinable_expr(
	expr: &LogicalExpr,
	params: &[&str],
	registry: &FunctionRegistry,
) -> bool {
	let mut nodes = 0;
	!expr.any(|node| {
		nodes += 1;
		nodes > MAX_NODES || !admits(node, params, registry)
	})
}

/// Whether `node`, on its own, is a function of its operands alone.
fn admits(node: &LogicalExpr, params: &[&str], registry: &FunctionRegistry) -> bool {
	match node {
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::Prefix {
			..
		}
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::If(_)
		| LogicalExpr::First(_)
		| LogicalExpr::Last(_)
		| LogicalExpr::Flatten {
			..
		}
		| LogicalExpr::Index {
			..
		} => true,
		// A record id is a value; reading a field of one would fetch it.
		LogicalExpr::RecordId(_) => true,
		LogicalExpr::Param(param) => params.contains(&param.as_str()),
		LogicalExpr::Call(call) => match &call.receiver {
			crate::expr::Function::Normal(name) => registry.get(name).is_some_and(|function| {
				function.is_pure()
					&& !function.is_async()
					&& !registry.is_index_function(name)
					&& !registry.is_projection(name)
			}),
			_ => false,
		},
		// A field is read from a row, an access may fetch a record, and the
		// rest run statements, closures or lookups.
		_ => false,
	}
}
