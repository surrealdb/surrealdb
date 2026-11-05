//! Constant `LET` propagation over the logical plan.
//!
//! A `LET` whose value is a literal binds its name to that literal for the
//! statements after it in its block, and for everything nested in them, until
//! a `LET` or a `FOR` binds the name again. The pass rewrites each read of such
//! a name into the literal. It changes nothing else.
//!
//! An `IF` condition is left as written, at any depth, in statement and in
//! expression position: whether an arm is taken is decided where the `IF`
//! runs, so no rule after this pass meets a literal condition a `LET` settled,
//! and an arm such a condition rules out is planned as an arm that may run is,
//! its refusals raised.
//!
//! A read inside a closure's body is left as written, since the closure reads
//! the name where it is called. So is a read in a table permission's predicate
//! or a layer's columns, definer code that never sees the caller's bindings.
//! A statement or expression that binds the name anywhere is left as written
//! too, so every read that is rewritten reads the literal it is rewritten to.

use crate::exec::planner::util::try_literal_to_value;
use crate::expr::logical::{LogicalExpr, LogicalLet, LogicalOp};

/// The names in scope, innermost binding last: each with the constant its
/// binding is, where it is a `LET` of a literal.
type Scope = Vec<(String, Option<LogicalExpr>)>;

/// Rewrite the reads of constant names in `op`, a statement.
pub(crate) fn propagate(op: &mut LogicalOp) {
	propagate_op(op, &mut Vec::new());
}

fn propagate_op(op: &mut LogicalOp, scope: &mut Scope) {
	match op {
		LogicalOp::Sequence(sequence) => {
			let outer = scope.len();
			for stmt in &mut sequence.stmts {
				propagate_op(stmt, scope);
				// A `LET` binds its name for the statements after it.
				if let LogicalOp::Let(stmt) = stmt {
					let constant = constant_of(stmt);
					scope.push((stmt.name.clone(), constant));
				}
			}
			scope.truncate(outer);
		}
		LogicalOp::Let(stmt) => propagate_op(&mut stmt.value, scope),
		LogicalOp::Foreach(foreach) => {
			substitute(&mut foreach.range, scope);
			let outer = scope.len();
			scope.push((foreach.param.as_str().to_owned(), None));
			propagate_op(&mut foreach.body, scope);
			scope.truncate(outer);
		}
		LogicalOp::If(ifs) => {
			for arm in &mut ifs.arms {
				propagate_op(arm, scope);
			}
		}
		LogicalOp::Return(ret) => {
			propagate_op(&mut ret.what, scope);
			for fetch in &mut ret.fetch {
				substitute(fetch, scope);
			}
		}
		LogicalOp::Throw(throw) => substitute(&mut throw.value, scope),
		// The explained statement reads the constants as the statement it explains would.
		LogicalOp::Explain(explain) => propagate_op(&mut explain.input, scope),
		LogicalOp::Break(_) | LogicalOp::Continue(_) | LogicalOp::Sleep(_) => {}
		LogicalOp::Eval(eval) => propagate_expr(&mut eval.expr, scope),
		// A statement that is no block, binding or control flow binds nothing
		// for the statements after it; its expressions read the constants.
		other @ (LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Window(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Info(_)) => {
			for (name, constant) in visible(scope) {
				if !rebinds(other, name) {
					with_constant_op(other, name, constant);
				}
			}
		}
	}
}

/// An expression statement. An `IF`, a block or a `THROW` in it is control
/// flow, its arms and values read as the statements they hold do.
fn propagate_expr(expr: &mut LogicalExpr, scope: &mut Scope) {
	if let LogicalExpr::If(ifs) = expr {
		for (_, value) in &mut ifs.branches {
			propagate_branch(value, scope);
		}
		if let Some(value) = &mut ifs.otherwise {
			propagate_branch(value, scope);
		}
	} else if let LogicalExpr::Subquery(op) = expr {
		propagate_op(op, scope);
	} else if let LogicalExpr::Throw(value) = expr {
		substitute(value, scope);
	} else {
		substitute(expr, scope);
	}
}

/// A value of an `IF` expression: a statement it holds, or an expression.
fn propagate_branch(value: &mut LogicalExpr, scope: &mut Scope) {
	if let LogicalExpr::Subquery(op) = value {
		propagate_op(op, scope);
	} else {
		substitute(value, scope);
	}
}

/// The constant a `LET` binds its name to: its value, where it is a literal
/// with no declared kind.
fn constant_of(stmt: &LogicalLet) -> Option<LogicalExpr> {
	if stmt.kind.is_some() {
		return None;
	}
	if let LogicalOp::Eval(eval) = stmt.value.as_ref()
		&& try_literal_to_value(&eval.expr).is_some()
	{
		return Some(eval.expr.clone());
	}
	None
}

/// The constants in scope, each name once at its innermost binding, where
/// that binding is a constant.
fn visible(scope: &Scope) -> Vec<(&str, &LogicalExpr)> {
	let mut seen: Vec<&str> = Vec::new();
	let mut out = Vec::new();
	for (name, constant) in scope.iter().rev() {
		if seen.contains(&name.as_str()) {
			continue;
		}
		seen.push(name.as_str());
		if let Some(constant) = constant {
			out.push((name.as_str(), constant));
		}
	}
	out
}

/// `expr` with each constant in scope read as its literal, where nothing in
/// `expr` binds the name again.
fn substitute(expr: &mut LogicalExpr, scope: &Scope) {
	for (name, constant) in visible(scope) {
		if !rebinds_in_expr(expr, name) {
			with_constant(expr, name, constant);
		}
	}
}

/// `expr` with each read of parameter `name` replaced by `constant`, in the
/// statements it holds too, but not in a closure's body, which reads the
/// name where the closure is called, nor in an `IF` condition.
fn with_constant(expr: &mut LogicalExpr, name: &str, constant: &LogicalExpr) {
	if let LogicalExpr::Param(param) = expr {
		if param.as_str() == name {
			*expr = constant.clone();
		}
	} else if let LogicalExpr::Closure(_) = expr {
	} else if let LogicalExpr::Subquery(op) = expr {
		with_constant_op(op, name, constant);
	} else if let LogicalExpr::If(ifs) = expr {
		for (_, value) in &mut ifs.branches {
			with_constant(value, name, constant);
		}
		if let Some(value) = &mut ifs.otherwise {
			with_constant(value, name, constant);
		}
	} else {
		for child in expr.children_mut() {
			with_constant(child, name, constant);
		}
	}
}

/// [`with_constant`] for every expression of statement `op` and of the
/// statements beneath it, except the conditions of a statement `IF`, a table
/// permission's predicate and a layer's columns — a field's permission or
/// `COMPUTED` body ([`LogicalOp::is_layer`]). Those are definer code, run in a
/// privileged frame that holds none of the caller's bindings, so the caller's
/// `LET` never reaches them, nor the statements they hold.
fn with_constant_op(op: &mut LogicalOp, name: &str, constant: &LogicalExpr) {
	if !matches!(op, LogicalOp::If(_) | LogicalOp::PermissionFilter(_)) && !op.is_layer() {
		let _ = op.visit_scalars_mut(&mut |scalar| {
			with_constant(scalar, name, constant);
			Ok::<(), std::convert::Infallible>(())
		});
	}
	for child in op.children_mut() {
		with_constant_op(child, name, constant);
	}
}

/// Whether statement `op` binds parameter `name` anywhere inside it: a
/// `LET`, a `FOR`, or a closure's parameter, in the statements its
/// expressions hold too.
fn rebinds(op: &LogicalOp, name: &str) -> bool {
	op.any(|node| {
		let binds = match node {
			LogicalOp::Let(stmt) => stmt.name == name,
			LogicalOp::Foreach(foreach) => foreach.param.as_str() == name,
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::Write(_)
			| LogicalOp::Ddl(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
			| LogicalOp::Filter(_)
			| LogicalOp::PermissionFilter(_)
			| LogicalOp::Split(_)
			| LogicalOp::Aggregate(_)
			| LogicalOp::Distinct(_)
			| LogicalOp::Sort(_)
			| LogicalOp::Window(_)
			| LogicalOp::Limit(_)
			| LogicalOp::Project(_)
			| LogicalOp::Fetch(_)
			| LogicalOp::Timeout(_)
			| LogicalOp::VersionScope(_)
			| LogicalOp::Only(_)
			| LogicalOp::Explain(_)
			| LogicalOp::Eval(_)
			| LogicalOp::Sequence(_)
			| LogicalOp::Return(_)
			| LogicalOp::If(_)
			| LogicalOp::Throw(_)
			| LogicalOp::Break(_)
			| LogicalOp::Continue(_)
			| LogicalOp::Info(_)
			| LogicalOp::Sleep(_) => false,
		};
		binds || node.scalars().into_iter().any(|scalar| rebinds_in_expr(scalar, name))
	})
}

/// [`rebinds`] for an expression.
fn rebinds_in_expr(expr: &LogicalExpr, name: &str) -> bool {
	expr.any(|node| {
		if let LogicalExpr::Closure(closure) = node {
			closure.args.iter().any(|(param, _)| param.as_str() == name)
		} else if let LogicalExpr::Subquery(op) = node {
			rebinds(op, name)
		} else {
			false
		}
	})
}

#[cfg(test)]
mod tests {
	use super::super::lower_test as lower;
	use super::propagate;

	fn propagated(sql: &str) -> String {
		let mut op = lower(sql);
		propagate(&mut op);
		op.to_string()
	}

	/// A read of a name a `LET` bound to a literal reads the literal, in a
	/// statement nested after the `LET` too.
	#[test]
	fn a_constant_let_is_read_as_its_literal() {
		let out = propagated("{ LET $x = 7; SELECT * FROM t WHERE a = $x }");
		assert!(out.contains("a = 7"), "the read of $x was left: {out}");
	}

	/// An `IF` condition reads the name where the `IF` runs, so no literal a
	/// `LET` binds settles an arm; the arms themselves read the literal.
	#[test]
	fn an_if_condition_is_left_as_written() {
		for sql in [
			"{ LET $x = false; IF $x { SELECT * FROM t WHERE a = $x } ELSE { 2 } }",
			"{ LET $x = false; RETURN IF $x THEN (SELECT * FROM t WHERE a = $x) ELSE 2 END }",
			"{ LET $x = false; SELECT * FROM t WHERE (IF $x THEN a ELSE b END) = $x }",
		] {
			let out = propagated(sql);
			// The `LET` and the condition name $x; nothing else does.
			assert_eq!(out.matches("$x").count(), 2, "`{sql}`: only the condition reads $x: {out}");
			assert!(out.contains("false"), "`{sql}`: the arm did not read the literal: {out}");
		}
	}

	/// The statement an `EXPLAIN` describes reads the constants of its own
	/// blocks, and a block's binding ends with the block.
	#[test]
	fn an_explained_statement_reads_its_constants() {
		let out = propagated("EXPLAIN RETURN { { LET $x = 1; $x + 1 }; $x }");
		assert!(out.contains("1 + 1"), "the read of $x in the block was left: {out}");
		assert_eq!(
			out.matches("$x").count(),
			2,
			"only the LET and the read after it name $x: {out}"
		);
	}
}
