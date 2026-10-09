//! Control flow whose choice the query already made.
//!
//! An [`IF`](crate::expr::logical::LogicalIf) selects a branch by evaluating
//! conditions in order. Where a condition is a literal the choice is settled
//! without running anything, so the branch it settles can replace the whole
//! expression and the branches it rules out can go.
//!
//! Nothing is reordered. Conditions are evaluated in the order written, so a
//! condition before the settled one still runs and still decides first; this
//! rule only drops what the order has already made unreachable. That keeps it
//! sound for a condition that writes, throws or draws entropy, and keeps which
//! error a statement raises exactly as it was.
//!
//! Three shapes, in the order they are recognised:
//!
//! - A branch whose condition is `false` never runs. It goes, and its value goes with it.
//! - A branch whose condition is `true` always runs, *given that every condition before it was
//!   false*. It becomes the `ELSE`, and every branch after it goes — they were unreachable.
//! - No branches left is no choice left: the expression is its `ELSE`, or `NONE` where it has none.
//!
//! Code that never runs goes only where it holds no `LET` that planning refuses: a `LET` of a
//! protected name (`$auth`, `$access`, `$token`, `$session`), or a `LET` written as an expression
//! (`WHERE (LET $x = 1)`, `[(LET $x = 1)]`). Either is refused wherever the statement holds it,
//! whether or not it runs, and the refusal is raised where the statement is mapped, so a `false`
//! branch holding one is kept, and so is a `true` branch followed by an arm or an `ELSE` holding
//! one. Any other code an arm that never runs holds — a statement, a closure — goes with it.

use super::{ExprContext, RuleContext, RuleDef};
use crate::exec::logical::{holds_let_expression, holds_protected_let};
use crate::expr::logical::{
	LogicalEval, LogicalExpr, LogicalIf, LogicalIfOp, LogicalLiteral, LogicalOp, OpKind,
};

/// The same collapse for an `IF` in statement position
/// ([`LogicalOp::If`]): what is left of it, or — where no condition is left
/// to evaluate — the expression statement of the arm it falls through to, as
/// the expression form collapses to that arm's value.
///
/// An arm that never runs goes, with the statements and closures it holds,
/// unless it holds a `LET` planning refuses whether or not it runs
/// ([`op_holds_refused_let`]): a `false` arm holding one is kept, and so is a
/// `true` arm followed by a condition, an arm or an `ELSE` holding one.
pub(super) const RULE: RuleDef = RuleDef {
	name: "control_flow",
	kinds: &[OpKind::If],
	apply: apply_op,
};

fn apply_op(node: &mut LogicalOp, _cx: &mut RuleContext<'_>) -> bool {
	let LogicalOp::If(ifs) = node else {
		return false;
	};
	if !foldable_op(ifs) {
		return false;
	}
	let (branches, mut otherwise) = std::mem::replace(
		ifs,
		LogicalIfOp {
			conditions: Vec::new(),
			arms: Vec::new(),
		},
	)
	.into_parts();
	let mut kept = Vec::with_capacity(branches.len());
	let mut arms = branches.into_iter();
	while let Some((condition, arm)) = arms.next() {
		match as_literal_bool(&condition) {
			// Never taken: the arm goes, and so do the statements it holds.
			Some(false) if !op_holds_refused_let(&arm) => {}
			// Always taken, given the conditions before it were false: it is the
			// `ELSE`, and every arm after it is unreachable.
			Some(true)
				if !unreached_arms_hold_a_refused_let(arms.as_slice(), otherwise.as_ref()) =>
			{
				otherwise = Some(arm);
				break;
			}
			// Undecided, or holding a refused `LET`: kept where it was, so the
			// order of evaluation stands.
			_ => kept.push((condition, arm)),
		}
	}
	*node = if kept.is_empty() {
		match otherwise {
			// An `ELSE` that writes stays a branch: a write in one spends the
			// computation-depth budget the branch's nesting spends
			// ([`crate::exec::vm::depth`]).
			Some(arm) if writes_op(&arm) => LogicalOp::If(LogicalIfOp::new(Vec::new(), Some(arm))),
			otherwise => LogicalOp::Eval(LogicalEval {
				expr: otherwise
					.map_or(LogicalExpr::Literal(LogicalLiteral::None), LogicalOp::into_scalar),
			}),
		}
	} else {
		LogicalOp::If(LogicalIfOp::new(kept, otherwise))
	};
	true
}

/// Whether [`apply_op`] removes an arm of `ifs`: a `false` arm holding no
/// refused `LET`, or a `true` arm whose unreached conditions, arms and `ELSE`
/// hold none ([`op_holds_refused_let`]).
fn foldable_op(ifs: &LogicalIfOp) -> bool {
	let branches: Vec<(&LogicalExpr, &LogicalOp)> = ifs.branches().collect();
	branches.iter().enumerate().any(|(at, (condition, arm))| match as_literal_bool(condition) {
		Some(false) => !op_holds_refused_let(arm),
		Some(true) => {
			!branches[at + 1..]
				.iter()
				.any(|(condition, arm)| holds_refused_let(condition) || op_holds_refused_let(arm))
				&& !ifs.otherwise().is_some_and(op_holds_refused_let)
		}
		None => false,
	})
}

/// Whether the arms `after` a `true` arm, or the `otherwise` after them, hold
/// a refused `LET` ([`op_holds_refused_let`]).
fn unreached_arms_hold_a_refused_let(
	after: &[(LogicalExpr, LogicalOp)],
	otherwise: Option<&LogicalOp>,
) -> bool {
	after.iter().any(|(condition, arm)| holds_refused_let(condition) || op_holds_refused_let(arm))
		|| otherwise.is_some_and(op_holds_refused_let)
}

/// [`holds_refused_let`] for statement `op`: a `LET` of a protected name
/// anywhere in it, or a `LET` written as an expression in any of its
/// expressions or those of the statements it holds.
fn op_holds_refused_let(op: &LogicalOp) -> bool {
	use std::ops::ControlFlow;
	op.visit_ops_deep(&mut |node| {
		let protected = matches!(
			node,
			LogicalOp::Let(stmt) if surrealdb_cnf::PROTECTED_PARAM_NAMES.contains(&stmt.name.as_str())
		);
		match protected || node.scalars().into_iter().any(holds_let_expression) {
			true => ControlFlow::Break(()),
			false => ControlFlow::Continue(()),
		}
	})
	.is_break()
}

/// Collapse an `IF` around the conditions the query wrote as literals.
pub(super) fn apply(expr: &mut LogicalExpr, _cx: &ExprContext<'_>) -> bool {
	let LogicalExpr::If(conditional) = expr else {
		return false;
	};
	let Some(collapsed) = collapse(conditional) else {
		return false;
	};
	*expr = collapsed;
	true
}

/// What `conditional` becomes, or `None` where no literal condition settles anything that can go.
fn collapse(conditional: &mut LogicalIf) -> Option<LogicalExpr> {
	if !foldable(conditional) {
		return None;
	}
	let mut kept: Vec<(LogicalExpr, LogicalExpr)> = Vec::with_capacity(conditional.branches.len());
	let mut otherwise = conditional.otherwise.take();
	let mut arms = std::mem::take(&mut conditional.branches).into_iter();
	while let Some((condition, value)) = arms.next() {
		match as_literal_bool(&condition) {
			// Never taken: the branch goes, and so does its value.
			Some(false) if !holds_refused_let(&value) => {}
			// Always taken, given the conditions before it were false: it is the `ELSE`, and
			// every branch after it is unreachable.
			Some(true) if !unreachable_holds_a_refused_let(arms.as_slice(), otherwise.as_ref()) => {
				otherwise = Some(value);
				break;
			}
			// Undecided, or holding a refused `LET`: kept where it was, so the order of evaluation
			// stands.
			_ => kept.push((condition, value)),
		}
	}
	if kept.is_empty() {
		// No condition left to evaluate: the expression is whatever it falls through to. An `IF`
		// with no branch and no `ELSE` yields `NONE`, which is what the evaluator answers for one
		// whose conditions all fail. A branch that writes stays a branch: a write in one spends
		// the computation-depth budget the branch's nesting spends ([`crate::exec::vm::depth`]).
		return Some(match otherwise {
			Some(value) if writes(&value) => LogicalExpr::If(Box::new(LogicalIf {
				branches: Vec::new(),
				otherwise: Some(value),
			})),
			otherwise => otherwise.unwrap_or(LogicalExpr::Literal(LogicalLiteral::None)),
		});
	}
	Some(LogicalExpr::If(Box::new(LogicalIf {
		branches: kept,
		otherwise,
	})))
}

/// Whether statement `op`, or a statement it holds, writes.
fn writes_op(op: &LogicalOp) -> bool {
	use std::ops::ControlFlow;
	op.visit_ops_deep(&mut |node| match node {
		LogicalOp::Write(_) => ControlFlow::Break(()),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => ControlFlow::Continue(()),
	})
	.is_break()
}

/// Whether [`collapse`] removes something from `conditional`: a `false` branch holding no refused
/// `LET`, or a `true` branch whose unreachable arms and `ELSE` hold none ([`holds_refused_let`]).
fn foldable(conditional: &LogicalIf) -> bool {
	let branches = &conditional.branches;
	branches.iter().enumerate().any(|(at, (condition, value))| match as_literal_bool(condition) {
		Some(false) => !holds_refused_let(value),
		Some(true) => {
			!unreachable_holds_a_refused_let(&branches[at + 1..], conditional.otherwise.as_ref())
		}
		None => false,
	})
}

/// Whether the arms `after` a `true` branch, or the `otherwise` after them, hold a refused `LET`
/// ([`holds_refused_let`]).
fn unreachable_holds_a_refused_let(
	after: &[(LogicalExpr, LogicalExpr)],
	otherwise: Option<&LogicalExpr>,
) -> bool {
	after.iter().any(|(condition, value)| holds_refused_let(condition) || holds_refused_let(value))
		|| otherwise.is_some_and(holds_refused_let)
}

/// Whether `expr` holds a `LET` planning refuses whether or not it runs: one of a protected name,
/// or one written as an expression.
fn holds_refused_let(expr: &LogicalExpr) -> bool {
	holds_protected_let(expr) || holds_let_expression(expr)
}

/// Whether `expr` holds a statement that writes.
fn writes(expr: &LogicalExpr) -> bool {
	use std::ops::ControlFlow;
	expr.visit_deep(&mut |node| {
		let LogicalExpr::Subquery(plan) = node else {
			return ControlFlow::Continue(());
		};
		match plan
			.visit_ops_deep(&mut |op| {
				if matches!(op, LogicalOp::Write(_)) {
					ControlFlow::Break(())
				} else {
					ControlFlow::Continue(())
				}
			})
			.is_break()
		{
			true => ControlFlow::Break(()),
			false => ControlFlow::Continue(()),
		}
	})
	.is_break()
}

/// The value of `expr` when it is a literal boolean.
#[expect(clippy::wildcard_enum_match_arm, reason = "only a literal boolean has a value")]
fn as_literal_bool(expr: &LogicalExpr) -> Option<bool> {
	match expr {
		LogicalExpr::Literal(LogicalLiteral::Bool(value)) => Some(*value),
		_ => None,
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::super::super::lower_test as lower;
	use super::super::test_support::rewritten;

	/// `IF true` is what it selects, and the branches after it go.
	#[test]
	fn a_true_condition_selects_its_branch() {
		assert_eq!(
			rewritten(lower("RETURN IF true THEN 1 ELSE 2 END")).to_string(),
			"Return\n  Eval[1]"
		);
		assert_eq!(
			rewritten(lower("RETURN IF true THEN 1 ELSE IF $c THEN 2 ELSE 3 END")).to_string(),
			"Return\n  Eval[1]",
			"a branch after the taken one is unreachable and goes"
		);
	}

	/// `IF false` drops the branch, and with it whatever the branch would have run.
	#[test]
	fn a_false_condition_drops_its_branch() {
		assert_eq!(
			rewritten(lower("RETURN IF false THEN 1 ELSE 2 END")).to_string(),
			"Return\n  Eval[2]"
		);
		assert_eq!(
			rewritten(lower("RETURN IF false THEN 1 END")).to_string(),
			"Return\n  Eval[NONE]",
			"no branch and no ELSE yields NONE, as the evaluator answers"
		);
	}

	/// A branch that never runs is kept where it binds a protected name, so the refusal of that
	/// `LET` is raised; one holding any other statement goes.
	#[test]
	fn unreachable_code_binding_a_protected_name_is_kept() {
		let kept =
			rewritten(lower("RETURN IF false THEN { LET $auth = 1; 1 } ELSE 2 END")).to_string();
		assert!(kept.contains("IF false"), "a false branch binding $auth went: {kept}");
		let kept =
			rewritten(lower("RETURN IF true THEN 1 ELSE { LET $auth = 1; 1 } END")).to_string();
		assert!(
			kept.contains("IF true"),
			"a true branch before an ELSE binding $auth was folded: {kept}"
		);
		assert_eq!(
			rewritten(lower("RETURN IF false THEN (CREATE t:1) ELSE 2 END")).to_string(),
			"Return\n  Eval[2]",
			"a false branch holding a write but no protected LET goes"
		);
		let kept = rewritten(lower("RETURN IF true THEN 1 ELSE IF $c THEN { LET $auth = 1 } END"))
			.to_string();
		assert!(
			kept.contains("IF true"),
			"a true branch before an arm binding $auth was folded: {kept}"
		);
		assert_eq!(
			rewritten(lower("RETURN IF false THEN { LET $z = 1 } ELSE 2 END")).to_string(),
			"Return\n  Eval[2]",
			"a LET of an unprotected name goes as any statement does"
		);
	}

	/// A branch that never runs is kept where it holds a `LET` written as an expression, so the
	/// refusal of that `LET` is raised; a `LET` that is a block's statement, or the value of a
	/// `RETURN` or another `LET`, goes with its branch.
	#[test]
	fn unreachable_code_holding_a_let_expression_is_kept() {
		let kept = rewritten(lower(
			"RETURN IF false THEN (SELECT * FROM t WHERE (LET $x = 1)) ELSE 2 END",
		))
		.to_string();
		assert!(kept.contains("IF false"), "a false branch holding a LET expression went: {kept}");
		let kept = rewritten(lower("RETURN IF true THEN 1 ELSE [(LET $x = 1)] END")).to_string();
		assert!(
			kept.contains("IF true"),
			"a true branch before an ELSE holding a LET expression was folded: {kept}"
		);
		assert_eq!(
			rewritten(lower("RETURN IF false THEN { RETURN (LET $x = 1) } ELSE 2 END")).to_string(),
			"Return\n  Eval[2]",
			"a LET a RETURN takes as its value goes with its branch"
		);
	}

	/// A condition that is not a literal stays, and stays in its place.
	#[test]
	fn an_undecided_condition_is_left_where_it_was() {
		assert_eq!(
			rewritten(lower("RETURN IF $c THEN 1 ELSE 2 END")).to_string(),
			"Return\n  Eval[IF $c THEN 1 ELSE 2 END]",
			"nothing to decide, nothing to change"
		);
		assert_eq!(
			rewritten(lower("RETURN IF $c THEN 1 ELSE IF false THEN 2 ELSE 3 END")).to_string(),
			"Return\n  Eval[IF $c THEN 1 ELSE 3 END]",
			"the undecided condition keeps its position and its order"
		);
	}

	/// A condition before the decided one is still evaluated first.
	///
	/// The rule drops what the written order has made unreachable; it never
	/// moves a condition past another. So a condition that writes, throws or
	/// draws entropy runs exactly when it would have.
	#[test]
	fn a_condition_before_the_decided_one_is_kept() {
		assert_eq!(
			rewritten(lower("RETURN IF $c THEN 1 ELSE IF true THEN 2 ELSE 3 END")).to_string(),
			"Return\n  Eval[IF $c THEN 1 ELSE 2 END]",
			"the always-true branch becomes the ELSE, beneath the condition that precedes it"
		);
	}

	/// A statement `IF` arm that never runs goes, with the statements it
	/// holds, and is kept where it holds a `LET` planning refuses.
	#[test]
	fn a_statement_arm_that_never_runs_is_kept_only_for_a_refused_let() {
		for sql in [
			"IF false { 1 } ELSE { 2 }",
			"IF false { LET $a = 1 } ELSE { 2 }",
			"IF false { SELECT * FROM t } ELSE { 2 }",
			"IF true { 1 } ELSE { LET $a = 1 }",
			"IF true { 1 } ELSE IF $c { SELECT * FROM t }",
		] {
			let folded = rewritten(lower(sql)).to_string();
			assert!(!folded.contains("If"), "`{sql}`: an arm that never runs was kept: {folded}");
		}
		for sql in [
			"IF false { LET $auth = 1 } ELSE { 2 }",
			"IF true { 1 } ELSE { LET $session = 1 }",
			"IF false { SELECT * FROM t WHERE (LET $x = 1) } ELSE { 2 }",
			"IF true { 1 } ELSE IF $c { [(LET $x = 1)] }",
		] {
			let kept = rewritten(lower(sql)).to_string();
			assert!(kept.contains("If"), "`{sql}`: an arm holding a refused LET went: {kept}");
		}
	}
}
