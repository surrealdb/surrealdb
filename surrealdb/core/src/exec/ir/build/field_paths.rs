//! The paths a statement's `FETCH` and `OMIT` expressions name, resolved before the statement is
//! bound.
//!
//! A `FETCH` or `OMIT` names its paths as written, in a parameter that holds a path as text, or in
//! a projection function whose arguments name paths ([`Planner::resolve_field_paths`]). The binder
//! binds only paths, so every other form is resolved here, against the planner's context, into
//! the paths it names, in order: a `FETCH $p` and a `FETCH a` that name one path bind alike. An
//! expression that names no path is an error the planner raises while the statement is planned.
//!
//! An expression is resolved under the names bound around it inside the statement: a `FOR`
//! parameter in the loop's body, and a `LET` in the statements of its block after it. The planner
//! shadows them ([`Planner::shadowing`]), so a name the statement binds is never read from the
//! context. A parameter that the request's own `LET`s bind, and that no name around the expression
//! shadows, is not in the planner's context while the request is planned ([`PlanSession`]). A
//! statement whose `FETCH` or `OMIT` reads one is a late bind, planned when it is reached, after
//! the `LET` has run.
//!
//! Every plan of the statement is walked: its operators, and the plans nested in a
//! [`LogicalExpr::Subquery`] at any depth; a [`LogicalExpr::Closure`] body is not entered, since it
//! is planned where it is invoked ([`LogicalOp::visit_ops_deep`]).

use std::borrow::Cow;
use std::ops::ControlFlow;

use crate::exec::ir::error::IrError;
use crate::exec::planner::Planner;
use crate::exec::vm::PlanSession;
use crate::exec::vm::payload::Refusal;
use crate::expr::logical::{LogicalExpr, LogicalOp};

/// Which clause an expression to resolve is written in, which names the error an unresolvable
/// one raises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Clause {
	Fetch,
	Omit,
}

/// One `FETCH` or `OMIT` expression that is not a path, with the names bound around it.
type Pending = (Clause, LogicalExpr, Vec<String>);

/// `op` with every `FETCH` and `OMIT` expression of its plans resolved to the paths it names:
/// borrowed where every one is a path already.
///
/// Answers [`IrError::LateBind`] where an expression reads a parameter `session` binds by `LET`
/// that nothing around it shadows, [`IrError::Rejected`] with the planner's refusal where an
/// expression names no path, and [`IrError::Invariant`] where the resolution fails otherwise.
pub(crate) async fn resolved<'a>(
	planner: &Planner<'_>,
	session: &PlanSession,
	op: &'a LogicalOp,
) -> Result<Cow<'a, LogicalOp>, IrError> {
	if !holds_pending(op) {
		return Ok(Cow::Borrowed(op));
	}
	let mut owned = op.clone();
	let pending = pending(&mut owned);
	let mut answers: Vec<Vec<LogicalExpr>> = Vec::with_capacity(pending.len());
	for (at, (clause, expr, bound)) in pending.iter().enumerate() {
		// The same expression under the same names resolves alike.
		if let Some(earlier) =
			pending[..at].iter().position(|(_, held, around)| held == expr && around == bound)
		{
			let paths = answers.get(earlier).cloned().ok_or(IrError::Invariant {
				what: "a FETCH or OMIT resolved out of order",
			})?;
			answers.push(paths);
			continue;
		}
		if reads_session_let(expr, session, bound) {
			return Err(IrError::LateBind {
				what: "a FETCH or OMIT that reads a parameter a LET of the request binds",
			});
		}
		let scoped = planner.shadowing(bound.iter().cloned());
		let resolving = scoped.resolve_field_paths(expr.clone());
		let paths = crate::kvs::catalog_reads::sealed(false, resolving).await.map_err(|error| {
			match Refusal::of(&error) {
				Some(refusal) => IrError::Rejected(Box::new(refusal)),
				None => IrError::Invariant {
					what: match clause {
						Clause::Fetch => "the FETCH did not resolve",
						Clause::Omit => "an OMIT path the operator tree cannot resolve",
					},
				},
			}
		})?;
		answers.push(paths);
	}
	replace(&mut owned, answers)?;
	Ok(Cow::Owned(owned))
}

/// Whether any `FETCH` or `OMIT` expression of `op`'s plans is not a path.
fn holds_pending(op: &LogicalOp) -> bool {
	op.visit_ops_deep(&mut |node| match clause_of(node) {
		Some((_, exprs)) if exprs.iter().any(|expr| !expr.is_path()) => ControlFlow::Break(()),
		_ => ControlFlow::Continue(()),
	})
	.is_break()
}

/// The clause `op` writes `FETCH` or `OMIT` expressions in, and the expressions.
fn clause_of(op: &LogicalOp) -> Option<(Clause, &[LogicalExpr])> {
	match op {
		LogicalOp::Project(project) => Some((Clause::Omit, &project.omit)),
		LogicalOp::Fetch(fetch) => Some((Clause::Fetch, &fetch.fetches)),
		LogicalOp::Return(statement) => Some((Clause::Fetch, &statement.fetch)),
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
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => None,
	}
}

/// [`clause_of`], mutably.
fn clause_of_mut(op: &mut LogicalOp) -> Option<(Clause, &mut Vec<LogicalExpr>)> {
	match op {
		LogicalOp::Project(project) => Some((Clause::Omit, &mut project.omit)),
		LogicalOp::Fetch(fetch) => Some((Clause::Fetch, &mut fetch.fetches)),
		LogicalOp::Return(statement) => Some((Clause::Fetch, &mut statement.fetch)),
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
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_)
		| LogicalOp::Only(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => None,
	}
}

/// The `FETCH` and `OMIT` expressions of `op`'s plans that are not paths, in the order [`walk`]
/// meets them, each with the names bound around it.
fn pending(op: &mut LogicalOp) -> Vec<Pending> {
	let mut out = Vec::new();
	walk(op, &mut Vec::new(), &mut |clause, exprs, bound| {
		for expr in exprs.iter().filter(|expr| !expr.is_path()) {
			out.push((clause, expr.clone(), bound.to_vec()));
		}
	});
	out
}

/// Replaces each `FETCH` and `OMIT` expression of `op`'s plans that is not a path, in the order
/// [`pending`] lists them, with the paths `answers` holds for it.
fn replace(op: &mut LogicalOp, answers: Vec<Vec<LogicalExpr>>) -> Result<(), IrError> {
	let mut answers = answers.into_iter();
	let mut short = false;
	walk(op, &mut Vec::new(), &mut |_, exprs, _| {
		if exprs.iter().all(LogicalExpr::is_path) {
			return;
		}
		let mut out = Vec::with_capacity(exprs.len());
		for expr in std::mem::take(exprs) {
			if expr.is_path() {
				out.push(expr);
				continue;
			}
			match answers.next() {
				Some(paths) => out.extend(paths),
				None => short = true,
			}
		}
		*exprs = out;
	});
	if short || answers.next().is_some() {
		return Err(IrError::Invariant {
			what: "a FETCH or OMIT resolved out of order",
		});
	}
	Ok(())
}

/// Whether `expr` reads a parameter a `LET` of the request binds, by its value or as computed,
/// that none of `bound` shadows.
fn reads_session_let(expr: &LogicalExpr, session: &PlanSession, bound: &[String]) -> bool {
	if let LogicalExpr::Param(name) = expr {
		let name = name.as_str();
		return !bound.iter().any(|around| around == name)
			&& (session.computed.contains(name) || session.binds_constant(name));
	}
	let mut reads = false;
	expr.for_each_child(&mut |child| reads = reads || reads_session_let(child, session, bound));
	reads
}

/// The callback [`walk`] calls with each `FETCH` or `OMIT` clause, its expressions and the names
/// bound around them.
type ClauseVisitor<'a> = dyn FnMut(Clause, &mut Vec<LogicalExpr>, &[String]) + 'a;

/// Calls `f` with the `FETCH` or `OMIT` expressions of every plan of `op` and the names bound
/// around them, in pre-order: a node's own, then the plans its scalars hold, then its inputs. A
/// `FOR` binds its parameter for its body; a `LET` in a block binds its name for the statements
/// after it. `bound` holds the names bound around `op`, and is as it was when the walk returns.
fn walk(op: &mut LogicalOp, bound: &mut Vec<String>, f: &mut ClauseVisitor<'_>) {
	if let Some((clause, exprs)) = clause_of_mut(op) {
		f(clause, exprs, bound);
	}
	for scalar in op.scalars_mut() {
		walk_scalar(scalar, bound, f);
	}
	let outer = bound.len();
	if let LogicalOp::Foreach(foreach) = op {
		bound.push(foreach.param.as_str().to_owned());
	}
	let sequence = matches!(op, LogicalOp::Sequence(_));
	for child in op.children_mut() {
		walk(child, bound, f);
		if let (true, LogicalOp::Let(statement)) = (sequence, &*child) {
			bound.push(statement.name.clone());
		}
	}
	bound.truncate(outer);
}

/// [`walk`] over the plans `expr` holds; a closure body is not entered.
fn walk_scalar(expr: &mut LogicalExpr, bound: &mut Vec<String>, f: &mut ClauseVisitor<'_>) {
	match expr {
		LogicalExpr::Subquery(plan) => walk(plan, bound, f),
		LogicalExpr::Closure(_) => {}
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Field(_)
		| LogicalExpr::Doc
		| LogicalExpr::Current
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
		| LogicalExpr::Lookup {
			..
		}
		| LogicalExpr::Recurse(_)
		| LogicalExpr::RepeatRecurse(_)
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
		| LogicalExpr::Call(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue => {
			for child in expr.children_mut() {
				walk_scalar(child, bound, f);
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{Clause, holds_pending, pending, replace};
	use crate::expr::logical::{LogicalExpr, LogicalOp, lower};

	fn plan(sql: &str) -> LogicalOp {
		let parsed = crate::syn::parse(sql).expect("the statement parses").expressions;
		let Some(surrealdb_expr::parser::TopLevelExpr::Expr(expr)) = parsed.into_iter().next()
		else {
			panic!("one statement");
		};
		lower(expr).expect("the statement lowers")
	}

	fn param(name: &str) -> LogicalExpr {
		LogicalExpr::Param(name.to_owned().into())
	}

	fn field(name: &str) -> LogicalExpr {
		LogicalExpr::Field(name.into())
	}

	#[test]
	fn a_path_is_not_resolved() {
		assert!(!holds_pending(&plan("SELECT * OMIT a FROM t FETCH b.c")));
	}

	#[test]
	fn a_parameter_is_resolved_in_every_plan_of_the_statement() {
		let mut held = plan("SELECT *, (SELECT * FROM u FETCH $p) AS v OMIT $q FROM t FETCH $p");
		assert!(holds_pending(&held));
		let pending = pending(&mut held);
		assert_eq!(pending.len(), 3, "{pending:?}");
		assert!(pending.contains(&(Clause::Fetch, param("p"), Vec::new())), "{pending:?}");
		assert!(pending.contains(&(Clause::Omit, param("q"), Vec::new())), "{pending:?}");
	}

	#[test]
	fn a_name_the_statement_binds_is_bound_around_what_reads_it() {
		let mut held = plan(
			"{ LET $a = 1; FOR $p IN [1] { SELECT * FROM t FETCH $p }; SELECT * FROM u FETCH $p; }",
		);
		let pending = pending(&mut held);
		let around: Vec<Vec<String>> = pending.into_iter().map(|(_, _, bound)| bound).collect();
		assert_eq!(
			around,
			vec![vec!["a".to_owned(), "p".to_owned()], vec!["a".to_owned()]],
			"the loop's body binds $p, and the statement after the loop does not"
		);
	}

	#[test]
	fn a_resolved_expression_is_replaced_by_its_paths_in_order() {
		let mut held = plan("SELECT *, (SELECT * FROM u FETCH $p) AS v FROM t FETCH a, $p");
		let count = pending(&mut held).len();
		let answers = vec![vec![field("x"), field("y")]; count];
		replace(&mut held, answers).expect("every expression has its answer");
		assert!(!holds_pending(&held));
		let mut fetches = Vec::new();
		let _ = held.visit_ops_deep(&mut |node| {
			if let LogicalOp::Fetch(fetch) = node {
				fetches.push(fetch.fetches.clone());
			}
			std::ops::ControlFlow::<()>::Continue(())
		});
		assert!(fetches.contains(&vec![field("a"), field("x"), field("y")]), "{fetches:?}");
		assert!(fetches.contains(&vec![field("x"), field("y")]), "{fetches:?}");
	}

	#[test]
	fn answers_that_do_not_match_the_expressions_are_an_invariant_error() {
		let mut held = plan("SELECT * FROM t FETCH $p");
		assert!(replace(&mut held.clone(), Vec::new()).is_err());
		assert!(replace(&mut held, vec![vec![field("x")], vec![field("y")]]).is_err());
	}

	#[test]
	fn a_closure_body_is_not_walked() {
		let mut held = plan("SELECT *, |$x| (SELECT * FROM u FETCH $p) AS f FROM t");
		assert!(!holds_pending(&held));
		assert!(pending(&mut held).is_empty());
	}
}
