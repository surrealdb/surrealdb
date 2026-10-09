//! The rewrite rules over the logical algebra.
//!
//! There are two kinds of rule, and the driver (`super::optimise`) owns the
//! traversal for both, bottom-up and nested plans included, repeated until a
//! full pass changes nothing.
//!
//! A **node rule** ([`Rule`]) rewrites one operator through a `&mut` and
//! answers whether it did. It sees the node it is applied to and what the
//! node's inputs expose — their shape, their schema, whether an input is a
//! layer, the frame a join carries — and nothing above the node, so no rule
//! reasons about where in the statement it stands. It is reached only at a
//! node whose [`LogicalOp::kind`] its [`RuleDef::kinds`] names, which is both
//! what spares it an arm for every operator it ignores and where most of the
//! set's work goes unspent. Declining is leaving the node as it is, so a
//! variant no rule names is simply not rewritten.
//!
//! A node rule that rewrites the scalars of any node rather than the node
//! itself declares [`OpKind::ALL`] and reaches them through
//! [`rewrite_row_positions`], which owns that traversal and the row schema it
//! is read against.
//!
//! An **expression rule** ([`ExprRule`]) rewrites one scalar in place. The
//! driver applies it to every scalar of every node, so it states no node
//! kinds and walks nothing; what it rewrites holds wherever the expression
//! sits, whatever the plan around it has become. Constant folding is such a
//! rule, which is why it reaches every expression of every operator rather
//! than the predicates one pass thought to fold.
//!
//! Every rule is a strict improvement of the plan it rewrites — a predicate
//! judged once rather than twice, a node that does nothing removed, a call
//! replaced by its body — so applying the set eagerly to a fixpoint is sound
//! and terminates. A choice with a cost is not a rule: which access path
//! answers a predicate, in which order two inputs join, whether a lookup join
//! beats a hash join, are the physical planner's, and the driver records no
//! alternatives. A rule sees one node: a subexpression two nodes hold is
//! rewritten where each occurrence stands, since the plan is a tree and no
//! occurrence knows of another. A pure subexpression one evaluation region
//! repeats is evaluated once per row within that region, which is decided when
//! the plan is lowered to a program, through [`crate::expr::logical::dag`], and
//! leaves the plan as it is.
//!
//! The rules, each in its own module:
//!
//! | Rule | Module |
//! |---|---|
//! | Adjacent filters merge; a predicate moves through a user projection, and into a subquery that returns its rows unchanged | [`filter`] |
//! | The conjuncts that read no layer column move beneath a chain of layers, to directly above the source or the permission node the chain stands on | [`layers`] |
//! | A conjunct over one side of a join moves to that side | [`join`] |
//! | An identity projection goes; a layer column nothing above reads goes | [`prune`] |
//! | A filter that admits every row, a paging clause that pages nothing, a sort of one row, a doubled `ONLY` and a block within a block go | [`identity`] |
//! | A membership subquery in a predicate becomes a semi or anti join | [`decorrelate`] |
//! | A call to a `DEFINE FUNCTION` whose body is one expression is replaced by that expression | [`inline`] |
//! | A cast to the kind an expression already has, and a broadcast over a value that is not a collection, go | [`kind`] |
//! | A statement `IF` drops the arms a literal condition rules out, and becomes the arm a literal condition selects | [`control_flow`] |
//!
//! And the expression rules:
//!
//! | Rule | Module |
//! |---|---|
//! | An expression over constants is replaced by the value it computes to | [`super::fold`] |
//!
//! # Barriers
//!
//! The rules uphold the barrier rule of the logical module (see its
//! documentation): a user expression never moves below a
//! [`LogicalOp::PermissionFilter`] or a join in a permission frame, and below
//! a layer only when every field it reads is a passthrough of that layer. A
//! column whose frame is not the caller's is opaque to substitution, and so is
//! any read that cannot be told from the expression — a row parameter, the
//! document, a nested plan, a call evaluated against the row
//! ([`reads::reads_row_opaquely`]).

use std::collections::HashSet;
use std::ops::ControlFlow;

use super::effects::{Oracle, ParamsOnly};
use super::functions::Functions;
use super::visit_deep;
use crate::dbs::capabilities::Capabilities;
use crate::exec::function::FunctionRegistry;
use crate::exec::planner::util::RestrictedFields;
use crate::expr::BinaryOperator;
use crate::expr::logical::{
	EvalMode, JoinKind, LogicalExpr, LogicalJoin, LogicalLiteral, LogicalOp, LogicalSource, OpKind,
	Schema, SourceRef, effects_of,
};
pub(super) use crate::expr::logical::{ROW_PARAMS, may_dereference};

pub(super) mod control_flow;
pub(super) mod decorrelate;
pub(super) mod filter;
pub(super) mod identity;
pub(super) mod inline;
pub(super) mod join;
pub(super) mod kind;
pub(super) mod layers;
pub(super) mod prune;

pub(super) use super::reads;

/// What every rule may consult besides the node it is applied to.
pub(super) struct RuleContext<'a> {
	/// Parameter names bound per row (`$this`, `$parent`, …), which no rule
	/// substitutes and which make a read opaque.
	pub(super) row_scoped: &'a [&'a str],
	/// Names the builtins that are evaluated against the row rather than
	/// against their arguments.
	pub(super) registry: &'a FunctionRegistry,
	/// The user-defined functions a call in the plan may be replaced by.
	pub(super) functions: &'a Functions,
	/// Names a read of which fetches a `DEFINE PARAM` where it is read, the
	/// definition's `PERMISSIONS` clause judged per fetch. Reading one may be
	/// denied, so it is not an inert read ([`may_raise`]); see
	/// [`super::params::permission_checked_names`].
	pub(super) permission_checked_params: &'a HashSet<String>,
	/// One index past the highest join alias the plan carries, advanced by
	/// every rule that mints one.
	pub(super) next_alias: usize,
	/// What the caller may execute, which bounds what a call folds to.
	pub(super) capabilities: &'a Capabilities,
	/// Whether a `time::now()` in a condition folds to the time of binding
	/// ([`super::BindContext::folds_clock`]).
	pub(super) folds_clock: bool,
}

/// One node rewrite rule; see the module documentation for what a rule may
/// see.
///
/// A rule inspects the node through the reference and answers whether it
/// rewrote it. It is reached only for a node whose [`LogicalOp::kind`] its
/// [`RuleDef::kinds`] names, so it needs no arm for a node it never
/// rewrites — and where it declines after looking closer, it returns
/// `false` without having to put back what it took apart. Tree surgery is
/// still by value: take the node with [`fire`] once the decision is made.
pub(super) type Rule = fn(&mut LogicalOp, &mut RuleContext<'_>) -> bool;

/// What a rule's helper made of a node it took by value.
///
/// A rule decides whether to fire by looking through its `&mut LogicalOp`;
/// once it has decided, surgery is easier by value, and a helper that may
/// still back out hands the node back rather than a rebuilt copy of it. See
/// [`fire`].
pub(super) enum Outcome {
	/// The node was replaced.
	Rewritten(LogicalOp),
	/// The node as it came.
	Unchanged(LogicalOp),
}

/// Run `f` over the node by value and put back whatever it made, answering
/// whether that was a rewrite.
pub(super) fn fire(node: &mut LogicalOp, f: impl FnOnce(LogicalOp) -> Outcome) -> bool {
	let mut rewritten = false;
	rebuild(node, |taken| match f(taken) {
		Outcome::Rewritten(replacement) => {
			rewritten = true;
			replacement
		}
		Outcome::Unchanged(same) => same,
	});
	rewritten
}

/// A rule and the node kinds it fires on.
pub(super) struct RuleDef {
	/// For reading the table, and for naming the rule in the driver's
	/// bound assertions.
	pub(super) name: &'static str,
	/// The kinds this rule can rewrite. The driver skips it at every other
	/// node, which is both what lets a rule match only what it handles and
	/// where most of the rule set's work is avoided.
	pub(super) kinds: &'static [OpKind],
	pub(super) apply: Rule,
}

/// Replace `node` with what `f` makes of it.
///
/// The placeholder is never observed: `f` is called at once and its result
/// put back before this returns.
fn rebuild(node: &mut LogicalOp, f: impl FnOnce(LogicalOp) -> LogicalOp) {
	let taken = std::mem::replace(
		node,
		LogicalOp::Eval(crate::expr::logical::LogicalEval {
			expr: LogicalExpr::Break,
		}),
	);
	*node = f(taken);
}

/// What an expression rule may consult besides the scalar it is given.
///
/// The scalars of one node share a context, which the driver builds once per
/// node and only for a node whose scalars a rule could rewrite.
pub(super) struct ExprContext<'a> {
	/// Names the builtins that are evaluated against the row rather than
	/// against their arguments.
	pub(super) registry: &'a FunctionRegistry,
	/// What the caller may execute.
	pub(super) capabilities: &'a Capabilities,
	/// The fields whose read off the row these scalars are evaluated against
	/// is not inert, or `None` when no field read is. A read that is not
	/// inert runs a `COMPUTED` body or crosses a conditional permission, so
	/// evaluating it here would skip the machinery that guards it.
	pub(super) restricted: Option<&'a RestrictedFields>,
	/// Whether the aggregate extractor reads these scalars after binding and
	/// matches their calls by name, so a call it knows must survive whole.
	pub(super) aggregates_survive: bool,
	/// Whether these scalars are a condition, where `time::now()` is one value
	/// for the statement; see [`super::fold::is_condition`].
	pub(super) condition: bool,
}

/// One expression rewrite rule.
///
/// The driver applies it to every scalar of every node, so a rule rewrites in
/// place and answers whether it changed anything — it never has to say which
/// node kinds it applies to, nor reach the scalars itself. A rule that only
/// wants some expression shapes tests for them; the walk is the driver's.
pub(super) type ExprRule = fn(&mut LogicalExpr, &ExprContext<'_>) -> bool;

/// The expression rules, applied at every scalar of every node before the
/// node rules see it. Order affects only how many passes the fixpoint takes.
pub(super) const EXPR_RULES: &[(&str, ExprRule)] =
	&[("fold", super::fold::apply), ("control_flow", control_flow::apply)];

/// Rewrite every position of every scalar of `node`, against the schema of
/// the row that position is evaluated on: the node's row schema at the row,
/// an open schema inside a body evaluated against another value.
///
/// `candidate` says whether an expression is one `replacement` could rewrite.
/// The row schema is a copy of the source's schema with every layer's columns
/// applied, so it is built only for a node that holds a candidate. A `MATCH`
/// plan is opaque to every pass and is left as it is.
///
/// Answers whether anything was replaced.
pub(super) fn rewrite_row_positions(
	node: &mut LogicalOp,
	candidate: impl Fn(&LogicalExpr) -> bool,
	mut replacement: impl FnMut(&LogicalExpr, &Schema) -> Option<LogicalExpr>,
) -> bool {
	if matches!(node, LogicalOp::Match(_)) {
		return false;
	}
	let holds_candidate = node
		.visit_scalars(&mut |scalar| {
			if scalar.any(&candidate) {
				Err(())
			} else {
				Ok(())
			}
		})
		.is_err();
	if !holds_candidate {
		return false;
	}
	let schema = row_schema(node);
	let open = Schema::open();
	let mut changed = false;
	let Ok(()) = node.visit_scalars_mut(&mut |scalar| {
		rewrite_positions(scalar, true, &mut |e, at_row| {
			let schema = if at_row {
				&schema
			} else {
				&open
			};
			if let Some(new) = replacement(e, schema) {
				*e = new;
				changed = true;
			}
		});
		Ok::<_, std::convert::Infallible>(())
	});
	changed
}

/// The rule set, in the order the driver tries them at a node. The order
/// affects only how many passes the fixpoint takes, never what it is.
pub(super) const RULES: &[RuleDef] = &[
	control_flow::RULE,
	filter::RULE,
	layers::RULE,
	join::RULE,
	prune::RULE,
	identity::RULE,
	decorrelate::RULE,
	inline::RULE,
	kind::RULE,
];

/// The terms of `expr`'s top-level `AND` chain, in evaluation order.
pub(crate) fn conjuncts(expr: &LogicalExpr) -> Vec<&LogicalExpr> {
	fn collect<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalExpr>) {
		if let LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} = expr
		{
			collect(left, out);
			collect(right, out);
		} else {
			out.push(expr);
		}
	}
	let mut out = Vec::new();
	collect(expr, &mut out);
	out
}

/// [`conjuncts`], taking the expression apart.
pub(super) fn split_and(expr: LogicalExpr) -> Vec<LogicalExpr> {
	if let LogicalExpr::Binary {
		left,
		op: BinaryOperator::And,
		right,
	} = expr
	{
		let mut out = split_and(*left);
		out.extend(split_and(*right));
		out
	} else {
		vec![expr]
	}
}

/// The `AND` chain of `terms`, in order; `None` for no terms.
pub(super) fn and_all(terms: Vec<LogicalExpr>) -> Option<LogicalExpr> {
	terms.into_iter().reduce(and)
}

/// `left AND right`.
pub(crate) fn and(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
	LogicalExpr::Binary {
		left: Box::new(left),
		op: BinaryOperator::And,
		right: Box::new(right),
	}
}

pub(super) fn is_true(expr: &LogicalExpr) -> bool {
	matches!(expr, LogicalExpr::Literal(LogicalLiteral::Bool(true)))
}

pub(crate) fn is_false(expr: &LogicalExpr) -> bool {
	matches!(expr, LogicalExpr::Literal(LogicalLiteral::Bool(false)))
}

/// Whether `expr` holds a `MATCHES` or `KNN` operator at its own level, a
/// nested plan excluded. Either names an index of the statement it is
/// written in and is answered by that statement's access path, so a conjunct
/// holding one is never moved out of the statement's own predicate. This
/// looks everywhere in the expression — a field's own parts, an `IF`, a
/// closure body — where the planner's `expr_contains_matches` looks only
/// through operator applications and call arguments: that one asks whether
/// the enclosing scan's access path can answer the operator, this one whether
/// the conjunct belongs to its statement at all.
pub(super) fn mentions_index_operator(expr: &LogicalExpr) -> bool {
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

/// Whether `expr` names one of the row-scoped parameters `names` anywhere, as
/// a parameter or as a `FROM $param` source, nested plans included. Moving
/// such an expression into or out of a nested plan would rebind the name.
pub(super) fn mentions_row_scoped_deep(expr: &LogicalExpr, names: &[&str]) -> bool {
	let mut found = false;
	let _ = expr.visit(&mut |e| {
		if let LogicalExpr::Param(param) = e {
			if names.contains(&param.as_str()) {
				found = true;
			}
		} else if let LogicalExpr::Subquery(plan) = e {
			visit_deep(plan, &mut |node| {
				if let LogicalOp::Source(LogicalSource {
					source: SourceRef::Param(param),
					..
				}) = node && names.contains(&param.as_str())
				{
					found = true;
				}
				let Ok(()) = node.visit_scalars(&mut |scalar| {
					if mentions_row_scoped_deep(scalar, names) {
						found = true;
					}
					Ok::<_, std::convert::Infallible>(())
				});
			});
		}
		if found {
			ControlFlow::Break(())
		} else {
			ControlFlow::Continue(())
		}
	});
	found
}

/// Whether `join` is a semi or anti join in a permission frame — one lifted
/// out of a `PERMISSIONS FOR select` predicate — which is a barrier as a
/// [`LogicalOp::PermissionFilter`] is.
pub(super) fn is_permission_join(join: &LogicalJoin) -> bool {
	join.frame.mode != EvalMode::Caller && matches!(join.kind, JoinKind::Semi | JoinKind::Anti)
}

/// Whether `node` is a barrier no user expression moves below: a permission
/// filter or a permission join.
pub(super) fn is_permission_node(node: &LogicalOp) -> bool {
	match node {
		LogicalOp::PermissionFilter(_) => true,
		LogicalOp::Join(join) => is_permission_join(join),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Write(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Filter(_)
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
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Info(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::Sleep(_) => false,
	}
}

/// The schema of the row the scalars of `node` are evaluated against: its
/// one input's for an operator over rows. Open for a join, whose keys read two
/// rows, for a leaf, and for a statement operator, whose expressions read the
/// row of whatever statement holds it.
pub(super) fn row_schema(node: &LogicalOp) -> Schema {
	match node {
		LogicalOp::Filter(_)
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
		| LogicalOp::Only(_) => node.children().first().map_or_else(Schema::open, LogicalOp::schema),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Write(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Info(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::Sleep(_) => Schema::open(),
	}
}

/// The index of the first input of `expr` that is a *body*: an expression
/// evaluated against a value other than the one `expr` itself is evaluated
/// against — the element a broadcast maps over, the element a `[WHERE]`
/// filter judges, the value an optional continues from, each depth of a
/// recursion, the value a destructure alias reads, the edge rows a lookup's
/// clauses see, a closure's arguments. `None` when every input is evaluated
/// against the same value as `expr`. Indexes are into
/// [`LogicalExpr::children`].
fn body_inputs_from(expr: &LogicalExpr) -> Option<usize> {
	match expr {
		LogicalExpr::Filter {
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
		| LogicalExpr::Broadcast {
			..
		} => Some(1),
		LogicalExpr::Closure(_) => Some(0),
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
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::RepeatRecurse(_)
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
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => None,
	}
}

/// Walks `expr` in pre-order, telling `f` at each position whether it is
/// evaluated against the same value as `expr` itself (`true`) or inside a body
/// evaluated against another value (`false`; see [`body_inputs_from`]). A
/// nested plan is not entered, and neither is a closure body: the closure
/// literal is visited, what it holds is not, since a closure body is bound by
/// no rule — it resolves against the row, catalog and transaction of the
/// statement that invokes the closure ([`super::visit_binding`]). `at_row` is
/// the answer for `expr` itself.
pub(super) fn visit_positions<B>(
	expr: &LogicalExpr,
	at_row: bool,
	f: &mut impl FnMut(&LogicalExpr, bool) -> ControlFlow<B>,
) -> ControlFlow<B> {
	// The recursion is over `dyn FnMut` rather than the caller's closure type:
	// a generic function that handed itself a fresh closure at every level
	// would have no monomorphic form.
	fn walk<B>(
		expr: &LogicalExpr,
		at_row: bool,
		f: &mut dyn FnMut(&LogicalExpr, bool) -> ControlFlow<B>,
	) -> ControlFlow<B> {
		f(expr, at_row)?;
		if matches!(expr, LogicalExpr::Closure(_)) {
			return ControlFlow::Continue(());
		}
		let bodies_from = body_inputs_from(expr);
		let mut position = 0;
		let mut flow = ControlFlow::Continue(());
		expr.for_each_child(&mut |child| {
			if flow.is_continue() {
				let child_at_row = at_row && bodies_from.is_none_or(|from| position < from);
				flow = walk(child, child_at_row, &mut *f);
			}
			position += 1;
		});
		flow
	}
	walk(expr, at_row, f)
}

/// Rewrites `expr` in place, bottom-up, telling `f` at each position whether
/// it is evaluated against the same value as `expr` itself; see
/// [`visit_positions`]. A nested plan is not entered, and a closure body is
/// left as written.
pub(super) fn rewrite_positions(
	expr: &mut LogicalExpr,
	at_row: bool,
	f: &mut impl FnMut(&mut LogicalExpr, bool),
) {
	if !matches!(expr, LogicalExpr::Closure(_)) {
		let bodies_from = body_inputs_from(expr);
		for (i, child) in expr.children_mut().into_iter().enumerate() {
			let child_at_row = at_row && bodies_from.is_none_or(|from| i < from);
			rewrite_positions(child, child_at_row, f);
		}
	}
	f(expr, at_row);
}

/// Whether evaluating `expr` twice yields the same value as evaluating it
/// once, with no effect either way: it only reads, and every call in it is a
/// builtin that answers the same for the same arguments and the same row. A
/// user-defined, scripted, module, silo or model call, a method, a closure, an
/// applied value, a nested plan and control flow are not; nor is a `MATCHES`
/// or `KNN`, which is answered by an access path rather than by evaluation. A
/// rule that duplicates or drops an expression — substituting it for every
/// occurrence of a name — requires this. The read-only half is the expression
/// layer's own classification; the rest is [`Effects::impure`](crate::expr::logical::Effects) and
/// [`Effects::index_operator`](crate::expr::logical::Effects), read from the declared effects of
/// the builtins the expression calls.
pub(super) fn is_pure(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	if !expr.read_only() {
		return false;
	}
	effects_of(expr, &Oracle::new(registry)).pure()
}

/// Whether evaluating `expr` a second time, or before rather than after
/// another expression of the same row, leaves the world as evaluating it once
/// in written order would: it stages no write, advances no counter, reaches
/// nothing outside the transaction, waits for nothing and runs no body the
/// plan cannot see. This is what lets an operator evaluate a batch one column
/// at a time, and evaluate a column again after it failed part-way.
///
/// Reading the transaction's own snapshot is all of this, because the snapshot
/// answers the same however often it is asked. **A record dereference is not a
/// read of that kind**: fetching the record derives its `COMPUTED` fields and
/// judges its `PERMISSIONS`, which are bodies the plan cannot see, so what the
/// fetch leaves behind is whatever those bodies do. A body drawing from a
/// sequence advances it once per fetch, and `SELECT link.n AS a, link.n AS b`
/// then reports which column was evaluated first rather than what the row
/// holds. A `[WHERE …]` filter fetches the same way, because its predicate is
/// evaluated with each element of the base as the current value.
///
/// [`Effects::impure`](crate::expr::logical::Effects) is where that hazard is
/// stated, alongside the shapes whose two evaluations can answer differently at
/// all — a method, a closure, an applied value, control flow, a nested
/// statement, a generated record-id key. A nested statement is rejected whole:
/// [`effects_of`] counts it as one opaque node and does not enter it, so what
/// its own scalars do is not in the answer, and only the arm naming
/// `Subquery` keeps that from being read as inertness.
///
/// [`is_pure`] is what a rule needs before it may keep one of two evaluations
/// in place of both. It rests on the same read-only classification and the same
/// `impure`, and differs in turning down a `MATCHES` or `KNN` — answered by an
/// access path rather than by evaluation, so a rule may not move it, while an
/// operator choosing the shape of its own batch is free to. That relation is
/// [`Effects::pure`](crate::expr::logical::Effects), which is defined in terms
/// of [`Effects::effect_free`](crate::expr::logical::Effects) so it holds by
/// construction.
pub(crate) fn is_effect_free(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	if !expr.read_only() {
		return false;
	}
	effects_of(expr, &Oracle::new(registry)).effect_free()
}

/// Whether evaluating `expr` can end in `RETURN`, `BREAK` or `CONTINUE`
/// reaching the caller in place of a value.
///
/// An operator that evaluates a column for a whole batch learns only that the
/// call ended in a signal, not which row raised it, so it evaluates the column
/// again a row at a time to find out — which repeats whatever the rows it did
/// reach already did. Where no node in the column can produce a signal at all,
/// that second evaluation is unreachable, and the column may be evaluated as a
/// batch however much it dereferences.
///
/// The signal comes from a `RETURN` in a body the column encloses, so the
/// nodes that can carry one are the bare signals themselves and every node
/// whose body the expression does not contain: a nested statement, a call, a
/// closure, a method, an applied value. A graph or reference lookup is not
/// among them — its plan turns a signal reaching the traversal into an error —
/// and neither is any other path step, operator or literal.
pub(crate) fn may_signal_control_flow(expr: &LogicalExpr) -> bool {
	expr.visit(&mut |e| match e {
		// A signal of its own, or a body the expression does not contain and
		// so cannot be read for one.
		LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_)
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Postfix {
			..
		} => ControlFlow::Break(()),
		// Everything the expression evaluates itself: a value, a read, a
		// path step, an operator. `Throw` raises rather than signals, and a
		// lookup's plan turns a signal reaching the traversal into an error.
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
		| LogicalExpr::Binary {
			..
		}
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_) => ControlFlow::Continue(()),
	})
	.is_break()
}

/// Whether evaluating `expr` may raise, judged without the bind pass's
/// parameter classification: every parameter a bound plan still carries and
/// that is not answered from the row is taken to be a fetch that may deny the
/// caller. This is [`plan_may_raise`]'s treatment of the same question, for a
/// caller holding one expression rather than an operator.
///
/// The names answered from the row are `$this`, `$self` and `$parent`. `$value`
/// is not among them: a layer binds it for the layer's own columns, and this
/// judges a statement's columns — a projection, a sort key, an aggregate
/// argument — where it is an ordinary name, so reading it fetches a
/// `DEFINE PARAM` whose `PERMISSIONS` may deny the caller.
fn column_may_raise(expr: &LogicalExpr) -> bool {
	let mut checked: HashSet<String> = HashSet::new();
	let _ = expr.visit(&mut |e| {
		if let LogicalExpr::Param(param) = e
			&& !crate::exec::planner::util::SELECT_ITERATION_PARAMS.contains(&param.as_str())
		{
			checked.insert(param.to_string());
		}
		ControlFlow::<()>::Continue(())
	});
	may_raise(expr, &checked)
}

/// What an operator does with a column whose batch call failed part-way,
/// which decides whether column-major evaluation can repeat one.
///
/// `evaluate_batch` reports a failure for the batch, not for the row that
/// caused it, so an operator that needs to know which row it was evaluates
/// the column again from its first row — repeating whatever the rows it did
/// reach already did.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum BatchRetry {
	/// Nothing is repeated: the failure is passed on as the batch's.
	None,
	/// The column is evaluated again when the call ended in a signal, so
	/// that the row signalling `RETURN` yields the value it returned.
	OnSignal,
	/// The column is evaluated again on any failure, so that a row whose
	/// error is ignorable yields `NONE` rather than ending the statement.
	OnFailure,
}

/// What [`columns_may_run_column_major`] reads of one column, judged from the
/// expression as written and carried where the expression itself is not — a
/// registry holds only the compiled form.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ColumnEffects {
	/// [`is_effect_free`]: whether the column has an effect to reorder or
	/// repeat at all.
	pub(crate) effect_free: bool,
	/// [`may_signal_control_flow`].
	pub(crate) may_signal: bool,
	/// [`column_may_raise`].
	pub(crate) may_raise: bool,
}

impl ColumnEffects {
	/// What a column made of `expr` does.
	pub(crate) fn of(expr: &LogicalExpr, registry: &FunctionRegistry) -> Self {
		Self {
			effect_free: is_effect_free(expr, registry),
			may_signal: may_signal_control_flow(expr),
			may_raise: column_may_raise(expr),
		}
	}

	/// A column that does nothing and cannot fail: `SELECT *`, whose value
	/// the row already holds.
	pub(crate) const INERT: Self = Self {
		effect_free: true,
		may_signal: false,
		may_raise: false,
	};
}

/// Whether a batch of rows may be evaluated one column at a time — each
/// column for the whole batch before the next — rather than a row at a time
/// with the columns in written order.
///
/// A column at a time is the faster shape: it is what lets a column of record
/// dereferences overlap its rows. What it changes is the order two columns'
/// effects happen in, and whether a column that failed part-way is evaluated
/// again; [`is_effect_free`] states which columns have an effect to reorder,
/// and `retry` what the operator would repeat.
///
/// Neither is reachable when **at most one column has effects**:
///
/// - With no effectful column there is no effect to reorder or repeat, whatever the columns do
///   otherwise. This is the shape a projection of plain reads and arithmetic has, and the only one
///   admitted before.
/// - With exactly one, there is no second effect for it to reorder against. What remains is that
///   another column raising could stop it before a row it would have reached in written order, so
///   the others must be total as well as effect-free; and that the operator could evaluate it
///   twice, so whatever `retry` repeats on must be out of its reach.
///
/// Two effectful columns are declined: `SELECT link.n AS a, other.n AS b`
/// would report the two fetches in the order the batch ran them rather than
/// the order the statement writes them.
///
/// What this does **not** answer is the order the rows of one column are
/// evaluated in. A batch call is free to overlap them, and a graph or
/// reference traversal does: [`crate::exec::fan_out`] is where that decision
/// lives, and it is gated on the work's [`AccessMode`](crate::exec::AccessMode)
/// and an empty recursion context, not on anything here. So a column of
/// traversals derives its targets' `COMPUTED` fields in whatever order the
/// scheduler runs the rows in, and on a failure the rows already in flight
/// finish — which is true of a single row's traversal as well, since the graph
/// scan overlaps the sources within one lookup. Admitting a column to a batch
/// call subjects it to that contract; it does not create it.
pub(crate) fn columns_may_run_column_major(columns: &[ColumnEffects], retry: BatchRetry) -> bool {
	let mut effectful = None;
	for (i, column) in columns.iter().enumerate() {
		if column.effect_free {
			continue;
		}
		if effectful.is_some() {
			return false;
		}
		effectful = Some(i);
	}
	let Some(effectful) = effectful else {
		return true;
	};
	let column = columns[effectful];
	let repeat_is_out_of_reach = match retry {
		BatchRetry::None => true,
		BatchRetry::OnSignal => !column.may_signal,
		BatchRetry::OnFailure => !column.may_signal && !column.may_raise,
	};
	repeat_is_out_of_reach
		&& columns.iter().enumerate().all(|(i, c)| i == effectful || !c.may_raise)
}

/// Whether every row a value source built from `expr` materialises into
/// itself.
///
/// A source materialises each row it yields, and a record id among them names
/// a record that is fetched: its `COMPUTED` fields are derived and its
/// `PERMISSIONS` judged, either of which can raise. That evaluation is the
/// source's own, so it outlives a filter no row survives, and a rewrite that
/// drops the source drops it.
///
/// Only a literal, or a collection of them, answers `true`. A value whose
/// shape the run decides could be a record id, and a source is the one place
/// where guessing wrong drops an evaluation rather than merely keeping one —
/// the shapes this turns down (`WHERE false` over a parameter or a call) cost
/// nothing to leave standing.
pub(super) fn source_rows_are_inert(expr: &LogicalExpr) -> bool {
	!may_dereference(expr)
}

/// Whether evaluating `expr` may raise an error or signal control flow, so
/// that a rule dropping it unevaluated may change what a statement reports:
/// [`Effects::may_raise`](crate::expr::logical::Effects), which states what the evaluators answer
/// for every input and what may not.
///
/// A parameter is inert where its value is in hand before the plan runs, and
/// raises where reading it fetches a `DEFINE PARAM` whose `PERMISSIONS`
/// clause denies the caller; `permission_checked` names the second kind
/// ([`RuleContext::permission_checked_params`]).
pub(super) fn may_raise(expr: &LogicalExpr, permission_checked: &HashSet<String>) -> bool {
	effects_of(expr, &ParamsOnly(permission_checked)).may_raise
}

/// Whether any expression `op` evaluates can raise, judged without the bind
/// pass's parameter classification.
///
/// [`may_raise`] is told which parameter reads fetch a `DEFINE PARAM`
/// ([`RuleContext::permission_checked_params`]), and only binding holds that
/// set. The physical planner asks the same question of a subtree when it
/// would choose an access path that evaluates the subtree on fewer rows than
/// the plan as bound, so it needs the answer without the set. A parameter a
/// bound plan still carries is either bound per row — answered from the row
/// before any catalog read — or one binding left to runtime, which is the
/// permission-checked case; every other name is taken here to be a fetch.
/// The names bound per row are `$this`, `$self` and `$parent` everywhere, and
/// `$value` in a layer's own columns, which the layer binds.
pub(crate) fn plan_may_raise(op: &LogicalOp) -> bool {
	// `$this`, `$self` and `$parent` are answered from the row wherever they
	// appear; `$value` is bound by a layer for its own columns and is an
	// ordinary name anywhere else.
	let row_scoped: &[&str] = if op.is_layer() {
		ROW_PARAMS
	} else {
		crate::exec::planner::util::SELECT_ITERATION_PARAMS
	};
	let mut checked: HashSet<String> = HashSet::new();
	for scalar in op.scalars() {
		let _ = scalar.visit(&mut |e| {
			if let LogicalExpr::Param(param) = e
				&& !row_scoped.contains(&param.as_str())
			{
				checked.insert(param.to_string());
			}
			ControlFlow::Continue::<()>(())
		});
	}
	op.scalars().into_iter().any(|scalar| may_raise(scalar, &checked))
		|| op.children().iter().any(plan_may_raise)
}

/// Whether `expr` reads nothing of the value it is evaluated against — no
/// field, not the document, not the current value, no row parameter, no
/// lookup from the row, no nested plan, no call evaluated against the row —
/// so that it means the same at any position of any expression: the negation
/// of [`Effects::reads_row`](crate::expr::logical::Effects).
pub(super) fn is_row_free(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	!effects_of(expr, &Oracle::new(registry)).reads_row
}

/// The root field of every read `expr` makes of the row it is evaluated
/// against, at any depth (a read inside a body over another value is
/// included: it over-approximates, which is the safe direction for a demand).
/// `None` when the row is read other than by name — see
/// [`reads::reads_row_opaquely`] — through a closure, whose body runs against
/// whatever row it is later invoked with, or through a step applied to the
/// row's own `id`, which dereferences the record and so reads it whole.
pub(crate) fn named_reads(
	expr: &LogicalExpr,
	registry: &FunctionRegistry,
) -> Option<std::collections::HashSet<String>> {
	if reads::reads_row_opaquely(expr, registry)
		|| expr.any(|e| {
			matches!(e, LogicalExpr::Closure(_))
				|| e.step_base().is_some_and(
					|base| matches!(base, LogicalExpr::Field(name) if name.as_str() == "id"),
				)
		}) {
		return None;
	}
	let mut out = std::collections::HashSet::new();
	let _ = expr.visit(&mut |e| {
		if let LogicalExpr::Field(name) = e {
			out.insert(name.to_string());
		}
		ControlFlow::<()>::Continue(())
	});
	Some(out)
}

#[cfg(test)]
pub(super) mod test_support {
	//! The driver over a plan with no caller, no catalog and no functions,
	//! for the golden tests of each rule.

	use std::collections::HashSet;
	use std::sync::Arc;

	use surrealdb_strand::TableName;

	use super::super::functions::Functions;
	use super::super::lower_test;
	use super::super::optimise::optimise;
	use super::RuleContext;
	use crate::exec::function::FunctionRegistry;
	use crate::exec::planner::util::SELECT_ITERATION_PARAMS;
	use crate::expr::logical::{LogicalOp, LogicalSource, Schema, SourceRef};

	/// The fixpoint of the rule set over `op` with no functions to inline.
	pub(in crate::exec::logical) fn rewritten(op: LogicalOp) -> LogicalOp {
		rewritten_with(op, &Functions::none())
	}

	/// [`rewritten`] with `functions` available to the inlining rule.
	pub(in crate::exec::logical) fn rewritten_with(
		op: LogicalOp,
		functions: &Functions,
	) -> LogicalOp {
		rewritten_in(op, functions, &HashSet::new())
	}

	/// [`rewritten`] with `permission_checked` naming the parameters a read of
	/// which fetches a `DEFINE PARAM` and is judged by its `PERMISSIONS`
	/// clause.
	pub(in crate::exec::logical) fn rewritten_checking(
		op: LogicalOp,
		permission_checked: &[&str],
	) -> LogicalOp {
		let permission_checked: HashSet<String> =
			permission_checked.iter().map(|name| (*name).to_owned()).collect();
		rewritten_in(op, &Functions::none(), &permission_checked)
	}

	fn rewritten_in(
		op: LogicalOp,
		functions: &Functions,
		permission_checked: &HashSet<String>,
	) -> LogicalOp {
		let registry = FunctionRegistry::with_builtins();
		let capabilities = crate::dbs::capabilities::Capabilities::all();
		let mut cx = RuleContext {
			row_scoped: SELECT_ITERATION_PARAMS,
			registry: &registry,
			functions,
			permission_checked_params: permission_checked,
			next_alias: 0,
			capabilities: &capabilities,
			folds_clock: true,
		};
		optimise(op, &mut cx)
	}

	/// [`rewritten`] over the unbound plan of `sql`, rendered.
	pub(in crate::exec::logical) fn rewritten_sql(sql: &str) -> String {
		rewritten(lower_test(sql)).to_string()
	}

	/// `op` with every source of table `table` resolved to `schema`.
	pub(in crate::exec::logical) fn resolved(
		op: LogicalOp,
		table: &str,
		schema: &Schema,
	) -> LogicalOp {
		let table = TableName::from(table);
		let Ok(op) = op.transform_up(&mut |mut node| {
			if let LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(name),
				schema: slot,
				..
			}) = &mut node
				&& *name == table
			{
				*slot = Some(Arc::new(schema.clone()));
			}
			Ok::<_, std::convert::Infallible>(node)
		});
		op
	}

	/// The operator names down the first-input spine of `op`.
	pub(in crate::exec::logical) fn spine(op: &LogicalOp) -> Vec<&'static str> {
		let mut out = vec![op.name()];
		let mut current = op;
		while let Some(child) = current.children().first() {
			out.push(child.name());
			current = child;
		}
		out
	}
}

#[cfg(test)]
mod tests {
	use super::super::lower_test as lower;
	use super::*;
	use crate::expr::Kind;
	use crate::expr::logical::{
		EvalFrame, LogicalField, LogicalFields, LogicalFilter, LogicalPostfixOperator,
		LogicalProject, LogicalRecordIdKey, OutputPath,
	};

	/// A projection over `SELECT * FROM t`, a layer when `frame` is not the
	/// caller's.
	fn project(expr: LogicalExpr, frame: EvalFrame) -> LogicalOp {
		let field = if frame.is_caller() {
			LogicalField::Single {
				expr,
				alias: Some(OutputPath::field("c")),
				kind: None,
				frame,
			}
		} else {
			LogicalField::column("c", expr, Some(Kind::Int), frame)
		};
		LogicalOp::Project(LogicalProject {
			input: Box::new(lower("SELECT * FROM t")),
			fields: LogicalFields::Select(vec![LogicalField::All, field]),
			omit: vec![],
		})
	}

	fn param(name: &str) -> LogicalExpr {
		LogicalExpr::Param(name.to_owned().into())
	}

	#[test]
	fn plan_may_raise_reads_a_parameter_as_a_fetch_unless_the_row_binds_it() {
		// A plain read of the row raises nothing.
		assert!(!plan_may_raise(&project(LogicalExpr::Field("a".into()), EvalFrame::caller())));
		// A cast does, wherever it sits.
		let cast = LogicalExpr::Prefix {
			op: crate::expr::PrefixOperator::Cast(Kind::Int),
			expr: Box::new(LogicalExpr::Field("a".into())),
		};
		assert!(plan_may_raise(&project(cast, EvalFrame::caller())));

		// A layer binds `$value` for its own columns, so reading it there is
		// no catalog fetch; every other name is taken to be one, since the
		// bind pass's classification is not available here.
		assert!(!plan_may_raise(&project(param("value"), EvalFrame::computed(None))));
		assert!(plan_may_raise(&project(param("value"), EvalFrame::caller())));
		assert!(plan_may_raise(&project(param("secret"), EvalFrame::computed(None))));
		for name in ["this", "self", "parent"] {
			assert!(!plan_may_raise(&project(param(name), EvalFrame::caller())), "${name}");
		}
	}

	/// A method call dispatches on its receiver and an application runs a
	/// body the expression does not hold, so neither is pure. A registered
	/// builtin method is read-only, which is what lets it reach the match at
	/// all.
	#[test]
	fn a_method_call_and_an_application_are_not_pure() {
		let registry = FunctionRegistry::with_builtins();
		let receiver =
			|| Box::new(LogicalExpr::Array(vec![LogicalExpr::Literal(LogicalLiteral::Integer(1))]));

		for name in ["shuffle", "len"] {
			let method = LogicalExpr::Method {
				base: receiver(),
				name: name.to_owned().into(),
				args: vec![],
			};
			assert!(method.read_only(), ".{name}() reads only");
			assert!(!is_pure(&method, &registry), "method .{name}()");
		}

		// Applying a value runs its body.
		let call = LogicalExpr::Invoke {
			target: receiver(),
			args: vec![],
		};
		assert!(!is_pure(&call, &registry));

		// A range postfix only wraps its operand, and stays inert.
		let range = LogicalExpr::Postfix {
			expr: Box::new(LogicalExpr::Literal(LogicalLiteral::Integer(1))),
			op: LogicalPostfixOperator::Range,
		};
		assert!(is_pure(&range, &registry));
	}

	#[test]
	fn a_generated_record_id_key_is_not_pure() {
		use std::ops::Bound;

		use crate::expr::RecordIdKeyGen;
		use crate::expr::logical::{LogicalRecordIdKeyRange, LogicalRecordIdLit};

		let registry = FunctionRegistry::with_builtins();
		let id = |key: LogicalRecordIdKey| {
			LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
				table: "t".into(),
				key,
			}))
		};

		// A written-down key is the same record every time.
		assert!(is_pure(&id(LogicalRecordIdKey::Number(1)), &registry));
		assert!(is_pure(&id(LogicalRecordIdKey::String("a".into())), &registry));

		// A generated one is not, wherever it sits — including a range
		// bound, which no expression walk reaches.
		for generated in [RecordIdKeyGen::Rand, RecordIdKeyGen::Ulid, RecordIdKeyGen::Uuid] {
			let label = format!("{generated:?}");
			let key = LogicalRecordIdKey::Generate(generated);
			assert!(!is_pure(&id(key), &registry), "{label}");
		}
		let range = LogicalRecordIdKey::Range(Box::new(LogicalRecordIdKeyRange {
			start: Bound::Included(LogicalRecordIdKey::Number(1)),
			end: Bound::Excluded(LogicalRecordIdKey::Generate(RecordIdKeyGen::Ulid)),
		}));
		assert!(!is_pure(&id(range), &registry));
		let range = LogicalRecordIdKey::Range(Box::new(LogicalRecordIdKeyRange {
			start: Bound::Included(LogicalRecordIdKey::Number(1)),
			end: Bound::Excluded(LogicalRecordIdKey::Number(5)),
		}));
		assert!(is_pure(&id(range), &registry));
	}

	#[test]
	fn a_field_read_off_a_record_id_is_not_pure() {
		use crate::expr::logical::LogicalRecordIdLit;

		let registry = FunctionRegistry::with_builtins();
		let access = |base: LogicalExpr| LogicalExpr::Access {
			base: Box::new(base),
			field: "coin".into(),
		};

		// The fetch derives the record's `COMPUTED` fields, whose bodies this
		// expression does not hold and need not answer the same twice.
		let rid = LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
			table: "cfg".into(),
			key: LogicalRecordIdKey::String("one".into()),
		}));
		assert!(!is_pure(&access(rid), &registry));

		// A field may hold a record link, so a read off one may fetch too.
		assert!(!is_pure(&access(LogicalExpr::Field("link".into())), &registry));

		// A row-scoped name denotes the row in hand, which is not always a
		// record: a source can yield bare record ids, and a second read off
		// one fetches again. The exemption that reads such a base as a row
		// already materialised belongs to `may_raise`, which asks whether an
		// evaluation can be dropped, not to purity, which licenses keeping one
		// evaluation in place of two.
		assert!(!is_pure(
			&access(LogicalExpr::Param(crate::expr::Param::from("parent".to_string()))),
			&registry
		));

		// `_join<n>` is not the tell it looks like: the name is reserved so a
		// join mints a *different* index when a plan already carries it, which
		// leaves the field free to be a user's, holding a link.
		assert!(!is_pure(&access(LogicalExpr::Field("_join0".into())), &registry));
	}

	#[test]
	fn a_field_read_off_a_record_id_is_not_effect_free() {
		use crate::expr::logical::LogicalRecordIdLit;

		let registry = FunctionRegistry::with_builtins();
		let access = |base: LogicalExpr| LogicalExpr::Access {
			base: Box::new(base),
			field: "coin".into(),
		};

		// The same four bases `a_field_read_off_a_record_id_is_not_pure`
		// builds. A batch shape is judged by this predicate rather than by
		// `is_pure`, so the hazard has to be turned down here too: the fetch
		// derives `COMPUTED` fields whose bodies the plan cannot see, and
		// evaluating one column of fetches before another reorders them.
		let rid = LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
			table: "cfg".into(),
			key: LogicalRecordIdKey::String("one".into()),
		}));
		assert!(!is_effect_free(&access(rid), &registry));
		assert!(!is_effect_free(&access(LogicalExpr::Field("link".into())), &registry));
		assert!(!is_effect_free(
			&access(LogicalExpr::Param(crate::expr::Param::from("parent".to_string()))),
			&registry
		));
		assert!(!is_effect_free(&access(LogicalExpr::Field("_join0".into())), &registry));

		// A literal base cannot name a record, so nothing is fetched.
		assert!(is_effect_free(
			&access(LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Integer(1))),
			&registry
		));
	}

	#[test]
	fn a_terminal_filter_over_a_link_is_not_effect_free() {
		let registry = FunctionRegistry::with_builtins();
		let filter = |base: LogicalExpr| LogicalExpr::Filter {
			base: Box::new(base),
			predicate: Box::new(LogicalExpr::Field("n".into())),
		};

		// `links[WHERE n]` evaluates the predicate with each element of
		// `links` as the current value, so `n` reads off that element and an
		// element holding a record id is fetched. The predicate names the
		// field directly rather than through a step whose base is the element,
		// so nothing inside it shows the fetch — only the filter node does.
		assert!(!is_effect_free(&filter(LogicalExpr::Field("links".into())), &registry));
		assert!(!is_pure(&filter(LogicalExpr::Field("links".into())), &registry));

		// A literal base holds no record id to fetch.
		assert!(is_effect_free(
			&filter(LogicalExpr::Array(vec![LogicalExpr::Literal(
				crate::expr::logical::LogicalLiteral::Integer(1)
			)])),
			&registry
		));
	}

	#[test]
	fn purity_is_effect_freedom_plus_the_index_operators() {
		use crate::expr::logical::{EffectOracle, Effects, effects_of};

		// `is_pure` and `is_effect_free` share a basis, and the only term that
		// separates them is `index_operator`. Stating it here keeps a future
		// `node_set` arm from making `is_pure` admit what `is_effect_free`
		// refuses.
		let registry = FunctionRegistry::with_builtins();
		let oracle = Oracle::new(&registry);
		let matches = LogicalExpr::Binary {
			left: Box::new(LogicalExpr::Field("body".into())),
			op: crate::expr::BinaryOperator::Matches(crate::expr::operator::MatchesOperator {
				rf: None,
				operator: crate::expr::operator::BooleanOperator::And,
			}),
			right: Box::new(LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::String(
				"x".into(),
			))),
		};
		let effects: Effects = effects_of(&matches, &oracle as &dyn EffectOracle);
		assert!(effects.effect_free());
		assert!(!effects.pure());
		assert!(is_effect_free(&matches, &registry));
		assert!(!is_pure(&matches, &registry));
	}

	#[test]
	fn plan_may_raise_looks_through_the_whole_plan() {
		let cast = LogicalExpr::Prefix {
			op: crate::expr::PrefixOperator::Cast(Kind::Int),
			expr: Box::new(LogicalExpr::Field("a".into())),
		};
		let inner = project(cast, EvalFrame::caller());
		let outer = LogicalOp::Filter(LogicalFilter {
			input: Box::new(inner),
			predicate: LogicalExpr::Field("b".into()),
		});
		assert!(plan_may_raise(&outer));
	}
}
