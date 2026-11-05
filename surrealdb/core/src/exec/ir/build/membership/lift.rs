//! Recognising the memberships of a filter that run once as a join: a conjunct
//! `x IN (SELECT VALUE k FROM t WHERE c)` asks, for each row, whether some row of `t` admitted by
//! `c` has `k = x`. The subquery is built once and every row probes that one relation, with `c`
//! split into the parts the probe can use: a conjunct `r = $parent.p`, a field of `t` against a
//! field of the outer row, is a further key; any other conjunct that reads the outer row through
//! `$parent` is evaluated per candidate pair; a conjunct that reads only `t`'s row filters the
//! relation once. `x NOT IN (…)` is the anti form, and `(…) CONTAINS x` and `(…) CONTAINSNOT x` are
//! the same predicates written the other way round. The keys match by SurrealQL's `=`, as `IN` and
//! `CONTAINS` compare elements, so the two forms admit the same rows.
//!
//! [`lifted`] reads a statement and answers it with each membership it recognises written as the
//! semi or anti join of its shape; the unlift of this module's parent then writes every such join
//! as the membership filter the statement binds, so a join a front-end hands over and one
//! recognised here are bound the same way.
//!
//! # What is lifted
//!
//! A conjunct of a [`LogicalOp::Filter`] or [`LogicalOp::PermissionFilter`] predicate, a term of
//! its top-level `AND` chain, is lifted when every one of these holds:
//!
//! - it is `x IN s`, `x NOT IN s`, `s CONTAINS x` or `s CONTAINSNOT x`, `s` a scalar subquery;
//! - `s` is `Project[VALUE k]` with no `OMIT`, over an optional `Filter[c]`, over a table source
//!   alone or under its permission filter, layers, semi or anti joins and filters: the shape
//!   `SELECT VALUE k FROM t WHERE c` resolves to, with the memberships of its own permission and
//!   filter already lifted;
//! - every scalar of that right pipeline is pure ([`is_pure`]): the probe builds it once and every
//!   outer row reads that one relation, where the per-row form builds a fresh one for each, so only
//!   a right side that answers the same each time makes the two agree;
//! - `x` and `k` are built from literals, parameters that are not row-scoped, field reads, `.field`
//!   and `[index]` access, `[0]`, `[$]`, containers, unary and binary operators and `IF`; `k` may
//!   not read the outer row and `x` may not read a row-scoped parameter, and `k` is pure for the
//!   same reason the right pipeline is;
//! - every conjunct of `c` that reads the outer row does so as `$parent.path`, is built from the
//!   same forms, names no row-scoped parameter other than `$parent`, and cannot raise: a key is
//!   evaluated for every probe row whatever the build side holds, and a residual only for the pairs
//!   the keys matched, where the per-row form evaluates the whole condition once per right row per
//!   outer row. Every other conjunct of `c` names no row-scoped parameter, in a nested plan
//!   included, and is pure: it filters the build side, so it too decides the one relation every
//!   outer row sees;
//! - the probe side is read-only: the predicate whole, the lifted conjuncts included, and the
//!   pipeline the filter stands on. The right side's purity says the relation is the same each time
//!   it is read; this says nothing on the left changes it between the reads. A write on the probe
//!   side does: the per-row form reads the relation the writes so far have left, and the probe
//!   reads it once for all of them;
//! - lifted out of a `PermissionFilter`, it reads no computed field of the table off the outer row,
//!   at the outer position or through `$parent`. The rows reach the filter as stored, carrying no
//!   computed field, and the filter judges its predicate against the row with the computed fields
//!   it reads derived; a join above the filter would read the field as `NONE`, an anti join then
//!   admitting the rows the predicate was written to deny;
//! - where the probe rows are not objects, the conjunct leaves no residual: a residual reads the
//!   outer row through the join's row, which holds the alias alone where the probe row is no
//!   object, and a field read off a record id fetches the record.
//!
//! # What is not lifted
//!
//! Anything else stays a scalar subquery evaluated per row: a subquery with a `LIMIT`, `ORDER BY`,
//! `GROUP BY`, `SPLIT`, `FETCH` or a `VALUE` over a non-table source; a right side that answers
//! differently each time it is evaluated, such as one filtered by `rand::bool()`; a correlation
//! that can raise, such as `k = <int> $parent.bad`, which the per-row form never evaluates over an
//! empty right side; a correlation through a function argument, a closure, a graph lookup, a method
//! or a nested plan; a bare `$parent`, or `$this` / `$self` anywhere; a `MATCHES` or `KNN` operator
//! in the predicate, the value or the subquery's filter, which names an index of the statement it
//! is written in and is answered by that statement's access path; a scalar subquery in array or
//! comparison position, a `(SELECT … LIMIT 1)` scalar, and a graph hop. The per-row form is what
//! defines the result of each, and the lift never changes the rows a statement returns.
//!
//! The terms of a predicate keep their order around a lifted conjunct where that order is
//! observable. `AND` short-circuits left to right, so a term written before a lifted conjunct is
//! judged for every row whatever the membership answers, and stands beneath the join, where an
//! index the pipeline chose for it still serves the probe side. A term written after is judged only
//! for the rows the membership kept: a term that can raise stands between the joins it was written
//! between, and so does every term after a lifted conjunct that can raise itself, since a join
//! builds its right side only once its probe yields a row. A term that cannot raise answers the
//! same wherever it stands and sinks beneath every join.
//!
//! The memberships of a statement are lifted bottom-up, plans nested in its expressions included,
//! so the subquery a conjunct lifts has had its own memberships lifted first. A join lifted out of
//! a `PermissionFilter` carries the permission frame and its `AUTH LIMIT`.

use std::collections::HashSet;
use std::ops::ControlFlow;

use surrealdb_strand::Strand;

use super::super::inline::mentions_index_operator;
use crate::exec::function::FunctionRegistry;
use crate::exec::logical::reads::reads_beneath;
use crate::exec::logical::rewrite_deep;
use crate::exec::planner::util::SELECT_ITERATION_PARAMS;
use crate::expr::BinaryOperator;
use crate::expr::logical::{
	EffectOracle, EffectSet, EvalFrame, FieldPath, JoinKind, LogicalExpr, LogicalFields,
	LogicalFilter, LogicalIf, LogicalJoin, LogicalOp, LogicalPermissionFilter, LogicalProject,
	LogicalSource, ROW_PARAMS, RelationAlias, SourceRef, effects_of,
};

/// The parameter a subquery reads its caller's row through.
const PARENT: &str = "parent";

/// What deciding a lift consults besides the conjunct itself.
#[derive(Clone, Copy)]
pub(super) struct Consults<'a> {
	/// Whether the statement binds `name` around the point being bound: its value is the one that
	/// binding gives it when the statement runs.
	pub(super) is_bound: &'a dyn Fn(&str) -> bool,
	/// Decides which calls are deterministic.
	pub(super) registry: &'a FunctionRegistry,
}

impl Consults<'_> {
	/// Whether `name` denotes a row or a value the statement binds, which no lift moves.
	fn row_scoped(&self, name: &str) -> bool {
		SELECT_ITERATION_PARAMS.contains(&name) || (self.is_bound)(name)
	}
}

/// `op` with every membership of a filter it holds, in plans nested in its expressions too, written
/// as the semi or anti join it is; `None` where it holds none.
pub(super) fn lifted(op: &LogicalOp, consults: Consults<'_>) -> Option<LogicalOp> {
	if !holds_a_candidate(op) {
		return None;
	}
	let mut next_alias = next_alias_index(op);
	let mut changed = false;
	let out = rewrite_deep(op.clone(), &mut |node| {
		let (node, lifted) = lift_node(node, consults, &mut next_alias);
		changed |= lifted;
		node
	});
	changed.then_some(out)
}

/// One index past the highest index any name of the minted form carries in
/// `op` — the alias of a join, and any field a scalar reads or names — so a
/// pass adding joins continues the plan's numbering and mints no alias the
/// plan already names.
///
/// A join names the matched right row on the joined row, and its residual is
/// judged over that row, so an alias equal to a field the plan reads would be
/// read there as the right row rather than as the field. A field named
/// `_join0` reached through `$parent` is exactly that: lifting rewrites `$parent._join0` to the
/// field `_join0` of the left row.
fn next_alias_index(op: &LogicalOp) -> usize {
	let mut next = 0;
	let mut reserve = |index: Option<usize>| {
		if let Some(index) = index {
			next = next.max(index + 1);
		}
	};
	let _ = op.visit_ops_deep(&mut |node| {
		if let LogicalOp::Join(join) = node {
			reserve(join.alias.index());
		}
		// Every scalar, closure bodies included: a name the plan reads
		// anywhere is a name the joined row must not shadow.
		let Ok(()) = node.visit_scalars(&mut |scalar| {
			let _ = scalar.visit(&mut |expr| {
				if let LogicalExpr::Field(name) = expr {
					reserve(RelationAlias::index_of(name.as_str()));
				} else if let LogicalExpr::Access {
					field,
					..
				} = expr
				{
					reserve(RelationAlias::index_of(field.as_str()));
				}
				ControlFlow::<()>::Continue(())
			});
			Ok::<_, std::convert::Infallible>(())
		});
		ControlFlow::<()>::Continue(())
	});
	next
}

/// Whether `op`, or a plan nested in it, filters by a conjunct of a membership shape.
fn holds_a_candidate(op: &LogicalOp) -> bool {
	op.visit_ops_deep(&mut |node| match node {
		LogicalOp::Filter(LogicalFilter {
			predicate,
			..
		})
		| LogicalOp::PermissionFilter(LogicalPermissionFilter {
			predicate,
			..
		}) if conjuncts(predicate).into_iter().any(is_membership_shape) => ControlFlow::Break(()),
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

/// Whether `conjunct` is `x IN s`, `x NOT IN s`, `s CONTAINS x` or `s CONTAINSNOT x` for a scalar
/// subquery `s`.
fn is_membership_shape(conjunct: &LogicalExpr) -> bool {
	matches!(
		conjunct,
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::Contain | BinaryOperator::NotContain,
			..
		} if matches!(left.as_ref(), LogicalExpr::Subquery(_))
	) || matches!(
		conjunct,
		LogicalExpr::Binary {
			right,
			op: BinaryOperator::Inside | BinaryOperator::NotInside,
			..
		} if matches!(right.as_ref(), LogicalExpr::Subquery(_))
	)
}

/// `node` with its liftable memberships lifted when it is a filter, and whether any was.
fn lift_node(node: LogicalOp, consults: Consults<'_>, next_alias: &mut usize) -> (LogicalOp, bool) {
	match node {
		LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) => lift(
			*input,
			predicate,
			&EvalFrame::caller(),
			&HashSet::new(),
			consults,
			next_alias,
			&|input, predicate| {
				LogicalOp::Filter(LogicalFilter {
					input: Box::new(input),
					predicate,
				})
			},
		),
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			predicate,
			auth,
			guard,
		}) => {
			let frame = EvalFrame::permission(auth.clone());
			// The rows reach a permission filter as stored.
			let absent = computed_fields(&input);
			lift(*input, predicate, &frame, &absent, consults, next_alias, &|input, predicate| {
				LogicalOp::PermissionFilter(LogicalPermissionFilter {
					input: Box::new(input),
					predicate,
					auth: auth.clone(),
					guard,
				})
			})
		}
		other @ (LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
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
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::If(_)
		| LogicalOp::Throw(_)
		| LogicalOp::Break(_)
		| LogicalOp::Continue(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_)) => (other, false),
	}
}

/// A filter of `predicate` over `input` with every liftable conjunct lifted into a join above the
/// filter of the remaining conjuncts, and whether any was. `absent` names the fields `input`'s rows
/// do not carry though its schema declares them, the computed fields beneath a permission filter,
/// which no lifted conjunct may read off the outer row. `rebuild` makes a filter of the original
/// kind.
fn lift(
	input: LogicalOp,
	predicate: LogicalExpr,
	frame: &EvalFrame,
	absent: &HashSet<Strand>,
	consults: Consults<'_>,
	next_alias: &mut usize,
	rebuild: &dyn Fn(LogicalOp, LogicalExpr) -> LogicalOp,
) -> (LogicalOp, bool) {
	let terms = conjuncts(&predicate);
	// The join reads the right side once and every probe row reads what it read; the scalar form
	// reads it afresh for each. The two answer the same only where nothing between the probe rows
	// changes what that read answers, and the purity the lift asks of the right side settles only
	// half of that: a write on the probe side, in the predicate that stays below the join or in the
	// pipeline beneath it, changes the relation the later rows would have read.
	if !input.read_only() || !predicate.read_only() {
		return (rebuild(input, predicate), false);
	}
	let probe_rows_are_objects = yields_object_rows(&input);
	let liftable = |conjunct: &LogicalExpr| {
		Lift::of(conjunct, consults)
			.filter(|lift| probe_rows_are_objects || lift.residual.is_empty())
			.filter(|lift| !lift.reads_outer_any_of(absent))
	};
	if !terms.iter().any(|term| liftable(term).is_some()) {
		return (rebuild(input, predicate), false);
	}
	enum Step {
		Lift(Box<Lift>),
		/// A conjunct that stays where it was written, because it can raise.
		Held(LogicalExpr),
		/// A conjunct that sinks beneath every join.
		Sunk,
	}
	let mut steps = Vec::new();
	let mut below = Vec::new();
	let mut a_raising_lift_precedes = false;
	for conjunct in split_and(predicate) {
		match liftable(&conjunct) {
			Some(lift) => {
				a_raising_lift_precedes |= lift.may_raise(consults);
				steps.push(Step::Lift(Box::new(lift)));
			}
			None if a_raising_lift_precedes || may_raise(&conjunct, consults) => {
				steps.push(Step::Held(conjunct));
			}
			None => {
				below.push(conjunct);
				steps.push(Step::Sunk);
			}
		}
	}
	let mut op = match and_all(below) {
		Some(below) => rebuild(input, below),
		None => input,
	};
	let mut held: Vec<LogicalExpr> = Vec::new();
	for step in steps {
		match step {
			Step::Held(term) => held.push(term),
			Step::Sunk => {}
			Step::Lift(lift) => {
				if let Some(held) = and_all(std::mem::take(&mut held)) {
					op = rebuild(op, held);
				}
				let alias = RelationAlias::new(*next_alias);
				*next_alias += 1;
				op = LogicalOp::Join(lift.into_join(op, alias, frame.clone()));
			}
		}
	}
	let op = match and_all(held) {
		Some(held) => rebuild(op, held),
		None => op,
	};
	(op, true)
}

/// The computed fields `op`'s schema declares, by name.
fn computed_fields(op: &LogicalOp) -> HashSet<Strand> {
	op.schema()
		.fields
		.iter()
		.filter(|field| field.computed.is_some())
		.filter_map(|field| field.path.as_field().cloned())
		.collect()
}

/// Whether every row `op` yields is an object.
///
/// A `VALUE` projection yields the bare value of its expression and a value source yields whatever
/// its expression holds, so neither answers `true`. A shape this cannot see through is taken as one
/// that does not, so a caller reading this never assumes a field the row has nowhere to hold.
fn yields_object_rows(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Source(source) => source_yields_object_rows(&source.source),
		LogicalOp::Project(LogicalProject {
			fields,
			..
		}) => !matches!(fields, LogicalFields::Value { .. }),
		LogicalOp::Filter(LogicalFilter {
			input,
			..
		})
		| LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			..
		}) => yields_object_rows(input),
		// Everything this cannot see rows through. Listed rather than matched with a wildcard so
		// that adding a variant to `LogicalOp` fails to compile until its answer here is decided.
		LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Write(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Window(_)
		| LogicalOp::Limit(_)
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

/// Whether every row a source of this shape yields is an object.
///
/// Rows read out of the store are records, and a record is an object. Every other source holds
/// whatever the run puts in it: `FROM $rows` and `FROM <expr>` yield the value the expression
/// evaluates to, which may be a scalar, and a scalar is not always a value on which every field
/// read answers `NONE`: a geometry answers `type` and `coordinates`, so judging a residual against
/// an object carrying the alias alone would answer differently from the per-row form. A mock
/// generates record ids rather than reading records, so it is not one either.
fn source_yields_object_rows(source: &SourceRef) -> bool {
	match source {
		SourceRef::Table(_) | SourceRef::RecordId(_) => true,
		SourceRef::Mock(_) | SourceRef::Param(_) | SourceRef::Dynamic(_) => false,
	}
}

/// A lifted conjunct, before its alias is minted.
struct Lift {
	kind: JoinKind,
	/// The outer expression, evaluated against the left row.
	outer: LogicalExpr,
	/// The subquery's `VALUE` expression, in terms of the right row.
	key: LogicalExpr,
	/// The right pipeline beneath the subquery's filter and projection.
	right: LogicalOp,
	/// The subquery's conjuncts that read only the right row.
	right_filter: Vec<LogicalExpr>,
	/// `(outer path, right expression)` pairs from `r = $parent.p` conjuncts.
	keys: Vec<(LogicalExpr, LogicalExpr)>,
	/// The other conjuncts reading the outer row, in terms of the outer and right rows.
	residual: Vec<LogicalExpr>,
}

impl Lift {
	/// Whether the lift reads one of `fields` off the outer row: at the outer position, or through
	/// `$parent` in a correlation key or the residual.
	fn reads_outer_any_of(&self, fields: &HashSet<Strand>) -> bool {
		if fields.is_empty() {
			return false;
		}
		let paths: Vec<FieldPath> = fields.iter().cloned().map(FieldPath::field).collect();
		let through_parent = |expr: &LogicalExpr| {
			expr.any(|e| {
				matches!(e, LogicalExpr::Access { base, field }
					if fields.contains(field)
						&& matches!(base.as_ref(), LogicalExpr::Param(param) if param.as_str() == PARENT))
			})
		};
		reads_beneath(&self.outer, &paths)
			|| self.keys.iter().any(|(outer, _)| through_parent(outer))
			|| self.residual.iter().any(through_parent)
	}

	/// Whether evaluating this lift can raise: its keys and residual, judged per probe row, the
	/// subquery's own filter, and the pipeline beneath it.
	fn may_raise(&self, consults: Consults<'_>) -> bool {
		let expr_may_raise = |e: &LogicalExpr| may_raise(e, consults);
		expr_may_raise(&self.outer)
			|| expr_may_raise(&self.key)
			|| self.right_filter.iter().any(&expr_may_raise)
			|| self.residual.iter().any(&expr_may_raise)
			|| self.keys.iter().any(|(outer, right)| expr_may_raise(outer) || expr_may_raise(right))
			|| plan_may_raise(&self.right, consults)
	}

	/// The lift of `conjunct`, or `None` when it is not a shape this pass lifts; see the module
	/// documentation.
	fn of(conjunct: &LogicalExpr, consults: Consults<'_>) -> Option<Self> {
		let LogicalExpr::Binary {
			left,
			op,
			right,
		} = conjunct
		else {
			return None;
		};
		let (outer, plan, kind) = match (op, left.as_ref(), right.as_ref()) {
			(BinaryOperator::Inside, outer, LogicalExpr::Subquery(plan)) => {
				(outer, plan, JoinKind::Semi)
			}
			(BinaryOperator::NotInside, outer, LogicalExpr::Subquery(plan)) => {
				(outer, plan, JoinKind::Anti)
			}
			(BinaryOperator::Contain, LogicalExpr::Subquery(plan), outer) => {
				(outer, plan, JoinKind::Semi)
			}
			(BinaryOperator::NotContain, LogicalExpr::Subquery(plan), outer) => {
				(outer, plan, JoinKind::Anti)
			}
			_ => return None,
		};
		// At the outer position a field read is a read of the left row; a `$parent` there denotes a
		// row this pass cannot place.
		if !matches!(Side::of(outer, consults)?, Side::Right | Side::Neither) {
			return None;
		}
		if mentions_index_operator(conjunct) {
			return None;
		}
		let LogicalOp::Project(LogicalProject {
			input,
			fields: LogicalFields::Value {
				expr: key,
				..
			},
			omit,
		}) = plan.as_ref()
		else {
			return None;
		};
		// The key is evaluated once per right row for the whole join rather than once per right row
		// per outer row, so a call that answers differently each time would decide every outer row
		// alike.
		if !omit.is_empty() || !is_pure(key, consults.registry) {
			return None;
		}
		let (cond, right) = if let LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) = input.as_ref()
		{
			(Some(predicate), input.as_ref())
		} else {
			(None, input.as_ref())
		};
		if !is_chain_over_table(right) || !plan_is_pure(right, consults.registry) {
			return None;
		}
		if Side::of(key, consults)? != Side::Right {
			return None;
		}
		let mut right_filter = Vec::new();
		let mut keys = Vec::new();
		let mut residual = Vec::new();
		for c in cond.map(conjuncts).unwrap_or_default() {
			if !c.read_only() || mentions_index_operator(c) {
				return None;
			}
			if !mentions_parent(c) {
				// Reads only the right row, or nothing of either row. A nested plan inside it
				// denotes its own caller by `$parent`, so it must not read a row-scoped parameter
				// anywhere.
				if mentions_row_scoped_deep(c, consults) {
					return None;
				}
				// Beneath the join this conjunct decides the one relation every outer row probes,
				// where the scalar form decides a fresh relation per outer row; only a conjunct
				// that answers the same each time makes the two agree.
				if !is_pure(c, consults.registry) {
					return None;
				}
				right_filter.push(c.clone());
				continue;
			}
			// Reads the outer row. The join evaluates a key for every left row whatever the build
			// side holds, and a residual only for the pairs the keys matched; the scalar form
			// evaluates the whole condition once per right row per outer row, so it evaluates
			// neither when the right side is empty and both on every pair. A conjunct that can
			// raise would therefore report where the other form does not, in one direction or the
			// other.
			if may_raise(c, consults) {
				return None;
			}
			if let LogicalExpr::Binary {
				left,
				op: BinaryOperator::Equal,
				right,
			} = c && let Some(pair) = key_pair(left, right, consults)
			{
				keys.push(pair);
				continue;
			}
			Side::of(c, consults)?;
			residual.push(c.clone());
		}
		Some(Self {
			kind,
			outer: outer.clone(),
			key: key.clone(),
			right: right.clone(),
			right_filter,
			keys,
			residual,
		})
	}

	/// The join of `left` against the lifted subquery under `alias`.
	fn into_join(self, left: LogicalOp, alias: RelationAlias, frame: EvalFrame) -> LogicalJoin {
		let right = match and_all(self.right_filter) {
			Some(predicate) => LogicalOp::Filter(LogicalFilter {
				input: Box::new(self.right),
				predicate,
			}),
			None => self.right,
		};
		let mut on = vec![(self.outer, reroot(self.key, &alias))];
		on.extend(
			self.keys.into_iter().map(|(outer, key)| (reroot(outer, &alias), reroot(key, &alias))),
		);
		let filter = and_all(self.residual.into_iter().map(|c| reroot(c, &alias)).collect());
		LogicalJoin::new(left, right, self.kind, on, filter, alias, frame)
	}
}

/// Which row an expression reads.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
	/// Nothing of either row: literals and parameters only.
	Neither,
	/// The outer row, through `$parent.path`.
	Outer,
	/// The right row, through its fields.
	Right,
	/// Both.
	Both,
}

impl Side {
	fn join(self, other: Side) -> Side {
		match (self, other) {
			(Side::Neither, side) | (side, Side::Neither) => side,
			(Side::Both, _) | (_, Side::Both) => Side::Both,
			(Side::Outer, Side::Outer) => Side::Outer,
			(Side::Right, Side::Right) => Side::Right,
			(Side::Outer, Side::Right) | (Side::Right, Side::Outer) => Side::Both,
		}
	}

	/// The side `expr` reads, or `None` when it is not built from the forms the pass rewrites: a
	/// field read, `$parent.path`, `.field`, `[index]`, `[0]`, `[$]`, a literal, a parameter that
	/// is not row-scoped, a container, a unary or binary operator, an `IF`.
	///
	/// A field read alone is [`Side::Right`]: at the outer position the caller treats a right read
	/// as a read of the left row instead, which is the same form.
	fn of(expr: &LogicalExpr, consults: Consults<'_>) -> Option<Side> {
		let of = |e: &LogicalExpr| Side::of(e, consults);
		match expr {
			LogicalExpr::Literal(_)
			| LogicalExpr::Constant(_)
			| LogicalExpr::Table(_)
			| LogicalExpr::Mock(_) => Some(Side::Neither),
			LogicalExpr::RecordId(_) => {
				if expr.children().is_empty() {
					Some(Side::Neither)
				} else {
					None
				}
			}
			LogicalExpr::Param(param) => {
				if consults.row_scoped(param.as_str()) || param.as_str() == "value" {
					None
				} else {
					Some(Side::Neither)
				}
			}
			LogicalExpr::Field(_) => Some(Side::Right),
			LogicalExpr::Access {
				base,
				..
			} => {
				if matches!(base.as_ref(), LogicalExpr::Param(param) if param.as_str() == PARENT) {
					Some(Side::Outer)
				} else {
					of(base)
				}
			}
			LogicalExpr::Index {
				base,
				index,
			} => Some(of(base)?.join(of(index)?)),
			LogicalExpr::First(inner) | LogicalExpr::Last(inner) => of(inner),
			LogicalExpr::Array(items) | LogicalExpr::Set(items) => {
				items.iter().try_fold(Side::Neither, |acc, item| Some(acc.join(of(item)?)))
			}
			LogicalExpr::Object(pairs) => {
				pairs.iter().try_fold(Side::Neither, |acc, (_, item)| Some(acc.join(of(item)?)))
			}
			LogicalExpr::Prefix {
				expr,
				..
			} => of(expr),
			LogicalExpr::Binary {
				left,
				right,
				..
			} => Some(of(left)?.join(of(right)?)),
			LogicalExpr::If(branches) => {
				let LogicalIf {
					branches,
					otherwise,
				} = branches.as_ref();
				let mut side = Side::Neither;
				for (condition, value) in branches {
					side = side.join(of(condition)?).join(of(value)?);
				}
				if let Some(otherwise) = otherwise {
					side = side.join(of(otherwise)?);
				}
				Some(side)
			}
			LogicalExpr::Doc
			| LogicalExpr::Current
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
			| LogicalExpr::Postfix {
				..
			}
			| LogicalExpr::Invoke {
				..
			}
			| LogicalExpr::Call(_)
			| LogicalExpr::Closure(_)
			| LogicalExpr::Throw(_)
			| LogicalExpr::Break
			| LogicalExpr::Continue
			| LogicalExpr::Subquery(_) => None,
		}
	}
}

/// The `(outer, right)` key pair of `left = right`, when one side reads the outer row alone and the
/// other reads no more than the right row.
fn key_pair(
	left: &LogicalExpr,
	right: &LogicalExpr,
	consults: Consults<'_>,
) -> Option<(LogicalExpr, LogicalExpr)> {
	let left_side = Side::of(left, consults)?;
	let right_side = Side::of(right, consults)?;
	match (left_side, right_side) {
		(Side::Outer, Side::Right | Side::Neither) => Some((left.clone(), right.clone())),
		(Side::Right | Side::Neither, Side::Outer) => Some((right.clone(), left.clone())),
		_ => None,
	}
}

/// Rewrites an expression of the subquery's frame into one of the join's: `$parent.path` becomes
/// `path`, a read of the left row, and a field read becomes a read through the alias, of the right
/// row.
fn reroot(expr: LogicalExpr, alias: &RelationAlias) -> LogicalExpr {
	let Ok(out) = expr.transform(&mut |e| {
		Ok::<_, std::convert::Infallible>(if let LogicalExpr::Field(name) = e {
			alias.access(name)
		} else if let LogicalExpr::Access {
			base,
			field,
		} = e
		{
			if matches!(base.as_ref(), LogicalExpr::Param(param) if param.as_str() == PARENT) {
				LogicalExpr::Field(field)
			} else {
				LogicalExpr::Access {
					base,
					field,
				}
			}
		} else {
			e
		})
	});
	out
}

/// Whether `expr` names `$parent` at its own level, a nested plan excluded.
fn mentions_parent(expr: &LogicalExpr) -> bool {
	expr.any(|e| matches!(e, LogicalExpr::Param(param) if param.as_str() == PARENT))
}

/// Whether `op` is a table source alone or under its permission filter, layers, semi or anti joins
/// and filters: the pipeline `SELECT … FROM t` resolves to beneath its own clauses, with any
/// membership already lifted out of its permission or its filter. A locked read is excluded.
fn is_chain_over_table(op: &LogicalOp) -> bool {
	let mut cur = op;
	loop {
		match cur {
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input,
				..
			})
			| LogicalOp::Filter(LogicalFilter {
				input,
				..
			}) => cur = input,
			LogicalOp::Project(LogicalProject {
				input,
				..
			}) if cur.is_layer() => cur = input,
			LogicalOp::Join(join) if matches!(join.kind, JoinKind::Semi | JoinKind::Anti) => {
				cur = join.left();
			}
			LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(_),
				for_update,
				..
			}) => return !*for_update,
			LogicalOp::Source(_)
			| LogicalOp::Values(_)
			| LogicalOp::Match(_)
			| LogicalOp::Write(_)
			| LogicalOp::Subquery(_)
			| LogicalOp::UnionAll(_)
			| LogicalOp::Join(_)
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
			| LogicalOp::Sleep(_) => return false,
		}
	}
}

/// Whether every scalar in `op` and its inputs is [`is_pure`]: the plan yields the same rows
/// however many times it is executed, and executing it once for the whole join answers as executing
/// it per outer row does.
fn plan_is_pure(op: &LogicalOp, registry: &FunctionRegistry) -> bool {
	!op.any(|node| node.scalars().into_iter().any(|e| !is_pure(e, registry)))
}

/// Whether `expr` names one of the row-scoped parameters anywhere, as a parameter or as a
/// `FROM $param` source, nested plans included. Moving such an expression into or out of a nested
/// plan would rebind the name.
fn mentions_row_scoped_deep(expr: &LogicalExpr, consults: Consults<'_>) -> bool {
	let mut found = false;
	let _ = expr.visit(&mut |e| {
		if let LogicalExpr::Param(param) = e {
			if consults.row_scoped(param.as_str()) {
				found = true;
			}
		} else if let LogicalExpr::Subquery(plan) = e {
			let _ = plan.visit_ops_deep(&mut |node| {
				if let LogicalOp::Source(LogicalSource {
					source: SourceRef::Param(param),
					..
				}) = node && consults.row_scoped(param.as_str())
				{
					found = true;
				}
				let Ok(()) = node.visit_scalars(&mut |scalar| {
					if mentions_row_scoped_deep(scalar, consults) {
						found = true;
					}
					Ok::<_, std::convert::Infallible>(())
				});
				ControlFlow::<()>::Continue(())
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

/// Whether evaluating `expr` twice yields the same value as evaluating it once, with no effect
/// either way: it only reads, and every call in it is a builtin that answers the same for the same
/// arguments and the same row. A user-defined, scripted, module, silo or model call, a method, a
/// closure, an applied value, a nested plan and control flow are not; nor is a `MATCHES` or `KNN`,
/// which is answered by an access path rather than by evaluation.
fn is_pure(expr: &LogicalExpr, registry: &FunctionRegistry) -> bool {
	expr.read_only()
		&& effects_of(expr, &crate::exec::logical::effects::Oracle::new(registry)).pure()
}

/// The parameter reads that fetch a `DEFINE PARAM` where they are read: every one that is not
/// row-scoped. The fetch judges the definition's `PERMISSIONS` clause against the caller and is
/// refused where the clause denies, so a read is not inert.
///
/// `Effects::may_raise` consults nothing else of the oracle; the other fields of an `Effects`
/// computed through this are not answers and are not read.
struct Fetching<'a>(&'a dyn Fn(&str) -> bool);

impl EffectOracle for Fetching<'_> {
	fn builtin(&self, _name: &str) -> Option<EffectSet> {
		None
	}

	fn closure_args(&self, _name: &str) -> Option<&'static [usize]> {
		None
	}

	fn method_always_dispatches(&self, _name: &str) -> bool {
		false
	}

	fn evaluates_against_row(&self, _name: &str) -> bool {
		false
	}

	fn permission_checked_param(&self, name: &str) -> bool {
		!(self.0)(name)
	}
}

/// Whether evaluating `expr` may raise an error or signal control flow, so that a lift that moves
/// it may change what a statement reports: `Effects::may_raise`, which states what the evaluators
/// answer for every input and what may not. A parameter is inert where it names a row or a value
/// the statement binds, and raises otherwise, since reading it may fetch a `DEFINE PARAM` the
/// caller is denied.
fn may_raise(expr: &LogicalExpr, consults: Consults<'_>) -> bool {
	let inert = |name: &str| consults.row_scoped(name);
	effects_of(expr, &Fetching(&inert)).may_raise
}

/// Whether any expression `op` evaluates can raise. The names bound per row are `$this`, `$self`
/// and `$parent` everywhere, and `$value` in a layer's own columns, which the layer binds.
fn plan_may_raise(op: &LogicalOp, consults: Consults<'_>) -> bool {
	let row_params: &[&str] = if op.is_layer() {
		ROW_PARAMS
	} else {
		SELECT_ITERATION_PARAMS
	};
	let inert = |name: &str| row_params.contains(&name) || (consults.is_bound)(name);
	op.scalars().into_iter().any(|scalar| effects_of(scalar, &Fetching(&inert)).may_raise)
		|| op.children().iter().any(|child| plan_may_raise(child, consults))
}

/// The terms of `expr`'s top-level `AND` chain, in evaluation order.
fn conjuncts(expr: &LogicalExpr) -> Vec<&LogicalExpr> {
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
fn split_and(expr: LogicalExpr) -> Vec<LogicalExpr> {
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
fn and_all(terms: Vec<LogicalExpr>) -> Option<LogicalExpr> {
	terms.into_iter().reduce(|left, right| LogicalExpr::Binary {
		left: Box::new(left),
		op: BinaryOperator::And,
		right: Box::new(right),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn lowered(sql: &str) -> LogicalOp {
		let ast = crate::syn::parse(sql).expect("the statement parses");
		let Some(surrealdb_expr::parser::TopLevelExpr::Expr(expr)) =
			ast.expressions.into_iter().next()
		else {
			panic!("`{sql}` is not an expression statement");
		};
		crate::expr::logical::lower(expr).expect("the statement lowers")
	}

	fn lifts(sql: &str) -> bool {
		let registry = FunctionRegistry::with_builtins();
		let consults = Consults {
			is_bound: &|_| false,
			registry: &registry,
		};
		lifted(&lowered(sql), consults).is_some()
	}

	/// The statement `sql` as it binds, with its memberships written as predicates.
	fn bound(sql: &str) -> String {
		let registry = FunctionRegistry::with_builtins();
		super::super::memberships_of(&lowered(sql), &registry, &|_| false)
			.expect("the memberships are written")
			.to_string()
	}

	#[test]
	fn a_membership_over_a_table_is_lifted_in_each_written_form() {
		for sql in [
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE name = 'x')",
			"SELECT * FROM person WHERE team NOT IN (SELECT VALUE id FROM team)",
			"SELECT * FROM person WHERE (SELECT VALUE team FROM grant WHERE who = $parent.id AND n > 1) CONTAINS team",
			"SELECT * FROM person WHERE (SELECT VALUE team FROM grant) CONTAINSNOT team",
			"SELECT * FROM person WHERE 'A' IN (SELECT VALUE name FROM team)",
			"SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant WHERE n > 1 AND $parent.name > 'p2')",
		] {
			assert!(lifts(sql), "`{sql}` is lifted");
		}
	}

	#[test]
	fn the_correlation_keys_and_the_right_only_terms_are_placed_in_the_subquery() {
		let plan = bound(
			"SELECT * FROM person WHERE (SELECT VALUE team FROM grant WHERE who = $parent.id AND n > 1) CONTAINS team",
		);
		// The membership reads in its canonical form, the key as `r = $parent.p` in the
		// subquery's filter, and the term over the right row alone beneath it.
		assert!(plan.contains("team INSIDE ("), "{plan}");
		assert!(plan.contains("who = $parent.id"), "{plan}");
		assert!(plan.contains("n > 1"), "{plan}");
	}

	#[test]
	fn the_terms_around_a_lifted_one_keep_their_order() {
		let plan = bound(
			"SELECT * FROM person WHERE name = 'a' AND team NOT IN (SELECT VALUE id FROM team) AND name != 'b'",
		);
		assert!(plan.contains("name = 'a' AND name != 'b' AND team NOTINSIDE ("), "{plan}");
	}

	#[test]
	fn shapes_the_pass_does_not_lift_stay_scalar_subqueries() {
		for sql in [
			// A LIMIT.
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team LIMIT 1)",
			// An ORDER BY.
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team ORDER BY name)",
			// A field list rather than a VALUE.
			"SELECT * FROM person WHERE team IN (SELECT id FROM team)",
			// A correlation through a function argument.
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE name = string::uppercase($parent.name))",
			// A bare `$parent`, and a `$this`.
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE $parent = 1)",
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE $this.active = true)",
			// A right side that is not a table.
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team:a)",
			"SELECT * FROM person WHERE name IN (SELECT VALUE name FROM [{ name: 'a' }])",
			// A projected value with a call in it.
			"SELECT * FROM person WHERE name IN (SELECT VALUE string::lowercase(name) FROM team)",
			// An outer side reading a row parameter.
			"SELECT * FROM person WHERE $this.team IN (SELECT VALUE id FROM team)",
			// A comparison other than membership.
			"SELECT * FROM person WHERE team = (SELECT VALUE id FROM team)",
			// A right side that answers differently each time it is evaluated.
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE rand::bool())",
			// A correlation that can raise: over an empty right side the per-row form evaluates it
			// not at all, the join once per left row.
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE name = <string> $parent.age)",
			// A MATCHES or a KNN in the subquery's filter, or in the predicate.
			"SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant WHERE text @@ 'x')",
			"SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant WHERE vec <|2|> [1, 2])",
			"SELECT * FROM person WHERE (name @@ 'x') IN (SELECT VALUE who FROM grant)",
		] {
			assert!(!lifts(sql), "`{sql}` is not lifted");
		}
	}

	#[test]
	fn a_probe_side_that_writes_keeps_the_subquery_scalar() {
		// The join reads the right side once and every outer row probes what it read; the per-row
		// form reads it afresh after each row's writes. A conjunct beside the membership one writes
		// here, so the two forms see different relations and the lift is refused.
		assert!(!lifts(
			"SELECT * FROM person WHERE fn::touch(name) AND id IN (SELECT VALUE who FROM grant)"
		));
		assert!(lifts("SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant)"));
	}

	#[test]
	fn a_name_the_statement_binds_is_a_row_scoped_name() {
		let registry = FunctionRegistry::with_builtins();
		let sql = "SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant WHERE n = $bound)";
		let consults = Consults {
			is_bound: &|name| name == "bound",
			registry: &registry,
		};
		assert!(lifted(&lowered(sql), consults).is_none());
		let consults = Consults {
			is_bound: &|_| false,
			registry: &registry,
		};
		assert!(lifted(&lowered(sql), consults).is_some());
	}

	/// The predicate of the filter in `sql`'s plan.
	fn predicate_of(sql: &str) -> LogicalExpr {
		let mut found = None;
		let _ = lowered(sql).visit_ops_deep(&mut |node| {
			if let LogicalOp::Filter(filter) = node {
				found = Some(filter.predicate.clone());
			}
			ControlFlow::<()>::Continue(())
		});
		found.expect("the plan filters")
	}

	/// A permission filter over `squad`, whose `lead` is computed from `owner`, holding
	/// `predicate`.
	fn permission_over_squad(predicate: LogicalExpr) -> LogicalOp {
		use std::sync::Arc;

		use crate::expr::Kind;
		use crate::expr::logical::{ComputedBody, FieldSchema, GuardKind, Schema};
		let computed = FieldSchema {
			computed: Some(ComputedBody {
				expr: LogicalExpr::Field("owner".into()),
				kind: None,
				auth: None,
			}),
			..FieldSchema::new(FieldPath::field("lead"), Kind::String)
		};
		let schema = Schema::closed(vec![
			FieldSchema::new(FieldPath::field("owner"), Kind::String),
			computed,
		]);
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input: Box::new(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table("squad".into()),
				schema: Some(Arc::new(schema)),
				with: None,
				for_update: false,
			})),
			predicate,
			auth: None,
			guard: GuardKind::Read,
		})
	}

	/// The rows reach a permission filter as stored, carrying no computed field, so a membership
	/// that reads one off the row, at the outer position or through `$parent` in the subquery's
	/// condition, stays in the filter, which judges it with the field derived. Lifted, the join
	/// would read the field as `NONE`, and `NONE NOT IN …` admits every row.
	#[test]
	fn a_permission_membership_reading_a_computed_field_is_not_lifted() {
		let registry = FunctionRegistry::with_builtins();
		let consults = Consults {
			is_bound: &|_| false,
			registry: &registry,
		};
		let lifts_in_permission =
			|sql: &str| lifted(&permission_over_squad(predicate_of(sql)), consults).is_some();
		assert!(!lifts_in_permission(
			"SELECT * FROM squad WHERE lead NOT IN (SELECT VALUE who FROM grant)"
		));
		assert!(!lifts_in_permission(
			"SELECT * FROM squad WHERE $auth IN (SELECT VALUE who FROM grant WHERE team = $parent.lead)"
		));
		// The same membership over the stored field is lifted.
		assert!(lifts_in_permission(
			"SELECT * FROM squad WHERE owner NOT IN (SELECT VALUE who FROM grant)"
		));
	}
}
