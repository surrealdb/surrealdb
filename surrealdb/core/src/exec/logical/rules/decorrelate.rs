//! Decorrelation: a scalar subquery in a predicate becomes a join.
//!
//! A predicate of the form `x IN (SELECT VALUE k FROM t WHERE c)` asks, for
//! each row, whether some row of `t` admitted by `c` has `k = x`. Evaluated as
//! written, the subquery runs once per row. Lifted, it is a semi join of the
//! rows against `t` on `x = k`, with `c` split into the parts the join can
//! use: a conjunct `r = $parent.p` — a field of `t` against a field of the
//! outer row — is a further join key; any other conjunct that reads the outer
//! row through `$parent` is the join's residual filter, evaluated per
//! candidate pair; a conjunct that reads only `t`'s row stays a filter on the
//! right side, evaluated once. `x NOT IN (…)` is the anti join of the same
//! shape, and `(…) CONTAINS x` / `(…) CONTAINSNOT x` are the same predicates
//! written the other way round. The join's keys match by SurrealQL's `=`, as
//! `IN` and `CONTAINS` compare elements, so the two forms admit the same rows.
//!
//! # What is lifted
//!
//! A conjunct of a [`LogicalOp::Filter`] or [`LogicalOp::PermissionFilter`]
//! predicate — a term of its top-level `AND` chain — is lifted when every one
//! of these holds:
//!
//! - it is `x IN s`, `x NOT IN s`, `s CONTAINS x` or `s CONTAINSNOT x`, `s` a scalar subquery;
//! - `s` is `Project[VALUE k]` with no `OMIT`, over an optional `Filter[c]`, over a table source
//!   alone or under its permission filter, layers, semi or anti joins and filters the rules have
//!   already moved beneath those — the shape the resolver and the other rules leave a `SELECT VALUE
//!   k FROM t WHERE c` in, with the subqueries of its own permission and filter already lifted;
//! - every scalar of that right pipeline is pure (see [`is_pure`]): the join builds it once and
//!   every outer row probes that one relation, where the per-row form builds a fresh one for each,
//!   so only a right side that answers the same each time makes the two agree;
//! - `x` and `k` are built from literals, parameters that are not row-scoped, field reads, `.field`
//!   and `[index]` access, `[0]`, `[$]`, containers, unary and binary operators and `IF`; `k` may
//!   not read the outer row and `x` may not read a row-scoped parameter, and `k` is pure for the
//!   same reason the right pipeline is;
//! - every conjunct of `c` that reads the outer row does so as `$parent.path`, is built from the
//!   same forms, names no row-scoped parameter other than `$parent`, and cannot raise — a key is
//!   evaluated for every left row whatever the build side holds, and a residual only for the pairs
//!   the keys matched, where the per-row form evaluates the whole condition once per right row per
//!   outer row; every other conjunct of `c` names no row-scoped parameter, in a nested plan
//!   included, and is pure — it is pushed onto the build side, so it too decides the one relation
//!   every outer row sees;
//! - the probe side is read-only: the predicate whole, the lifted conjuncts included, and the
//!   pipeline the filter stands on. The right side's purity says the relation is the same each time
//!   it is read; this says nothing on the left changes it between the reads. A write on the probe
//!   side does — the per-row form reads the relation the writes so far have left, and the join
//!   reads it once for all of them;
//! - lifted out of a `PermissionFilter`, it reads no computed field of the table off the outer row,
//!   at the outer position or through `$parent`. The rows reach the filter as stored, carrying no
//!   computed field, and the filter judges its predicate against the row with the computed fields
//!   it reads derived (the VM's `Lowering::permission`); a join above the filter would read the
//!   field as `NONE` — an anti join then admitting the rows the predicate was written to deny.
//!
//! The other conjuncts of the predicate keep their order in a filter of the
//! same kind beneath the join, so an index the pipeline chose for them still
//! serves the left side; the join adds no field to a semi or anti joined row,
//! so a conjunct over the row judges the same value on either side of it.
//!
//! # What is not lifted
//!
//! Anything else stays a scalar subquery evaluated per row: a subquery with
//! a `LIMIT`, `ORDER BY`, `GROUP BY`, `SPLIT`, `FETCH` or a `VALUE` over a
//! non-table source; a right side that answers differently each time it is
//! evaluated, such as one filtered by `rand::bool()`; a correlation that can
//! raise, such as `k = <int> $parent.bad`, which the per-row form never
//! evaluates over an empty right side; a correlation through a
//! function argument, a closure, a graph lookup, a method or a nested plan; a
//! bare `$parent`, or `$this` / `$self` anywhere; a `MATCHES` or `KNN`
//! operator in the predicate, the value or the subquery's filter, which names
//! an index of the statement it is written in and is answered by that
//! statement's access path; a scalar
//! subquery in array or comparison position, a `(SELECT … LIMIT 1)` scalar,
//! and a graph hop. Each of these has a join form; none is lifted here
//! because the per-row form is what defines its result, and the lift never
//! changes the rows a statement returns. What the lift does not preserve is
//! the order conjuncts of one predicate are evaluated in: a conjunct another
//! rule split off is evaluated apart from its siblings, so an error one
//! conjunct raises may be raised on a row another conjunct would have
//! refused first.
//!
//! # As a rule
//!
//! The rule fires at a [`LogicalOp::Filter`] or [`LogicalOp::PermissionFilter`]
//! whose inputs are at their fixpoint, so the subquery it lifts has had its
//! own predicates rewritten first: a conjunct the `layers` rule moved beneath
//! the subquery's layers is part of the right pipeline it lifts. The
//! constants were folded before any rule ran, so a lifted conjunct is the one
//! the per-row evaluation would have seen. A join lifted out of a
//! `PermissionFilter` carries the permission frame and its `AUTH LIMIT`; the
//! rules and the physical planner treat it as the barrier the filter was.
//! Aliases continue the numbering of the joins the plan already carries,
//! through the driver's counter.

use std::collections::HashSet;

use surrealdb_strand::Strand;

use super::reads::reads_beneath;
use super::{
	Outcome, RuleContext, RuleDef, and_all, conjuncts, fire, is_pure, may_raise,
	mentions_index_operator, mentions_row_scoped_deep, plan_may_raise, split_and,
};
use crate::exec::function::FunctionRegistry;
use crate::expr::BinaryOperator;
use crate::expr::logical::{
	EvalFrame, FieldPath, JoinKind, LogicalExpr, LogicalFields, LogicalFilter, LogicalIf,
	LogicalJoin, LogicalOp, LogicalPermissionFilter, LogicalProject, LogicalSource, OpKind,
	RelationAlias, SourceRef,
};

/// The parameter a subquery reads its caller's row through.
const PARENT: &str = "parent";

pub(super) const RULE: RuleDef = RuleDef {
	name: "decorrelate",
	kinds: &[OpKind::Filter, OpKind::PermissionFilter],
	apply,
};

#[expect(
	clippy::wildcard_enum_match_arm,
	reason = "the kinds this rule fires on are declared in `RULE`"
)]
fn apply(node: &mut LogicalOp, cx: &mut RuleContext<'_>) -> bool {
	fire(node, |node| match node {
		LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) => lift(
			*input,
			predicate,
			&EvalFrame::caller(),
			&HashSet::new(),
			cx,
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
			lift(*input, predicate, &frame, &absent, cx, &|input, predicate| {
				LogicalOp::PermissionFilter(LogicalPermissionFilter {
					input: Box::new(input),
					predicate,
					auth: auth.clone(),
					guard,
				})
			})
		}
		other => Outcome::Unchanged(other),
	})
}

/// What deciding a lift consults besides the conjunct itself: which names
/// denote the row the predicate is evaluated against, which builtins are
/// deterministic, and which parameter reads may be denied.
#[derive(Clone, Copy)]
struct Consults<'a> {
	row_scoped: &'a [&'a str],
	registry: &'a FunctionRegistry,
	permission_checked: &'a HashSet<String>,
}

impl<'a> Consults<'a> {
	fn of(cx: &'a RuleContext<'_>) -> Self {
		Self {
			row_scoped: cx.row_scoped,
			registry: cx.registry,
			permission_checked: cx.permission_checked_params,
		}
	}
}

/// Rebuilds a filter of `predicate` over `input` with every liftable
/// conjunct lifted into a join above the filter of the remaining conjuncts.
/// `absent` names the fields `input`'s rows do not carry though its schema
/// declares them — the computed fields beneath a permission filter — which no
/// lifted conjunct may read off the outer row. `rebuild` makes a filter of the
/// original kind.
fn lift(
	input: LogicalOp,
	predicate: LogicalExpr,
	frame: &EvalFrame,
	absent: &HashSet<Strand>,
	cx: &mut RuleContext<'_>,
	rebuild: &dyn Fn(LogicalOp, LogicalExpr) -> LogicalOp,
) -> Outcome {
	let consults = Consults::of(cx);
	let conjuncts = conjuncts(&predicate);
	// The join reads the right side once and every outer row probes what it
	// read; the scalar form reads it afresh for each. The two answer the same
	// only where nothing between the outer rows changes what that read
	// answers, and the purity the lift asks of the right side settles only
	// half of that: a write on the probe side — the predicate that stays
	// below the join, or the pipeline beneath it — changes the relation the
	// later rows would have read. So a lift stands over a read-only probe
	// alone.
	if !input.read_only() || !predicate.read_only() {
		return Outcome::Unchanged(rebuild(input, predicate));
	}
	// A residual reads the outer row through the join's row, and a probe row
	// that is not an object has no field to carry the alias: the join judges
	// the residual against an object holding the alias alone, where every
	// other name answers `NONE`. Off a record id those names do not answer
	// `NONE` — a field read fetches the record — so the residual would be
	// judged on a row the per-row form never sees, and rows it matches would
	// be dropped. Where the probe rows are not objects, only a conjunct that
	// leaves no residual is lifted.
	let probe_rows_are_objects = yields_object_rows(&input);
	let liftable = |conjunct: &LogicalExpr| {
		Lift::of(conjunct, consults)
			.filter(|lift| probe_rows_are_objects || lift.residual.is_empty())
			.filter(|lift| !lift.reads_outer_any_of(absent))
	};
	if !conjuncts.iter().any(|c| liftable(c).is_some()) {
		return Outcome::Unchanged(rebuild(input, predicate));
	}
	// `AND` short-circuits left to right, so where a conjunct that stays
	// belongs is a question of where it was written. One written before a
	// lifted conjunct is already judged for every row that reaches it,
	// whatever the membership answers, and beneath the join it is judged for
	// the same rows — where an index the pipeline chose for it still serves
	// the left side. One written after is judged only for the rows the
	// membership kept, so a term that can raise stands between the two joins
	// it was written between, judged where the joins before it have already
	// refused the rows it must not act on.
	//
	// A term that cannot raise answers the same wherever it stands and sinks
	// to the bottom, below every join — but only past joins that raise
	// nothing themselves. A join builds its right side only once its probe
	// yields a row, so a term that refuses every probe row beneath a join
	// takes the right side's evaluation with it, and a lift whose key,
	// filter, residual or pipeline can raise owes that evaluation for every
	// row the per-row form reaches. Such a lift holds every later term where
	// it was written.
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
				a_raising_lift_precedes |= lift.may_raise(cx.permission_checked_params);
				steps.push(Step::Lift(Box::new(lift)));
			}
			None if a_raising_lift_precedes
				|| may_raise(&conjunct, cx.permission_checked_params) =>
			{
				steps.push(Step::Held(conjunct))
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
				let alias = RelationAlias::new(cx.next_alias);
				cx.next_alias += 1;
				op = LogicalOp::Join(lift.into_join(op, alias, frame.clone()));
			}
		}
	}
	Outcome::Rewritten(match and_all(held) {
		Some(held) => rebuild(op, held),
		None => op,
	})
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
/// A `VALUE` projection yields the bare value of its expression and a value
/// source yields whatever its expression holds, so neither answers `true`.
/// A shape this cannot see through is taken as one that does not, so a
/// caller reading this never assumes a field the row has nowhere to hold.
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
		// Everything this cannot see rows through. Listed rather than
		// matched with a wildcard so that adding a variant to `LogicalOp`
		// fails to compile until its answer here is decided.
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
/// Rows read out of the store are records, and a record is an object. Every
/// other source holds whatever the run puts in it: `FROM $rows` and
/// `FROM <expr>` yield the value the expression evaluates to, which may be a
/// scalar, and a scalar is not always a value on which every field read
/// answers `NONE` — a geometry answers `type` and `coordinates`, so judging a
/// residual against an object carrying the alias alone would answer
/// differently from the per-row form. A mock generates record ids rather
/// than reading records, so it is not one either.
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
	/// The other conjuncts reading the outer row, in terms of the outer and
	/// right rows.
	residual: Vec<LogicalExpr>,
}

impl Lift {
	/// Whether the lift reads one of `fields` off the outer row: at the outer
	/// position, or through `$parent` in a correlation key or the residual.
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

	/// Whether evaluating this lift can raise: its keys and residual, judged
	/// per probe row, the subquery's own filter, and the pipeline beneath it.
	fn may_raise(&self, permission_checked: &HashSet<String>) -> bool {
		let expr_may_raise = |e: &LogicalExpr| may_raise(e, permission_checked);
		expr_may_raise(&self.outer)
			|| expr_may_raise(&self.key)
			|| self.right_filter.iter().any(&expr_may_raise)
			|| self.residual.iter().any(&expr_may_raise)
			|| self.keys.iter().any(|(outer, right)| expr_may_raise(outer) || expr_may_raise(right))
			|| plan_may_raise(&self.right)
	}

	/// The lift of `conjunct`, or `None` when it is not a shape this pass
	/// lifts; see the module docs.
	fn of(conjunct: &LogicalExpr, consults: Consults<'_>) -> Option<Self> {
		let Consults {
			row_scoped,
			registry,
			permission_checked,
		} = consults;
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
		// At the outer position a field read is a read of the left row; a
		// `$parent` there denotes a row this pass cannot place.
		if !matches!(Side::of(outer, row_scoped)?, Side::Right | Side::Neither) {
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
		// The key is evaluated once per right row for the whole join
		// rather than once per right row per outer row, so a call that
		// answers differently each time would decide every outer row alike.
		if !omit.is_empty() || !is_pure(key, registry) {
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
		if !is_chain_over_table(right) || !plan_is_pure(right, registry) {
			return None;
		}
		if Side::of(key, row_scoped)? != Side::Right {
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
				// Reads only the right row, or nothing of either row. A nested
				// plan inside it denotes its own caller by `$parent`, so it
				// must not read a row-scoped parameter anywhere.
				if mentions_row_scoped_deep(c, row_scoped) {
					return None;
				}
				// Beneath the join this conjunct decides the one relation
				// every outer row probes, where the scalar form decides a
				// fresh relation per outer row; only a conjunct that answers
				// the same each time makes the two agree.
				if !is_pure(c, registry) {
					return None;
				}
				right_filter.push(c.clone());
				continue;
			}
			// Reads the outer row. The join evaluates a key for every left
			// row whatever the build side holds, and a residual only for the
			// pairs the keys matched; the scalar form evaluates the whole
			// condition once per right row per outer row, so it evaluates
			// neither when the right side is empty and both on every pair.
			// A conjunct that can raise would therefore report where the
			// other form does not, in one direction or the other.
			if may_raise(c, permission_checked) {
				return None;
			}
			if let LogicalExpr::Binary {
				left,
				op: BinaryOperator::Equal,
				right,
			} = c && let Some(pair) = key_pair(left, right, row_scoped)
			{
				keys.push(pair);
				continue;
			}
			Side::of(c, row_scoped)?;
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

	/// The side `expr` reads, or `None` when it is not built from the forms
	/// the pass rewrites: a field read, `$parent.path`, `.field`, `[index]`,
	/// `[0]`, `[$]`, a literal, a parameter that is not row-scoped, a
	/// container, a unary or binary operator, an `IF`.
	///
	/// A field read alone is [`Side::Right`]: at the outer position the
	/// caller treats a right read as a read of the left row instead, which
	/// is the same form.
	fn of(expr: &LogicalExpr, row_scoped: &[&str]) -> Option<Side> {
		let of = |e: &LogicalExpr| Side::of(e, row_scoped);
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
				if row_scoped.contains(&param.as_str()) || param.as_str() == "value" {
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

/// The `(outer, right)` key pair of `left = right`, when one side reads the
/// outer row alone and the other reads no more than the right row.
fn key_pair(
	left: &LogicalExpr,
	right: &LogicalExpr,
	row_scoped: &[&str],
) -> Option<(LogicalExpr, LogicalExpr)> {
	let left_side = Side::of(left, row_scoped)?;
	let right_side = Side::of(right, row_scoped)?;
	match (left_side, right_side) {
		(Side::Outer, Side::Right | Side::Neither) => Some((left.clone(), right.clone())),
		(Side::Right | Side::Neither, Side::Outer) => Some((right.clone(), left.clone())),
		_ => None,
	}
}

/// Rewrites an expression of the subquery's frame into one of the join's:
/// `$parent.path` becomes `path`, a read of the left row, and a field read
/// becomes a read through the alias, of the right row.
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

/// Whether `op` is a table source alone or under its permission filter,
/// layers, semi or anti joins and filters — the pipeline `SELECT … FROM t`
/// resolves to beneath its own clauses, with any subquery already lifted out
/// of its permission or its filter and any conjunct the `layers` rule moved
/// beneath the layers. A locked read is excluded.
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

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::super::super::test_fixture::{Fixture, owner, record_user};

	/// People linked to teams, a grant table, and two tables whose select
	/// permission asks the grant table: `team` correlated on the row, `note`
	/// beside a plain conjunct.
	async fn fixture() -> Fixture {
		Fixture::new(
			"DEFINE TABLE person SCHEMAFULL;
			 DEFINE FIELD name ON person TYPE string;
			 DEFINE FIELD team ON person TYPE option<record<team>>;
			 DEFINE TABLE team SCHEMAFULL
			 	PERMISSIONS FOR select WHERE $auth IN (SELECT VALUE who FROM grant WHERE team = $parent.id);
			 DEFINE FIELD name ON team TYPE string;
			 DEFINE FIELD active ON team TYPE bool;
			 DEFINE TABLE grant SCHEMALESS;
			 DEFINE TABLE note SCHEMAFULL
			 	PERMISSIONS FOR select WHERE active = true AND $auth IN (SELECT VALUE who FROM grant);
			 DEFINE FIELD active ON note TYPE bool;
			 DEFINE FUNCTION fn::touch($n: string) { CREATE grant SET who = $n; RETURN true };",
		)
		.await
	}

	#[tokio::test]
	async fn an_in_subquery_over_a_table_is_a_semi_join_on_its_value() {
		let fx = fixture().await;
		let plan = fx
			.bind_as(
				&owner(),
				"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE name = 'x')",
			)
			.await;
		// The subquery's own conjunct filters the right side once; the value
		// it projects is the right key, read through the alias.
		assert_eq!(
			plan,
			"\
Project[*]
  Join[SEMI AS _join0    ON team = _join0.id]
    Source[person]
    Filter[name = 'x']
      Source[team]"
		);
	}

	#[tokio::test]
	async fn a_not_in_subquery_is_an_anti_join_above_the_other_conjuncts() {
		let fx = fixture().await;
		let plan = fx
			.bind_as(
				&owner(),
				"SELECT * FROM person WHERE name = 'a' AND team NOT IN (SELECT VALUE id FROM team) AND name != 'b'",
			)
			.await;
		// The conjuncts around the lifted one keep their order in a filter
		// beneath the join, where the left side's access path can use them.
		assert_eq!(
			plan,
			"\
Project[*]
  Join[ANTI AS _join0    ON team = _join0.id]
    Filter[name = 'a' AND name != 'b']
      Source[person]
    Source[team]"
		);
	}

	#[tokio::test]
	async fn contains_lifts_correlated_equalities_as_keys() {
		let fx = fixture().await;
		let plan = fx
			.bind_as(
				&owner(),
				"SELECT * FROM person WHERE (SELECT VALUE team FROM grant WHERE who = $parent.id AND n > 1) CONTAINS team",
			)
			.await;
		// `who = $parent.id` is a second key pair; `n > 1` reads the right
		// row alone and stays its filter.
		assert_eq!(
			plan,
			"\
Project[*]
  Join[SEMI AS _join0    ON team = _join0.team AND id = _join0.who]
    Source[person]
    Filter[n > 1]
      Source[grant]"
		);
	}

	#[tokio::test]
	async fn a_correlated_conjunct_that_is_not_an_equality_is_the_joins_filter() {
		let fx = fixture().await;
		let plan = fx
			.bind_as(
				&owner(),
				"SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant WHERE n > 1 AND $parent.name > 'p2')",
			)
			.await;
		// The residual reads the left row alone, so the join rule moves it to
		// the left input, where it is judged once per row rather than once
		// per candidate pair.
		assert_eq!(
			plan,
			"\
Project[*]
  Join[SEMI AS _join0    ON id = _join0.who]
    Filter[name > 'p2']
      Source[person]
    Filter[n > 1]
      Source[grant]"
		);
	}

	#[tokio::test]
	async fn a_constant_outer_side_is_lifted() {
		let fx = fixture().await;
		let plan = fx
			.bind_as(&owner(), "SELECT * FROM person WHERE 'A' IN (SELECT VALUE name FROM team)")
			.await;
		assert_eq!(
			plan,
			"\
Project[*]
  Join[SEMI AS _join0    ON 'A' = _join0.name]
    Source[person]
    Source[team]"
		);
	}

	#[tokio::test]
	async fn a_lift_inside_a_lifted_subquery_keeps_the_outer_predicate() {
		let fx = fixture().await;
		let plan = fx
			.bind_as(
				&owner(),
				"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE id IN (SELECT VALUE team FROM grant))",
			)
			.await;
		// The inner subquery is lifted first, bottom-up. The outer lift then
		// reads `_join0.team` off the join it produced, and a field read may
		// fetch a record: `_join<n>` does not tell an alias from a user's
		// field, because the name is reserved so that a join carrying it mints
		// a different index. Judging that read as possibly raising, the outer
		// lift declines and its predicate stays where it was.
		//
		// Telling the two apart needs the indices this pass minted, which the
		// purity predicates do not carry; with that, the outer lift returns.
		assert_eq!(
			plan,
			"\
Project[*]
  Filter[team INSIDE (
    Project[VALUE id]
      Join[SEMI AS _join0    ON id = _join0.team]
        Source[team]
        Source[grant]
  )]
    Source[person]"
		);
	}

	#[tokio::test]
	async fn shapes_the_pass_does_not_lift_stay_scalar_subqueries() {
		let fx = fixture().await;
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
			// A correlation that can raise: over an empty right side the
			// per-row form evaluates it not at all, the join once per left row.
			"SELECT * FROM person WHERE team IN (SELECT VALUE id FROM team WHERE name = <string> $parent.age)",
			// A MATCHES or a KNN in the subquery's filter, or in the predicate.
			"SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant WHERE text @@ 'x')",
			"SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant WHERE vec <|2|> [1, 2])",
			"SELECT * FROM person WHERE (name @@ 'x') IN (SELECT VALUE who FROM grant)",
		] {
			let plan = fx.bind_as(&owner(), sql).await;
			assert!(!plan.contains("Join["), "{sql} must not be lifted:\n{plan}");
			assert!(
				plan.contains("Project[VALUE") || plan.contains("Project[id]"),
				"{sql}:\n{plan}"
			);
		}
	}

	#[tokio::test]
	async fn a_probe_side_that_writes_keeps_the_subquery_scalar() {
		// The join reads the right side once and every outer row probes what
		// it read; the per-row form reads it afresh after each row's writes.
		// A conjunct beside the membership one writes here, so the two forms
		// see different relations and the lift is refused.
		let fx = fixture().await;
		let plan = fx
			.bind_as(
				&owner(),
				"SELECT * FROM person
				 WHERE fn::touch(name) AND id IN (SELECT VALUE who FROM grant)",
			)
			.await;
		assert!(
			!plan.contains("Join["),
			"a writing conjunct must keep the subquery scalar:\n{plan}"
		);
		// The same statement with the writing conjunct gone still lifts.
		let plan = fx
			.bind_as(&owner(), "SELECT * FROM person WHERE id IN (SELECT VALUE who FROM grant)")
			.await;
		assert!(plan.contains("Join["), "{plan}");
	}

	#[tokio::test]
	async fn a_pushed_conjunct_beneath_the_subquerys_layers_is_lifted_with_it() {
		// `c` is computed above `team`; `name = 'x'` reads a stored field and
		// moves beneath the layer before the lift, which accepts the filter on
		// the chain and carries it into the right pipeline.
		let fx = Fixture::new(
			"DEFINE TABLE person SCHEMALESS;
			 DEFINE TABLE team SCHEMAFULL;
			 DEFINE FIELD name ON team TYPE string;
			 DEFINE FIELD c ON team TYPE string COMPUTED string::uppercase(name);",
		)
		.await;
		let plan = fx
			.bind_as(
				&owner(),
				"SELECT * FROM person WHERE team IN (SELECT VALUE c FROM team WHERE name = 'x')",
			)
			.await;
		assert_eq!(
			plan,
			"\
Project[*]
  Join[SEMI AS _join0    ON team = _join0.c]
    Source[person]
    Project[*, string::uppercase(name) AS c : string @computed]
      Filter[name = 'x']
        Source[team]"
		);
	}

	#[tokio::test]
	async fn a_permission_subquery_is_a_join_in_the_permission_frame() {
		let fx = fixture().await;
		// The correlated grant lookup: the row's id is a key, the caller a
		// constant key, and the join carries the permission frame.
		assert_eq!(
			fx.bind_as(&record_user(), "SELECT * FROM team").await,
			"\
Join[SEMI AS _join0    ON 'user:tobie' = _join0.who AND id = _join0.team    @permission]
  Source[team]
  Source[grant]"
		);
		// The other conjunct of the permission stays a permission filter
		// beneath the join; the user's own predicate stays above both.
		assert_eq!(
			fx.bind_as(&record_user(), "SELECT * FROM note WHERE id != note:1").await,
			"\
Filter[id != (note:1)]
  Join[SEMI AS _join0    ON 'user:tobie' = _join0.who    @permission]
    PermissionFilter[active = true]
      Source[note]
    Source[grant]"
		);
	}

	/// The rows a permission filter judges carry no computed field, so a
	/// membership that reads one off the row — at the outer position, or
	/// through `$parent` in the subquery's condition — stays in the filter,
	/// which judges it with the field derived. Lifted, the join would read the
	/// field as `NONE`, and `NONE NOT IN …` admits every row.
	#[tokio::test]
	async fn a_permission_membership_reading_a_computed_field_is_not_lifted() {
		let fx = Fixture::new(
			"DEFINE TABLE grant SCHEMALESS;
			 DEFINE TABLE squad SCHEMAFULL
			 	PERMISSIONS FOR select WHERE lead NOT IN (SELECT VALUE who FROM grant);
			 DEFINE FIELD owner ON squad TYPE string;
			 DEFINE FIELD lead ON squad COMPUTED owner;
			 DEFINE TABLE crew SCHEMAFULL
			 	PERMISSIONS FOR select WHERE $auth IN (SELECT VALUE who FROM grant WHERE team = $parent.lead);
			 DEFINE FIELD owner ON crew TYPE string;
			 DEFINE FIELD lead ON crew COMPUTED owner;
			 DEFINE TABLE band SCHEMAFULL
			 	PERMISSIONS FOR select WHERE owner NOT IN (SELECT VALUE who FROM grant);
			 DEFINE FIELD owner ON band TYPE string;
			 DEFINE FIELD lead ON band COMPUTED owner;",
		)
		.await;
		for table in ["squad", "crew"] {
			let plan = fx.bind_as(&record_user(), &format!("SELECT id FROM {table}")).await;
			assert!(!plan.contains("Join["), "no permission join over {table}:\n{plan}");
			assert!(plan.contains("PermissionFilter["), "the filter keeps the membership:\n{plan}");
		}
		// The same membership over the stored field is lifted.
		let plan = fx.bind_as(&record_user(), "SELECT id FROM band").await;
		assert!(plan.contains("Join[ANTI"), "a stored key lifts:\n{plan}");
	}
}

/// Whether every scalar in `op` and its inputs is [`is_pure`]: the plan
/// yields the same rows however many times it is executed, and executing it
/// once for the whole join answers as executing it per outer row does.
fn plan_is_pure(op: &LogicalOp, registry: &FunctionRegistry) -> bool {
	!op.any(|node| node.scalars().into_iter().any(|e| !is_pure(e, registry)))
}
