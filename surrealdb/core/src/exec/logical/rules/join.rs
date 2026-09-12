//! A conjunct over one side of a join moves to that side.
//!
//! A [`LogicalOp::Filter`] over a [`LogicalOp::Join`] in the caller's frame
//! judges the joined row: the left row, with the matched right row under the
//! join's alias for an inner or left join. A conjunct that reads the left row
//! alone — no read through the alias, none of the row other than by name —
//! admits or refuses a left row whatever it is joined with, so it moves to a
//! filter over the left input, for every join kind. A conjunct of an inner
//! join that reads the right row alone — every field read at the row's level
//! is `alias.field`, none is the bare alias — moves to a filter over the
//! right input with `alias.field` rewritten to `field`: an inner join emits
//! a joined row for each match, and a match the conjunct refuses is a right
//! row it refuses. Not for a left join, whose unmatched rows hold `NULL`
//! under the alias and are judged by the conjunct above but not by a filter
//! on the right; nor for a semi or anti join, whose rows do not carry the
//! alias. The other conjuncts stay above the join. Nothing moves when the
//! predicate holds a `MATCHES` or `KNN`, which the statement's access path
//! answers where the predicate stands.
//!
//! A join's own residual filter is split the same way: a conjunct reading the
//! right row alone moves into the right input for every kind — it only decides
//! what matches — and one reading the left row alone moves to the left input
//! for an inner or semi join, where a left row that matches nothing is not
//! emitted.
//!
//! A join in a permission frame is a barrier (see the module documentation of
//! [`super`]); a filter above one is left where it is.

use super::reads::reads_row_opaquely;
use super::{
	Outcome, RuleContext, RuleDef, and_all, fire, is_permission_join, may_raise,
	mentions_index_operator, plan_may_raise, rewrite_positions, split_and, visit_positions,
};
use crate::expr::logical::{
	EvalFrame, JoinKind, LogicalExpr, LogicalFilter, LogicalJoin, LogicalOp, OpKind, RelationAlias,
};

pub(super) const RULE: RuleDef = RuleDef {
	name: "join",
	kinds: &[OpKind::Filter, OpKind::Join],
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
		}) => match *input {
			LogicalOp::Join(join) => filter_over_join(join, predicate, cx),
			input => Outcome::Unchanged(LogicalOp::Filter(LogicalFilter {
				input: Box::new(input),
				predicate,
			})),
		},
		LogicalOp::Join(join) => residual(join, cx),
		other => Outcome::Unchanged(other),
	})
}

/// Which row a conjunct of a predicate over a joined row reads.
enum Reads {
	/// The left row alone, by name.
	Left,
	/// The right row alone, through the alias.
	Right,
	/// Both rows, or the row other than by name.
	Both,
}

impl Reads {
	fn of(term: &LogicalExpr, alias: &RelationAlias, cx: &RuleContext<'_>) -> Self {
		if reads_row_opaquely(term, cx.registry) {
			return Reads::Both;
		}
		let mut left = false;
		let mut right = false;
		let mut bare_alias = false;
		// The alias field beneath an `alias.field` access is the access's own
		// root, not a bare read of the alias.
		let mut through_access = 0usize;
		let _ = visit_positions(term, true, &mut |e, at_row| {
			if !at_row {
				return std::ops::ControlFlow::<()>::Continue(());
			}
			if through_access > 0 {
				through_access -= 1;
				return std::ops::ControlFlow::Continue(());
			}
			if let LogicalExpr::Access {
				base,
				..
			} = e && matches!(base.as_ref(), LogicalExpr::Field(name) if name == alias.name())
			{
				right = true;
				through_access = 1;
			} else if let LogicalExpr::Field(name) = e {
				if name == alias.name() {
					bare_alias = true;
				} else {
					left = true;
				}
			}
			std::ops::ControlFlow::Continue(())
		});
		match (left, right, bare_alias) {
			(_, _, true) | (true, true, false) => Reads::Both,
			(false, true, false) => Reads::Right,
			(true, false, false) | (false, false, false) => Reads::Left,
		}
	}
}

/// `alias.field` rewritten to `field`, a read of the right row itself.
fn reroot(mut term: LogicalExpr, alias: &RelationAlias) -> LogicalExpr {
	rewrite_positions(&mut term, true, &mut |e, at_row| {
		if at_row
			&& let LogicalExpr::Access {
				base,
				field,
			} = e && matches!(base.as_ref(), LogicalExpr::Field(name) if name == alias.name())
		{
			*e = LogicalExpr::Field(field.clone());
		}
	});
	term
}

/// Whether `term` may be judged on the rows the join drops.
///
/// Above a join a conjunct is judged for the rows the join kept; on an input
/// it is judged for every row that input yields, the dropped ones included. A
/// term that only reads answers the same either way and is merely judged more
/// often. One that writes, or that can raise, would act on a row the statement
/// never admits — a `THROW` beneath a semi join turns a query that returned
/// rows into an error.
fn survives_the_move(term: &LogicalExpr, cx: &RuleContext<'_>) -> bool {
	term.read_only() && !may_raise(term, cx.permission_checked_params)
}

/// Whether what the join owes per probe row can raise: its keys, its residual
/// and the pipeline it builds its right side from.
///
/// A join builds its right side only once the probe has yielded a row, so a
/// term moved onto the probe that refuses every row takes that build with it
/// — and with it every evaluation the build owes. Where the build can raise,
/// the per-row form reports an error that the moved term would suppress, so
/// nothing moves onto the probe at all.
fn the_build_may_raise(join: &LogicalJoin, cx: &RuleContext<'_>) -> bool {
	let checked = cx.permission_checked_params;
	// Judged rerooted, as the split judges a right-side term: off the right
	// row the alias is gone and the read is a plain field, where a read off
	// the alias itself counts as a possible dereference.
	let raises = |e: &LogicalExpr| may_raise(&reroot(e.clone(), &join.alias), checked);
	join.on.iter().any(|(l, r)| raises(l) || raises(r))
		|| join.filter.as_ref().is_some_and(raises)
		|| plan_may_raise(&join.inputs[1])
}

fn filter_over_join(join: LogicalJoin, predicate: LogicalExpr, cx: &RuleContext<'_>) -> Outcome {
	let unchanged = |join: LogicalJoin, predicate: LogicalExpr| {
		Outcome::Unchanged(LogicalOp::Filter(LogicalFilter {
			input: Box::new(LogicalOp::Join(join)),
			predicate,
		}))
	};
	if join.frame != EvalFrame::caller() || mentions_index_operator(&predicate) {
		return unchanged(join, predicate);
	}
	// Nothing reaches the probe where the build can raise: emptying the probe
	// would skip the build, and with it the error the per-row form reports.
	let probe_takes = !the_build_may_raise(&join, cx);
	let mut left = Vec::new();
	let mut right = Vec::new();
	let mut above = Vec::new();
	for term in split_and(predicate) {
		match Reads::of(&term, &join.alias, cx) {
			Reads::Left if probe_takes && survives_the_move(&term, cx) => left.push(term),
			Reads::Right if join.kind == JoinKind::Inner => {
				// Judged as it will stand on the input: the alias is the
				// join's own name for the right row, and off the right row
				// the same read is a plain field.
				let rerooted = reroot(term.clone(), &join.alias);
				if survives_the_move(&rerooted, cx) {
					right.push(rerooted);
				} else {
					above.push(term);
				}
			}
			Reads::Left | Reads::Right | Reads::Both => above.push(term),
		}
	}
	if left.is_empty() && right.is_empty() {
		return unchanged(join, and_all(above).expect("a predicate has at least one conjunct"));
	}
	let LogicalJoin {
		inputs,
		kind,
		on,
		filter,
		alias,
		frame,
	} = join;
	let [left_input, right_input] = *inputs;
	let join = LogicalOp::Join(LogicalJoin::new(
		filtered(left_input, left),
		filtered(right_input, right),
		kind,
		on,
		filter,
		alias,
		frame,
	));
	Outcome::Rewritten(match and_all(above) {
		Some(predicate) => LogicalOp::Filter(LogicalFilter {
			input: Box::new(join),
			predicate,
		}),
		None => join,
	})
}

/// The join's residual filter split onto its inputs; see the module
/// documentation.
fn residual(join: LogicalJoin, cx: &RuleContext<'_>) -> Outcome {
	let Some(residual) = &join.filter else {
		return Outcome::Unchanged(LogicalOp::Join(join));
	};
	if is_permission_join(&join) || mentions_index_operator(residual) {
		return Outcome::Unchanged(LogicalOp::Join(join));
	}
	let left_takes =
		matches!(join.kind, JoinKind::Inner | JoinKind::Semi) && !the_build_may_raise(&join, cx);
	let mut left = Vec::new();
	let mut right = Vec::new();
	let mut stays = Vec::new();
	for term in split_and(residual.clone()) {
		match Reads::of(&term, &join.alias, cx) {
			Reads::Left if left_takes && survives_the_move(&term, cx) => left.push(term),
			Reads::Right => {
				// Judged rerooted, as in `filter_over_join`.
				let rerooted = reroot(term.clone(), &join.alias);
				if survives_the_move(&rerooted, cx) {
					right.push(rerooted);
				} else {
					stays.push(term);
				}
			}
			Reads::Left | Reads::Both => stays.push(term),
		}
	}
	if left.is_empty() && right.is_empty() {
		return Outcome::Unchanged(LogicalOp::Join(join));
	}
	let LogicalJoin {
		inputs,
		kind,
		on,
		alias,
		frame,
		..
	} = join;
	let [left_input, right_input] = *inputs;
	Outcome::Rewritten(LogicalOp::Join(LogicalJoin::new(
		filtered(left_input, left),
		filtered(right_input, right),
		kind,
		on,
		and_all(stays),
		alias,
		frame,
	)))
}

fn filtered(input: LogicalOp, terms: Vec<LogicalExpr>) -> LogicalOp {
	match and_all(terms) {
		Some(predicate) => LogicalOp::Filter(LogicalFilter {
			input: Box::new(input),
			predicate,
		}),
		None => input,
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::super::super::lower_test as lower;
	use super::super::test_support::rewritten;
	use super::*;
	use crate::expr::logical::{LogicalProject, LogicalSource, SourceRef};

	fn table(name: &str) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::Table(TableName::from(name)),
			schema: None,
			with: None,
			for_update: false,
		})
	}

	/// `t JOIN u AS _join0 ON t.k = _join0.k`, of `kind`, in `frame`.
	fn join(kind: JoinKind, frame: EvalFrame, filter: Option<LogicalExpr>) -> LogicalJoin {
		let alias = RelationAlias::new(0);
		LogicalJoin::new(
			table("t"),
			table("u"),
			kind,
			vec![(LogicalExpr::Field("k".into()), alias.access("k"))],
			filter,
			alias,
			frame,
		)
	}

	/// The predicate of `SELECT * FROM t WHERE <sql>`.
	fn predicate(sql: &str) -> LogicalExpr {
		let LogicalOp::Project(LogicalProject {
			input,
			..
		}) = lower(&format!("SELECT * FROM t WHERE {sql}"))
		else {
			panic!("expected a projection");
		};
		let LogicalOp::Filter(LogicalFilter {
			predicate,
			..
		}) = *input
		else {
			panic!("expected a filter");
		};
		predicate
	}

	fn filter_over(join: LogicalJoin, sql: &str) -> LogicalOp {
		LogicalOp::Filter(LogicalFilter {
			input: Box::new(LogicalOp::Join(join)),
			predicate: predicate(sql),
		})
	}

	#[test]
	fn a_filter_splits_around_an_inner_join() {
		let plan = filter_over(
			join(JoinKind::Inner, EvalFrame::caller(), None),
			"a = 1 AND _join0.b = 2 AND a = _join0.c AND $this.d = 3",
		);
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Filter[a = _join0.c AND $this.d = 3]
  Join[INNER AS _join0    ON k = _join0.k]
    Filter[a = 1]
      Source[t]
    Filter[b = 2]
      Source[u]"
		);
	}

	#[test]
	fn a_left_only_conjunct_moves_for_every_kind_and_a_right_only_one_for_inner_alone() {
		for kind in [JoinKind::Left, JoinKind::Semi, JoinKind::Anti] {
			let plan = filter_over(join(kind, EvalFrame::caller(), None), "a = 1 AND _join0.b = 2");
			let after = rewritten(plan).to_string();
			assert!(after.starts_with("Filter[_join0.b = 2]"), "{kind:?}:\n{after}");
			assert!(after.contains("    Filter[a = 1]\n      Source[t]"), "{kind:?}:\n{after}");
			assert!(after.ends_with("    Source[u]"), "{kind:?}:\n{after}");
		}
	}

	#[test]
	fn nothing_moves_through_a_permission_join_or_around_an_index_operator() {
		let permission =
			filter_over(join(JoinKind::Semi, EvalFrame::permission(None), None), "a = 1");
		assert_eq!(rewritten(permission.clone()), permission);
		let indexed =
			filter_over(join(JoinKind::Inner, EvalFrame::caller(), None), "a = 1 AND b @@ 'x'");
		assert_eq!(rewritten(indexed.clone()), indexed);
	}

	#[test]
	fn a_bare_alias_read_stays_above() {
		let plan = filter_over(join(JoinKind::Inner, EvalFrame::caller(), None), "_join0 = NONE");
		assert_eq!(rewritten(plan.clone()), plan);
	}

	#[test]
	fn a_residual_splits_onto_the_inputs() {
		let residual = predicate("a > 1 AND _join0.b > 2 AND a > _join0.c");
		let plan = LogicalOp::Join(join(JoinKind::Semi, EvalFrame::caller(), Some(residual)));
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Join[SEMI AS _join0    ON k = _join0.k    WHERE a > _join0.c]
  Filter[a > 1]
    Source[t]
  Filter[b > 2]
    Source[u]"
		);
		// An anti join emits the left rows that match nothing, so a left
		// conjunct of its residual stays where it decides the match.
		let residual = predicate("a > 1 AND _join0.b > 2");
		let plan = LogicalOp::Join(join(JoinKind::Anti, EvalFrame::caller(), Some(residual)));
		assert_eq!(
			rewritten(plan).to_string(),
			"\
Join[ANTI AS _join0    ON k = _join0.k    WHERE a > 1]
  Source[t]
  Filter[b > 2]
    Source[u]"
		);
	}
}
