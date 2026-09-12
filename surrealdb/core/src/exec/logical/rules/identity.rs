//! An operator that does nothing goes.
//!
//! - A [`LogicalOp::Filter`] or [`LogicalOp::PermissionFilter`] whose predicate is the literal
//!   `true` admits every row and goes. One whose predicate is `false` is kept: it is the physical
//!   planner's cue to read nothing, and a `PermissionFilter[false]` is `PERMISSIONS NONE`. The one
//!   exception is a `Filter[false]` directly over a [`LogicalOp::Values`] built from an inert
//!   literal — read-only and unable to raise: it becomes the empty literal, which yields the same
//!   nothing and keeps `ONLY`'s `NONE`-on-empty behaviour for a literal source. A source that
//!   writes or throws keeps its filter, since evaluating it is observable even where no row
//!   survives.
//! - A [`LogicalOp::Limit`] with neither `LIMIT` nor `START` pages nothing and goes.
//! - A [`LogicalOp::Sort`] over an input that yields at most one row orders nothing and goes: a
//!   single record id, a `GROUP ALL`, an `ONLY`, and any row-preserving or row-reducing operators
//!   over one of those. Dropping the sort drops the evaluation of its keys, so it goes only where
//!   every key is inert — read-only and unable to raise. Ordering by a key that is neither is not a
//!   shape the pipeline planner admits today; the guard is what keeps the rewrite sound if it ever
//!   is.
//! - `ONLY` over `ONLY` is one `ONLY`.
//! - A block directly inside a block is spliced into it when it is not empty and declares no `LET`
//!   of its own: a `LET` is scoped to its block, and an empty block yields `NONE` where the spliced
//!   sequence would yield the statement before it. Control flow — `RETURN`, `BREAK`, `CONTINUE` —
//!   propagates out of a nested block exactly as it does out of the spliced statements.
//!
//! Two rewrites that look like they belong here do not:
//!
//! - **Canonicalising projection-function field references.** `type::field('a')` denotes the same
//!   field as the idiom `a`, and the physical planner rewrites it that way on a *clone* to offer
//!   the predicate to index analysis, while the predicate that is actually evaluated keeps the
//!   call. The two evaluate through different code paths, so rewriting the evaluated predicate is a
//!   behaviour change rather than a normalisation.
//! - **Extracting `MATCHES` and KNN operators.** The physical planner reads those from the bound
//!   predicate when it chooses an access path, and strips them only once it knows an operator will
//!   evaluate them, so which conjuncts survive depends on the access path chosen.

use super::reads::{reads_beneath, reads_row_opaquely};
use super::{
	Outcome, RuleContext, RuleDef, fire, is_false, is_true, may_raise, source_rows_are_inert,
};
use crate::expr::logical::{
	FieldPath, JoinKind, LogicalAggregate, LogicalExpr, LogicalField, LogicalFields, LogicalFilter,
	LogicalLimit, LogicalOnly, LogicalOp, LogicalPermissionFilter, LogicalRecordIdKey,
	LogicalSequence, LogicalSort, LogicalSource, LogicalValues, OpKind, SourceRef,
};

pub(super) const RULE: RuleDef = RuleDef {
	name: "identity",
	kinds: &[
		OpKind::Filter,
		OpKind::PermissionFilter,
		OpKind::Limit,
		OpKind::Sort,
		OpKind::Only,
		OpKind::Sequence,
	],
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
		}) => {
			if is_true(&predicate) {
				return Outcome::Rewritten(*input);
			}
			if is_false(&predicate)
					&& let LogicalOp::Values(LogicalValues {
						expr,
						for_update,
					}) = input.as_ref()
					// Dropping the source drops the evaluation of the literal it
					// is built from, so it goes only where that literal is inert
					// — read-only and unable to raise. A source that writes or
					// throws is observed even where nothing survives the filter.
					&& !*for_update
					&& expr.read_only()
					&& !may_raise(expr, cx.permission_checked_params)
					// The rows are materialised, and materialising a record id
					// fetches the record it names — an evaluation of the
					// source's own, which outlives a filter no row survives.
					&& source_rows_are_inert(expr)
			{
				return Outcome::Rewritten(LogicalOp::Values(LogicalValues {
					expr: LogicalExpr::Array(Vec::new()),
					for_update: *for_update,
				}));
			}
			Outcome::Unchanged(LogicalOp::Filter(LogicalFilter {
				input,
				predicate,
			}))
		}
		LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input,
			predicate,
			auth,
		}) => {
			if is_true(&predicate) {
				Outcome::Rewritten(*input)
			} else {
				Outcome::Unchanged(LogicalOp::PermissionFilter(LogicalPermissionFilter {
					input,
					predicate,
					auth,
				}))
			}
		}
		LogicalOp::Limit(LogicalLimit {
			input,
			limit,
			start,
		}) => {
			if limit.is_none() && start.is_none() {
				Outcome::Rewritten(*input)
			} else {
				Outcome::Unchanged(LogicalOp::Limit(LogicalLimit {
					input,
					limit,
					start,
				}))
			}
		}
		LogicalOp::Sort(LogicalSort {
			input,
			ordering,
			tempfiles,
		}) => {
			// The sort is the only thing that evaluates its keys, so a key
			// that writes or raises is an effect the plan would lose with it.
			// A key naming a layer column is such a key: the column's own
			// expression is what the sort evaluates through it, and once the
			// sort no longer demands the column the `prune` rule removes it.
			let mut keys = Vec::new();
			ordering.exprs(&mut keys);
			let effectful = effectful_layer_columns(&input, cx);
			let keys_are_inert = keys.iter().all(|key| {
				key.read_only()
					&& !may_raise(key, cx.permission_checked_params)
					&& (effectful.is_empty()
						|| (!reads_beneath(key, &effectful)
							&& !reads_row_opaquely(key, cx.registry)))
			});
			if keys_are_inert && at_most_one_row(&input) {
				Outcome::Rewritten(*input)
			} else {
				Outcome::Unchanged(LogicalOp::Sort(LogicalSort {
					input,
					ordering,
					tempfiles,
				}))
			}
		}
		LogicalOp::Only(LogicalOnly {
			input,
		}) => {
			if matches!(input.as_ref(), LogicalOp::Only(_)) {
				Outcome::Rewritten(*input)
			} else {
				Outcome::Unchanged(LogicalOp::Only(LogicalOnly {
					input,
				}))
			}
		}
		LogicalOp::Sequence(LogicalSequence {
			stmts,
		}) => {
			let splice = |stmt: &LogicalOp| {
				matches!(stmt, LogicalOp::Sequence(LogicalSequence { stmts: inner })
						if !inner.is_empty() && !inner.iter().any(|s| matches!(s, LogicalOp::Let(_))))
			};
			if !stmts.iter().any(splice) {
				return Outcome::Unchanged(LogicalOp::Sequence(LogicalSequence {
					stmts,
				}));
			}
			let mut out = Vec::with_capacity(stmts.len());
			for stmt in stmts {
				if splice(&stmt) {
					let LogicalOp::Sequence(LogicalSequence {
						stmts: inner,
					}) = stmt
					else {
						unreachable!("splice matched a sequence")
					};
					out.extend(inner);
				} else {
					out.push(stmt);
				}
			}
			Outcome::Rewritten(LogicalOp::Sequence(LogicalSequence {
				stmts: out,
			}))
		}
		// The rule fires on no other kind; see `RULE`.
		other => Outcome::Unchanged(other),
	})
}

/// The fields the layers beneath `op` set with an expression that is not
/// inert — one that writes, or that can raise.
///
/// A layer column is evaluated for the rows the layer passes, and the
/// `prune` rule removes one nothing above reads. So a node that reads such a
/// column is what keeps its evaluation in the plan, and dropping that node
/// drops the evaluation as surely as dropping the expression itself would.
/// Only a layer's columns are collected: a user projection's fields are never
/// pruned, so nothing above one holds its evaluation up.
fn effectful_layer_columns(op: &LogicalOp, cx: &RuleContext<'_>) -> Vec<FieldPath> {
	let mut out = Vec::new();
	collect_effectful_layer_columns(op, cx, &mut out);
	out
}

fn collect_effectful_layer_columns(op: &LogicalOp, cx: &RuleContext<'_>, out: &mut Vec<FieldPath>) {
	if let LogicalOp::Project(project) = op
		&& project.is_layer()
		&& let LogicalFields::Select(fields) = &project.fields
	{
		for field in fields {
			let LogicalField::Single {
				expr,
				..
			} = field
			else {
				continue;
			};
			if expr.read_only() && !may_raise(expr, cx.permission_checked_params) {
				continue;
			}
			if let Some(path) = field.output_path() {
				out.push(FieldPath::field(path.name()));
			}
		}
	}
	for child in op.children() {
		collect_effectful_layer_columns(child, cx, out);
	}
}

/// Whether `op` yields at most one row: a single record id, a `GROUP ALL`,
/// an `ONLY`, or an operator that never adds rows over one of those.
fn at_most_one_row(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Source(LogicalSource {
			source: SourceRef::RecordId(rid),
			..
		}) => !matches!(rid.key, LogicalRecordIdKey::Range(_)),
		LogicalOp::Aggregate(LogicalAggregate {
			group,
			..
		}) => group.is_empty(),
		LogicalOp::Only(_) => true,
		LogicalOp::Join(join) => {
			matches!(join.kind, JoinKind::Semi | JoinKind::Anti) && at_most_one_row(join.left())
		}
		LogicalOp::Subquery(_)
		| LogicalOp::Filter(_)
		| LogicalOp::PermissionFilter(_)
		| LogicalOp::Distinct(_)
		| LogicalOp::Sort(_)
		| LogicalOp::Limit(_)
		| LogicalOp::Project(_)
		| LogicalOp::Fetch(_)
		| LogicalOp::Timeout(_)
		| LogicalOp::VersionScope(_) => op.children().iter().all(at_most_one_row),
		LogicalOp::Source(_)
		| LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Split(_)
		| LogicalOp::Explain(_)
		| LogicalOp::Eval(_)
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => false,
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::TableName;

	use super::super::super::lower_test as lower;
	use super::super::test_support::{rewritten, rewritten_checking, rewritten_sql, spine};
	use super::*;
	use crate::expr::logical::LogicalLiteral;

	#[test]
	fn a_true_predicate_removes_the_filter_and_a_false_one_keeps_it() {
		assert_eq!(spine(&rewritten(lower("SELECT * FROM t WHERE true"))), ["Project", "Source"]);
		assert_eq!(
			spine(&rewritten(lower("SELECT * FROM t WHERE false"))),
			["Project", "Filter", "Source"]
		);
	}

	#[test]
	fn a_true_permission_filter_is_removed_and_a_false_one_is_kept() {
		let over = |predicate: bool| {
			LogicalOp::PermissionFilter(LogicalPermissionFilter {
				input: Box::new(LogicalOp::Source(LogicalSource {
					source: SourceRef::Table(TableName::from("t")),
					schema: None,
					with: None,
					for_update: false,
				})),
				predicate: LogicalExpr::Literal(LogicalLiteral::Bool(predicate)),
				auth: None,
			})
		};
		assert_eq!(rewritten(over(true)).name(), "Source");
		assert_eq!(rewritten(over(false)).name(), "PermissionFilter");
	}

	#[test]
	fn a_false_filter_over_a_literal_becomes_the_empty_literal() {
		let plan = rewritten_sql("SELECT * FROM [1, 2] WHERE false");
		assert_eq!(plan, "Project[*]\n  Values[[]]");
	}

	#[test]
	fn a_false_filter_keeps_a_source_whose_rows_are_not_inert() {
		// Reading `$secret` fetches a `DEFINE PARAM` whose `PERMISSIONS`
		// clause is judged there, so the read reports what the clause
		// decides even though no row survives the filter.
		let plan = rewritten_checking(lower("SELECT * FROM [$secret] WHERE false"), &["secret"]);
		assert_eq!(plan.to_string(), "Project[*]\n  Filter[false]\n    Values[[$secret]]");
		// A parameter the analyzer did not resolve could hold a record id,
		// and materialising one fetches the record it names: an evaluation
		// of the source's own, which the filter does not stand between.
		let plan = rewritten_checking(lower("SELECT * FROM [$open] WHERE false"), &["secret"]);
		assert_eq!(plan.to_string(), "Project[*]\n  Filter[false]\n    Values[[$open]]");
	}

	#[test]
	fn a_nested_plan_is_rewritten_too() {
		let plan = rewritten_sql("SELECT * FROM t WHERE a IN (SELECT VALUE b FROM u WHERE true)");
		assert!(!plan.contains("Filter[true]"), "{plan}");
	}

	#[test]
	fn a_projection_function_reference_is_left_as_written() {
		let plan = rewritten_sql("SELECT * FROM t WHERE type::field('a') > 1");
		assert!(plan.contains("type::field('a') > 1"), "{plan}");
	}

	#[test]
	fn a_limit_that_pages_nothing_goes() {
		let plan = rewritten(LogicalOp::Limit(LogicalLimit {
			input: Box::new(lower("SELECT * FROM t")),
			limit: None,
			start: None,
		}));
		assert_eq!(spine(&plan), ["Project", "Source"]);
		assert_eq!(
			spine(&rewritten(lower("SELECT * FROM t LIMIT 1"))),
			["Project", "Limit", "Source"]
		);
	}

	#[test]
	fn a_sort_of_at_most_one_row_goes() {
		assert_eq!(spine(&rewritten(lower("SELECT * FROM t:1 ORDER BY a"))), ["Project", "Source"]);
		assert_eq!(
			spine(&rewritten(lower("SELECT * FROM t:1 WHERE b ORDER BY a"))),
			["Project", "Filter", "Source"]
		);
		assert_eq!(
			spine(&rewritten(lower("SELECT count() FROM t GROUP ALL ORDER BY count"))),
			["Aggregate", "Source"]
		);
		// A range, a table and a grouped query may yield several rows.
		for sql in [
			"SELECT * FROM t:1..5 ORDER BY a",
			"SELECT * FROM t ORDER BY a",
			"SELECT a, count() FROM t GROUP BY a ORDER BY a",
		] {
			let plan = rewritten(lower(sql));
			assert!(spine(&plan).contains(&"Sort"), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn a_sort_keyed_on_an_effectful_layer_column_stays_over_one_row() {
		use crate::expr::logical::{
			EvalFrame, LogicalFields, LogicalOrder, LogicalOrdering, LogicalProject,
		};
		use crate::expr::{Kind, PrefixOperator};

		// One row beneath a layer that sets `c` from `expr`.
		let layered = |expr: LogicalExpr| {
			LogicalOp::Project(LogicalProject {
				input: Box::new(lower("SELECT * FROM t:1")),
				fields: LogicalFields::Select(vec![
					LogicalField::All,
					LogicalField::column("c", expr, Some(Kind::Int), EvalFrame::computed(None)),
				]),
				omit: vec![],
			})
		};
		let sorted_on = |key: &str, input: LogicalOp| {
			rewritten(LogicalOp::Sort(LogicalSort {
				input: Box::new(input),
				ordering: LogicalOrdering::Order(vec![LogicalOrder {
					value: LogicalExpr::Field(key.into()),
					collate: false,
					numeric: false,
					direction: true,
				}]),
				tempfiles: false,
			}))
		};

		// The key is a plain path, but the column it names casts, so it can
		// raise. The sort is what demands the column; drop the sort and
		// `prune` takes the column, and the cast, with it.
		let cast = LogicalExpr::Prefix {
			op: PrefixOperator::Cast(Kind::Int),
			expr: Box::new(LogicalExpr::Field("bad".into())),
		};
		let plan = sorted_on("c", layered(cast));
		assert!(spine(&plan).contains(&"Sort"), "{plan}");

		// A layer column that only reads the row holds nothing up, so the
		// sort over one row still goes.
		let plan = sorted_on("c", layered(LogicalExpr::Field("a".into())));
		assert!(!spine(&plan).contains(&"Sort"), "{plan}");
	}

	#[test]
	fn a_sort_whose_key_is_not_inert_stays_over_one_row() {
		use crate::expr::logical::{LogicalEval, LogicalOrder, LogicalOrdering};

		// `ORDER BY` takes an idiom, and every key the pipeline planner
		// admits today is a plain path, so the keys are built here rather
		// than lowered from a SELECT.
		fn key(sql: &str) -> crate::expr::logical::LogicalExpr {
			let lowered = lower(sql);
			let LogicalOp::Eval(LogicalEval {
				expr,
			}) = lowered
			else {
				panic!("`{sql}` is not an expression statement");
			};
			expr
		}
		let sorted = |value: crate::expr::logical::LogicalExpr| {
			rewritten(LogicalOp::Sort(LogicalSort {
				input: Box::new(lower("SELECT * FROM t:1")),
				ordering: LogicalOrdering::Order(vec![LogicalOrder {
					value,
					collate: false,
					numeric: false,
					direction: true,
				}]),
				tempfiles: false,
			}))
		};

		// The sort is the only thing that evaluates the key, so dropping it
		// would drop a write or an error the statement owes.
		for sql in ["fn::writer()", "<int> a", "a.method()"] {
			let plan = sorted(key(sql));
			assert!(spine(&plan).contains(&"Sort"), "`{sql}`:\n{plan}");
		}
		// A path is inert, so that sort still goes.
		let plan = sorted(key("a"));
		assert!(!spine(&plan).contains(&"Sort"), "{plan}");
	}

	#[test]
	fn a_doubled_only_collapses() {
		let only = |input: LogicalOp| {
			LogicalOp::Only(LogicalOnly {
				input: Box::new(input),
			})
		};
		let plan = rewritten(only(only(lower("SELECT * FROM t"))));
		assert_eq!(spine(&plan), ["Only", "Project", "Source"]);
	}

	#[test]
	fn a_block_in_a_block_is_spliced_unless_it_declares_or_is_empty() {
		let plan = rewritten_sql("{ 1; { 2; 3 }; 4 }");
		assert_eq!(plan, "Sequence\n  Eval[1]\n  Eval[2]\n  Eval[3]\n  Eval[4]");
		let plan = rewritten_sql("{ LET $x = 1; { LET $x = 2; $x }; RETURN $x }");
		assert!(plan.contains("Sequence\n  Let[$x]"), "{plan}");
		assert!(
			plan.lines().filter(|l| l.trim_start().starts_with("Sequence")).count() == 2,
			"{plan}"
		);
		// An empty block yields `NONE` where the statement before it would
		// otherwise be the value.
		let plan = rewritten(LogicalOp::Sequence(LogicalSequence {
			stmts: vec![
				LogicalOp::Eval(crate::expr::logical::LogicalEval {
					expr: LogicalExpr::Literal(LogicalLiteral::Integer(1)),
				}),
				LogicalOp::Sequence(LogicalSequence {
					stmts: vec![],
				}),
			],
		}));
		assert_eq!(plan.to_string(), "Sequence\n  Eval[1]\n  Sequence");
	}
}
