//! Filters merge, and a predicate moves through a user projection and into a
//! subquery.
//!
//! Three rewrites at a [`LogicalOp::Filter`], none of which changes which
//! record image a user expression observes:
//!
//! - **Adjacent filters merge.** Two stacked filters become one conjunction, the inner predicate's
//!   conjuncts first, so the inner predicate is still evaluated first; a conjunct that is the
//!   literal `true` is dropped on the way.
//! - **A conjunct moves through the user's projection.** Over a `Project` that is not a layer —
//!   directly, or as the root of a `Subquery` — a conjunct whose every field read is either a field
//!   the projection passes through (`*` present, the name not projected and not omitted) or a
//!   projected field whose expression is free to duplicate — see [`free_to_duplicate`]: a path, a
//!   literal, a parameter, a constant, or a comparison or logical operator over those — moves
//!   beneath the projection with each projected name replaced by its expression. A conjunct reading
//!   the row other than by name, a projected field that is coerced or evaluated in another frame or
//!   whose expression is any other (a call, arithmetic, a method), a name whose projection lands at
//!   a nested position or is omitted, or a name the projection does not supply, stays above.
//!   Nothing moves through a `VALUE` projection, whose rows are not objects, through a projection
//!   over a `GROUP BY`, whose fields are the aggregation's, or when a literal source lies beneath,
//!   whose rows a projection reshapes. Nothing moves through a projection any of whose fields is
//!   not inert — read-only and unable to raise — whatever the predicate reads: a filter beneath the
//!   projection is applied first, so a write or an error would reach fewer rows than the projection
//!   as written.
//! - **A predicate moves into a subquery that returns its rows unchanged.** Over a `Subquery` whose
//!   plan is `SELECT *` over something, or has no projection at all, the predicate moves inside,
//!   directly beneath the identity projection or over the plan's root, where it merges with the
//!   subquery's own predicate and is offered to the inner source. Not when the predicate names a
//!   row-scoped parameter, which the subquery would rebind, nor when a literal source lies beneath.
//!
//! A layer is never taken for a user projection or an identity projection —
//! it has columns beside `*` — so the moved predicate lands above every layer
//! and every [`LogicalOp::PermissionFilter`] in its path; the `layers` rule
//! decides what may go further.

use std::collections::{HashMap, HashSet};

use super::reads::reads_row_opaquely;
use super::{
	Outcome, RuleContext, RuleDef, and_all, fire, is_true, may_dereference, may_raise,
	mentions_row_scoped_deep, rewrite_positions, split_and, visit_positions,
};
use crate::expr::logical::{
	EvalMode, LogicalDestructureField, LogicalExpr, LogicalField, LogicalFields, LogicalFilter,
	LogicalOp, LogicalProject, LogicalSource, LogicalSubquery, OpKind, SourceRef,
};
use crate::expr::{BinaryOperator, PrefixOperator};

pub(super) const RULE: RuleDef = RuleDef {
	name: "filter",
	kinds: &[OpKind::Filter],
	apply,
};

#[expect(
	clippy::wildcard_enum_match_arm,
	reason = "an operator this rule does not rewrite is left as it is"
)]
fn apply(node: &mut LogicalOp, cx: &mut RuleContext<'_>) -> bool {
	fire(node, |node| {
		let LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		}) = node
		else {
			unreachable!("the rule fires on a filter")
		};
		let unchanged = |input: LogicalOp, predicate: LogicalExpr| {
			Outcome::Unchanged(LogicalOp::Filter(LogicalFilter {
				input: Box::new(input),
				predicate,
			}))
		};
		match *input {
			LogicalOp::Filter(LogicalFilter {
				input: inner_input,
				predicate: inner,
			}) => {
				let mut terms = split_and(inner);
				terms.extend(split_and(predicate));
				terms.retain(|term| !is_true(term));
				Outcome::Rewritten(match and_all(terms) {
					Some(predicate) => LogicalOp::Filter(LogicalFilter {
						input: inner_input,
						predicate,
					}),
					None => *inner_input,
				})
			}
			LogicalOp::Project(project) => {
				if project.is_layer() {
					return unchanged(LogicalOp::Project(project), predicate);
				}
				match through_projection(project, predicate, cx) {
					Through::Moved(rewritten) => Outcome::Rewritten(rewritten),
					Through::Kept(project, predicate) => {
						unchanged(LogicalOp::Project(project), predicate)
					}
				}
			}
			LogicalOp::Subquery(LogicalSubquery {
				input: plan,
			}) => match into_subquery(plan, predicate, cx) {
				Ok(rewritten) => Outcome::Rewritten(rewritten),
				Err((plan, predicate)) => unchanged(
					LogicalOp::Subquery(LogicalSubquery {
						input: plan,
					}),
					predicate,
				),
			},
			// Anything else beneath the filter: the filter stays where it is.
			other => unchanged(other, predicate),
		}
	})
}

/// What the user's projection says about each name a predicate above it may
/// read.
struct Projection {
	/// Names a predicate above may substitute: each projected as a
	/// caller-frame expression that is free to duplicate, with no coercion,
	/// landing at exactly that name.
	substitutable: HashMap<String, LogicalExpr>,
	/// Names whose value above the projection differs from the value beneath
	/// in a way no substitution expresses.
	opaque: HashSet<String>,
	/// Whether `*` is projected, so a name that is neither substitutable nor
	/// opaque is the same field on both sides.
	passes_through: bool,
}

impl Projection {
	fn of(project: &LogicalProject, permission_checked: &HashSet<String>) -> Option<Self> {
		let LogicalFields::Select(fields) = &project.fields else {
			return None;
		};
		if matches!(project.input.as_ref(), LogicalOp::Aggregate(_)) {
			return None;
		}
		if project.input.any(|node| matches!(node, LogicalOp::Values(_))) {
			return None;
		}
		let mut out = Self {
			substitutable: HashMap::new(),
			opaque: HashSet::new(),
			passes_through: false,
		};
		for field in fields {
			match field {
				LogicalField::All => out.passes_through = true,
				LogicalField::Single {
					expr,
					alias,
					kind,
					frame,
				} => {
					// A predicate beneath the projection is applied before
					// the fields are evaluated, so a field that is not inert
					// — that writes, or that can raise — runs for fewer rows
					// than the projection as written. One such field
					// withdraws the whole projection, however the predicate
					// reads it.
					if !expr.read_only() || may_raise(expr, permission_checked) {
						return None;
					}
					let path = alias.clone().unwrap_or_else(|| expr.output_path());
					let Some(root) = path.parts().first() else {
						continue;
					};
					let crate::expr::logical::OutputPathPart::Field(root) = root else {
						continue;
					};
					let root = root.clone();
					let plain = path.as_field().is_some()
						&& kind.is_none() && frame.mode == EvalMode::Caller
						&& frame.auth.is_none()
						&& free_to_duplicate(expr);
					if plain
						&& !out.opaque.contains(&root)
						&& !out.substitutable.contains_key(&root)
					{
						out.substitutable.insert(root, expr.clone());
					} else {
						out.substitutable.remove(&root);
						out.opaque.insert(root);
					}
				}
			}
		}
		for omitted in &project.omit {
			if let LogicalExpr::Field(name) = omitted.path_root() {
				out.substitutable.remove(name.as_str());
				out.opaque.insert(name.to_string());
			} else {
				return None;
			}
		}
		Some(out)
	}

	/// Whether `term` reads the projected row only in ways the projection's
	/// input row answers the same after substitution.
	fn admits(&self, term: &LogicalExpr, cx: &RuleContext<'_>) -> bool {
		if reads_row_opaquely(term, cx.registry) {
			return false;
		}
		visit_positions(term, true, &mut |e, at_row| {
			if at_row && let LogicalExpr::Field(name) = e {
				let name = name.as_str();
				if self.opaque.contains(name)
					|| (!self.substitutable.contains_key(name) && !self.passes_through)
				{
					return std::ops::ControlFlow::Break(());
				}
			}
			std::ops::ControlFlow::Continue(())
		})
		.is_continue()
	}

	/// `term` in terms of the projection's input row.
	fn substitute(&self, mut term: LogicalExpr) -> LogicalExpr {
		rewrite_positions(&mut term, true, &mut |e, at_row| {
			if at_row
				&& let LogicalExpr::Field(name) = e
				&& let Some(expr) = self.substitutable.get(name.as_str())
			{
				*e = expr.clone();
			}
		});
		term
	}
}

/// Whether substituting `expr` for each mention of its name costs no more
/// than the mention did: a path ([`is_path`]), a literal, a parameter, a
/// constant, or a comparison or logical operator over those. Anything else
/// — a call, arithmetic, a method — is evaluated once per mention after
/// substitution where the projection evaluated it once per row, and a
/// predicate over it drives no index, since an index is keyed by a path; a
/// name projected as one is opaque to the predicate above.
fn free_to_duplicate(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Literal(_) | LogicalExpr::Param(_) | LogicalExpr::Constant(_) => true,
		LogicalExpr::Binary {
			left,
			op,
			right,
		} => is_comparison_or_logical(op) && free_to_duplicate(left) && free_to_duplicate(right),
		LogicalExpr::Prefix {
			op: PrefixOperator::Not,
			expr,
		} => free_to_duplicate(expr),
		LogicalExpr::Prefix {
			op:
				PrefixOperator::Positive
				| PrefixOperator::Negate
				| PrefixOperator::Range
				| PrefixOperator::RangeInclusive
				| PrefixOperator::Cast(_),
			..
		}
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Postfix {
			..
		}
		| LogicalExpr::Invoke {
			..
		}
		| LogicalExpr::Call(_)
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_)
		| LogicalExpr::Method {
			..
		}
		| LogicalExpr::Filter {
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
		} => false,
		LogicalExpr::Field(_)
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
		| LogicalExpr::Destructure {
			..
		}
		| LogicalExpr::Optional {
			..
		} => is_path(expr),
	}
}

/// Whether `expr` is a path: a field or the document, or a step over a path
/// — an access, an index whose index expression is free to duplicate, `[0]`,
/// `[$]`, `.*`, a flatten, a destructure whose aliased bodies are paths, or
/// an optional step whose continuation is a path. The current value stands
/// as a path root inside a continuation or an aliased body.
fn is_path(expr: &LogicalExpr) -> bool {
	match expr {
		LogicalExpr::Field(_) | LogicalExpr::Doc | LogicalExpr::Current => true,
		// A field read off a value that may be a record id fetches that
		// record, deriving its `COMPUTED` fields afresh, so the two
		// evaluations substitution leaves behind — the predicate's and the
		// projection's — need not answer the same. An iteration parameter is
		// no exemption: it names the row in hand, but a row is not always a
		// record, and a second read off a bare record id fetches again.
		LogicalExpr::Access {
			base,
			..
		} => is_path(base) && !may_dereference(base),
		// `.*` and `.{ … }` fetch a record id the same way a field read does,
		// so they carry the same guard.
		LogicalExpr::Each(base) => is_path(base) && !may_dereference(base),
		LogicalExpr::First(base)
		| LogicalExpr::Last(base)
		| LogicalExpr::Flatten {
			base,
			..
		} => is_path(base),
		LogicalExpr::Index {
			base,
			index,
		} => is_path(base) && free_to_duplicate(index),
		LogicalExpr::Destructure {
			base,
			fields,
		} => is_path(base) && !may_dereference(base) && fields.iter().all(destructure_field_is_path),
		LogicalExpr::Optional {
			base,
			then,
		} => is_path(base) && is_path(then),
		LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Constant(_)
		| LogicalExpr::Filter {
			..
		}
		| LogicalExpr::Method {
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
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => false,
	}
}

fn destructure_field_is_path(field: &LogicalDestructureField) -> bool {
	match field {
		LogicalDestructureField::All(_)
		| LogicalDestructureField::Field(_)
		| LogicalDestructureField::Rest => true,
		LogicalDestructureField::Aliased(_, body) => is_path(body),
		LogicalDestructureField::Destructure(_, fields) => {
			fields.iter().all(destructure_field_is_path)
		}
	}
}

/// A comparison, a containment test, or `AND` / `OR` / `NOT`: an operator
/// whose evaluation is one step over its operands. `MATCHES` and `KNN` are
/// not: each is answered by an access path of the statement it is written in.
fn is_comparison_or_logical(op: &BinaryOperator) -> bool {
	match op {
		BinaryOperator::Equal
		| BinaryOperator::ExactEqual
		| BinaryOperator::NotEqual
		| BinaryOperator::AllEqual
		| BinaryOperator::AnyEqual
		| BinaryOperator::LessThan
		| BinaryOperator::LessThanEqual
		| BinaryOperator::MoreThan
		| BinaryOperator::MoreThanEqual
		| BinaryOperator::Contain
		| BinaryOperator::NotContain
		| BinaryOperator::ContainAll
		| BinaryOperator::ContainAny
		| BinaryOperator::ContainNone
		| BinaryOperator::Inside
		| BinaryOperator::NotInside
		| BinaryOperator::AllInside
		| BinaryOperator::AnyInside
		| BinaryOperator::NoneInside
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::And
		| BinaryOperator::Or => true,
		BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_)
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::Add
		| BinaryOperator::Subtract
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive => false,
	}
}

/// What [`through_projection`] did with a predicate over a projection.
enum Through {
	/// Some conjunct moved beneath the projection: the projection over a
	/// filter over its input, under the filter of the conjuncts kept, if any.
	Moved(LogicalOp),
	/// No conjunct moved; the projection and the predicate as they came.
	Kept(LogicalProject, LogicalExpr),
}

/// The conjuncts of `predicate` the projection admits, moved beneath it.
fn through_projection(
	project: LogicalProject,
	predicate: LogicalExpr,
	cx: &RuleContext<'_>,
) -> Through {
	let Some(projection) = Projection::of(&project, cx.permission_checked_params) else {
		return Through::Kept(project, predicate);
	};
	let (moved, kept): (Vec<LogicalExpr>, Vec<LogicalExpr>) =
		split_and(predicate).into_iter().partition(|term| projection.admits(term, cx));
	if moved.is_empty() {
		let predicate = and_all(kept).expect("a predicate has at least one conjunct");
		return Through::Kept(project, predicate);
	}
	let LogicalProject {
		input,
		fields,
		omit,
	} = project;
	let moved = moved.into_iter().map(|term| projection.substitute(term)).collect();
	let beneath = LogicalOp::Project(LogicalProject {
		input: Box::new(LogicalOp::Filter(LogicalFilter {
			input,
			predicate: and_all(moved).expect("at least one conjunct moved"),
		})),
		fields,
		omit,
	});
	Through::Moved(match and_all(kept) {
		Some(predicate) => LogicalOp::Filter(LogicalFilter {
			input: Box::new(beneath),
			predicate,
		}),
		None => beneath,
	})
}

/// Where a pushed predicate lands inside a subquery.
enum PushRoot {
	/// Beneath the subquery's `SELECT *`.
	IdentityProjection,
	/// Over the subquery's root, which emits rows as they are.
	Rows,
}

/// Whether `predicate` may move inside a subquery whose plan is `plan`, and
/// where it lands.
///
/// Three things must hold. The subquery must return its rows unchanged — its
/// root is an identity projection with no `OMIT`, or it has no projection and
/// its root emits rows: a stored source, a filter, a permission filter, a
/// join or a layer, which sets columns on the row it passes through — or
/// filtering before the root is not the same as filtering after it. No
/// source may be a literal, whose rows a projection can reshape rather than
/// pass through. And the predicate must not read a row-scoped parameter,
/// because `$parent` inside a subquery denotes that subquery's caller: moving
/// the predicate in would silently rebind it.
fn pushable(plan: &LogicalOp, predicate: &LogicalExpr, row_scoped: &[&str]) -> Option<PushRoot> {
	let (root, beneath) = match plan {
		LogicalOp::Project(LogicalProject {
			input,
			fields,
			omit,
		}) => {
			if plan.is_layer() {
				(PushRoot::Rows, plan)
			} else if !omit.is_empty() || !is_identity_projection(fields) {
				return None;
			} else {
				(PushRoot::IdentityProjection, input.as_ref())
			}
		}
		LogicalOp::Source(LogicalSource {
			source,
			..
		}) => match source {
			SourceRef::Table(_)
			| SourceRef::RecordId(_)
			| SourceRef::Mock(_)
			| SourceRef::Param(_)
			| SourceRef::Dynamic(_) => (PushRoot::Rows, plan),
		},
		LogicalOp::Filter(_) | LogicalOp::PermissionFilter(_) | LogicalOp::Join(_) => {
			(PushRoot::Rows, plan)
		}
		LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::UnionAll(_)
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
		| LogicalOp::Sequence(_)
		| LogicalOp::Let(_)
		| LogicalOp::Return(_)
		| LogicalOp::Foreach(_)
		| LogicalOp::Info(_)
		| LogicalOp::Sleep(_) => return None,
	};
	if beneath.any(|n| matches!(n, LogicalOp::Values(_))) {
		return None;
	}
	if mentions_row_scoped_deep(predicate, row_scoped) {
		return None;
	}
	Some(root)
}

/// `predicate` moved inside the subquery `plan`, or both handed back. A
/// subquery whose root is the user's projection takes the conjuncts the
/// projection admits, through it; see [`through_projection`].
fn into_subquery(
	plan: Box<LogicalOp>,
	predicate: LogicalExpr,
	cx: &RuleContext<'_>,
) -> Result<LogicalOp, (Box<LogicalOp>, LogicalExpr)> {
	if matches!(plan.as_ref(), LogicalOp::Project(_)) && !plan.is_layer() {
		let LogicalOp::Project(project) = *plan else {
			unreachable!("matched a projection")
		};
		if is_identity_projection(&project.fields) && project.omit.is_empty() {
			return into_subquery_rows(
				Box::new(LogicalOp::Project(project)),
				predicate,
				cx.row_scoped,
			);
		}
		// A conjunct naming a row-scoped parameter stays outside, where the
		// name denotes this statement's row.
		let (outside, candidates): (Vec<LogicalExpr>, Vec<LogicalExpr>) =
			split_and(predicate.clone())
				.into_iter()
				.partition(|term| mentions_row_scoped_deep(term, cx.row_scoped));
		let Some(candidates) = and_all(candidates) else {
			return Err((Box::new(LogicalOp::Project(project)), predicate));
		};
		return match through_projection(project, candidates, cx) {
			Through::Moved(inner) => {
				let subquery = LogicalOp::Subquery(LogicalSubquery {
					input: Box::new(inner),
				});
				Ok(match and_all(outside) {
					Some(predicate) => LogicalOp::Filter(LogicalFilter {
						input: Box::new(subquery),
						predicate,
					}),
					None => subquery,
				})
			}
			Through::Kept(project, _) => Err((Box::new(LogicalOp::Project(project)), predicate)),
		};
	}
	into_subquery_rows(plan, predicate, cx.row_scoped)
}

/// `predicate` moved over or beneath the root of the subquery `plan`, which
/// returns its rows unchanged ([`pushable`]), or both handed back.
fn into_subquery_rows(
	plan: Box<LogicalOp>,
	predicate: LogicalExpr,
	row_scoped: &[&str],
) -> Result<LogicalOp, (Box<LogicalOp>, LogicalExpr)> {
	let Some(root) = pushable(&plan, &predicate, row_scoped) else {
		return Err((plan, predicate));
	};
	let filtered = |input: Box<LogicalOp>| {
		LogicalOp::Filter(LogicalFilter {
			input,
			predicate,
		})
	};
	let inner = match root {
		PushRoot::IdentityProjection => {
			let LogicalOp::Project(LogicalProject {
				input: projected,
				fields,
				omit,
			}) = *plan
			else {
				unreachable!("pushable classified the root as a projection")
			};
			LogicalOp::Project(LogicalProject {
				input: Box::new(filtered(projected)),
				fields,
				omit,
			})
		}
		PushRoot::Rows => filtered(plan),
	};
	Ok(LogicalOp::Subquery(LogicalSubquery {
		input: Box::new(inner),
	}))
}

/// Whether the projection returns each row unchanged, as `SELECT *` does.
pub(super) fn is_identity_projection(fields: &LogicalFields) -> bool {
	matches!(fields, LogicalFields::Select(selected) if selected.as_slice() == [LogicalField::All])
}

#[cfg(test)]
mod tests {
	use std::ops::ControlFlow;
	use std::sync::Arc;

	use surrealdb_strand::TableName;

	use super::super::super::lower_test as lower;
	use super::super::test_support::{rewritten, rewritten_sql};
	use super::*;
	use crate::expr::Kind;
	use crate::expr::logical::{EvalFrame, FieldPath, FieldSchema, Schema};

	fn filters(op: &LogicalOp) -> usize {
		let mut n = 0;
		let _ = op.visit(&mut |node| {
			if matches!(node, LogicalOp::Filter(_)) {
				n += 1;
			}
			ControlFlow::<()>::Continue(())
		});
		n
	}

	/// The lines of a rendered plan above its first `Subquery`.
	fn outer(rendered: &str) -> Vec<&str> {
		rendered.lines().take_while(|l| !l.contains("Subquery")).collect()
	}

	// --- merge ---

	#[test]
	fn adjacent_filters_merge_with_the_inner_predicate_first() {
		// A predicate that throws must do so on exactly the rows it did
		// before, so the inner predicate has to remain the left operand.
		let plan = rewritten_sql("SELECT * FROM (SELECT * FROM t WHERE a) WHERE b");
		assert!(plan.contains("Filter[a AND b]"), "{plan}");
	}

	#[test]
	fn a_true_conjunct_is_dropped_when_filters_merge() {
		let inner = LogicalOp::Filter(LogicalFilter {
			input: Box::new(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from("t")),
				schema: None,
				with: None,
				for_update: false,
			})),
			predicate: LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Bool(true)),
		});
		let plan = rewritten(LogicalOp::Filter(LogicalFilter {
			input: Box::new(inner),
			predicate: LogicalExpr::Field("b".into()),
		}));
		assert_eq!(plan.to_string(), "Filter[b]\n  Source[t]");
	}

	// --- through a user projection ---

	#[test]
	fn a_conjunct_moves_through_a_projection_by_alias_substitution() {
		// `WHERE` is beneath the projection in a statement, so a filter above
		// a user projection comes from nesting.
		let plan = rewritten_sql("SELECT * FROM (SELECT a AS b, c FROM t) WHERE b = 2 AND c > 1");
		assert_eq!(
			plan,
			"\
Project[*]
  Subquery
    Project[a AS b, c]
      Filter[a = 2 AND c > 1]
        Source[t]"
		);
	}

	#[test]
	fn a_passthrough_name_moves_and_a_missing_one_stays() {
		// `*` passes `d` through; without `*`, `d` above the projection is
		// `NONE` and reads differently beneath.
		let plan = rewritten_sql("SELECT * FROM (SELECT *, a AS b FROM t) WHERE d = 1");
		assert_eq!(
			plan,
			"\
Project[*]
  Subquery
    Project[*, a AS b]
      Filter[d = 1]
        Source[t]"
		);
		let plan = rewritten_sql("SELECT * FROM (SELECT a AS b FROM t) WHERE d = 1");
		assert_eq!(
			plan,
			"\
Project[*]
  Filter[d = 1]
    Subquery
      Project[a AS b]
        Source[t]"
		);
	}

	#[test]
	fn a_conjunct_reading_the_row_whole_stays_above_the_projection() {
		for sql in [
			"SELECT * FROM (SELECT a AS b FROM t) WHERE $this.b = 2",
			"SELECT * FROM (SELECT a AS b FROM t) WHERE b IN (SELECT VALUE x FROM u LIMIT 1)",
			"SELECT * FROM (SELECT a AS b FROM t) WHERE type::field('b') = 2",
		] {
			let plan = rewritten_sql(sql);
			assert!(outer(&plan).iter().any(|l| l.contains("Filter[")), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn a_path_a_literal_a_param_or_a_comparison_substitutes() {
		for (sql, beneath) in [
			("SELECT * FROM (SELECT a AS x FROM t) WHERE x = 1", "Filter[a = 1]"),
			("SELECT * FROM (SELECT a = 1 AS x FROM t) WHERE x", "Filter[a = 1]"),
			(
				"SELECT * FROM (SELECT a > 1 AND b < 2 AS x FROM t) WHERE x",
				"Filter[a > 1 AND b < 2]",
			),
			("SELECT * FROM (SELECT $p AS x FROM t) WHERE x = 1", "Filter[$p = 1]"),
			("SELECT * FROM (SELECT 1 AS x FROM t) WHERE x = 1", "Filter[1 = 1]"),
		] {
			let plan = rewritten_sql(sql);
			assert!(plan.contains(beneath), "`{sql}`:\n{plan}");
			assert!(!outer(&plan).iter().any(|l| l.contains("Filter[")), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn a_path_that_may_dereference_a_record_keeps_its_predicate_above() {
		// A step off a field may read a record link, and the fetch derives the
		// linked record's `COMPUTED` fields afresh. Substituting evaluates the
		// path twice — once as the predicate, once as the projection — which
		// may answer differently, so the predicate stays where the projection
		// evaluated the path once.
		for sql in [
			"SELECT * FROM (SELECT a.b.c AS x FROM t) WHERE x = 1",
			"SELECT * FROM (SELECT a.*.b AS x FROM t) WHERE x = 1",
			"SELECT * FROM (SELECT link.coin AS x FROM t) WHERE x",
		] {
			let plan = rewritten_sql(sql);
			assert!(outer(&plan).iter().any(|l| l.contains("Filter[")), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn a_call_or_arithmetic_projection_keeps_its_predicate_above() {
		// Substituting a call or arithmetic evaluates it once per mention where
		// the projection evaluated it once per row, and no index answers it.
		for sql in [
			"SELECT * FROM (SELECT *, crypto::sha512(body) AS h FROM t) WHERE h = 'x' OR h = 'y'",
			"SELECT * FROM (SELECT string::len(a) AS n FROM t) WHERE n = 3",
			"SELECT * FROM (SELECT a + 1 AS n FROM t) WHERE n = 3",
			"SELECT * FROM (SELECT -a AS n FROM t) WHERE n = 3",
			"SELECT * FROM (SELECT <int> a AS n FROM t) WHERE n = 3",
			"SELECT * FROM (SELECT [a, b] AS n FROM t) WHERE n CONTAINS 3",
		] {
			let plan = rewritten_sql(sql);
			assert!(outer(&plan).iter().any(|l| l.contains("Filter[")), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn a_projection_field_that_is_not_inert_keeps_the_predicate_above() {
		// A filter beneath the projection is applied first, so a field that
		// writes or can raise would reach fewer rows than the projection as
		// written — whatever the predicate itself reads. An index step is a
		// field of the second kind even where it is the substituted field.
		for sql in [
			"SELECT * FROM (SELECT a[0] AS x FROM t) WHERE x = 1",
			"SELECT * FROM (SELECT a, <int> bad AS c FROM t) WHERE a = 1",
			"SELECT * FROM (SELECT a, b + 1 AS s FROM t) WHERE a = 1",
			"SELECT * FROM (SELECT a, string::len(b) AS n FROM t) WHERE a = 1",
		] {
			let plan = rewritten_sql(sql);
			assert!(outer(&plan).iter().any(|l| l.contains("Filter[")), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn an_impure_or_nested_or_omitted_projection_keeps_its_predicate_above() {
		for sql in [
			// A call is never substituted; this one would also draw a second
			// random number.
			"SELECT * FROM (SELECT rand::int() AS r FROM t) WHERE r > 2",
			// A nested landing position is not a name to substitute.
			"SELECT * FROM (SELECT a.b FROM t) WHERE a.b = 1",
			// An omitted field reads `NONE` above and its value beneath.
			"SELECT * FROM (SELECT * OMIT a FROM t) WHERE a = 1",
			// A `VALUE` row is not an object.
			"SELECT * FROM (SELECT VALUE a FROM t) WHERE a = 1",
			// The projection above a grouped query is the aggregation's.
			"SELECT * FROM (SELECT a, count() AS n FROM t GROUP BY a) WHERE n > 1",
		] {
			let plan = rewritten_sql(sql);
			assert!(outer(&plan).iter().any(|l| l.contains("Filter[")), "`{sql}`:\n{plan}");
		}
	}

	#[test]
	fn a_conjunct_splits_around_the_projection() {
		// `b` substitutes; `$this.c` observes the projected row and stays.
		let plan =
			rewritten_sql("SELECT * FROM (SELECT a AS b, c FROM t) WHERE b = 2 AND $this.c = 3");
		assert_eq!(
			plan,
			"\
Project[*]
  Filter[$this.c = 3]
    Subquery
      Project[a AS b, c]
        Filter[a = 2]
          Source[t]"
		);
	}

	#[test]
	fn a_layer_column_is_never_substituted() {
		// The column is in a non-caller frame: the value under `c` above the
		// layer is not `a` beneath it.
		let layer = LogicalOp::Project(LogicalProject {
			input: Box::new(LogicalOp::Source(LogicalSource {
				source: SourceRef::Table(TableName::from("t")),
				schema: Some(Arc::new(Schema::closed(vec![FieldSchema::new(
					FieldPath::field("a"),
					Kind::Int,
				)]))),
				with: None,
				for_update: false,
			})),
			fields: LogicalFields::Select(vec![
				LogicalField::All,
				LogicalField::column(
					"c",
					LogicalExpr::Field("a".into()),
					Some(Kind::Int),
					EvalFrame::computed(None),
				),
			]),
			omit: vec![],
		});
		let filter = LogicalOp::Filter(LogicalFilter {
			input: Box::new(layer),
			predicate: LogicalExpr::Binary {
				left: Box::new(LogicalExpr::Field("c".into())),
				op: crate::expr::BinaryOperator::Equal,
				right: Box::new(LogicalExpr::Literal(
					crate::expr::logical::LogicalLiteral::Integer(2),
				)),
			},
		});
		let plan = rewritten(filter.clone());
		assert_eq!(plan, filter, "{plan}");
	}

	// --- into a subquery ---

	#[test]
	fn a_predicate_moves_into_a_star_subquery_and_merges() {
		let plan = rewritten(lower("SELECT * FROM (SELECT * FROM t WHERE a > 1) WHERE b = 2"));
		assert_eq!(filters(&plan), 1, "the two predicates should be one:\n{plan}");
		let rendered = plan.to_string();
		let inner =
			rendered.lines().skip_while(|l| !l.contains("Subquery")).collect::<Vec<_>>().join("\n");
		assert!(inner.contains("Filter[a > 1 AND b = 2]"), "{rendered}");
		assert!(inner.contains("Source[t]"), "{rendered}");
	}

	#[test]
	fn a_projecting_subquery_keeps_a_predicate_it_does_not_supply_outside() {
		// The inner projection supplies `b` alone: `c` above it is `NONE`.
		let plan = rewritten(lower("SELECT * FROM (SELECT a AS b FROM t) WHERE c = 2"));
		assert_eq!(filters(&plan), 1);
		let rendered = plan.to_string();
		assert!(outer(&rendered).iter().any(|l| l.contains("Filter[c = 2]")), "{rendered}");
	}

	#[test]
	fn an_omitting_subquery_keeps_a_predicate_on_the_omitted_field_outside() {
		let rendered = rewritten_sql("SELECT * FROM (SELECT * OMIT x FROM t) WHERE x = 2");
		assert!(outer(&rendered).iter().any(|l| l.contains("Filter[x = 2]")), "{rendered}");
		// A field the projection passes through moves.
		let rendered = rewritten_sql("SELECT * FROM (SELECT * OMIT x FROM t) WHERE b = 2");
		assert!(!outer(&rendered).iter().any(|l| l.contains("Filter[")), "{rendered}");
		assert!(rendered.contains("      Filter[b = 2]\n        Source[t]"), "{rendered}");
	}

	#[test]
	fn a_predicate_reading_parent_is_never_pushed() {
		// `$parent` inside a subquery denotes that subquery's caller, so
		// moving the predicate in would rebind it to a different row.
		for sql in [
			"SELECT * FROM (SELECT * FROM t) WHERE a = $parent.b",
			"SELECT * FROM (SELECT * FROM t) WHERE a = $this.b",
			"SELECT * FROM (SELECT * FROM t) WHERE a IN (SELECT id FROM u WHERE x = $parent.y)",
		] {
			let rendered = rewritten_sql(sql);
			assert!(
				outer(&rendered).iter().any(|l| l.contains("Filter[")),
				"predicate should stay outside for `{sql}`:\n{rendered}"
			);
		}
	}

	#[test]
	fn a_literal_source_blocks_the_push() {
		// A projection over a plain value can reshape the row, so filtering
		// before it is not the same as filtering after it.
		let rendered = rewritten_sql("SELECT * FROM (SELECT * FROM [1, 2]) WHERE a = 2");
		assert!(outer(&rendered).iter().any(|l| l.contains("Filter[a = 2]")), "{rendered}");
	}

	#[test]
	fn a_scalar_subquery_is_rewritten_with_its_statement() {
		let plan = rewritten_sql(
			"SELECT * FROM t WHERE a IN (SELECT VALUE b FROM (SELECT * FROM u WHERE c) WHERE d)",
		);
		assert!(plan.contains("Filter[c AND d]"), "{plan}");
	}
}
