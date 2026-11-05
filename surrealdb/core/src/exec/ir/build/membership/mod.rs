//! The memberships of a statement, in the form the binder binds them: `x IN (SELECT VALUE k FROM t
//! WHERE c)`, its `NOT IN` form and the `CONTAINS` forms read as one, as a predicate over a
//! statement nested in a filter, which the VM evaluates.
//!
//! [`memberships_of`] first recognises the memberships a filter holds that run once as a join
//! ([`lift`]), and then writes every semi and anti join of the statement, a front-end's own
//! included, as the membership filter it stands for, at any depth of the statement, before the
//! statement binds. The rows a membership keeps are the join's, because the lift never changes
//! them.
//!
//! The rewrite runs on the binder's input, so everything the binder derives from a predicate (the
//! computed fields a permission reads, the access a filter offers) is derived from the predicate
//! as the VM evaluates it.

mod lift;

use std::borrow::Cow;
use std::ops::ControlFlow;

use crate::exec::function::FunctionRegistry;
use crate::exec::ir::error::IrError;
use crate::exec::ir::logical::seam::invariant;
use crate::expr::logical::{
	EvalFrame, EvalMode, GuardKind, JoinKind, LogicalEval, LogicalExpr, LogicalFields,
	LogicalFilter, LogicalJoin, LogicalLet, LogicalOp, LogicalPermissionFilter, LogicalProject,
	LogicalSequence, RelationAlias, SourceRef,
};
use crate::expr::operator::BinaryOperator;

/// The prefix of the parameters [`membership_filter`] binds a key to, once per probe row.
const MEMBERSHIP_KEY: &str = "__membership_key_";

/// Whether `name` is a parameter [`membership_filter`] binds a key to: a `LET` the binder makes,
/// which the request never wrote.
pub(crate) fn is_membership_key(name: &str) -> bool {
	name.starts_with(MEMBERSHIP_KEY)
}

/// Statement `op` with every membership of a filter written as the predicate the binder binds: the
/// memberships that run once as a join are recognised ([`lift`]), and every semi and anti join,
/// those included, is written as the membership predicate it stands for, at any depth of the
/// statement, the statements nested in its expressions included; `op` itself where it holds none.
///
/// `is_bound` says whether the statement binds a name around the point being bound, `registry`
/// decides which calls are deterministic.
///
/// A join lifted out of a table's permission is put back as a permission filter over its left
/// input, under the frame and `AUTH LIMIT` it carries. The terms held beneath the join are judged
/// before the membership, and the terms held above it after it: the one conjunction, read left to
/// right.
///
/// Answers an invariant error for a semi or anti join in a frame that holds no predicate, one with
/// no key, and one whose further keys read a probe row that is not an object inside a permission.
pub(crate) fn memberships_of<'a>(
	op: &'a LogicalOp,
	registry: &FunctionRegistry,
	is_bound: &dyn Fn(&str) -> bool,
) -> Result<Cow<'a, LogicalOp>, IrError> {
	let consults = lift::Consults {
		is_bound,
		registry,
	};
	match lift::lifted(op, consults) {
		Some(lifted) => unlifted(&lifted).map(|unlifted| Cow::Owned(unlifted.into_owned())),
		None => unlifted(op),
	}
}

/// `op` with every semi and anti join written as the membership filter it stands for, at any depth
/// of the statement; `op` itself where it holds none.
fn unlifted(op: &LogicalOp) -> Result<Cow<'_, LogicalOp>, IrError> {
	if !lifts_a_membership(op) {
		return Ok(Cow::Borrowed(op));
	}
	op.clone()
		.transform_up(&mut |node| {
			let mut node = match node {
				LogicalOp::Join(join) if matches!(join.kind, JoinKind::Semi | JoinKind::Anti) => {
					match membership_filter(join)? {
						// The one conjunction the moved terms and the membership make is the
						// predicate an access path is chosen from.
						LogicalOp::Filter(membership) => match *membership.input {
							LogicalOp::Filter(left) => merged_filters(LogicalFilter {
								input: left.input,
								predicate: LogicalExpr::Binary {
									left: Box::new(left.predicate),
									op: BinaryOperator::And,
									right: Box::new(membership.predicate),
								},
							}),
							input @ (LogicalOp::Source(_)
							| LogicalOp::Values(_)
							| LogicalOp::Match(_)
							| LogicalOp::Subquery(_)
							| LogicalOp::Write(_)
							| LogicalOp::Ddl(_)
							| LogicalOp::UnionAll(_)
							| LogicalOp::Join(_)
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
							| LogicalOp::Sleep(_)) => LogicalOp::Filter(LogicalFilter {
								input: Box::new(input),
								predicate: membership.predicate,
							}),
						},
						other @ (LogicalOp::Source(_)
						| LogicalOp::Values(_)
						| LogicalOp::Match(_)
						| LogicalOp::Subquery(_)
						| LogicalOp::Write(_)
						| LogicalOp::Ddl(_)
						| LogicalOp::UnionAll(_)
						| LogicalOp::Join(_)
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
						| LogicalOp::Sleep(_)) => other,
					}
				}
				LogicalOp::Filter(outer) => match *outer.input {
					LogicalOp::Filter(inner) if holds_a_membership(&inner.predicate) => {
						LogicalOp::Filter(LogicalFilter {
							input: inner.input,
							predicate: LogicalExpr::Binary {
								left: Box::new(inner.predicate),
								op: BinaryOperator::And,
								right: Box::new(outer.predicate),
							},
						})
					}
					input @ (LogicalOp::Source(_)
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
					| LogicalOp::Sleep(_)) => LogicalOp::Filter(LogicalFilter {
						input: Box::new(input),
						predicate: outer.predicate,
					}),
				},
				other @ (LogicalOp::Source(_)
				| LogicalOp::Values(_)
				| LogicalOp::Match(_)
				| LogicalOp::Subquery(_)
				| LogicalOp::Write(_)
				| LogicalOp::Ddl(_)
				| LogicalOp::UnionAll(_)
				| LogicalOp::Join(_)
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
				| LogicalOp::Sleep(_)) => other,
			};
			node.visit_scalars_mut(&mut |scalar| {
				scalar.visit_mut(&mut |expr| {
					if let LogicalExpr::Subquery(plan) = expr
						&& lifts_a_membership(plan)
					{
						let taken = std::mem::replace(
							plan.as_mut(),
							LogicalOp::Eval(LogicalEval {
								expr: LogicalExpr::Break,
							}),
						);
						**plan = unlifted(&taken)?.into_owned();
					}
					Ok::<(), IrError>(())
				})
			})?;
			Ok(node)
		})
		.map(Cow::Owned)
}

/// `filter` as one filter: the filters it stands directly over are merged into it, the predicate
/// of the filter beneath first, so the conjuncts keep their written order.
fn merged_filters(mut filter: LogicalFilter) -> LogicalOp {
	while let LogicalOp::Filter(beneath) = *filter.input {
		filter = LogicalFilter {
			input: beneath.input,
			predicate: LogicalExpr::Binary {
				left: Box::new(beneath.predicate),
				op: BinaryOperator::And,
				right: Box::new(filter.predicate),
			},
		};
	}
	LogicalOp::Filter(filter)
}

/// Whether a conjunct of `predicate` is a membership [`membership_filter`] put back.
fn holds_a_membership(predicate: &LogicalExpr) -> bool {
	match predicate {
		LogicalExpr::Binary {
			left,
			op: BinaryOperator::And,
			right,
		} => holds_a_membership(left) || holds_a_membership(right),
		LogicalExpr::Binary {
			op: BinaryOperator::Inside | BinaryOperator::NotInside,
			right,
			..
		} => matches!(right.as_ref(), LogicalExpr::Subquery(_)),
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
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_) => false,
	}
}

/// Whether `op`, or a statement nested in it, holds a semi or anti join.
fn lifts_a_membership(op: &LogicalOp) -> bool {
	op.visit_ops_deep(&mut |node| match node {
		LogicalOp::Join(join) if matches!(join.kind, JoinKind::Semi | JoinKind::Anti) => {
			ControlFlow::Break(())
		}
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

/// The filter semi or anti join `join` was lifted from: its left input kept where
/// `outer IN (SELECT VALUE key FROM right WHERE …)` holds (`NOT IN` for an anti join), the
/// subquery's condition the join's further keys and its residual, each read with `$parent` as the
/// left row and the subquery's row as the right one.
///
/// A further key reads the probe row once, where the predicate it came from reads `$parent` for
/// each row of the subquery. The two agree for an object row, which carries what it holds, and not
/// for a row that is a bare value (a record id), whose field is read from the record each time it
/// is named. Over such a row the key is bound once per probe row, as a `LET` of a block ahead of
/// the subquery, and the subquery reads the binding.
fn membership_filter(join: LogicalJoin) -> Result<LogicalOp, IrError> {
	// A join lifted out of a `PERMISSIONS` clause is that clause's predicate again, judged under
	// the frame and the `AUTH LIMIT` the join carries.
	let permission = match join.frame.mode {
		EvalMode::Caller if join.frame == EvalFrame::caller() => None,
		EvalMode::Permission => Some(GuardKind::Read),
		EvalMode::MutablePermission => Some(GuardKind::Write),
		EvalMode::Caller | EvalMode::Computed => {
			return Err(invariant("a semi or anti join in a frame with no predicate form"));
		}
	};
	let alias = join.alias.clone();
	let frame_auth = join.frame.auth.clone();
	let kind = join.kind;
	let filter = join.filter.clone();
	let mut on = join.on.clone().into_iter();
	let (left, right) = join.into_inputs();
	let Some((outer, key)) = on.next() else {
		return Err(invariant("a semi or anti join with no key"));
	};
	let bind_once = on.len() > 0 && !yields_object_rows(&left);
	if bind_once && permission.is_some() {
		return Err(invariant("a join key read off a probe row that is not an object"));
	}
	let in_subquery = |expr: LogicalExpr| in_subquery_frame(expr, &alias);
	let mut condition: Option<LogicalExpr> = None;
	let mut and = |term: LogicalExpr| {
		condition = Some(match condition.take() {
			Some(left) => LogicalExpr::Binary {
				left: Box::new(left),
				op: BinaryOperator::And,
				right: Box::new(term),
			},
			None => term,
		});
	};
	let mut bound: Vec<LogicalOp> = Vec::new();
	for (at, (outer_key, right_key)) in on.enumerate() {
		let read = if bind_once {
			let name = format!("{MEMBERSHIP_KEY}{at}");
			bound.push(LogicalOp::Let(LogicalLet {
				name: name.clone(),
				kind: None,
				value: Box::new(LogicalOp::Eval(LogicalEval {
					expr: outer_key,
				})),
				synthetic: true,
			}));
			LogicalExpr::Param(crate::expr::Param::from(name))
		} else {
			in_subquery(outer_key)
		};
		and(LogicalExpr::Binary {
			left: Box::new(in_subquery(right_key)),
			op: BinaryOperator::Equal,
			right: Box::new(read),
		});
	}
	if let Some(residual) = filter {
		and(in_subquery(residual));
	}
	let input = match condition {
		Some(predicate) => LogicalOp::Filter(LogicalFilter {
			input: Box::new(right),
			predicate,
		}),
		None => right,
	};
	let subquery = LogicalOp::Project(LogicalProject {
		input: Box::new(input),
		fields: LogicalFields::Value {
			expr: in_subquery(key),
			alias: None,
		},
		omit: Vec::new(),
	});
	let op = match kind {
		JoinKind::Semi => BinaryOperator::Inside,
		JoinKind::Anti => BinaryOperator::NotInside,
		JoinKind::Inner | JoinKind::Left => return Err(invariant("an inner or left join")),
	};
	// The keys bound once are the statements of a block ahead of the subquery, whose rows the
	// block yields.
	let subquery = if bound.is_empty() {
		subquery
	} else {
		bound.push(subquery);
		LogicalOp::Sequence(LogicalSequence {
			stmts: bound,
		})
	};
	let predicate = LogicalExpr::Binary {
		left: Box::new(outer),
		op,
		right: Box::new(LogicalExpr::Subquery(Box::new(subquery))),
	};
	Ok(match permission {
		None => LogicalOp::Filter(LogicalFilter {
			input: Box::new(left),
			predicate,
		}),
		Some(guard) => LogicalOp::PermissionFilter(LogicalPermissionFilter {
			input: Box::new(left),
			predicate,
			auth: frame_auth,
			guard,
		}),
	})
}

/// Whether every row `op` yields is an object: read from a table or a record, or projected as a
/// set of fields, and passed on by operators that keep rows as they are.
fn yields_object_rows(op: &LogicalOp) -> bool {
	match op {
		LogicalOp::Source(source) => {
			matches!(source.source, SourceRef::Table(_) | SourceRef::RecordId(_))
		}
		LogicalOp::Project(project) => matches!(project.fields, LogicalFields::Select(_)),
		LogicalOp::Filter(inner) => yields_object_rows(&inner.input),
		LogicalOp::PermissionFilter(inner) => yields_object_rows(&inner.input),
		LogicalOp::Limit(inner) => yields_object_rows(&inner.input),
		LogicalOp::Sort(inner) => yields_object_rows(&inner.input),
		LogicalOp::Distinct(inner) => yields_object_rows(&inner.input),
		LogicalOp::Values(_)
		| LogicalOp::Match(_)
		| LogicalOp::Subquery(_)
		| LogicalOp::Write(_)
		| LogicalOp::Ddl(_)
		| LogicalOp::UnionAll(_)
		| LogicalOp::Join(_)
		| LogicalOp::Split(_)
		| LogicalOp::Aggregate(_)
		| LogicalOp::Window(_)
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
		| LogicalOp::Sleep(_) => false,
	}
}

/// `expr`, written in the frame of a join under `alias`, in the frame of the subquery it was
/// lifted from: a field of the right row (`alias.field`) is a field of the subquery's row, and a
/// field of the left row is a field of `$parent`.
fn in_subquery_frame(expr: LogicalExpr, alias: &RelationAlias) -> LogicalExpr {
	match expr {
		LogicalExpr::Access {
			base,
			field,
		} if *base == alias.field() => LogicalExpr::Field(field),
		LogicalExpr::Field(name) => LogicalExpr::Access {
			base: Box::new(LogicalExpr::Param(crate::expr::Param::from("parent".to_owned()))),
			field: name,
		},
		mut other @ (LogicalExpr::Literal(_)
		| LogicalExpr::Array(_)
		| LogicalExpr::Set(_)
		| LogicalExpr::Object(_)
		| LogicalExpr::RecordId(_)
		| LogicalExpr::Param(_)
		| LogicalExpr::Table(_)
		| LogicalExpr::Mock(_)
		| LogicalExpr::Constant(_)
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
		| LogicalExpr::Closure(_)
		| LogicalExpr::If(_)
		| LogicalExpr::Throw(_)
		| LogicalExpr::Break
		| LogicalExpr::Continue
		| LogicalExpr::Subquery(_)) => {
			for child in other.children_mut() {
				let taken = std::mem::replace(child, LogicalExpr::Break);
				*child = in_subquery_frame(taken, alias);
			}
			other
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::logical::{LogicalSource, SourceRef};

	fn source(source: SourceRef) -> LogicalOp {
		LogicalOp::Source(LogicalSource {
			source,
			schema: None,
			with: None,
			for_update: false,
		})
	}

	fn table(name: &str) -> LogicalOp {
		source(SourceRef::Table(name.into()))
	}

	fn field(name: &str) -> LogicalExpr {
		LogicalExpr::Field(name.into())
	}

	fn semi(kind: JoinKind, on: Vec<(LogicalExpr, LogicalExpr)>, left: LogicalOp) -> LogicalOp {
		LogicalOp::Join(LogicalJoin::new(
			left,
			table("b"),
			kind,
			on,
			None,
			RelationAlias::new(0),
			EvalFrame::caller(),
		))
	}

	fn aliased(name: &str) -> LogicalExpr {
		LogicalExpr::Access {
			base: Box::new(RelationAlias::new(0).field()),
			field: name.into(),
		}
	}

	/// The membership a semi join was lifted from: `x IN (SELECT VALUE k FROM b)` over the left
	/// input, read in the subquery's frame.
	#[test]
	fn a_semi_join_is_the_membership_it_was_lifted_from() {
		let op = semi(JoinKind::Semi, vec![(field("x"), aliased("k"))], table("a"));
		let LogicalOp::Filter(filter) = unlifted(&op).expect("un-lifts").into_owned() else {
			panic!("a filter");
		};
		assert!(matches!(*filter.input, LogicalOp::Source(_)));
		let LogicalExpr::Binary {
			left,
			op: BinaryOperator::Inside,
			right,
		} = filter.predicate
		else {
			panic!("an INSIDE");
		};
		assert_eq!(*left, field("x"));
		let LogicalExpr::Subquery(subquery) = *right else {
			panic!("a subquery");
		};
		let LogicalOp::Project(project) = *subquery else {
			panic!("a projection");
		};
		assert!(matches!(
			project.fields,
			LogicalFields::Value {
				expr: LogicalExpr::Field(_),
				..
			}
		));
	}

	/// An anti join is the negated membership.
	#[test]
	fn an_anti_join_is_the_negated_membership() {
		let op = semi(JoinKind::Anti, vec![(field("x"), aliased("k"))], table("a"));
		let LogicalOp::Filter(filter) = unlifted(&op).expect("un-lifts").into_owned() else {
			panic!("a filter");
		};
		assert!(matches!(
			filter.predicate,
			LogicalExpr::Binary {
				op: BinaryOperator::NotInside,
				..
			}
		));
	}

	/// A further key over rows that are not objects is bound once per probe row ahead of the
	/// subquery, in a block.
	#[test]
	fn a_further_key_over_bare_rows_is_bound_once_ahead_of_the_subquery() {
		let values = source(SourceRef::Param("rows".to_owned().into()));
		let op = semi(
			JoinKind::Semi,
			vec![(field("x"), aliased("k")), (field("y"), aliased("j"))],
			values,
		);
		let LogicalOp::Filter(filter) = unlifted(&op).expect("un-lifts").into_owned() else {
			panic!("a filter");
		};
		let LogicalExpr::Binary {
			right,
			..
		} = filter.predicate
		else {
			panic!("a membership");
		};
		let LogicalExpr::Subquery(subquery) = *right else {
			panic!("a subquery");
		};
		let LogicalOp::Sequence(block) = *subquery else {
			panic!("a block");
		};
		assert!(matches!(block.stmts.first(), Some(LogicalOp::Let(_))));
		assert!(matches!(block.stmts.last(), Some(LogicalOp::Project(_))));
	}

	/// A statement that holds no lifted membership is the statement itself.
	#[test]
	fn a_statement_with_no_membership_is_borrowed() {
		let op = table("a");
		assert!(matches!(unlifted(&op), Ok(Cow::Borrowed(_))));
	}

	/// A join lifted out of a permission is that permission's predicate again.
	#[test]
	fn a_join_lifted_out_of_a_permission_is_a_permission_filter() {
		let op = LogicalOp::Join(LogicalJoin::new(
			table("a"),
			table("b"),
			JoinKind::Semi,
			vec![(field("x"), aliased("k"))],
			None,
			RelationAlias::new(0),
			EvalFrame::permission(None),
		));
		let LogicalOp::PermissionFilter(filter) = unlifted(&op).expect("un-lifts").into_owned()
		else {
			panic!("a permission filter");
		};
		assert_eq!(filter.guard, GuardKind::Read);
	}
}
