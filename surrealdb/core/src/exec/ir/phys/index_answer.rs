//! The conjuncts of a predicate that a B-tree scan's access covers.
//!
//! A B-tree scan lists the records whose index entries lie in its access, which was chosen from
//! comparisons of the predicate. [`answers`] decides whether one conjunct states such a comparison:
//! the conjunct is read as the planner holds it ([`read_conjunct`]) and matched against the access
//! by the planner's own matcher ([`strip_index_conditions_in`]), so the conjuncts a scan covers are
//! the ones the index analysis recognises, by one definition.
//!
//! A full-text scan lists the records its probe admits; [`answers_search`] decides whether one
//! conjunct is that probe, and [`answers_union`] whether one conjunct is the disjunction of the
//! probes of an index union's full-text branches.
//!
//! A conjunct the access covers judges nothing an exact scan has not already judged
//! ([`IndexScanAttrs::exact`]), and a filter of such conjuncts alone is the scan's own recheck of
//! each record it fetches. A value fixed per execution and unknown at plan time (the statement
//! clock, an operand of a slot) is read as a parameter, which states no operand of an access.
//!
//! [`IndexScanAttrs::exact`]: super::ops::IndexScanAttrs::exact

#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::BTreeMap;
use std::ops::Bound;

use super::access_read::read_conjunct;
use super::conjunct::{def_op, literal_record};
use crate::exec::index::access_path::BTreeAccess;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::ids::ValueId;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::planner::util::{
	idiom_matches_column, strip_index_conditions_in, try_literal_to_value,
};
use crate::expr::logical::LogicalExpr;
use crate::expr::operator::MatchesOperator;
use crate::expr::{BinaryOperator, Idiom};
use crate::val::{Array, Object, Range, Set, Value};

/// How deep a literal aggregate is read before it is taken as no literal.
const DEPTH: usize = 32;

/// Whether the conjunct `conjunct` states a comparison the B-tree `access` over the index columns
/// `cols` covers. A conjunct the reading cannot take whole is covered by no access.
pub(super) fn answers(ir: &Ir, conjunct: ValueId, access: &BTreeAccess, cols: &[Idiom]) -> bool {
	let mut operands = Vec::new();
	read_conjunct(ir, conjunct, &mut operands)
		.is_ok_and(|expr| strip_index_conditions_in(&expr, access, cols, None).is_none())
}

/// Whether the conjunct `conjunct` is the probe of a full-text scan over the index column `column`
/// that searches for `query` under `operator`: a `MATCHES` with that operator whose left side is
/// the column and whose right side is `query` as a string literal. The scan lists exactly the
/// records its probe admits.
pub(super) fn answers_search(
	ir: &Ir,
	conjunct: ValueId,
	column: &Idiom,
	operator: &MatchesOperator,
	query: &str,
) -> bool {
	let mut operands = Vec::new();
	read_conjunct(ir, conjunct, &mut operands).is_ok_and(|expr| {
		is_probe(
			&expr,
			&SearchProbe {
				column,
				operator,
				query,
			},
		)
	})
}

/// What a full-text scan searches for: its index column, its operator (which names its clause)
/// and its query.
pub(super) struct SearchProbe<'a> {
	pub(super) column: &'a Idiom,
	pub(super) operator: &'a MatchesOperator,
	pub(super) query: &'a str,
}

/// Whether `expr` is the probe `probe` states: a `MATCHES` with its operator whose left side is
/// its column and whose right side is its query as a string literal.
fn is_probe(expr: &LogicalExpr, probe: &SearchProbe<'_>) -> bool {
	let LogicalExpr::Binary {
		left,
		op: BinaryOperator::Matches(held),
		right,
	} = expr
	else {
		return false;
	};
	held == probe.operator
		&& matches!(try_literal_to_value(right), Some(Value::String(text)) if text.as_str() == probe.query)
		&& idiom_matches_column(left, probe.column)
}

/// Whether the conjunct `conjunct` is the disjunction of the probes of an index union's
/// full-text branches, `branches`: an `OR` of terms each the probe of one branch, with every
/// branch's probe among them. A union lists exactly the records some branch lists, and each
/// branch exactly the records its probe admits, so such a conjunct holds of every row the union
/// lists. A term that is no branch's probe, or a branch no term probes, leaves it unanswered.
pub(super) fn answers_union(ir: &Ir, conjunct: ValueId, branches: &[SearchProbe<'_>]) -> bool {
	let mut operands = Vec::new();
	let Ok(expr) = read_conjunct(ir, conjunct, &mut operands) else {
		return false;
	};
	let mut terms = Vec::new();
	disjuncts(&expr, &mut terms);
	let mut probed = vec![false; branches.len()];
	for term in terms {
		let Some(at) = branches.iter().position(|branch| is_probe(term, branch)) else {
			return false;
		};
		probed[at] = true;
	}
	probed.iter().all(|probed| *probed)
}

/// Whether the conjunct `conjunct` states the exact spatial predicate `filter` that a point-only
/// `SPATIAL` index union's inline trailer filter already evaluates.
pub(super) fn answers_spatial(
	ir: &Ir,
	conjunct: ValueId,
	filter: &crate::exec::index::access_path::SpatialPointFilter,
) -> bool {
	let mut operands = Vec::new();
	read_conjunct(ir, conjunct, &mut operands).is_ok_and(|expr| {
		crate::exec::index::analysis::expr_matches_spatial_point_filter(&expr, filter)
	})
}

/// The terms of `expr` read as a disjunction: the operands of every `OR` it nests, left to right.
fn disjuncts<'a>(expr: &'a LogicalExpr, into: &mut Vec<&'a LogicalExpr>) {
	if let LogicalExpr::Binary {
		left,
		op: BinaryOperator::Or,
		right,
	} = expr
	{
		disjuncts(left, into);
		disjuncts(right, into);
	} else {
		into.push(expr);
	}
}

/// The value `value` is when it is built of literals alone ([`literal_value`]).
pub(super) fn literal(ir: &Ir, value: ValueId) -> Option<Value> {
	literal_value(ir, value, DEPTH)
}

/// The value `value` is when it is built of literals alone: a literal, a table name, a record id
/// with a literal key, an array, a set or an object of such values, or a range between two.
fn literal_value(ir: &Ir, value: ValueId, depth: usize) -> Option<Value> {
	let depth = depth.checked_sub(1)?;
	let data = ir.op(def_op(ir, value)?)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	let each = |operands: &[ValueId]| -> Option<Vec<Value>> {
		operands.iter().map(|operand| literal_value(ir, *operand, depth)).collect()
	};
	if let ExprOp::Literal(held) = kind {
		return Some((**held).clone());
	}
	if let ExprOp::Table {
		name,
	} = kind
	{
		return Some(Value::Table(name.clone()));
	}
	if matches!(kind, ExprOp::RecordId(..)) {
		return literal_record(ir, value).map(Value::RecordId);
	}
	if matches!(kind, ExprOp::Array) {
		return each(&data.operands).map(|values| Value::Array(Array(values)));
	}
	if matches!(kind, ExprOp::Set) {
		let mut set = Set::new();
		for item in each(&data.operands)? {
			set.insert(item);
		}
		return Some(Value::Set(set));
	}
	if let ExprOp::Object {
		keys,
	} = kind
	{
		let values = each(&data.operands)?;
		let entries: BTreeMap<String, Value> = keys.iter().cloned().zip(values).collect();
		return Some(Value::Object(Object::from(entries)));
	}
	let ExprOp::Binary {
		op,
	} = kind
	else {
		return None;
	};
	let [start, end] = data.operands.as_slice() else {
		return None;
	};
	let (inclusive_start, inclusive_end) = if *op == BinaryOperator::Range {
		(true, false)
	} else if *op == BinaryOperator::RangeInclusive {
		(true, true)
	} else if *op == BinaryOperator::RangeSkip {
		(false, false)
	} else if *op == BinaryOperator::RangeSkipInclusive {
		(false, true)
	} else {
		return None;
	};
	let start = literal_value(ir, *start, depth)?;
	let end = literal_value(ir, *end, depth)?;
	Some(Value::Range(Box::new(Range {
		start: if inclusive_start {
			Bound::Included(start)
		} else {
			Bound::Excluded(start)
		},
		end: if inclusive_end {
			Bound::Included(end)
		} else {
			Bound::Excluded(end)
		},
	})))
}
