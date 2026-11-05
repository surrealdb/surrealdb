//! The predicate of an access-path decision, rebuilt as the planner's logical expression.
//!
//! The index analyzer reads [`LogicalExpr`], and a predicate of the IR is ops of the expression
//! dialect, so [`predicate`] rebuilds the part of a predicate the analyzer can inspect: field
//! idioms rooted at the row of the filter with their index, first, last and method steps,
//! literals, parameters, the statement timestamp, comparisons, containment, `MATCHES`, `AND`,
//! `OR`, `NOT` and the `KNN` operators. The rebuilt expression feeds the analysis only; the
//! filter above the access path keeps the IR's own ops.
//!
//! The predicate is split into the conjuncts of its top-level `AND` chain, numbered in order. A
//! conjunct that holds any other sub-expression (a call, a flattening step, a step whose operand
//! the rebuild does not read) is read whole by [`super::access_read`], as the planner holds the
//! expression, so the analysis sees every call and path step the planner's own analysis sees. A
//! conjunct the whole reading cannot read, or that holds a form the analysis cannot inspect (an
//! opaque node: a nested statement, a closure, a field off a value that is not the row), is
//! withheld: the analysis never sees it, so no index is matched to it and it stays a residual of
//! the filter. A conjunct that names an index operator (`MATCHES`, `KNN`) is never withheld,
//! because no residual can evaluate one: an error reading it is the decision's error.
//!
//! A value of the statement that is fixed per execution and unknown at plan time (the statement
//! timestamp, an operand of a slot) is rebuilt as a parameter named after its position in
//! [`Reconstructed::operands`] and is never a literal, so the analyzer matches no index range from
//! it and a cached plan reads the timestamp of its own execution.

#![deny(clippy::wildcard_enum_match_arm)]

use surrealdb_strand::Strand;

use super::access_read::{owes_index_operator, read_conjunct, read_conjunct_whole};
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, KeyShape, ShortCircuitOp};
use crate::exec::ir::iface::ids::{RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::planner::util::try_value_to_logical_expr;
use crate::expr::logical::{
	LogicalExpr, LogicalOrder, LogicalOrdering, LogicalRecordIdKey, LogicalRecordIdLit,
};
use crate::expr::{BinaryOperator, Param};

/// The most ops one predicate is rebuilt from; a larger predicate is withheld whole, one conjunct
/// at a time.
const NODE_BUDGET: usize = 512;

/// A predicate as the index analyzer reads it.
#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct Reconstructed {
	/// The conjuncts the analyzer may read, each with its position in the predicate's `AND` chain.
	pub(crate) conjuncts: Vec<(usize, LogicalExpr)>,
	/// The positions of the conjuncts the analyzer never sees.
	pub(crate) withheld: Vec<usize>,
	/// The values of the container that the rebuilt parameters `__operand0`, `__operand1`, ...
	/// stand for, in that order.
	pub(crate) operands: Vec<ValueId>,
}

impl Reconstructed {
	/// The conjuncts of `exprs`, each readable by the analyzer, in order.
	pub(crate) fn of_exprs(exprs: impl IntoIterator<Item = LogicalExpr>) -> Self {
		Self {
			conjuncts: exprs.into_iter().enumerate().collect(),
			..Self::default()
		}
	}

	/// The conjuncts of the `AND` chain `cond` is, each readable by the analyzer.
	pub(crate) fn of_condition(cond: &LogicalExpr) -> Self {
		let mut leaves = Vec::new();
		and_leaves(cond, &mut leaves);
		Self::of_exprs(leaves.into_iter().cloned())
	}

	/// How many conjuncts the predicate has, withheld ones included.
	pub(crate) fn len(&self) -> usize {
		self.conjuncts.len() + self.withheld.len()
	}

	/// Whether the predicate has no conjunct.
	pub(crate) fn is_empty(&self) -> bool {
		self.len() == 0
	}

	/// The condition the analyzer reads: the readable conjuncts joined by `AND` in position order,
	/// or `None` when there are none.
	pub(crate) fn condition(&self) -> Option<LogicalExpr> {
		let mut conjuncts = self.conjuncts.iter().map(|(_, expr)| expr.clone());
		let first = conjuncts.next()?;
		Some(conjuncts.fold(first, |left, right| LogicalExpr::Binary {
			left: Box::new(left),
			op: BinaryOperator::And,
			right: Box::new(right),
		}))
	}
}

/// The conjuncts of the `AND` chain `expr` is, in order.
fn and_leaves<'a>(expr: &'a LogicalExpr, out: &mut Vec<&'a LogicalExpr>) {
	if let LogicalExpr::Binary {
		left,
		op: BinaryOperator::And,
		right,
	} = expr
	{
		and_leaves(left, out);
		and_leaves(right, out);
	} else {
		out.push(expr);
	}
}

/// The predicate `value` yields, split into conjuncts and rebuilt for the analyzer.
///
/// `value` is the value a filter's `Clause(Where)` region yields. A conjunct that holds an op the
/// rebuild does not read, a field that is not read from the row of a `Clause(Where)` region, or
/// more ops than the rebuild's budget is read whole ([`read_conjunct`]), and withheld when that
/// reading fails or holds an opaque node and it names no index operator.
pub(crate) fn predicate(ir: &Ir, value: ValueId) -> Result<Reconstructed, IrError> {
	let mut out = Reconstructed::default();
	predicate_into(ir, value, &mut out)?;
	Ok(out)
}

/// The predicate of the `Clause(Where)` region `region`, appended to `out` after the conjuncts it
/// holds: the conjuncts of its `AND` chain numbered on from them.
///
/// Answers `false` for a region that yields no value, which has no predicate to read.
pub(crate) fn predicate_of_region(
	ir: &Ir,
	region: RegionId,
	out: &mut Reconstructed,
) -> Result<bool, IrError> {
	let Some(value) = yielded(ir, region) else {
		return Ok(false);
	};
	predicate_into(ir, value, out)?;
	Ok(true)
}

/// The value of the cone of `leaf`, a conjunct's value, that rebuilds as `expr`, the first in a
/// walk from `leaf` through each op's operands and the values its regions yield. A value is read
/// as the predicate read its conjunct: by the rebuild, or whole ([`read_conjunct`]) where the
/// rebuild does not read it. Parameters for per-execution operands are named by their index in
/// `operands`, the operand list of the predicate that `expr` was rebuilt in.
pub(super) fn value_rebuilt_as(
	ir: &Ir,
	leaf: ValueId,
	operands: &[ValueId],
	expr: &LogicalExpr,
) -> Option<ValueId> {
	let mut pending = vec![leaf];
	let mut seen = FxHashSet::default();
	while let Some(value) = pending.pop() {
		if !seen.insert(value) {
			continue;
		}
		let mut rebuild = Rebuild {
			ir,
			operands: operands.to_vec(),
			budget: NODE_BUDGET,
		};
		match rebuild.expr(value) {
			Some(rebuilt) if rebuilt == *expr => return Some(value),
			Some(_) => {}
			None => {
				let mut read = operands.to_vec();
				if read_conjunct(ir, value, &mut read).is_ok_and(|whole| whole == *expr) {
					return Some(value);
				}
			}
		}
		let Some(ValueDef::OpResult {
			op,
			..
		}) = ir.value(value).map(|data| data.def)
		else {
			continue;
		};
		let Some(data) = ir.op(op) else {
			continue;
		};
		pending.extend(data.operands.iter().copied());
		pending.extend(data.regions.iter().filter_map(|region| yielded(ir, *region)));
	}
	None
}

/// The values of the conjuncts of the `Clause(Where)` region `region`, in the order
/// [`predicate_of_region`] numbers them, and whether the region yields its value under a
/// truthiness coercion. `None` for a region that yields no value.
pub(super) fn conjunct_values(ir: &Ir, region: RegionId) -> Option<(Vec<ValueId>, bool)> {
	let value = yielded(ir, region)?;
	let truthy = matches!(expr_def(ir, value), Some((ExprOp::Truthy, _)));
	let mut leaves = Vec::new();
	split_and(ir, value, &mut leaves);
	Some((leaves, truthy))
}

/// Appends the conjuncts of the `AND` chain `value` is to `out`, numbered on from the conjuncts it
/// holds. The operands the conjuncts stand for are numbered on from `out.operands`.
///
/// A conjunct the rebuild cannot read is read whole ([`read_conjunct`]). One that names an index
/// operator ([`owes_index_operator`]) answers the reading's error, since no residual can evaluate
/// it; any other is withheld when the reading fails or makes an opaque node
/// ([`read_conjunct_whole`]).
fn predicate_into(ir: &Ir, value: ValueId, out: &mut Reconstructed) -> Result<(), IrError> {
	let mut leaves = Vec::new();
	split_and(ir, value, &mut leaves);
	let base = out.len();
	for (at, leaf) in leaves.into_iter().enumerate() {
		let position = base + at;
		let mut rebuild = Rebuild {
			ir,
			operands: out.operands.clone(),
			budget: NODE_BUDGET,
		};
		match rebuild.expr(leaf) {
			Some(expr) => {
				out.operands = rebuild.operands;
				out.conjuncts.push((position, expr));
			}
			None if owes_index_operator(ir, leaf) => {
				let expr = read_conjunct(ir, leaf, &mut out.operands)?;
				out.conjuncts.push((position, expr));
			}
			None => {
				let mut operands = out.operands.clone();
				match read_conjunct_whole(ir, leaf, &mut operands) {
					Ok((expr, false)) => {
						out.operands = operands;
						out.conjuncts.push((position, expr));
					}
					Ok(_) | Err(_) => out.withheld.push(position),
				}
			}
		}
	}
	Ok(())
}

/// The ordering the `Clause(OrderKey)` regions `regions` state, with the flags of `keys`: `None`
/// when a key's value is not one the analyzer reads.
pub(crate) fn ordering_of(
	ir: &Ir,
	keys: &[(bool, bool, bool)],
	regions: &[RegionId],
) -> Option<LogicalOrdering> {
	if keys.len() != regions.len() {
		return None;
	}
	let mut order = Vec::with_capacity(keys.len());
	for ((collate, numeric, ascending), region) in keys.iter().zip(regions) {
		let value = yielded(ir, *region)?;
		let mut rebuild = Rebuild {
			ir,
			operands: Vec::new(),
			budget: NODE_BUDGET,
		};
		let expr = rebuild.expr(value)?;
		if !rebuild.operands.is_empty() {
			return None;
		}
		order.push(LogicalOrder {
			value: expr,
			collate: *collate,
			numeric: *numeric,
			direction: *ascending,
		});
	}
	Some(LogicalOrdering::Order(order))
}

/// The expression op that defines `value`, with its data.
pub(super) fn expr_def(
	ir: &Ir,
	value: ValueId,
) -> Option<(&ExprOp, &crate::exec::ir::core::op::OpData)> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let data = ir.op(op)?;
	match &data.kind {
		OpKind::Expr(kind) => Some((kind, data)),
		OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// `value` without the truthiness coercions around it, which a conjunct's meaning does not
/// depend on.
pub(super) fn strip_truthy(ir: &Ir, mut value: ValueId) -> ValueId {
	while let Some((ExprOp::Truthy, data)) = expr_def(ir, value) {
		match data.operands.first() {
			Some(inner) => value = *inner,
			None => break,
		}
	}
	value
}

/// The value the single block of `region` yields.
pub(super) fn yielded(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let last = ops_in(ir, region).last()?;
	let data = ir.op(last)?;
	match &data.kind {
		OpKind::Expr(ExprOp::Yield) => data.operands.first().copied(),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// The left value and the right value of `value` when an `AND` defines it.
fn and_parts(ir: &Ir, value: ValueId) -> Option<(ValueId, ValueId)> {
	let (
		ExprOp::ShortCircuit {
			op: ShortCircuitOp::And,
		},
		data,
	) = expr_def(ir, value)?
	else {
		return None;
	};
	let left = *data.operands.first()?;
	let right = yielded(ir, *data.regions.first()?)?;
	Some((left, right))
}

/// Appends the conjuncts of the `AND` chain `value` is to `out`, in order.
///
/// The chain is walked with an explicit stack, so a chain of any length splits completely and no
/// returned conjunct is an `AND`.
fn split_and(ir: &Ir, value: ValueId, out: &mut Vec<ValueId>) {
	let mut pending = vec![value];
	while let Some(value) = pending.pop() {
		let value = strip_truthy(ir, value);
		match and_parts(ir, value) {
			Some((left, right)) => {
				pending.push(right);
				pending.push(left);
			}
			None => out.push(value),
		}
	}
}

/// The parameter standing for the per-execution value `value`, named after its position in
/// `operands`, where it is appended when it is not there.
pub(super) fn operand_param(operands: &mut Vec<ValueId>, value: ValueId) -> LogicalExpr {
	let at = match operands.iter().position(|seen| *seen == value) {
		Some(at) => at,
		None => {
			operands.push(value);
			operands.len() - 1
		}
	};
	LogicalExpr::Param(Param::from(format!("__operand{at}")))
}

/// Whether `value` is the row of a filter, a write's `WHERE`, a sort key or a permission: the
/// argument of a `Clause(Where)`, `Clause(WriteCond)`, `Clause(OrderKey)` or `PermissionFrame`
/// region.
pub(super) fn is_row(ir: &Ir, value: ValueId) -> bool {
	let Some(ValueDef::BlockArg {
		block,
		..
	}) = ir.value(value).map(|data| data.def)
	else {
		return false;
	};
	ir.block(block).and_then(|block| ir.region(block.parent)).is_some_and(|region| {
		matches!(
			region.kind,
			RegionKind::Clause(ClauseRole::Where | ClauseRole::WriteCond | ClauseRole::OrderKey)
				| RegionKind::PermissionFrame
		)
	})
}

/// Whether `value` is an argument of a `Statement` region, which holds the statement timestamp.
pub(super) fn is_statement_argument(ir: &Ir, value: ValueId) -> bool {
	let Some(ValueDef::BlockArg {
		block,
		..
	}) = ir.value(value).map(|data| data.def)
	else {
		return false;
	};
	ir.block(block)
		.and_then(|block| ir.region(block.parent))
		.is_some_and(|region| matches!(region.kind, RegionKind::Statement(_)))
}

/// Whether an op computes `value` in a region no clause region encloses: a value of the statement,
/// computed before the rows its clauses judge.
fn computed_outside_clauses(ir: &Ir, value: ValueId) -> bool {
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(value).map(|data| data.def)
	else {
		return false;
	};
	let mut region = Some(ir.enclosing_region(op));
	while let Some(at) = region {
		let Some(data) = ir.region(at) else {
			return false;
		};
		if matches!(data.kind, RegionKind::Clause(_)) {
			return false;
		}
		if matches!(data.kind, RegionKind::Statement(_)) {
			return true;
		}
		region = data.parent.map(|owner| ir.enclosing_region(owner));
	}
	true
}

/// The state of rebuilding one conjunct.
struct Rebuild<'a> {
	ir: &'a Ir,
	/// The values the parameters standing for per-execution operands name, in order.
	operands: Vec<ValueId>,
	/// The ops left to rebuild before the conjunct is withheld.
	budget: usize,
}

impl Rebuild<'_> {
	/// The parameter standing for the per-execution value `value`.
	fn operand(&mut self, value: ValueId) -> LogicalExpr {
		operand_param(&mut self.operands, value)
	}

	/// Whether `value` is the row of a filter or a sort key.
	fn is_row(&self, value: ValueId) -> bool {
		is_row(self.ir, value)
	}

	/// Whether `value` is an argument of a `Statement` region, which holds the statement
	/// timestamp.
	fn is_statement_argument(&self, value: ValueId) -> bool {
		is_statement_argument(self.ir, value)
	}

	/// `value`, an element of a record id's key, as the analyzer reads it, when it is a literal or
	/// a parameter.
	fn key_element(&mut self, value: ValueId) -> Option<LogicalExpr> {
		let expr = self.expr(value)?;
		matches!(expr, LogicalExpr::Literal(_) | LogicalExpr::Param(_)).then_some(expr)
	}

	/// `value` as the analyzer reads it, or `None` when it holds a form the analyzer does not read.
	///
	/// A value an op computes outside every clause, which a `KNN` operator's query vector moved out
	/// of its predicate is ([`super::knn::hoist_vectors`]), holds one value for every row the
	/// clause judges: where its form is not read, it is a per-execution operand.
	fn expr(&mut self, value: ValueId) -> Option<LogicalExpr> {
		match self.expr_form(value) {
			Some(expr) => Some(expr),
			None if computed_outside_clauses(self.ir, value) => Some(self.operand(value)),
			None => None,
		}
	}

	/// [`Self::expr`], reading the form of `value` alone.
	fn expr_form(&mut self, value: ValueId) -> Option<LogicalExpr> {
		let value = strip_truthy(self.ir, value);
		self.budget = self.budget.checked_sub(1)?;
		if self.is_statement_argument(value) {
			return Some(self.operand(value));
		}
		let (kind, data) = expr_def(self.ir, value)?;
		if let ExprOp::Literal(literal) = kind {
			return try_value_to_logical_expr((**literal).clone());
		}
		if let ExprOp::FreeParam {
			name,
		}
		| ExprOp::SessionParam {
			name,
		}
		| ExprOp::CatalogParam {
			name,
		} = kind
		{
			return Some(LogicalExpr::Param(Param::from(name.clone())));
		}
		if let ExprOp::Now {
			..
		} = kind
		{
			return Some(self.operand(value));
		}
		// A composite key is read when each element is a literal or a parameter: such a record id
		// names one record for the whole statement, as a literal key does.
		if let ExprOp::RecordId(attrs) = kind {
			let key = match &attrs.key_shape {
				KeyShape::Number(number) => LogicalRecordIdKey::Number(*number),
				KeyShape::String(string) => LogicalRecordIdKey::String(string.clone()),
				KeyShape::Uuid(uuid) => LogicalRecordIdKey::Uuid(*uuid),
				KeyShape::Array {
					len,
				} => {
					let len = usize::try_from(*len).ok()?;
					if data.operands.len() != len {
						return None;
					}
					let mut items = Vec::with_capacity(len);
					for operand in &data.operands {
						items.push(self.key_element(*operand)?);
					}
					LogicalRecordIdKey::Array(items)
				}
				KeyShape::Object {
					keys,
				} => {
					if data.operands.len() != keys.len() {
						return None;
					}
					let mut entries = Vec::with_capacity(keys.len());
					for (key, operand) in keys.iter().zip(&data.operands) {
						entries.push((key.clone(), self.key_element(*operand)?));
					}
					LogicalRecordIdKey::Object(entries)
				}
				KeyShape::Generate(_) | KeyShape::Range(_) => return None,
			};
			return Some(LogicalExpr::RecordId(Box::new(LogicalRecordIdLit {
				table: attrs.table.clone(),
				key,
			})));
		}
		if let ExprOp::Field {
			name,
		} = kind
		{
			return self.is_row(*data.operands.first()?).then(|| LogicalExpr::Field(name.clone()));
		}
		if let ExprOp::Doc = kind {
			return self
				.is_row(*data.operands.first()?)
				.then(|| LogicalExpr::Field(Strand::from("id")));
		}
		if let ExprOp::Access {
			field,
		} = kind
		{
			let base = self.expr(*data.operands.first()?)?;
			return Some(LogicalExpr::Access {
				base: Box::new(base),
				field: field.clone(),
			});
		}
		// The steps that address a component of a record id (`id[1]`, `id[0]`, `id.id().r`) are
		// read as the paths they are, so a conjunct over a component matches the index over it.
		if let ExprOp::Index = kind {
			let base = self.expr(*data.operands.first()?)?;
			let index = self.expr(*data.operands.get(1)?)?;
			return Some(LogicalExpr::Index {
				base: Box::new(base),
				index: Box::new(index),
			});
		}
		if let ExprOp::First = kind {
			let base = self.expr(*data.operands.first()?)?;
			return Some(LogicalExpr::First(Box::new(base)));
		}
		if let ExprOp::Last = kind {
			let base = self.expr(*data.operands.first()?)?;
			return Some(LogicalExpr::Last(Box::new(base)));
		}
		// Operands of a method: the dispatch result, the base and the arguments.
		if let ExprOp::Method {
			name,
		} = kind
		{
			let [_, base, args @ ..] = data.operands.as_slice() else {
				return None;
			};
			let base = self.expr(*base)?;
			let args = args.iter().map(|arg| self.expr(*arg)).collect::<Option<Vec<_>>>()?;
			return Some(LogicalExpr::Method {
				base: Box::new(base),
				name: name.clone(),
				args,
			});
		}
		if let ExprOp::Array = kind {
			let items =
				data.operands.iter().map(|op| self.expr(*op)).collect::<Option<Vec<_>>>()?;
			return Some(LogicalExpr::Array(items));
		}
		// `NOT (...)` is parsed as a call of `not`, which negates its argument as the prefix does.
		if let ExprOp::Call(attrs) = kind
			&& matches!(&attrs.function, crate::expr::Function::Normal(name) if name == "not")
			&& let [operand] = data.operands.as_slice()
		{
			let expr = self.expr(*operand)?;
			return Some(LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
				receiver: attrs.function.clone(),
				args: vec![expr],
			})));
		}
		if let ExprOp::Call(attrs) = kind
			&& matches!(
				&attrs.function,
				crate::expr::Function::Normal(name) if name.starts_with("vector::distance::")
			) && data.operands.len() >= 2
		{
			let ValueDef::OpResult {
				op,
				..
			} = self.ir.value(value)?.def
			else {
				return None;
			};
			let region = self.ir.enclosing_region(op);
			let mut args = Vec::with_capacity(data.operands.len());
			for operand in &data.operands {
				let saved_operands = self.operands.len();
				let saved_budget = self.budget;
				if let Some(expr) = self.expr_form(*operand) {
					args.push(expr);
				} else {
					self.operands.truncate(saved_operands);
					self.budget = saved_budget;
					if !super::access_regions::reads_region_args(self.ir, region, *operand) {
						args.push(LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
							receiver: crate::expr::Function::Normal(String::new()),
							args: Vec::new(),
						})));
					} else {
						return None;
					}
				}
			}
			return Some(LogicalExpr::Call(Box::new(crate::expr::logical::LogicalCall {
				receiver: attrs.function.clone(),
				args,
			})));
		}
		if let ExprOp::Prefix(op) = kind {
			let expr = self.expr(*data.operands.first()?)?;
			return Some(LogicalExpr::Prefix {
				op: (**op).clone(),
				expr: Box::new(expr),
			});
		}
		if let ExprOp::Binary {
			op,
		} = kind
		{
			let left = self.expr(*data.operands.first()?)?;
			let right = self.expr(*data.operands.get(1)?)?;
			return Some(LogicalExpr::Binary {
				left: Box::new(left),
				op: op.clone(),
				right: Box::new(right),
			});
		}
		if let ExprOp::ShortCircuit {
			op,
		} = kind
		{
			let op = match op {
				ShortCircuitOp::And => BinaryOperator::And,
				ShortCircuitOp::Or => BinaryOperator::Or,
				ShortCircuitOp::NullCoalescing | ShortCircuitOp::TernaryCondition => return None,
			};
			let left = self.expr(*data.operands.first()?)?;
			let right = self.expr(yielded(self.ir, *data.regions.first()?)?)?;
			return Some(LogicalExpr::Binary {
				left: Box::new(left),
				op,
				right: Box::new(right),
			});
		}
		None
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::logical::LogicalLiteral;

	fn field(name: &str) -> LogicalExpr {
		LogicalExpr::Field(Strand::from(name))
	}

	fn int(value: i64) -> LogicalExpr {
		LogicalExpr::Literal(LogicalLiteral::Integer(value))
	}

	fn equals(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(left),
			op: BinaryOperator::Equal,
			right: Box::new(right),
		}
	}

	fn and(left: LogicalExpr, right: LogicalExpr) -> LogicalExpr {
		LogicalExpr::Binary {
			left: Box::new(left),
			op: BinaryOperator::And,
			right: Box::new(right),
		}
	}

	#[test]
	fn the_condition_joins_the_readable_conjuncts_in_order() {
		let a = equals(field("a"), int(1));
		let b = equals(field("b"), int(2));
		let c = equals(field("c"), int(3));
		let predicate = Reconstructed::of_exprs([a.clone(), b.clone(), c.clone()]);
		assert_eq!(predicate.condition(), Some(and(and(a, b), c)));
	}

	#[test]
	fn a_withheld_conjunct_is_not_in_the_condition() {
		let a = equals(field("a"), int(1));
		let c = equals(field("c"), int(3));
		let predicate = Reconstructed {
			conjuncts: vec![(0, a.clone()), (2, c.clone())],
			withheld: vec![1],
			..Reconstructed::default()
		};
		assert_eq!(predicate.len(), 3);
		assert_eq!(predicate.condition(), Some(and(a, c)));
	}

	#[test]
	fn an_empty_predicate_has_no_condition() {
		let predicate = Reconstructed::default();
		assert!(predicate.is_empty());
		assert_eq!(predicate.condition(), None);
	}

	#[test]
	fn a_timestamp_operand_is_a_parameter_never_a_literal() {
		let source = include_str!("access_expr.rs");
		let code = source.split("#[cfg(test)]").next().expect("the module has code");
		assert!(code.contains("__operand"));
		assert!(!code.contains("LogicalLiteral::Datetime"));
	}
}
