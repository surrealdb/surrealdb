//! The conjuncts of a physical filter's predicate: their cost classes, which of them may move, and
//! the rewrites that reorder or drop them.
//!
//! A filter keeps a row when its predicate is truthy, and `AND` evaluates its operands left to
//! right, stopping at the first falsy one. Moving a conjunct past another changes which of the two
//! is evaluated for a row the first rejects, so a conjunct moves only past conjuncts that are
//! *total*: they raise no error, have no effect, and read nothing beyond the row, the parameters
//! and the literals they name. Between two total conjuncts the order decides only how much work a
//! rejected row costs, never what the statement raises, writes or returns. A conjunct that is not
//! total stays where it is written, and no conjunct crosses it.
//!
//! [`judge`] answers both facts of a conjunct from the ops that compute it: [`Judgement::total`]
//! is true when every op is one of the total forms below, and [`Judgement::class`] is the dearest
//! class of any op, the ops of nested regions included. A conjunct larger than [`NODE_BUDGET`] ops
//! is neither total nor cheap, so it stays in place.
//!
//! The total forms are literals, parameters, constants, the row's fields and its record id, array
//! and set constructors, a literal record id, the comparison, containment and boolean operators,
//! `NOT`, the lazy boolean operators, and a one-argument call of a builtin type test
//! (`type::is_*`). Every other op is not total: a field access may fetch a record, a computed read
//! may run a definition, a call of any other function or a method may raise, and a statement may do
//! anything.
//!
//! A chain is rebuilt with [`rebuild_lazy_and_chain`]: the first conjunct is evaluated before the
//! chain and each later one only for a row the conjuncts before it keep, so a conjunct that moves
//! to a later place is evaluated no more often than the order it moved to implies. A conjunct that
//! may raise and is evaluated before the chain, whatever its place in the chain, is never moved
//! into a lazy region: the rewrite declines, because its error would then be raised for fewer
//! rows. A rewrite checks every move it will make before it changes anything.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::{SmallVec, smallvec};

use super::ops::PhysOp;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, KeyShape, ShortCircuitOp};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::passes::rehome::{and_chain, can_rehome, rebuild_lazy_and_chain};
use crate::expr::{BinaryOperator, Function, PrefixOperator};
use crate::val::{Array, Object, RecordId, RecordIdKey, Value};

/// The most ops one conjunct is judged from before it is taken as neither total nor cheap.
const NODE_BUDGET: usize = 512;

/// What a conjunct costs to evaluate per row, cheapest first. A conjunct's class is the dearest
/// class of anything it evaluates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum CostClass {
	/// Comparisons of fields of the row, parameters and literals.
	Field,
	/// A call of a builtin function.
	Builtin,
	/// A call of a compiled catalog unit: a user-defined function, a model, a module, or a
	/// closure.
	Unit,
	/// A statement evaluated per row.
	Subquery,
}

/// What evaluating one conjunct can do, and what it costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Judgement {
	/// Whether evaluating the conjunct against any row raises no error, has no effect and reads
	/// nothing but the row's own fields, parameters and literals.
	pub(super) total: bool,
	pub(super) class: CostClass,
}

/// The ops that compute `value`, walked once each.
struct Judging<'a> {
	ir: &'a Ir,
	seen: FxHashSet<OpId>,
	budget: usize,
	total: bool,
	class: CostClass,
}

impl Judging<'_> {
	fn value(&mut self, value: ValueId) {
		if let Some(ValueDef::OpResult {
			op,
			..
		}) = self.ir.value(value).map(|data| data.def)
		{
			self.op(op);
		}
	}

	fn op(&mut self, op: OpId) {
		if !self.seen.insert(op) {
			return;
		}
		let Some(data) = self.ir.op(op) else {
			self.total = false;
			return;
		};
		if self.budget == 0 {
			self.total = false;
			self.class = CostClass::Subquery;
			return;
		}
		self.budget -= 1;
		let (total, class) = classify(self.ir, data);
		self.total &= total;
		self.class = self.class.max(class);
		for operand in &data.operands {
			self.value(*operand);
		}
		for region in &data.regions {
			for inner in ops_in(self.ir, *region) {
				self.op(inner);
			}
		}
	}
}

/// Whether `value` is the row of a `WHERE` clause: the argument of its region.
pub(super) fn is_row(ir: &Ir, value: Option<&ValueId>) -> bool {
	let Some(ValueDef::BlockArg {
		block,
		..
	}) = value.and_then(|value| ir.value(*value)).map(|data| data.def)
	else {
		return false;
	};
	ir.block(block)
		.and_then(|block| ir.region(block.parent))
		.is_some_and(|region| region.kind == RegionKind::Clause(ClauseRole::Where))
}

/// Whether builtin `name` called with one argument is a type test, which takes a value of any kind
/// and answers a boolean.
fn is_type_test(name: &str) -> bool {
	name.strip_prefix("type::is_").is_some_and(|kind| !kind.is_empty())
}

/// Whether binary operator `op` answers for any pair of values without raising: equality,
/// ordering, containment and the boolean connectives.
fn total_operator(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::AllEqual
			| BinaryOperator::AnyEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
			| BinaryOperator::And
			| BinaryOperator::Or
			| BinaryOperator::NullCoalescing
			| BinaryOperator::TenaryCondition
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
	)
}

/// Whether the op `data` is one of the total forms, judged alone: the answer [`judge`] folds over
/// every op that computes a conjunct.
pub(super) fn is_total_form(ir: &Ir, data: &OpData) -> bool {
	classify(ir, data).0
}

/// Whether the op `data` is a total form, and the class of the op alone.
fn classify(ir: &Ir, data: &OpData) -> (bool, CostClass) {
	let kind = match &data.kind {
		OpKind::Expr(kind) => kind,
		OpKind::Logical(_) | OpKind::Phys(_) => return (false, CostClass::Subquery),
	};
	match kind {
		ExprOp::Literal(_)
		| ExprOp::Array
		| ExprOp::Set
		| ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::Constant {
			..
		}
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::Truthy
		| ExprOp::Yield => (true, CostClass::Field),
		ExprOp::Field {
			..
		}
		| ExprOp::Doc => (is_row(ir, data.operands.first()), CostClass::Field),
		// A key whose every element is a literal names its record as a literal key does.
		ExprOp::RecordId(attrs) => (
			match attrs.key_shape {
				KeyShape::Number(_) | KeyShape::String(_) | KeyShape::Uuid(_) => true,
				KeyShape::Array {
					..
				}
				| KeyShape::Object {
					..
				} => data.operands.iter().all(|operand| {
					def_op(ir, *operand)
						.and_then(|op| ir.op(op))
						.is_some_and(|held| matches!(held.kind, OpKind::Expr(ExprOp::Literal(_))))
				}),
				KeyShape::Generate(_) | KeyShape::Range(_) => false,
			},
			CostClass::Field,
		),
		ExprOp::Binary {
			op,
		} => (total_operator(op), CostClass::Field),
		ExprOp::Prefix(op) => (matches!(**op, PrefixOperator::Not), CostClass::Field),
		ExprOp::Call(function) => match &function.function {
			Function::Normal(name) => {
				(data.operands.len() == 1 && is_type_test(name), CostClass::Builtin)
			}
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => (false, CostClass::Unit),
		},
		ExprOp::Method {
			..
		}
		| ExprOp::Now {
			..
		} => (false, CostClass::Builtin),
		ExprOp::ClosureCreate(_) | ExprOp::Invoke => (false, CostClass::Unit),
		ExprOp::Subquery(_) => (false, CostClass::Subquery),
		ExprOp::Object {
			..
		}
		| ExprOp::Table {
			..
		}
		| ExprOp::Mock(_)
		| ExprOp::ComputedRead(_)
		| ExprOp::Access {
			..
		}
		| ExprOp::Deref {
			..
		}
		| ExprOp::Index
		| ExprOp::First
		| ExprOp::Last
		| ExprOp::Each
		| ExprOp::Flatten {
			..
		}
		| ExprOp::Dispatch(_)
		| ExprOp::Destructure {
			..
		}
		| ExprOp::Optional
		| ExprOp::RepeatRecurse
		| ExprOp::SetField {
			..
		}
		| ExprOp::TupleRow {
			..
		}
		| ExprOp::Convert(_)
		| ExprOp::Postfix {
			..
		}
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Agg {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::DepthCharge {
			..
		} => (false, CostClass::Field),
	}
}

/// What evaluating the conjunct `value` can do, from every op that computes it.
pub(super) fn judge(ir: &Ir, value: ValueId) -> Judgement {
	let mut judging = Judging {
		ir,
		seen: FxHashSet::default(),
		budget: NODE_BUDGET,
		total: true,
		class: CostClass::Field,
	};
	judging.value(value);
	Judgement {
		total: judging.total,
		class: judging.class,
	}
}

/// The conjuncts `chain` holds with each run of adjacent total conjuncts ordered by cost class,
/// cheapest first and conjuncts of one class keeping their written order, or `None` when that
/// order is the written one. `judged` holds the judgement of each conjunct of `chain`.
pub(super) fn ordered(chain: &[ValueId], judged: &[Judgement]) -> Option<Vec<ValueId>> {
	if chain.len() < 2 || chain.len() != judged.len() {
		return None;
	}
	let mut order: Vec<usize> = (0..chain.len()).collect();
	let mut start = 0;
	while start < chain.len() {
		if !judged[start].total {
			start += 1;
			continue;
		}
		let mut end = start;
		while end < chain.len() && judged[end].total {
			end += 1;
		}
		order[start..end].sort_by_key(|&at| judged[at].class);
		start = end;
	}
	if order.iter().enumerate().all(|(at, &was)| at == was) {
		return None;
	}
	Some(order.into_iter().map(|at| chain[at]).collect())
}

/// A `WHERE` predicate the rewrites may rebuild: the filter, its input, the clause's closing yield,
/// the op that reads the predicate's value and the root of its `AND` chain.
pub(super) struct Site {
	pub(super) filter: OpId,
	pub(super) input: ValueId,
	/// The op that reads the chain's root: the clause's yield, or the truthiness coercion that
	/// stands between the two.
	holder: OpId,
	/// The root of the predicate's `AND` chain, without the truthiness coercions around it.
	pub(super) root: ValueId,
	/// Whether the filter also holds a predicate judged once, which a drop keeps in place.
	pub(super) once: bool,
}

/// The op that defines `value`.
pub(super) fn def_op(ir: &Ir, value: ValueId) -> Option<OpId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => ir.op(op).map(|_| op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// The `WHERE` predicate of the filter `filter`, or `None` for another op, a filter that holds no
/// predicate region, or a region that does not close with a yield.
pub(super) fn site(ir: &Ir, filter: OpId) -> Option<Site> {
	let data = ir.op(filter)?;
	if !matches!(data.kind, OpKind::Phys(PhysOp::Filter)) {
		return None;
	}
	let input = *data.operands.first()?;
	let region = ir.region(*data.regions.first()?)?;
	if region.kind != RegionKind::Clause(ClauseRole::Where) {
		return None;
	}
	let [block] = region.blocks.as_slice() else {
		return None;
	};
	let yielded = *ir.block(*block)?.ops.last()?;
	let closing = ir.op(yielded)?;
	let (OpKind::Expr(ExprOp::Yield), [predicate]) = (&closing.kind, closing.operands.as_slice())
	else {
		return None;
	};
	let mut holder = yielded;
	let mut root = *predicate;
	while let Some(def) = def_op(ir, root)
		&& ir.uses(root).len() == 1
		&& let Some(inner) =
			ir.op(def).and_then(|held| match (&held.kind, held.operands.as_slice()) {
				(OpKind::Expr(ExprOp::Truthy), [inner]) => Some(*inner),
				_ => None,
			}) {
		holder = def;
		root = inner;
	}
	Some(Site {
		filter,
		input,
		holder,
		root,
		once: data.regions.len() > 1,
	})
}

/// The conjuncts of the predicate of `site`, left to right.
pub(super) fn leaves(ir: &Ir, site: &Site) -> SmallVec<[ValueId; 4]> {
	and_chain(ir, site.root)
}

/// The value of `value` without the truthiness coercions around it.
pub(super) fn strip_truthy(ir: &Ir, mut value: ValueId) -> ValueId {
	while let Some(def) = def_op(ir, value)
		&& let Some(inner) =
			ir.op(def).and_then(|held| match (&held.kind, held.operands.as_slice()) {
				(OpKind::Expr(ExprOp::Truthy), [inner]) => Some(*inner),
				_ => None,
			}) {
		value = inner;
	}
	value
}

/// The record id `value` is when it is a record id literal whose key holds no expression.
///
/// A record id built from a literal key and a collapsed record id value are the same record, so
/// both answer, and so does one whose array or object key has a literal for every element. A key
/// that is a range or generated, or that has an operand of any other kind, is not a literal.
pub(super) fn literal_record(ir: &Ir, value: ValueId) -> Option<RecordId> {
	let data = ir.op(def_op(ir, strip_truthy(ir, value))?)?;
	let OpKind::Expr(kind) = &data.kind else {
		return None;
	};
	if let ExprOp::Literal(held) = kind {
		if let Value::RecordId(record) = &**held
			&& !matches!(record.key, RecordIdKey::Range(_))
		{
			return Some(record.clone());
		}
		return None;
	}
	let ExprOp::RecordId(attrs) = kind else {
		return None;
	};
	let element = |operand: &ValueId| {
		let held = ir.op(def_op(ir, *operand)?)?;
		match &held.kind {
			OpKind::Expr(ExprOp::Literal(value)) => Some((**value).clone()),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
		}
	};
	match &attrs.key_shape {
		KeyShape::Number(number) => Some(RecordId::new(attrs.table.clone(), *number)),
		KeyShape::String(string) => Some(RecordId::new(attrs.table.clone(), string.clone())),
		KeyShape::Uuid(uuid) => Some(RecordId::new(attrs.table.clone(), *uuid)),
		KeyShape::Array {
			..
		} => {
			let items: Option<Array> = data.operands.iter().map(element).collect();
			Some(RecordId::new(attrs.table.clone(), items?))
		}
		KeyShape::Object {
			keys,
		} => {
			if keys.len() != data.operands.len() {
				return None;
			}
			let entries: Option<Object> = keys
				.iter()
				.zip(&data.operands)
				.map(|(key, operand)| Some((key.clone(), element(operand)?)))
				.collect();
			Some(RecordId::new(attrs.table.clone(), entries?))
		}
		KeyShape::Generate(_) | KeyShape::Range(_) => None,
	}
}

/// Whether `conjunct` is `id = record` or `record = id`, which a point lookup of `record` holds
/// true of the one row it yields. Only a record id literal is matched: a key that holds an
/// expression may evaluate otherwise in the filter than it did for the lookup.
pub(super) fn pins_record(ir: &Ir, conjunct: ValueId, record: &RecordId) -> bool {
	let Some(data) = def_op(ir, strip_truthy(ir, conjunct)).and_then(|def| ir.op(def)) else {
		return false;
	};
	let (
		OpKind::Expr(ExprOp::Binary {
			op: BinaryOperator::Equal | BinaryOperator::ExactEqual,
		}),
		[left, right],
	) = (&data.kind, data.operands.as_slice())
	else {
		return false;
	};
	let other = match (reads_id(ir, *left), reads_id(ir, *right)) {
		(true, false) => *right,
		(false, true) => *left,
		_ => return false,
	};
	literal_record(ir, other).is_some_and(|literal| literal == *record)
}

/// Whether `conjunct` is `id INSIDE list` or `list CONTAINS id`, `list` an array or set literal
/// holding every record of `records`, which a read of those records holds true of each row it
/// yields: the row's `id` is the record it was read as.
pub(super) fn pins_record_list(ir: &Ir, conjunct: ValueId, records: &[RecordId]) -> bool {
	let Some(data) = def_op(ir, strip_truthy(ir, conjunct)).and_then(|def| ir.op(def)) else {
		return false;
	};
	let (
		OpKind::Expr(ExprOp::Binary {
			op,
		}),
		[left, right],
	) = (&data.kind, data.operands.as_slice())
	else {
		return false;
	};
	let list = if *op == BinaryOperator::Inside && reads_id(ir, *left) {
		*right
	} else if *op == BinaryOperator::Contain && reads_id(ir, *right) {
		*left
	} else {
		return false;
	};
	let Some(OpKind::Expr(ExprOp::Literal(held))) =
		def_op(ir, list).and_then(|def| ir.op(def)).map(|data| &data.kind)
	else {
		return false;
	};
	if let Value::Array(items) = &**held {
		records_in(items.iter(), records)
	} else if let Value::Set(items) = &**held {
		records_in(items.iter(), records)
	} else {
		false
	}
}

/// Whether every record of `records` is among the record ids of `items`.
fn records_in<'a>(items: impl Iterator<Item = &'a Value>, records: &[RecordId]) -> bool {
	let listed: std::collections::BTreeSet<&RecordId> = items
		.filter_map(|item| {
			if let Value::RecordId(record) = item {
				Some(record)
			} else {
				None
			}
		})
		.collect();
	records.iter().all(|record| listed.contains(record))
}

/// Whether `value` reads the row's record id: its `id` field, or the row itself where the row is
/// a record id.
fn reads_id(ir: &Ir, value: ValueId) -> bool {
	let Some(held) = def_op(ir, value).and_then(|def| ir.op(def)) else {
		return false;
	};
	match &held.kind {
		OpKind::Expr(ExprOp::Doc) => is_row(ir, held.operands.first()),
		OpKind::Expr(ExprOp::Field {
			name,
		}) => name.as_str() == "id" && is_row(ir, held.operands.first()),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// The `AND` ops along the left side of the chain whose root is `root`, root first: the ops a
/// rebuilt chain leaves unread.
fn chain_spine(ir: &Ir, root: ValueId) -> Vec<OpId> {
	let mut spine = Vec::new();
	let mut value = root;
	while let Some(op) = def_op(ir, value) {
		let Some(data) = ir.op(op) else {
			break;
		};
		let OpKind::Expr(ExprOp::ShortCircuit {
			op: ShortCircuitOp::And,
		}) = &data.kind
		else {
			break;
		};
		spine.push(op);
		match data.operands.first() {
			Some(left) if ir.uses(*left).len() == 1 => value = *left,
			Some(_) | None => break,
		}
	}
	spine
}

/// Whether the chain of `conjuncts` can be built before the op that reads the predicate: each is
/// read by one op alone, so moving its tree changes no other reader, and the move passes the
/// checks of [`can_rehome`].
///
/// A conjunct after the first is evaluated only for the rows the conjuncts before it keep. One
/// that may raise and is computed in the clause's own block is evaluated for every row, so
/// moving it behind another conjunct could drop or reorder its error: such a chain is not
/// rebuilt. Moving a total conjunct is unobservable.
fn can_rebuild(ir: &Ir, site: &Site, conjuncts: &[ValueId]) -> bool {
	let eager = ir.op(site.holder).map(|holder| holder.parent);
	conjuncts.iter().enumerate().all(|(slot, value)| {
		ir.uses(*value).len() == 1
			&& def_op(ir, *value).is_none_or(|def| {
				can_rehome(ir, def, InsertPoint::Before(site.holder), &[])
					&& (slot == 0
						|| ir.op(def).map(|data| data.parent) != eager
						|| judge(ir, *value).total)
			})
	})
}

/// Erases the replaced chain once the predicate reads another value: its `AND` ops, which take the
/// ops of their regions with them, and then the ops of each dropped conjunct that nothing reads.
///
/// Every op a dropped conjunct holds is total, so none has an effect that erasing it would lose.
fn retire(rw: &mut Rewriter<'_>, spine: &[OpId], dropped: &[ValueId]) -> Result<(), IrError> {
	for gate in spine {
		if rw.ir().op(*gate).is_none() {
			continue;
		}
		match rw.erase(*gate, EliminationReason::DeadPure) {
			Ok(()) | Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	let mut pending: Vec<ValueId> = dropped.to_vec();
	let mut budget = NODE_BUDGET;
	while let Some(value) = pending.pop() {
		let Some(op) = def_op(rw.ir(), value) else {
			continue;
		};
		let Some(data) = rw.ir().op(op) else {
			continue;
		};
		let unused = data.results.iter().all(|result| rw.ir().uses(*result).is_empty());
		if !unused || budget == 0 {
			continue;
		}
		budget -= 1;
		let operands = data.operands.clone();
		match rw.erase(op, EliminationReason::DeadPure) {
			Ok(()) => pending.extend(operands),
			Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	Ok(())
}

/// Rebuilds the chain of the predicate of `site` in the order `order`, which holds the same
/// conjuncts as the chain, and answers whether it did. Nothing is changed when a move cannot be
/// made.
pub(super) fn reorder(
	rw: &mut Rewriter<'_>,
	site: &Site,
	order: &[ValueId],
) -> Result<bool, IrError> {
	if !can_rebuild(rw.ir(), site, order) {
		return Ok(false);
	}
	let spine = chain_spine(rw.ir(), site.root);
	let rebuilt = rebuild_lazy_and_chain(rw, InsertPoint::Before(site.holder), order)?;
	rw.set_operand(site.holder, 0, rebuilt)?;
	retire(rw, &spine, &[])?;
	Ok(true)
}

/// Whether [`keep_per_row`] can make `kept`, conjuncts of the predicate of `site`, the predicate
/// judged per row.
pub(super) fn can_keep(ir: &Ir, site: &Site, kept: &[ValueId]) -> bool {
	kept.is_empty() || can_rebuild(ir, site, kept)
}

/// Makes `kept`, conjuncts of the predicate of `site` in written order, the predicate the filter
/// judges per row, or the literal `true` when it is empty, and erases the replaced chain. The
/// conjuncts left out must already be read elsewhere or be erased by the caller.
pub(super) fn keep_per_row(
	rw: &mut Rewriter<'_>,
	site: &Site,
	kept: &[ValueId],
) -> Result<(), IrError> {
	let spine = chain_spine(rw.ir(), site.root);
	let rebuilt = if kept.is_empty() {
		literal_true(rw, site)?
	} else {
		rebuild_lazy_and_chain(rw, InsertPoint::Before(site.holder), kept)?
	};
	rw.set_operand(site.holder, 0, rebuilt)?;
	retire(rw, &spine, &[])
}

/// The literal `true`, made before the op that reads the predicate of `site`.
fn literal_true(rw: &mut Rewriter<'_>, site: &Site) -> Result<ValueId, IrError> {
	let data = rw.ir().op(site.holder).ok_or(IrError::Stale("op"))?;
	let (frame, snapshot, loc) = (data.frame, data.snapshot, data.loc.clone());
	let kind = ExprOp::Literal(Box::new(Value::Bool(true)));
	let ty = result_type(&kind, &[]);
	let spec = OpSpec {
		kind: OpKind::Expr(kind),
		operands: SmallVec::new(),
		results: smallvec![ty],
		frame,
		snapshot,
		loc,
	};
	let made =
		rw.derived(&[site.filter], |rw| rw.create(InsertPoint::Before(site.holder), spec))?;
	rw.ir().op(made).and_then(|data| data.results.first().copied()).ok_or(IrError::Stale("op"))
}

/// Removes the conjuncts `dropped` from the predicate of `site`, whose chain is `chain`, and
/// answers whether it did.
///
/// The conjuncts that stay keep their order and their laziness. A predicate left with nothing to
/// judge removes the filter, unless the filter also holds a predicate judged once: the rows are
/// then still judged by it, so the predicate stays as the literal `true`. Nothing is changed when
/// a move cannot be made.
pub(super) fn drop_conjuncts(
	rw: &mut Rewriter<'_>,
	site: &Site,
	chain: &[ValueId],
	dropped: &[ValueId],
) -> Result<bool, IrError> {
	let kept: Vec<ValueId> = chain.iter().copied().filter(|v| !dropped.contains(v)).collect();
	if kept.len() == chain.len() {
		return Ok(false);
	}
	if kept.is_empty() {
		if !site.once {
			rw.replace_op(site.filter, &[site.input])?;
			return Ok(true);
		}
		let spine = chain_spine(rw.ir(), site.root);
		let truth = literal_true(rw, site)?;
		rw.set_operand(site.holder, 0, truth)?;
		retire(rw, &spine, dropped)?;
		return Ok(true);
	}
	if !can_rebuild(rw.ir(), site, &kept) {
		return Ok(false);
	}
	let spine = chain_spine(rw.ir(), site.root);
	let rebuilt = rebuild_lazy_and_chain(rw, InsertPoint::Before(site.holder), &kept)?;
	rw.set_operand(site.holder, 0, rebuilt)?;
	retire(rw, &spine, dropped)?;
	Ok(true)
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::location::{LocOrigin, Location};
	use crate::exec::ir::expr::ops::{ConvertAttrs, ConvertMode, ConvertSite, OnFail};
	use crate::exec::ir::iface::ids::{BlockId, FrameId, SnapshotId};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::expr::Kind;
	use crate::expr::logical::LogicalPostfixOperator;

	const LOC: Location = Location {
		stmt: 0,
		span: None,
		origin: LocOrigin::Synthesized,
	};

	fn make_where_ir() -> (Ir, BlockId, ValueId) {
		let mut ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Request,
		);
		let mut facts = FactCache::new();
		let top = ir.region(ir.top()).expect("top").blocks[0];
		let filter = {
			let mut rw = Rewriter::new(&mut ir, &mut facts);
			let src = rw
				.create(
					InsertPoint::End(top),
					OpSpec {
						kind: OpKind::Expr(ExprOp::Literal(Box::new(Value::Bool(true)))),
						operands: SmallVec::new(),
						results: smallvec![TypeKnowledge::Unknown],
						frame: FrameId::new(0),
						snapshot: SnapshotId::new(0),
						loc: LOC,
					},
				)
				.expect("src");
			let src_val = rw.ir().op(src).expect("src").results[0];
			rw.create(
				InsertPoint::End(top),
				OpSpec {
					kind: OpKind::Phys(PhysOp::Filter),
					operands: smallvec![src_val],
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("filter")
		};
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		let (_, block) = rw
			.create_region(filter, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])
			.expect("where region");
		let row = rw.ir().block(block).expect("where block").args[0];
		(ir, block, row)
	}

	fn emit(ir: &mut Ir, at: InsertPoint, kind: ExprOp, operands: &[ValueId]) -> ValueId {
		let mut facts = FactCache::new();
		let mut rw = Rewriter::new(ir, &mut facts);
		let op = rw
			.create(
				at,
				OpSpec {
					kind: OpKind::Expr(kind),
					operands: operands.iter().copied().collect(),
					results: smallvec![TypeKnowledge::Unknown],
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("emit op");
		rw.ir().op(op).expect("op").results[0]
	}

	#[test]
	fn judge_inspects_nested_regions_for_totality_and_cost() {
		let (mut ir, block, row) = make_where_ir();
		let field_a = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Field {
				name: "a".into(),
			},
			&[row],
		);
		let one = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Literal(Box::new(Value::from(1i64))),
			&[],
		);
		let zero = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Literal(Box::new(Value::from(0i64))),
			&[],
		);
		let eq_a = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Binary {
				op: BinaryOperator::Equal,
			},
			&[field_a, one],
		);

		// `a = 1 OR (1 / 0 = 1)`: the fallible division inside the lazy `OR` region makes the
		// whole conjunct non-total.
		let or_fallible = {
			let mut facts = FactCache::new();
			let mut rw = Rewriter::new(&mut ir, &mut facts);
			let or_op = rw
				.create(
					InsertPoint::End(block),
					OpSpec {
						kind: OpKind::Expr(ExprOp::ShortCircuit {
							op: ShortCircuitOp::Or,
						}),
						operands: smallvec![eq_a],
						results: smallvec![TypeKnowledge::Unknown],
						frame: FrameId::new(0),
						snapshot: SnapshotId::new(0),
						loc: LOC,
					},
				)
				.expect("or op");
			let (_, rhs_block) =
				rw.create_region(or_op, RegionKind::LazyOperand, &[]).expect("lazy region");
			let div = rw
				.create(
					InsertPoint::End(rhs_block),
					OpSpec {
						kind: OpKind::Expr(ExprOp::Binary {
							op: BinaryOperator::Divide,
						}),
						operands: smallvec![one, zero],
						results: smallvec![TypeKnowledge::Unknown],
						frame: FrameId::new(0),
						snapshot: SnapshotId::new(0),
						loc: LOC,
					},
				)
				.expect("div");
			let div_val = rw.ir().op(div).expect("div").results[0];
			rw.create(
				InsertPoint::End(rhs_block),
				OpSpec {
					kind: OpKind::Expr(ExprOp::Yield),
					operands: smallvec![div_val],
					results: SmallVec::new(),
					frame: FrameId::new(0),
					snapshot: SnapshotId::new(0),
					loc: LOC,
				},
			)
			.expect("yield");
			rw.ir().op(or_op).expect("or").results[0]
		};
		assert_eq!(
			judge(&ir, or_fallible),
			Judgement {
				total: false,
				class: CostClass::Field,
			}
		);
	}

	#[test]
	fn judge_distinguishes_total_not_from_fallible_prefix_postfix_each_and_convert() {
		let (mut ir, block, row) = make_where_ir();
		let field_a = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Field {
				name: "a".into(),
			},
			&[row],
		);

		let not_a = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Prefix(Box::new(PrefixOperator::Not)),
			&[field_a],
		);
		assert!(judge(&ir, not_a).total);

		let neg_a = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Prefix(Box::new(PrefixOperator::Negate)),
			&[field_a],
		);
		assert!(!judge(&ir, neg_a).total);

		let pos_a = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Prefix(Box::new(PrefixOperator::Positive)),
			&[field_a],
		);
		assert!(!judge(&ir, pos_a).total);

		let postfix_a = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Postfix {
				op: LogicalPostfixOperator::Range,
			},
			&[field_a],
		);
		assert!(!judge(&ir, postfix_a).total);

		let each_a = emit(&mut ir, InsertPoint::End(block), ExprOp::Each, &[field_a]);
		assert!(!judge(&ir, each_a).total);

		let convert_a = emit(
			&mut ir,
			InsertPoint::End(block),
			ExprOp::Convert(Box::new(ConvertAttrs {
				mode: ConvertMode::Cast,
				kind: Kind::Int,
				site: ConvertSite(0),
				on_fail: OnFail::Error,
			})),
			&[field_a],
		);
		assert!(!judge(&ir, convert_a).total);
	}
}
