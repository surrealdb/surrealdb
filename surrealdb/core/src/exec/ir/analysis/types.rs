//! Type knowledge of a value, from the language and from derivation.
//!
//! A [`TypeKnowledge::Fact`] is consumed without a check, so a rule here is added only for a
//! shape the language guarantees. The knowledge a value holds in its own data (a literal, a typed
//! argument) is taken as it stands. Derived knowledge comes from the op that defines the value:
//! the kind a conversion guarantees, a comparison's boolean, integer arithmetic, a loop element's
//! kind from its source, and the record id of a loop element whose source is a distinct scan of
//! one table that keeps its `id`.
//!
//! Knowledge that rests on stored state (a field's declared kind, a migrated row layout) needs a
//! catalog premise, and this module reads no catalog: a field read, a dereference and a
//! `COMPUTED` read are `Unknown`, and [`classify_with`] never answers a fact sourced from storage.
//! [`generation_rule`] describes how such a fact is formed. No caller uses it.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};

#[cfg(test)]
use super::cx::default_cx;
use super::multiplicity::{self, MAX_DEPTH};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag, OnFail};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::ids::{OpId, ValueId};
use crate::exec::ir::iface::types::{FactSource, HintSource, TypeKnowledge};
use crate::exec::ir::logical::ops::{LogOp, LogTag, LoopKind, SourceRefShape};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::{BinaryOperator, Kind, PrefixOperator};

/// The widest `Kind::Either` a join keeps. A wider union answers `Unknown`.
pub(crate) const MAX_JOIN_KINDS: usize = 4;

/// The most generations of one table's layout that may be live at once. More answer `Unknown`.
pub(crate) const MAX_LIVE_GENERATIONS: usize = 4;

/// The declared kinds of one generation of a strict table's layout.
#[derive(Clone, Debug)]
pub(crate) struct GenerationKinds {
	pub(crate) generation: u16,
	pub(crate) fields: SmallVec<[(Strand, Kind); 8]>,
}

/// What is known about the type of `value`, under the default context.
#[cfg(test)]
pub(crate) fn classify(ir: &Ir, facts: &mut FactCache, value: ValueId) -> TypeKnowledge {
	classify_with(ir, facts, &default_cx(ir), value)
}

/// What is known about the type of `value` under `cx`.
///
/// The answer is derived from the container alone and never memoised. It is never a fact sourced
/// from storage. A dangling id answers `Unknown`.
pub(crate) fn classify_with(
	ir: &Ir,
	_facts: &mut FactCache,
	_cx: &EffectCx<'_>,
	value: ValueId,
) -> TypeKnowledge {
	classify_at(ir, value, 0)
}

/// The knowledge held in the value itself, which analysis has not derived and does not compute:
/// the value's own language-level knowledge, or `Unknown`.
pub(crate) fn known(ir: &Ir, _facts: &FactCache, value: ValueId) -> TypeKnowledge {
	ir.value(value).map_or(TypeKnowledge::Unknown, |data| sanitize(&data.ty))
}

/// The knowledge that holds on both paths that meet at a value.
///
/// - `Fact` with `Fact` is a `Fact` of the union of the kinds: `Kind::Either`, flattened, and the
///   kind itself when equal.
/// - A `Hint` on either side makes a `Hint` of the union.
/// - `Unknown` on either side, or a union wider than [`MAX_JOIN_KINDS`], is `Unknown`.
///
/// The source of a joined fact is the common source, else the one that rests on storage, else
/// `Derived`; the source of a joined hint is the first hint's.
pub(crate) fn join(a: &TypeKnowledge, b: &TypeKnowledge) -> TypeKnowledge {
	match (a, b) {
		(TypeKnowledge::Unknown | TypeKnowledge::Token(_), _)
		| (_, TypeKnowledge::Unknown | TypeKnowledge::Token(_)) => TypeKnowledge::Unknown,
		(
			TypeKnowledge::Fact {
				kind: left,
				source: left_source,
			},
			TypeKnowledge::Fact {
				kind: right,
				source: right_source,
			},
		) => match union(left, right) {
			Some(kind) => TypeKnowledge::Fact {
				kind,
				source: join_fact_source(left_source, right_source),
			},
			None => TypeKnowledge::Unknown,
		},
		(
			TypeKnowledge::Fact {
				kind: left,
				..
			},
			TypeKnowledge::Hint {
				kind: right,
				source,
			},
		)
		| (
			TypeKnowledge::Hint {
				kind: left,
				source,
			},
			TypeKnowledge::Fact {
				kind: right,
				..
			},
		)
		| (
			TypeKnowledge::Hint {
				kind: left,
				source,
			},
			TypeKnowledge::Hint {
				kind: right,
				..
			},
		) => match union(left, right) {
			Some(kind) => TypeKnowledge::Hint {
				kind,
				source: source.clone(),
			},
			None => TypeKnowledge::Unknown,
		},
	}
}

/// The union of two kinds: the kind itself when equal, else the flattened `Kind::Either`, or
/// `None` when it is wider than [`MAX_JOIN_KINDS`].
fn union(left: &Kind, right: &Kind) -> Option<Kind> {
	if left == right {
		return Some(left.clone());
	}
	let joined = Kind::either(vec![left.clone(), right.clone()]);
	if let Kind::Either(kinds) = &joined
		&& kinds.len() > MAX_JOIN_KINDS
	{
		None
	} else {
		Some(joined)
	}
}

/// The source of two joined facts. A fact that rests on stored state keeps that premise.
fn join_fact_source(left: &FactSource, right: &FactSource) -> FactSource {
	if left == right {
		return left.clone();
	}
	if let FactSource::StrictRowMigrated {
		..
	} = left
	{
		return left.clone();
	}
	if let FactSource::StrictRowMigrated {
		..
	} = right
	{
		return right.clone();
	}
	FactSource::Derived
}

/// The knowledge a strict-layout field `f` of `table` has, given the table's live generations.
///
/// - One live generation, not migrating, with `f` declared `K`: `Fact(K)` sourced from the migrated
///   rows.
/// - Several live generations, or a migration in flight: `Hint` of the union of the kinds each
///   generation declares, a generation that does not declare `f` contributing `none`.
/// - No generation, more than [`MAX_LIVE_GENERATIONS`], or no generation that declares `f`:
///   `Unknown`.
pub(crate) fn generation_rule(
	table: &TableName,
	live: &[GenerationKinds],
	migrating: bool,
	f: &str,
) -> TypeKnowledge {
	if live.is_empty() || live.len() > MAX_LIVE_GENERATIONS {
		return TypeKnowledge::Unknown;
	}
	let declared = |generation: &GenerationKinds| {
		generation.fields.iter().find(|entry| entry.0 == f).map(|entry| entry.1.clone())
	};
	if let [only] = live
		&& !migrating
	{
		return match declared(only) {
			Some(kind) => TypeKnowledge::Fact {
				kind,
				source: FactSource::StrictRowMigrated {
					table: table.clone(),
				},
			},
			None => TypeKnowledge::Unknown,
		};
	}
	let mut kinds = Vec::with_capacity(live.len());
	let mut any = false;
	for generation in live {
		match declared(generation) {
			Some(kind) => {
				any = true;
				kinds.push(kind);
			}
			None => kinds.push(Kind::None),
		}
	}
	if !any {
		return TypeKnowledge::Unknown;
	}
	TypeKnowledge::Hint {
		kind: Kind::either(kinds),
		source: HintSource::MidMigration {
			table: table.clone(),
		},
	}
}

/// The one table the write `write_op` is pinned to by derivation, if any.
///
/// The write has exactly one subject, and that subject is the element of a loop over a distinct
/// scan of one table, the `id` of such an element, or a value known to be a record id of one
/// table. A write with several subjects, an unpinned dynamic subject, or no recognisable subject
/// answers `None`.
pub(crate) fn write_target_table(
	ir: &Ir,
	_facts: &mut FactCache,
	_cx: &EffectCx<'_>,
	write_op: OpId,
) -> Option<TableName> {
	let subject = subject_loop(ir, write_op)?;
	let targets = loop_targets(ir, subject);
	let [target] = targets.as_slice() else {
		return None;
	};
	pinned_table(ir, *target)
}

/// The loop of kind `Write` whose result the write `write_op` consumes.
pub(crate) fn subject_loop(ir: &Ir, write_op: OpId) -> Option<OpId> {
	let data = ir.op(write_op)?;
	if data.kind.tag() != OpTag::Logical(LogTag::Write) {
		return None;
	}
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(*data.operands.first()?)?.def
	else {
		return None;
	};
	if multiplicity::loop_kind(ir, op) == Some(LoopKind::Write) {
		Some(op)
	} else {
		None
	}
}

/// The values a write loop writes to: the operands of the array that gathers its targets, or the
/// source value itself when it is not gathered. A source op that reads the record ids or key
/// ranges its operand holds (`SourceRefShape::RecordId`) stands for that operand.
pub(crate) fn loop_targets(ir: &Ir, write_loop: OpId) -> SmallVec<[ValueId; 2]> {
	let mut out = SmallVec::new();
	let Some(mut source) = ir.op(write_loop).and_then(|data| data.operands.first().copied()) else {
		return out;
	};
	if let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(source).map(|data| data.def)
		&& let Some(data) = ir.op(op)
		&& let OpKind::Logical(LogOp::Source(attrs)) = &data.kind
		&& matches!(attrs.source_ref, SourceRefShape::RecordId)
		&& let Some(read) = data.operands.first().copied()
	{
		source = read;
	}
	let gathered = match ir.value(source).map(|data| data.def) {
		Some(ValueDef::OpResult {
			op,
			..
		}) => ir.op(op).filter(|data| data.kind.tag() == OpTag::Expr(ExprTag::Array)),
		Some(ValueDef::BlockArg {
			..
		})
		| None => None,
	};
	match gathered {
		Some(array) => out.extend(array.operands.iter().copied()),
		None => out.push(source),
	}
	out
}

/// The one table `target` is a record of by derivation: the element of a loop whose stream is a
/// distinct scan of one table, or a value known to be a record id of exactly one table.
pub(crate) fn pinned_table(ir: &Ir, target: ValueId) -> Option<TableName> {
	if let Some((loop_op, 0)) = multiplicity::loop_arg_owner(ir, target)
		&& multiplicity::loop_kind(ir, loop_op) != Some(LoopKind::Select)
		&& let Some(table) = multiplicity::stream_table(ir, loop_op)
	{
		return Some(table);
	}
	if let TypeKnowledge::Fact {
		kind: Kind::Record(tables),
		..
	} = classify_at(ir, target, 0)
		&& let [table] = tables.as_slice()
	{
		return Some(table.clone());
	}
	None
}

/// Whether `value` is known to be a record id.
pub(crate) fn is_record(ir: &Ir, value: ValueId) -> bool {
	matches!(
		classify_at(ir, value, 0),
		TypeKnowledge::Fact {
			kind: Kind::Record(_),
			..
		}
	)
}

/// Whether `value` is known to be an integer. `depth` counts the defining ops already followed.
pub(super) fn is_int_at(ir: &Ir, value: ValueId, depth: usize) -> bool {
	if let TypeKnowledge::Fact {
		kind,
		..
	} = classify_at(ir, value, depth)
	{
		kind == Kind::Int
	} else {
		false
	}
}

/// `ty` with a fact that rests on storage withdrawn: no consumer holds a catalog premise for it.
fn sanitize(ty: &TypeKnowledge) -> TypeKnowledge {
	if let TypeKnowledge::Fact {
		source: FactSource::StrictRowMigrated {
			..
		},
		..
	} = ty
	{
		TypeKnowledge::Unknown
	} else {
		ty.clone()
	}
}

fn derived(kind: Kind) -> TypeKnowledge {
	TypeKnowledge::Fact {
		kind,
		source: FactSource::Derived,
	}
}

/// The knowledge of `value`, `depth` defining ops from the first call.
pub(super) fn classify_at(ir: &Ir, value: ValueId, depth: usize) -> TypeKnowledge {
	if depth > MAX_DEPTH {
		return TypeKnowledge::Unknown;
	}
	let Some(data) = ir.value(value) else {
		return TypeKnowledge::Unknown;
	};
	if data.ty != TypeKnowledge::Unknown {
		return sanitize(&data.ty);
	}
	match data.def {
		ValueDef::BlockArg {
			..
		} => element_knowledge(ir, value, depth),
		ValueDef::OpResult {
			op,
			index,
		} => {
			if index == 0 {
				op_knowledge(ir, op, depth)
			} else {
				TypeKnowledge::Unknown
			}
		}
	}
}

/// The knowledge of the element of a loop over an array or a set of known element kind.
///
/// Only a loop that hands out its source's elements untouched has one: a `SELECT` reads rows and a
/// write reads its subjects, so neither does.
fn element_knowledge(ir: &Ir, value: ValueId, depth: usize) -> TypeKnowledge {
	let Some((loop_op, 0)) = multiplicity::loop_arg_owner(ir, value) else {
		return TypeKnowledge::Unknown;
	};
	let Some(kind) = multiplicity::loop_kind(ir, loop_op) else {
		return TypeKnowledge::Unknown;
	};
	match kind {
		LoopKind::For | LoopKind::Closure | LoopKind::ElementFilter | LoopKind::Broadcast => {}
		LoopKind::Select | LoopKind::Write | LoopKind::GraphStep | LoopKind::Recurse => {
			return TypeKnowledge::Unknown;
		}
	}
	let Some(source) = ir.op(loop_op).and_then(|data| data.operands.first().copied()) else {
		return TypeKnowledge::Unknown;
	};
	match classify_at(ir, source, depth + 1) {
		TypeKnowledge::Fact {
			kind,
			..
		} => element_kind(&kind).map_or(TypeKnowledge::Unknown, derived),
		TypeKnowledge::Hint {
			kind,
			source,
		} => element_kind(&kind).map_or(TypeKnowledge::Unknown, |kind| TypeKnowledge::Hint {
			kind,
			source,
		}),
		TypeKnowledge::Unknown | TypeKnowledge::Token(_) => TypeKnowledge::Unknown,
	}
}

/// The kind of the elements of an array or set kind, when it says more than `any`.
fn element_kind(kind: &Kind) -> Option<Kind> {
	if let Kind::Array(inner, _) | Kind::Set(inner, _) = kind
		&& **inner != Kind::Any
	{
		Some((**inner).clone())
	} else {
		None
	}
}

fn op_knowledge(ir: &Ir, op: OpId, depth: usize) -> TypeKnowledge {
	let Some(data) = ir.op(op) else {
		return TypeKnowledge::Unknown;
	};
	match &data.kind {
		OpKind::Expr(expr) => expr_knowledge(ir, data, expr, depth),
		OpKind::Logical(_) | OpKind::Phys(_) => TypeKnowledge::Unknown,
	}
}

fn expr_knowledge(ir: &Ir, data: &OpData, expr: &ExprOp, depth: usize) -> TypeKnowledge {
	match expr {
		ExprOp::Convert(attrs) => match attrs.on_fail {
			OnFail::Error => derived(attrs.kind.clone()),
			OnFail::None => derived(Kind::option(attrs.kind.clone())),
		},
		ExprOp::Binary {
			op,
		} => {
			let both_int = data.operands.len() == 2
				&& data.operands.iter().all(|operand| is_int_at(ir, *operand, depth + 1));
			match binary_class(op) {
				BinaryClass::Bool => derived(Kind::Bool),
				BinaryClass::IntClosed if both_int => derived(Kind::Int),
				BinaryClass::IntDivide if both_int => derived(Kind::Number),
				BinaryClass::IntClosed | BinaryClass::IntDivide | BinaryClass::Other => {
					TypeKnowledge::Unknown
				}
			}
		}
		ExprOp::Truthy => derived(Kind::Bool),
		ExprOp::Prefix(op) => {
			if **op == PrefixOperator::Not {
				derived(Kind::Bool)
			} else {
				TypeKnowledge::Unknown
			}
		}
		ExprOp::Field {
			name,
		} => {
			if *name == "id" {
				id_knowledge(ir, data.operands.first().copied(), depth)
			} else {
				TypeKnowledge::Unknown
			}
		}
		ExprOp::Access {
			field,
		} => {
			if *field == "id" {
				id_knowledge(ir, data.operands.first().copied(), depth)
			} else {
				TypeKnowledge::Unknown
			}
		}
		ExprOp::Literal(_)
		| ExprOp::Array
		| ExprOp::Set
		| ExprOp::Object {
			..
		}
		| ExprOp::RecordId(_)
		| ExprOp::FreeParam {
			..
		}
		| ExprOp::SessionParam {
			..
		}
		| ExprOp::CatalogParam {
			..
		}
		| ExprOp::Table {
			..
		}
		| ExprOp::Mock(_)
		| ExprOp::Constant {
			..
		}
		| ExprOp::ComputedRead(_)
		| ExprOp::Doc
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
		| ExprOp::Method {
			..
		}
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
		| ExprOp::Postfix {
			..
		}
		| ExprOp::Invoke
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::Call(_)
		| ExprOp::ClosureCreate(_)
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Subquery(_)
		| ExprOp::Yield
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::DepthCharge {
			..
		} => TypeKnowledge::Unknown,
	}
}

/// What a binary operator guarantees of its result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BinaryClass {
	/// A comparison or a containment test: always a boolean.
	Bool,
	/// `+`, `-` and `*`: an integer over two integers, and an error on overflow.
	IntClosed,
	/// `/`: a number over two integers.
	IntDivide,
	Other,
}

fn binary_class(op: &BinaryOperator) -> BinaryClass {
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
		| BinaryOperator::NoneInside => BinaryClass::Bool,
		BinaryOperator::Add | BinaryOperator::Subtract | BinaryOperator::Multiply => {
			BinaryClass::IntClosed
		}
		BinaryOperator::Divide => BinaryClass::IntDivide,
		BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Or
		| BinaryOperator::And
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive
		| BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_) => BinaryClass::Other,
	}
}

/// The row a `logical.filter` judges, when `value` is the row argument of the filter's clause:
/// the clause is evaluated over the filter's input row, which the filter passes on unchanged.
fn filtered_row(ir: &Ir, value: ValueId) -> Option<ValueId> {
	let ValueDef::BlockArg {
		block,
		index: 0,
	} = ir.value(value)?.def
	else {
		return None;
	};
	let region = ir.block(block)?.parent;
	if ir.region(region)?.blocks.first() != Some(&block) {
		return None;
	}
	let owner = ir.op(ir.parent_op(region)?)?;
	if owner.kind.tag() != OpTag::Logical(LogTag::Filter) {
		return None;
	}
	owner.operands.first().copied()
}

/// The knowledge of the field `id` read off `base`, where the row a filter's clause reads is the
/// row the filter judges ([`filtered_row`]).
///
/// Over the element of a `SELECT` whose source is a scan of one table, the row is the stored row
/// and its `id` is a record id of that table. Over the element of any other loop whose stream is
/// distinct along `id` for one table, the element is a row that kept its `id`, and the `id` is a
/// record id of that table by provenance.
fn id_knowledge(ir: &Ir, base: Option<ValueId>, depth: usize) -> TypeKnowledge {
	let Some(base) = base else {
		return TypeKnowledge::Unknown;
	};
	let base = filtered_row(ir, base).unwrap_or(base);
	let Some((loop_op, 0)) = multiplicity::loop_arg_owner(ir, base) else {
		return TypeKnowledge::Unknown;
	};
	match multiplicity::loop_kind(ir, loop_op) {
		Some(LoopKind::Select) => {
			let source = ir.op(loop_op).and_then(|data| data.operands.first().copied());
			match source.and_then(|source| multiplicity::source_table(ir, source, depth + 1)) {
				Some(table) => derived(Kind::Record(vec![table])),
				None => TypeKnowledge::Unknown,
			}
		}
		Some(
			LoopKind::For
			| LoopKind::Write
			| LoopKind::Closure
			| LoopKind::ElementFilter
			| LoopKind::GraphStep
			| LoopKind::Recurse
			| LoopKind::Broadcast,
		) => match multiplicity::stream_table(ir, loop_op) {
			Some(table) => TypeKnowledge::Fact {
				kind: Kind::Record(vec![table.clone()]),
				source: FactSource::Provenance {
					table,
				},
			},
			None => TypeKnowledge::Unknown,
		},
		None => TypeKnowledge::Unknown,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn fact(kind: Kind) -> TypeKnowledge {
		TypeKnowledge::Fact {
			kind,
			source: FactSource::Literal,
		}
	}

	fn hint(kind: Kind) -> TypeKnowledge {
		TypeKnowledge::Hint {
			kind,
			source: HintSource::Annotation,
		}
	}

	fn table(name: &str) -> TableName {
		TableName::new(name)
	}

	fn generation(number: u16, fields: &[(&str, Kind)]) -> GenerationKinds {
		GenerationKinds {
			generation: number,
			fields: fields.iter().map(|(name, kind)| (Strand::from(*name), kind.clone())).collect(),
		}
	}

	#[test]
	fn join_table() {
		let int = fact(Kind::Int);
		let string = fact(Kind::String);
		assert_eq!(join(&int, &int), int, "equal facts join to themselves");
		assert_eq!(
			join(&int, &string),
			TypeKnowledge::Fact {
				kind: Kind::Either(vec![Kind::Int, Kind::String]),
				source: FactSource::Literal,
			},
			"equal sources are kept"
		);
		assert_eq!(
			join(&join(&int, &string), &fact(Kind::Int)),
			join(&int, &string),
			"a kind already in the union adds nothing"
		);

		let derived_int = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::Derived,
		};
		assert_eq!(
			join(&int, &derived_int),
			TypeKnowledge::Fact {
				kind: Kind::Int,
				source: FactSource::Derived,
			},
			"unequal sources join to a derived source"
		);

		let hinted = hint(Kind::String);
		assert_eq!(
			join(&int, &hinted),
			TypeKnowledge::Hint {
				kind: Kind::Either(vec![Kind::Int, Kind::String]),
				source: HintSource::Annotation,
			}
		);
		assert_eq!(
			join(&hinted, &int),
			TypeKnowledge::Hint {
				kind: Kind::Either(vec![Kind::String, Kind::Int]),
				source: HintSource::Annotation,
			},
			"a hint's source survives on either side"
		);
		assert_eq!(join(&hinted, &hinted), hinted);
		assert_eq!(
			join(&hint(Kind::Int), &hint(Kind::Bool)),
			hint(Kind::Either(vec![Kind::Int, Kind::Bool]))
		);

		for known in [&int, &hinted] {
			assert_eq!(join(known, &TypeKnowledge::Unknown), TypeKnowledge::Unknown);
			assert_eq!(join(&TypeKnowledge::Unknown, known), TypeKnowledge::Unknown);
		}
		assert_eq!(join(&TypeKnowledge::Unknown, &TypeKnowledge::Unknown), TypeKnowledge::Unknown);

		let mut wide = fact(Kind::Int);
		for kind in [Kind::String, Kind::Bool, Kind::Float] {
			wide = join(&wide, &fact(kind));
		}
		assert!(
			matches!(&wide, TypeKnowledge::Fact { kind: Kind::Either(kinds), .. } if kinds.len() == MAX_JOIN_KINDS)
		);
		assert_eq!(
			join(&wide, &fact(Kind::Uuid)),
			TypeKnowledge::Unknown,
			"a union wider than the bound is no knowledge"
		);
		assert_eq!(join(&wide, &fact(Kind::Bool)), wide, "a member of the union adds nothing");

		let migrated = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::StrictRowMigrated {
				table: table("t"),
			},
		};
		assert_eq!(
			join(&int, &migrated),
			TypeKnowledge::Fact {
				kind: Kind::Int,
				source: FactSource::StrictRowMigrated {
					table: table("t"),
				},
			},
			"a premise survives a join"
		);
		assert_eq!(
			join(&migrated, &fact(Kind::Bool)),
			TypeKnowledge::Fact {
				kind: Kind::Either(vec![Kind::Int, Kind::Bool]),
				source: FactSource::StrictRowMigrated {
					table: table("t"),
				},
			}
		);
	}

	#[test]
	fn generation_rule_rows() {
		let t = table("t");
		let one = [generation(1, &[("a", Kind::Int)])];
		assert_eq!(
			generation_rule(&t, &one, false, "a"),
			TypeKnowledge::Fact {
				kind: Kind::Int,
				source: FactSource::StrictRowMigrated {
					table: t.clone(),
				},
			}
		);
		assert_eq!(generation_rule(&t, &one, false, "b"), TypeKnowledge::Unknown);

		assert_eq!(
			generation_rule(&t, &one, true, "a"),
			TypeKnowledge::Hint {
				kind: Kind::Int,
				source: HintSource::MidMigration {
					table: t.clone(),
				},
			},
			"a migration in flight is a hint even over one generation"
		);

		let two = [generation(1, &[("a", Kind::Int)]), generation(2, &[("a", Kind::String)])];
		assert_eq!(
			generation_rule(&t, &two, false, "a"),
			TypeKnowledge::Hint {
				kind: Kind::Either(vec![Kind::Int, Kind::String]),
				source: HintSource::MidMigration {
					table: t.clone(),
				},
			}
		);

		let gap = [generation(1, &[("a", Kind::Int)]), generation(2, &[("b", Kind::Bool)])];
		assert_eq!(
			generation_rule(&t, &gap, false, "a"),
			TypeKnowledge::Hint {
				kind: Kind::Either(vec![Kind::Int, Kind::None]),
				source: HintSource::MidMigration {
					table: t.clone(),
				},
			},
			"a field absent from a generation contributes none"
		);
		assert_eq!(generation_rule(&t, &gap, false, "c"), TypeKnowledge::Unknown);

		let five: Vec<GenerationKinds> =
			(0..=MAX_LIVE_GENERATIONS as u16).map(|n| generation(n, &[("a", Kind::Int)])).collect();
		assert_eq!(five.len(), MAX_LIVE_GENERATIONS + 1);
		assert_eq!(generation_rule(&t, &five, false, "a"), TypeKnowledge::Unknown);
		assert!(matches!(
			generation_rule(&t, &five[..MAX_LIVE_GENERATIONS], false, "a"),
			TypeKnowledge::Hint { .. }
		));
		assert_eq!(generation_rule(&t, &[], false, "a"), TypeKnowledge::Unknown);
	}

	#[test]
	fn no_storage_fact_without_layout() {
		let migrated = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::StrictRowMigrated {
				table: table("t"),
			},
		};
		assert_eq!(sanitize(&migrated), TypeKnowledge::Unknown);
		let literal = fact(Kind::Int);
		assert_eq!(sanitize(&literal), literal);
		let provenance = TypeKnowledge::Fact {
			kind: Kind::Record(vec![table("t")]),
			source: FactSource::Provenance {
				table: table("t"),
			},
		};
		assert_eq!(sanitize(&provenance), provenance);

		// Only the layout rule and the join that must carry its premise name a storage source.
		let source = include_str!("types.rs");
		let production = source.split("#[cfg(test)]").next().unwrap_or("");
		let mut current = "";
		for line in production.lines() {
			let trimmed = line.trim_start();
			if let Some(rest) = trimmed
				.strip_prefix("pub(crate) fn ")
				.or_else(|| trimmed.strip_prefix("pub(super) fn "))
				.or_else(|| trimmed.strip_prefix("fn "))
			{
				current = rest.split(['(', '<']).next().unwrap_or("");
			}
			if trimmed.starts_with("//") || !trimmed.contains("StrictRowMigrated") {
				continue;
			}
			assert!(
				["generation_rule", "join_fact_source", "sanitize"].contains(&current),
				"`{current}` names a storage-sourced fact"
			);
		}
	}
}
