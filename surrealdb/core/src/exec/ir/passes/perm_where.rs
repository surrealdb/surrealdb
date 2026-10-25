//! Merging a table's permission filter with the statement's `WHERE` into one predicate.
//!
//! The merged predicate evaluates in the order that keeps the first error the statement raises
//! unchanged.
//!
//! Three passes own that order:
//!
//! - [`SpecialiseGuards`] reads the literals the permission guards bind. A table permission filter
//!   that yields the literal `true` admits every row and goes. One that yields `false` stays: its
//!   rows are all hidden, which the access path plans. A field guard that yields a literal decides
//!   what a read of the masked field sees: `true` reads the stored row's field, `false` reads
//!   `NONE`. Field guards constrain the analyses only; the host still enforces them.
//! - [`PushBelowLayer`] moves the conjuncts of a `WHERE` filter that read no column of the
//!   projection layers beneath it to a filter directly above the op the stack of layers stands on,
//!   reading the stored row. A conjunct that reads key columns only is the same expression over the
//!   element of the record id each stands for, and moves with those reads replaced. The layers
//!   derive their columns only for the rows that reach them.
//! - [`PushBelowPermission`] moves a left prefix of a `WHERE` conjunction from the filter above a
//!   permission filter to a filter directly below it. It runs only when every moved conjunct is
//!   infallible and reads nothing beyond the row, the permission predicate is total, and no write
//!   of the statement reaches what the permission reads.
//!
//! Conjuncts keep their written order, and none moves once a conjunct that stays can raise, so the
//! first error of a row is the one the unmoved plan raises. [`PushBelowPermission`] moves a left
//! prefix only. Neither push drops a permission conjunct, and no conjunct crosses a permission
//! filter but through [`PushBelowPermission`]. Moved ops are re-created under `derived`, so
//! provenance names the filter they left, and an op keeps its frame and snapshot. A push checks
//! everything a re-creation can refuse before it changes anything. Code inside a stored clause is
//! never rewritten.
#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::{SmallVec, smallvec};

use super::driver::{LoCx, LoPass, PassConfig, PassReport, PassRun};
use super::rehome::{and_chain, can_rehome, rebuild_and_chain, rebuild_and_chain_bound};
use crate::exec::ir::analysis::{effects, fallibility, frames};
use crate::exec::ir::core::census::{Census, Flag};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::{FxHashMap, FxHashSet};
use crate::exec::ir::core::ir::{Ir, OpList, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, LogOp, LogTag};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::BinaryOperator;
use crate::expr::logical::EffectSet;
use crate::val::Value;

/// Specialises the literals the permission guards bind.
///
/// A table permission filter whose predicate yields the literal `true` is erased and its readers
/// read its input. One that yields `false` is left in place. A field guard of a projection layer
/// pairs with the field it guards by position, and only when the layer holds one guard for every
/// field frame and one named column for every guard. A guard that yields `true` points the reads
/// of its field at the stored row, provided the field's frame only reads that stored field; one
/// that yields `false` replaces the reads with `NONE`. The derivation of a hidden computed field
/// is left to dead-code elimination.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SpecialiseGuards;

impl LoPass for SpecialiseGuards {
	fn name(&self) -> &'static str {
		"specialise_guards"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.specialise_guards
	}

	/// [`run_specialise`] offers each op outside stored code to [`erase_true_permission`] and
	/// [`specialise_field_guards`]. The first acts only on a permission filter whose first region
	/// is a `PermissionFrame`. The second, through [`guarded_fields`], needs at least one
	/// `PermissionFrame` region among the op's regions. Both answer before touching the container
	/// or the facts when none stands there, which [`Flag::PermissionFrame`] states: an op outside
	/// stored code owns such a region.
	fn needs(&self, census: &Census) -> bool {
		census.flag(Flag::PermissionFrame)
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_specialise(cx))
	}
}

/// Moves the `WHERE` conjuncts that read no column of the layers beneath the filter below them.
///
/// Applies to a filter whose only input is a stack of projection layers in the same block. A
/// conjunct that reads a computed, guarded or key column, the row other than by a field's name, an
/// index operator or code the conjunct does not hold stays above the stack, except that a read of
/// a key column is replaced by the read of the element of the record id it stands for.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PushBelowLayer;

impl LoPass for PushBelowLayer {
	fn name(&self) -> &'static str {
		"push_below_layer"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.push_below_layer
	}

	/// [`run_push`] with [`Target::Layer`] offers each op outside stored code to
	/// [`push_below_layer`], which returns before any fact is read or the container touched
	/// unless [`where_filter`] finds a `logical.filter` and [`layer_stack`], through
	/// [`target_below`], finds a `logical.project` whose `layer` attribute is set beneath it.
	/// A filter and a layer projection outside stored code are what the census states: the filter
	/// is read through `live_ops`, and the layer stands in the filter's own block.
	fn needs(&self, census: &Census) -> bool {
		census.has(LogTag::Filter) && census.flag(Flag::ProjectionLayer)
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_push(cx, Target::Layer))
	}
}

/// Moves a prefix of the `WHERE` conjuncts below a permission filter.
///
/// Applies to a filter whose only input is a permission filter in the same block. Moves the
/// longest left prefix of conjuncts that are infallible and read nothing beyond the row, and only
/// when the permission predicate is total and no write of the statement reaches what it reads.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PushBelowPermission;

impl LoPass for PushBelowPermission {
	fn name(&self) -> &'static str {
		"push_below_permission"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.push_below_permission
	}

	/// [`run_push`] with [`Target::Permission`] offers each op outside stored code to
	/// [`push_below_permission`], which returns before any fact is read or the container touched
	/// unless [`where_filter`] finds a `logical.filter` and [`target_below`] finds a
	/// `logical.permission_filter` in the same block beneath it. Both are tags the census
	/// states for ops outside stored code.
	fn needs(&self, census: &Census) -> bool {
		census.has(LogTag::Filter) && census.has(LogTag::PermissionFilter)
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_push(cx, Target::Permission))
	}
}

/// Whether `op` stands inside a stored clause, whose code is bound and analysed but never
/// rewritten.
pub(super) fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Every op outside stored clauses, in program order: an op before the ops of its regions.
pub(super) fn live_ops(ir: &Ir) -> OpList {
	ir.collect_ops(ir.top(), WalkOrder::PreOrder, |op| !in_stored_clause(ir, op))
}

/// The op that defines `value`, or `None` for a block argument and for a dangling id.
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

/// The value the one-block region `region` yields, or `None` when its block does not end in a
/// yield of one value.
pub(super) fn yielded_value(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let [block] = ir.region(region)?.blocks.as_slice() else {
		return None;
	};
	let last = ir.op(*ir.block(*block)?.ops.last()?)?;
	match (&last.kind, last.operands.as_slice()) {
		(OpKind::Expr(ExprOp::Yield), [value]) => Some(*value),
		(OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_), _) => None,
	}
}

/// The boolean literal the one-block guard region `region` yields, or `None` when it yields
/// anything else.
fn guard_literal(ir: &Ir, region: RegionId) -> Option<bool> {
	let literal = ir.op(def_op(ir, yielded_value(ir, region)?)?)?;
	if let OpKind::Expr(ExprOp::Literal(held)) = &literal.kind
		&& let Value::Bool(flag) = &**held
	{
		return Some(*flag);
	}
	None
}

fn run_specialise(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	for op in live_ops(cx.ir) {
		if erase_true_permission(cx, op)? {
			report.rewrites = report.rewrites.saturating_add(1);
			continue;
		}
		report.rewrites = report.rewrites.saturating_add(specialise_field_guards(cx, op)?);
	}
	Ok(report)
}

/// Erases `op` when it is a permission filter whose predicate yields the literal `true`, pointing
/// its readers at its input, and answers whether it went.
fn erase_true_permission(cx: &mut LoCx<'_>, op: OpId) -> Result<bool, IrError> {
	let Some(data) = cx.ir.op(op) else {
		return Ok(false);
	};
	if !matches!(data.kind, OpKind::Logical(LogOp::PermissionFilter { .. })) {
		return Ok(false);
	}
	let ([region], [input], [result]) =
		(data.regions.as_slice(), data.operands.as_slice(), data.results.as_slice())
	else {
		return Ok(false);
	};
	let framed = cx.ir.region(*region).is_some_and(|held| held.kind == RegionKind::PermissionFrame);
	if !framed || guard_literal(cx.ir, *region) != Some(true) {
		return Ok(false);
	}
	let (input, result) = (*input, *result);
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	rw.replace_all_uses(result, input)?;
	match rw.erase(op, EliminationReason::FoldedConstant) {
		Ok(()) => Ok(true),
		Err(IrError::HasUses(_)) => Ok(false),
		Err(error) => Err(error),
	}
}

/// A field of a projection layer whose guard yields a literal.
struct GuardedField {
	name: String,
	/// The literal the guard yields.
	admits: bool,
	/// Whether the field's frame yields a plain read of the stored field, so that the masked
	/// field and the stored one are the same value.
	plain: bool,
}

/// The fields of the layer `layer` whose guards yield a literal.
///
/// Guards pair with field frames and with the named columns by position, and only when the three
/// counts agree and are not zero.
fn guarded_fields(ir: &Ir, layer: OpId) -> Vec<GuardedField> {
	let Some(data) = ir.op(layer) else {
		return Vec::new();
	};
	let OpKind::Logical(LogOp::Project(attrs)) = &data.kind else {
		return Vec::new();
	};
	let FieldsShape::Select(shape) = &attrs.fields else {
		return Vec::new();
	};
	if !attrs.layer {
		return Vec::new();
	}
	let Some(names) = select_names(shape) else {
		return Vec::new();
	};
	let of_kind = |wanted: fn(RegionKind) -> bool| -> Vec<RegionId> {
		data.regions
			.iter()
			.copied()
			.filter(|region| ir.region(*region).is_some_and(|held| wanted(held.kind)))
			.collect()
	};
	let guards = of_kind(|kind| kind == RegionKind::PermissionFrame);
	let frames = of_kind(|kind| kind == RegionKind::FieldFrame(FieldRole::Computed));
	if guards.is_empty() || guards.len() != frames.len() || guards.len() != names.len() {
		return Vec::new();
	}
	let mut out = Vec::new();
	for ((name, guard), frame) in names.into_iter().zip(guards).zip(frames) {
		if let Some(admits) = guard_literal(ir, guard) {
			let plain = frame_reads_stored_field(ir, frame, &name);
			out.push(GuardedField {
				name,
				admits,
				plain,
			});
		}
	}
	out
}

/// The output names of the columns `shape` selects, in order, or `None` when a column has no plain
/// field name. `*` names nothing.
fn select_names(shape: &[FieldShape]) -> Option<Vec<String>> {
	let mut names = Vec::new();
	for field in shape {
		match field {
			FieldShape::All => {}
			FieldShape::Single {
				alias,
				..
			} => names.push(alias.as_ref().and_then(|path| path.as_field())?.to_owned()),
		}
	}
	Some(names)
}

/// Whether the field frame `frame` yields a plain read of the stored field `name`.
fn frame_reads_stored_field(ir: &Ir, frame: RegionId, name: &str) -> bool {
	let Some(read) =
		yielded_value(ir, frame).and_then(|value| def_op(ir, value)).and_then(|op| ir.op(op))
	else {
		return false;
	};
	is_field_read_of(&read.kind, name)
}

/// Whether `kind` is a read of the field `name`.
fn is_field_read_of(kind: &OpKind, name: &str) -> bool {
	matches!(kind, OpKind::Expr(ExprOp::Field { name: read }) if read.as_str() == name)
}

/// The reads of the field `name` off the row the layer `layer` delivers, in the order they are
/// found: reads of the layer's result and reads of the row of every filter that passes it on.
fn masked_reads(ir: &Ir, layer: OpId, name: &str) -> Vec<OpId> {
	let Some(delivered) = ir.op(layer).and_then(|data| data.results.first().copied()) else {
		return Vec::new();
	};
	let mut rows: Vec<ValueId> = vec![delivered];
	let mut current = delivered;
	while let [only] = ir.uses(current) {
		let Some(next) = ir.op(only.user) else {
			break;
		};
		let (OpKind::Logical(LogOp::Filter), [region], [result], 0) =
			(&next.kind, next.regions.as_slice(), next.results.as_slice(), only.operand)
		else {
			break;
		};
		let row = ir
			.region(*region)
			.and_then(|held| held.blocks.first())
			.and_then(|block| ir.block(*block))
			.and_then(|block| block.args.first().copied());
		rows.extend(row);
		rows.push(*result);
		current = *result;
	}
	let mut reads: Vec<OpId> = Vec::new();
	for row in rows {
		for held in ir.uses(row) {
			let is_read = held.operand == 0
				&& ir.op(held.user).is_some_and(|user| is_field_read_of(&user.kind, name));
			if is_read && !reads.contains(&held.user) {
				reads.push(held.user);
			}
		}
	}
	reads
}

/// Applies the guard literals of `op` when it is a layer, and answers how many reads it rewrote.
fn specialise_field_guards(cx: &mut LoCx<'_>, op: OpId) -> Result<u32, IrError> {
	let guarded = guarded_fields(cx.ir, op);
	if guarded.is_empty() {
		return Ok(0);
	}
	let Some(stored) = cx.ir.op(op).and_then(|data| data.operands.first().copied()) else {
		return Ok(0);
	};
	let mut rewrites = 0;
	for field in guarded {
		if field.admits && !field.plain {
			continue;
		}
		for read in masked_reads(cx.ir, op, &field.name) {
			if field.admits {
				let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
				rw.set_operand(read, 0, stored)?;
				rewrites += 1;
			} else if hide_read(cx, read)? {
				rewrites += 1;
			}
		}
	}
	Ok(rewrites)
}

/// Points every reader of the field read `read` at a `NONE` literal and erases it, and answers
/// whether it went.
fn hide_read(cx: &mut LoCx<'_>, read: OpId) -> Result<bool, IrError> {
	let Some(data) = cx.ir.op(read) else {
		return Ok(false);
	};
	let (frame, snapshot, loc) = (data.frame, data.snapshot, data.loc.clone());
	let results = data.results.clone();
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	if results.iter().any(|result| !rw.ir().uses(*result).is_empty()) {
		let kind = ExprOp::Literal(Box::new(Value::None));
		let ty = result_type(&kind, &[]);
		let spec = OpSpec {
			kind: OpKind::Expr(kind),
			operands: SmallVec::new(),
			results: smallvec![ty],
			frame,
			snapshot,
			loc,
		};
		let literal = rw.derived(&[read], |rw| rw.create(InsertPoint::Before(read), spec))?;
		let Some(none) = rw.ir().op(literal).and_then(|made| made.results.first().copied()) else {
			return Ok(false);
		};
		for result in &results {
			rw.replace_all_uses(*result, none)?;
		}
	}
	match rw.erase(read, EliminationReason::InvisibleToPrincipal) {
		Ok(()) => Ok(true),
		Err(IrError::HasUses(_)) => Ok(false),
		Err(error) => Err(error),
	}
}

/// Which op the new filter goes below.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
	Layer,
	Permission,
}

/// A `WHERE` filter: its input, its row argument and the value its clause yields.
pub(super) struct WhereFilter {
	pub(super) op: OpId,
	pub(super) input: ValueId,
	pub(super) result: ValueId,
	pub(super) row: ValueId,
	pub(super) yielded: OpId,
	pub(super) predicate: ValueId,
}

/// The shape of `op` when it is a filter holding one `Clause(Where)` region of one block that
/// takes the row and ends in a yield of one value.
pub(super) fn where_filter(ir: &Ir, op: OpId) -> Option<WhereFilter> {
	let data = ir.op(op)?;
	let (OpKind::Logical(LogOp::Filter), [input], [result], [region]) =
		(&data.kind, data.operands.as_slice(), data.results.as_slice(), data.regions.as_slice())
	else {
		return None;
	};
	let held = ir.region(*region)?;
	if held.kind != RegionKind::Clause(ClauseRole::Where) {
		return None;
	}
	let [block] = held.blocks.as_slice() else {
		return None;
	};
	let body = ir.block(*block)?;
	let [row] = body.args.as_slice() else {
		return None;
	};
	let yielded = *body.ops.last()?;
	Some(WhereFilter {
		op,
		input: *input,
		result: *result,
		row: *row,
		yielded,
		predicate: yielded_value(ir, *region)?,
	})
}

/// The op `filter` reads its input from when that op is the one `target` names, stands in the
/// same block as the filter, and has no other reader.
fn target_below(ir: &Ir, filter: &WhereFilter, target: Target) -> Option<OpId> {
	let below = def_op(ir, filter.input)?;
	let data = ir.op(below)?;
	if data.parent != ir.op(filter.op)?.parent || ir.uses(filter.input).len() != 1 {
		return None;
	}
	if data.operands.is_empty() || data.results.len() != 1 {
		return None;
	}
	match (target, &data.kind) {
		(Target::Layer, OpKind::Logical(LogOp::Project(attrs))) if attrs.layer => Some(below),
		(
			Target::Permission,
			OpKind::Logical(LogOp::PermissionFilter {
				..
			}),
		) => {
			// The predicate is the first region; a filter that derives computed fields lists their
			// bodies after it, and `permission_is_total` judges every region.
			let region = data.regions.first()?;
			let framed =
				ir.region(*region).is_some_and(|held| held.kind == RegionKind::PermissionFrame);
			framed.then_some(below)
		}
		(
			Target::Layer | Target::Permission,
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_),
		) => None,
	}
}

/// The ops that compute `value`, in program order: its defining op and, transitively, the ops of
/// the same block that define their operands. The ops inside their regions are not listed; an
/// analysis of one of them covers its regions.
pub(super) fn computing_ops(ir: &Ir, value: ValueId) -> Vec<OpId> {
	let Some(root) = def_op(ir, value) else {
		return Vec::new();
	};
	let Some(block) = ir.op(root).map(|data| data.parent) else {
		return Vec::new();
	};
	let mut seen: FxHashSet<OpId> = FxHashSet::default();
	let mut pending = vec![root];
	while let Some(op) = pending.pop() {
		if !seen.insert(op) {
			continue;
		}
		let Some(data) = ir.op(op) else {
			continue;
		};
		for operand in &data.operands {
			if let Some(def) = def_op(ir, *operand)
				&& ir.op(def).is_some_and(|held| held.parent == block)
			{
				pending.push(def);
			}
		}
		for region in &data.regions {
			ir.walk(*region, WalkOrder::PreOrder, &mut |nested| {
				if let Some(nested_data) = ir.op(nested) {
					for operand in &nested_data.operands {
						if let Some(def) = def_op(ir, *operand)
							&& ir.op(def).is_some_and(|held| held.parent == block)
						{
							pending.push(def);
						}
					}
				}
				Walk::Continue
			});
		}
	}
	ir.block(block)
		.map(|held| held.ops.iter().copied().filter(|op| seen.contains(op)).collect())
		.unwrap_or_default()
}

/// Whether every op of `ops`, regions included, is infallible and has effects within the row
/// alone.
pub(super) fn pure_and_infallible(cx: &mut LoCx<'_>, ops: &[OpId]) -> bool {
	ops.iter().all(|op| {
		let answer = effects::of_with(&*cx.ir, &mut *cx.facts, cx.effects, *op);
		let class = answer.fallibility.union(fallibility::floor(answer.set));
		guards::infallible(class) && guards::pure(answer.set)
	})
}

/// Whether every op of `ops`, regions included, is infallible and has effects within the row alone,
/// a plain read of a field off `row`, by name or through `$this`, counting as both: a row the
/// filter reads holds the value of each of its fields, so reading one neither raises nor leaves the
/// row.
pub(super) fn infallible_over_row(cx: &mut LoCx<'_>, ops: &[OpId], row: ValueId) -> bool {
	ops.iter().all(|op| {
		let Some(data) = cx.ir.op(*op) else {
			return false;
		};
		let plain_read =
			matches!(data.kind, OpKind::Expr(ExprOp::Field { .. } | ExprOp::Access { .. }))
				&& data.operands.as_slice() == [row];
		if plain_read {
			return true;
		}
		if matches!(data.kind, OpKind::Expr(ExprOp::ShortCircuit { .. } | ExprOp::If)) {
			let nested: Vec<OpId> =
				data.regions.iter().flat_map(|region| ops_in(cx.ir, *region)).collect();
			return infallible_over_row(cx, &nested, row);
		}
		let answer = effects::of_with(&*cx.ir, &mut *cx.facts, cx.effects, *op);
		let class = answer.fallibility.union(fallibility::floor(answer.set));
		guards::infallible(class) && guards::pure(answer.set)
	})
}

/// Whether the permission predicate of the filter `permission` is total: every op in its region
/// is infallible and has effects within the row alone.
fn permission_is_total(cx: &mut LoCx<'_>, permission: OpId) -> bool {
	let Some(regions) = cx.ir.op(permission).map(|data| data.regions.clone()) else {
		return false;
	};
	let ops: Vec<OpId> = regions.iter().flat_map(|region| ops_in(cx.ir, *region)).collect();
	pure_and_infallible(cx, &ops)
}

/// The name of the field `kind` reads off its operand, when it is a plain field read.
fn field_read_name(kind: &OpKind) -> Option<&str> {
	match kind {
		OpKind::Expr(ExprOp::Field {
			name,
		})
		| OpKind::Expr(ExprOp::Access {
			field: name,
		}) => Some(name.as_str()),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
	}
}

/// Whether the conjunct `value` of `filter` may move below `anchor`, reading `source` for the
/// row. It must be computed inside the filter's clause and pass every check a re-creation makes.
pub(super) fn can_move(
	ir: &Ir,
	filter: &WhereFilter,
	value: ValueId,
	anchor: OpId,
	source: ValueId,
) -> bool {
	let Some(def) = def_op(ir, value) else {
		return false;
	};
	let inside = ancestors(ir, def).any(|(_, owner)| owner == Some(filter.op));
	inside && can_rehome(ir, def, InsertPoint::Before(anchor), &[(filter.row, source)])
}

/// How many times a layer push is repeated while it keeps moving: each run moves a filter below
/// one more layer of a stack.
const PUSH_ROUNDS: u8 = 16;

fn run_push(cx: &mut LoCx<'_>, target: Target) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	for _ in 0..PUSH_ROUNDS {
		let mut moved = false;
		for op in live_ops(cx.ir) {
			let pushed = match target {
				Target::Layer => push_below_layer(cx, op, &mut report)?,
				Target::Permission => push_below_permission(cx, op)?,
			};
			if pushed {
				report.rewrites = report.rewrites.saturating_add(1);
				moved = true;
			}
		}
		if !moved || target == Target::Permission {
			break;
		}
	}
	Ok(report)
}

/// Moves the longest admissible left prefix of the conjuncts of the filter `op` below the
/// permission filter it reads, and answers whether anything moved.
fn push_below_permission(cx: &mut LoCx<'_>, op: OpId) -> Result<bool, IrError> {
	let Some(filter) = where_filter(cx.ir, op) else {
		return Ok(false);
	};
	let Some(anchor) = target_below(cx.ir, &filter, Target::Permission) else {
		return Ok(false);
	};
	let Some(source) = cx.ir.op(anchor).and_then(|data| data.operands.first().copied()) else {
		return Ok(false);
	};
	if !(permission_is_total(cx, anchor)
		&& frames::merge_ok_with(&*cx.ir, &mut *cx.facts, cx.effects, anchor).condition1)
	{
		return Ok(false);
	}
	let chain = and_chain(cx.ir, filter.predicate);
	let mut moving = 0;
	for value in &chain {
		if !admits_below_permission(cx, *value) || !can_move(cx.ir, &filter, *value, anchor, source)
		{
			break;
		}
		moving += 1;
	}
	if moving == 0 {
		return Ok(false);
	}
	move_prefix(cx, &filter, &chain, moving, anchor, source)?;
	Ok(true)
}

/// A column of a projection layer.
struct LayerColumn {
	/// The field the column lands under.
	name: String,
	/// Whether the column reads one element of the record id, which depends on the id alone.
	key: bool,
	/// The field frame that derives the column.
	frame: RegionId,
}

/// The columns of the layer `layer`, in order, or `None` when its regions do not pair with them.
///
/// A column is a `Single` field of the layer's shape; `*` names nothing. Its field frame is the
/// `FieldFrame(Computed)` region at the same position among the layer's frames, and it is a key
/// column when that frame is the key-part frame. The frames after the columns' derive the computed
/// fields the layer's guards read (`ProjectAttrs::judged`).
fn layer_columns(ir: &Ir, layer: OpId) -> Option<Vec<LayerColumn>> {
	let data = ir.op(layer)?;
	let OpKind::Logical(LogOp::Project(attrs)) = &data.kind else {
		return None;
	};
	let FieldsShape::Select(shape) = &attrs.fields else {
		return None;
	};
	if !attrs.layer {
		return None;
	}
	let names = select_names(shape)?;
	let frames: Vec<RegionId> = data
		.regions
		.iter()
		.copied()
		.filter(|region| {
			ir.region(*region)
				.is_some_and(|held| held.kind == RegionKind::FieldFrame(FieldRole::Computed))
		})
		.collect();
	if frames.len().checked_sub(attrs.judged_regions()) != Some(names.len()) {
		return None;
	}
	Some(
		names
			.into_iter()
			.zip(frames)
			.map(|(name, frame)| LayerColumn {
				name,
				key: frame_is_key_part(ir, frame),
				frame,
			})
			.collect(),
	)
}

/// Whether the ops of the field frame `frame` run under the key-part frame.
fn frame_is_key_part(ir: &Ir, frame: RegionId) -> bool {
	let Some(last) = ir
		.region(frame)
		.and_then(|held| held.blocks.first())
		.and_then(|block| ir.block(*block))
		.and_then(|block| block.ops.last())
		.and_then(|op| ir.op(*op))
	else {
		return false;
	};
	ir.frame(last.frame).is_some_and(|authority| authority.eval.key_part)
}

/// What the conjunct of a filter reads of the rows of the layers beneath it.
enum Reads {
	/// No column of the layer: the conjunct judges the same value on either side.
	Free,
	/// Key columns only, by the field reads listed: each is the read of one element of the record
	/// id, which the layer passes through.
	Keys(Vec<OpId>),
	/// A column that is computed or guarded, the row other than by a field's name, or an index
	/// operator, which the statement's own access path answers.
	Held,
}

/// Whether `op` runs code its expression does not hold, or reads the row or the plan around it
/// other than through an operand: the invocation of a value, a method, a closure, a nested
/// statement (a `{ ... }` block bound in place included), the document, and a call whose body or
/// closure the planner cannot see. What such an op reaches cannot be told from the expression, so a
/// layer that hides or derives fields must stay beneath it.
pub(super) fn reads_row_opaquely(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	op: OpId,
) -> bool {
	match ir.op(op).map(|data| &data.kind) {
		Some(OpKind::Expr(
			ExprOp::Invoke
			| ExprOp::Dispatch(_)
			| ExprOp::Method {
				..
			}
			| ExprOp::ClosureCreate(_)
			| ExprOp::Subquery(_)
			| ExprOp::Doc,
		))
		| Some(OpKind::Logical(_))
		| None => true,
		Some(OpKind::Expr(_) | OpKind::Phys(_)) => {
			effects::summary_with(ir, facts, cx, op).set.intersects(EffectSet::OPAQUE_BODY)
		}
	}
}

/// What the conjunct `value` of `filter` reads of the layer with the columns `columns`.
fn reads_of(
	cx: &mut LoCx<'_>,
	filter: &WhereFilter,
	columns: &[LayerColumn],
	value: ValueId,
) -> Reads {
	let ir: &Ir = &*cx.ir;
	let computing = computing_ops(ir, value);
	if computing.is_empty() {
		return Reads::Held;
	}
	let mut tree: Vec<OpId> = computing.clone();
	for op in &computing {
		if let Some(data) = ir.op(*op) {
			for region in data.regions.iter().copied() {
				ir.walk(region, WalkOrder::PreOrder, &mut |nested| {
					tree.push(nested);
					Walk::Continue
				});
			}
		}
	}
	// An index operator is answered by the access path of the statement it is written in.
	let names_an_index_operator = tree.iter().any(|op| {
		matches!(
			ir.op(*op).map(|data| &data.kind),
			Some(OpKind::Expr(ExprOp::Binary {
				op: BinaryOperator::Matches(_) | BinaryOperator::NearestNeighbor(_),
			}))
		)
	});
	if names_an_index_operator
		|| tree.iter().any(|op| reads_row_opaquely(ir, &mut *cx.facts, cx.effects, *op))
	{
		return Reads::Held;
	}
	let mut keys = Vec::new();
	for held in ir.uses(filter.row) {
		if !tree.contains(&held.user) {
			continue;
		}
		// A read inside a body the conjunct holds, a loop or a closure, names that body's own
		// value; the operands of `AND` and `OR` and the arms of an `IF` read the row.
		if !(computing.contains(&held.user) || reads_through_operands(ir, held.user, &computing))
			|| held.operand != 0
		{
			return Reads::Held;
		}
		let Some(OpKind::Expr(ExprOp::Field {
			name,
		})) = ir.op(held.user).map(|data| &data.kind)
		else {
			return Reads::Held;
		};
		match columns.iter().find(|column| column.name == name.as_str()) {
			None => {}
			Some(column) if column.key && key_tree(ir, column).is_some() => keys.push(held.user),
			Some(_) => return Reads::Held,
		}
	}
	if keys.is_empty() {
		Reads::Free
	} else {
		Reads::Keys(keys)
	}
}

/// Whether `user` stands in the lazy operand of an `AND`, an `OR` or an `IF` arm of one of `ops`,
/// possibly nested, where the row is the row of the clause the op stands in.
pub(super) fn reads_through_operands(ir: &Ir, user: OpId, ops: &[OpId]) -> bool {
	for (region, owner) in ancestors(ir, user) {
		let held = ir.region(region).map(|region| region.kind);
		if !matches!(
			held,
			Some(RegionKind::LazyOperand | RegionKind::ValueIf | RegionKind::ErrorScope(_))
		) {
			return false;
		}
		if owner.is_some_and(|owner| ops.contains(&owner)) {
			return true;
		}
	}
	false
}

/// The op that yields the value of the key column `column` and the row its frame reads, when the
/// frame is a tree of ops that read that row alone and own no region.
fn key_tree(ir: &Ir, column: &LayerColumn) -> Option<(OpId, ValueId)> {
	let block = ir.block(*ir.region(column.frame)?.blocks.first()?)?;
	// The frame's arguments: the statement timestamp, the row and the judged value.
	let [timestamp, row, judged] = block.args.as_slice() else {
		return None;
	};
	if !ir.uses(*timestamp).is_empty() || !ir.uses(*judged).is_empty() {
		return None;
	}
	let root = def_op(ir, yielded_value(ir, column.frame)?)?;
	let tree = computing_ops(ir, ir.op(root)?.results.first().copied()?);
	let inert = tree.iter().all(|op| {
		ir.op(*op).is_some_and(|data| {
			data.regions.is_empty()
				&& data.operands.iter().all(|operand| {
					*operand == *row
						|| def_op(ir, *operand).is_some_and(|defining| tree.contains(&defining))
				})
		})
	});
	inert.then_some((root, *row))
}

/// Gives each block of the filter's clause its own read of every field of the row, and its own copy
/// of every literal, the clause shares between several blocks.
///
/// Common-subexpression elimination shares one read of a field or one literal between the
/// conjuncts of a predicate, so that a later conjunct, which stands in a block of its own, reads a
/// value the earlier one computes. A conjunct that reads a value outside its own block cannot be
/// moved on its own. A read of a field of the row and a literal have no effect and no cost to
/// speak of, so each reader outside the block of the value gets a copy of it at the start of its
/// own block.
pub(super) fn localize_row_reads(
	cx: &mut LoCx<'_>,
	filter: &WhereFilter,
	report: &mut PassReport,
) -> Result<(), IrError> {
	let mut shared: Vec<(OpId, Vec<(OpId, u16)>)> = Vec::new();
	for held in cx.ir.uses(filter.row) {
		let Some(read) = cx.ir.op(held.user) else {
			continue;
		};
		let (
			OpKind::Expr(ExprOp::Field {
				..
			}),
			0,
			[result],
		) = (&read.kind, held.operand, read.results.as_slice())
		else {
			continue;
		};
		let elsewhere: Vec<(OpId, u16)> = cx
			.ir
			.uses(*result)
			.iter()
			.filter(|reader| cx.ir.op(reader.user).is_some_and(|user| user.parent != read.parent))
			.map(|reader| (reader.user, reader.operand))
			.collect();
		if !elsewhere.is_empty() {
			shared.push((held.user, elsewhere));
		}
	}
	let ir: &Ir = &*cx.ir;
	let mut literals: Vec<OpId> = Vec::new();
	for region in ir.op(filter.op).map(|data| data.regions.clone()).unwrap_or_default() {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			if matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Expr(ExprOp::Literal(_)))) {
				literals.push(op);
			}
			Walk::Continue
		});
	}
	for literal in literals {
		let Some(read) = ir.op(literal) else {
			continue;
		};
		let [result] = read.results.as_slice() else {
			continue;
		};
		let elsewhere: Vec<(OpId, u16)> = ir
			.uses(*result)
			.iter()
			.filter(|reader| ir.op(reader.user).is_some_and(|user| user.parent != read.parent))
			.map(|reader| (reader.user, reader.operand))
			.collect();
		if !elsewhere.is_empty() {
			shared.push((literal, elsewhere));
		}
	}
	for (read, readers) in shared {
		let data = cx.ir.op(read).ok_or(IrError::Stale("op"))?.clone();
		let mut copies: FxHashMap<crate::exec::ir::iface::ids::BlockId, ValueId> =
			FxHashMap::default();
		for (reader, operand) in readers {
			let block = cx.ir.op(reader).ok_or(IrError::Stale("op"))?.parent;
			let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
			let copy = match copies.get(&block) {
				Some(copy) => *copy,
				None => {
					let Some(first) =
						rw.ir().block(block).and_then(|held| held.ops.first().copied())
					else {
						continue;
					};
					let ty = rw
						.ir()
						.value(*data.results.first().ok_or(IrError::Stale("value"))?)
						.ok_or(IrError::Stale("value"))?
						.ty
						.clone();
					let spec = OpSpec {
						kind: data.kind.clone(),
						operands: data.operands.clone(),
						results: smallvec![ty],
						frame: data.frame,
						snapshot: data.snapshot,
						loc: data.loc.clone(),
					};
					let made =
						rw.derived(&[read], |rw| rw.create(InsertPoint::Before(first), spec))?;
					let value = rw
						.ir()
						.op(made)
						.and_then(|held| held.results.first().copied())
						.ok_or(IrError::Stale("op"))?;
					copies.insert(block, value);
					report.rewrites = report.rewrites.saturating_add(1);
					value
				}
			};
			rw.set_operand(reader, operand, copy)?;
		}
	}
	Ok(())
}

/// The layers the filter reads its rows from, the topmost first: the layer directly beneath it and
/// every layer beneath that one, down to the op that is not a layer. Each layer is read by the op
/// above it alone, in the block of the filter. `None` when the filter reads no layer.
fn layer_stack(ir: &Ir, filter: &WhereFilter) -> Option<Vec<OpId>> {
	let mut stack = vec![target_below(ir, filter, Target::Layer)?];
	loop {
		let last = *stack.last()?;
		let input = *ir.op(last)?.operands.first()?;
		let Some(below) = def_op(ir, input) else {
			return Some(stack);
		};
		let Some(data) = ir.op(below) else {
			return Some(stack);
		};
		let is_layer = matches!(&data.kind, OpKind::Logical(LogOp::Project(attrs)) if attrs.layer);
		if !is_layer
			|| data.parent != ir.op(last)?.parent
			|| ir.uses(input).len() != 1
			|| data.results.len() != 1
		{
			return Some(stack);
		}
		stack.push(below);
	}
}

/// Moves the conjuncts of the filter `op` that read no column of the layers it reads beneath
/// those layers, directly above the op the stack stands on, and answers whether anything moved.
///
/// A conjunct that reads no column of the layer judges the same value on either side of it. A
/// conjunct that reads key columns only is the same expression over the element of the record id
/// each stands for, which the layer passes through, and moves with those reads replaced by a copy
/// of the column's frame over the stored row. Conjuncts move in written order, and none moves once
/// a conjunct that stays can raise, since `AND` reaches the one that stays first. A literal
/// `false` moves only with every other conjunct, so that the predicate keeps the cue that it
/// admits nothing. The layer derives its columns only for the rows that reach it.
fn push_below_layer(cx: &mut LoCx<'_>, op: OpId, report: &mut PassReport) -> Result<bool, IrError> {
	let Some(filter) = where_filter(cx.ir, op) else {
		return Ok(false);
	};
	let Some(stack) = layer_stack(cx.ir, &filter) else {
		return Ok(false);
	};
	let mut columns: Vec<LayerColumn> = Vec::new();
	for layer in &stack {
		let Some(held) = layer_columns(cx.ir, *layer) else {
			return Ok(false);
		};
		columns.extend(held);
	}
	// A name two layers set is told from neither: no read of it is a read of an element of the id.
	for at in 0..columns.len() {
		if columns.iter().filter(|other| other.name == columns[at].name).count() > 1 {
			columns[at].key = false;
		}
	}
	let Some(layer) = stack.last().copied() else {
		return Ok(false);
	};
	let Some(source) = cx.ir.op(layer).and_then(|data| data.operands.first().copied()) else {
		return Ok(false);
	};
	localize_row_reads(cx, &filter, report)?;
	let chain = and_chain(cx.ir, filter.predicate);
	let mut moved: Vec<usize> = Vec::new();
	let mut falses: Vec<usize> = Vec::new();
	let mut raising_stands = false;
	let mut stays = 0;
	for (at, value) in chain.iter().enumerate() {
		if is_false_literal(cx.ir, *value) {
			falses.push(at);
			continue;
		}
		let computing = computing_ops(cx.ir, *value);
		let infallible = !computing.is_empty() && infallible_over_row(cx, &computing, filter.row);
		let movable = match reads_of(cx, &filter, &columns, *value) {
			Reads::Free => can_move(cx.ir, &filter, *value, layer, source),
			Reads::Keys(_) => can_move(cx.ir, &filter, *value, layer, source),
			Reads::Held => false,
		};
		if movable && !raising_stands && (stays == 0 && falses.is_empty() || infallible) {
			moved.push(at);
		} else {
			stays += 1;
			raising_stands |= !infallible;
		}
	}
	if stays == 0 {
		moved.extend(falses);
		moved.sort_unstable();
	}
	if moved.is_empty() {
		return Ok(false);
	}
	move_selected(cx, &filter, &chain, &moved, layer, source, &mut |rw, row| {
		substitute_key_reads(rw, row, &columns)
	})?;
	Ok(true)
}

/// Whether `value` is the literal `false`.
fn is_false_literal(ir: &Ir, value: ValueId) -> bool {
	def_op(ir, value).and_then(|op| ir.op(op)).is_some_and(|data| {
		matches!(&data.kind, OpKind::Expr(ExprOp::Literal(held)) if matches!(**held, Value::Bool(false)))
	})
}

/// Re-creates the conjuncts of `chain` at the positions `moved` in a new filter over `source`,
/// created directly before `anchor`, and makes `anchor` read the new filter. The conjuncts that
/// stay keep their order in `filter`; when none stays, `filter` is replaced by its input. Once the
/// conjuncts stand in the new filter, `rebind` is called with the rewriter and the row of the new
/// filter, to replace the reads of the row the conjuncts made. Answers the new filter.
pub(super) fn move_selected(
	cx: &mut LoCx<'_>,
	filter: &WhereFilter,
	chain: &[ValueId],
	moved: &[usize],
	anchor: OpId,
	source: ValueId,
	rebind: &mut dyn FnMut(&mut Rewriter<'_>, ValueId) -> Result<(), IrError>,
) -> Result<OpId, IrError> {
	let taken: Vec<ValueId> = moved.iter().map(|at| chain[*at]).collect();
	let kept: Vec<ValueId> =
		(0..chain.len()).filter(|at| !moved.contains(at)).map(|at| chain[at]).collect();
	let spine = chain_spine(cx.ir, filter.predicate);
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	let template = rw.ir().op(filter.op).ok_or(IrError::Stale("op"))?.clone();
	let ty = rw.ir().value(source).ok_or(IrError::Stale("value"))?.ty.clone();
	let spec = OpSpec {
		kind: OpKind::Logical(LogOp::Filter),
		operands: smallvec![source],
		results: smallvec![ty],
		frame: template.frame,
		snapshot: template.snapshot,
		loc: template.loc.clone(),
	};
	let lower = rw.derived(&[filter.op], |rw| rw.create(InsertPoint::Before(anchor), spec))?;
	let (_, body) =
		rw.create_region(lower, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])?;
	let row = rw
		.ir()
		.block(body)
		.and_then(|held| held.args.first().copied())
		.ok_or(IrError::Stale("block"))?;
	let chain_moved =
		rebuild_and_chain_bound(&mut rw, InsertPoint::End(body), &taken, &[(filter.row, row)])?;
	let close = OpSpec {
		kind: OpKind::Expr(ExprOp::Yield),
		operands: smallvec![chain_moved],
		results: SmallVec::new(),
		frame: template.frame,
		snapshot: template.snapshot,
		loc: template.loc,
	};
	rw.derived(&[filter.op], |rw| rw.create(InsertPoint::End(body), close))?;
	let lower_result = rw
		.ir()
		.op(lower)
		.and_then(|data| data.results.first().copied())
		.ok_or(IrError::Stale("op"))?;
	rw.set_operand(anchor, 0, lower_result)?;
	rebind(&mut rw, row)?;

	if kept.is_empty() {
		rw.replace_all_uses(filter.result, filter.input)?;
		rw.erase(filter.op, EliminationReason::MergedInto(lower))?;
		return Ok(lower);
	}
	let rest = rebuild_and_chain(&mut rw, InsertPoint::Before(filter.yielded), &kept)?;
	rw.set_operand(filter.yielded, 0, rest)?;
	for gate in spine {
		if rw.ir().op(gate).is_none() {
			continue;
		}
		match rw.erase(gate, EliminationReason::DeadPure) {
			Ok(()) | Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	Ok(lower)
}

/// Replaces each read of a key column off `row` by a copy of the frame of the column over `row`.
fn substitute_key_reads(
	rw: &mut Rewriter<'_>,
	row: ValueId,
	columns: &[LayerColumn],
) -> Result<(), IrError> {
	let mut reads: Vec<(OpId, usize)> = Vec::new();
	for held in rw.ir().uses(row) {
		let Some(OpKind::Expr(ExprOp::Field {
			name,
		})) = rw.ir().op(held.user).map(|data| &data.kind)
		else {
			continue;
		};
		if let Some(at) =
			columns.iter().position(|column| column.key && column.name == name.as_str())
		{
			reads.push((held.user, at));
		}
	}
	for (read, at) in reads {
		let Some((root, frame_row)) = key_tree(rw.ir(), &columns[at]) else {
			return Err(IrError::Refused("key column frame"));
		};
		let data = rw.ir().op(read).ok_or(IrError::Stale("op"))?;
		let (result, frame, snapshot) =
			(data.results.first().copied().ok_or(IrError::Stale("op"))?, data.frame, data.snapshot);
		let root_result = rw
			.ir()
			.op(root)
			.and_then(|held| held.results.first().copied())
			.ok_or(IrError::Stale("op"))?;
		let tree = computing_ops(rw.ir(), root_result);
		let copied = copy_ops(rw, &tree, read, (frame_row, row), frame, snapshot)?;
		let Some(value) = copied.get(&root_result).copied() else {
			return Err(IrError::Refused("key column frame"));
		};
		rw.replace_all_uses(result, value)?;
		rw.erase(read, EliminationReason::DeadPure)?;
	}
	Ok(())
}

/// Creates a copy of the region-free ops `tree`, in order, before `at`, reading `to` where they
/// read `from`, under `frame` and `snapshot`, and answers the copy of each result.
fn copy_ops(
	rw: &mut Rewriter<'_>,
	tree: &[OpId],
	at: OpId,
	(from, to): (ValueId, ValueId),
	frame: FrameId,
	snapshot: SnapshotId,
) -> Result<FxHashMap<ValueId, ValueId>, IrError> {
	let mut renamed: FxHashMap<ValueId, ValueId> = FxHashMap::from_iter([(from, to)]);
	for old in tree {
		let data = rw.ir().op(*old).ok_or(IrError::Stale("op"))?.clone();
		if !data.regions.is_empty() {
			return Err(IrError::Refused("key column frame"));
		}
		let mut operands: SmallVec<[ValueId; 4]> = SmallVec::new();
		for operand in &data.operands {
			operands.push(renamed.get(operand).copied().ok_or(IrError::Stale("operand"))?);
		}
		let mut results: SmallVec<[TypeKnowledge; 2]> = SmallVec::new();
		for result in &data.results {
			results.push(rw.ir().value(*result).ok_or(IrError::Stale("value"))?.ty.clone());
		}
		let spec = OpSpec {
			kind: data.kind.clone(),
			operands,
			results,
			frame,
			snapshot,
			loc: data.loc.clone(),
		};
		let new = rw.derived(&[*old], |rw| rw.create(InsertPoint::Before(at), spec))?;
		let made = rw.ir().op(new).ok_or(IrError::Stale("op"))?.results.clone();
		for (before, after) in data.results.iter().zip(&made) {
			renamed.insert(*before, *after);
		}
	}
	Ok(renamed)
}

/// Whether the conjunct `value` is infallible and reads nothing beyond the row.
fn admits_below_permission(cx: &mut LoCx<'_>, value: ValueId) -> bool {
	let computing = computing_ops(cx.ir, value);
	!computing.is_empty() && pure_and_infallible(cx, &computing)
}

/// Re-creates the first `moving` conjuncts of `chain` in a new filter over `source`, created
/// directly before `anchor`, and makes `anchor` read the new filter. The conjuncts that stay keep
/// their order in `filter`; when none stays, `filter` is replaced by its input.
pub(super) fn move_prefix(
	cx: &mut LoCx<'_>,
	filter: &WhereFilter,
	chain: &[ValueId],
	moving: usize,
	anchor: OpId,
	source: ValueId,
) -> Result<(), IrError> {
	let spine = chain_spine(cx.ir, filter.predicate);
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	let template = rw.ir().op(filter.op).ok_or(IrError::Stale("op"))?.clone();
	let ty = rw.ir().value(source).ok_or(IrError::Stale("value"))?.ty.clone();
	let spec = OpSpec {
		kind: OpKind::Logical(LogOp::Filter),
		operands: smallvec![source],
		results: smallvec![ty],
		frame: template.frame,
		snapshot: template.snapshot,
		loc: template.loc.clone(),
	};
	let lower = rw.derived(&[filter.op], |rw| rw.create(InsertPoint::Before(anchor), spec))?;
	let (_, body) =
		rw.create_region(lower, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])?;
	let row = rw
		.ir()
		.block(body)
		.and_then(|held| held.args.first().copied())
		.ok_or(IrError::Stale("block"))?;
	let moved = rebuild_and_chain_bound(
		&mut rw,
		InsertPoint::End(body),
		&chain[..moving],
		&[(filter.row, row)],
	)?;
	let close = OpSpec {
		kind: OpKind::Expr(ExprOp::Yield),
		operands: smallvec![moved],
		results: SmallVec::new(),
		frame: template.frame,
		snapshot: template.snapshot,
		loc: template.loc,
	};
	rw.derived(&[filter.op], |rw| rw.create(InsertPoint::End(body), close))?;
	let lower_result = rw
		.ir()
		.op(lower)
		.and_then(|data| data.results.first().copied())
		.ok_or(IrError::Stale("op"))?;
	rw.set_operand(anchor, 0, lower_result)?;

	if moving == chain.len() {
		rw.replace_all_uses(filter.result, filter.input)?;
		rw.erase(filter.op, EliminationReason::MergedInto(lower))?;
		return Ok(());
	}
	let rest = rebuild_and_chain(&mut rw, InsertPoint::Before(filter.yielded), &chain[moving..])?;
	rw.set_operand(filter.yielded, 0, rest)?;
	for gate in spine {
		if rw.ir().op(gate).is_none() {
			continue;
		}
		match rw.erase(gate, EliminationReason::DeadPure) {
			Ok(()) | Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	Ok(())
}

/// The `AND` ops along the left side of the chain whose root is `root`, root first: the ops a
/// rebuilt chain leaves unread.
pub(super) fn chain_spine(ir: &Ir, root: ValueId) -> Vec<OpId> {
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
