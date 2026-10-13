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
//! - [`PushBelowLayer`] moves a left prefix of a `WHERE` conjunction from the filter above a
//!   projection layer to a filter directly below it, reading the stored row. It runs only when the
//!   conjuncts read the stored row alone and the layer's per-row work cannot be observed when it is
//!   skipped for a rejected row.
//! - [`PushBelowPermission`] moves a left prefix of a `WHERE` conjunction from the filter above a
//!   permission filter to a filter directly below it. It runs only when every moved conjunct is
//!   infallible and reads nothing beyond the row, the permission predicate is total, and no write
//!   of the statement reaches what the permission reads.
//!
//! Conjuncts keep their written order and only a left prefix moves, so the first error of a row is
//! the one the unmoved plan raises. Neither push drops a permission conjunct. Moved ops are
//! re-created under `derived`, so provenance names the filter they left, and an op keeps its frame
//! and snapshot. A push checks everything a re-creation can refuse before it changes anything.
//! Code inside a stored clause is never rewritten.
#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;
use std::future::{Future, ready};
use std::pin::Pin;

use smallvec::{SmallVec, smallvec};

use super::driver::{LoCx, LoPass, PassConfig, PassReport};
use super::rehome::{and_chain, can_rehome, rebuild_and_chain, rebuild_and_chain_bound};
use crate::exec::ir::analysis::frames::{self, RowView};
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ShortCircuitOp};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, LogOp};
use crate::exec::ir::op_kind::OpKind;
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

	fn run<'a, 'b>(
		&'a self,
		cx: &'a mut LoCx<'b>,
	) -> Pin<Box<dyn Future<Output = Result<PassReport, IrError>> + Send + 'a>> {
		Box::pin(ready(run_specialise(cx)))
	}
}

/// Moves a prefix of the `WHERE` conjuncts below a projection layer.
///
/// Applies to a filter whose only input is a layer in the same block. Moves the longest left
/// prefix of conjuncts that need only the stored row and can each be re-created below the layer,
/// and only when the layer's per-row work is unobservable.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PushBelowLayer;

impl LoPass for PushBelowLayer {
	fn name(&self) -> &'static str {
		"push_below_layer"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.push_below_layer
	}

	fn run<'a, 'b>(
		&'a self,
		cx: &'a mut LoCx<'b>,
	) -> Pin<Box<dyn Future<Output = Result<PassReport, IrError>> + Send + 'a>> {
		Box::pin(ready(run_push(cx, Target::Layer)))
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

	fn run<'a, 'b>(
		&'a self,
		cx: &'a mut LoCx<'b>,
	) -> Pin<Box<dyn Future<Output = Result<PassReport, IrError>> + Send + 'a>> {
		Box::pin(ready(run_push(cx, Target::Permission)))
	}
}

/// Whether `op` stands inside a stored clause, whose code is bound and analysed but never
/// rewritten.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Every op outside stored clauses, in program order: an op before the ops of its regions.
fn live_ops(ir: &Ir) -> Vec<OpId> {
	let mut ops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if !in_stored_clause(ir, op) {
			ops.push(op);
		}
		Walk::Continue
	});
	ops
}

/// The op that defines `value`, or `None` for a block argument and for a dangling id.
fn def_op(ir: &Ir, value: ValueId) -> Option<OpId> {
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
fn yielded_value(ir: &Ir, region: RegionId) -> Option<ValueId> {
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

/// The names of the columns the layer `layer` guards or computes, whose reads a filter must not
/// take below the layer; `None` when they cannot be told, and no read may then move.
///
/// A layer that holds a permission guard or a field frame guards every column it names, since the
/// guards and frames pair with the named columns. A layer that holds neither guards nothing.
pub(super) fn guarded_names(ir: &Ir, layer: OpId) -> Option<Vec<String>> {
	let data = ir.op(layer)?;
	let carries_guards = data.regions.iter().any(|region| {
		ir.region(*region).is_some_and(|held| {
			matches!(held.kind, RegionKind::PermissionFrame | RegionKind::FieldFrame(_))
		})
	});
	if !carries_guards {
		return Some(Vec::new());
	}
	let OpKind::Logical(LogOp::Project(attrs)) = &data.kind else {
		return None;
	};
	let FieldsShape::Select(shape) = &attrs.fields else {
		return None;
	};
	if shape.iter().any(|field| matches!(field, FieldShape::All)) {
		return None;
	}
	select_names(shape)
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
struct WhereFilter {
	op: OpId,
	input: ValueId,
	result: ValueId,
	row: ValueId,
	yielded: OpId,
	predicate: ValueId,
}

/// The shape of `op` when it is a filter holding one `Clause(Where)` region of one block that
/// takes the row and ends in a yield of one value.
fn where_filter(ir: &Ir, op: OpId) -> Option<WhereFilter> {
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
fn computing_ops(ir: &Ir, value: ValueId) -> Vec<OpId> {
	let Some(root) = def_op(ir, value) else {
		return Vec::new();
	};
	let Some(block) = ir.op(root).map(|data| data.parent) else {
		return Vec::new();
	};
	let mut seen: HashSet<OpId> = HashSet::new();
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
	}
	ir.block(block)
		.map(|held| held.ops.iter().copied().filter(|op| seen.contains(op)).collect())
		.unwrap_or_default()
}

/// Whether every op of `ops`, regions included, is infallible and has effects within the row
/// alone.
fn pure_and_infallible(cx: &mut LoCx<'_>, ops: &[OpId]) -> bool {
	ops.iter().all(|op| {
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

/// Whether every read of `row` by `ops`, or by an op inside one, is a read of a field off it that
/// is not named in `guarded`. Any other use sees the row as a whole.
pub(super) fn reads_row_by_field(ir: &Ir, row: ValueId, ops: &[OpId], guarded: &[String]) -> bool {
	ir.uses(row)
		.iter()
		.filter(|held| {
			ops.contains(&held.user)
				|| ancestors(ir, held.user)
					.any(|(_, owner)| owner.is_some_and(|owner| ops.contains(&owner)))
		})
		.all(|held| {
			held.operand == 0
				&& ir
					.op(held.user)
					.and_then(|user| field_read_name(&user.kind))
					.is_some_and(|name| !guarded.iter().any(|guard| guard.as_str() == name))
		})
}

/// Whether the conjunct `value` of `filter` may move below `anchor`, reading `source` for the
/// row. It must be computed inside the filter's clause and pass every check a re-creation makes.
fn can_move(ir: &Ir, filter: &WhereFilter, value: ValueId, anchor: OpId, source: ValueId) -> bool {
	let Some(def) = def_op(ir, value) else {
		return false;
	};
	let inside = ancestors(ir, def).any(|(_, owner)| owner == Some(filter.op));
	inside && can_rehome(ir, def, InsertPoint::Before(anchor), &[(filter.row, source)])
}

fn run_push(cx: &mut LoCx<'_>, target: Target) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	for op in live_ops(cx.ir) {
		if push_prefix(cx, op, target)? {
			report.rewrites = report.rewrites.saturating_add(1);
		}
	}
	Ok(report)
}

/// Moves the longest admissible left prefix of the conjuncts of the filter `op` below the op
/// `target` names, and answers whether anything moved.
fn push_prefix(cx: &mut LoCx<'_>, op: OpId, target: Target) -> Result<bool, IrError> {
	let Some(filter) = where_filter(cx.ir, op) else {
		return Ok(false);
	};
	let Some(anchor) = target_below(cx.ir, &filter, target) else {
		return Ok(false);
	};
	let Some(source) = cx.ir.op(anchor).and_then(|data| data.operands.first().copied()) else {
		return Ok(false);
	};
	let gate_open = match target {
		Target::Layer => {
			frames::layer_work_unobservable_with(&*cx.ir, &mut *cx.facts, cx.effects, anchor)
		}
		Target::Permission => {
			permission_is_total(cx, anchor)
				&& frames::merge_ok_with(&*cx.ir, &mut *cx.facts, cx.effects, anchor).condition1
		}
	};
	if !gate_open {
		return Ok(false);
	}
	let chain = and_chain(cx.ir, filter.predicate);
	let mut moving = 0;
	for value in &chain {
		let admitted = match target {
			Target::Layer => admits_below_layer(cx, &filter, anchor, *value),
			Target::Permission => admits_below_permission(cx, *value),
		};
		if !admitted || !can_move(cx.ir, &filter, *value, anchor, source) {
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

/// Whether the conjunct `value` needs only the stored row the layer `layer` takes in.
///
/// The layer's own answer covers reads of the row the layer delivers as an operand; a filter reads
/// that row through its clause's argument, so every read of the argument must also be a read of a
/// field the layer neither guards nor computes. Below the layer a read sees the stored row, not
/// the masked or derived value.
fn admits_below_layer(
	cx: &mut LoCx<'_>,
	filter: &WhereFilter,
	layer: OpId,
	value: ValueId,
) -> bool {
	let Some(def) = def_op(cx.ir, value) else {
		return false;
	};
	let Some(guarded) = guarded_names(cx.ir, layer) else {
		return false;
	};
	let computing = computing_ops(cx.ir, value);
	frames::row_view(&*cx.ir, &mut *cx.facts, def, layer) == RowView::Real
		&& reads_row_by_field(cx.ir, filter.row, &computing, &guarded)
}

/// Whether the conjunct `value` is infallible and reads nothing beyond the row.
fn admits_below_permission(cx: &mut LoCx<'_>, value: ValueId) -> bool {
	let computing = computing_ops(cx.ir, value);
	!computing.is_empty() && pure_and_infallible(cx, &computing)
}

/// Re-creates the first `moving` conjuncts of `chain` in a new filter over `source`, created
/// directly before `anchor`, and makes `anchor` read the new filter. The conjuncts that stay keep
/// their order in `filter`; when none stays, `filter` is replaced by its input.
fn move_prefix(
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
