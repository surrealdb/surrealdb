//! Dead-code elimination: an op whose results nothing reads and whose removal no run can observe
//! is erased, and every erase records why.
//!
//! Five reasons remove an op:
//!
//! - `DeadPure`: the op is transitively infallible and its effects are reads of the snapshot, the
//!   catalog, the row, the session or the clock. An `expr.computed_read` is removed only when an
//!   earlier read of the same field of the same row, under the same frame and snapshot, dominates
//!   it: the first read derives the field and every later one reads the memo. A lone read is kept,
//!   because its definition may draw, write or raise and the derivation happens at the read.
//! - `InvisibleToPrincipal`: an `expr.computed_read` in the field frame of a layer projection whose
//!   guard is the literal `false`. Its readers see `NONE`. Guards and field frames are paired by
//!   position and only when the projection holds one guard per field frame, so a projection without
//!   guard regions matches nothing.
//! - `DeadLayerColumn`: a column of a projection layer that no op reads, and a layer left with no
//!   column. The reads of the layer's rows come from its readers: a field read adds the field to
//!   what is demanded, and any other use of the row, or a `*`, demands every field. A column that
//!   another column of the same layer reads stays, and the readers of an erased layer read its
//!   input.
//! - `Unreachable`: an op that follows a terminator in its block.
//! - `DeadBeforeCertainError`, behind `PassConfig::dce_certain_error`: an op standing before a
//!   raise that is certain, inside the region the raise cancels. The raise stands in a statement
//!   block, reached through statement and block regions only, and no op before it can divert
//!   control past it. The cancelling region is the enclosing auto-committed statement or explicit
//!   transaction, never a session transaction. A predecessor goes only when it is infallible, its
//!   effects are reads or every one is undone by the cancellation, and nothing a rollback leaves
//!   behind is among them. An op that writes the session goes only when no op that stays after it
//!   reads the session or the catalog.
//!
//! Some ops are never removed for lack of uses: terminators, requests, transactions and
//! statements, writes and write stages, `TIMEOUT`, depth charges, ops without a result, ops that
//! hold a write, ops whose region holds a `RETURN`, `BREAK` or `CONTINUE` that lands outside
//! them, a `LET` in a container that serves late-bound statements, which read it by name, and the
//! ops of a statement an `EXPLAIN` explains, which prints them. Code inside a stored
//! clause is bound and analysed but never rewritten.
//!
//! Ops go in reverse program order, so an op's readers are gone before the op is judged. A sweep
//! that erases anything is followed by another, up to the greedy driver's pass bound.
#![deny(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;

use smallvec::{SmallVec, smallvec};

use super::driver::{LoCx, LoPass, PassConfig, PassReport, PassRun};
use super::perm_where::reads_row_opaquely;
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashSet;
use crate::exec::ir::core::ir::{Ir, IrScope, OpList, Walk, WalkOrder};
use crate::exec::ir::core::pattern::{GREEDY_DEFAULT, Limit};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ControlTarget, ancestors, control_target};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ComputedReadAttrs, ExprOp, RaiseKind};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::effects::Rollback;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{
	ClauseRole, FieldRole, RegionKind, RegionProperties, StatementCommit, TxnKind,
};
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, LogOp, LoopKind, ProjectAttrs};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::logical::EffectSet;
use crate::val::Value;

/// How many times the sweeps are repeated while they keep erasing.
const MAX_ROUNDS: u8 = GREEDY_DEFAULT.max_passes;

/// The effects that only read: the snapshot, the catalog, the row, the session and the clock.
const READS: EffectSet = EffectSet::READS_DATA
	.union(EffectSet::READS_CATALOG)
	.union(EffectSet::READS_ROW)
	.union(EffectSet::READS_SESSION)
	.union(EffectSet::CLOCK);

/// The reads through which a later op can observe what a session write left.
const SESSION_READS: EffectSet = EffectSet::READS_SESSION.union(EffectSet::READS_CATALOG);

/// Removes the ops of a container that nothing observes.
///
/// `DeadPure`, `InvisibleToPrincipal` and `Unreachable` run whenever the pass is enabled;
/// `DeadBeforeCertainError` also needs `PassConfig::dce_certain_error`.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Dce;

impl LoPass for Dce {
	fn name(&self) -> &'static str {
		"dce"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.dce
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_dce(cx))
	}
}

fn run_dce(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	for round in 0..MAX_ROUNDS {
		let mut erased = erase_unreachable(cx)?;
		erased += erase_invisible(cx)?;
		erased += erase_dead_layer_columns(cx)?;
		if cx.cfg.dce_certain_error {
			erased += erase_before_certain_error(cx)?;
		}
		erased += erase_dead_pure(cx)?;
		report.rewrites = report.rewrites.saturating_add(erased);
		if erased == 0 {
			return Ok(report);
		}
		if round + 1 == MAX_ROUNDS {
			report.stopped_by = Some(Limit::Passes);
		}
	}
	Ok(report)
}

/// Whether `op` stands inside a stored clause, whose code is bound and analysed but never
/// rewritten.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Every op outside stored clauses, in program order: an op before the ops of its regions.
fn live_ops(ir: &Ir) -> OpList {
	ir.collect_ops(ir.top(), WalkOrder::PreOrder, |op| !in_stored_clause(ir, op))
}

/// Erases `op` and answers whether it was erased. An op whose results are still read is left
/// alone. Every caller judges `op` live before erasing it, so an op that has gone is a stale id.
fn erase(cx: &mut LoCx<'_>, op: OpId, reason: EliminationReason) -> Result<bool, IrError> {
	if cx.ir.op(op).is_none() {
		return Err(IrError::Stale("dce: an op to erase"));
	}
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	match rw.erase(op, reason) {
		Ok(()) => Ok(true),
		Err(IrError::HasUses(_)) => Ok(false),
		Err(error) => Err(error),
	}
}

/// Whether no result of `op` is read.
fn results_unused(ir: &Ir, op: OpId) -> bool {
	ir.op(op).is_some_and(|data| data.results.iter().all(|result| ir.uses(*result).is_empty()))
}

/// Whether `op` is never removed for lack of uses.
fn is_pinned(ir: &Ir, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return true;
	};
	if data.results.is_empty() || data.kind.is_terminator() {
		return true;
	}
	if matches!(
		data.kind,
		OpKind::Phys(_)
			| OpKind::Logical(
				LogOp::Request { .. }
					| LogOp::Txn { .. }
					| LogOp::Stmt { .. }
					| LogOp::Statement { .. }
					| LogOp::Write(_)
					| LogOp::WriteStage { .. }
					| LogOp::Timeout
			) | OpKind::Expr(ExprOp::DepthCharge { .. })
	) {
		return true;
	}
	holds_write(ir, op) || lets_control_escape(ir, op) || read_by_served_statements(ir, op)
}

/// Whether `op` is a `LET` local to its list in a container that serves late-bound statements:
/// such a statement is planned when the list reaches it and reads the names bound around it by
/// name, so the binding stands whatever the container's own ops read of it.
fn read_by_served_statements(ir: &Ir, op: OpId) -> bool {
	if !matches!(ir.op(op).map(|data| &data.kind), Some(OpKind::Logical(LogOp::Let(_)))) {
		return false;
	}
	let mut serves = false;
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |at| {
		if matches!(
			ir.op(at).map(|data| &data.kind),
			Some(OpKind::Logical(LogOp::Statement {
				served: Some(_),
				..
			}))
		) {
			serves = true;
			return Walk::Stop;
		}
		Walk::Continue
	});
	serves
}

/// Whether `op` stands in a region of an op whose value an `EXPLAIN` explains: the `EXPLAIN`
/// prints every statement of that plan and runs none of them.
fn in_explained_statement(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).filter_map(|(_, owner)| ir.op(owner?)).any(|owner| {
		owner.results.iter().any(|result| {
			ir.uses(*result).iter().any(|used| {
				matches!(
					ir.op(used.user).map(|user| &user.kind),
					Some(OpKind::Logical(LogOp::Explain { .. }))
				)
			})
		})
	})
}

/// Whether a write or a write stage stands in a region `op` owns.
fn holds_write(ir: &Ir, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let mut found = false;
	for region in &data.regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |inner| {
			let is_write = ir.op(inner).is_some_and(|held| {
				matches!(held.kind, OpKind::Logical(LogOp::Write(_) | LogOp::WriteStage { .. }))
			});
			if is_write {
				found = true;
				return Walk::Stop;
			}
			Walk::Continue
		});
		if found {
			break;
		}
	}
	found
}

/// Whether a `RETURN`, `BREAK` or `CONTINUE` in a region `owner` owns lands outside `owner`.
fn lets_control_escape(ir: &Ir, owner: OpId) -> bool {
	let Some(data) = ir.op(owner) else {
		return false;
	};
	let mut escapes = false;
	for region in &data.regions {
		ir.walk(*region, WalkOrder::PreOrder, &mut |inner| {
			let stays = match control_target(ir, inner) {
				ControlTarget::NotControl => true,
				ControlTarget::Region(target) => lands_within(ir, inner, target, owner),
				ControlTarget::Escapes(_) | ControlTarget::Barrier(_) => false,
			};
			if stays {
				Walk::Continue
			} else {
				escapes = true;
				Walk::Stop
			}
		});
		if escapes {
			break;
		}
	}
	escapes
}

/// Whether `target`, where the control op `control` lands, is a region `owner` owns or lies
/// beneath one.
fn lands_within(ir: &Ir, control: OpId, target: RegionId, owner: OpId) -> bool {
	let mut at_target = false;
	for (region, held_by) in ancestors(ir, control) {
		at_target |= region == target;
		if at_target && held_by == Some(owner) {
			return true;
		}
	}
	false
}

/// Erases the ops that follow a terminator in their block, and answers how many went.
fn erase_unreachable(cx: &mut LoCx<'_>) -> Result<u32, IrError> {
	let mut tails: Vec<Vec<OpId>> = Vec::new();
	let mut seen = FxHashSet::default();
	for op in live_ops(cx.ir) {
		let Some(data) = cx.ir.op(op) else {
			continue;
		};
		if !data.kind.is_terminator() || !seen.insert(data.parent) {
			continue;
		}
		let Some(block) = cx.ir.block(data.parent) else {
			continue;
		};
		let Some(at) = block.ops.iter().position(|held| *held == op) else {
			continue;
		};
		let tail = block.ops[at + 1..].to_vec();
		if !tail.is_empty() {
			tails.push(tail);
		}
	}
	let mut erased = 0;
	for tail in tails {
		for op in tail.into_iter().rev() {
			if cx.ir.op(op).is_some() && erase(cx, op, EliminationReason::Unreachable)? {
				erased += 1;
			}
		}
	}
	Ok(erased)
}

/// Erases the computed reads a literal-`false` field guard hides, and answers how many went. The
/// readers of an erased read see `NONE`.
fn erase_invisible(cx: &mut LoCx<'_>) -> Result<u32, IrError> {
	let mut erased = 0;
	for read in invisible_reads(cx.ir).into_iter().rev() {
		if cx.ir.op(read).is_some() && hide_read(cx, read)? {
			erased += 1;
		}
	}
	Ok(erased)
}

/// The computed reads in the field frames of layer projections whose paired guard is the literal
/// `false`, in program order.
fn invisible_reads(ir: &Ir) -> Vec<OpId> {
	let mut hidden = Vec::new();
	for op in live_ops(ir) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		let OpKind::Logical(LogOp::Project(attrs)) = &data.kind else {
			continue;
		};
		if !attrs.layer {
			continue;
		}
		let regions_of = |wanted: fn(RegionKind) -> bool| -> Vec<RegionId> {
			data.regions
				.iter()
				.copied()
				.filter(|region| ir.region(*region).is_some_and(|held| wanted(held.kind)))
				.collect()
		};
		let guard_regions = regions_of(|kind| matches!(kind, RegionKind::PermissionFrame));
		let frames = regions_of(|kind| matches!(kind, RegionKind::FieldFrame(FieldRole::Computed)));
		if guard_regions.is_empty() || guard_regions.len() != frames.len() {
			continue;
		}
		for (guard, frame) in guard_regions.iter().zip(&frames) {
			if !guard_is_false(ir, *guard) {
				continue;
			}
			ir.walk(*frame, WalkOrder::PreOrder, &mut |inner| {
				let is_read = ir
					.op(inner)
					.is_some_and(|held| matches!(held.kind, OpKind::Expr(ExprOp::ComputedRead(_))));
				if is_read {
					hidden.push(inner);
				}
				Walk::Continue
			});
		}
	}
	hidden
}

/// Whether the guard `region` yields the literal `false`.
fn guard_is_false(ir: &Ir, region: RegionId) -> bool {
	let Some([block]) = ir.region(region).map(|held| held.blocks.as_slice()) else {
		return false;
	};
	let Some(last) =
		ir.block(*block).and_then(|held| held.ops.last()).and_then(|last| ir.op(*last))
	else {
		return false;
	};
	let (OpKind::Expr(ExprOp::Yield), [value]) = (&last.kind, last.operands.as_slice()) else {
		return false;
	};
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(*value).map(|data| data.def)
	else {
		return false;
	};
	ir.op(op).is_some_and(|def| {
		matches!(&def.kind, OpKind::Expr(ExprOp::Literal(held)) if matches!(**held, Value::Bool(false)))
	})
}

/// Points every reader of the computed read `read` at a `NONE` literal and erases it, and answers
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
		let literal = rw.create(InsertPoint::Before(read), spec)?;
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

/// What the readers of some rows read of them.
enum Demand {
	/// Every field: a use of the row that is not a read of a named field, and a `*`.
	Whole,
	/// These fields and no other.
	Fields(HashSet<String>),
}

impl Demand {
	fn none() -> Self {
		Self::Fields(HashSet::new())
	}

	fn join(&mut self, other: Demand) {
		match (&mut *self, other) {
			(Demand::Fields(held), Demand::Fields(more)) => held.extend(more),
			(Demand::Whole, _) => {}
			(Demand::Fields(_), Demand::Whole) => *self = Demand::Whole,
		}
	}
}

/// How deep the demand follows the rows through the operators that pass them on.
const DEMAND_DEPTH: usize = 32;

/// The fields of the rows `value` carries that the ops after it read.
///
/// The row passes through the operators that keep it, which read it through the regions they own,
/// and through a layer, which sets its own columns on it. A projection or an aggregate ends the
/// row. Any other reader takes the whole row.
fn demand_of_rows(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	value: ValueId,
	depth: usize,
) -> Demand {
	if depth > DEMAND_DEPTH {
		return Demand::Whole;
	}
	let mut demand = Demand::none();
	for held in ir.uses(value) {
		let Some(user) = ir.op(held.user) else {
			return Demand::Whole;
		};
		if held.operand != 0 {
			return Demand::Whole;
		}
		match &user.kind {
			OpKind::Logical(
				LogOp::Filter
				| LogOp::PermissionFilter {
					..
				}
				| LogOp::Sort(_)
				| LogOp::Split {
					..
				}
				| LogOp::Limit {
					..
				}
				| LogOp::Only,
			) => {
				demand.join(demand_of_regions(ir, facts, cx, &user.regions));
				match user.results.first() {
					Some(result) => demand.join(demand_of_rows(ir, facts, cx, *result, depth + 1)),
					None => return Demand::Whole,
				}
			}
			// A window passes its input's rows on with a field per call appended: a demand for one
			// of those fields is met by the window, not by its input.
			OpKind::Logical(LogOp::Window(shape)) => {
				let Some(result) = user.results.first() else {
					return Demand::Whole;
				};
				let passed = match demand_of_rows(ir, facts, cx, *result, depth + 1) {
					Demand::Fields(fields) => Demand::Fields(
						fields
							.into_iter()
							.filter(|name| {
								!shape.calls.iter().any(|call| call.output.as_str() == name)
							})
							.collect(),
					),
					Demand::Whole => Demand::Whole,
				};
				demand.join(passed);
				demand.join(demand_of_regions(ir, facts, cx, &user.regions));
			}
			OpKind::Logical(LogOp::Project(attrs)) if attrs.layer => {
				let Some(result) = user.results.first() else {
					return Demand::Whole;
				};
				let columns = layer_column_names(attrs);
				let passed = match demand_of_rows(ir, facts, cx, *result, depth + 1) {
					Demand::Fields(fields) => Demand::Fields(
						fields.into_iter().filter(|name| !columns.contains(name)).collect(),
					),
					Demand::Whole => Demand::Whole,
				};
				demand.join(passed);
				demand.join(demand_of_regions(ir, facts, cx, &user.regions));
			}
			OpKind::Logical(LogOp::Project(attrs)) => {
				if attrs.omit > 0 || !attrs.sort_keys.is_empty() || shape_has_all(&attrs.fields) {
					return Demand::Whole;
				}
				demand.join(demand_of_regions(ir, facts, cx, &user.regions));
			}
			OpKind::Logical(LogOp::Aggregate(shape)) => {
				if shape_has_all(&shape.fields) {
					return Demand::Whole;
				}
				demand.join(demand_of_regions(ir, facts, cx, &user.regions));
			}
			// The rows leave a `SELECT` loop as the rows its output holds.
			OpKind::Logical(LogOp::Yield) => {
				let Some(owner) = ir
					.block(user.parent)
					.and_then(|block| ir.parent_op(block.parent))
					.and_then(|owner| ir.op(owner))
				else {
					return Demand::Whole;
				};
				let (
					OpKind::Logical(LogOp::Loop {
						kind: LoopKind::Select,
						..
					}),
					Some(output),
				) = (&owner.kind, owner.results.first())
				else {
					return Demand::Whole;
				};
				demand.join(demand_of_rows(ir, facts, cx, *output, depth + 1));
			}
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return Demand::Whole,
		}
		if matches!(demand, Demand::Whole) {
			return demand;
		}
	}
	demand
}

/// Whether the projection shape holds a `*`.
fn shape_has_all(fields: &FieldsShape) -> bool {
	match fields {
		FieldsShape::Value {
			..
		} => false,
		FieldsShape::Select(shape) => shape.iter().any(|field| matches!(field, FieldShape::All)),
	}
}

/// The names the columns of a layer land under.
pub(super) fn layer_column_names(attrs: &ProjectAttrs) -> Vec<String> {
	let FieldsShape::Select(shape) = &attrs.fields else {
		return Vec::new();
	};
	shape
		.iter()
		.filter_map(|field| match field {
			FieldShape::Single {
				alias,
				..
			} => alias.as_ref().and_then(|path| path.as_field()).map(str::to_owned),
			FieldShape::All => None,
		})
		.collect()
}

/// The fields the regions read off their row argument.
fn demand_of_regions(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	regions: &[RegionId],
) -> Demand {
	let mut demand = Demand::none();
	for region in regions {
		let Some(held) = ir.region(*region) else {
			return Demand::Whole;
		};
		// Code the region holds that reads the row without naming a field needs the whole row.
		let mut opaque = false;
		ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
			if reads_row_opaquely(ir, facts, cx, op) {
				opaque = true;
				return Walk::Stop;
			}
			Walk::Continue
		});
		if opaque {
			return Demand::Whole;
		}
		let skipped = usize::from(held.kind.isolated_from_above());
		for block in held.blocks.iter().copied() {
			let Some(block) = ir.block(block) else {
				return Demand::Whole;
			};
			let Some(row) = block.args.get(skipped) else {
				continue;
			};
			demand.join(reads_of_row(ir, *row));
			if matches!(demand, Demand::Whole) {
				return demand;
			}
		}
	}
	demand
}

/// The fields the readers of the row `row` read: every use must be a read of a named field.
fn reads_of_row(ir: &Ir, row: ValueId) -> Demand {
	let mut fields = HashSet::new();
	for held in ir.uses(row) {
		let name = ir.op(held.user).and_then(|user| match &user.kind {
			OpKind::Expr(ExprOp::Field {
				name,
			}) if held.operand == 0 => Some(name.as_str().to_owned()),
			OpKind::Expr(ExprOp::Access {
				field,
			}) if held.operand == 0 => Some(field.as_str().to_owned()),
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => None,
		});
		match name {
			Some(name) => {
				fields.insert(name);
			}
			None => return Demand::Whole,
		}
	}
	Demand::Fields(fields)
}

/// Erases the columns of the layers that no op reads, and the layers left with none, and answers
/// how many ops went.
///
/// A layer is judged from the layers above it first, so a column only an erased column read goes
/// in the next sweep.
fn erase_dead_layer_columns(cx: &mut LoCx<'_>) -> Result<u32, IrError> {
	let mut erased = 0;
	for layer in live_ops(cx.ir).into_iter().rev() {
		if cx.ir.op(layer).is_some() {
			erased += prune_layer(cx, layer)?;
		}
	}
	Ok(erased)
}

/// Erases the columns of `layer` that nothing reads, and answers `1` when it changed the layer.
fn prune_layer(cx: &mut LoCx<'_>, layer: OpId) -> Result<u32, IrError> {
	let Some(data) = cx.ir.op(layer) else {
		return Ok(0);
	};
	let (OpKind::Logical(LogOp::Project(attrs)), [input], [result]) =
		(&data.kind, data.operands.as_slice(), data.results.as_slice())
	else {
		return Ok(0);
	};
	let FieldsShape::Select(shape) = &attrs.fields else {
		return Ok(0);
	};
	// A layer whose guards read derived fields is left whole: its regions end with the derivations
	// those guards read (`ProjectAttrs::judged`).
	if !attrs.layer || attrs.judged.is_some() {
		return Ok(0);
	}
	let (input, result) = (*input, *result);
	let names = layer_column_names(attrs);
	let singles = shape.iter().filter(|field| matches!(field, FieldShape::Single { .. })).count();
	if singles != names.len() || names.is_empty() {
		return Ok(0);
	}
	// The regions pair with the columns: one stored read each, then the `OMIT` paths, then one
	// field frame each.
	let omit = usize::from(attrs.omit);
	let kind_of = |region: RegionId| cx.ir.region(region).map(|held| held.kind);
	let reads: Vec<RegionId> = data
		.regions
		.iter()
		.copied()
		.filter(|region| kind_of(*region) == Some(RegionKind::Clause(ClauseRole::Projection)))
		.collect();
	let frames: Vec<RegionId> = data
		.regions
		.iter()
		.copied()
		.filter(|region| kind_of(*region) == Some(RegionKind::FieldFrame(FieldRole::Computed)))
		.collect();
	if reads.len() != names.len() + omit
		|| frames.len() != names.len()
		|| data.regions.len() != reads.len() + frames.len()
	{
		return Ok(0);
	}
	let Demand::Fields(needed) = demand_of_rows(cx.ir, cx.facts, cx.effects, result, 0) else {
		return Ok(0);
	};
	let mut kept: Vec<bool> = names.iter().map(|name| needed.contains(name)).collect();
	// A column another kept column of the layer reads stays: the frame of that column reads the
	// row with the layer's columns set.
	loop {
		let mut grew = false;
		for (at, frame) in frames.iter().enumerate() {
			if !kept[at] {
				continue;
			}
			match demand_of_regions(cx.ir, cx.facts, cx.effects, std::slice::from_ref(frame)) {
				Demand::Whole => return Ok(0),
				Demand::Fields(read) => {
					for (other, name) in names.iter().enumerate() {
						if read.contains(name) && !kept[other] {
							kept[other] = true;
							grew = true;
						}
					}
				}
			}
		}
		if !grew {
			break;
		}
	}
	if kept.iter().all(|held| *held) {
		return Ok(0);
	}
	let mut keep_columns = kept.iter();
	let new_shape: Vec<FieldShape> = shape
		.iter()
		.filter(|field| match field {
			FieldShape::All => true,
			FieldShape::Single {
				..
			} => keep_columns.next().copied().unwrap_or(true),
		})
		.cloned()
		.collect();
	let kept_reads: Vec<RegionId> = reads
		.iter()
		.take(names.len())
		.zip(&kept)
		.filter_map(|(region, held)| held.then_some(*region))
		.chain(reads.iter().skip(names.len()).copied())
		.collect();
	let kept_frames: Vec<RegionId> =
		frames.iter().zip(&kept).filter_map(|(region, held)| held.then_some(*region)).collect();
	let (frame, snapshot, loc) = (data.frame, data.snapshot, data.loc.clone());
	let new_attrs = ProjectAttrs {
		fields: FieldsShape::Select(new_shape.into_boxed_slice()),
		layer: true,
		omit: attrs.omit,
		names: attrs.names.clone(),
		computed: attrs.computed.clone(),
		sort_keys: attrs.sort_keys.clone(),
		judged: None,
	};
	let ty = cx.ir.value(result).map(|value| value.ty.clone()).ok_or(IrError::Stale("value"))?;
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	if kept.iter().all(|held| !*held) && omit == 0 {
		rw.replace_all_uses(result, input)?;
		rw.erase(layer, EliminationReason::DeadLayerColumn)?;
		return Ok(1);
	}
	let spec = OpSpec {
		kind: OpKind::Logical(LogOp::Project(Box::new(new_attrs))),
		operands: smallvec![input],
		results: smallvec![ty],
		frame,
		snapshot,
		loc,
	};
	let replacement = rw.derived(&[layer], |rw| rw.create(InsertPoint::Before(layer), spec))?;
	for region in kept_reads.into_iter().chain(kept_frames) {
		rw.move_region(region, replacement)?;
	}
	let moved = rw
		.ir()
		.op(replacement)
		.and_then(|data| data.results.first().copied())
		.ok_or(IrError::Stale("op"))?;
	rw.replace_all_uses(result, moved)?;
	rw.erase(layer, EliminationReason::DeadLayerColumn)?;
	Ok(1)
}

/// Erases the ops whose results are unread, whose effects are reads and whose failures are none,
/// and answers how many went.
fn erase_dead_pure(cx: &mut LoCx<'_>) -> Result<u32, IrError> {
	let mut erased = 0;
	let ops = live_ops(cx.ir);
	let explains = ops.iter().any(|op| {
		matches!(cx.ir.op(*op).map(|data| &data.kind), Some(OpKind::Logical(LogOp::Explain { .. })))
	});
	for op in ops.into_iter().rev() {
		if is_dead_pure(cx, op, explains) && erase(cx, op, EliminationReason::DeadPure)? {
			erased += 1;
		}
	}
	Ok(erased)
}

/// Whether `op` is pure and unread. `explains` says whether the container holds an `EXPLAIN`,
/// whose explained statements keep every op.
fn is_dead_pure(cx: &mut LoCx<'_>, op: OpId, explains: bool) -> bool {
	if is_pinned(cx.ir, op)
		|| !results_unused(cx.ir, op)
		|| (explains && in_explained_statement(cx.ir, op))
	{
		return false;
	}
	let Some(data) = cx.ir.op(op) else {
		return false;
	};
	if let OpKind::Expr(ExprOp::ComputedRead(attrs)) = &data.kind {
		return shadowed_by_earlier_read(cx.ir, op, attrs);
	}
	let answer = effects::of_with(&*cx.ir, &mut *cx.facts, cx.effects, op);
	let class = answer.fallibility.union(fallibility::floor(answer.set));
	guards::infallible(class) && READS.contains(answer.set)
}

/// Whether an earlier read of the same field of the same row, under the same frame and snapshot,
/// dominates the computed read `op`: the memo it filled answers `op`.
fn shadowed_by_earlier_read(ir: &Ir, op: OpId, attrs: &ComputedReadAttrs) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let Some(&row) = data.operands.first() else {
		return false;
	};
	ir.uses(row).iter().any(|held| {
		if held.user == op || held.operand != 0 {
			return false;
		}
		let Some(earlier) = ir.op(held.user) else {
			return false;
		};
		let OpKind::Expr(ExprOp::ComputedRead(other)) = &earlier.kind else {
			return false;
		};
		**other == *attrs
			&& earlier.frame == data.frame
			&& earlier.snapshot == data.snapshot
			&& earlier.results.first().is_some_and(|result| ir.dominates(*result, op))
	})
}

/// Erases the predecessors of each certain raise inside the region it cancels, and answers how
/// many went. A cancelling region is served by its first certain raise.
fn erase_before_certain_error(cx: &mut LoCx<'_>) -> Result<u32, IrError> {
	if !matches!(cx.ir.scope(), IrScope::Request { .. }) {
		return Ok(0);
	}
	let mut served: Vec<RegionId> = Vec::new();
	let mut erased = 0;
	for (raise, cancel) in certain_raises(cx.ir) {
		if served.contains(&cancel) {
			continue;
		}
		served.push(cancel);
		erased += erase_predecessors(cx, raise, cancel)?;
	}
	Ok(erased)
}

/// The certain raises of the container with the region each cancels, in program order.
fn certain_raises(ir: &Ir) -> Vec<(OpId, RegionId)> {
	let mut raises = Vec::new();
	for op in live_ops(ir) {
		let Some(data) = ir.op(op) else {
			continue;
		};
		let certain = matches!(data.kind, OpKind::Expr(ExprOp::Throw))
			|| matches!(
				&data.kind,
				OpKind::Expr(ExprOp::Raise(kind)) if !matches!(**kind, RaiseKind::Misplaced(_))
			);
		if !certain {
			continue;
		}
		if let Some(cancel) = cancel_region(ir, op)
			&& control_cannot_skip(ir, op, cancel)
		{
			raises.push((op, cancel));
		}
	}
	raises
}

/// The region the raise `raise` cancels: the enclosing auto-committed statement or explicit
/// transaction, reached through statement and block regions only. A raise inside any other region
/// may be caught or skipped, and a session transaction is never cancelled.
fn cancel_region(ir: &Ir, raise: OpId) -> Option<RegionId> {
	for (region, _) in ancestors(ir, raise) {
		let kind = ir.region(region)?.kind;
		if matches!(
			kind,
			RegionKind::Statement(StatementCommit::Auto)
				| RegionKind::Transaction(TxnKind::Explicit)
		) {
			return Some(region);
		}
		if !matches!(
			kind,
			RegionKind::Statement(StatementCommit::InTxn)
				| RegionKind::Block
				| RegionKind::ValueBlock
		) {
			return None;
		}
	}
	None
}

/// Whether no op before `raise` in `cancel` can end a region `raise` stands in, or leave the
/// container, by a `RETURN`, `BREAK` or `CONTINUE`.
fn control_cannot_skip(ir: &Ir, raise: OpId, cancel: RegionId) -> bool {
	let enclosing: Vec<RegionId> = ancestors(ir, raise).map(|(region, _)| region).collect();
	predecessors(ir, raise, cancel).into_iter().all(|op| match control_target(ir, op) {
		ControlTarget::NotControl => true,
		ControlTarget::Region(target) => !enclosing.contains(&target),
		ControlTarget::Escapes(_) | ControlTarget::Barrier(_) => false,
	})
}

/// The ops of `cancel` that run before `raise`, in program order, without the ops whose regions
/// hold `raise`.
fn predecessors(ir: &Ir, raise: OpId, cancel: RegionId) -> Vec<OpId> {
	let owners: FxHashSet<OpId> = ancestors(ir, raise).filter_map(|(_, owner)| owner).collect();
	let mut before = Vec::new();
	ir.walk(cancel, WalkOrder::PreOrder, &mut |op| {
		if op == raise {
			return Walk::Stop;
		}
		if !owners.contains(&op) && !in_stored_clause(ir, op) {
			before.push(op);
		}
		Walk::Continue
	});
	before
}

/// Erases the predecessors of `raise` that no run observes, and answers how many went.
fn erase_predecessors(cx: &mut LoCx<'_>, raise: OpId, cancel: RegionId) -> Result<u32, IrError> {
	let before = predecessors(cx.ir, raise, cancel);
	let mut session_read_later = false;
	let mut erased = 0;
	for op in before.into_iter().rev() {
		if is_removable_before_error(cx, op, session_read_later)
			&& erase(cx, op, EliminationReason::DeadBeforeCertainError)?
		{
			erased += 1;
			continue;
		}
		if cx.ir.op(op).is_some() {
			let local = effects::local(&*cx.ir, &mut *cx.facts, cx.effects, op);
			session_read_later |= local.set.intersects(SESSION_READS);
		}
	}
	Ok(erased)
}

/// Whether the predecessor `op` of a certain raise may go: it is infallible, nothing it leaves
/// survives a rollback, its effects are reads or all undone by the cancellation, and a session
/// write is not observed by an op that stays after it (`session_read_later`).
fn is_removable_before_error(cx: &mut LoCx<'_>, op: OpId, session_read_later: bool) -> bool {
	if is_pinned(cx.ir, op) || !results_unused(cx.ir, op) || in_explained_statement(cx.ir, op) {
		return false;
	}
	let answer = effects::of_with(&*cx.ir, &mut *cx.facts, cx.effects, op);
	let class = answer.fallibility.union(fallibility::floor(answer.set));
	if !guards::infallible(class) || answer.rollback == Rollback::Survives {
		return false;
	}
	READS.contains(answer.set) || (answer.rollback == Rollback::Undone && !session_read_later)
}
