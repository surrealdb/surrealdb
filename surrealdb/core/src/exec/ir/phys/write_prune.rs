//! The stages of a physical write that no record can observe, and the events no record fires.
//!
//! A statement `UPDATE` or `DELETE` that states its `WHERE` condition is a literal no record
//! satisfies ([`WriteFlags::where_never`](crate::exec::ir::iface::statements::WriteFlags::where_never)) runs no stage after its `WHERE` stage. [`run`] removes
//! those stages when the table's definitions say that lowering them could not have refused the
//! statement ([`lowers_after_cond`]); the decision reads the catalog the container was bound
//! against, which is why it is made here and not at bind.
//!
//! [`run`] also marks an event dormant when its `WHEN` clause is never truthy for the action of
//! the events unit it stands in ([`mark_dormant`]): the lowering then emits no clause for it and
//! a record runs only the depth check entering its `WHEN` would make. The events unit's stages
//! are never removed.
//!
//! Under `RETURN NONE` the record's output is `None` whether the select gate admits or skips it
//! ([`output_blind`]), so only the gate's evaluation can be observed: a raise or an effect. [`run`]
//! removes the select gate stage of such a write when its evaluation can be neither, and installs
//! the body that names the stages left ([`without_stages`]).
//!
//! The gate is removed only when all of the following hold:
//!
//! - The write states its select clause as `FULL`, `NONE`, absent, or a predicate. A clause the
//!   write cannot state (`Dynamic`, `LateBound`, `Unlowerable`) lowers to a distinct refusal that
//!   the compile must still reach.
//! - The gate states its regions as the clause says ([`gates_fault`]), and a predicate's `derives`
//!   flag agrees with the fields the gate derives.
//! - The gate derives no field. A predicate that reads a `COMPUTED` field reads it through a
//!   [`ExprOp::ComputedRead`], which the analyses answer with the top element, so no such predicate
//!   is inert; the derivation steps the lowering emits around a field's region (the coercion to the
//!   field's kind and the frame under its `AUTH LIMIT`) lie outside every region and are therefore
//!   never judged either.
//! - Every region the gate hosts is inert: the join of the effects of its ops is deterministic but
//!   for the clock, and the join of their failure classes is [`Fallibility::INFALLIBLE`]. An op the
//!   analyses know nothing about answers the top element, which keeps the gate. A read of a row
//!   field is declared as able to fail, so a predicate over the stored fields of the record keeps
//!   the gate too.
//! - Every op of every such region is one the expression lowering accepts whatever the catalog says
//!   ([`lowers_without_refusal`]), so erasing the regions hides no compile-time refusal.
//! - The steps the lowering emits around the regions cannot fail. Each is infallible on the images
//!   a gate judges, which are records:
//!   - The gate judges only a row that has an id, whose images are objects, and runs in a database
//!     context, because the tenant guard that opens every order requires one. A write whose run
//!     holds no tenant guard before the gate keeps it.
//!   - A permission frame is built once when the program starts, so entering it neither checks nor
//!     charges computation depth. A predicate whose frame narrows the caller's authority keeps the
//!     gate.
//!   - `JumpIfNoId`, `JumpIfUnchecked`, `Judge` and `Jump`, and the `Load`, `Emit` and `EndRow` of
//!     the predicate's pipeline, read the row's id, the database context and the session, clone the
//!     object image and move registers the lowering fills.
//!
//! The pass runs before the pipelines are built, so no pipeline holds a region it erases. A write
//! that names no one table states its select clause as `Dynamic` and keeps the gate, and the stages
//! of each table it reaches come from per-table bundles. Every write of a container whose top
//! region is a `WriteStage` keeps its stages: the bundle of a table's stages, a view table's rows,
//! a cascade and a table's events are dispatched to by a statement that states the stages of each
//! run it hands over.

#![deny(clippy::wildcard_enum_match_arm)]

use super::ops::PhysOp;
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ConvertMode, ExprOp, OnFail};
use crate::exec::ir::iface::catalog::PhysCx;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::fallibility::Fallibility;
use crate::exec::ir::iface::ids::{FrameId, OpId, RegionId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::logical::ops::{
	EventsAttrs, GateClause, ImageView, Stored, WriteAttrs, WriteBody, WriteStageKind,
};
use crate::exec::ir::logical::stages::{
	Derivation, SchemaRegion, TableGates, TableIndexes, TableSchema, after_cond, body_fault,
	gates_fault, gates_of, indexes_of, needs_caller, observed_of, output_blind, schema_of,
	when_never_fires, without_stages, write_gate_hosts, write_stage_ops,
};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::logical::EffectSet;
use crate::expr::{BinaryOperator, Function, PrefixOperator};

/// Removes the stages of every physical write of `ir` that no record can observe.
///
/// Runs on the converted container, before its pipelines are built.
pub(crate) fn run(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>) -> Result<(), IrError> {
	#[cfg(test)]
	{
		ENTERED.with(|entered| entered.set(entered.get() + 1));
		if DISABLED.with(std::cell::Cell::get) {
			return Ok(());
		}
	}
	// The write of a stage bundle, a view table's rows or a cascade stands in a container whose top
	// region is a `WriteStage`. The statement that dispatches to such a unit states the stages of
	// each run it hands over, so the unit keeps every stage it was bound with.
	let stage_unit = ir.region(ir.top()).is_none_or(|top| top.kind == RegionKind::WriteStage);
	let mut writes = Vec::new();
	let mut events = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		match ir.op(op).map(|data| &data.kind) {
			Some(OpKind::Phys(PhysOp::Write(_))) if !stage_unit => writes.push(op),
			Some(OpKind::Phys(PhysOp::Events(_))) => events.push(op),
			Some(OpKind::Logical(_) | OpKind::Expr(_) | OpKind::Phys(_)) | None => {}
		}
		Walk::Continue
	});
	for write in writes {
		prune(ir, facts, cx, write)?;
	}
	for op in events {
		mark_dormant(ir, facts, op)?;
	}
	Ok(())
}

/// Whether each event of `attrs` is dormant, in the order of its definitions, or `None` for an op
/// that states no clause for its events.
pub(super) fn dormancy(attrs: &EventsAttrs) -> Option<Vec<bool>> {
	if attrs.unlowerable || attrs.events.len() != attrs.defs.definitions().len() {
		return None;
	}
	Some(
		attrs
			.defs
			.iter()
			.map(|definition| {
				definition.when_logical().is_ok_and(|when| when_never_fires(when, attrs.action))
			})
			.collect(),
	)
}

/// Marks every event of the op `op` whose `WHEN` clause is never truthy for the op's action
/// ([`when_never_fires`]) as dormant.
///
/// The clause is read from the definition's logical form, the one the clause's region was bound
/// from. A definition with no logical form is never dormant, and neither is any event of an op
/// that states no clause. The regions and the definitions stay whole: dormancy only says that no
/// record fires the event.
fn mark_dormant(ir: &mut Ir, facts: &mut FactCache, op: OpId) -> Result<(), IrError> {
	let Some(OpKind::Phys(PhysOp::Events(attrs))) = ir.op(op).map(|data| &data.kind) else {
		return Ok(());
	};
	let Some(dormant) = dormancy(attrs) else {
		return Ok(());
	};
	if attrs.events.iter().map(|event| event.dormant).eq(dormant.iter().copied()) {
		return Ok(());
	}
	let mut rw = Rewriter::new(ir, facts);
	rw.update_kind(op, |kind| {
		if let OpKind::Phys(PhysOp::Events(attrs)) = kind {
			for (shape, dormant) in attrs.events.iter_mut().zip(dormant) {
				shape.dormant = dormant;
			}
		}
	})
}

/// Removes the output-blind stage ops of the write `write` that are inert, and remaps its body.
fn prune(ir: &mut Ir, facts: &mut FactCache, cx: &PhysCx<'_>, write: OpId) -> Result<(), IrError> {
	let Some(data) = ir.op(write) else {
		return Ok(());
	};
	let OpKind::Phys(PhysOp::Write(attrs)) = &data.kind else {
		return Ok(());
	};
	let Some(stage_ops) = write_stage_ops(ir, data) else {
		return Ok(());
	};
	let (output, gates, body) = (attrs.write.output, attrs.write.gates, attrs.write.body.clone());
	let derives = !attrs.write.derived.select.is_empty();
	// A write whose gate hosts cannot be read keeps its stages.
	let Some(hosts) = write_gate_hosts(ir, data) else {
		return Ok(());
	};
	if body_fault(&body, stage_ops.len()).is_some() || gates_fault(gates, &hosts).is_some() {
		return Ok(());
	}
	let kinds: Vec<WriteStageKind> = stage_ops.iter().map(|(_, stage)| *stage).collect();
	let cut = attrs.write.flags.where_never && cut_is_sound(cx, &attrs.write);
	let mut dropped: Vec<(OpId, u16)> = Vec::new();
	if cut {
		for position in after_cond(&body, &kinds)? {
			let (op, _) = stage_ops
				.get(usize::from(position))
				.ok_or(IrError::Stale("write_prune: a stage position beyond the stage ops"))?;
			dropped.push((*op, position));
		}
	} else {
		for (at, (op, stage)) in stage_ops.iter().enumerate() {
			if !output_blind(output, *stage) {
				continue;
			}
			// The tenant guard opens every order and requires a database context, which the
			// gate's own instructions need as well.
			let guarded = kinds[..at].contains(&WriteStageKind::CheckRecordUserAccess);
			if !guarded {
				continue;
			}
			if gate_is_inert(ir, facts, cx, *op, gates.select, derives) {
				let position = u16::try_from(at)
					.map_err(|_| IrError::Stale("write_prune: a stage position beyond u16"))?;
				dropped.push((*op, position));
			}
		}
	}
	if dropped.is_empty() {
		return Ok(());
	}
	#[cfg(test)]
	REMOVED.with(|removed| removed.set(removed.get() + dropped.len()));
	let positions: Vec<u16> = dropped.iter().map(|(_, position)| *position).collect();
	let body: WriteBody = without_stages(&body, stage_ops.len(), &positions)?;
	let mut rw = Rewriter::new(ir, facts);
	rw.update_kind(write, |kind| {
		if let OpKind::Phys(PhysOp::Write(attrs)) = kind {
			attrs.write.body = body;
		}
	})?;
	let reason = match cut {
		true => EliminationReason::Unreachable,
		false => EliminationReason::DeadPure,
	};
	for (op, _) in dropped {
		rw.erase(op, reason)?;
	}
	Ok(())
}

/// Whether the stages after the `WHERE` stage of the write whose attributes are `attrs` are
/// removed without hiding what lowering them would have raised: the definitions of the table the
/// write names, read from the catalog the container was bound against, pass
/// [`lowers_after_cond`].
///
/// A write that names no one table states no definitions, and keeps its stages.
fn cut_is_sound(cx: &PhysCx<'_>, attrs: &WriteAttrs) -> bool {
	let table = attrs.stored.table.as_ref();
	if table.is_none() {
		return false;
	}
	let catalog = cx.effects.catalog;
	let schema = schema_of(catalog, cx.effects.registry, table);
	let indexes = indexes_of(catalog, table);
	let gates = gates_of(catalog, table);
	let observed = observed_of(catalog, table);
	lowers_after_cond(attrs, &schema, &indexes, &gates, observed.as_deref())
}

/// Whether a statement write of the table its attributes `attrs` state may drop the stages after
/// its `WHERE` stage without hiding a refusal that lowering those stages would raise when the
/// statement compiles, or a catalog change it would depend on through a unit those stages request.
///
/// The stages after the `WHERE` stage plan the units of the table's events, views and cascades
/// and lower its field schema, index columns and permission clauses. A refusal in one of them
/// fails the statement even when no record reaches it. So a write is cut only when it names one
/// table whose stored definitions are all read, and when what the cut stages would lower is
/// refusal-free by construction:
///
/// - the table stores no event, no view and no `REFERENCE` field, and no `REFERENCE` field of
///   another table names it, since each of those is a unit planned when the statement compiles;
/// - every clause the stages lower has a logical form and runs as the caller, holding no statement,
///   no closure and no call of a user-defined function, which are planned for the principal that
///   runs them;
/// - no permission clause or field view is unread, unnamed or unlowerable.
fn lowers_after_cond(
	attrs: &WriteAttrs,
	schema: &TableSchema,
	indexes: &TableIndexes,
	gates: &TableGates,
	observed: Option<&[Derivation]>,
) -> bool {
	let read = |stored: Stored| matches!(stored, Stored::Nothing | Stored::Declared);
	let none = |stored: Stored| stored == Stored::Nothing;
	let stored = &attrs.stored;
	let states = stored.table.is_some()
		&& read(stored.fields)
		&& read(stored.indexes)
		&& none(stored.events)
		&& none(stored.views)
		&& none(stored.references)
		&& none(stored.referenced)
		&& !stored.schema.unlowerable
		&& !stored.schema.id_default_bound
		&& !stored.index_set.unlowerable;
	let clause = |gate: GateClause| {
		matches!(
			gate,
			GateClause::Full | GateClause::None | GateClause::Absent | GateClause::Predicate { .. }
		)
	};
	let gated = clause(attrs.gates.create)
		&& clause(attrs.gates.update)
		&& clause(attrs.gates.delete)
		&& clause(attrs.gates.select);
	let shown = matches!(attrs.image.view, ImageView::Plain | ImageView::Cut | ImageView::Derived);
	let plain = |region: &SchemaRegion| region.frame.auth.is_none() && !needs_caller(&region.body);
	let derived = |list: &[Derivation]| {
		list.iter().all(|derivation| derivation.auth.is_none() && !needs_caller(&derivation.body))
	};
	let predicates = [&gates.create, &gates.update, &gates.delete, &gates.select];
	states
		&& gated
		&& shown
		&& schema.regions.iter().chain(schema.id_default.iter()).all(plain)
		&& indexes.regions.iter().all(plain)
		&& predicates.iter().all(|predicate| predicate.as_ref().is_none_or(|p| !needs_caller(p)))
		&& derived(&gates.derivations.create)
		&& derived(&gates.derivations.update)
		&& derived(&gates.derivations.delete)
		&& derived(&gates.derivations.select)
		&& observed.is_none_or(derived)
}

/// Whether evaluating the select gate stage op `stage` can neither raise nor have an effect, and
/// erasing it hides no refusal of the compile.
///
/// `derives` is whether the write states derived fields for the gate. A gate that derives keeps
/// its op (see the module contract).
fn gate_is_inert(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &PhysCx<'_>,
	stage: OpId,
	clause: GateClause,
	derives: bool,
) -> bool {
	let Some(data) = ir.op(stage) else {
		return false;
	};
	let states = match judged(clause) {
		Judged::Unconditional => {
			// A clause that is not a predicate hosts only the stage's own region.
			return data.regions.len() == 1;
		}
		Judged::Predicate {
			derives,
		} => derives,
		Judged::Kept => return false,
	};
	// The stage's own region, then the predicate and nothing else: a derivation region, or a
	// `derives` flag that disagrees with the fields the write states, keeps the gate.
	if states || derives || data.regions.len() != 2 {
		return false;
	}
	let predicate = data.regions[1];
	let predicate_kind = ir.region(predicate).map(|region| region.kind);
	predicate_kind == Some(RegionKind::PermissionFrame)
		&& !narrows(ir, predicate)
		&& inert(ir, facts, cx, predicate)
}

/// What a select clause lets the pass judge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Judged {
	/// The clause hosts no region of its own.
	Unconditional,
	/// The clause hosts a predicate region, and a derivation region per derived field when
	/// `derives`.
	Predicate {
		derives: bool,
	},
	/// The write cannot state the clause. Each of these lowers to a distinct refusal that the
	/// compile must still reach.
	Kept,
}

/// How the pass reads the clause the write states as its select clause.
pub(super) fn judged(clause: GateClause) -> Judged {
	match clause {
		GateClause::Full | GateClause::None | GateClause::Absent => Judged::Unconditional,
		GateClause::Predicate {
			derives,
		} => Judged::Predicate {
			derives,
		},
		GateClause::Dynamic | GateClause::LateBound | GateClause::Unlowerable => Judged::Kept,
	}
}

/// Whether the frame the ops of `region` run under limits the caller's authority.
///
/// A region of no op, or of a frame the container does not hold, answers `true`, which keeps the
/// gate.
fn narrows(ir: &Ir, region: RegionId) -> bool {
	match region_frame(ir, region).and_then(|frame| ir.frame(frame)) {
		Some(frame) => frame.eval.auth.is_some(),
		None => true,
	}
}

/// The frame of the last op of `region`, which is the frame its program evaluates the region in.
fn region_frame(ir: &Ir, region: RegionId) -> Option<FrameId> {
	let last = ops_in(ir, region).last()?;
	Some(ir.op(last)?.frame)
}

/// Whether every op of `region` and of the regions below it has no effect but a read of the clock
/// and cannot fail, and the lowering accepts it under the frame of the region.
fn inert(ir: &Ir, facts: &mut FactCache, cx: &PhysCx<'_>, region: RegionId) -> bool {
	let Some(frame) = region_frame(ir, region) else {
		return false;
	};
	let mut fails = Fallibility::INFALLIBLE;
	let mut effect = EffectSet::PURE;
	for op in ops_in(ir, region).collect::<Vec<_>>() {
		let answer = effects::of_with(ir, facts, &cx.effects, op);
		effect = effect.union(answer.set);
		fails = fails.union(fallibility::of_with(ir, facts, &cx.effects, op));
	}
	effect.is_deterministic_but_for_clock()
		&& fails == Fallibility::INFALLIBLE
		&& lowers_without_refusal(ir, cx.effects.registry, region, frame)
}

/// Whether the expression lowering emits every op of `region`, and of the regions below it,
/// whatever the catalog holds, so that erasing the region hides no refusal of the compile
/// (`NotYet`, `LateBind` or `Invariant`).
///
/// An op is accepted when its kind lowers by its own operands alone and refuses nothing but an
/// op that runs under a frame other than the region's, which `frame` rules out. Every kind not
/// listed here (a call of a stored function, a statement or a closure in value position, an
/// index function, a `MATCHES` or `KNN` operator, a method) consults the catalog or the
/// registry, or has a refusal of its own, and keeps the gate.
fn lowers_without_refusal(
	ir: &Ir,
	registry: &FunctionRegistry,
	region: RegionId,
	frame: FrameId,
) -> bool {
	let mut accepted = true;
	ir.walk(region, WalkOrder::PreOrder, &mut |op| match ir
		.op(op)
		.filter(|data| data.frame == frame)
		.map(|data| &data.kind)
	{
		Some(kind) if lowers(registry, kind) => Walk::Continue,
		Some(_) | None => {
			accepted = false;
			Walk::Stop
		}
	});
	accepted
}

/// Whether the op `kind` is one the expression lowering emits without a refusal of its own.
fn lowers(registry: &FunctionRegistry, kind: &OpKind) -> bool {
	match kind {
		OpKind::Expr(op) => expr_lowers(registry, op),
		OpKind::Phys(PhysOp::Yield) => true,
		OpKind::Phys(_) | OpKind::Logical(_) => false,
	}
}

/// [`lowers`] for an expression op: the kinds that lower from their operands alone.
pub(super) fn expr_lowers(registry: &FunctionRegistry, op: &ExprOp) -> bool {
	match op {
		ExprOp::Literal(_)
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
		| ExprOp::Field {
			..
		}
		| ExprOp::ComputedRead(_)
		| ExprOp::Doc
		| ExprOp::Access {
			..
		}
		| ExprOp::ShortCircuit {
			..
		}
		| ExprOp::Truthy
		| ExprOp::Yield => true,
		ExprOp::Binary {
			op,
		} => comparison(op),
		ExprOp::Prefix(operator) => matches!(
			&**operator,
			PrefixOperator::Not | PrefixOperator::Negate | PrefixOperator::Positive
		),
		ExprOp::Convert(attrs) => {
			matches!((attrs.mode, attrs.on_fail), (ConvertMode::Cast, OnFail::Error))
		}
		ExprOp::Call(call) => match &call.function {
			Function::Normal(name) => !registry.is_index_function(name),
			Function::Custom(_)
			| Function::Script(_)
			| Function::Model(_)
			| Function::Module(..)
			| Function::Silo {
				..
			} => false,
		},
		ExprOp::Array
		| ExprOp::Set
		| ExprOp::Object {
			..
		}
		| ExprOp::RecordId(_)
		| ExprOp::Table {
			..
		}
		| ExprOp::Mock(_)
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
		| ExprOp::ClosureCreate(_)
		| ExprOp::If
		| ExprOp::Throw
		| ExprOp::Subquery(_)
		| ExprOp::Agg {
			..
		}
		| ExprOp::Now {
			..
		}
		| ExprOp::Raise(_)
		| ExprOp::DepthCharge {
			..
		} => false,
	}
}

/// Whether `op` is a comparison the lowering emits as one compare instruction.
fn comparison(op: &BinaryOperator) -> bool {
	matches!(
		op,
		BinaryOperator::Equal
			| BinaryOperator::ExactEqual
			| BinaryOperator::NotEqual
			| BinaryOperator::LessThan
			| BinaryOperator::LessThanEqual
			| BinaryOperator::MoreThan
			| BinaryOperator::MoreThanEqual
	)
}

#[cfg(test)]
thread_local! {
	static DISABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
	static ENTERED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
	static REMOVED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many times [`run`] was entered and how many stage ops it removed on this thread since
/// the last call, which resets both.
#[cfg(test)]
pub(super) fn take_tally() -> (usize, usize) {
	(ENTERED.with(|entered| entered.replace(0)), REMOVED.with(|removed| removed.replace(0)))
}

/// While it lives, [`run`] changes nothing on the thread that holds it, so a test can plan the
/// same statement with and without the pass.
#[cfg(test)]
pub(super) struct Disabled;

#[cfg(test)]
impl Disabled {
	pub(super) fn new() -> Self {
		DISABLED.with(|disabled| disabled.set(true));
		Self
	}
}

#[cfg(test)]
impl Drop for Disabled {
	fn drop(&mut self) {
		DISABLED.with(|disabled| disabled.set(false));
	}
}
