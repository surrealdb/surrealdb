//! Running the body of a user-defined function in the caller.
//!
//! The binder holds the body of a function a call may run in place in a `FunctionBody` region of
//! the call. [`InlineCalls`] moves that body to the call site, with the call's arguments for its
//! parameters, when the site qualifies, and drops the region otherwise, so no later pass sees a
//! body beneath a call.
//!
//! A body may run in the caller when it is a function of its arguments alone, wherever it is
//! evaluated, and small enough that the inlined ops are the smaller program. [`inline_admits`]
//! answers that over the body's region; it rewrites nothing. What a site must satisfy beyond it,
//! so that the moved body evaluates to what the call would have:
//!
//! - The call supplies every argument, or every argument it leaves out is one whose kind admits
//!   `NONE`; a missing argument is bound to `NONE`, as the call leaves it unbound.
//! - Every argument is pure and cannot raise. A call evaluates and coerces every argument before
//!   its body; keeping that order for an argument that may raise would take proving that the body
//!   evaluates it on every path, so such an argument keeps the call and the error the call would
//!   report is reported.
//! - The kind known of every argument is one its declared kind coerces by identity, and the kind
//!   known of the result one the declared return kind coerces by identity: the call coerces each
//!   argument and the result, and a coercion that could convert or fail is not elided. A kind that
//!   is only declared, as a stored field's is, is not known.
//! - An argument that reads the row stands only where the body evaluates it against the row of the
//!   call: directly in the body, or in an arm of a conditional or the right operand of a
//!   short-circuit operator. In a region that reads another value as its row, a field read means
//!   that value's field.
//!
//! The call also keeps its site when it is not under the current catalog state or runs in code the
//! catalog stores.
#![deny(clippy::wildcard_enum_match_arm)]

use std::sync::Arc;

use smallvec::{SmallVec, smallvec};

use super::driver::{LoCx, LoPass, PassConfig, PassReport, PassRun};
use super::perm_where::{computing_ops, def_op};
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::census::{Census, Flag};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::location::{LocOrigin, Location};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::catalog::{CatalogAnswer, EffectCx};
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::verify::timestamp_arg;
use crate::exec::ir::op_kind::OpKind;
use crate::expr::logical::{EffectSet, coercion_is_identity};
use crate::expr::{Function, Kind};
use crate::val::Value;

/// Runs the body of each call that holds one in the caller where the site qualifies, and drops
/// the body of every other call.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct InlineCalls;

impl LoPass for InlineCalls {
	fn name(&self) -> &'static str {
		"inline_calls"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.inline_calls
	}

	/// Candidates are collected by [`run_inline`]'s walk over every op of the container, stored
	/// code included, through [`call_with_body`]: a call op that owns a `FunctionBody` region.
	/// Without one, `sites` is empty and the run answers the default report without touching the
	/// container or the facts. [`Flag::FunctionBodyCall`] holds exactly when such a call stands
	/// anywhere in the container.
	fn needs(&self, census: &Census) -> bool {
		census.flag(Flag::FunctionBodyCall)
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_inline(cx))
	}
}

/// The call `op` and the `FunctionBody` region it holds, when it holds one.
fn call_with_body(ir: &Ir, op: OpId) -> Option<RegionId> {
	let data = ir.op(op)?;
	let OpKind::Expr(ExprOp::Call(_)) = &data.kind else {
		return None;
	};
	data.regions
		.iter()
		.copied()
		.find(|region| ir.region(*region).is_some_and(|held| held.kind == RegionKind::FunctionBody))
}

/// What an admitted call is replaced by: the value each parameter takes, in order, `None` for a
/// parameter the call leaves out, which is `NONE`.
struct Site {
	args: SmallVec<[Option<ValueId>; 4]>,
}

fn run_inline(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	let mut sites: Vec<(OpId, RegionId, bool)> = Vec::new();
	cx.ir.walk(cx.ir.top(), WalkOrder::PreOrder, &mut |op| {
		if let Some(region) = call_with_body(cx.ir, op) {
			sites.push((op, region, super::perm_where::in_stored_clause(cx.ir, op)));
		}
		Walk::Continue
	});
	for (op, region, stored) in sites {
		if cx.ir.op(op).is_none() {
			continue;
		}
		let site = if stored {
			None
		} else {
			admitted(cx, op, region)
		};
		match site {
			Some(site) => inline_at(cx, op, region, &site)?,
			None => {
				Rewriter::new(&mut *cx.ir, &mut *cx.facts)
					.erase_region(region, EliminationReason::DeadPure)?;
			}
		}
		report.rewrites = report.rewrites.saturating_add(1);
	}
	Ok(report)
}

/// The kind known of `value`: the kind a guarantee states, and `any` for anything less.
fn known_kind(ir: &Ir, value: ValueId) -> Kind {
	match ir.value(value).map(|data| &data.ty) {
		Some(TypeKnowledge::Fact {
			kind,
			..
		}) => kind.clone(),
		Some(
			TypeKnowledge::Hint {
				..
			}
			| TypeKnowledge::Token(_)
			| TypeKnowledge::Unknown,
		)
		| None => Kind::Any,
	}
}

/// The site of the call `op`, whose body is `region`, when it qualifies; see the module
/// documentation.
fn admitted(cx: &mut LoCx<'_>, op: OpId, region: RegionId) -> Option<Site> {
	let data = cx.ir.op(op)?;
	let OpKind::Expr(ExprOp::Call(attrs)) = &data.kind else {
		return None;
	};
	let Function::Custom(name) = &attrs.function else {
		return None;
	};
	if !matches!(cx.ir.snapshot(data.snapshot), Some(Snapshot::Current)) {
		return None;
	}
	let given: SmallVec<[ValueId; 4]> = data
		.operands
		.iter()
		.copied()
		.take(data.operands.len().checked_sub(usize::from(attrs.row_names))?)
		.collect();
	let CatalogAnswer::Found(definition) = cx.effects.catalog.function(name) else {
		return None;
	};
	if given.len() > definition.args.len() {
		return None;
	}
	let formals = formals_of(cx.ir, region)?;
	if formals.len() != definition.args.len() {
		return None;
	}
	if !inline_admits(cx.ir, cx.facts, cx.effects, region) {
		return None;
	}
	let mut args: SmallVec<[Option<ValueId>; 4]> = SmallVec::new();
	for (at, (_, declared)) in definition.args.iter().enumerate() {
		let arg = given.get(at).copied();
		match arg {
			Some(arg) => {
				if !coercion_is_identity(declared, &known_kind(cx.ir, arg))
					|| !argument_is_inert(cx, arg)
				{
					return None;
				}
			}
			None => {
				// Whether a named type admits `NONE` is answered by the catalog when the call
				// runs, so an omitted argument of one keeps the call.
				if declared.has_named_type() || !declared.can_be_none() {
					return None;
				}
			}
		}
		args.push(arg);
	}
	if let Some(returns) = &definition.returns {
		let yielded = yielded_of(cx.ir, region)?;
		if !coercion_is_identity(returns, &known_kind(cx.ir, yielded)) {
			return None;
		}
	}
	for (formal, arg) in formals.iter().zip(&args) {
		if let Some(arg) = arg
			&& reads_row(cx, *arg)
			&& !used_at_the_call_row(cx.ir, *formal, region)
		{
			return None;
		}
	}
	Some(Site {
		args,
	})
}

/// The parameters of the body `region`: the arguments of its block after the statement timestamp.
fn formals_of(ir: &Ir, region: RegionId) -> Option<SmallVec<[ValueId; 4]>> {
	let [block] = ir.region(region)?.blocks.as_slice() else {
		return None;
	};
	Some(ir.block(*block)?.args.iter().copied().skip(1).collect())
}

/// The value the body `region` yields.
fn yielded_of(ir: &Ir, region: RegionId) -> Option<ValueId> {
	let [block] = ir.region(region)?.blocks.as_slice() else {
		return None;
	};
	let last = ir.op(*ir.block(*block)?.ops.last()?)?;
	last.operands.first().copied()
}

/// Whether evaluating `arg` is pure and cannot raise. A plain read of a field off a row the
/// enclosing region receives counts as both: the row holds the value of each of its fields.
fn argument_is_inert(cx: &mut LoCx<'_>, arg: ValueId) -> bool {
	computing_ops(cx.ir, arg).into_iter().all(|op| {
		let plain_read = cx.ir.op(op).is_some_and(|data| {
			matches!(data.kind, OpKind::Expr(ExprOp::Field { .. } | ExprOp::Access { .. }))
				&& matches!(
					data.operands.as_slice(),
					[row] if def_op(cx.ir, *row).is_none()
				)
		});
		if plain_read {
			return true;
		}
		let answer = effects::of_with(&*cx.ir, &mut *cx.facts, cx.effects, op);
		let class = answer.fallibility.union(fallibility::floor(answer.set));
		guards::infallible(class) && guards::pure(answer.set)
	})
}

/// Whether `arg` reads the row it is evaluated against.
fn reads_row(cx: &mut LoCx<'_>, arg: ValueId) -> bool {
	computing_ops(cx.ir, arg).into_iter().any(|op| {
		effects::of_with(&*cx.ir, &mut *cx.facts, cx.effects, op)
			.set
			.intersects(EffectSet::READS_ROW)
	})
}

/// Whether every read of `formal` in the body `body` is where the body evaluates against the row
/// of the call: no region between the read and the body is one that reads another value as its
/// row.
fn used_at_the_call_row(ir: &Ir, formal: ValueId, body: RegionId) -> bool {
	ir.uses(formal).iter().all(|used| {
		ancestors(ir, used.user).take_while(|(region, _)| *region != body).all(|(_, owner)| {
			owner.and_then(|owner| ir.op(owner)).is_some_and(|owner| {
				matches!(owner.kind, OpKind::Expr(ExprOp::If | ExprOp::ShortCircuit { .. }))
			})
		})
	})
}

/// The statement timestamp visible at `op`: the first argument of the nearest enclosing block
/// that receives one.
fn timestamp_at(ir: &Ir, op: OpId) -> Option<ValueId> {
	ancestors(ir, op).find_map(|(region, _)| {
		let block = ir.block(*ir.region(region)?.blocks.first()?)?;
		let first = *block.args.first()?;
		(ir.value(first)?.ty == timestamp_arg()).then_some(first)
	})
}

/// Replaces the call `op` by its body `region`, bound to the arguments of `site`.
fn inline_at(cx: &mut LoCx<'_>, op: OpId, region: RegionId, site: &Site) -> Result<(), IrError> {
	let Some(timestamp) = timestamp_at(cx.ir, op) else {
		Rewriter::new(&mut *cx.ir, &mut *cx.facts)
			.erase_region(region, EliminationReason::DeadPure)?;
		return Ok(());
	};
	let data = cx.ir.op(op).ok_or(IrError::Stale("op"))?;
	let (frame, snapshot, loc, home) = (data.frame, data.snapshot, data.loc.clone(), data.parent);
	let call_text = loc.text().map(|text| Arc::new(text.to_owned()));
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	rw.derived(&[op], |rw| {
		let mut args: SmallVec<[ValueId; 5]> = smallvec![timestamp];
		for arg in &site.args {
			match arg {
				Some(arg) => args.push(*arg),
				None => {
					let kind = ExprOp::Literal(Box::new(Value::None));
					let ty = result_type(&kind, &[]);
					let spec = OpSpec {
						kind: OpKind::Expr(kind),
						operands: SmallVec::new(),
						results: smallvec![ty],
						frame,
						snapshot,
						loc: loc.clone(),
					};
					let literal = rw.create(InsertPoint::Before(op), spec)?;
					let none = rw
						.ir()
						.op(literal)
						.and_then(|made| made.results.first().copied())
						.ok_or(IrError::Stale("created op"))?;
					args.push(none);
				}
			}
		}
		let yielded = rw.inline_region(region, InsertPoint::Before(op), &args)?;
		// The op that computes the body's value stands for the call: where it states no text of
		// its own, the text the call stated names it.
		if let (Some(text), Some(value)) = (call_text, yielded.first())
			&& let Some(def) = def_op(rw.ir(), *value)
			&& let Some(held) = rw.ir().op(def)
			&& held.parent == home
			&& held.loc.text().is_none()
		{
			let loc = Location {
				origin: LocOrigin::Written(text),
				..held.loc.clone()
			};
			rw.set_location(def, loc)?;
		}
		if yielded.iter().all(|value| same_error_scope(rw.ir(), op, *value)) {
			return rw.replace_op(op, &yielded);
		}
		let results: SmallVec<[ValueId; 2]> =
			rw.ir().op(op).ok_or(IrError::Stale("op"))?.results.iter().copied().collect();
		for (result, value) in results.iter().zip(&yielded) {
			rw.replace_all_uses(*result, *value)?;
		}
		let reason = yielded
			.iter()
			.find_map(|value| def_op(rw.ir(), *value))
			.map_or(EliminationReason::DeadPure, EliminationReason::MergedInto);
		rw.erase(op, reason)
	})
}

/// Whether `value` is defined in the same error scope as `op`, or is not defined by an op.
fn same_error_scope(ir: &Ir, op: OpId, value: ValueId) -> bool {
	let Some(def) = def_op(ir, value) else {
		return true;
	};
	let (Some(op_block), Some(def_block)) =
		(ir.op(op).map(|data| data.parent), ir.op(def).map(|data| data.parent))
	else {
		return false;
	};
	ir.error_scope_owner(op_block) == ir.error_scope_owner(def_block)
}

/// The most ops a body inlined may have: past it, a call is the smaller program.
pub(crate) const MAX_NODES: usize = 64;

/// Whether a call of the function whose body region is `body` may run the body inlined in the
/// caller.
///
/// The body is one block of expression ops closed by a yield of one value, the function's value. It
/// has at most [`MAX_NODES`] ops, and its transitive effects are [`EffectSet::PURE`]. It reads no
/// parameter but its arguments, which are block arguments, and no row and no record. Every call in
/// it names a builtin that is pure and synchronous and is neither an index function nor a
/// projection. Any other region, or a body with a different shape, answers `false`. A body that
/// refers to row names or binds statement ops is refused too.
pub(crate) fn inline_admits(
	ir: &Ir,
	facts: &mut FactCache,
	cx: &EffectCx<'_>,
	body: RegionId,
) -> bool {
	let Some(region) = ir.region(body) else {
		return false;
	};
	if region.kind != RegionKind::FunctionBody
		|| !closes_with_one_value(ir, region.blocks.as_slice())
	{
		return false;
	}
	let mut nodes = 0;
	let mut admitted = true;
	ir.walk(body, WalkOrder::PreOrder, &mut |op| {
		nodes += 1;
		let Some(data) = ir.op(op) else {
			admitted = false;
			return Walk::Stop;
		};
		if nodes > MAX_NODES || !admits(&data.kind, cx) {
			admitted = false;
			return Walk::Stop;
		}
		Walk::Continue
	});
	admitted
		&& ops_in(ir, body)
			.all(|op| effects::summary_with(ir, facts, cx, op).set == EffectSet::PURE)
}

/// Whether `blocks` is one block whose last op yields one value.
fn closes_with_one_value(ir: &Ir, blocks: &[BlockId]) -> bool {
	let [block] = blocks else {
		return false;
	};
	let Some(last) = ir.block(*block).and_then(|held| held.ops.last()).and_then(|op| ir.op(*op))
	else {
		return false;
	};
	matches!(last.kind, OpKind::Expr(ExprOp::Yield)) && last.operands.len() == 1
}

/// Whether `kind`, on its own, is a function of its operands alone.
///
/// A parameter, a row read, a record read, a statement, a closure and a lookup are refused; a call
/// is admitted when it names a pure, synchronous builtin.
fn admits(kind: &OpKind, cx: &EffectCx<'_>) -> bool {
	match kind {
		OpKind::Expr(ExprOp::Call(function)) => match &function.function {
			Function::Normal(name) => cx.registry.get(name).is_some_and(|builtin| {
				builtin.is_pure()
					&& !builtin.is_async()
					&& !cx.registry.is_index_function(name)
					&& !cx.registry.is_projection(name)
			}),
			Function::Custom(_)
			| Function::Script(..)
			| Function::Model(..)
			| Function::Module(..)
			| Function::Silo {
				..
			} => false,
		},
		OpKind::Expr(op) => admits_tag(op.tag()),
		OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// Whether the expression op `tag` is a value of its operands alone.
fn admits_tag(tag: ExprTag) -> bool {
	match tag {
		ExprTag::Literal
		| ExprTag::Array
		| ExprTag::Set
		| ExprTag::Object
		| ExprTag::RecordId
		| ExprTag::Table
		| ExprTag::Constant
		| ExprTag::First
		| ExprTag::Last
		| ExprTag::Flatten
		| ExprTag::Index
		| ExprTag::Prefix
		| ExprTag::Postfix
		| ExprTag::Binary
		| ExprTag::ShortCircuit
		| ExprTag::If
		| ExprTag::Truthy
		| ExprTag::Convert
		| ExprTag::Yield => true,
		ExprTag::FreeParam
		| ExprTag::SessionParam
		| ExprTag::CatalogParam
		| ExprTag::Mock
		| ExprTag::Field
		| ExprTag::ComputedRead
		| ExprTag::Doc
		| ExprTag::Access
		| ExprTag::Deref
		| ExprTag::Each
		| ExprTag::Dispatch
		| ExprTag::Method
		| ExprTag::Destructure
		| ExprTag::Optional
		| ExprTag::RepeatRecurse
		| ExprTag::SetField
		| ExprTag::Invoke
		| ExprTag::Call
		| ExprTag::ClosureCreate
		| ExprTag::Throw
		| ExprTag::Subquery
		| ExprTag::Agg
		| ExprTag::Now
		| ExprTag::Raise
		| ExprTag::DepthCharge
		| ExprTag::TupleRow => false,
	}
}
