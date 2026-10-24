//! Loop unrolling: a loop over a source of a known small size is replaced by its iterations.
//!
//! A loop is unrolled only within the bounds on iterations and created ops; past either the loop
//! stays a loop.
//!
//! [`LoopUnroll`] replaces a loop over a literal array or set, or over a value the folder answered
//! with a constant, by one `logical.block` per element. The block holds a copy of the loop's body
//! in which the element and its position are literals, so every `LET` stays in the scope of its
//! iteration and every op keeps its location. A clock read stays the op that reads it, and an
//! iteration that raises still raises when it runs, because no body is evaluated here.
#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::{SmallVec, smallvec};
use surrealdb_strand::Strand;

use super::driver::{FoldObserver, LoCx, LoPass, PassConfig, PassReport, PassRun};
use crate::exec::ir::analysis::loop_legality::{exit_of, of_with};
use crate::exec::ir::analysis::multiplicity::loop_body;
use crate::exec::ir::core::census::Census;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::fxhash::FxHashMap;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::{ControlTarget, ancestors, control_target};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::expr::types::result_type;
use crate::exec::ir::iface::bounds::{UNROLL_MAX_ITERATIONS, UNROLL_MAX_OPS};
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::fold::{FoldConsumer, FoldOutcome};
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::r#loop::Exit;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{
	LetAttrs, LetBinding, LogOp, LogTag, LoopKind, LoopSink, StepAttrs,
};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::val::{Array, Value};

/// The deepest region nesting a copy follows. Nesting is finite by construction, so a well-formed
/// container never meets the bound; a body that does is left as a loop.
const MAX_DEPTH: usize = 128;

/// Replaces a loop over a known, small source by its iterations.
///
/// A loop qualifies when all of these hold:
///
/// - it is a `FOR`, closure or element-filter loop with one source, one result and one body region,
///   whose sink is `Output` or `Discard`;
/// - its source is an `expr.literal` array or set, or a value whose folded outcome is a constant
///   array or set. A set is taken only by a `Discard` sink: the result shape of a collecting sink
///   over a set is not stated;
/// - no iteration leaves the loop early (the loop has no exit), and no `BREAK` or `CONTINUE` inside
///   the body lands on the body itself, because the body stops being a loop body;
/// - the loop has no barrier, so every iteration's effects keep their order unchanged;
/// - the source holds at most [`UNROLL_MAX_ITERATIONS`] elements, and the elements times the ops of
///   the body come to at most [`UNROLL_MAX_OPS`];
/// - the body is one block that closes with a `logical.yield` of one value (`Output`) or none
///   (`Discard`), and every region inside it is one block that is not stored code.
///
/// Each element becomes a `logical.block` placed before the loop, in element order. The block
/// holds a literal for the element and one for its position, each only when the body reads it,
/// then a copy of the body in which the loop's block arguments read those literals. The block is
/// a statement block when the loop's sink discards and the loop stands in a statement list, and a
/// value block otherwise. The loop's value becomes an `expr.array` of the blocks' values for
/// `Output` (an empty array literal when the source is empty) and `NONE` for `Discard`.
///
/// A fold that raises is not applied, so a source that raises stays a loop that raises when it
/// runs. When the elements come from a folded outcome, the observer is told of the source before
/// the loop is replaced. The loops of one run are visited innermost first from a list taken before
/// the first rewrite, so a loop's body already holds the iterations of the loops inside it when
/// its bounds are judged, and the loops a rewrite creates are not visited.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct LoopUnroll;

impl LoPass for LoopUnroll {
	fn name(&self) -> &'static str {
		"loop_unroll"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.unroll
	}

	/// [`run_unroll`] visits the loops [`live_loops`] returns: the `logical.loop` ops outside
	/// stored code. With none it reads no fact, rewrites nothing and reports the default.
	fn needs(&self, census: &Census) -> bool {
		census.has(LogTag::Loop)
	}

	fn run<'a, 'b>(&'a self, cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		PassRun::done(run_unroll(cx))
	}
}

// ---------------------------------------------------------------------------------------------
// Container walks
// ---------------------------------------------------------------------------------------------

/// Whether `op` stands inside a stored clause, whose code is bound and analysed but never
/// rewritten.
fn in_stored_clause(ir: &Ir, op: OpId) -> bool {
	ancestors(ir, op).any(|(region, _)| {
		ir.region(region).is_some_and(|held| matches!(held.kind, RegionKind::StoredClause(_)))
	})
}

/// Every loop outside stored clauses, in program order: a loop before the loops of its body.
fn live_loops(ir: &Ir) -> Vec<OpId> {
	let mut loops = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		let is_loop = ir.op(op).is_some_and(|data| data.kind.tag() == OpTag::Logical(LogTag::Loop));
		if is_loop && !in_stored_clause(ir, op) {
			loops.push(op);
		}
		Walk::Continue
	});
	loops
}

/// The ops inside `region`, at any depth.
fn ops_inside(ir: &Ir, region: RegionId) -> usize {
	let mut count = 0;
	ir.walk(region, WalkOrder::PreOrder, &mut |_| {
		count += 1;
		Walk::Continue
	});
	count
}

/// Whether a `BREAK` or `CONTINUE` inside `body` lands on `body` itself.
fn lands_on_body(ir: &Ir, body: RegionId) -> bool {
	let mut lands = false;
	ir.walk(body, WalkOrder::PreOrder, &mut |op| {
		let is_exit = ir.op(op).is_some_and(|data| {
			matches!(data.kind.tag(), OpTag::Logical(LogTag::Break | LogTag::Next))
		});
		if is_exit && control_target(ir, op) == ControlTarget::Region(body) {
			lands = true;
			return Walk::Stop;
		}
		Walk::Continue
	});
	lands
}

/// Whether `body`, a `StatementFor` region, holds a statement whose result is discarded because it
/// stands in `body` (or in `StatementIf` arms rooted in `body`) rather than being consumed by a
/// `LET` or `RETURN`. Unrolling `body` into a `Block` or `ValueBlock` would change its collection
/// mode from `Collect::Discard` to `Collect::Block`.
fn discards_statement_in_body(ir: &Ir, body: RegionId) -> bool {
	if ir.region(body).is_none_or(|info| info.kind != RegionKind::StatementFor) {
		return false;
	}
	let mut discards = false;
	ir.walk(body, WalkOrder::PreOrder, &mut |op| {
		let Some(data) = ir.op(op) else {
			return Walk::Continue;
		};
		let is_statement = matches!(
			&data.kind,
			OpKind::Logical(
				LogOp::Statement { .. }
					| LogOp::Info { .. }
					| LogOp::Stmt { .. }
					| LogOp::Loop {
						kind: LoopKind::Select | LoopKind::Write,
						..
					}
			)
		);
		if !is_statement {
			return Walk::Continue;
		}
		let consumed = data.results.first().is_some_and(|result| match ir.uses(*result) {
			[only] => ir.op(only.user).is_some_and(|user| {
				matches!(user.kind, OpKind::Logical(LogOp::Let(_) | LogOp::Return))
			}),
			_ => false,
		});
		if consumed {
			return Walk::Continue;
		}
		let mut region = ir.enclosing_region(op);
		while region != body {
			let Some(info) = ir.region(region) else {
				return Walk::Continue;
			};
			if info.kind != RegionKind::StatementIf {
				return Walk::Continue;
			}
			let Some(owner) = ir.parent_op(region) else {
				return Walk::Continue;
			};
			region = ir.enclosing_region(owner);
		}
		discards = true;
		Walk::Stop
	});
	discards
}

/// Whether `region` and every region inside it is one block and is not stored code, which is what
/// lets the ops of `region` be recreated one by one.
fn copyable(ir: &Ir, region: RegionId, depth: usize) -> bool {
	if depth > MAX_DEPTH {
		return false;
	}
	let Some(info) = ir.region(region) else {
		return false;
	};
	if matches!(info.kind, RegionKind::StoredClause(_)) {
		return false;
	}
	let [block] = info.blocks.as_slice() else {
		return false;
	};
	let Some(block) = ir.block(*block) else {
		return false;
	};
	block.ops.iter().all(|op| {
		ir.op(*op)
			.is_some_and(|data| data.regions.iter().all(|inner| copyable(ir, *inner, depth + 1)))
	})
}

// ---------------------------------------------------------------------------------------------
// Candidates
// ---------------------------------------------------------------------------------------------

/// The one block of a loop's body and what a copy of it needs.
struct BodyShape {
	/// The element and its position, the block arguments the copy replaces by literals.
	args: [ValueId; 2],
	/// The ops of the block in order, the closing yield last.
	ops: Vec<OpId>,
}

/// A loop and the elements it is replaced by.
struct Plan {
	loop_op: OpId,
	/// The value the loop iterates.
	source: ValueId,
	/// Whether the elements are a folded outcome and not a literal.
	folded: bool,
	elements: Vec<Value>,
	/// Whether the sink is `Discard`; the sink is `Output` otherwise.
	discard: bool,
	/// The parameter name the loop binds its element to, when it is a `FOR` with a named
	/// parameter.
	param: Option<Strand>,
	/// The kind of the region of each iteration's block.
	arms: RegionKind,
	body: BodyShape,
}

/// The literal value `value` holds, when it is the result of an `expr.literal`.
fn literal_of(ir: &Ir, value: ValueId) -> Option<&Value> {
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(value)?.def
	else {
		return None;
	};
	let OpKind::Expr(ExprOp::Literal(held)) = &ir.op(op)?.kind else {
		return None;
	};
	Some(&**held)
}

/// The value the loop's source stands for and whether it is a folded outcome: the literal that
/// defines it, else a constant fold outcome.
fn source_value<'a>(
	ir: &'a Ir,
	facts: &'a FactCache,
	source: ValueId,
) -> Option<(&'a Value, bool)> {
	if let Some(value) = literal_of(ir, source) {
		return Some((value, false));
	}
	if let Some(FoldOutcome::Constant {
		value,
		..
	}) = facts.fold_outcome(source)
	{
		return Some((value, true));
	}
	None
}

/// The elements of `value` in iteration order, when it is an array, or a set under a discarding
/// sink, of at most [`UNROLL_MAX_ITERATIONS`] elements.
fn elements_of(value: &Value, discard: bool) -> Option<Vec<Value>> {
	let within = |len: usize| u32::try_from(len).is_ok_and(|len| len <= UNROLL_MAX_ITERATIONS);
	if let Value::Array(array) = value {
		return within(array.0.len()).then(|| array.0.clone());
	}
	if let Value::Set(set) = value
		&& discard
	{
		return within(set.0.len()).then(|| set.0.iter().cloned().collect());
	}
	None
}

/// The shape of the body of the loop whose body region is `body`, or `None` when it is not one
/// block that closes with a yield of the arity its sink states.
fn body_shape(ir: &Ir, body: RegionId, discard: bool) -> Option<BodyShape> {
	let [block] = ir.region(body)?.blocks.as_slice() else {
		return None;
	};
	let block = ir.block(*block)?;
	let [element, position] = block.args.as_slice() else {
		return None;
	};
	let closing = ir.op(*block.ops.last()?)?;
	let yields = matches!(closing.kind, OpKind::Logical(LogOp::Yield));
	let wanted = usize::from(!discard);
	(yields && closing.operands.len() == wanted).then(|| BodyShape {
		args: [*element, *position],
		ops: block.ops.clone(),
	})
}

/// The unroll `loop_op` admits, or `None`.
///
/// Everything that can refuse the rewrite is decided here, before the container is touched.
fn plan_for(ir: &Ir, facts: &mut FactCache, cx: &EffectCx<'_>, loop_op: OpId) -> Option<Plan> {
	let data = ir.op(loop_op)?;
	let OpKind::Logical(LogOp::Loop {
		kind,
		sink,
		step,
		..
	}) = &data.kind
	else {
		return None;
	};
	if !matches!(kind, LoopKind::For | LoopKind::Closure | LoopKind::ElementFilter)
		|| !matches!(sink, LoopSink::Output | LoopSink::Discard)
	{
		return None;
	}
	let param = match (kind, step.as_deref()) {
		(_, None) => None,
		(LoopKind::For, Some(StepAttrs::Param(name))) => Some(name.clone()),
		_ => return None,
	};
	let discard = matches!(sink, LoopSink::Discard);
	let ([source], [_], [region]) =
		(data.operands.as_slice(), data.results.as_slice(), data.regions.as_slice())
	else {
		return None;
	};
	let (source, region) = (*source, *region);
	if loop_body(ir, loop_op) != Some(region) {
		return None;
	}
	let body = body_shape(ir, region, discard)?;
	if exit_of(ir, loop_op) != Exit::None
		|| lands_on_body(ir, region)
		|| discards_statement_in_body(ir, region)
		|| !copyable(ir, region, 0)
	{
		return None;
	}
	let (elements, folded) = {
		let (value, folded) = source_value(ir, facts, source)?;
		(elements_of(value, discard)?, folded)
	};
	let trips = u32::try_from(elements.len()).ok()?;
	let body_ops = u32::try_from(ops_inside(ir, region)).ok()?;
	if trips.checked_mul(body_ops)? > UNROLL_MAX_OPS {
		return None;
	}
	if of_with(ir, facts, cx, loop_op).ok()?.barrier.is_some() {
		return None;
	}
	let arms = if discard && ir.holds_statement_list(ir.enclosing_region(loop_op)) {
		RegionKind::Block
	} else {
		RegionKind::ValueBlock
	};
	Some(Plan {
		loop_op,
		source,
		folded,
		elements,
		discard,
		param,
		arms,
		body,
	})
}

// ---------------------------------------------------------------------------------------------
// The rewrite
// ---------------------------------------------------------------------------------------------

/// The description of an op of `kind` that stands where `template` stands.
fn spec_at(
	template: &OpData,
	kind: OpKind,
	operands: SmallVec<[ValueId; 4]>,
	results: SmallVec<[TypeKnowledge; 2]>,
) -> OpSpec {
	OpSpec {
		kind,
		operands,
		results,
		frame: template.frame,
		snapshot: template.snapshot,
		loc: template.loc.clone(),
	}
}

/// The first result of `op`.
fn first_result(rw: &Rewriter<'_>, op: OpId) -> Result<ValueId, IrError> {
	rw.ir().op(op).and_then(|data| data.results.first().copied()).ok_or(IrError::Stale("value"))
}

/// Creates the literal `value` at `at` on behalf of the loop `scope` and answers its value.
fn make_literal(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	template: &OpData,
	scope: OpId,
	value: Value,
) -> Result<ValueId, IrError> {
	let literal = ExprOp::Literal(Box::new(value));
	let ty = result_type(&literal, &[]);
	let spec = spec_at(template, OpKind::Expr(literal), SmallVec::new(), smallvec![ty]);
	let op = rw.derived(&[scope], |rw| rw.create(at, spec))?;
	first_result(rw, op)
}

/// Recreates `ops`, in order, at the end of `into`, and each region of each op with them.
///
/// An operand that `renamed` holds reads its replacement; any other operand is read as it is. Each
/// result and block argument of the copy is added to `renamed`. An op keeps its frame, snapshot
/// and location, and stands for the op it copies.
fn copy_ops(
	rw: &mut Rewriter<'_>,
	ops: &[OpId],
	into: BlockId,
	renamed: &mut FxHashMap<ValueId, ValueId>,
	depth: usize,
) -> Result<(), IrError> {
	if depth > MAX_DEPTH {
		return Err(IrError::Capacity("unroll depth"));
	}
	for old in ops {
		let data = rw.ir().op(*old).ok_or(IrError::Stale("op"))?.clone();
		let operands: SmallVec<[ValueId; 4]> =
			data.operands.iter().map(|read| renamed.get(read).copied().unwrap_or(*read)).collect();
		let mut results: SmallVec<[TypeKnowledge; 2]> = SmallVec::new();
		for result in &data.results {
			results.push(rw.ir().value(*result).ok_or(IrError::Stale("value"))?.ty.clone());
		}
		let spec = spec_at(&data, data.kind.clone(), operands, results);
		let new = rw.derived(&[*old], |rw| rw.create(InsertPoint::End(into), spec))?;
		let made = rw.ir().op(new).ok_or(IrError::Stale("op"))?.results.clone();
		for (before, after) in data.results.iter().zip(&made) {
			renamed.insert(*before, *after);
		}
		for region in &data.regions {
			copy_region(rw, *region, new, renamed, depth)?;
		}
	}
	Ok(())
}

/// Recreates the one block of `region` as a region of `owner`.
fn copy_region(
	rw: &mut Rewriter<'_>,
	region: RegionId,
	owner: OpId,
	renamed: &mut FxHashMap<ValueId, ValueId>,
	depth: usize,
) -> Result<(), IrError> {
	let info = rw.ir().region(region).ok_or(IrError::Stale("region"))?;
	let kind = info.kind;
	let [block] = info.blocks.as_slice() else {
		return Err(IrError::Stale("unroll region"));
	};
	let held = rw.ir().block(*block).ok_or(IrError::Stale("block"))?;
	let (ops, args) = (held.ops.clone(), held.args.clone());
	let mut types: SmallVec<[TypeKnowledge; 2]> = SmallVec::new();
	for arg in &args {
		types.push(rw.ir().value(*arg).ok_or(IrError::Stale("value"))?.ty.clone());
	}
	let (_, copy) = rw.create_region(owner, kind, &types)?;
	let copied = rw.ir().block(copy).ok_or(IrError::Stale("block"))?.args.clone();
	for (before, after) in args.iter().zip(&copied) {
		renamed.insert(*before, *after);
	}
	copy_ops(rw, &ops, copy, renamed, depth + 1)
}

/// Creates the block of the iteration over `element` at position `index`, before the loop, and
/// answers the value of the block.
fn iteration(
	rw: &mut Rewriter<'_>,
	plan: &Plan,
	template: &OpData,
	index: usize,
	element: &Value,
) -> Result<ValueId, IrError> {
	let loop_op = plan.loop_op;
	let spec = spec_at(
		template,
		OpKind::Logical(LogOp::Block),
		SmallVec::new(),
		smallvec![TypeKnowledge::Unknown],
	);
	let block_op = rw.derived(&[loop_op], |rw| rw.create(InsertPoint::Before(loop_op), spec))?;
	let (_, inner) = rw.create_region(block_op, plan.arms, &[])?;
	let at = InsertPoint::End(inner);
	let position = i64::try_from(index).map_err(|_| IrError::Capacity("unroll position"))?;
	let mut renamed: FxHashMap<ValueId, ValueId> = FxHashMap::default();
	let [element_arg, position_arg] = plan.body.args;
	if let Some(name) = &plan.param {
		let made = make_literal(rw, at, template, loop_op, element.clone())?;
		let ty = rw.ir().value(made).ok_or(IrError::Stale("value"))?.ty.clone();
		let let_spec = spec_at(
			template,
			OpKind::Logical(LogOp::Let(Box::new(LetAttrs {
				name: name.clone(),
				binding: LetBinding::Local,
				ty: None,
			}))),
			smallvec![made],
			smallvec![ty],
		);
		let let_op = rw.derived(&[loop_op], |rw| rw.create(at, let_spec))?;
		renamed.insert(element_arg, first_result(rw, let_op)?);
	} else if !rw.ir().uses(element_arg).is_empty() {
		let made = make_literal(rw, at, template, loop_op, element.clone())?;
		renamed.insert(element_arg, made);
	}
	if !rw.ir().uses(position_arg).is_empty() {
		let made = make_literal(rw, at, template, loop_op, Value::from(position))?;
		renamed.insert(position_arg, made);
	}
	copy_ops(rw, &plan.body.ops, inner, &mut renamed, 0)?;
	first_result(rw, block_op)
}

/// Replaces the loop of `plan` by the blocks of its iterations.
fn apply(
	rw: &mut Rewriter<'_>,
	observer: &mut dyn FoldObserver,
	plan: &Plan,
) -> Result<(), IrError> {
	let loop_op = plan.loop_op;
	let template = rw.ir().op(loop_op).ok_or(IrError::Stale("op"))?.clone();
	let mut values: SmallVec<[ValueId; 8]> = SmallVec::new();
	for (index, element) in plan.elements.iter().enumerate() {
		values.push(iteration(rw, plan, &template, index, element)?);
	}
	let here = InsertPoint::Before(loop_op);
	let answer = if plan.discard {
		make_literal(rw, here, &template, loop_op, Value::None)?
	} else if values.is_empty() {
		make_literal(rw, here, &template, loop_op, Value::Array(Array(Vec::new())))?
	} else {
		let ty = result_type(&ExprOp::Array, &[]);
		let spec = spec_at(
			&template,
			OpKind::Expr(ExprOp::Array),
			values.iter().copied().collect(),
			smallvec![ty],
		);
		let made = rw.derived(&[loop_op], |rw| rw.create(here, spec))?;
		first_result(rw, made)?
	};
	if plan.folded {
		observer.consumed(rw.ir(), plan.source, FoldConsumer::Unroll);
	}
	rw.replace_op(loop_op, &[answer])
}

fn run_unroll(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	let mut loops = live_loops(cx.ir);
	loops.reverse();
	for loop_op in loops {
		if cx.ir.op(loop_op).is_none() {
			continue;
		}
		let Some(plan) = plan_for(&*cx.ir, &mut *cx.facts, cx.effects, loop_op) else {
			continue;
		};
		let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
		apply(&mut rw, &mut *cx.observer, &plan)?;
		report.rewrites = report.rewrites.saturating_add(1);
	}
	Ok(report)
}
