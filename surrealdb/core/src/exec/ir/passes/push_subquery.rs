//! Moving a `WHERE` filter into the relational subquery its rows come from.
//!
//! A statement that selects from `(SELECT * FROM t WHERE a)` and filters the rows by `b` judges
//! `b` for the rows the subquery returns. Judging `b` directly beneath the subquery's projection
//! admits the same rows, and merges with the subquery's own filter into the one conjunction an
//! access path is chosen from: the subquery's conjuncts first, then the moved ones.
//!
//! [`PushIntoSubquery`] applies to a filter that
//!
//! - reads the one loop row of a `SELECT` loop whose source is a relational subquery, and nothing
//!   else reads that row;
//! - stands over a subquery that is one `SELECT` loop whose rows leave through a projection that is
//!   not a layer, with no literal source beneath it, whose rows a projection reshapes.
//!
//! Conjuncts are judged one by one and keep their written order; those that move land in a new
//! filter directly beneath the projection, and the rest stay in the filter over the subquery.
//!
//! - Over a projection that returns its rows unchanged, `*` alone with no `OMIT`, a filter moves
//!   whole or not at all: it stays when any conjunct names a row-scoped parameter (`$this`,
//!   `$self`, `$parent`) or reads the row other than by a field's name, since the name denotes the
//!   row of the statement it is written in. No conjunct is judged by whether it can raise.
//! - Over any other projection, a conjunct moves when it reads the row by field names alone, runs
//!   no code its expression does not hold, and every name it reads is either passed through (`*` is
//!   projected, and the name is neither projected nor omitted) or projected as an expression that
//!   is free to duplicate: a path, a literal, a parameter, a constant, or a comparison or logical
//!   operator over those. Such a name is replaced by a copy of its expression over the row beneath
//!   the projection. A name that is omitted, projected twice, coerced to a kind, evaluated under
//!   another frame, or projected as any other expression stays above, as does a name the projection
//!   does not supply. No conjunct moves through a projection any of whose fields can write or
//!   raise, whatever the conjunct reads: a filter beneath the projection is applied first, so a
//!   write or an error would reach fewer rows than the projection as written.
//!
//! A subquery whose rows pass through a `SPLIT`, `FETCH`, `GROUP BY` or any other op after the
//! projection takes no conjunct: the rows the filter judges are not the rows beneath the
//! projection.
//!
//! The conjuncts land in the subquery's own filter when the projection reads one, and in a new
//! filter directly beneath the projection otherwise. Each merge is a run of the pass: a filter that
//! lands over a further subquery moves on in the next one, so a chain of subqueries ends with the
//! predicate over the innermost source. Moved ops are re-created under `derived`. Code inside a
//! stored clause is never rewritten, and a conjunct that writes state stays where it is.
#![deny(clippy::wildcard_enum_match_arm)]

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::future::{Future, ready};
use std::pin::Pin;

use smallvec::SmallVec;

use super::driver::{LoCx, LoPass, PassConfig, PassReport};
use super::perm_where::{
	WhereFilter, can_move, chain_spine, computing_ops, def_op, in_stored_clause, live_ops,
	localize_row_reads, move_selected, reads_row_opaquely, reads_through_operands, where_filter,
	yielded_value,
};
use super::rehome::{and_chain, can_rehome, copy_tree_bound, rebuild_and_chain_bound};
use crate::exec::ir::analysis::{effects, fallibility};
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::core::walk::{ancestors, ops_in};
use crate::exec::ir::egraph::guards;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, LogOp, LoopKind};
use crate::exec::ir::op_kind::OpKind;
use crate::expr::logical::{EvalMode, ITERATION_PARAMS, OutputPathPart};
use crate::expr::{BinaryOperator, PrefixOperator};

/// Moves the conjuncts of the `WHERE` filter over a relational subquery that the subquery's
/// projection admits into the subquery.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct PushIntoSubquery;

impl LoPass for PushIntoSubquery {
	fn name(&self) -> &'static str {
		"push_into_subquery"
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		cfg.push_into_subquery
	}

	fn run<'a, 'b>(
		&'a self,
		cx: &'a mut LoCx<'b>,
	) -> Pin<Box<dyn Future<Output = Result<PassReport, IrError>> + Send + 'a>> {
		Box::pin(ready(run_push(cx)))
	}
}

/// The expression a projected name stands for, free to duplicate: the op that yields it in the
/// field's region and the row argument of that region.
#[derive(Clone, Copy)]
struct Source {
	root: OpId,
	row: ValueId,
}

/// What a projection says about each name a conjunct over its rows may read.
#[derive(Default)]
struct Projection {
	/// Names a conjunct may replace by their expression: each projected under exactly that name,
	/// once, as an expression free to duplicate that is not coerced and runs under the caller's
	/// frame.
	substitutable: HashMap<String, Source>,
	/// Names whose value above the projection differs from the value beneath in a way no
	/// substitution expresses.
	opaque: HashSet<String>,
	/// Whether `*` is projected, so a name that is neither substitutable nor opaque is the same
	/// field on both sides.
	passes_through: bool,
}

/// How the projection of the subquery treats its rows.
enum Shape {
	/// `*` alone, with no `OMIT`.
	Identity,
	/// Any other projection that is not a layer and has no field that can write or raise.
	Named(Projection),
}

/// Where the moved conjuncts land: directly beneath the projection of the subquery.
struct Target {
	/// The projection.
	project: OpId,
	/// The value the projection reads, which a new filter reads too.
	beneath: ValueId,
	/// The subquery's own filter, when the projection reads one.
	inner: Option<WhereFilter>,
	shape: Shape,
}

/// How many times the push is repeated while it keeps moving: each run moves a filter into one
/// more subquery of a chain.
const PUSH_ROUNDS: u8 = 32;

/// Runs the push over every filter, the innermost first, until a run moves nothing.
fn run_push(cx: &mut LoCx<'_>) -> Result<PassReport, IrError> {
	let mut report = PassReport::default();
	for _ in 0..PUSH_ROUNDS {
		let mut candidates = live_ops(cx.ir);
		candidates.sort_by_key(|op| Reverse(ancestors(cx.ir, *op).count()));
		let mut moved = false;
		for op in candidates {
			if cx.ir.op(op).is_some() && push_into(cx, op, &mut report)? {
				report.rewrites = report.rewrites.saturating_add(1);
				moved = true;
			}
		}
		if !moved {
			break;
		}
	}
	Ok(report)
}

/// What a conjunct reads of the row it is judged over.
struct Reads {
	/// The fields it reads off the row by name.
	names: Vec<String>,
	/// Whether it names a row-scoped parameter or reads the row other than by a field's name.
	row_scoped: bool,
	/// Whether it runs code its expression does not hold, or reads the plan around it.
	opaque: bool,
}

/// Moves the conjuncts of the filter `op` that its subquery admits, and answers whether any moved.
fn push_into(cx: &mut LoCx<'_>, op: OpId, report: &mut PassReport) -> Result<bool, IrError> {
	let Some(filter) = where_filter(cx.ir, op) else {
		return Ok(false);
	};
	let Some(target) = subquery_target(cx, &filter) else {
		return Ok(false);
	};
	localize_row_reads(cx, &filter, report)?;
	let chain = and_chain(cx.ir, filter.predicate);
	let moved = admitted(cx, &filter, &chain, &target);
	if moved.is_empty() {
		return Ok(false);
	}
	let lower = move_selected(
		cx,
		&filter,
		&chain,
		&moved,
		target.project,
		target.beneath,
		&mut |rw, row| match &target.shape {
			Shape::Identity => Ok(()),
			Shape::Named(projection) => substitute(rw, row, projection),
		},
	)?;
	if let Some(inner) = &target.inner {
		merge_if_landable(cx, lower, inner)?;
	}
	Ok(true)
}

/// The positions of the conjuncts of `chain` that move, in written order.
fn admitted(
	cx: &mut LoCx<'_>,
	filter: &WhereFilter,
	chain: &[ValueId],
	target: &Target,
) -> Vec<usize> {
	let reads: Vec<Reads> = chain.iter().map(|value| reads_of(cx, filter, *value)).collect();
	let mut moved = Vec::new();
	for (at, value) in chain.iter().enumerate() {
		let read = &reads[at];
		let fits = match &target.shape {
			// A name of the row is the same name on both sides of a projection that returns its
			// rows unchanged, but `$this` denotes the row of the statement it is written in, so one
			// conjunct that names it keeps the whole filter above.
			Shape::Identity => {
				if reads.iter().any(|held| held.row_scoped) {
					return Vec::new();
				}
				true
			}
			Shape::Named(projection) => {
				!read.row_scoped
					&& !read.opaque && read
					.names
					.iter()
					.all(|name| projection.admits(cx.ir, name, target))
			}
		};
		if fits
			&& writes_no_state(cx, *value)
			&& can_move(cx.ir, filter, *value, target.project, target.beneath)
		{
			moved.push(at);
		}
	}
	moved
}

impl Projection {
	/// Whether a read of the field `name` off the projected row is answered beneath the
	/// projection: by the same field, or by the copy of the expression it is projected as.
	fn admits(&self, ir: &Ir, name: &str, target: &Target) -> bool {
		if self.opaque.contains(name) {
			return false;
		}
		match self.substitutable.get(name) {
			Some(source) => can_rehome(
				ir,
				source.root,
				InsertPoint::Before(target.project),
				&[(source.row, target.beneath)],
			),
			None => self.passes_through,
		}
	}
}

/// What the conjunct `value` of `filter` reads of the row, and what it runs.
fn reads_of(cx: &mut LoCx<'_>, filter: &WhereFilter, value: ValueId) -> Reads {
	let ir: &Ir = &*cx.ir;
	let computing = computing_ops(ir, value);
	let mut out = Reads {
		names: Vec::new(),
		row_scoped: false,
		opaque: false,
	};
	if computing.is_empty() {
		out.row_scoped = true;
		out.opaque = true;
		return out;
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
	for op in &tree {
		if let Some(OpKind::Expr(ExprOp::FreeParam {
			name,
		})) = ir.op(*op).map(|data| &data.kind)
			&& ITERATION_PARAMS.contains(&name.as_str())
		{
			out.row_scoped = true;
		}
	}
	for held in ir.uses(filter.row) {
		if !tree.contains(&held.user) {
			continue;
		}
		// A read inside a body the conjunct holds, a loop or a closure, names that body's own
		// value; the operands of `AND` and `OR` and the arms of an `IF` read the row.
		let reaches = held.operand == 0
			&& (computing.contains(&held.user)
				|| reads_through_operands(ir, held.user, &computing));
		match ir.op(held.user).map(|data| &data.kind) {
			Some(OpKind::Expr(ExprOp::Field {
				name,
			})) if reaches => out.names.push(name.as_str().to_owned()),
			_ => out.row_scoped = true,
		}
	}
	out.opaque = tree.iter().any(|op| reads_row_opaquely(ir, &mut *cx.facts, cx.effects, *op));
	out
}

/// Whether no op that computes the conjunct `value` changes state that outlives the statement.
fn writes_no_state(cx: &mut LoCx<'_>, value: ValueId) -> bool {
	let computing = computing_ops(cx.ir, value);
	!computing.is_empty()
		&& computing.iter().all(|op| {
			!effects::of_with(&*cx.ir, &mut *cx.facts, cx.effects, *op).set.mutates_state()
		})
}

/// Replaces each read of a substitutable name off `row` by a copy of the expression the name is
/// projected as, over `row`.
fn substitute(rw: &mut Rewriter<'_>, row: ValueId, projection: &Projection) -> Result<(), IrError> {
	let mut reads: Vec<(OpId, Source)> = Vec::new();
	for held in rw.ir().uses(row) {
		if held.operand != 0 {
			continue;
		}
		if let Some(OpKind::Expr(ExprOp::Field {
			name,
		})) = rw.ir().op(held.user).map(|data| &data.kind)
			&& let Some(source) = projection.substitutable.get(name.as_str())
		{
			reads.push((held.user, *source));
		}
	}
	for (read, source) in reads {
		let result = rw
			.ir()
			.op(read)
			.and_then(|data| data.results.first().copied())
			.ok_or(IrError::Stale("op"))?;
		let copied =
			copy_tree_bound(rw, source.root, InsertPoint::Before(read), &[(source.row, row)])?;
		rw.replace_all_uses(result, copied)?;
		rw.erase(read, EliminationReason::DeadPure)?;
	}
	Ok(())
}

/// Merges the filter `lower`, created over the subquery's own filter `inner`, into `inner` when
/// every conjunct can be re-created there; otherwise the two filters stand one over the other.
fn merge_if_landable(cx: &mut LoCx<'_>, lower: OpId, inner: &WhereFilter) -> Result<(), IrError> {
	let Some(moved) = where_filter(cx.ir, lower) else {
		return Ok(());
	};
	let chain = and_chain(cx.ir, moved.predicate);
	if moved.input == inner.result
		&& chain.iter().all(|value| can_land_in(cx.ir, &moved, *value, inner))
	{
		merge_into(cx, &moved, &chain, inner)?;
	}
	Ok(())
}

/// Whether the conjunct `value` of `filter` can be re-created in the predicate of `inner`, reading
/// its row.
fn can_land_in(ir: &Ir, filter: &WhereFilter, value: ValueId, inner: &WhereFilter) -> bool {
	let Some(def) = def_op(ir, value) else {
		return false;
	};
	let inside = ancestors(ir, def).any(|(_, owner)| owner == Some(filter.op));
	inside && can_rehome(ir, def, InsertPoint::Before(inner.yielded), &[(filter.row, inner.row)])
}

/// Appends the conjuncts of `filter` to the predicate of `inner`, the conjuncts `inner` holds
/// first, and erases `filter`.
fn merge_into(
	cx: &mut LoCx<'_>,
	filter: &WhereFilter,
	chain: &[ValueId],
	inner: &WhereFilter,
) -> Result<(), IrError> {
	let mut all: SmallVec<[ValueId; 8]> = and_chain(cx.ir, inner.predicate).into_iter().collect();
	all.extend(chain.iter().copied());
	let spine = chain_spine(cx.ir, inner.predicate);
	let mut rw = Rewriter::new(&mut *cx.ir, &mut *cx.facts);
	let merged = rebuild_and_chain_bound(
		&mut rw,
		InsertPoint::Before(inner.yielded),
		&all,
		&[(filter.row, inner.row)],
	)?;
	rw.set_operand(inner.yielded, 0, merged)?;
	for gate in spine {
		if rw.ir().op(gate).is_none() {
			continue;
		}
		match rw.erase(gate, EliminationReason::DeadPure) {
			Ok(()) | Err(IrError::HasUses(_)) => {}
			Err(error) => return Err(error),
		}
	}
	rw.replace_all_uses(filter.result, filter.input)?;
	rw.erase(filter.op, EliminationReason::MergedInto(inner.op))
}

/// Where the conjuncts of `filter` land, when its rows come from a subquery whose rows leave
/// through a projection.
fn subquery_target(cx: &mut LoCx<'_>, filter: &WhereFilter) -> Option<Target> {
	let (project, beneath, inner, identity) = {
		let ir: &Ir = &*cx.ir;
		if in_stored_clause(ir, filter.op) {
			return None;
		}
		// The filter reads the row of its loop, and is the only op that does.
		let block = ir.block(ir.op(filter.op)?.parent)?;
		let row = *block.args.first()?;
		if filter.input != row || ir.uses(row).len() != 1 {
			return None;
		}
		let outer = ir.op(ir.parent_op(block.parent)?)?;
		if !is_select_loop(&outer.kind) {
			return None;
		}
		// The loop reads its rows from a subquery nothing else reads.
		let source = *outer.operands.first()?;
		let subquery = ir.op(def_op(ir, source)?)?;
		if !matches!(subquery.kind, OpKind::Logical(LogOp::Subquery)) || ir.uses(source).len() != 1
		{
			return None;
		}
		let [region] = subquery.regions.as_slice() else {
			return None;
		};
		let [subquery_block] = ir.region(*region)?.blocks.as_slice() else {
			return None;
		};
		// No literal source stands anywhere beneath it.
		let mut literal = false;
		ir.walk(*region, WalkOrder::PreOrder, &mut |op| {
			if ir
				.op(op)
				.is_some_and(|data| matches!(data.kind, OpKind::Logical(LogOp::Values { .. })))
			{
				literal = true;
				return Walk::Stop;
			}
			Walk::Continue
		});
		if literal {
			return None;
		}
		// The subquery is one `SELECT` loop whose rows are the result of the subquery.
		let yielded = ir.op(*ir.block(*subquery_block)?.ops.last()?)?;
		let (OpKind::Logical(LogOp::Yield), [rows]) = (&yielded.kind, yielded.operands.as_slice())
		else {
			return None;
		};
		let inner_loop = ir.op(def_op(ir, *rows)?)?;
		if !is_select_loop(&inner_loop.kind) || ir.uses(*rows).len() != 1 {
			return None;
		}
		let [body] = inner_loop.regions.as_slice() else {
			return None;
		};
		let [body_block] = ir.region(*body)?.blocks.as_slice() else {
			return None;
		};
		// Its rows leave through the projection, and nothing stands after it.
		let ending = ir.op(*ir.block(*body_block)?.ops.last()?)?;
		let (OpKind::Logical(LogOp::Yield), [projected]) =
			(&ending.kind, ending.operands.as_slice())
		else {
			return None;
		};
		let project = def_op(ir, *projected)?;
		let projection = ir.op(project)?;
		let OpKind::Logical(LogOp::Project(attrs)) = &projection.kind else {
			return None;
		};
		if attrs.layer {
			return None;
		}
		let identity = is_identity_projection(&projection.kind) && projection.regions.is_empty();
		let beneath = *projection.operands.first()?;
		let inner =
			def_op(ir, beneath).and_then(|candidate| where_filter(ir, candidate)).filter(|held| {
				ir.uses(beneath).len() == 1
					&& ir.op(held.op).map(|data| data.parent) == Some(projection.parent)
			});
		(project, beneath, inner, identity)
	};
	let shape = if identity {
		Shape::Identity
	} else {
		Shape::Named(projection_of(cx, project)?)
	};
	Some(Target {
		project,
		beneath,
		inner,
		shape,
	})
}

/// One field of a projection, read out of the container.
struct Field {
	/// The name the field lands under.
	path: crate::expr::logical::OutputPath,
	/// The ops directly in the field's region.
	ops: Vec<OpId>,
	/// The row argument of the region.
	row: ValueId,
	/// The value the region yields.
	value: ValueId,
	/// Whether the field is coerced to a kind.
	coerced: bool,
}

/// What the projection `project` says of the names a conjunct over its rows reads, or `None` when
/// no conjunct may move through it: a field can write or raise, an `OMIT` names no field, or the
/// fields do not pair with their regions.
fn projection_of(cx: &mut LoCx<'_>, project: OpId) -> Option<Projection> {
	let (fields, omitted, passes_through) = {
		let ir: &Ir = &*cx.ir;
		let data = ir.op(project)?;
		let OpKind::Logical(LogOp::Project(attrs)) = &data.kind else {
			return None;
		};
		let FieldsShape::Select(shape) = &attrs.fields else {
			return None;
		};
		if !attrs.sort_keys.is_empty() {
			return None;
		}
		let singles =
			shape.iter().filter(|field| matches!(field, FieldShape::Single { .. })).count();
		let omit = usize::from(attrs.omit);
		if data.regions.len() != singles + omit {
			return None;
		}
		let mut fields = Vec::with_capacity(singles);
		let (mut at, mut unaliased) = (0, 0);
		for field in shape.iter() {
			let FieldShape::Single {
				alias,
				kind,
			} = field
			else {
				continue;
			};
			let path = match alias {
				Some(path) => path.clone(),
				None => {
					unaliased += 1;
					attrs.names.get(unaliased - 1)?.clone()
				}
			};
			let region = *data.regions.get(at)?;
			at += 1;
			let [block] = ir.region(region)?.blocks.as_slice() else {
				return None;
			};
			let [row] = ir.block(*block)?.args.as_slice() else {
				return None;
			};
			fields.push(Field {
				path,
				ops: ops_in(ir, region).collect(),
				row: *row,
				value: yielded_value(ir, region)?,
				coerced: kind.is_some(),
			});
		}
		let mut omitted: Vec<String> = Vec::with_capacity(omit);
		for region in &data.regions[at..] {
			omitted.push(omitted_root(ir, *region)?);
		}
		let passes_through = shape.iter().any(|field| matches!(field, FieldShape::All));
		(fields, omitted, passes_through)
	};
	let mut out = Projection {
		passes_through,
		..Projection::default()
	};
	for field in &fields {
		if !inert_field(cx, field) {
			return None;
		}
		let Some(OutputPathPart::Field(root)) = field.path.parts().first() else {
			continue;
		};
		let ir: &Ir = &*cx.ir;
		let source = def_op(ir, field.value).map(|root| Source {
			root,
			row: field.row,
		});
		let plain = field.path.as_field().is_some()
			&& !field.coerced
			&& runs_as_caller(ir, &field.ops)
			&& free_to_duplicate(ir, field.value, field.row, 0);
		match source {
			Some(source)
				if plain
					&& !out.opaque.contains(root.as_str())
					&& !out.substitutable.contains_key(root.as_str()) =>
			{
				out.substitutable.insert(root.clone(), source);
			}
			_ => {
				out.substitutable.remove(root.as_str());
				out.opaque.insert(root.clone());
			}
		}
	}
	for name in omitted {
		out.substitutable.remove(&name);
		out.opaque.insert(name);
	}
	Some(out)
}

/// The name of the field the `OMIT` path in `region` removes: the field read the path starts
/// with, or `None` when it starts with anything else.
fn omitted_root(ir: &Ir, region: RegionId) -> Option<String> {
	let mut value = yielded_value(ir, region)?;
	for _ in 0..32 {
		let op = ir.op(def_op(ir, value)?)?;
		match &op.kind {
			OpKind::Expr(ExprOp::Field {
				name,
			}) => return Some(name.as_str().to_owned()),
			OpKind::Expr(
				ExprOp::Access {
					..
				}
				| ExprOp::Index
				| ExprOp::First
				| ExprOp::Last
				| ExprOp::Each
				| ExprOp::Flatten {
					..
				}
				| ExprOp::Optional,
			) => value = *op.operands.first()?,
			OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => return None,
		}
	}
	None
}

/// Whether the field evaluates read-only and cannot raise.
fn inert_field(cx: &mut LoCx<'_>, field: &Field) -> bool {
	inert_ops(cx, &field.ops, field.row)
}

/// Whether every op of `ops` is read-only and cannot raise. A plain read of a field off `row` and
/// a read of a parameter whose value is in hand count as both: a row holds the value of each of
/// its fields, and a parameter that fetches a definition is a different op. The operands of an
/// `AND` or `OR` are judged one by one, since the gate itself raises nothing.
fn inert_ops(cx: &mut LoCx<'_>, ops: &[OpId], row: ValueId) -> bool {
	ops.iter().all(|op| {
		let Some(data) = cx.ir.op(*op) else {
			return false;
		};
		if matches!(data.kind, OpKind::Expr(ExprOp::Field { .. } | ExprOp::Access { .. }))
			&& data.operands.as_slice() == [row]
		{
			return true;
		}
		if matches!(data.kind, OpKind::Expr(ExprOp::FreeParam { .. } | ExprOp::SessionParam { .. }))
		{
			return true;
		}
		if matches!(data.kind, OpKind::Expr(ExprOp::ShortCircuit { .. })) {
			let nested: Vec<OpId> =
				data.regions.iter().flat_map(|region| ops_in(cx.ir, *region)).collect();
			return inert_ops(cx, &nested, row);
		}
		let answer = effects::of_with(&*cx.ir, &mut *cx.facts, cx.effects, *op);
		let class = answer.fallibility.union(fallibility::floor(answer.set));
		!answer.set.mutates_state() && guards::infallible(class)
	})
}

/// Whether every op of `ops` is evaluated as the caller, under the caller's authority.
fn runs_as_caller(ir: &Ir, ops: &[OpId]) -> bool {
	ops.iter().all(|op| {
		ir.op(*op)
			.and_then(|data| ir.frame(data.frame))
			.is_some_and(|frame| frame.eval.mode == EvalMode::Caller && frame.eval.auth.is_none())
	})
}

/// Whether substituting the expression `value` for each mention of its name costs no more than
/// the mention did: a path, a literal, a parameter, a constant, or a comparison, `AND`, `OR` or
/// `NOT` over those. Anything else, a call, arithmetic or a method, is evaluated once per mention
/// where the projection evaluated it once per row, and a predicate over it drives no index.
fn free_to_duplicate(ir: &Ir, value: ValueId, row: ValueId, depth: u8) -> bool {
	if depth > 32 {
		return false;
	}
	if value == row {
		return true;
	}
	let Some(data) = def_op(ir, value).and_then(|op| ir.op(op)) else {
		return false;
	};
	let free = |operand: &ValueId| free_to_duplicate(ir, *operand, row, depth + 1);
	match &data.kind {
		OpKind::Expr(
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
			},
		) => true,
		OpKind::Expr(ExprOp::Binary {
			op,
		}) => is_comparison_or_logical(op) && data.operands.iter().all(free),
		OpKind::Expr(ExprOp::ShortCircuit {
			..
		}) => {
			let [region] = data.regions.as_slice() else {
				return false;
			};
			data.operands.iter().all(free)
				&& yielded_value(ir, *region).is_some_and(|lazy| free(&lazy))
		}
		OpKind::Expr(ExprOp::Prefix(operator)) => {
			matches!(**operator, PrefixOperator::Not) && data.operands.iter().all(free)
		}
		OpKind::Expr(
			ExprOp::Field {
				..
			}
			| ExprOp::First
			| ExprOp::Last
			| ExprOp::Flatten {
				..
			}
			| ExprOp::Index,
		) => is_path(ir, value, row, depth),
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// Whether `value` is a path over `row`: a field read off it, or a step over a path, an index
/// whose index expression is free to duplicate included.
fn is_path(ir: &Ir, value: ValueId, row: ValueId, depth: u8) -> bool {
	if depth > 32 {
		return false;
	}
	if value == row {
		return true;
	}
	let Some(data) = def_op(ir, value).and_then(|op| ir.op(op)) else {
		return false;
	};
	match &data.kind {
		OpKind::Expr(ExprOp::Field {
			..
		}) => data.operands.as_slice() == [row],
		OpKind::Expr(
			ExprOp::First
			| ExprOp::Last
			| ExprOp::Flatten {
				..
			},
		) => data.operands.first().is_some_and(|base| is_path(ir, *base, row, depth + 1)),
		OpKind::Expr(ExprOp::Index) => match data.operands.as_slice() {
			[base, index] => {
				is_path(ir, *base, row, depth + 1) && free_to_duplicate(ir, *index, row, depth + 1)
			}
			_ => false,
		},
		OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_) => false,
	}
}

/// A comparison, a containment test, or `AND` / `OR`: an operator whose evaluation is one step
/// over its operands. `MATCHES` and `KNN` are not: each is answered by an access path of the
/// statement it is written in.
fn is_comparison_or_logical(op: &BinaryOperator) -> bool {
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
		| BinaryOperator::NoneInside
		| BinaryOperator::Outside
		| BinaryOperator::Intersects
		| BinaryOperator::And
		| BinaryOperator::Or => true,
		BinaryOperator::Matches(_)
		| BinaryOperator::NearestNeighbor(_)
		| BinaryOperator::NullCoalescing
		| BinaryOperator::TenaryCondition
		| BinaryOperator::Add
		| BinaryOperator::Subtract
		| BinaryOperator::Multiply
		| BinaryOperator::Divide
		| BinaryOperator::Remainder
		| BinaryOperator::Power
		| BinaryOperator::Range
		| BinaryOperator::RangeInclusive
		| BinaryOperator::RangeSkip
		| BinaryOperator::RangeSkipInclusive => false,
	}
}

/// Whether `kind` is a `SELECT` loop.
fn is_select_loop(kind: &OpKind) -> bool {
	matches!(
		kind,
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::Select,
			..
		})
	)
}

/// Whether `kind` is a projection that returns its rows as they are: `*` alone, no `OMIT`, and not
/// a layer.
fn is_identity_projection(kind: &OpKind) -> bool {
	let OpKind::Logical(LogOp::Project(attrs)) = kind else {
		return false;
	};
	!attrs.layer
		&& attrs.omit == 0
		&& attrs.names.is_empty()
		&& attrs.computed.is_empty()
		&& attrs.sort_keys.is_empty()
		&& matches!(&attrs.fields, FieldsShape::Select(shape) if matches!(&shape[..], [FieldShape::All]))
}
