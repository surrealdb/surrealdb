//! Greedy rewrites over the physical dialect.
//!
//! Every rule is a [`Pattern`] rooted at one physical tag. A rule delivers the same rows, in the
//! same order, raises the same errors and evaluates each expression as often as the plan it
//! replaces. Each rule's doc names the invariant it keeps and the ops it may cross.
//!
//! [`run`] drives the registered rules to a fixed point in at most [`GREEDY_DEFAULT`] passes. It
//! runs on a container in [`Phase::Physical`], after [`super::plan_physical`] and before the
//! physical common-subexpression pass. For a container in any other phase it returns an empty
//! report. At the end it places the container again from the strategies the rules decided. The
//! placement [`super::plan_physical`] made was for the plan before the rules, so it is replaced:
//! the container's final contents are what runs, and its placement must match them.
//!
//! A rule reads the ops beneath its root only through the chain of ops that keep the order and
//! the identity of their input rows. A rule never reaches past an op whose result has another
//! user: a value read by two consumers gives both the same rows, so changing its producer would
//! change the other consumer's rows. Each rule's list of crossable ops is exactly the set that
//! keeps the fields and rows the rule reads. An op outside that set may change a field's value
//! (a layer) or hide an access path (a permission) in a way the rule does not judge. The per-row
//! ops of a statement sit in the body of a select loop inside a pipeline. The chain is read
//! through both ([`chain`]), so a rule sees the source of a loop as the source of the ops in its
//! body.
//!
//! A rule that removes a breaker never edits a legality. The removed op's provenance joins the op
//! that defines the rows it read. The legality of the logical segment it belonged to then follows
//! the surviving pipeline, through the provenance placement reads it by.

#![deny(clippy::wildcard_enum_match_arm)]

use smallvec::SmallVec;

use super::facts::NestedReading;
use super::ops::{KeyEqShape, KeyRangeShape, PhysOp, PhysTag, SortAttrs, SortKeyReadShape};
use super::placement::{Placement, install, place_all_reading};
use super::{
	chain, conjunct, hold_left, hop_key, index_answer, join_lookup, knn_ids, nested, once,
	prefilter, recheck,
};
use crate::catalog::KeyStructure;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::index::key_range::DeliveredKeyOrder;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::pattern::{GREEDY_DEFAULT, GreedyReport, Pattern, apply_greedy};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::PhysCx;
use crate::exec::ir::iface::conversion::{Phase, PhysDialect};
use crate::exec::ir::iface::fold::PlanTimeFolder;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{FieldRole, RegionKind};
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::registry;
use crate::exec::operators::SortDirection;
use crate::exec::topk_pushdown::{field_path_wire_segments, reads_key_component};
use crate::exec::vm::payload::{CoarseCutoff, Spill, TopKKey};
use crate::kvs::Direction;
use crate::val::{Number, Value};

/// How deep the walk over a sort's wire path descends when a top-k key is installed.
const TOPK_DEPTH_LIMIT: u32 = 256;

/// The physical rules, in the order they are registered. A rule with a higher benefit is offered
/// an op first.
pub(crate) fn patterns() -> Vec<Box<dyn Pattern<PhysDialect>>> {
	vec![
		Box::new(ElideDeliveredSort),
		Box::new(BoundSortTopK),
		Box::new(CoarseCutoffSort),
		Box::new(AnsweredByLookup),
		Box::new(AnsweredByIndex),
		Box::new(AnsweredBySearch),
		Box::new(ConjunctOrder),
	]
}

/// Runs the registered physical rules over `ir`, which must be in [`Phase::Physical`], until none
/// applies or [`GREEDY_DEFAULT`] is reached. It then decides the strategy of every statement
/// written in value position ([`nested::run`]) from the container the rules left.
///
/// The strategies are decided after the rules reach their fixed point. A statement is judged from
/// the predicate and the access path it holds at the end, and each statement is decided after the
/// statements nested in it. `cx` answers the catalog questions of that step. A catalog that answers
/// no index leaves the strategies `Apply`, `Once` and `Hash`.
///
/// Before the strategies, every join that reads an indexed table as its right input reads that
/// input through the index for its first left rows ([`join_lookup::run`]). Every join left
/// without a lookup whose left input yields at most one row then holds that row
/// ([`hold_left::run`]), judged from the inputs the rules left.
///
/// Then every filter judges once the conjuncts that answer the same at every row ([`once::run`]).
///
/// Then each scan states the prefilter of the filter it feeds ([`prefilter::run`]), and each KNN
/// scan whether its rows need only their ids ([`knn_ids::run`]). Each index scan
/// states whether that filter only rechecks its access ([`recheck::run`]). Both use the predicate
/// the rules left. Each graph hop states the key condition its `WHERE` is judged by
/// ([`hop_key::run`]).
///
/// Then every pipeline is placed again through `placement`, using the facts of the container the
/// rules left and the strategies ([`NestedReading::Strategy`]). The new placements replace the
/// ones [`super::plan_physical`] installed. The facts of a pipeline come from the sorts the rules
/// removed, the limits they bounded and the statements that took an arm.
///
/// A container in another phase is left as it is, and the report is empty. The caller verifies the
/// container afterwards.
pub(crate) async fn run(
	ir: &mut Ir,
	facts: &mut FactCache,
	folder: &dyn PlanTimeFolder,
	cx: &PhysCx<'_>,
	placement: &Placement,
) -> Result<GreedyReport, IrError> {
	if ir.phase() != Phase::Physical {
		return Ok(GreedyReport {
			passes: 0,
			rewrites: 0,
			stopped_by: None,
		});
	}
	let mut report = apply_greedy(
		ir,
		facts,
		&registry::build().phys_patterns,
		folder,
		cx.effects.registry,
		GREEDY_DEFAULT,
	)
	.await?;
	report.rewrites += join_lookup::run(ir, facts, cx)?;
	report.rewrites += hold_left::run(ir, facts, cx)?;
	report.rewrites += nested::run(ir, facts, cx)?;
	report.rewrites += once::run(ir, facts, cx)?;
	prefilter::run(ir, facts, cx)?;
	knn_ids::run(ir, facts, cx)?;
	recheck::run(ir, facts)?;
	hop_key::run(ir, facts, cx);
	let placed = place_all_reading(
		ir,
		facts,
		cx.effects.registry,
		placement,
		cx.physical.backend(),
		NestedReading::Strategy,
	)?;
	install(ir.placement_table_mut(), placed);
	Ok(report)
}

/// The physical op `op` is, or `None` for an op of another dialect or one that is gone.
fn phys(ir: &Ir, op: OpId) -> Option<&PhysOp> {
	match &ir.op(op)?.kind {
		OpKind::Phys(kind) => Some(kind),
		OpKind::Expr(_) | OpKind::Logical(_) => None,
	}
}

/// The op that defines `value`, when `value` is an op's result.
fn producer(ir: &Ir, value: ValueId) -> Option<OpId> {
	match ir.value(value)?.def {
		ValueDef::OpResult {
			op,
			..
		} => Some(op),
		ValueDef::BlockArg {
			..
		} => None,
	}
}

/// The ops beneath the rows `from`: the ops of `crossing` that keep their input's row order and
/// identity, nearest first, and the op that produces the rows they pass on.
pub(super) struct Beneath {
	/// The ops crossed, nearest `from` first.
	pub(super) crossed: SmallVec<[OpId; 8]>,
	/// The op that produces the rows the crossed ops pass on.
	pub(super) source: OpId,
}

/// Follows the first operand from `from` through every op `crossing` admits to the op that
/// produces the rows.
///
/// A `phys.pipeline` and a select loop are read through ([`chain::enter`]): the ops of a loop's
/// body are crossed like the ops of a flat chain, and the walk continues at the loop's source.
/// A select loop over the statement's rows is therefore crossed the same way as a flat chain.
///
/// Answers `None` when a value on the way is read by another op as well, or is not the result of
/// a physical op. Every op in the answer is therefore read only by the next one, so a rule may
/// rewrite them without changing any other consumer.
pub(super) fn beneath(ir: &Ir, from: ValueId, crossing: fn(&PhysOp) -> bool) -> Option<Beneath> {
	let mut crossed = SmallVec::new();
	let mut value = from;
	loop {
		if ir.uses(value).len() != 1 {
			return None;
		}
		if let Some(through) = chain::enter(ir, value) {
			value = through;
			continue;
		}
		let op = producer(ir, value)?;
		let kind = phys(ir, op)?;
		if !crossing(kind) {
			return Some(Beneath {
				crossed,
				source: op,
			});
		}
		crossed.push(op);
		value = *ir.op(op)?.operands.first()?;
	}
}

/// The ops that pass their input's rows on in the order they arrive and leave a row's fields as
/// stored or as a resolver layer gives them: a layer, a predicate, a permission and table
/// enforcement.
pub(super) fn keeps_order(kind: &PhysOp) -> bool {
	matches!(
		kind,
		PhysOp::Layer(_) | PhysOp::Filter | PhysOp::PermissionFilter(_) | PhysOp::Enforced { .. }
	)
}

/// The ops a top-k key may be installed beneath: a predicate alone.
fn keeps_scan(kind: &PhysOp) -> bool {
	matches!(kind, PhysOp::Filter)
}

/// Whether the access path beneath the sort `sort` lists its rows in the order the sort's keys
/// ask for.
///
/// - A table scan that may be taken to deliver record ids in id order delivers them along its
///   direction: a sort of exactly the key `id` in that direction is delivered.
/// - A key-range scan delivers record ids in its direction where every component of the key stores
///   in value order, and otherwise the components its access orders by, when the access delivers
///   the offered ordering exactly and each key reads the component at its place.
/// - An index scan or an index union whose `ordered_by` names exactly the fields the keys read, in
///   order.
///
/// A scan of a table whose key order is not known to be id order is never taken to deliver an
/// order: its sort stays.
fn order_delivered(ir: &Ir, sort: OpId) -> bool {
	let Some(PhysOp::Sort(attrs)) = phys(ir, sort) else {
		return false;
	};
	if attrs.random {
		return false;
	}
	let Some(input) = ir.op(sort).and_then(|data| data.operands.first().copied()) else {
		return false;
	};
	let Some(chain) = beneath(ir, input, keeps_order) else {
		return false;
	};
	let ordered_by = match phys(ir, chain.source) {
		Some(PhysOp::TableScan(scan)) => {
			return scan.id_order
				&& keyed_by_id(ir, sort, attrs, scan.direction == Direction::Forward);
		}
		Some(PhysOp::KeyRangeScan(scan)) => {
			let access = &scan.access;
			let forward = access.direction == Direction::Forward;
			return (access.key.all_exactly_ordered() && keyed_by_id(ir, sort, attrs, forward))
				|| key_range_delivers(ir, sort, attrs, access, &chain.crossed);
		}
		Some(PhysOp::IndexScan(scan)) => scan.ordered_by.as_deref(),
		Some(PhysOp::IndexUnion(union)) => union.ordered_by.as_deref(),
		Some(_) | None => None,
	};
	ordered_by.is_some_and(|fields| reads_exactly(ir, sort, attrs, fields))
}

/// Whether the keys of the sort `op` are the one key `id`, plain, in the direction `forward`
/// names.
fn keyed_by_id(ir: &Ir, op: OpId, sort: &SortAttrs, forward: bool) -> bool {
	matches!(
		sort.keys.as_slice(),
		[key] if key_path(ir, op, sort, 0).is_some_and(|path| path == FieldPath::field("id"))
			&& !key.collate
			&& !key.numeric
			&& key.ascending == forward
	)
}

/// The plain path of stored field names the key `at` of the sort `op` reads from its row: the
/// path a key states, or the one its `Clause(OrderKey)` region yields as field reads of the
/// region's row alone. `None` for a key that computes anything else.
fn key_path(ir: &Ir, op: OpId, sort: &SortAttrs, at: usize) -> Option<FieldPath> {
	match &sort.keys.get(at)?.read {
		SortKeyReadShape::Path(path) => Some(path.clone()),
		SortKeyReadShape::Computed => {
			let earlier = sort.keys[..at]
				.iter()
				.filter(|key| matches!(key.read, SortKeyReadShape::Computed))
				.count();
			region_path(ir, *ir.op(op)?.regions.get(earlier)?)
		}
	}
}

/// The plain path of stored field names the region `region` yields as field reads of its row
/// alone. `None` for a region that computes anything else.
pub(super) fn region_path(ir: &Ir, region: RegionId) -> Option<FieldPath> {
	let last = ops_in_region(ir, region).last().copied()?;
	let closing = ir.op(last)?;
	let (OpKind::Expr(ExprOp::Yield), [value]) = (&closing.kind, closing.operands.as_slice())
	else {
		return None;
	};
	let mut names = Vec::new();
	let mut at = *value;
	loop {
		let read = ir.op(conjunct::def_op(ir, at)?)?;
		match (&read.kind, read.operands.as_slice()) {
			(
				OpKind::Expr(ExprOp::Field {
					name,
				}),
				[row],
			) if is_row_of(ir, *row, region) => {
				names.push(name.clone());
				break;
			}
			(
				OpKind::Expr(ExprOp::Access {
					field,
				}),
				[base],
			) => {
				names.push(field.clone());
				at = *base;
			}
			_ => return None,
		}
	}
	names.reverse();
	Some(FieldPath(names.into_iter().map(|name| FieldPathPart::Field(name.to_string())).collect()))
}

/// Whether the key-range access `access` delivers the order of the keys of `sort` exactly: it
/// delivers its ordering exactly, and each key reads the component the access orders by at its
/// place, without `COLLATE` or `NUMERIC`, in the access's direction unless the access pins the
/// component to one value. `crossed` are the ops between the sort and the scan, nearest first.
fn key_range_delivers(
	ir: &Ir,
	sort: OpId,
	attrs: &SortAttrs,
	access: &KeyRangeShape,
	crossed: &[OpId],
) -> bool {
	if access.delivers != DeliveredKeyOrder::Exact || attrs.keys.len() != access.ordered_by.len() {
		return false;
	}
	let forward = access.direction == Direction::Forward;
	attrs.keys.iter().zip(&access.ordered_by).enumerate().all(|(at, (key, &position))| {
		let pinned = matches!(access.eq.get(position as usize), Some(KeyEqShape::One(_)));
		!key.collate
			&& !key.numeric
			&& sort_key_component(ir, sort, attrs, at, &access.key, crossed) == Some(position)
			&& (pinned || key.ascending == forward)
	})
}

/// The position of the record id component the key `at` of `sort` reads, over a table whose ids
/// have the structure `key`: `id[i]` evaluated, or a field that a layer among `crossed` derives
/// as a column of the key part reading the component, whether the key names the field as its path
/// or evaluates a read of it. A key that reads anything else answers `None`, and so does a layer
/// column that is not of the key part: the sort then stays.
///
/// The nearest layer that sets the column is the one whose value the sort reads, whether or not
/// that column reads a component.
fn sort_key_component(
	ir: &Ir,
	sort: OpId,
	attrs: &SortAttrs,
	at: usize,
	key: &KeyStructure,
	crossed: &[OpId],
) -> Option<u32> {
	match &attrs.keys.get(at)?.read {
		SortKeyReadShape::Computed => {
			let earlier = attrs.keys[..at]
				.iter()
				.filter(|key| matches!(key.read, SortKeyReadShape::Computed))
				.count();
			let region = *ir.op(sort)?.regions.get(earlier)?;
			component_read(ir, region, key, false).or_else(|| {
				let path = region_row_path(ir, region)?;
				let [name] = path.as_slice() else {
					return None;
				};
				layer_column_component(ir, crossed, name, key)
			})
		}
		SortKeyReadShape::Path(path) => {
			let [FieldPathPart::Field(name)] = path.0.as_slice() else {
				return None;
			};
			layer_column_component(ir, crossed, name, key)
		}
	}
}

/// The component position the column `name` reads, set by the nearest layer among `crossed` that
/// sets it, when that column is of the key part ([`layer_key_part`]).
fn layer_column_component(
	ir: &Ir,
	crossed: &[OpId],
	name: &str,
	key: &KeyStructure,
) -> Option<u32> {
	let column = crate::expr::logical::OutputPath::field(name);
	crossed.iter().find_map(|layer| {
		let Some(PhysOp::Layer(attrs)) = phys(ir, *layer) else {
			return None;
		};
		let FieldsShape::Select(fields) = &attrs.fields else {
			return None;
		};
		// An unaliased column is named by the layer's next unaliased name, in field order.
		let mut unaliased = attrs.names.iter();
		let singles = fields.iter().filter_map(|field| match field {
			FieldShape::Single {
				alias,
				..
			} => Some(alias.as_ref().or_else(|| unaliased.next())),
			FieldShape::All => None,
		});
		let nth = singles
			.enumerate()
			.find_map(|(nth, name)| name.is_some_and(|name| *name == column).then_some(nth))?;
		Some(layer_key_part(ir, *layer, nth, key))
	})?
}

/// The field names the single value `region` yields reads from the row the region binds, when it
/// is nothing but plain field reads ([`row_path`]).
fn region_row_path(ir: &Ir, region: RegionId) -> Option<Vec<&str>> {
	let last = ops_in_region(ir, region).last().copied()?;
	let closing = ir.op(last)?;
	let (OpKind::Expr(ExprOp::Yield), [yielded]) = (&closing.kind, closing.operands.as_slice())
	else {
		return None;
	};
	row_path(ir, *yielded, region)
}

/// The component position the `nth` column of the layer `layer` derives, when the column is of
/// the key part: the column's expression is evaluated in the field frame region it holds, under
/// the frame recorded for the column.
fn layer_key_part(ir: &Ir, layer: OpId, nth: usize, key: &KeyStructure) -> Option<u32> {
	let region = ir
		.op(layer)?
		.regions
		.iter()
		.copied()
		.filter(|region| {
			ir.region(*region)
				.is_some_and(|data| data.kind == RegionKind::FieldFrame(FieldRole::Computed))
		})
		.nth(nth)?;
	let first = ops_in_region(ir, region).first().copied()?;
	let in_key_part = ir.frame(ir.op(first)?.frame)?.eval.key_part;
	in_key_part.then(|| component_read(ir, region, key, true)).flatten()
}

/// The ops of the first block of `region`, in order.
fn ops_in_region(ir: &Ir, region: RegionId) -> &[OpId] {
	ir.region(region)
		.and_then(|data| data.blocks.first().copied())
		.and_then(|block| ir.block(block))
		.map_or(&[], |block| block.ops.as_slice())
}

/// The component position the single value `region` yields reads: `id[i]` for an `i` within the
/// arity of `key`, or, with `allow_first`, the first element of `id` and a path that addresses a
/// component through an alias of `key` ([`KeyStructure::aliases`]). The `id` and the path are read
/// from the row the region binds.
///
/// A path beneath a component, a path with a step that is not a plain field read, and a path no
/// alias names read no component.
fn component_read(ir: &Ir, region: RegionId, key: &KeyStructure, allow_first: bool) -> Option<u32> {
	let last = ops_in_region(ir, region).last().copied()?;
	let closing = ir.op(last)?;
	let (OpKind::Expr(ExprOp::Yield), [yielded]) = (&closing.kind, closing.operands.as_slice())
	else {
		return None;
	};
	let read = ir.op(producer(ir, *yielded)?)?;
	let position = match (&read.kind, read.operands.as_slice()) {
		(OpKind::Expr(ExprOp::Index), [base, index]) if reads_id_of_row(ir, *base, region) => {
			let op = ir.op(producer(ir, *index)?)?;
			let OpKind::Expr(ExprOp::Literal(held)) = &op.kind else {
				return None;
			};
			let Value::Number(Number::Int(position)) = **held else {
				return None;
			};
			u32::try_from(position).ok()?
		}
		(OpKind::Expr(ExprOp::First), [base])
			if allow_first && reads_id_of_row(ir, *base, region) =>
		{
			0
		}
		(
			OpKind::Expr(
				ExprOp::Field {
					..
				}
				| ExprOp::Access {
					..
				},
			),
			_,
		) if allow_first => {
			return alias_component(key, &row_path(ir, *yielded, region)?);
		}
		_ => return None,
	};
	((position as usize) < key.arity()).then_some(position)
}

/// The field names `value` reads from the row `region` binds, outermost first, when it is nothing
/// but plain field reads.
fn row_path(ir: &Ir, value: ValueId, region: RegionId) -> Option<Vec<&str>> {
	let data = ir.op(producer(ir, value)?)?;
	match (&data.kind, data.operands.as_slice()) {
		(
			OpKind::Expr(ExprOp::Field {
				name,
			}),
			[row],
		) if is_row_of(ir, *row, region) => Some(vec![name.as_str()]),
		(
			OpKind::Expr(ExprOp::Access {
				field,
			}),
			[base],
		) => {
			let mut path = row_path(ir, *base, region)?;
			path.push(field.as_str());
			Some(path)
		}
		(OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_), _) => None,
	}
}

/// Whether `value` is a block argument of `region`, the row the region binds.
fn is_row_of(ir: &Ir, value: ValueId, region: RegionId) -> bool {
	matches!(
		ir.value(value).map(|data| data.def),
		Some(ValueDef::BlockArg { block, .. })
			if ir.block(block).is_some_and(|data| data.parent == region)
	)
}

/// The position of the component of `key` that the path of plain field names `path` is an alias
/// of.
fn alias_component(key: &KeyStructure, path: &[&str]) -> Option<u32> {
	key.aliases.iter().find_map(|(alias, position)| {
		let names = alias.0.len() == path.len()
			&& alias.0.iter().zip(path).all(
				|(part, name)| matches!(part, crate::expr::Part::Field(field) if field.as_str() == *name),
			);
		names.then_some(*position)
	})
}

/// Whether `value` is the field `id` of the row `region` binds.
fn reads_id_of_row(ir: &Ir, value: ValueId, region: RegionId) -> bool {
	let Some(read) = producer(ir, value).and_then(|op| ir.op(op)) else {
		return false;
	};
	let OpKind::Expr(ExprOp::Field {
		name,
	}) = &read.kind
	else {
		return false;
	};
	name.as_str() == "id" && read.operands.first().is_some_and(|row| is_row_of(ir, *row, region))
}

/// Whether the keys of the sort `op` read the field paths `fields`, in order, as plain paths: a
/// key that computes anything else reads a value no stored field holds.
fn reads_exactly(ir: &Ir, op: OpId, sort: &SortAttrs, fields: &[FieldPath]) -> bool {
	sort.keys.len() == fields.len()
		&& fields
			.iter()
			.enumerate()
			.all(|(at, field)| key_path(ir, op, sort, at).is_some_and(|path| path == *field))
}

/// A sort whose input already delivers its order is removed.
///
/// A layer, a predicate, a permission and
/// table enforcement keep their input's order, so the order reaching the sort is the source's. An
/// index scan or union whose `ordered_by` covers the pipeline's ordering delivers it over the
/// record fields it names, ties in the index's key order. Only a sort whose keys read exactly
/// those fields as plain paths is removed: a computed key would go unevaluated, and a key that a
/// projection renamed reads another value under the same name.
///
/// The sort's rows are its input's, so every user of the sort's result reads the input directly.
/// The sort's provenance joins the input's defining op, which is how the legality of the logical
/// segment the sort ended is restated for the fused pipeline.
struct ElideDeliveredSort;

impl Pattern<PhysDialect> for ElideDeliveredSort {
	fn name(&self) -> &'static str {
		"elide_delivered_sort"
	}

	fn root(&self) -> PhysTag {
		PhysTag::Sort
	}

	fn benefit(&self) -> u16 {
		20
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		if !order_delivered(rw.ir(), op) {
			return Ok(false);
		}
		let Some(input) = rw.ir().op(op).and_then(|data| data.operands.first().copied()) else {
			return Ok(false);
		};
		rw.replace_op(op, &[input])?;
		Ok(true)
	}
}

/// The count `value` holds when it is a literal non-negative integer.
pub(super) fn literal_count(ir: &Ir, value: ValueId) -> Option<usize> {
	let op = producer(ir, value)?;
	let OpKind::Expr(ExprOp::Literal(held)) = &ir.op(op)?.kind else {
		return None;
	};
	if let Value::Number(Number::Int(n)) = **held {
		usize::try_from(n).ok()
	} else {
		None
	}
}

/// The top-k key of the sort `op`, which a scan beneath reads from each record's stored bytes: the
/// first key as a plain path of stored fields ([`key_path`]).
///
/// A `COLLATE` or `NUMERIC` key orders by another comparison, which the scan does not replicate,
/// so such a sort publishes no threshold. The path of a key that opens with the record id is not
/// stored in the record body and has no wire form.
fn topk_key(ir: &Ir, op: OpId, sort: &SortAttrs) -> Option<TopKKey> {
	let first = sort.keys.first()?;
	if first.collate || first.numeric {
		return None;
	}
	let path = key_path(ir, op, sort, 0)?;
	let segments = field_path_wire_segments(&path)?;
	Some(TopKKey {
		path: segments.into(),
		direction: if first.ascending {
			SortDirection::Asc
		} else {
			SortDirection::Desc
		},
		single_key: sort.keys.len() == 1,
		depth_limit: TOPK_DEPTH_LIMIT,
		coarse_cutoff: None,
	})
}

/// A sort under a literal `LIMIT` keeps only the rows the limit can reach.
///
/// The bound is sound because the sort
/// is stable: the first `start + limit` rows of the order are the same whether the rest were kept
/// or not. The sort's input is still drained whole, so every key and predicate is evaluated as
/// before.
///
/// The sort records the rows it keeps, its spill as bounded, and, when its first key is a plain
/// stored path, that it publishes a threshold (`pushdown`). The table scan or key-range scan
/// beneath it, reached through predicates alone, then reads that key from each record's stored
/// bytes ([`TopKKey`]). A key-range scan takes no key whose path opens with the name of a
/// component of the record id, which the record body does not store; the sort still publishes its
/// threshold. A permission or a layer between the scan and the sort is not crossed: the scan stays
/// as it is.
///
/// A sort that already keeps exactly the rows the limit reaches, as the conversion of a sort under
/// a literal limit states it, takes its threshold and its scan's key here too; a sort that keeps
/// another number of rows is left as it is.
///
/// A sort that [`ElideDeliveredSort`] removes is left to it, so the outcome does not depend on
/// the order the rules are offered ops in.
struct BoundSortTopK;

impl BoundSortTopK {
	/// The rows `limit` and `start` reach, when the limit is a literal and so is the start.
	fn rows_reached(ir: &Ir, limit_op: OpId) -> Option<usize> {
		let data = ir.op(limit_op)?;
		let PhysOp::Limit {
			limit: Some(limit),
			start,
		} = phys(ir, limit_op)?
		else {
			return None;
		};
		let limit = literal_count(ir, *data.operands.get(usize::from(*limit))?)?;
		match start {
			Some(start) => {
				let start = literal_count(ir, *data.operands.get(usize::from(*start))?)?;
				limit.checked_add(start)
			}
			None => Some(limit),
		}
	}
}

impl Pattern<PhysDialect> for BoundSortTopK {
	fn name(&self) -> &'static str {
		"bound_sort_topk"
	}

	fn root(&self) -> PhysTag {
		PhysTag::Limit
	}

	fn benefit(&self) -> u16 {
		10
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let ir = rw.ir();
		let Some(keep) = Self::rows_reached(ir, op) else {
			return Ok(false);
		};
		let Some(sorted) = ir.op(op).and_then(|data| data.operands.first().copied()) else {
			return Ok(false);
		};
		if ir.uses(sorted).len() != 1 {
			return Ok(false);
		}
		let Some(sort) = producer(ir, sorted) else {
			return Ok(false);
		};
		let Some(PhysOp::Sort(attrs)) = phys(ir, sort) else {
			return Ok(false);
		};
		if attrs.keep.is_some_and(|held| held != keep) || order_delivered(ir, sort) {
			return Ok(false);
		}
		let key = topk_key(ir, sort, attrs);
		let pushdown = key.is_some();
		let scan = key.as_ref().and_then(|key| {
			let input = ir.op(sort)?.operands.first().copied()?;
			let chain = beneath(ir, input, keeps_scan)?;
			let takes_key = match phys(ir, chain.source) {
				Some(PhysOp::TableScan(_)) => true,
				Some(PhysOp::KeyRangeScan(scan)) => {
					!reads_key_component(&key.path, &scan.access.key)
				}
				Some(_) | None => false,
			};
			takes_key.then_some(chain.source)
		});
		let scan_takes = scan.filter(|scan| match phys(ir, *scan) {
			Some(PhysOp::TableScan(attrs)) => attrs.topk.is_none(),
			Some(PhysOp::KeyRangeScan(attrs)) => attrs.topk.is_none(),
			Some(_) | None => false,
		});
		if attrs.keep == Some(keep) && attrs.pushdown == pushdown && scan_takes.is_none() {
			return Ok(false);
		}
		if let (Some(scan), Some(key)) = (scan_takes, key) {
			rw.update_kind(scan, |kind| {
				if let OpKind::Phys(PhysOp::TableScan(attrs)) = kind {
					attrs.topk = Some(key);
				} else if let OpKind::Phys(PhysOp::KeyRangeScan(attrs)) = kind {
					attrs.topk = Some(key);
				}
			})?;
		}
		rw.update_kind(sort, |kind| {
			if let OpKind::Phys(PhysOp::Sort(attrs)) = kind {
				attrs.keep = Some(keep);
				attrs.pushdown = pushdown;
				attrs.spill = Spill::of_sort(Some(keep));
			}
		})?;
		Ok(true)
	}
}

/// A sort that keeps rows over a key-range scan delivering its first key by the whole second of
/// a datetime component of the record id has the scan cut where no later row can take a kept
/// place.
///
/// The cut
/// applies where only predicates, permissions, layers and enforcement lie between the sort and the
/// scan, which pass rows on in the order they arrive; where the scan delivers the order of the
/// first key coarsely at the component position (`DeliveredKeyOrder::Coarse`); where the first key
/// reads that component in the scan's direction, without `COLLATE` or `NUMERIC`; and where the
/// scan carries no other top-k key. The scan then stops past the second of the sort's threshold,
/// reading no stored bytes for the key, and the sort publishes its threshold (`pushdown`). The
/// sort keeps its rows, its keys and its spill: every key and predicate is still evaluated for
/// the rows the scan reads.
///
/// The sort must already keep rows, which [`BoundSortTopK`] decides from the `LIMIT` above it.
/// This rule therefore only sees a sort that the bound has already marked.
struct CoarseCutoffSort;

impl Pattern<PhysDialect> for CoarseCutoffSort {
	fn name(&self) -> &'static str {
		"coarse_cutoff_sort"
	}

	fn root(&self) -> PhysTag {
		PhysTag::Sort
	}

	fn benefit(&self) -> u16 {
		8
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let ir = rw.ir();
		let Some(PhysOp::Sort(attrs)) = phys(ir, op) else {
			return Ok(false);
		};
		let (Some(keep), false, Some(first)) = (attrs.keep, attrs.random, attrs.keys.first())
		else {
			return Ok(false);
		};
		if first.collate || first.numeric {
			return Ok(false);
		}
		let Some(input) = ir.op(op).and_then(|data| data.operands.first().copied()) else {
			return Ok(false);
		};
		let Some(chain) = beneath(ir, input, keeps_order) else {
			return Ok(false);
		};
		let Some(PhysOp::KeyRangeScan(scan)) = phys(ir, chain.source) else {
			return Ok(false);
		};
		let access = &scan.access;
		let DeliveredKeyOrder::Coarse {
			position,
		} = access.delivers
		else {
			return Ok(false);
		};
		if scan.topk.is_some()
			|| first.ascending != (access.direction == Direction::Forward)
			|| sort_key_component(ir, op, attrs, 0, &access.key, &chain.crossed) != Some(position)
		{
			return Ok(false);
		}
		let Ok(keep) = u64::try_from(keep) else {
			return Ok(false);
		};
		let key = TopKKey {
			path: std::sync::Arc::from([]),
			direction: if first.ascending {
				SortDirection::Asc
			} else {
				SortDirection::Desc
			},
			single_key: attrs.keys.len() == 1,
			depth_limit: 0,
			coarse_cutoff: Some(CoarseCutoff {
				position,
				keep,
			}),
		};
		let scan = chain.source;
		rw.update_kind(scan, |kind| {
			if let OpKind::Phys(PhysOp::KeyRangeScan(attrs)) = kind {
				attrs.topk = Some(key);
			}
		})?;
		rw.update_kind(op, |kind| {
			if let OpKind::Phys(PhysOp::Sort(attrs)) = kind {
				attrs.pushdown = true;
			}
		})?;
		Ok(true)
	}
}

/// The ops a record lookup's answer is read through: a layer, a permission and table enforcement.
/// Each keeps a row's `id`, which is the only field the lookup answers.
fn keeps_id(kind: &PhysOp) -> bool {
	matches!(kind, PhysOp::Layer(_) | PhysOp::PermissionFilter(_) | PhysOp::Enforced { .. })
}

/// A conjunct that the record lookup beneath a predicate answers is dropped.
///
/// `id = r` over the point lookup of the
/// literal record `r` holds of the one row the lookup can yield, so a total conjunct of that form
/// neither rejects a row nor raises, and its absence changes nothing the filter does. A table's
/// permission, layers and enforcement keep a row's `id`, so they may stand between the filter and
/// the lookup; another filter does not, since the conjunct it holds may judge the row first. A
/// predicate left with nothing to judge removes the filter. The conjuncts that stay keep their
/// order and their laziness.
struct AnsweredByLookup;

impl Pattern<PhysDialect> for AnsweredByLookup {
	fn name(&self) -> &'static str {
		"answered_by_lookup"
	}

	fn root(&self) -> PhysTag {
		PhysTag::Filter
	}

	fn benefit(&self) -> u16 {
		16
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let ir = rw.ir();
		let Some(site) = conjunct::site(ir, op) else {
			return Ok(false);
		};
		let Some(chain) = beneath(ir, site.input, keeps_id) else {
			return Ok(false);
		};
		let Some(PhysOp::RecordScan(..)) = phys(ir, chain.source) else {
			return Ok(false);
		};
		let Some(record) = ir
			.op(chain.source)
			.and_then(|data| data.operands.first().copied())
			.and_then(|value| conjunct::literal_record(ir, value))
		else {
			return Ok(false);
		};
		let leaves = conjunct::leaves(ir, &site);
		let dropped: Vec<ValueId> = leaves
			.iter()
			.copied()
			.filter(|leaf| {
				conjunct::judge(ir, *leaf).total && conjunct::pins_record(ir, *leaf, &record)
			})
			.collect();
		if dropped.is_empty() {
			return Ok(false);
		}
		conjunct::drop_conjuncts(rw, &site, &leaves, &dropped)
	}
}

/// The ops an exact index scan's answer is read through: a permission alone. It keeps a row's
/// fields as stored, which the scan's columns were keyed by; a layer or table enforcement may
/// give a field another value than the one the index holds.
fn keeps_stored_fields(kind: &PhysOp) -> bool {
	matches!(kind, PhysOp::PermissionFilter(_))
}

/// A conjunct that the exact B-tree scan beneath a predicate answers is dropped.
///
/// An exact scan lists precisely the
/// records whose pinned columns satisfy the conjuncts its access was chosen from, so a conjunct
/// that states one of those comparisons ([`index_answer::answers`]) judges no row the scan kept.
/// Where the scan answers every conjunct the whole predicate goes; otherwise only the answered
/// conjuncts that are total ([`conjunct::judge`]) go, and one that may raise, such as a membership
/// in a range the predicate builds, stays where it is judged.
/// Only the table's permission may stand between the filter and the scan, since it keeps a row's
/// fields as stored; another filter does not, since the conjunct it holds may judge the row first.
/// A predicate left with nothing to judge removes the filter, and one that also holds a predicate
/// judged once keeps it as the literal `true`. The conjuncts that stay keep their order and their
/// laziness; an index operator (`MATCHES`, `KNN`) in a conjunct that stays keeps the decisions
/// the physical plan made for it, which follow the op through the rebuild.
struct AnsweredByIndex;

impl Pattern<PhysDialect> for AnsweredByIndex {
	fn name(&self) -> &'static str {
		"answered_by_index"
	}

	fn root(&self) -> PhysTag {
		PhysTag::Filter
	}

	fn benefit(&self) -> u16 {
		14
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let ir = rw.ir();
		let Some(site) = conjunct::site(ir, op) else {
			return Ok(false);
		};
		let Some(chain) = beneath(ir, site.input, keeps_stored_fields) else {
			return Ok(false);
		};
		let Some(PhysOp::IndexScan(scan)) = phys(ir, chain.source) else {
			return Ok(false);
		};
		if !scan.exact() {
			return Ok(false);
		}
		let Some(cols) = scan.stated.as_deref() else {
			return Ok(false);
		};
		let leaves = conjunct::leaves(ir, &site);
		let answered: Vec<ValueId> = leaves
			.iter()
			.copied()
			.filter(|leaf| index_answer::answers(ir, *leaf, &scan.access, cols))
			.collect();
		// The scan answers the whole predicate, or the conjuncts that neither raise nor have an
		// effect alone among those it answers.
		let dropped: Vec<ValueId> = match answered.len() == leaves.len() {
			true => answered,
			false => answered.into_iter().filter(|leaf| conjunct::judge(ir, *leaf).total).collect(),
		};
		if dropped.is_empty() {
			return Ok(false);
		}
		conjunct::drop_conjuncts(rw, &site, &leaves, &dropped)
	}
}

/// A conjunct that is the probe of the full-text scan beneath a predicate is dropped.
///
/// A full-text scan lists exactly the records whose indexed column its query matches under its
/// operator, so its probe ([`index_answer::answers_search`]) holds of every row it lists. The ops
/// between the filter and the scan pass the scan's rows on in order ([`keeps_order`]); each filter
/// among them drops the probe from its own predicate. A predicate left with nothing to judge
/// removes the filter, and one that also holds a predicate judged once keeps it as the literal
/// `true`. The conjuncts that stay keep their order and their laziness.
struct AnsweredBySearch;

impl Pattern<PhysDialect> for AnsweredBySearch {
	fn name(&self) -> &'static str {
		"answered_by_search"
	}

	fn root(&self) -> PhysTag {
		PhysTag::Filter
	}

	fn benefit(&self) -> u16 {
		14
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let ir = rw.ir();
		let Some(site) = conjunct::site(ir, op) else {
			return Ok(false);
		};
		let Some(chain) = beneath(ir, site.input, keeps_order) else {
			return Ok(false);
		};
		let Some(PhysOp::FullTextScan(scan)) = phys(ir, chain.source) else {
			return Ok(false);
		};
		let Some(column) = scan.column.as_ref() else {
			return Ok(false);
		};
		let leaves = conjunct::leaves(ir, &site);
		let dropped: Vec<ValueId> = leaves
			.iter()
			.copied()
			.filter(|leaf| {
				index_answer::answers_search(ir, *leaf, column, &scan.operator, &scan.query)
			})
			.collect();
		if dropped.is_empty() {
			return Ok(false);
		}
		conjunct::drop_conjuncts(rw, &site, &leaves, &dropped)
	}
}

/// The conjuncts of a predicate are ordered by cost class.
///
/// Within each run of total conjuncts, field
/// comparisons come before builtin calls; a conjunct that may raise or has an effect keeps its
/// place and none moves across it, so a row raises, short-circuits and is kept exactly as before.
/// Only the predicate of a [`PhysOp::Filter`] is ordered: a permission's conjuncts and the
/// predicate judged once are not, and a conjunct never crosses from one filter to another.
struct ConjunctOrder;

impl Pattern<PhysDialect> for ConjunctOrder {
	fn name(&self) -> &'static str {
		"conjunct_order"
	}

	fn root(&self) -> PhysTag {
		PhysTag::Filter
	}

	fn benefit(&self) -> u16 {
		12
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let ir = rw.ir();
		let Some(site) = conjunct::site(ir, op) else {
			return Ok(false);
		};
		let leaves = conjunct::leaves(ir, &site);
		let judged: Vec<conjunct::Judgement> =
			leaves.iter().map(|leaf| conjunct::judge(ir, *leaf)).collect();
		let Some(order) = conjunct::ordered(&leaves, &judged) else {
			return Ok(false);
		};
		conjunct::reorder(rw, &site, &order)
	}
}
