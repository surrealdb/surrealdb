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
use super::ops::{
	BranchReadShape, KeyEqShape, KeyRangeShape, PhysOp, PhysTag, ScoreSum, SortAttrs,
	SortKeyReadShape,
};
use super::placement::{Placement, install, place_all_reading};
use super::placement_attr::PlacementAttr;
use super::{
	chain, conjunct, hold_left, hop_key, index_answer, io, join_lookup, knn_ids, nested, once,
	prefilter, recheck, topk_packed,
};
use crate::catalog::KeyStructure;
use crate::exec::field_path::{FieldPath, FieldPathPart};
use crate::exec::index::key_range::DeliveredKeyOrder;
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::pattern::{GREEDY_DEFAULT, GreedyReport, Pattern, apply_greedy};
use crate::exec::ir::core::rewriter::Rewriter;
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::ExprOp;
use crate::exec::ir::iface::catalog::PhysCx;
use crate::exec::ir::iface::conversion::{Phase, PhysDialect};
use crate::exec::ir::iface::fold::PlanTimeFolder;
use crate::exec::ir::iface::ids::{OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::registry;
use crate::exec::operators::SortDirection;
use crate::exec::topk_pushdown::{field_path_wire_segments, reads_key_component};
use crate::exec::vm::payload::{CoarseCutoff, Spill, TopKKey};
use crate::kvs::Direction;
use crate::val::{Number, RecordId, Value};

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
		Box::new(AnsweredByUnion),
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
/// ([`hop_key::run`]). Last, each scan that holds a top-k key states whether it may reject a packed
/// row unread, from the ops between it and its sort ([`topk_packed::run`]).
///
/// Then every pipeline is placed again through `placement`, using the facts of the container the
/// rules left and the strategies ([`NestedReading::Strategy`]). The new placements replace the
/// ones [`super::plan_physical`] installed. The facts of a pipeline come from the sorts the rules
/// removed, the limits they bounded and the statements that took an arm. Placement judges whether
/// each pipeline that does not prune by its legality prunes by score ([`prunes_by_score`]) before
/// it decides the pipeline, so it holds serial exactly the full-text pipelines whose scan keeps
/// rejecting hits. A pipeline whose placement does not prune has its scan reject no entry by a
/// top-k threshold ([`settle_thresholds`]).
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
	topk_packed::run(ir, facts, &cx.effects)?;
	let placed = place_all_reading(
		ir,
		facts,
		cx.effects.registry,
		placement,
		cx.physical.backend(),
		NestedReading::Strategy,
		&mut |ir, facts, pipeline, physical| {
			prunes_by_score(ir, facts, &cx.effects, pipeline, physical)
		},
	)?;
	settle_thresholds(ir, facts, &placed)?;
	install(ir.placement_table_mut(), placed);
	io::run(ir, facts, cx)?;
	Ok(report)
}

/// States that the scan of every pipeline whose placement does not prune rejects no entry by a
/// top-k threshold ([`TopKKey::rejects_entries`]).
///
/// A scan that rejects an entry whose key the sort's threshold shows worse than every row the sort
/// keeps leaves the entry's body unrun. The serial run runs the body of every entry, so only a
/// pipeline that raises for no row may leave one unrun: the placement of each pipeline in `placed`
/// states whether it prunes, by its legality or by score alone ([`PlacementAttr::prunes`],
/// [`PlacementAttr::prunes_by_score`]). The plan keeps the key, which `EXPLAIN` shows as written; a
/// scan that a datetime component of its keys cuts keeps its cutoff, which stops the scan and
/// rejects no single entry.
fn settle_thresholds(
	ir: &mut Ir,
	facts: &mut FactCache,
	placed: &[(OpId, PlacementAttr)],
) -> Result<(), IrError> {
	let mut keyed = Vec::new();
	for (pipeline, attr) in placed {
		if attr.prunes() || attr.prunes_by_score() {
			continue;
		}
		let Some(source) = super::facts::source_of(ir, *pipeline) else {
			continue;
		};
		let key = match phys(ir, source) {
			Some(PhysOp::TableScan(attrs)) => attrs.topk.as_ref(),
			Some(PhysOp::KeyRangeScan(attrs)) => attrs.topk.as_ref(),
			Some(PhysOp::FullTextScan(attrs)) => attrs.topk.as_ref(),
			Some(PhysOp::IndexUnion(attrs)) => attrs.topk.as_ref().map(|(key, _)| key),
			Some(_) | None => None,
		};
		let keep_coarse = key.is_some_and(|key| key.coarse_cutoff.is_some())
			&& pipeline_keeps_coarse_cutoff(ir, *pipeline, source);
		if key
			.is_some_and(|key| key.rejects_entries || (key.coarse_cutoff.is_some() && !keep_coarse))
		{
			keyed.push((source, keep_coarse));
		}
	}
	let mut rw = Rewriter::new(ir, facts);
	for (scan, keep_coarse) in keyed {
		rw.update_kind(scan, |kind| {
			let key = match kind {
				OpKind::Phys(PhysOp::TableScan(attrs)) => attrs.topk.as_mut(),
				OpKind::Phys(PhysOp::KeyRangeScan(attrs)) => attrs.topk.as_mut(),
				OpKind::Phys(PhysOp::FullTextScan(attrs)) => attrs.topk.as_mut(),
				OpKind::Phys(PhysOp::IndexUnion(attrs)) => attrs.topk.as_mut().map(|(key, _)| key),
				OpKind::Phys(_) | OpKind::Logical(_) | OpKind::Expr(_) => None,
			};
			if let Some(key) = key {
				key.rejects_entries = false;
				if !keep_coarse {
					key.coarse_cutoff = None;
				}
			}
		})?;
	}
	Ok(())
}

/// Whether `pipeline` holds a sort over `source` whose coarse cutoff is safe to keep because no
/// crossed op or sort key between `source` and the sort can raise or have an effect.
fn pipeline_keeps_coarse_cutoff(ir: &Ir, _pipeline: OpId, source: OpId) -> bool {
	let Some(PhysOp::KeyRangeScan(scan)) = phys(ir, source) else {
		return false;
	};
	let mut sorts = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(phys(ir, op), Some(PhysOp::Sort(_))) {
			sorts.push(op);
		}
		Walk::Continue
	});
	sorts.into_iter().any(|sort| {
		let Some(PhysOp::Sort(attrs)) = phys(ir, sort) else {
			return false;
		};
		let Some(input) = ir.op(sort).and_then(|data| data.operands.first().copied()) else {
			return false;
		};
		let Some(chain) = beneath(ir, input, keeps_order) else {
			return false;
		};
		chain.source == source
			&& coarse_cutoff_safe(ir, sort, attrs, &scan.access.key, &chain.crossed)
	})
}

/// Whether the full-text scan or index union of `pipeline`, which does not prune, may still reject
/// a hit by the threshold of the sort ordered first by a sum of clause scores
/// ([`PlacementAttr::prunes_by_score`], [`score_target`]).
///
/// A rejected hit skips its record's read and the ops between the scan and the sort: the
/// predicates crossed and the sort's keys. It may be skipped when none of those raises for the
/// hit. Every crossed predicate and every key after the first must raise for no row; the first
/// key is the sum of `search::score` calls whose value the scan computed for the hit, every score
/// a number, so it can raise nothing the scan did not raise first. The logical legality is not
/// read: it still counts the `MATCHES` the scan answers, which no op of the pipeline evaluates. A
/// pipeline holding a block or a nested statement is refused, as for [`PlacementAttr::prunes`].
fn prunes_by_score(
	ir: &Ir,
	facts: &mut FactCache,
	effects: &crate::exec::ir::iface::catalog::EffectCx<'_>,
	pipeline: OpId,
	physical: &crate::exec::planner::placement::PhysicalFacts,
) -> bool {
	if physical.host_blocks || physical.nested != crate::exec::planner::placement::NestedUse::None {
		return false;
	}
	let Some(source) = super::facts::source_of(ir, pipeline) else {
		return false;
	};
	let keyed = match phys(ir, source) {
		Some(PhysOp::FullTextScan(scan)) => scan.topk.is_some(),
		Some(PhysOp::IndexUnion(union)) => union.topk.is_some(),
		Some(_) | None => false,
	};
	if !keyed {
		return false;
	}
	let mut sorts = Vec::new();
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
		if matches!(phys(ir, op), Some(PhysOp::Sort(_))) {
			sorts.push(op);
		}
		Walk::Continue
	});
	sorts.into_iter().any(|sort| {
		let Some(PhysOp::Sort(attrs)) = phys(ir, sort) else {
			return false;
		};
		let Some((target, _, computing)) = score_target(ir, sort, attrs) else {
			return false;
		};
		if target.op() != source {
			return false;
		}
		let raises = |facts: &mut FactCache, op: OpId| {
			!crate::exec::ir::analysis::fallibility::of_with(ir, facts, effects, op).is_infallible()
		};
		let Some(chain) = ir
			.op(sort)
			.and_then(|data| data.operands.first().copied())
			.and_then(|input| beneath(ir, input, keeps_scan))
		else {
			return false;
		};
		if chain.crossed.iter().any(|op| raises(facts, *op)) {
			return false;
		}
		let regions = ir.op(sort).map(|data| data.regions.clone()).unwrap_or_default();
		regions.iter().all(|region| {
			ops_in_region(ir, *region)
				.iter()
				.all(|op| computing.contains(op) || !raises(facts, *op))
		})
	})
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
pub(super) fn keeps_scan(kind: &PhysOp) -> bool {
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
	match phys(ir, chain.source) {
		Some(PhysOp::TableScan(scan)) => {
			scan.id_order
				&& !layer_writes_field(ir, &chain.crossed, &FieldPath::field("id"))
				&& keyed_by_id(ir, sort, attrs, scan.direction == Direction::Forward)
		}
		Some(PhysOp::KeyRangeScan(scan)) => {
			let access = &scan.access;
			let forward = access.direction == Direction::Forward;
			(access.key.all_exactly_ordered()
				&& !layer_writes_field(ir, &chain.crossed, &FieldPath::field("id"))
				&& keyed_by_id(ir, sort, attrs, forward))
				|| key_range_delivers(ir, sort, attrs, access, &chain.crossed)
		}
		Some(PhysOp::IndexScan(scan)) => {
			let Some(fields) = scan.ordered_by.as_deref() else {
				return false;
			};
			if !reads_exactly(ir, sort, attrs, fields)
				|| fields.iter().any(|field| layer_writes_field(ir, &chain.crossed, field))
			{
				return false;
			}
			let forward = scan.direction == Direction::Forward;
			let eq_len = match &scan.access {
				crate::exec::index::access_path::BTreeAccess::Equality(_) => {
					usize::from(scan.pinned.is_some() || scan.stated.is_some())
				}
				crate::exec::index::access_path::BTreeAccess::Compound {
					prefix,
					..
				} => prefix.len(),
				crate::exec::index::access_path::BTreeAccess::Range {
					..
				}
				| crate::exec::index::access_path::BTreeAccess::FullText {
					..
				}
				| crate::exec::index::access_path::BTreeAccess::Knn {
					..
				} => 0,
			};
			if eq_len > 0 && scan.pinned.is_none() && scan.stated.is_none() {
				return false;
			}
			let eq_pinned: Vec<FieldPath> = match scan.pinned.as_deref() {
				Some(pinned) => pinned.get(..eq_len).unwrap_or(&[]).to_vec(),
				None => {
					let Some(stated) = scan.stated.as_deref().and_then(|s| s.get(..eq_len)) else {
						return false;
					};
					stated
						.iter()
						.filter_map(|col| {
							crate::exec::field_path_convert::field_path_from_idiom(col).ok()
						})
						.collect()
				}
			};
			if !attrs
				.keys
				.iter()
				.zip(fields)
				.all(|(key, field)| eq_pinned.contains(field) || key.ascending == forward)
			{
				return false;
			}
			if let Some(stated) = scan.stated.as_deref() {
				let rest: Vec<&FieldPath> =
					fields.iter().skip_while(|field| eq_pinned.contains(field)).collect();
				let stated_paths: Option<Vec<FieldPath>> = stated
					.iter()
					.skip(eq_len)
					.map(|col| crate::exec::field_path_convert::field_path_from_idiom(col).ok())
					.collect();
				let Some(mut allowed) = stated_paths else {
					return false;
				};
				allowed.push(FieldPath::field("id"));
				if rest.len() > allowed.len()
					|| !rest.iter().zip(&allowed).all(|(field, col)| **field == *col)
				{
					return false;
				}
			}
			true
		}
		Some(PhysOp::IndexUnion(union)) => {
			let Some(fields) = union.ordered_by.as_deref() else {
				return false;
			};
			reads_exactly(ir, sort, attrs, fields)
				&& !fields.iter().any(|field| layer_writes_field(ir, &chain.crossed, field))
				&& union.branches.first().is_none_or(|branch| {
					let forward = branch.direction == Direction::Forward;
					attrs.keys.iter().all(|key| key.ascending == forward)
				})
		}
		Some(_) | None => false,
	}
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

/// Whether any layer among `crossed` writes to the root field of `path`, shadowing the stored
/// value on the row. A column of the key part derives the record id component the index already
/// orders by, and does not shadow it.
fn layer_writes_field(ir: &Ir, crossed: &[OpId], path: &FieldPath) -> bool {
	let Some(FieldPathPart::Field(root)) = path.0.first() else {
		return false;
	};
	crossed.iter().any(|layer| {
		let Some(PhysOp::Layer(attrs)) = phys(ir, *layer) else {
			return false;
		};
		match &attrs.fields {
			FieldsShape::Value {
				..
			} => true,
			FieldsShape::Select(fields) => {
				let mut unaliased = attrs.names.iter();
				let mut nth = 0usize;
				fields.iter().any(|field| match field {
					FieldShape::Single {
						alias,
						..
					} => {
						let col_nth = nth;
						nth += 1;
						let out = alias.as_ref().or_else(|| unaliased.next());
						out.and_then(|out| out.parts.first()).is_some_and(|part| {
							matches!(
								part,
								crate::expr::logical::OutputPathPart::Field(name)
									if name.as_str() == root.as_str()
							)
						}) && !layer_column_is_key_part(ir, *layer, col_nth)
					}
					FieldShape::All => false,
				})
			}
		}
	})
}

/// Whether the `nth` column of the layer `layer` is evaluated under a key-part frame.
fn layer_column_is_key_part(ir: &Ir, layer: OpId, nth: usize) -> bool {
	let Some(region) = ir
		.op(layer)
		.into_iter()
		.flat_map(|data| data.regions.iter().copied())
		.filter(|region| {
			ir.region(*region)
				.is_some_and(|data| data.kind == RegionKind::FieldFrame(FieldRole::Computed))
		})
		.nth(nth)
	else {
		return false;
	};
	let Some(first) = ops_in_region(ir, region).first().copied() else {
		return false;
	};
	ir.op(first).and_then(|op| ir.frame(op.frame)).is_some_and(|frame| frame.eval.key_part)
}

/// Whether the keys of the sort `op` read the field paths `fields`, in order, as plain paths
/// without `COLLATE` or `NUMERIC`: a key that computes anything else reads a value no stored
/// field holds.
fn reads_exactly(ir: &Ir, op: OpId, sort: &SortAttrs, fields: &[FieldPath]) -> bool {
	sort.keys.len() == fields.len()
		&& sort.keys.iter().zip(fields).enumerate().all(|(at, (key, field))| {
			!key.collate
				&& !key.numeric
				&& key_path(ir, op, sort, at).is_some_and(|path| path == *field)
		})
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
		packed_rows: false,
		coarse_cutoff: None,
		rejects_entries: true,
	})
}

/// What answers the first key of a bounded sort when that key is a sum of full-text clause
/// scores.
#[derive(Clone, Debug)]
pub(super) enum ScoreTarget {
	/// The full-text scan of the one clause the key scores.
	Scan(OpId),
	/// The index union whose full-text branches answer every clause the sum scores.
	Union(OpId, ScoreSum),
}

impl ScoreTarget {
	/// The op that rejects hits by the key.
	pub(super) fn op(&self) -> OpId {
		match self {
			Self::Scan(op) | Self::Union(op, _) => *op,
		}
	}
}

/// What answers the first key of the sort `op` beneath it, with the top-k key it rejects hits by
/// and the ops of the key's region that compute the score.
///
/// The first key must be a sum of `search::score(N)` calls with literal clauses, a lone call
/// included ([`score_sum`]), with no `COLLATE` or `NUMERIC`. Reached through predicates alone, it
/// is answered by:
/// - the full-text scan of clause `N`, where the key is that clause's score alone;
/// - an index union of full-text branches over the table's shared doc ids, merging nothing, where
///   every clause the sum scores is a branch's. A branch scores each hit under every such clause, a
///   clause the hit does not match scoring zero as `search::score` does, and adds the scores as the
///   key adds them.
///
/// The scan computes the very value the sort orders by before reading a hit's record
/// ([`crate::exec::function::MatchContext::scores`]), so it compares that value against the
/// sort's threshold.
fn score_target(ir: &Ir, op: OpId, sort: &SortAttrs) -> Option<(ScoreTarget, TopKKey, Vec<OpId>)> {
	let first = sort.keys.first()?;
	if first.collate || first.numeric || !matches!(first.read, SortKeyReadShape::Computed) {
		return None;
	}
	let (sum, computing) = score_sum(ir, *ir.op(op)?.regions.first()?)?;
	let input = ir.op(op)?.operands.first().copied()?;
	let chain = beneath(ir, input, keeps_scan)?;
	let source = phys(ir, chain.source)?;
	let target = if let PhysOp::FullTextScan(scan) = source {
		match sum {
			ScoreSum::Clause(clause) if scan.operator.rf == Some(clause) => {
				ScoreTarget::Scan(chain.source)
			}
			ScoreSum::Clause(_) | ScoreSum::Add(..) => return None,
		}
	} else if let PhysOp::IndexUnion(union) = source {
		if union.merge.is_some() {
			return None;
		}
		{
			let mut answered = Vec::with_capacity(union.branches.len());
			for branch in &union.branches {
				let BranchReadShape::FullText {
					operator,
					shared_doc_ids: true,
					..
				} = &branch.read
				else {
					return None;
				};
				answered.push(operator.rf?);
			}
			let mut scored = Vec::new();
			sum.clauses(&mut scored);
			if !scored.iter().all(|clause| answered.contains(clause)) {
				return None;
			}
			// One branch per reference, so the branch answering a summed clause is the one
			// branch carrying its reference.
			let mut distinct = answered.clone();
			distinct.sort_unstable();
			distinct.dedup();
			if distinct.len() != answered.len() {
				return None;
			}
			ScoreTarget::Union(chain.source, sum)
		}
	} else {
		return None;
	};
	// Each summed call scores the clause its reference resolves to, the last `MATCHES` written
	// with it ([`super::matches::IndexScope::resolve`]). That must be the clause the scan or the
	// branch carrying the reference answers: its column and its literal query. A reference
	// written on several `MATCHES` may otherwise name one clause to the sort and another to the
	// scan, which would then reject hits by a score the sort does not order by.
	for call in computing.iter().copied().filter(|op| is_score_call(ir, *op)) {
		let rf = score_clause_of(ir, call)?;
		let clause = ir.index_scope(call)?.resolve(rf)?;
		let super::matches::ClauseQuery::Text(text) = &clause.query else {
			return None;
		};
		let (column, query) = answering(ir, &target, rf)?;
		if text != query || column_path(column)? != *clause.field.as_ref()? {
			return None;
		}
	}
	Some((
		target,
		TopKKey {
			path: std::sync::Arc::from([]),
			direction: if first.ascending {
				SortDirection::Asc
			} else {
				SortDirection::Desc
			},
			single_key: sort.keys.len() == 1,
			depth_limit: 0,
			packed_rows: false,
			coarse_cutoff: None,
			rejects_entries: true,
		},
		computing,
	))
}

/// The column and the query of the scan or union branch of `target` that answers reference `rf`.
fn answering<'a>(
	ir: &'a Ir,
	target: &ScoreTarget,
	rf: crate::expr::operator::MatchRef,
) -> Option<(&'a crate::expr::Idiom, &'a str)> {
	match target {
		ScoreTarget::Scan(op) => {
			let Some(PhysOp::FullTextScan(scan)) = phys(ir, *op) else {
				return None;
			};
			Some((scan.column.as_ref()?, scan.query.as_str()))
		}
		ScoreTarget::Union(op, _) => {
			let Some(PhysOp::IndexUnion(union)) = phys(ir, *op) else {
				return None;
			};
			union.branches.iter().find_map(|branch| match &branch.read {
				BranchReadShape::FullText {
					query,
					operator,
					column: Some(column),
					..
				} if operator.rf == Some(rf) => Some((column, query.as_str())),
				BranchReadShape::FullText {
					..
				}
				| BranchReadShape::BTree(_) => None,
			})
		}
	}
}

/// The path a `MATCHES` clause over `column` reads off the row
/// ([`super::matches::field_path`]), when `column` is a chain of field names.
fn column_path(column: &crate::expr::Idiom) -> Option<crate::expr::logical::FieldPath> {
	let name = |part: &crate::expr::Part| {
		if let crate::expr::Part::Field(name) = part {
			Some(name.clone())
		} else {
			None
		}
	};
	let (first, rest) = column.0.split_first()?;
	rest.iter().try_fold(crate::expr::logical::FieldPath::field(name(first)?), |path, part| {
		Some(path.child(crate::expr::logical::FieldSegment::Field(name(part)?)))
	})
}

/// The sum of clause scores a region yields, when it yields exactly one: `search::score(N)` calls
/// with literal clauses ([`score_call`]) joined by `+`, shaped as written. Answers the sum with
/// the ops that compute it, every call and every addition.
fn score_sum(ir: &Ir, region: RegionId) -> Option<(ScoreSum, Vec<OpId>)> {
	let last = ops_in_region(ir, region).last().copied()?;
	let closing = ir.op(last)?;
	let (OpKind::Expr(ExprOp::Yield), [value]) = (&closing.kind, closing.operands.as_slice())
	else {
		return None;
	};
	let mut computing = Vec::new();
	let sum = score_term(ir, *value, &mut computing, SCORE_SUM_DEPTH)?;
	Some((sum, computing))
}

/// How many additions deep a sum of clause scores is read.
const SCORE_SUM_DEPTH: usize = 16;

/// The sum of clause scores `value` is, adding the ops computing it to `computing`.
fn score_term(
	ir: &Ir,
	value: ValueId,
	computing: &mut Vec<OpId>,
	depth: usize,
) -> Option<ScoreSum> {
	let depth = depth.checked_sub(1)?;
	let op = conjunct::def_op(ir, value)?;
	let data = ir.op(op)?;
	if let (
		OpKind::Expr(ExprOp::Binary {
			op: crate::expr::BinaryOperator::Add,
			..
		}),
		[left, right],
	) = (&data.kind, data.operands.as_slice())
	{
		computing.push(op);
		let left = score_term(ir, *left, computing, depth)?;
		let right = score_term(ir, *right, computing, depth)?;
		return Some(ScoreSum::Add(Box::new(left), Box::new(right)));
	}
	let clause = score_clause_of(ir, op)?;
	computing.push(op);
	Some(ScoreSum::Clause(clause))
}

/// The clause `N` of the call `op` when it is `search::score(N)` with a literal `N` and no other
/// argument ([`is_score_call`]).
fn score_clause_of(ir: &Ir, op: OpId) -> Option<crate::expr::operator::MatchRef> {
	let call = ir.op(op)?;
	if !is_score_call(ir, op) {
		return None;
	}
	u8::try_from(literal_count(ir, *call.operands.first()?)?).ok()
}

/// Whether `op` is a `search::score` call with one argument, its clause, beside the row names
/// every index function is passed. A call with more arguments raises however it is answered, so
/// it is not the call a scan answers.
fn is_score_call(ir: &Ir, op: OpId) -> bool {
	let Some(data) = ir.op(op) else {
		return false;
	};
	let OpKind::Expr(ExprOp::Call(attrs)) = &data.kind else {
		return false;
	};
	matches!(&attrs.function, crate::expr::Function::Normal(name) if name == SCORE_FUNCTION)
		&& data.operands.len() == 1 + usize::from(attrs.row_names)
}

/// The index function whose value a full-text scan computes for each hit it lists.
const SCORE_FUNCTION: &str = "search::score";

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
/// as it is. A scan rejects entries by the key only where the placement of its
/// pipeline prunes ([`settle_thresholds`]).
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
		// A sort ordered first by a full-text clause's score has the scan answering the clause
		// reject hits by it, before their records are read.
		let score = key.is_none().then(|| score_target(ir, sort, attrs)).flatten();
		let score_takes = score.clone().filter(|(target, ..)| match phys(ir, target.op()) {
			Some(PhysOp::FullTextScan(attrs)) => attrs.topk.is_none(),
			Some(PhysOp::IndexUnion(attrs)) => attrs.topk.is_none(),
			Some(_) | None => false,
		});
		let pushdown = key.is_some() || score.is_some();
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
		if attrs.keep == Some(keep)
			&& attrs.pushdown == pushdown
			&& scan_takes.is_none()
			&& score_takes.is_none()
		{
			return Ok(false);
		}
		if let Some((target, key, _)) = score_takes {
			rw.update_kind(target.op(), |kind| {
				if let (OpKind::Phys(PhysOp::FullTextScan(attrs)), ScoreTarget::Scan(_)) =
					(&mut *kind, &target)
				{
					attrs.topk = Some(key.clone());
				} else if let (
					OpKind::Phys(PhysOp::IndexUnion(attrs)),
					ScoreTarget::Union(_, sum),
				) = (kind, &target)
				{
					attrs.topk = Some((key.clone(), sum.clone()));
				}
			})?;
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
			|| keep == 0
			|| first.ascending != (access.direction == Direction::Forward)
			|| sort_key_component(ir, op, attrs, 0, &access.key, &chain.crossed) != Some(position)
			|| !coarse_cutoff_safe(ir, op, attrs, &access.key, &chain.crossed)
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
			packed_rows: false,
			coarse_cutoff: Some(CoarseCutoff {
				position,
				keep,
			}),
			rejects_entries: true,
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

/// Whether the ops crossed between a key-range scan over `key` and the sort `sort`, and the key
/// regions of `sort` itself, are total and free of effects over rows of `key`.
fn coarse_cutoff_safe(
	ir: &Ir,
	sort: OpId,
	_attrs: &SortAttrs,
	key: &KeyStructure,
	crossed: &[OpId],
) -> bool {
	if layer_writes_field(ir, crossed, &FieldPath::field("id")) {
		return false;
	}
	let crossed_safe = crossed.iter().all(|op| {
		let Some(data) = ir.op(*op) else {
			return false;
		};
		match phys(ir, *op) {
			Some(PhysOp::Filter) => {
				!data.regions.is_empty()
					&& data
						.regions
						.iter()
						.all(|region| region_is_total_on_key_range(ir, *region, *region, key))
			}
			Some(PhysOp::Layer(layer_attrs)) => {
				if layer_attrs.judged.is_some()
					|| !matches!(layer_attrs.fields, FieldsShape::Select(_))
				{
					return false;
				}
				data.regions.iter().all(|region| {
					let Some(info) = ir.region(*region) else {
						return false;
					};
					if info.kind == RegionKind::FieldFrame(FieldRole::Computed) {
						let is_key_part = ops_in_region(ir, *region)
							.first()
							.and_then(|first| ir.op(*first))
							.and_then(|op| ir.frame(op.frame))
							.is_some_and(|frame| frame.eval.key_part)
							&& component_read(ir, *region, key, true).is_some();
						is_key_part || region_is_total_on_key_range(ir, *region, *region, key)
					} else if info.kind == RegionKind::Clause(ClauseRole::Projection) {
						region_is_total_on_key_range(ir, *region, *region, key)
					} else {
						false
					}
				})
			}
			Some(PhysOp::Enforced {
				..
			}) => true,
			Some(_) | None => false,
		}
	});
	if !crossed_safe {
		return false;
	}
	let Some(sort_data) = ir.op(sort) else {
		return false;
	};
	sort_data.regions.iter().all(|region| region_is_total_on_key_range(ir, *region, *region, key))
}

/// Whether every op in `region` (whose outer row-binding region is `root_region`) is total and
/// free of effects over rows whose record id has the structure `key`.
fn region_is_total_on_key_range(
	ir: &Ir,
	region: RegionId,
	root_region: RegionId,
	key: &KeyStructure,
) -> bool {
	let ops = ops_in_region(ir, region);
	!ops.is_empty()
		&& ops.iter().all(|op| {
			let Some(data) = ir.op(*op) else {
				return false;
			};
			let OpKind::Expr(expr) = &data.kind else {
				return false;
			};
			if !data.regions.is_empty() {
				return matches!(expr, ExprOp::ShortCircuit { .. })
					&& data.regions.iter().all(|sub| {
						ir.region(*sub).is_some_and(|info| info.kind == RegionKind::LazyOperand)
							&& region_is_total_on_key_range(ir, *sub, root_region, key)
					});
			}
			if matches!(expr, ExprOp::Yield) {
				true
			} else if matches!(expr, ExprOp::Field { .. } | ExprOp::Doc) {
				data.operands.first().is_some_and(|row| is_row_of(ir, *row, root_region))
			} else if matches!(expr, ExprOp::Index) {
				matches!(
					data.operands.as_slice(),
					[base, index]
						if reads_id_of_row(ir, *base, root_region)
							&& literal_count(ir, *index).is_some_and(|i| i < key.arity())
				)
			} else if matches!(expr, ExprOp::First) {
				matches!(
					data.operands.as_slice(),
					[base] if reads_id_of_row(ir, *base, root_region) && key.arity() > 0
				)
			} else {
				conjunct::is_total_form(ir, data)
			}
		})
}

/// The ops a record lookup's answer is read through: a layer, a permission and table enforcement.
/// Each keeps a row's `id`, which is the only field the lookup answers.
fn keeps_id(kind: &PhysOp) -> bool {
	matches!(kind, PhysOp::Layer(_) | PhysOp::PermissionFilter(_) | PhysOp::Enforced { .. })
}

/// The record ids a record read over a list reads: `value` is an array literal of record ids.
fn literal_record_list(ir: &Ir, value: ValueId) -> Option<Vec<RecordId>> {
	let data = ir.op(conjunct::def_op(ir, value)?)?;
	let OpKind::Expr(ExprOp::Literal(held)) = &data.kind else {
		return None;
	};
	let Value::Array(items) = &**held else {
		return None;
	};
	items
		.iter()
		.map(|item| {
			if let Value::RecordId(record) = item {
				Some(record.clone())
			} else {
				None
			}
		})
		.collect()
}

/// A conjunct that the record lookup beneath a predicate answers is dropped.
///
/// `id = r` over the point lookup of the
/// literal record `r` holds of the one row the lookup can yield, and `id INSIDE list` over the
/// read of the records of a list holds of every row the read yields when `list` holds each of
/// them ([`conjunct::pins_record_list`]), so a total conjunct of either form neither rejects a row
/// nor raises, and its absence changes nothing the filter does. A table's
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
		let Some(read) = ir.op(chain.source).and_then(|data| data.operands.first().copied()) else {
			return Ok(false);
		};
		let (record, records) = match conjunct::literal_record(ir, read) {
			Some(record) => (Some(record), None),
			None => match literal_record_list(ir, read) {
				Some(records) => (None, Some(records)),
				None => return Ok(false),
			},
		};
		let pins = |leaf: ValueId| match (&record, &records) {
			(Some(record), _) => conjunct::pins_record(ir, leaf, record),
			(None, Some(records)) => conjunct::pins_record_list(ir, leaf, records),
			(None, None) => false,
		};
		let leaves = conjunct::leaves(ir, &site);
		let dropped: Vec<ValueId> = leaves
			.iter()
			.copied()
			.filter(|leaf| conjunct::judge(ir, *leaf).total && pins(*leaf))
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
		let leaves = conjunct::leaves(ir, &site);
		let answered: Vec<ValueId> = if let Some(point_filter) = scan.access.point_filter() {
			leaves
				.iter()
				.copied()
				.filter(|leaf| index_answer::answers_spatial(ir, *leaf, point_filter))
				.collect()
		} else {
			if !scan.exact() {
				return Ok(false);
			}
			let Some(cols) = scan.stated.as_deref() else {
				return Ok(false);
			};
			leaves
				.iter()
				.copied()
				.filter(|leaf| index_answer::answers(ir, *leaf, &scan.access, cols))
				.collect()
		};
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

/// A conjunct that is the disjunction of the probes of the index union beneath a predicate is
/// dropped.
///
/// An index union whose branches are all full-text scans lists exactly the records some branch's
/// probe admits, so a conjunct that is the `OR` of those probes, each once or more and none other
/// ([`index_answer::answers_union`]), holds of every row it lists. The ops between the filter and
/// the union pass its rows on in order ([`keeps_order`]). The conjuncts that stay keep their order
/// and their laziness, as for [`AnsweredBySearch`].
struct AnsweredByUnion;

impl Pattern<PhysDialect> for AnsweredByUnion {
	fn name(&self) -> &'static str {
		"answered_by_union"
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
		let Some(PhysOp::IndexUnion(union)) = phys(ir, chain.source) else {
			return Ok(false);
		};
		if let Some(probes) = union
			.branches
			.iter()
			.map(|branch| match &branch.read {
				BranchReadShape::FullText {
					query,
					operator,
					column: Some(column),
					..
				} => Some(index_answer::SearchProbe {
					column,
					operator,
					query,
				}),
				BranchReadShape::FullText {
					column: None,
					..
				}
				| BranchReadShape::BTree(_) => None,
			})
			.collect::<Option<Vec<_>>>()
		{
			let leaves = conjunct::leaves(ir, &site);
			let dropped: Vec<ValueId> = leaves
				.iter()
				.copied()
				.filter(|leaf| index_answer::answers_union(ir, *leaf, &probes))
				.collect();
			if dropped.is_empty() {
				return Ok(false);
			}
			return conjunct::drop_conjuncts(rw, &site, &leaves, &dropped);
		}
		// A point-only `SPATIAL` index union whose branches scan disjoint leaf-cell ranges of the
		// same index and evaluate the same `SpatialPointFilter` against the inline `(x, y)` point
		// trailer returns only records that satisfy that exact geometric predicate.
		let Some(stored_chain) = beneath(ir, site.input, keeps_stored_fields) else {
			return Ok(false);
		};
		if stored_chain.source != chain.source {
			return Ok(false);
		}
		let Some(point_filter) =
			crate::exec::index::access_path::disjoint_spatial_point_union_filter(
				union.branches.iter().map(|b| {
					let access = match &b.read {
						BranchReadShape::BTree(access) => Some(&**access),
						BranchReadShape::FullText {
							..
						} => None,
					};
					(&b.index, access)
				}),
			)
		else {
			return Ok(false);
		};
		let leaves = conjunct::leaves(ir, &site);
		let answered: Vec<ValueId> = leaves
			.iter()
			.copied()
			.filter(|leaf| index_answer::answers_spatial(ir, *leaf, point_filter))
			.collect();
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

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::*;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::tests_plan_request::plan_on;
	use crate::exec::ir::{RequestInput, RequestStatement};
	use crate::exec::operators::test_util::TestDb;

	async fn key_range_coarse_cutoff(setup: &str, sql: &str) -> Option<CoarseCutoff> {
		let db = TestDb::new(setup).await;
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(
			&statements,
			IrScope::Request {
				fragment: None,
			},
		);
		let planned = plan_on(&db, &input).await.expect("the request plans");
		let ir = &planned.ir;
		let mut cutoff = None;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if let Some(PhysOp::KeyRangeScan(scan)) = phys(ir, op) {
				cutoff = scan.topk.as_ref().and_then(|key| key.coarse_cutoff);
			}
			Walk::Continue
		});
		cutoff
	}

	async fn has_sort(setup: &str, sql: &str) -> bool {
		let db = TestDb::new(setup).await;
		let parsed = crate::syn::parse(sql).expect("the request parses").expressions;
		let statements = RequestStatement::of(&parsed);
		let input = RequestInput::request(
			&statements,
			IrScope::Request {
				fragment: None,
			},
		);
		let planned = plan_on(&db, &input).await.expect("the request plans");
		let ir = &planned.ir;
		let mut found = false;
		ir.walk(ir.top(), WalkOrder::PreOrder, &mut |op| {
			if matches!(phys(ir, op), Some(PhysOp::Sort(_))) {
				found = true;
			}
			Walk::Continue
		});
		found
	}

	#[tokio::test]
	async fn coarse_cutoff_sort_rejects_fallible_filters_and_sort_keys() {
		let setup = "
			DEFINE TABLE tick SCHEMALESS;
			DEFINE FIELD id ON tick TYPE [feed: string, at: datetime];
		";
		assert_eq!(
			key_range_coarse_cutoff(
				setup,
				"SELECT n FROM tick WHERE id[0] = 'f' ORDER BY id[1] LIMIT 2"
			)
			.await,
			Some(CoarseCutoff {
				position: 1,
				keep: 2,
			})
		);
		assert_eq!(
			key_range_coarse_cutoff(
				setup,
				"SELECT n FROM tick WHERE id[0] = 'f' AND <int>bad > 0 ORDER BY id[1] LIMIT 2"
			)
			.await,
			None
		);
		assert_eq!(
			key_range_coarse_cutoff(
				setup,
				"SELECT n FROM tick WHERE id[0] = 'f' AND 1 / n > 0 ORDER BY id[1] LIMIT 2"
			)
			.await,
			None
		);
		assert_eq!(
			key_range_coarse_cutoff(
				setup,
				"SELECT n, <int>bad AS k FROM tick WHERE id[0] = 'f' ORDER BY id[1], k LIMIT 2"
			)
			.await,
			None
		);
	}

	#[tokio::test]
	async fn elide_delivered_sort_keeps_collate_and_numeric_on_index_scan() {
		let setup = "
			DEFINE TABLE item SCHEMALESS;
			DEFINE INDEX idx_name ON item FIELDS name;
		";
		assert!(!has_sort(setup, "SELECT * FROM item WHERE name >= 'a' ORDER BY name ASC").await);
		assert!(
			has_sort(setup, "SELECT * FROM item WHERE name >= 'a' ORDER BY name COLLATE ASC").await
		);
		assert!(
			has_sort(setup, "SELECT * FROM item WHERE name >= 'a' ORDER BY name NUMERIC ASC").await
		);
	}
}
