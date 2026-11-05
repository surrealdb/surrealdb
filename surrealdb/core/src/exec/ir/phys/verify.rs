//! The physical dialect's own verifier rules, added to the generic rules of the container.
//!
//! [`verify_op`] checks one physical op against the rules the generic verifier does not cover:
//!
//! - `PhaseGated`: `phys.prefetch` and `phys.materialize` are rejected while the instruction or
//!   host service they stand for does not exist ([`schedule::PREFETCH_EXECUTABLE`],
//!   [`schedule::MATERIALIZE_EXECUTABLE`]). A gated op is rejected as such before its attributes
//!   are read, so the absence of the instruction is the one fact reported. This is the only check
//!   of the rule.
//! - `Choose`: the contract of every choice ([`choose_gap`]), and, where a physical choice's arms
//!   read different access paths, equivalence by a total sort key that the statement's own sort
//!   keys prove.
//! - `PerOp`: the statement structure and the transaction ends of the logical dialect hold of the
//!   physical `txn` and `stmt` ops that stand for them; a loop's `unroll` is `1..=8` and its batch
//!   bounds are non-zero; a prefetch names operands the op has and ranges whose begin operand
//!   precedes its end operand; the pipelines of a container form a DAG
//!   ([`PipelineDag::check_dag`]); the `phys.write_stage` ops of a write's regions are in
//!   [`WriteStageKind`] order and none runs a `Compute` half after an `Apply` half.
//!
//! An op of another dialect and an op with no rule here is accepted.

#![deny(clippy::wildcard_enum_match_arm)]

use surrealdb_strand::TableName;

use super::choose::sort_keys_total;
use super::join_lookup::{build_key_field, right_table};
use super::nested::yielded;
use super::ops::{
	Batch, IdBoundsShape, JoinAttrs, KeyBoundShape, KeyRangeShape, LookupKeyShape, PhysOp, PhysTag,
	PrefetchKeys,
};
use super::pipeline::PipelineDag;
use super::schedule;
use crate::exec::index::key_range::DeliveredKeyOrder;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::op::OpData;
use crate::exec::ir::core::verify::{VerifyError, VerifyRule, choose_gap};
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::iface::choose::{ArmEquivalence, ChooseSpec, Guard, IndexRef};
use crate::exec::ir::iface::ids::{OpId, RegionId};
use crate::exec::ir::logical::ops::{WriteMark, WriteStageKind};
use crate::exec::ir::logical::verify::{TopRole, statement_structure, transaction_ends};
use crate::exec::ir::op_kind::OpKind;
use crate::exec::vm::payload::JoinBuild;
use crate::kvs::Direction;

/// The most iterations of a batch form one execution of a loop takes at once.
const MAX_UNROLL: u8 = 8;

/// Checks the rules of the physical op `op` that the generic rules do not cover.
///
/// An op of another dialect, a dangling id and an op with no rule of its own answer `Ok`. The
/// pipeline rule reads the pipelines of the whole container, so it costs one walk of the
/// container for each pipeline op it is asked about.
pub(crate) fn verify_op(ir: &Ir, op: OpId) -> Result<(), VerifyError> {
	let Some(data) = ir.op(op) else {
		return Ok(());
	};
	let OpKind::Phys(kind) = &data.kind else {
		return Ok(());
	};
	own_rules(ir, op, data, kind)?;
	if let Some(enclosing) = ir.region(ir.enclosing_region(op)).map(|region| region.kind) {
		statement_structure(op, top_role(kind), kind.tag().mnemonic(), enclosing)?;
	}
	if let PhysOp::Txn {
		mode,
	} = kind
	{
		transaction_ends(ir, op, data, *mode)?;
	}
	Ok(())
}

/// The rules of `kind` itself, before the place it stands: a gated op is reported as gated first.
fn own_rules(ir: &Ir, op: OpId, data: &OpData, kind: &PhysOp) -> Result<(), VerifyError> {
	match kind {
		PhysOp::Prefetch(keys) => {
			phase_gated(op, "phys.prefetch", schedule::PREFETCH_EXECUTABLE)?;
			check_prefetch(op, keys, data.operands.len())
		}
		PhysOp::Materialize {
			..
		} => phase_gated(op, "phys.materialize", schedule::MATERIALIZE_EXECUTABLE),
		PhysOp::Choose(spec) => check_choose(ir, op, data, spec),
		PhysOp::Loop {
			batch,
			unroll,
			..
		} => check_loop(op, *batch, *unroll),
		PhysOp::Pipeline(_) => PipelineDag::of(ir).check_dag(),
		PhysOp::Write(attrs) => {
			check_write_stages(ir, op, data)?;
			check_write_body(ir, op, data, &attrs.write.body, attrs.write.gates)?;
			match crate::exec::ir::logical::stages::schema_fault(
				ir,
				data,
				&attrs.write.stored,
				attrs.write.image,
			) {
				Some(detail) => Err(fault(op, VerifyRule::PerOp, detail.to_owned())),
				None => Ok(()),
			}
		}
		PhysOp::Events(attrs) => check_events(op, attrs),
		PhysOp::KeyRangeScan(attrs) => check_key_range(op, &attrs.access, data.operands.len()),
		PhysOp::Join(attrs) => check_join(ir, op, data, attrs),
		PhysOp::TableScan(attrs) => check_id_bounds(op, attrs.bounds.as_ref(), data.operands.len()),
		PhysOp::IndexScan(_)
		| PhysOp::IndexUnion(_)
		| PhysOp::Bitmap(_)
		| PhysOp::IndexLookup(_)
		| PhysOp::FullTextScan(_)
		| PhysOp::KnnScan(_)
		| PhysOp::KnnTopK(_)
		| PhysOp::Count(_)
		| PhysOp::RecordScan(..)
		| PhysOp::Vacant {
			..
		}
		| PhysOp::Enforced {
			..
		}
		| PhysOp::Values {
			..
		}
		| PhysOp::Subquery
		| PhysOp::Probe {
			..
		}
		| PhysOp::Union
		| PhysOp::Distinct {
			..
		}
		| PhysOp::PermissionFilter(_)
		| PhysOp::Layer(_)
		| PhysOp::Filter
		| PhysOp::Fetch
		| PhysOp::Split {
			..
		}
		| PhysOp::Sort(_)
		| PhysOp::Window(_)
		| PhysOp::Aggregate(_)
		| PhysOp::Limit {
			..
		}
		| PhysOp::Project(_)
		| PhysOp::Eval
		| PhysOp::Request {
			..
		}
		| PhysOp::Txn {
			..
		}
		| PhysOp::Stmt {
			..
		}
		| PhysOp::Statement {
			..
		}
		| PhysOp::Match {
			..
		}
		| PhysOp::MatchStep(_)
		| PhysOp::DdlGuard {
			..
		}
		| PhysOp::Define(_)
		| PhysOp::Remove(_)
		| PhysOp::Alter(_)
		| PhysOp::Rebuild(_)
		| PhysOp::Info {
			..
		}
		| PhysOp::Use {
			..
		}
		| PhysOp::Option {
			..
		}
		| PhysOp::Access(_)
		| PhysOp::Kill
		| PhysOp::Live(_)
		| PhysOp::Subscriber(_)
		| PhysOp::Show(_)
		| PhysOp::WriteStage {
			..
		}
		| PhysOp::Timeout
		| PhysOp::VersionScope
		| PhysOp::Only
		| PhysOp::Explain {
			..
		}
		| PhysOp::Assign(_)
		| PhysOp::Unset {
			..
		}
		| PhysOp::Content {
			..
		}
		| PhysOp::Block
		| PhysOp::Let(_)
		| PhysOp::Sleep
		| PhysOp::If {
			..
		}
		| PhysOp::LoadUnit(_)
		| PhysOp::Yield
		| PhysOp::Return
		| PhysOp::Break
		| PhysOp::Next => Ok(()),
	}
}

/// The part `kind` plays among the top-level regions of a request, as its logical twin does.
fn top_role(kind: &PhysOp) -> TopRole {
	if let PhysOp::Txn {
		..
	} = kind
	{
		TopRole::Txn
	} else if let PhysOp::Stmt {
		..
	} = kind
	{
		TopRole::Stmt
	} else {
		TopRole::Other
	}
}

fn fault(op: OpId, rule: VerifyRule, detail: String) -> VerifyError {
	VerifyError {
		op: Some(op),
		rule,
		detail,
	}
}

// ---------------------------------------------------------------------------------------------
// PhaseGated
// ---------------------------------------------------------------------------------------------

/// Rejects the op named `what` while its instruction does not exist.
fn phase_gated(op: OpId, what: &str, executable: bool) -> Result<(), VerifyError> {
	if executable {
		Ok(())
	} else {
		Err(fault(
			op,
			VerifyRule::PhaseGated,
			format!("{what} has no instruction or host service to run on"),
		))
	}
}

// ---------------------------------------------------------------------------------------------
// PerOp
// ---------------------------------------------------------------------------------------------

/// A join that reads its right input through an index has one key pair that index serves, builds
/// on its right input, and ends in a lookup arm that yields the right input's chain over exactly
/// one `phys.index_lookup`: keyed by the arm's argument, over the field the pair's right key reads
/// of the alias, and of the table the right input scans.
fn check_join(ir: &Ir, op: OpId, data: &OpData, attrs: &JoinAttrs) -> Result<(), VerifyError> {
	let Some(pair) = attrs.lookup else {
		return Ok(());
	};
	let fail = |detail: String| Err(fault(op, VerifyRule::PerOp, detail));
	if pair >= attrs.keys {
		return fail(format!(
			"phys.join reads key pair {pair} through an index, and has {} key pairs",
			attrs.keys
		));
	}
	if attrs.build != JoinBuild::Right {
		return fail("phys.join reads an index lookup and holds its left row".to_owned());
	}
	let regions = usize::from(attrs.keys) * 2 + usize::from(attrs.residual) + 1;
	if data.regions.len() != regions {
		return fail(format!(
			"phys.join has {} regions, and its key pairs, residual and lookup arm are {regions}",
			data.regions.len()
		));
	}
	let (Some(arm), Some(right)) = (data.regions.last().copied(), data.operands.get(1).copied())
	else {
		return fail("phys.join reads an index lookup and has no right input".to_owned());
	};
	if yielded(ir, arm).is_none() {
		return fail("the lookup arm of a phys.join yields no value".to_owned());
	}
	let mut lookups =
		ops_in(ir, arm).filter_map(|member| match ir.op(member).map(|held| &held.kind) {
			Some(OpKind::Phys(PhysOp::IndexLookup(lookup))) => Some((member, lookup)),
			Some(OpKind::Phys(_) | OpKind::Expr(_) | OpKind::Logical(_)) | None => None,
		});
	let (Some((lookup_op, lookup)), None) = (lookups.next(), lookups.next()) else {
		return fail("the lookup arm of a phys.join holds other than one index lookup".to_owned());
	};
	let keyed = ir
		.region(arm)
		.and_then(|held| held.blocks.first().copied())
		.and_then(|block| ir.block(block))
		.and_then(|block| block.args.first().copied());
	let key = ir.op(lookup_op).and_then(|held| held.operands.first().copied());
	if lookup.key != LookupKeyShape::Probe || key.is_none() || key != keyed {
		return fail(
			"the index lookup of a phys.join is not keyed by its arm's argument".to_owned(),
		);
	}
	let field = data
		.regions
		.get(usize::from(pair) * 2 + 1)
		.and_then(|region| build_key_field(ir, *region, &attrs.alias));
	if field.as_ref() != Some(&lookup.field) {
		return fail(format!(
			"the index lookup of a phys.join reads `{}`, and the right key of pair {pair} reads \
			 another field of the alias",
			lookup.field
		));
	}
	if right_table(ir, right).as_ref() != Some(&lookup.table) {
		return fail(format!(
			"the index lookup of a phys.join reads table `{}`, which its right input does not scan",
			lookup.table
		));
	}
	Ok(())
}

/// A loop's `unroll` is `1..=MAX_UNROLL` and both of its batch bounds are non-zero.
fn check_loop(op: OpId, batch: Batch, unroll: u8) -> Result<(), VerifyError> {
	if !(1..=MAX_UNROLL).contains(&unroll) {
		return Err(fault(
			op,
			VerifyRule::PerOp,
			format!("phys.loop unrolls {unroll} iterations, outside 1..={MAX_UNROLL}"),
		));
	}
	if batch.max_iterations == 0 || batch.max_bytes == 0 {
		return Err(fault(
			op,
			VerifyRule::PerOp,
			format!(
				"phys.loop has a batch bounded by {} iterations and {} bytes, and each bound is at least 1",
				batch.max_iterations, batch.max_bytes
			),
		));
	}
	Ok(())
}

/// Every operand index a prefetch names is below the op's operand count, and the begin operand
/// of a range precedes its end operand.
fn check_prefetch(op: OpId, keys: &PrefetchKeys, operands: usize) -> Result<(), VerifyError> {
	let names = |index: u16| usize::from(index) < operands;
	if let Some(point) = keys.points.iter().find(|point| !names(**point)) {
		return Err(fault(
			op,
			VerifyRule::PerOp,
			format!("phys.prefetch names operand {point}, and the op has {operands}"),
		));
	}
	for (begin, end) in &keys.ranges {
		if !names(*begin) || !names(*end) {
			return Err(fault(
				op,
				VerifyRule::PerOp,
				format!(
					"phys.prefetch names the range ({begin}, {end}), and the op has {operands} operands"
				),
			));
		}
		if begin >= end {
			return Err(fault(
				op,
				VerifyRule::PerOp,
				format!(
					"phys.prefetch names the range ({begin}, {end}), whose begin does not precede its end"
				),
			));
		}
	}
	Ok(())
}

/// Checks a table scan's record-id bounds: one operand for each operand bound, numbered in slot
/// order, and no operand where the scan has no bounds.
fn check_id_bounds(
	op: OpId,
	bounds: Option<&IdBoundsShape>,
	operands: usize,
) -> Result<(), VerifyError> {
	let fail = |detail: String| Err(fault(op, VerifyRule::PerOp, detail));
	let holes = bounds.map_or(0, IdBoundsShape::holes);
	if operands != holes {
		return fail(format!(
			"phys.table_scan has {holes} operand bounds, and the op has {operands} operands"
		));
	}
	let Some(bounds) = bounds else {
		return Ok(());
	};
	if bounds.low.is_none() && bounds.high.is_none() {
		return fail("phys.table_scan has bounds that bound neither side".to_owned());
	}
	let slots = bounds.bounds().filter_map(|bound| match bound {
		KeyBoundShape::Operand(slot) => Some(*slot),
		KeyBoundShape::Literal(_) => None,
	});
	if let Some((expected, slot)) = slots.enumerate().find(|(at, slot)| usize::from(*slot) != *at) {
		return fail(format!(
			"phys.table_scan numbers its operand bounds in order, and the bound at operand \
			 {expected} holds slot {slot}"
		));
	}
	Ok(())
}

/// Checks a key-range scan: one operand for each operand bound, numbered in the order the access
/// lists its bounds, and positions that lie within the key.
fn check_key_range(op: OpId, access: &KeyRangeShape, operands: usize) -> Result<(), VerifyError> {
	let fail = |detail: String| Err(fault(op, VerifyRule::PerOp, detail));
	let holes = access.holes();
	if operands != holes {
		return fail(format!(
			"phys.key_range_scan has {holes} operand bounds, and the op has {operands} operands"
		));
	}
	let slots = access.bounds().filter_map(|bound| match bound {
		KeyBoundShape::Operand(slot) => Some(*slot),
		KeyBoundShape::Literal(_) => None,
	});
	if let Some((expected, slot)) = slots.enumerate().find(|(at, slot)| usize::from(*slot) != *at) {
		return fail(format!(
			"phys.key_range_scan numbers its operand bounds in order, and the bound at operand \
			 {expected} holds slot {slot}"
		));
	}
	let arity = access.key.arity();
	if access.eq.len() > arity {
		return fail(format!(
			"phys.key_range_scan pins {} positions of a key of {arity}",
			access.eq.len()
		));
	}
	if let Some(range) = &access.range
		&& (range.position as usize != access.eq.len() || range.position as usize >= arity)
	{
		return fail(format!(
			"phys.key_range_scan bounds position {}, which is not the position after its {} pinned \
			 positions within a key of {arity}",
			range.position,
			access.eq.len()
		));
	}
	if let Some(position) = access.ordered_by.iter().find(|position| **position as usize >= arity) {
		return fail(format!(
			"phys.key_range_scan orders by position {position}, and the key has {arity} components"
		));
	}
	if access.delivers == DeliveredKeyOrder::None && !access.ordered_by.is_empty() {
		return fail(
			"phys.key_range_scan delivers no order and names the positions it orders by".into(),
		);
	}
	Ok(())
}

/// The position of `stage` in the order the stages are declared, which is the order a write runs
/// them in. A stage that carries a payload takes the position of its variant.
fn stage_position(stage: WriteStageKind) -> u8 {
	match stage {
		WriteStageKind::CheckRecordUserAccess => 0,
		WriteStageKind::CheckRecordExists => 1,
		WriteStageKind::CheckTableNotView => 2,
		WriteStageKind::CheckTableNotLightweight(_) => 3,
		WriteStageKind::CheckTableType(_) => 4,
		WriteStageKind::CheckPermissionsQuickCreate => 5,
		WriteStageKind::CheckUpdatePermissions => 6,
		WriteStageKind::CheckDeletePermissions => 7,
		WriteStageKind::CheckCreatePermissions => 8,
		WriteStageKind::RecheckUpdatePermissions => 9,
		WriteStageKind::CheckWhereCondition => 10,
		WriteStageKind::ComputeInputData => 11,
		WriteStageKind::CheckDataFields => 12,
		WriteStageKind::ProcessRecordData => 13,
		WriteStageKind::ProcessMergeData => 14,
		WriteStageKind::DefaultRecordData => 15,
		WriteStageKind::GenerateRecordId => 16,
		WriteStageKind::ProcessTableFields => 17,
		WriteStageKind::CleanupTableFields => 18,
		WriteStageKind::CleanupTableReferences => 19,
		WriteStageKind::ClearRecordData => 20,
		WriteStageKind::StoreRecordData => 21,
		WriteStageKind::StoreEdgesData => 22,
		WriteStageKind::StoreInlineAdjacencyData => 23,
		WriteStageKind::StoreIndexData => 24,
		WriteStageKind::ReclaimDocId => 25,
		WriteStageKind::PurgeRecordData => 26,
		WriteStageKind::MaterialiseObservedFields => 27,
		WriteStageKind::ProcessTableReferences => 28,
		WriteStageKind::ProcessTableViews(_) => 29,
		WriteStageKind::ProcessTableEvents(_) => 30,
		WriteStageKind::ProcessTableLives(_) => 31,
		WriteStageKind::ProcessChangefeeds => 32,
		WriteStageKind::CheckSelectPermissions(_) => 33,
		WriteStageKind::OutputWrite => 34,
	}
}

/// An event the op marks dormant has a `WHEN` clause that is never truthy for the op's action
/// ([`when_never_fires`](crate::exec::ir::logical::stages::when_never_fires)), read from the same
/// definition the clause's region was bound from. An op that states no clause marks none.
fn check_events(
	op: OpId,
	attrs: &crate::exec::ir::logical::ops::EventsAttrs,
) -> Result<(), VerifyError> {
	use crate::exec::ir::logical::stages::when_never_fires;

	if !attrs.events.iter().any(|event| event.dormant) {
		return Ok(());
	}
	let live = attrs.unlowerable
		|| attrs.events.len() != attrs.defs.definitions().len()
		|| attrs.events.iter().zip(attrs.defs.iter()).any(|(shape, definition)| {
			shape.dormant
				&& !definition.when_logical().is_ok_and(|when| when_never_fires(when, attrs.action))
		});
	match live {
		true => Err(fault(
			op,
			VerifyRule::PerOp,
			"an event is dormant though its WHEN clause can be truthy".to_owned(),
		)),
		false => Ok(()),
	}
}

/// A write's body names the stages of its orders among the stage ops of its subject loop.
fn check_write_body(
	ir: &Ir,
	op: OpId,
	data: &OpData,
	body: &crate::exec::ir::logical::ops::WriteBody,
	gates: crate::exec::ir::logical::ops::WriteGates,
) -> Result<(), VerifyError> {
	let Some(run) = crate::exec::ir::logical::stages::write_run(ir, data) else {
		return Ok(());
	};
	if let Some(detail) = crate::exec::ir::logical::stages::body_fault(body, run.len()) {
		return Err(fault(op, VerifyRule::PerOp, detail.to_owned()));
	}
	let hosts = crate::exec::ir::logical::stages::write_gate_hosts(ir, data).unwrap_or_default();
	match crate::exec::ir::logical::stages::gates_fault(gates, &hosts) {
		Some(detail) => Err(fault(op, VerifyRule::PerOp, detail.to_owned())),
		None => Ok(()),
	}
}

/// The `phys.write_stage` ops standing directly in a region of the write `op` run in the order of
/// [`WriteStageKind`] and never run a `Compute` half after an `Apply` half.
///
/// Two stages of one kind may follow each other, as the `Compute` and the `Apply` half of one
/// stage do. Each region of the write is read on its own; a write with no region holds no stage.
fn check_write_stages(ir: &Ir, op: OpId, data: &OpData) -> Result<(), VerifyError> {
	for region in &data.regions {
		let mut last: Option<(u8, WriteStageKind)> = None;
		let mut applied = false;
		for stage_op in ops_in(ir, *region) {
			let Some(OpKind::Phys(PhysOp::WriteStage {
				stage,
				mark,
			})) = ir.op(stage_op).map(|data| &data.kind)
			else {
				continue;
			};
			let position = stage_position(*stage);
			if let Some((before, earlier)) = last
				&& position < before
			{
				return Err(fault(
					stage_op,
					VerifyRule::PerOp,
					format!(
						"write {op:?} runs {stage:?} after {earlier:?}, which the stage order puts later"
					),
				));
			}
			match mark {
				WriteMark::Compute if applied => {
					return Err(fault(
						stage_op,
						VerifyRule::PerOp,
						format!(
							"write {op:?} runs the Compute half of {stage:?} after an Apply half"
						),
					));
				}
				WriteMark::Compute => {}
				WriteMark::Apply => applied = true,
			}
			last = Some((position, *stage));
		}
	}
	Ok(())
}

// ---------------------------------------------------------------------------------------------
// Choose
// ---------------------------------------------------------------------------------------------

/// What a read of a choice's arm names of its access path: the op's kind, the table and the
/// indexes it reads, and the direction it reads in. Two arms read the same access path when they
/// hold the same keys in the same order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AccessKey {
	pub(crate) tag: PhysTag,
	pub(crate) table: Option<TableName>,
	pub(crate) indexes: Vec<IndexRef>,
	pub(crate) direction: Option<Direction>,
}

impl AccessKey {
	/// The access path `op` reads, or `None` for an op that is not a read of stored data.
	fn of(op: &PhysOp) -> Option<Self> {
		let key = |tag, table: Option<&TableName>, indexes: Vec<IndexRef>, direction| Self {
			tag,
			table: table.cloned(),
			indexes,
			direction,
		};
		match op {
			PhysOp::TableScan(attrs) => {
				Some(key(PhysTag::TableScan, Some(&attrs.table), Vec::new(), Some(attrs.direction)))
			}
			PhysOp::IndexScan(attrs) => Some(key(
				PhysTag::IndexScan,
				Some(&attrs.table),
				vec![attrs.index.clone()],
				Some(attrs.direction),
			)),
			PhysOp::IndexUnion(attrs) => Some(key(
				PhysTag::IndexUnion,
				Some(&attrs.table),
				attrs.branches.iter().map(|branch| branch.index.clone()).collect(),
				None,
			)),
			PhysOp::Bitmap(attrs) => {
				Some(key(PhysTag::Bitmap, Some(&attrs.table), Vec::new(), None))
			}
			PhysOp::IndexLookup(attrs) => {
				Some(key(PhysTag::IndexLookup, Some(&attrs.table), vec![attrs.index.clone()], None))
			}
			PhysOp::FullTextScan(attrs) => Some(key(
				PhysTag::FullTextScan,
				Some(&attrs.table),
				vec![attrs.index.clone()],
				None,
			)),
			PhysOp::KeyRangeScan(attrs) => Some(key(
				PhysTag::KeyRangeScan,
				Some(&attrs.table),
				Vec::new(),
				Some(attrs.access.direction),
			)),
			PhysOp::KnnScan(attrs) => {
				Some(key(PhysTag::KnnScan, Some(&attrs.table), vec![attrs.index.clone()], None))
			}
			PhysOp::KnnTopK(_) => Some(key(PhysTag::KnnTopK, None, Vec::new(), None)),
			PhysOp::Count(_) => Some(key(PhysTag::Count, None, Vec::new(), None)),
			PhysOp::RecordScan(..) => Some(key(PhysTag::RecordScan, None, Vec::new(), None)),
			PhysOp::Vacant {
				..
			}
			| PhysOp::Enforced {
				..
			}
			| PhysOp::Values {
				..
			}
			| PhysOp::Subquery
			| PhysOp::Probe {
				..
			}
			| PhysOp::Union
			| PhysOp::Join(_)
			| PhysOp::Distinct {
				..
			}
			| PhysOp::PermissionFilter(_)
			| PhysOp::Layer(_)
			| PhysOp::Filter
			| PhysOp::Fetch
			| PhysOp::Split {
				..
			}
			| PhysOp::Sort(_)
			| PhysOp::Window(_)
			| PhysOp::Aggregate(_)
			| PhysOp::Limit {
				..
			}
			| PhysOp::Project(_)
			| PhysOp::Eval
			| PhysOp::Request {
				..
			}
			| PhysOp::Txn {
				..
			}
			| PhysOp::Stmt {
				..
			}
			| PhysOp::Statement {
				..
			}
			| PhysOp::Match {
				..
			}
			| PhysOp::MatchStep(_)
			| PhysOp::DdlGuard {
				..
			}
			| PhysOp::Define(_)
			| PhysOp::Remove(_)
			| PhysOp::Alter(_)
			| PhysOp::Rebuild(_)
			| PhysOp::Info {
				..
			}
			| PhysOp::Use {
				..
			}
			| PhysOp::Option {
				..
			}
			| PhysOp::Access(_)
			| PhysOp::Kill
			| PhysOp::Live(_)
			| PhysOp::Subscriber(_)
			| PhysOp::Show(_)
			| PhysOp::Write(_)
			| PhysOp::WriteStage {
				..
			}
			| PhysOp::Events(..)
			| PhysOp::Timeout
			| PhysOp::VersionScope
			| PhysOp::Only
			| PhysOp::Explain {
				..
			}
			| PhysOp::Assign(_)
			| PhysOp::Unset {
				..
			}
			| PhysOp::Content {
				..
			}
			| PhysOp::Block
			| PhysOp::Let(_)
			| PhysOp::Sleep
			| PhysOp::If {
				..
			}
			| PhysOp::Loop {
				..
			}
			| PhysOp::Pipeline(_)
			| PhysOp::Choose(_)
			| PhysOp::Prefetch(_)
			| PhysOp::Materialize {
				..
			}
			| PhysOp::LoadUnit(_)
			| PhysOp::Yield
			| PhysOp::Return
			| PhysOp::Break
			| PhysOp::Next => None,
		}
	}
}

/// The access paths the ops of `region` and of the regions beneath them read, in pre-order.
pub(crate) fn access_paths_of(ir: &Ir, region: RegionId) -> Vec<AccessKey> {
	let mut keys = Vec::new();
	ir.walk(region, WalkOrder::PreOrder, &mut |op| {
		if let Some(OpKind::Phys(kind)) = ir.op(op).map(|data| &data.kind) {
			keys.extend(AccessKey::of(kind));
		}
		Walk::Continue
	});
	keys
}

/// Whether the arms of the choice `data` read different access paths: some arm does not hold the
/// same [`AccessKey`]s, in the same order, as the first. The planner that builds a physical choice
/// sets its equivalence by this answer.
pub(crate) fn arms_read_different_paths(ir: &Ir, data: &OpData) -> bool {
	let mut arms = data.regions.iter().map(|region| access_paths_of(ir, *region));
	let Some(first) = arms.next() else {
		return false;
	};
	arms.any(|arm| arm != first)
}

/// Whether the arm a choice runs is decided by the input alone: every guard before the last is
/// [`Guard::TableIs`] or [`Guard::ValueKindIs`], so the guards are mutually exclusive and one input
/// reaches the same arm on every run. Such arms are not alternatives for one input, and may read
/// different access paths without a total sort key.
fn dispatches_on_input(spec: &ChooseSpec) -> bool {
	let Some((_, before)) = spec.arms.split_last() else {
		return false;
	};
	before.iter().all(|arm| matches!(arm.guard, Guard::TableIs(..) | Guard::ValueKindIs(..)))
}

/// The rules of a guarded choice: each arm's guards and the equivalence of its access paths.
fn check_choose(ir: &Ir, op: OpId, data: &OpData, spec: &ChooseSpec) -> Result<(), VerifyError> {
	let choose = |detail: String| fault(op, VerifyRule::Choose, detail);
	if let Some(detail) = choose_gap(ir, spec, &data.regions) {
		return Err(choose(detail));
	}
	if !dispatches_on_input(spec) && arms_read_different_paths(ir, data) {
		if spec.equivalence != ArmEquivalence::OrderedByTotalKey {
			return Err(choose(
				"the arms read different access paths and the choice is not equivalent by a total key"
					.to_owned(),
			));
		}
		if sort_keys_total(ir, op).is_none() {
			return Err(choose(
				"the arms read different access paths and the statement's sort keys are not total"
					.to_owned(),
			));
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use smallvec::{SmallVec, smallvec};
	use surrealdb_strand::Strand;

	use super::*;
	use crate::exec::ir::core::facts::FactCache;
	use crate::exec::ir::core::ir::IrScope;
	use crate::exec::ir::core::location::{LocOrigin, Location};
	use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Rewriter};
	use crate::exec::ir::expr::ops::ExprOp;
	use crate::exec::ir::iface::choose::{
		ArmBody, ArmLabel, ArmSpec, ChooseGranularity, ChooseOrigin, ExhaustiveFact, GenericArm,
		IndexRef, MAX_CHOOSE_ARMS, ValueKindClass,
	};
	use crate::exec::ir::iface::ids::{BlockId, FrameId, SnapshotId, ValueId};
	use crate::exec::ir::iface::region_kind::{RegionKind, StatementCommit};
	use crate::exec::ir::iface::statements::{OutputShape, WriteFlags, WriteForm};
	use crate::exec::ir::iface::types::TypeKnowledge;
	use crate::exec::ir::iface::unit::UnitKind;
	use crate::exec::ir::logical::ops::{LoopKind, WriteAttrs};
	use crate::exec::ir::phys::ops::{
		KeyEqShape, KeyRangeBoundShape, KeyRangeOnShape, KeyRangeScanAttrs, MaterializeImage,
		PhysWriteAttrs, PipelineAttrs, PipelineEdge, PipelineId, ReadClauses, TableScanAttrs,
		WriteSubjectsShape, key_range_fixtures,
	};
	use crate::expr::Kind;

	/// A container whose top region is a statement, where any op may stand.
	fn empty_ir() -> Ir {
		Ir::new(
			IrScope::Request {
				fragment: None,
			},
			RegionKind::Statement(StatementCommit::Auto),
		)
	}

	fn top_block(ir: &Ir) -> BlockId {
		ir.region(ir.top()).expect("the top region exists").blocks[0]
	}

	fn op_spec(kind: OpKind, operands: &[ValueId], results: usize) -> OpSpec {
		OpSpec {
			kind,
			operands: operands.iter().copied().collect(),
			results: (0..results).map(|_| TypeKnowledge::Unknown).collect(),
			frame: FrameId::new(0),
			snapshot: SnapshotId::new(0),
			loc: Location {
				stmt: 0,
				span: None,
				origin: LocOrigin::Synthesized,
			},
		}
	}

	/// Creates the physical op `kind` at `at`, with `results` results, and answers the op and its
	/// results.
	fn create(
		rw: &mut Rewriter<'_>,
		at: InsertPoint,
		kind: PhysOp,
		operands: &[ValueId],
		results: usize,
	) -> (OpId, SmallVec<[ValueId; 2]>) {
		let op = rw
			.create(at, op_spec(OpKind::Phys(kind), operands, results))
			.expect("the op is created");
		let values = rw.ir().op(op).expect("the op exists").results.clone();
		(op, values)
	}

	/// A region of `kind` owned by `owner`, closed by a yield; answers its block.
	fn closed_region(rw: &mut Rewriter<'_>, owner: OpId, kind: RegionKind) -> BlockId {
		let (_, block) = rw.create_region(owner, kind, &[]).expect("the region is created");
		create(rw, InsertPoint::End(block), PhysOp::Yield, &[], 0);
		block
	}

	fn edit<R>(ir: &mut Ir, change: impl FnOnce(&mut Rewriter<'_>) -> R) -> R {
		let mut facts = FactCache::new();
		let mut rw = Rewriter::new(ir, &mut facts);
		change(&mut rw)
	}

	fn table(name: &str) -> TableName {
		TableName::new(Strand::new(name))
	}

	fn scan(name: &str) -> PhysOp {
		PhysOp::TableScan(Box::new(TableScanAttrs {
			probes: None,
			bounds: None,
			prefilter: None,
			table: table(name),
			direction: Direction::Forward,
			topk: None,
			lightweight: false,
			id_order: true,
			read: ReadClauses::default(),
		}))
	}

	fn arm(guard: Guard) -> ArmSpec {
		ArmSpec {
			guard,
			body: ArmBody::PreLowered,
			label: ArmLabel("arm"),
		}
	}

	fn spec_of(arms: SmallVec<[ArmSpec; 4]>) -> ChooseSpec {
		ChooseSpec {
			granularity: ChooseGranularity::Statement,
			arms,
			generic: GenericArm::Present,
			equivalence: ArmEquivalence::SameRowsSameOrder,
			origin: ChooseOrigin::PhysicalGuarded,
		}
	}

	fn two_arms() -> SmallVec<[ArmSpec; 4]> {
		smallvec![arm(Guard::TableIs(table("a"))), arm(Guard::Otherwise)]
	}

	/// Two arms whose choice depends on the state of the run, not on the input.
	fn state_arms() -> SmallVec<[ArmSpec; 4]> {
		let index = IndexRef {
			table: table("a"),
			index: Strand::new("ix"),
			position: 0,
			scalar: Box::default(),
		};
		smallvec![arm(Guard::IndexReady(index)), arm(Guard::Otherwise)]
	}

	/// A container whose top block holds `spec` as a choice with one closed arm region per entry
	/// of `paths` and, in the arm of an entry that names a table, a scan of that table; answers
	/// the container and the choice.
	fn choose_container(spec: ChooseSpec, paths: &[Option<&str>]) -> (Ir, OpId) {
		let mut ir = empty_ir();
		let block = top_block(&ir);
		let choose = edit(&mut ir, |rw| {
			let (choose, _) =
				create(rw, InsertPoint::End(block), PhysOp::Choose(Box::new(spec)), &[], 1);
			for path in paths {
				let arm = closed_region(rw, choose, RegionKind::ChooseArm);
				if let Some(name) = path {
					let end = rw.ir().block(arm).expect("the block exists").ops[0];
					create(rw, InsertPoint::Before(end), scan(name), &[], 1);
				}
			}
			create(rw, InsertPoint::End(block), PhysOp::Yield, &[], 0);
			choose
		});
		(ir, choose)
	}

	/// The rule `verify_op` breaks at `op`, or `None` when it accepts the op.
	fn broken(ir: &Ir, op: OpId) -> Option<VerifyRule> {
		verify_op(ir, op).err().map(|failure| failure.rule)
	}

	/// An op created in a container with `operands` operands of its own.
	fn lone(kind: PhysOp, operands: usize) -> (Ir, OpId) {
		let mut ir = empty_ir();
		let block = top_block(&ir);
		let op = edit(&mut ir, |rw| {
			let values: Vec<ValueId> = (0..operands)
				.map(|_| create(rw, InsertPoint::End(block), PhysOp::Eval, &[], 1).1[0])
				.collect();
			let (op, _) = create(rw, InsertPoint::End(block), kind, &values, 0);
			create(rw, InsertPoint::End(block), PhysOp::Yield, &[], 0);
			op
		});
		(ir, op)
	}

	/// The events of a table for an update, one for each `WHEN` clause, as the physical phase holds
	/// them with the events in `dormant` marked.
	fn events_op(whens: &[&str], dormant: &[usize], unlowerable: bool) -> (Ir, OpId) {
		use crate::catalog::auth::AuthLimit;
		use crate::catalog::{EventDefinition, EventKind};

		let definitions: Vec<EventDefinition> = whens
			.iter()
			.map(|when| {
				EventDefinition::new(
					"e".into(),
					"t".into(),
					crate::syn::expr(&format!("({when})")).expect("a clause"),
					vec![crate::syn::expr("1").expect("a clause")],
					EventKind::Sync,
					AuthLimit::new_no_limit(),
					None,
				)
			})
			.collect();
		let mut attrs = crate::exec::ir::logical::stages::events_over(
			&std::sync::Arc::from(definitions),
			crate::doc::Action::Update,
		)
		.attrs;
		for at in dormant {
			attrs.events[*at].dormant = true;
		}
		attrs.unlowerable = unlowerable;
		lone(PhysOp::Events(Box::new(attrs)), 0)
	}

	/// A dormant event has a `WHEN` clause that is never truthy for the op's action, and an op that
	/// states no clause has no dormant event.
	#[test]
	fn a_dormant_event_whose_when_can_be_truthy_is_refused() {
		let whens = ["$event = 'DELETE'", "$event = 'UPDATE'", "$after.flag", "false"];
		let (ir, op) = events_op(&whens, &[], false);
		assert_eq!(broken(&ir, op), None);
		for (dormant, refused) in [
			(&[0][..], false),
			(&[3][..], false),
			(&[0, 3][..], false),
			(&[1][..], true),
			(&[2][..], true),
			(&[0, 1][..], true),
		] {
			let (ir, op) = events_op(&whens, dormant, false);
			let expected = refused.then_some(VerifyRule::PerOp);
			assert_eq!(broken(&ir, op), expected, "{dormant:?}");
		}
		let (ir, op) = events_op(&whens[..1], &[0], true);
		assert_eq!(broken(&ir, op), Some(VerifyRule::PerOp), "an op that states no clause");
	}

	#[test]
	fn a_choice_without_arms_is_refused() {
		let (ir, choose) = choose_container(spec_of(SmallVec::new()), &[]);
		assert_eq!(broken(&ir, choose), Some(VerifyRule::Choose));
	}

	#[test]
	fn a_choice_over_the_arm_bound_is_refused() {
		let mut arms: SmallVec<[ArmSpec; 4]> =
			(0..MAX_CHOOSE_ARMS).map(|at| arm(Guard::TableIs(table(&format!("t{at}"))))).collect();
		arms.push(arm(Guard::Otherwise));
		let paths = vec![None; arms.len()];
		let (ir, choose) = choose_container(spec_of(arms), &paths);
		assert_eq!(broken(&ir, choose), Some(VerifyRule::Choose));
	}

	#[test]
	fn a_choice_needs_a_region_for_each_arm() {
		let (ir, choose) = choose_container(spec_of(two_arms()), &[None]);
		assert_eq!(broken(&ir, choose), Some(VerifyRule::Choose));
	}

	#[test]
	fn a_choice_without_a_generic_last_arm_is_refused() {
		let arms = smallvec![arm(Guard::TableIs(table("a"))), arm(Guard::TableIs(table("b")))];
		let (ir, choose) = choose_container(spec_of(arms), &[None, None]);
		assert_eq!(broken(&ir, choose), Some(VerifyRule::Choose));
	}

	#[test]
	fn an_exhaustive_choice_needs_no_generic_arm() {
		let arms = smallvec![arm(Guard::TableIs(table("a"))), arm(Guard::TableIs(table("b")))];
		let spec = ChooseSpec {
			generic: GenericArm::ExhaustiveBy(ExhaustiveFact::TypeFact(Kind::Any)),
			..spec_of(arms)
		};
		let (ir, choose) = choose_container(spec, &[None, None]);
		assert_eq!(broken(&ir, choose), None);
	}

	#[test]
	fn a_guarded_physical_arm_that_loads_a_unit_is_refused() {
		let arms = smallvec![
			arm(Guard::TableIs(table("a"))),
			ArmSpec {
				body: ArmBody::UnitLoad(UnitKind::Function),
				..arm(Guard::Otherwise)
			}
		];
		let (ir, choose) = choose_container(spec_of(arms), &[None, None]);
		assert_eq!(broken(&ir, choose), Some(VerifyRule::Choose));
	}

	#[test]
	fn a_dynamic_target_may_load_a_unit() {
		let arms = smallvec![
			arm(Guard::TableIs(table("a"))),
			ArmSpec {
				body: ArmBody::UnitLoad(UnitKind::Function),
				..arm(Guard::Otherwise)
			}
		];
		let spec = ChooseSpec {
			origin: ChooseOrigin::LogicalDynamicTarget,
			..spec_of(arms)
		};
		let (ir, choose) = choose_container(spec, &[None, None]);
		assert_eq!(broken(&ir, choose), None);
	}

	#[test]
	fn different_access_paths_need_a_total_key() {
		// The arms read two tables and the container holds no statement whose sort keys could be
		// total, so no equivalence admits the choice.
		for equivalence in [ArmEquivalence::SameRowsSameOrder, ArmEquivalence::OrderedByTotalKey] {
			let spec = ChooseSpec {
				equivalence,
				..spec_of(state_arms())
			};
			let (ir, choose) = choose_container(spec, &[Some("a"), Some("b")]);
			assert_eq!(broken(&ir, choose), Some(VerifyRule::Choose));
		}
	}

	#[test]
	fn different_access_paths_need_a_total_key_whatever_the_origin() {
		let spec = ChooseSpec {
			origin: ChooseOrigin::LogicalDynamicTarget,
			..spec_of(state_arms())
		};
		let (ir, choose) = choose_container(spec, &[Some("a"), Some("b")]);
		assert_eq!(broken(&ir, choose), Some(VerifyRule::Choose));
	}

	#[test]
	fn arms_dispatching_on_the_input_need_no_total_key() {
		let arms = smallvec![
			arm(Guard::TableIs(table("a"))),
			arm(Guard::ValueKindIs(ValueKindClass::Array)),
			arm(Guard::Otherwise)
		];
		let spec = ChooseSpec {
			origin: ChooseOrigin::LogicalDynamicTarget,
			..spec_of(arms)
		};
		let (ir, choose) = choose_container(spec, &[Some("a"), Some("b"), Some("c")]);
		assert_eq!(broken(&ir, choose), None);
	}

	#[test]
	fn arms_that_read_one_access_path_need_no_total_key() {
		let (ir, choose) = choose_container(spec_of(two_arms()), &[Some("a"), Some("a")]);
		assert_eq!(broken(&ir, choose), None);
	}

	fn for_loop(batch: Batch, unroll: u8) -> PhysOp {
		PhysOp::Loop {
			kind: LoopKind::For,
			first: false,
			batch,
			unroll,
			value: false,
			step: None,
		}
	}

	#[test]
	fn a_transaction_and_a_statement_stand_where_their_logical_twins_do() {
		let session = crate::exec::ir::logical::ops::TxnRegionMode::Session {
			id: 0,
		};
		let (ir, txn) = lone(
			PhysOp::Txn {
				mode: session,
			},
			0,
		);
		assert_eq!(broken(&ir, txn), Some(VerifyRule::PerOp), "a transaction in a statement");
		let (ir, stmt) = lone(
			PhysOp::Stmt {
				commit: StatementCommit::Auto,
				control: None,
			},
			0,
		);
		assert_eq!(broken(&ir, stmt), Some(VerifyRule::PerOp), "a statement in a statement");
	}

	#[test]
	fn a_loop_unrolls_one_to_eight_iterations() {
		let batch = Batch {
			max_iterations: 1,
			max_bytes: 1,
		};
		for (unroll, accepted) in [(0, false), (1, true), (8, true), (9, false)] {
			let (ir, op) = lone(for_loop(batch, unroll), 1);
			assert_eq!(broken(&ir, op), (!accepted).then_some(VerifyRule::PerOp), "{unroll}");
		}
	}

	#[test]
	fn a_loop_batch_has_no_zero_bound() {
		for (max_iterations, max_bytes) in [(0, 1), (1, 0)] {
			let batch = Batch {
				max_iterations,
				max_bytes,
			};
			let (ir, op) = lone(for_loop(batch, 1), 1);
			assert_eq!(broken(&ir, op), Some(VerifyRule::PerOp));
		}
	}

	#[test]
	fn a_prefetch_is_gated_while_its_instruction_does_not_exist() {
		let keys = PrefetchKeys {
			points: smallvec![0],
			ranges: SmallVec::new(),
		};
		let (ir, op) = lone(PhysOp::Prefetch(Box::new(keys)), 1);
		let expected = (!schedule::PREFETCH_EXECUTABLE).then_some(VerifyRule::PhaseGated);
		assert_eq!(broken(&ir, op), expected);
	}

	#[test]
	fn a_materialize_is_gated_while_its_service_does_not_exist() {
		let (ir, op) = lone(
			PhysOp::Materialize {
				image: MaterializeImage::KeyAndValue,
			},
			1,
		);
		let expected = (!schedule::MATERIALIZE_EXECUTABLE).then_some(VerifyRule::PhaseGated);
		assert_eq!(broken(&ir, op), expected);
	}

	#[test]
	fn a_prefetch_names_only_operands_it_has() {
		let (_, op) = lone(PhysOp::Eval, 2);
		let rule_of = |keys: PrefetchKeys, operands| {
			check_prefetch(op, &keys, operands).err().map(|failure| failure.rule)
		};
		let points = |points: &[u16]| PrefetchKeys {
			points: points.iter().copied().collect(),
			ranges: SmallVec::new(),
		};
		let ranges = |ranges: &[(u16, u16)]| PrefetchKeys {
			points: SmallVec::new(),
			ranges: ranges.iter().copied().collect(),
		};
		assert_eq!(rule_of(points(&[0, 1]), 2), None);
		assert_eq!(rule_of(points(&[2]), 2), Some(VerifyRule::PerOp));
		assert_eq!(rule_of(ranges(&[(0, 1)]), 2), None);
		assert_eq!(rule_of(ranges(&[(0, 2)]), 2), Some(VerifyRule::PerOp));
		assert_eq!(rule_of(ranges(&[(1, 0)]), 2), Some(VerifyRule::PerOp));
		assert_eq!(rule_of(ranges(&[(1, 1)]), 2), Some(VerifyRule::PerOp));
	}

	/// An access over `[a: int, b: int]` that pins `a` to an operand and bounds `b` below by an
	/// operand and above by a literal.
	fn key_range_access() -> KeyRangeShape {
		KeyRangeShape {
			key: key_range_fixtures::pair(),
			eq: vec![KeyEqShape::One(KeyBoundShape::Operand(0))],
			range: Some(KeyRangeOnShape {
				position: 1,
				low: Some(KeyRangeBoundShape {
					bound: KeyBoundShape::Operand(1),
					inclusive: true,
				}),
				high: Some(KeyRangeBoundShape {
					bound: KeyBoundShape::Literal(crate::val::Value::from(9i64)),
					inclusive: false,
				}),
			}),
			direction: Direction::Forward,
			delivers: DeliveredKeyOrder::Exact,
			ordered_by: vec![0, 1],
		}
	}

	#[test]
	fn a_key_range_scan_takes_one_operand_for_each_operand_bound() {
		let (_, op) = lone(PhysOp::Eval, 2);
		let rule_of = |access: &KeyRangeShape, operands| {
			check_key_range(op, access, operands).err().map(|failure| failure.rule)
		};
		let access = key_range_access();
		assert_eq!(access.holes(), 2);
		assert_eq!(rule_of(&access, 2), None);
		assert_eq!(rule_of(&access, 1), Some(VerifyRule::PerOp));
		assert_eq!(rule_of(&access, 3), Some(VerifyRule::PerOp));
	}

	#[test]
	fn a_key_range_scan_numbers_its_operand_bounds_in_order() {
		let (_, op) = lone(PhysOp::Eval, 2);
		let mut access = key_range_access();
		access.eq = vec![KeyEqShape::One(KeyBoundShape::Operand(1))];
		let failure = check_key_range(op, &access, 2).expect_err("the slots are out of order");
		assert_eq!(failure.rule, VerifyRule::PerOp);
	}

	#[test]
	fn a_key_range_scan_orders_only_by_positions_of_its_key() {
		let (_, op) = lone(PhysOp::Eval, 2);
		let mut access = key_range_access();
		access.ordered_by = vec![0, 2];
		let failure = check_key_range(op, &access, 2).expect_err("position 2 is past the key");
		assert_eq!(failure.rule, VerifyRule::PerOp);
		access.ordered_by = vec![0, 1];
		access.delivers = DeliveredKeyOrder::None;
		let failure = check_key_range(op, &access, 2).expect_err("no order is delivered");
		assert_eq!(failure.rule, VerifyRule::PerOp);
	}

	#[test]
	fn a_key_range_scan_bounds_the_position_after_its_pinned_run() {
		let (_, op) = lone(PhysOp::Eval, 2);
		let mut access = key_range_access();
		if let Some(range) = &mut access.range {
			range.position = 0;
		}
		let failure =
			check_key_range(op, &access, 2).expect_err("the range is on a pinned position");
		assert_eq!(failure.rule, VerifyRule::PerOp);
		let mut access = key_range_access();
		access.eq = vec![
			KeyEqShape::One(KeyBoundShape::Operand(0)),
			KeyEqShape::One(KeyBoundShape::Operand(1)),
			KeyEqShape::One(KeyBoundShape::Operand(2)),
		];
		access.range = None;
		let failure = check_key_range(op, &access, 3).expect_err("the run is longer than the key");
		assert_eq!(failure.rule, VerifyRule::PerOp);
	}

	#[test]
	fn a_key_range_scan_in_a_container_is_checked_by_its_op_rule() {
		let access = key_range_access();
		let attrs = KeyRangeScanAttrs {
			prefilter: None,
			table: table("t"),
			read: Default::default(),
			access,
			topk: None,
		};
		let (ir, op) = lone(PhysOp::KeyRangeScan(Box::new(attrs.clone())), 2);
		assert_eq!(broken(&ir, op), None);
		let (ir, op) = lone(PhysOp::KeyRangeScan(Box::new(attrs)), 1);
		assert_eq!(broken(&ir, op), Some(VerifyRule::PerOp));
	}

	#[test]
	fn a_table_scan_takes_one_operand_for_each_operand_bound() {
		use crate::exec::ir::phys::ops::KeyRangeBoundShape;
		let side = |bound| {
			Some(KeyRangeBoundShape {
				bound,
				inclusive: false,
			})
		};
		let bounds = IdBoundsShape {
			low: side(KeyBoundShape::Operand(0)),
			high: side(KeyBoundShape::Literal(crate::val::Value::from(9i64))),
		};
		let scan_with = |bounds: Option<IdBoundsShape>| {
			let PhysOp::TableScan(mut attrs) = scan("t") else {
				panic!("a table scan");
			};
			attrs.bounds = bounds;
			PhysOp::TableScan(attrs)
		};
		let (ir, op) = lone(scan_with(Some(bounds.clone())), 1);
		assert_eq!(broken(&ir, op), None);
		let (ir, op) = lone(scan_with(Some(bounds)), 0);
		assert_eq!(broken(&ir, op), Some(VerifyRule::PerOp));
		let (ir, op) = lone(scan_with(None), 1);
		assert_eq!(broken(&ir, op), Some(VerifyRule::PerOp));
		let (ir, op) = lone(scan_with(None), 0);
		assert_eq!(broken(&ir, op), None);
		let (ir, op) = lone(
			scan_with(Some(IdBoundsShape {
				low: None,
				high: None,
			})),
			0,
		);
		assert_eq!(broken(&ir, op), Some(VerifyRule::PerOp));
		let renumbered = IdBoundsShape {
			low: side(KeyBoundShape::Operand(1)),
			high: None,
		};
		let (ir, op) = lone(scan_with(Some(renumbered)), 1);
		assert_eq!(broken(&ir, op), Some(VerifyRule::PerOp));
	}

	/// A container of pipelines `ids`, each holding the edges `edges` lists for its id.
	fn pipelines(ids: &[u32], edges: &[(u32, PipelineEdge, u32)]) -> (Ir, Vec<OpId>) {
		let mut ir = empty_ir();
		let block = top_block(&ir);
		let ops = edit(&mut ir, |rw| {
			let ops: Vec<OpId> = ids
				.iter()
				.map(|id| {
					let attrs = PipelineAttrs {
						id: PipelineId(*id),
						edges: edges
							.iter()
							.filter(|(owner, ..)| owner == id)
							.map(|(_, kind, other)| (*kind, PipelineId(*other)))
							.collect(),
						logical: None,
					};
					let (op, _) = create(
						rw,
						InsertPoint::End(block),
						PhysOp::Pipeline(Box::new(attrs)),
						&[],
						0,
					);
					closed_region(rw, op, RegionKind::Pipeline);
					op
				})
				.collect();
			create(rw, InsertPoint::End(block), PhysOp::Yield, &[], 0);
			ops
		});
		(ir, ops)
	}

	#[test]
	fn pipelines_that_form_a_dag_are_accepted() {
		let (ir, ops) = pipelines(&[0, 1], &[(0, PipelineEdge::Feeds, 1)]);
		assert!(ops.iter().all(|op| broken(&ir, *op).is_none()));
	}

	#[test]
	fn a_pipeline_edge_to_no_pipeline_is_refused() {
		let (ir, ops) = pipelines(&[0], &[(0, PipelineEdge::Feeds, 5)]);
		assert_eq!(broken(&ir, ops[0]), Some(VerifyRule::PerOp));
	}

	#[test]
	fn a_pipeline_cycle_is_refused() {
		let (ir, ops) =
			pipelines(&[0, 1], &[(0, PipelineEdge::Feeds, 1), (1, PipelineEdge::Feeds, 0)]);
		assert_eq!(broken(&ir, ops[0]), Some(VerifyRule::PerOp));
	}

	/// A container holding a write whose stage region holds `stages` in order.
	fn write_with(stages: &[(WriteStageKind, WriteMark)]) -> (Ir, OpId) {
		let mut ir = empty_ir();
		let block = top_block(&ir);
		let write = edit(&mut ir, |rw| {
			let attrs = PhysWriteAttrs {
				write: WriteAttrs {
					form: WriteForm::Create,
					only: false,
					output: OutputShape::After,
					with: None,
					explain: None,
					flags: WriteFlags::default(),
					body: crate::exec::ir::logical::ops::WriteBody::default(),
					gates: crate::exec::ir::logical::ops::WriteGates::default(),
					image: crate::exec::ir::logical::ops::WriteImage::default(),
					stored: Default::default(),
					derived: Default::default(),
					into_text: None,
					statement: None,
					linked: None,
				},
				subjects: WriteSubjectsShape::Targets,
			};
			let (write, _) =
				create(rw, InsertPoint::End(block), PhysOp::Write(Box::new(attrs)), &[], 0);
			let (_, stage_block) =
				rw.create_region(write, RegionKind::Block, &[]).expect("the region is created");
			for (stage, mark) in stages {
				let op = PhysOp::WriteStage {
					stage: *stage,
					mark: *mark,
				};
				create(rw, InsertPoint::End(stage_block), op, &[], 0);
			}
			create(rw, InsertPoint::End(stage_block), PhysOp::Yield, &[], 0);
			create(rw, InsertPoint::End(block), PhysOp::Yield, &[], 0);
			write
		});
		(ir, write)
	}

	#[test]
	fn write_stages_run_in_declaration_order() {
		let (ir, write) = write_with(&[
			(WriteStageKind::CheckRecordExists, WriteMark::Compute),
			(WriteStageKind::ProcessRecordData, WriteMark::Compute),
			(WriteStageKind::StoreRecordData, WriteMark::Apply),
			(WriteStageKind::StoreRecordData, WriteMark::Apply),
			(WriteStageKind::OutputWrite, WriteMark::Apply),
		]);
		assert_eq!(broken(&ir, write), None);
	}

	#[test]
	fn write_stages_out_of_order_are_refused() {
		let (ir, write) = write_with(&[
			(WriteStageKind::StoreRecordData, WriteMark::Apply),
			(WriteStageKind::ProcessRecordData, WriteMark::Apply),
		]);
		assert_eq!(broken(&ir, write), Some(VerifyRule::PerOp));
	}

	#[test]
	fn a_compute_half_never_follows_an_apply_half() {
		let (ir, write) = write_with(&[
			(WriteStageKind::StoreRecordData, WriteMark::Apply),
			(WriteStageKind::StoreIndexData, WriteMark::Compute),
		]);
		assert_eq!(broken(&ir, write), Some(VerifyRule::PerOp));
	}

	#[test]
	fn an_op_of_another_dialect_is_accepted() {
		let mut ir = empty_ir();
		let block = top_block(&ir);
		let op = edit(&mut ir, |rw| {
			rw.create(InsertPoint::End(block), op_spec(OpKind::Expr(ExprOp::Yield), &[], 0))
				.expect("the op is created")
		});
		assert_eq!(broken(&ir, op), None);
	}
}
