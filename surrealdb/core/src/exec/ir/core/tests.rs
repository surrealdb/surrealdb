//! Tests that cross the modules of the container: the rewriter's mutations against the
//! use-lists, generations, side tables and fact cache they maintain, and the verifier's rules over
//! containers the rewriter builds.
//!
//! Ops are built the way a pass builds them, through the rewriter, and never through a bind: the
//! rewriter accepts any operand count and region kind, so each test names only what it checks.

use std::collections::BTreeMap;
use std::future::{Future, ready};
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use smallvec::SmallVec;
use surrealdb_strand::{Strand, TableName};

use super::convert::{
	ConversionPattern, ConversionReport, ConversionSet, Converted, convert, convert_logged,
};
use super::copy::OutsideRead;
use super::facts::FactCache;
use super::ir::{Ir, IrScope, Walk, WalkOrder};
use super::location::{LocOrigin, Location, Span};
use super::parse::{ParseError, parse, parse_unverified};
use super::pattern::{GREEDY_DEFAULT, GreedyLimits, Limit, Pattern, PatternSet, apply_greedy};
use super::print::{AttrSyntax, AttrSyntaxes, PrintOpts, print_with};
use super::rewriter::{InsertPoint, OpSpec, Provenance, Rewriter};
use super::text::{ALL_REGION_KINDS, region_kind_from_name, region_kind_name};
use super::value::{Use, ValueDef};
use super::value_table::ValueTable;
use super::verify::{VerifyAt, VerifyError, VerifyRule, verify, violation_of};
use super::walk::{ControlTarget, control_target, enclosing_statement, ops_in};
use crate::exec::function::FunctionRegistry;
use crate::exec::ir::build::phys::UNBATCHED;
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ClosureAttrs, ExprOp, ExprTag, NestedStrategy};
use crate::exec::ir::iface::catalog::{EffectCx, PhysCx, PhysicalCatalog, SemanticCatalog};
use crate::exec::ir::iface::choose::{
	ArmBody, ArmEquivalence, ArmLabel, ArmSpec, ChooseGranularity, ChooseOrigin, ChooseSpec,
	ExhaustiveFact, GenericArm, Guard, MAX_CHOOSE_ARMS, ValueKindClass,
};
use crate::exec::ir::iface::conversion::{
	EliminationReason, ExprDialect, LOGICAL_PHASE, LegalityTarget, LogicalDialect, PHYSICAL_PHASE,
	Phase, PhysDialect,
};
use crate::exec::ir::iface::effects::{ElemPath, Multiplicity};
use crate::exec::ir::iface::fold::{
	FoldConsumer, FoldOutcome, FoldRequest, NotYetFolder, PlanTimeFolder,
};
use crate::exec::ir::iface::frame::{AuthorityFrame, FrameTable, Snapshot};
use crate::exec::ir::iface::identity::{AttrDigest, ValueKey};
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, RegionId, SnapshotId, ValueId};
use crate::exec::ir::iface::r#loop::{
	ApplyCut, Exit, ExitCause, LoopMode, LoopRefusal, SourceDelivery,
};
use crate::exec::ir::iface::policy::LoopFacts;
use crate::exec::ir::iface::premise::{CatalogObjectKey, CatalogVersionVector, PremiseSet};
use crate::exec::ir::iface::region_kind::{
	ErrorScope, RegionKind, StatementCommit, StoredRole, TxnKind,
};
use crate::exec::ir::iface::statements::{OutputShape, WriteFlags, WriteForm};
use crate::exec::ir::iface::types::{FactSource, HintSource, TokenClass, TypeKnowledge};
use crate::exec::ir::iface::unit::UnitKind;
use crate::exec::ir::logical::ops::{
	LogOp, LogTag, LoopKind, LoopSink, SourceOrder, TxnRegionMode, WriteAttrs,
};
use crate::exec::ir::logical::verify::timestamp_arg;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::phys::ops::{
	MaterializeImage, PhysOp, PhysTag, PhysWriteAttrs, PipelineAttrs, PipelineId, PrefetchKeys,
	WriteSubjectsShape,
};
use crate::exec::ir::phys::placement::place_for_test;
use crate::exec::logical::footprint::CatalogKind;
use crate::exec::logical::pipelines::{
	Arrival, ChainLegality, ErrorOrder, Legality, Paging, SinkCombine, SourceSplit,
};
use crate::exec::planner::placement::{
	Combine, Decision, MorselInput, NestedUse, PhysicalFacts, SinkKind, SourceKind,
};
use crate::exec::vm::compile_cache::ObjectVersion;
use crate::exec::vm::program::UnitFrame;
use crate::expr::Kind;
use crate::expr::logical::{EvalFrame, GuardKind};
use crate::val::{Number, Value};

fn request() -> Ir {
	Ir::new(
		IrScope::Request {
			fragment: None,
		},
		RegionKind::Request,
	)
}

fn top_block(ir: &Ir) -> BlockId {
	ir.region(ir.top()).expect("the top region exists").blocks[0]
}

fn literal(value: i64) -> OpKind {
	OpKind::Expr(ExprOp::Literal(Box::new(Value::from(value))))
}

fn yield_kind() -> OpKind {
	OpKind::Expr(ExprOp::Yield)
}

fn stmt_kind() -> OpKind {
	OpKind::Logical(LogOp::Stmt {
		commit: StatementCommit::Auto,
		control: None,
	})
}

fn spec(kind: OpKind, operands: &[ValueId], results: usize) -> OpSpec {
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

/// Creates `kind` at `at` and answers the op and its results.
fn add(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	kind: OpKind,
	operands: &[ValueId],
	results: usize,
) -> (OpId, SmallVec<[ValueId; 2]>) {
	let op = rw.create(at, spec(kind, operands, results)).expect("the op is created");
	let values = rw.ir().op(op).expect("the op exists").results.clone();
	(op, values)
}

/// Runs `change` on a rewriter over `ir` with a fresh fact cache.
fn edit<R>(ir: &mut Ir, change: impl FnOnce(&mut Rewriter<'_>) -> R) -> R {
	let mut facts = FactCache::new();
	let mut rw = Rewriter::new(ir, &mut facts);
	change(&mut rw)
}

fn provenance_of(ir: &Ir, op: OpId) -> Provenance {
	ir.op(op).expect("the op exists").provenance.clone()
}

fn from(ids: &[OpId]) -> Provenance {
	let mut sorted = ids.to_vec();
	sorted.sort_unstable();
	Provenance::From(sorted.into_iter().collect())
}

#[test]
fn create_records_uses_in_operand_order() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, b, user) = edit(&mut ir, |rw| {
		let (_, a) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (_, b) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let (user, _) = add(rw, InsertPoint::End(block), yield_kind(), &[b[0], a[0], b[0]], 0);
		(a[0], b[0], user)
	});
	assert_eq!(
		ir.uses(a).to_vec(),
		vec![Use {
			user,
			operand: 1
		}]
	);
	assert_eq!(
		ir.uses(b).to_vec(),
		vec![
			Use {
				user,
				operand: 0
			},
			Use {
				user,
				operand: 2
			}
		]
	);
	assert_eq!(ir.op(user).expect("the op exists").operands.to_vec(), vec![b, a, b]);
}

#[test]
fn create_positions_the_op_at_the_insert_point() {
	let mut ir = request();
	let block = top_block(&ir);
	let (first, last, before, after) = edit(&mut ir, |rw| {
		let (first, _) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (last, _) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let (before, _) = add(rw, InsertPoint::Before(last), literal(3), &[], 1);
		let (after, _) = add(rw, InsertPoint::After(first), literal(4), &[], 1);
		(first, last, before, after)
	});
	assert_eq!(ir.block(block).expect("the block exists").ops, vec![first, after, before, last]);
}

#[test]
fn create_accepts_ids_outside_tables() {
	let mut ir = request();
	let block = top_block(&ir);
	let mut outside = spec(literal(1), &[], 1);
	outside.frame = FrameId::new(9);
	let op = edit(&mut ir, |rw| rw.create(InsertPoint::End(block), outside))
		.expect("the rewriter does not check frame ids");
	assert_eq!(ir.op_outside_tables(), Some(op));
}

#[test]
fn a_dangling_operand_or_insert_point_creates_nothing() {
	let mut ir = request();
	let block = top_block(&ir);
	let (gone, gone_value) = edit(&mut ir, |rw| {
		let (op, values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		rw.erase(op, EliminationReason::DeadPure).expect("the op is erased");
		(op, values[0])
	});
	let ops = ir.ops.len();
	let operand = edit(&mut ir, |rw| {
		rw.create(InsertPoint::End(block), spec(yield_kind(), &[gone_value], 0))
	});
	assert!(matches!(operand, Err(IrError::Stale(_))));
	let point = edit(&mut ir, |rw| rw.create(InsertPoint::After(gone), spec(literal(2), &[], 1)));
	assert!(matches!(point, Err(IrError::Stale(_))));
	assert_eq!(ir.ops.len(), ops);
	assert_eq!(ir.values.len(), 0);
}

#[test]
fn create_bumps_every_ancestor() {
	let mut ir = request();
	let top = top_block(&ir);
	let (outer, inner, innermost) = edit(&mut ir, |rw| {
		let (outer, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, outer_block) = rw
			.create_region(outer, RegionKind::Statement(StatementCommit::Auto), &[timestamp_arg()])
			.expect("the region is created");
		let (inner, _) = add(rw, InsertPoint::End(outer_block), stmt_kind(), &[], 0);
		let (_, innermost) =
			rw.create_region(inner, RegionKind::Block, &[]).expect("the region is created");
		(outer, inner, innermost)
	});
	let (outer_before, inner_before) = (ir.generation(outer), ir.generation(inner));
	let leaf = edit(&mut ir, |rw| add(rw, InsertPoint::End(innermost), literal(1), &[], 1).0);
	assert_eq!(ir.generation(leaf), 1);
	assert_eq!(ir.generation(inner), inner_before + 1);
	assert_eq!(ir.generation(outer), outer_before + 1);
}

#[test]
fn create_region_types_block_arguments_and_bumps_the_owner() {
	let mut ir = request();
	let top = top_block(&ir);
	let owner = edit(&mut ir, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0);
	let before = ir.generation(owner);
	let args = [TypeKnowledge::Unknown, TypeKnowledge::Unknown];
	let (region, block) = edit(&mut ir, |rw| {
		rw.create_region(owner, RegionKind::Block, &args).expect("the region is created")
	});
	assert_eq!(ir.generation(owner), before + 1);
	assert_eq!(ir.op(owner).expect("the op exists").regions.to_vec(), vec![region]);
	assert_eq!(ir.parent_op(region), Some(owner));
	let held = ir.block(block).expect("the block exists");
	assert!(held.ops.is_empty());
	assert_eq!(held.args.len(), 2);
	edit(&mut ir, |rw| rw.erase(owner, EliminationReason::DeadPure)).expect("the op is erased");
	let ops = ir.ops.len();
	let failure = edit(&mut ir, |rw| rw.create_region(owner, RegionKind::Block, &[]));
	assert!(matches!(failure, Err(IrError::Stale(_))));
	assert_eq!(ir.ops.len(), ops);
	assert_eq!(ir.regions.len(), 1, "only the top region remains");
}

#[test]
fn set_operand_moves_the_use_and_a_bad_index_changes_nothing() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, b, user) = edit(&mut ir, |rw| {
		let (_, a) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (_, b) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let (user, _) = add(rw, InsertPoint::End(block), yield_kind(), &[a[0]], 0);
		(a[0], b[0], user)
	});
	let before = ir.generation(user);
	let failure = edit(&mut ir, |rw| rw.set_operand(user, 1, b));
	assert!(matches!(failure, Err(IrError::Stale("operand index"))));
	assert_eq!(ir.generation(user), before);
	assert_eq!(ir.uses(a).len(), 1);
	edit(&mut ir, |rw| rw.set_operand(user, 0, b)).expect("the operand is set");
	assert!(ir.uses(a).is_empty());
	assert_eq!(
		ir.uses(b).to_vec(),
		vec![Use {
			user,
			operand: 0
		}]
	);
	assert_eq!(ir.op(user).expect("the op exists").operands.to_vec(), vec![b]);
	assert_eq!(ir.generation(user), before + 1);
}

#[test]
fn replace_all_uses_moves_every_use_after_the_existing_ones() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, b, first, second) = edit(&mut ir, |rw| {
		let (_, a) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (_, b) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let (first, _) = add(rw, InsertPoint::End(block), yield_kind(), &[b[0]], 0);
		let (second, _) = add(rw, InsertPoint::End(block), yield_kind(), &[a[0], a[0]], 0);
		(a[0], b[0], first, second)
	});
	edit(&mut ir, |rw| rw.replace_all_uses(a, b)).expect("the uses are moved");
	assert!(ir.uses(a).is_empty());
	assert_eq!(
		ir.uses(b).to_vec(),
		vec![
			Use {
				user: first,
				operand: 0
			},
			Use {
				user: second,
				operand: 0
			},
			Use {
				user: second,
				operand: 1
			}
		]
	);
	assert_eq!(ir.op(second).expect("the op exists").operands.to_vec(), vec![b, b]);
}

#[test]
fn a_failed_erase_changes_nothing() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, a_value, user) = edit(&mut ir, |rw| {
		let (a, values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (user, _) = add(rw, InsertPoint::End(block), yield_kind(), &[values[0]], 0);
		(a, values[0], user)
	});
	let snapshot = |ir: &Ir| {
		(
			ir.generation(a),
			ir.generation(user),
			ir.uses(a_value).to_vec(),
			ir.holders.clone(),
			ir.eliminated().to_vec(),
			ir.ops.len(),
			ir.block(block).expect("the block exists").ops.clone(),
		)
	};
	let before = snapshot(&ir);
	let failure = edit(&mut ir, |rw| rw.erase(a, EliminationReason::DeadPure));
	assert!(matches!(failure, Err(IrError::HasUses(op)) if op == a));
	assert!(snapshot(&ir) == before);
}

#[test]
fn erase_removes_nested_regions_and_their_outer_uses() {
	let mut ir = request();
	let top = top_block(&ir);
	let (kept, kept_value, stmt, region, block, arg, inner, inner_value) = edit(&mut ir, |rw| {
		let (kept, values) = add(rw, InsertPoint::End(top), literal(1), &[], 1);
		let (stmt, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (region, block) = rw
			.create_region(
				stmt,
				RegionKind::Statement(StatementCommit::Auto),
				&[TypeKnowledge::Unknown],
			)
			.expect("the region is created");
		let (inner, inner_values) = add(rw, InsertPoint::End(block), yield_kind(), &[values[0]], 1);
		let arg = rw.ir().block(block).expect("the block exists").args[0];
		(kept, values[0], stmt, region, block, arg, inner, inner_values[0])
	});
	assert_eq!(ir.uses(kept_value).len(), 1);
	edit(&mut ir, |rw| rw.erase(stmt, EliminationReason::DeadPure)).expect("the op is erased");
	assert!(ir.uses(kept_value).is_empty());
	assert!(ir.op(stmt).is_none());
	assert!(ir.op(inner).is_none());
	assert!(ir.region(region).is_none());
	assert!(ir.block(block).is_none());
	assert!(ir.value(arg).is_none());
	assert!(ir.value(inner_value).is_none());
	assert_eq!(ir.block(top).expect("the block exists").ops, vec![kept]);
	assert_eq!(
		ir.eliminated().to_vec(),
		vec![(inner, EliminationReason::DeadPure), (stmt, EliminationReason::DeadPure)]
	);
}

#[test]
fn erasing_a_region_whose_result_is_read_outside_changes_nothing() {
	let mut ir = request();
	let top = top_block(&ir);
	let (stmt, inner, inner_value) = edit(&mut ir, |rw| {
		let (stmt, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, block) = rw
			.create_region(stmt, RegionKind::Statement(StatementCommit::Auto), &[timestamp_arg()])
			.expect("the region is created");
		let (inner, values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		add(rw, InsertPoint::End(top), yield_kind(), &[values[0]], 0);
		(stmt, inner, values[0])
	});
	let ops = ir.ops.len();
	let failure = edit(&mut ir, |rw| rw.erase(stmt, EliminationReason::DeadPure));
	assert!(matches!(failure, Err(IrError::HasUses(op)) if op == inner));
	assert_eq!(ir.ops.len(), ops);
	assert_eq!(ir.uses(inner_value).len(), 1);
	assert!(ir.eliminated().is_empty());
}

#[test]
fn erase_records_unheld_ids_only() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, derived, lone) = edit(&mut ir, |rw| {
		let (a, _) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (lone, _) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let derived = rw.derived(&[a], |rw| add(rw, InsertPoint::End(block), literal(3), &[], 1).0);
		(a, derived, lone)
	});
	assert_eq!(provenance_of(&ir, derived), from(&[a]));
	edit(&mut ir, |rw| rw.erase(a, EliminationReason::DeadPure)).expect("the op is erased");
	assert!(ir.eliminated().is_empty(), "the derived op still holds the id");
	edit(&mut ir, |rw| rw.erase(derived, EliminationReason::FoldedConstant))
		.expect("the op is erased");
	assert_eq!(ir.eliminated().to_vec(), vec![(a, EliminationReason::FoldedConstant)]);
	edit(&mut ir, |rw| rw.erase(lone, EliminationReason::Unreachable)).expect("the op is erased");
	assert_eq!(
		ir.eliminated().to_vec(),
		vec![(a, EliminationReason::FoldedConstant), (lone, EliminationReason::Unreachable)]
	);
	assert!(ir.holders.is_empty());
}

/// Two literals and an op reading the data as of the second one, in the top block.
fn versioned_reader() -> (Ir, [ValueId; 2], OpId, SnapshotId) {
	let mut ir = request();
	let block = top_block(&ir);
	let (values, reader, snapshot) = edit(&mut ir, |rw| {
		let (_, first) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (_, second) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let snapshot = rw.intern_snapshot(Snapshot::Version(second[0])).expect("interned");
		let mut read = spec(literal(2), &[], 1);
		read.snapshot = snapshot;
		let reader = rw.create(InsertPoint::End(block), read).expect("the op is created");
		([first[0], second[0]], reader, snapshot)
	});
	(ir, values, reader, snapshot)
}

#[test]
fn replace_all_uses_rewrites_a_snapshot_naming_the_value() {
	let (mut ir, [first, second], reader, snapshot) = versioned_reader();
	let before = ir.generation(reader);
	edit(&mut ir, |rw| rw.replace_all_uses(second, first)).expect("the uses are replaced");
	assert_eq!(ir.snapshot(snapshot), Some(&Snapshot::Version(first)));
	assert_ne!(ir.generation(reader), before, "the reader of the snapshot is bumped");
}

#[test]
fn replace_op_keeps_a_snapshot_on_a_live_value() {
	let (mut ir, [first, second], reader, snapshot) = versioned_reader();
	let definer = ir.value_owner(second).expect("the value has a definer");
	edit(&mut ir, |rw| rw.replace_op(definer, &[first])).expect("the op is replaced");
	assert_eq!(ir.snapshot(snapshot), Some(&Snapshot::Version(first)));
	assert!(ir.value(second).is_none());
	assert_eq!(ir.op(reader).map(|data| data.snapshot), Some(snapshot));
}

#[test]
fn erase_refuses_a_value_a_snapshot_names_while_an_op_reads_it() {
	let (mut ir, [_, second], reader, _) = versioned_reader();
	let definer = ir.value_owner(second).expect("the value has a definer");
	let failure = edit(&mut ir, |rw| rw.erase(definer, EliminationReason::DeadPure));
	assert!(matches!(failure, Err(IrError::HasUses(op)) if op == definer));
	assert!(ir.value(second).is_some());
	edit(&mut ir, |rw| rw.erase(reader, EliminationReason::DeadPure)).expect("the reader goes");
	edit(&mut ir, |rw| rw.erase(definer, EliminationReason::DeadPure))
		.expect("nothing reads the snapshot any more");
}

#[test]
fn a_snapshot_rewritten_to_a_repeat_prints_and_reads_back() {
	let registry = text_registry();
	let (mut ir, [first, second], _, _) = versioned_reader();
	edit(&mut ir, |rw| {
		let snapshot = rw.intern_snapshot(Snapshot::Version(first)).expect("interned");
		let block = top_block(rw.ir());
		let mut read = spec(literal(3), &[], 1);
		read.snapshot = snapshot;
		rw.create(InsertPoint::End(block), read).expect("the op is created");
		rw.replace_all_uses(second, first).expect("the uses are replaced");
	});
	assert_eq!(ir.snapshots.len(), 3);
	let text = text_of(&ir, &registry);
	let parsed = reparse(&text, &registry).unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	assert_eq!(text_of(&parsed, &registry), text);
}

#[test]
fn update_kind_restores_on_tag_change() {
	let mut ir = request();
	let block = top_block(&ir);
	let op = edit(&mut ir, |rw| add(rw, InsertPoint::End(block), literal(1), &[], 1).0);
	let before = ir.generation(op);
	let failure = edit(&mut ir, |rw| rw.update_kind(op, |kind| *kind = yield_kind()));
	assert!(matches!(failure, Err(IrError::TagChanged(held)) if held == op));
	assert_eq!(ir.generation(op), before);
	let is_literal = |ir: &Ir, expected: i64| {
		matches!(
			&ir.op(op).expect("the op exists").kind,
			OpKind::Expr(ExprOp::Literal(value)) if **value == Value::from(expected)
		)
	};
	assert!(is_literal(&ir, 1));
	edit(&mut ir, |rw| rw.update_kind(op, |kind| *kind = literal(7))).expect("the kind is updated");
	assert!(is_literal(&ir, 7));
	assert_eq!(ir.generation(op), before + 1);
}

#[test]
fn derived_unions_nested_scopes() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, b, outer, inner, after_inner, after) = edit(&mut ir, |rw| {
		let (a, _) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (b, _) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let at = InsertPoint::End(block);
		let (outer, inner, after_inner) = rw.derived(&[a], |rw| {
			let outer = add(rw, at, literal(3), &[], 1).0;
			let inner = rw.derived(&[b], |rw| add(rw, at, literal(4), &[], 1).0);
			let after_inner = add(rw, at, literal(5), &[], 1).0;
			(outer, inner, after_inner)
		});
		let after = add(rw, at, literal(6), &[], 1).0;
		(a, b, outer, inner, after_inner, after)
	});
	assert_eq!(provenance_of(&ir, outer), from(&[a]));
	assert_eq!(provenance_of(&ir, inner), from(&[a, b]));
	assert_eq!(provenance_of(&ir, after_inner), from(&[a]));
	assert_eq!(provenance_of(&ir, after), Provenance::Bound);
}

#[test]
fn derived_from_a_derived_op_takes_the_ids_it_stands_for() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, first, second) = edit(&mut ir, |rw| {
		let (a, _) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let at = InsertPoint::End(block);
		let first = rw.derived(&[a], |rw| add(rw, at, literal(2), &[], 1).0);
		let second = rw.derived(&[first], |rw| add(rw, at, literal(3), &[], 1).0);
		(a, first, second)
	});
	assert_eq!(provenance_of(&ir, first), from(&[a]));
	assert_eq!(provenance_of(&ir, second), from(&[a]));
}

#[test]
fn create_records_the_error_scope_owner_and_keeps_it_after_erase() {
	let mut ir = request();
	let top = top_block(&ir);
	let (owner, inside, outside) = edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, block) = rw
			.create_region(owner, RegionKind::ErrorScope(ErrorScope::ShapeToNone), &[])
			.expect("the region is created");
		let (inside, _) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (outside, _) = add(rw, InsertPoint::End(top), literal(2), &[], 1);
		(owner, inside, outside)
	});
	assert_eq!(ir.bind_scope.get(inside), Some(&Some(owner)));
	assert_eq!(ir.bind_scope.get(outside), Some(&None));
	edit(&mut ir, |rw| rw.erase(inside, EliminationReason::DeadPure)).expect("the op is erased");
	assert_eq!(ir.bind_scope.get(inside), None);
	assert_eq!(ir.retired_scope.get(&inside), Some(&Some(owner)));
}

#[test]
fn logical_ops_are_listed_in_creation_order() {
	let mut ir = request();
	let top = top_block(&ir);
	let (first, second) = edit(&mut ir, |rw| {
		let (first, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		add(rw, InsertPoint::End(top), literal(1), &[], 1);
		let (second, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		(first, second)
	});
	assert_eq!(ir.logical_ids, vec![first, second]);
}

#[test]
fn a_logical_op_made_under_derived_is_not_a_logical_id_and_may_be_erased() {
	let mut ir = request();
	let top = top_block(&ir);
	let source = edit(&mut ir, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0);
	let made = edit(&mut ir, |rw| {
		rw.derived(&[source], |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0)
	});
	assert_eq!(ir.logical_ids, vec![source]);
	edit(&mut ir, |rw| rw.erase(made, EliminationReason::DeadPure)).expect("the op is erased");
	assert!(physical_boundary_violation(&ir, VerifyRule::Provenance).is_none());
}

#[test]
fn take_created_answers_the_ops_created_since_the_last_take() {
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		let (first, _) = add(rw, InsertPoint::End(top), literal(1), &[], 1);
		let (second, _) = add(rw, InsertPoint::End(top), literal(2), &[], 1);
		assert_eq!(rw.take_created(), vec![first, second]);
		assert!(rw.take_created().is_empty());
		let (third, _) = add(rw, InsertPoint::End(top), literal(3), &[], 1);
		assert_eq!(rw.take_created(), vec![third]);
	});
}

#[test]
fn facts_are_recomputed_after_an_edit() {
	let mut ir = request();
	let top = top_block(&ir);
	let stmt = edit(&mut ir, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0);
	let (_, block) = edit(&mut ir, |rw| {
		rw.create_region(stmt, RegionKind::Statement(StatementCommit::Auto), &[timestamp_arg()])
			.expect("the region is created")
	});
	let mut facts = FactCache::new();
	assert!(facts.get(stmt, ir.generation(stmt), &default_key(&ir)).is_none());
	Rewriter::new(&mut ir, &mut facts)
		.with_registry(crate::exec::ir::analysis::cx::builtin_registry())
		.facts(stmt);
	assert!(facts.get(stmt, ir.generation(stmt), &default_key(&ir)).is_some());
	{
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		add(&mut rw, InsertPoint::End(block), literal(1), &[], 1);
	}
	assert!(
		facts.get(stmt, ir.generation(stmt), &default_key(&ir)).is_none(),
		"the edit made the entry stale"
	);
	Rewriter::new(&mut ir, &mut facts)
		.with_registry(crate::exec::ir::analysis::cx::builtin_registry())
		.facts(stmt);
	assert!(facts.get(stmt, ir.generation(stmt), &default_key(&ir)).is_some());
}

#[test]
fn a_dangling_op_answers_the_top_facts_and_an_erased_op_keeps_its_last_facts() {
	let mut ir = request();
	let top = top_block(&ir);
	let op = edit(&mut ir, |rw| add(rw, InsertPoint::End(top), literal(1), &[], 1).0);
	let mut facts = FactCache::new();
	{
		let mut rw = Rewriter::new(&mut ir, &mut facts)
			.with_registry(crate::exec::ir::analysis::cx::builtin_registry());
		rw.facts(op);
		rw.erase(op, EliminationReason::DeadPure).expect("the op is erased");
		rw.facts(op);
	}
	assert!(facts.get(op, ir.generation(op), &default_key(&ir)).is_none());
	assert!(facts.last_under(op, &default_key(&ir)).is_some());
}

#[test]
fn the_cache_slots_answer_only_at_the_generation_they_were_set() {
	let mut ir = request();
	let top = top_block(&ir);
	let (op, value) = edit(&mut ir, |rw| {
		let (op, values) = add(rw, InsertPoint::End(top), literal(1), &[], 1);
		(op, values[0])
	});
	let mut facts = FactCache::new();
	facts.set_type(value, 3, TypeKnowledge::Unknown);
	assert!(facts.type_of(value, 3).is_some());
	assert!(facts.type_of(value, 4).is_none());
	facts.set_invariance(value, op, 3, 2);
	assert_eq!(facts.invariance(value, op, 3), Some(2));
	assert_eq!(facts.invariance(value, op, 4), None);
	facts.set_invariance(value, op, 4, 1);
	assert_eq!(facts.invariance(value, op, 3), None);
	assert_eq!(facts.invariance(value, op, 4), Some(1));
	assert!(facts.legality_snapshot().is_none());
	facts.set_legality_snapshot(Default::default());
	assert!(facts.legality_snapshot().is_some_and(|slot| slot.is_empty()));
	facts.invalidate_value(value);
	assert!(facts.type_of(value, 3).is_none());
	assert_eq!(facts.invariance(value, op, 4), None);
}

#[test]
fn advance_phase_is_monotonic_and_seals_stored_clauses() {
	let mut ir = request();
	let top = top_block(&ir);
	let (owner, inside, nested, deep) = edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, block) = rw
			.create_region(owner, RegionKind::StoredClause(StoredRole::Value), &[])
			.expect("the region is created");
		let (inside, _) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (nested, _) = add(rw, InsertPoint::End(block), stmt_kind(), &[], 0);
		let (_, nested_block) =
			rw.create_region(nested, RegionKind::Block, &[]).expect("the region is created");
		let (deep, _) = add(rw, InsertPoint::End(nested_block), literal(2), &[], 1);
		(owner, inside, nested, deep)
	});
	assert_eq!(ir.phase(), Phase::Bound);
	assert!(ir.sealed.is_empty());
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	assert_eq!(ir.phase(), Phase::Logical);
	for op in [inside, nested, deep] {
		assert_eq!(ir.sealed.get(op), Some(&ir.generation(op)));
	}
	assert_eq!(ir.sealed.get(owner), None);
	let failure = edit(&mut ir, |rw| rw.advance_phase(Phase::Bound));
	assert!(matches!(failure, Err(IrError::Refused("phase regress"))));
	assert_eq!(ir.phase(), Phase::Logical);
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the same phase is accepted");
	edit(&mut ir, |rw| rw.update_kind(inside, |kind| *kind = literal(9))).expect("kind updated");
	edit(&mut ir, |rw| rw.advance_phase(Phase::Physical)).expect("the phase advances");
	assert_eq!(ir.phase(), Phase::Physical);
	assert_ne!(
		ir.sealed.get(inside),
		Some(&ir.generation(inside)),
		"the seal records the generation at the move out of Bound only"
	);
}

#[test]
fn the_placement_table_is_reachable_without_the_rewriter() {
	let mut ir = request();
	let top = top_block(&ir);
	let op = edit(&mut ir, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0);
	let before = ir.generation(op);
	assert!(ir.placement_table_mut().get(op).is_none());
	assert_eq!(ir.generation(op), before);
}

/// Creates `kind` at the end of `block` with a location of `origin`.
fn add_located(rw: &mut Rewriter<'_>, block: BlockId, kind: OpKind, origin: LocOrigin) -> OpId {
	let mut described = spec(kind, &[], 0);
	described.loc.origin = origin;
	rw.create(InsertPoint::End(block), described).expect("the op is created")
}

#[test]
fn replace_op_unions_provenance_bound_and_from() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, b, user, b_value) = edit(&mut ir, |rw| {
		let (a, a_values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (b, b_values) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let (user, _) = add(rw, InsertPoint::End(block), yield_kind(), &[a_values[0]], 0);
		(a, b, user, b_values[0])
	});
	edit(&mut ir, |rw| rw.replace_op(a, &[b_value])).expect("the op is replaced");
	assert!(ir.op(a).is_none());
	assert_eq!(ir.op(user).expect("the op exists").operands.to_vec(), vec![b_value]);
	assert_eq!(provenance_of(&ir, b), from(&[a, b]), "a Bound definer contributes its own id");
	assert!(ir.eliminated().is_empty(), "the replaced op's id is held by its replacement");
	assert_eq!(ir.holders.get(&a), Some(&1));
	assert_eq!(ir.holders.get(&b), Some(&1));
	let (x, d, e, d_value) = edit(&mut ir, |rw| {
		let (x, _) = add(rw, InsertPoint::End(block), literal(3), &[], 1);
		let (d, d_values) =
			rw.derived(&[x], |rw| add(rw, InsertPoint::End(block), literal(4), &[], 1));
		let (e, _) = add(rw, InsertPoint::End(block), literal(5), &[], 1);
		(x, d, e, d_values[0])
	});
	edit(&mut ir, |rw| rw.replace_op(e, &[d_value])).expect("the op is replaced");
	assert_eq!(provenance_of(&ir, d), from(&[x, e]));
	assert_eq!(ir.holders.get(&x), Some(&2));
	assert_eq!(ir.holders.get(&e), Some(&1));
	edit(&mut ir, |rw| rw.replace_op(e, &[d_value]))
		.expect_err("an erased op cannot be replaced again");
}

#[test]
fn replace_op_fails_before_changing_anything() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, a_value, b_value, user) = edit(&mut ir, |rw| {
		let (a, a_values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (_, b_values) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let (user, _) = add(rw, InsertPoint::End(block), yield_kind(), &[a_values[0]], 0);
		(a, a_values[0], b_values[0], user)
	});
	let before = (ir.uses(a_value).to_vec(), ir.generation(a), ir.generation(user), ir.ops.len());
	let wrong_arity = edit(&mut ir, |rw| rw.replace_op(a, &[b_value, b_value]));
	assert!(matches!(wrong_arity, Err(IrError::Refused("replace_op arity"))));
	let itself = edit(&mut ir, |rw| rw.replace_op(a, &[a_value]));
	assert!(matches!(itself, Err(IrError::Refused("replace_op inside"))));
	assert_eq!(
		(ir.uses(a_value).to_vec(), ir.generation(a), ir.generation(user), ir.ops.len()),
		before
	);
}

#[test]
fn replace_op_with_a_block_argument_records_merged_into() {
	let mut ir = request();
	let top = top_block(&ir);
	let (owner, inner, argument, user) = edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, block) = rw
			.create_region(owner, RegionKind::Block, &[TypeKnowledge::Unknown])
			.expect("the region is created");
		let argument = rw.ir().block(block).expect("the block exists").args[0];
		let (inner, values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (user, _) = add(rw, InsertPoint::End(block), yield_kind(), &[values[0]], 0);
		(owner, inner, argument, user)
	});
	edit(&mut ir, |rw| rw.replace_op(inner, &[argument])).expect("the op is replaced");
	assert!(ir.op(inner).is_none());
	assert_eq!(ir.op(user).expect("the op exists").operands.to_vec(), vec![argument]);
	assert_eq!(ir.eliminated().to_vec(), vec![(inner, EliminationReason::MergedInto(owner))]);
	assert!(!ir.holders.contains_key(&inner));
}

#[test]
fn replace_all_uses_is_idempotent_on_self() {
	let mut ir = request();
	let block = top_block(&ir);
	let (value, user) = edit(&mut ir, |rw| {
		let (_, values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (user, _) = add(rw, InsertPoint::End(block), yield_kind(), &[values[0]], 0);
		(values[0], user)
	});
	let before = (ir.uses(value).to_vec(), ir.generation(user));
	let log = edit(&mut ir, |rw| {
		rw.record_replacements();
		rw.replace_all_uses(value, value).expect("a self replacement succeeds");
		rw.take_replacements()
	});
	assert!((ir.uses(value).to_vec(), ir.generation(user)) == before);
	assert!(log.is_empty(), "a replacement of a value by itself is not logged");
}

#[test]
fn replacement_log_records_only_while_on() {
	let mut ir = request();
	let block = top_block(&ir);
	let (a, b, c, e) = edit(&mut ir, |rw| {
		let (_, a) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (_, b) = add(rw, InsertPoint::End(block), literal(2), &[], 1);
		let (_, c) = add(rw, InsertPoint::End(block), literal(3), &[], 1);
		let (e, _) = add(rw, InsertPoint::End(block), literal(4), &[], 1);
		(a[0], b[0], c[0], e)
	});
	let e_value = ir.op(e).expect("the op exists").results[0];
	let (before, during, after) = edit(&mut ir, |rw| {
		rw.replace_all_uses(a, b).expect("the uses move");
		let before = rw.take_replacements();
		rw.record_replacements();
		rw.replace_all_uses(b, c).expect("the uses move");
		rw.replace_op(e, &[c]).expect("the op is replaced");
		let during = rw.take_replacements();
		rw.replace_all_uses(c, b).expect("the uses move");
		let after = rw.take_replacements();
		(before, during, after)
	});
	assert!(before.is_empty());
	assert_eq!(during, vec![(b, c), (e_value, c)]);
	assert!(after.is_empty());
}

#[test]
fn inline_region_binds_arguments_and_returns_yields() {
	let mut ir = request();
	let top = top_block(&ir);
	let (owner, region, outer, outer_value, x, x_value, z, yield_op) = edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (outer, outer_values) = add(rw, InsertPoint::End(top), literal(9), &[], 1);
		let (region, block) = rw
			.create_region(owner, RegionKind::ValueBlock, &[TypeKnowledge::Unknown])
			.expect("the region is created");
		let argument = rw.ir().block(block).expect("the block exists").args[0];
		let (x, x_values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		let (z, _) = add(rw, InsertPoint::End(block), literal(2), &[argument], 1);
		let (yield_op, _) =
			add(rw, InsertPoint::End(block), yield_kind(), &[argument, x_values[0]], 0);
		(owner, region, outer, outer_values[0], x, x_values[0], z, yield_op)
	});
	let argument = ir
		.block(ir.region(region).expect("the region exists").blocks[0])
		.expect("the block exists")
		.args[0];
	let wrong_arity = edit(&mut ir, |rw| rw.inline_region(region, InsertPoint::End(top), &[]));
	assert!(matches!(wrong_arity, Err(IrError::Refused("inline_region arity"))));
	assert!(ir.region(region).is_some(), "a failed call changes nothing");
	let yielded =
		edit(&mut ir, |rw| rw.inline_region(region, InsertPoint::End(top), &[outer_value]))
			.expect("the region is inlined");
	assert_eq!(yielded.to_vec(), vec![outer_value, x_value]);
	assert_eq!(ir.block(top).expect("the block exists").ops, vec![owner, outer, x, z]);
	assert_eq!(ir.op(x).expect("the op exists").parent, top);
	assert_eq!(ir.op(z).expect("the op exists").operands.to_vec(), vec![outer_value]);
	assert_eq!(
		ir.uses(outer_value).to_vec(),
		vec![Use {
			user: z,
			operand: 0
		}]
	);
	assert!(ir.op(yield_op).is_none());
	assert!(ir.value(argument).is_none());
	assert!(ir.region(region).is_none());
	assert!(ir.op(owner).expect("the op exists").regions.is_empty());
	assert_eq!(ir.eliminated().to_vec(), vec![(yield_op, EliminationReason::DeadPure)]);
	assert_eq!(provenance_of(&ir, x), Provenance::Bound);
}

fn write_kind() -> OpKind {
	OpKind::Phys(PhysOp::Write(Box::new(PhysWriteAttrs {
		write: WriteAttrs {
			form: WriteForm::Create,
			only: false,
			output: OutputShape::After,
			with: None,
			explain: None,
			flags: WriteFlags::default(),
			body: Default::default(),
			gates: Default::default(),
			image: Default::default(),
			stored: Default::default(),
			derived: Default::default(),
			into_text: None,
			statement: None,
			linked: None,
		},
		subjects: WriteSubjectsShape::Targets,
	})))
}

/// An owner holding one store token in and one out, a region under it whose read takes the
/// region's store token, and the read op. Answers the owner, its region, the read, the token the
/// owner takes and the token it produces.
fn token_owner(ir: &mut Ir, kind: OpKind) -> (OpId, RegionId, OpId, ValueId, ValueId) {
	let top = top_block(ir);
	edit(ir, |rw| {
		let incoming =
			rw.add_token_arg(top, TokenClass::Store).expect("the token argument is added");
		let (owner, _) = add(rw, InsertPoint::End(top), kind, &[], 0);
		rw.add_token_operand(owner, incoming).expect("the token operand is added");
		let produced =
			rw.add_token_result(owner, TokenClass::Store).expect("the token is produced");
		let (region, block) =
			rw.create_region(owner, RegionKind::ValueBlock, &[]).expect("the region is created");
		let formal =
			rw.add_token_arg(block, TokenClass::Store).expect("the token argument is added");
		let (read, _) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		rw.add_token_operand(read, formal).expect("the token operand is added");
		add(rw, InsertPoint::End(block), yield_kind(), &[], 0);
		(owner, region, read, incoming, produced)
	})
}

#[test]
fn inline_region_of_a_write_reads_the_token_the_write_produces() {
	let mut ir = request();
	let (owner, region, read, incoming, produced) = token_owner(&mut ir, write_kind());
	let formal = ir.token_operands(read).to_vec();
	let before = edit(&mut ir, |rw| rw.inline_region(region, InsertPoint::Before(owner), &[]));
	assert!(matches!(before, Err(IrError::Refused("inline_region token"))));
	assert!(ir.region(region).is_some(), "a refused call changes nothing");
	assert_eq!(ir.token_operands(read).to_vec(), formal);
	edit(&mut ir, |rw| rw.inline_region(region, InsertPoint::After(owner), &[]))
		.expect("the region is inlined after the write");
	assert_eq!(ir.token_operands(read).to_vec(), vec![produced]);
	assert!(ir.uses(incoming).iter().all(|used| used.user == owner));
}

#[test]
fn inline_region_of_a_pure_owner_reads_the_token_the_owner_takes() {
	let mut ir = request();
	let (owner, region, read, incoming, _) = token_owner(&mut ir, stmt_kind());
	edit(&mut ir, |rw| rw.inline_region(region, InsertPoint::Before(owner), &[]))
		.expect("the region is inlined");
	assert_eq!(ir.token_operands(read).to_vec(), vec![incoming]);
}

#[test]
fn inline_region_needs_a_yield_terminator() {
	let mut ir = request();
	let top = top_block(&ir);
	let (region, owner) = edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (region, block) =
			rw.create_region(owner, RegionKind::ValueBlock, &[]).expect("the region is created");
		add(rw, InsertPoint::End(block), literal(1), &[], 1);
		(region, owner)
	});
	let failure = edit(&mut ir, |rw| rw.inline_region(region, InsertPoint::End(top), &[]));
	assert!(matches!(failure, Err(IrError::Refused("inline_region terminator"))));
	assert_eq!(ir.op(owner).expect("the op exists").regions.to_vec(), vec![region]);
	let inside = ir.region(region).expect("the region exists").blocks[0];
	let failure = edit(&mut ir, |rw| rw.inline_region(region, InsertPoint::End(inside), &[]));
	assert!(matches!(failure, Err(IrError::Refused("inline_region terminator"))));
}

#[test]
fn move_region_rejects_a_cycle() {
	let mut ir = request();
	let top = top_block(&ir);
	let (a, b, c, region) = edit(&mut ir, |rw| {
		let (a, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (c, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (region, block) =
			rw.create_region(a, RegionKind::Block, &[]).expect("the region is created");
		let (b, _) = add(rw, InsertPoint::End(block), stmt_kind(), &[], 0);
		(a, b, c, region)
	});
	let generations = (ir.generation(a), ir.generation(b), ir.generation(c));
	let under_itself = edit(&mut ir, |rw| rw.move_region(region, b));
	assert!(matches!(under_itself, Err(IrError::Refused("move_region cycle"))));
	assert_eq!(ir.op(a).expect("the op exists").regions.to_vec(), vec![region]);
	assert_eq!(ir.parent_op(region), Some(a));
	assert_eq!((ir.generation(a), ir.generation(b), ir.generation(c)), generations);
	edit(&mut ir, |rw| rw.move_region(region, c)).expect("the region moves");
	assert!(ir.op(a).expect("the op exists").regions.is_empty());
	assert_eq!(ir.op(c).expect("the op exists").regions.to_vec(), vec![region]);
	assert_eq!(ir.parent_op(region), Some(c));
	assert_eq!(ir.generation(a), generations.0 + 1);
	assert_eq!(ir.generation(c), generations.2 + 1);
}

/// A statement holding `kind` regions nested in the given order, innermost last, and the
/// regions in that order, outermost (the statement's) first.
fn nest(ir: &mut Ir, kinds: &[RegionKind]) -> (OpId, SmallVec<[RegionId; 4]>, BlockId) {
	let top = top_block(ir);
	edit(ir, |rw| {
		let (statement, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let mut owner = statement;
		let mut regions = SmallVec::new();
		let mut block = top;
		for kind in kinds {
			let (region, inner) =
				rw.create_region(owner, *kind, &[]).expect("the region is created");
			regions.push(region);
			block = inner;
			let (next, _) = add(rw, InsertPoint::End(inner), stmt_kind(), &[], 0);
			owner = next;
		}
		(statement, regions, block)
	})
}

#[test]
fn control_target_return_stops_at_the_statement() {
	let mut ir = request();
	let (statement, regions, inner) = nest(
		&mut ir,
		&[RegionKind::Statement(StatementCommit::Auto), RegionKind::Loop, RegionKind::Block],
	);
	let (ret, phys_ret, plain) = edit(&mut ir, |rw| {
		(
			add_located(rw, inner, OpKind::Logical(LogOp::Return), LocOrigin::Query),
			add_located(rw, inner, OpKind::Phys(PhysOp::Return), LocOrigin::Query),
			add_located(rw, inner, yield_kind(), LocOrigin::Query),
		)
	});
	// A block is no context: the `RETURN` passes it and the loop, and ends the statement.
	assert_eq!(control_target(&ir, ret), ControlTarget::Region(regions[0]));
	assert_eq!(control_target(&ir, phys_ret), ControlTarget::Region(regions[0]));
	assert_eq!(control_target(&ir, plain), ControlTarget::NotControl);
	assert_eq!(enclosing_statement(&ir, ret), Some(statement));
	assert_eq!(enclosing_statement(&ir, statement), None);
	let top = top_block(&ir);
	let loose =
		edit(&mut ir, |rw| add_located(rw, top, OpKind::Logical(LogOp::Return), LocOrigin::Query));
	assert_eq!(control_target(&ir, loose), ControlTarget::Escapes(ir.top()));
	assert_eq!(enclosing_statement(&ir, loose), None);
	let mut passing = request();
	let (_, regions, inner) =
		nest(&mut passing, &[RegionKind::Statement(StatementCommit::Auto), RegionKind::Loop]);
	let through_loop = edit(&mut passing, |rw| {
		add_located(rw, inner, OpKind::Logical(LogOp::Return), LocOrigin::Query)
	});
	assert_eq!(control_target(&passing, through_loop), ControlTarget::Region(regions[0]));
	let dangling = {
		let removed = edit(&mut passing, |rw| {
			let op = add_located(rw, inner, OpKind::Logical(LogOp::Return), LocOrigin::Query);
			rw.erase(op, EliminationReason::DeadPure).expect("the op is erased");
			op
		});
		control_target(&passing, removed)
	};
	assert_eq!(dangling, ControlTarget::NotControl);
}

#[test]
fn control_target_break_meets_a_barrier() {
	let mut ir = request();
	let (_, regions, inner) = nest(
		&mut ir,
		&[
			RegionKind::Statement(StatementCommit::Auto),
			RegionKind::StatementFor,
			RegionKind::StatementIf,
		],
	);
	let (exit, next) = edit(&mut ir, |rw| {
		(
			add_located(rw, inner, OpKind::Logical(LogOp::Break), LocOrigin::Query),
			add_located(rw, inner, OpKind::Phys(PhysOp::Next), LocOrigin::Query),
		)
	});
	assert_eq!(control_target(&ir, exit), ControlTarget::Region(regions[1]));
	assert_eq!(control_target(&ir, next), ControlTarget::Region(regions[1]));
	let mut barred = request();
	let (_, regions, inner) = nest(
		&mut barred,
		&[
			RegionKind::Statement(StatementCommit::Auto),
			RegionKind::StatementFor,
			RegionKind::FunctionBody,
		],
	);
	let through_function = edit(&mut barred, |rw| {
		add_located(rw, inner, OpKind::Logical(LogOp::Break), LocOrigin::Query)
	});
	assert_eq!(control_target(&barred, through_function), ControlTarget::Barrier(regions[2]));
	let mut loose = request();
	let (_, _, inner) =
		nest(&mut loose, &[RegionKind::Statement(StatementCommit::Auto), RegionKind::Loop]);
	let without_for = edit(&mut loose, |rw| {
		add_located(rw, inner, OpKind::Logical(LogOp::Next), LocOrigin::Query)
	});
	assert_eq!(control_target(&loose, without_for), ControlTarget::Barrier(loose.top()));
}

#[test]
fn control_target_synthesized_break_targets_its_loop() {
	let mut ir = request();
	let (_, regions, inner) = nest(
		&mut ir,
		&[RegionKind::Statement(StatementCommit::Auto), RegionKind::Loop, RegionKind::FunctionBody],
	);
	let (synthesized, written) = edit(&mut ir, |rw| {
		(
			add_located(rw, inner, OpKind::Logical(LogOp::Break), LocOrigin::Synthesized),
			add_located(rw, inner, OpKind::Logical(LogOp::Break), LocOrigin::Query),
		)
	});
	assert_eq!(control_target(&ir, synthesized), ControlTarget::Region(regions[1]));
	assert_eq!(control_target(&ir, written), ControlTarget::Barrier(regions[2]));
	let mut without = request();
	let (_, _, inner) = nest(&mut without, &[RegionKind::Statement(StatementCommit::Auto)]);
	let nowhere = edit(&mut without, |rw| {
		add_located(rw, inner, OpKind::Logical(LogOp::Next), LocOrigin::Synthesized)
	});
	assert_eq!(control_target(&without, nowhere), ControlTarget::Barrier(without.top()));
}

#[test]
fn dominates_respects_isolation() {
	let mut ir = request();
	let top = top_block(&ir);
	let (
		a_value,
		later_value,
		shared,
		isolated,
		user,
		user_value,
		second,
		isolated_user,
		argument,
		inner_argument,
	) = edit(&mut ir, |rw| {
		let (_, a) = add(rw, InsertPoint::End(top), literal(1), &[], 1);
		let (shared, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (isolated, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, later) = add(rw, InsertPoint::End(top), literal(2), &[], 1);
		let (_, block) = rw
			.create_region(shared, RegionKind::Block, &[TypeKnowledge::Unknown])
			.expect("the region is created");
		let argument = rw.ir().block(block).expect("the block exists").args[0];
		let (user, user_values) = add(rw, InsertPoint::End(block), literal(3), &[a[0]], 1);
		let (second, _) = add(rw, InsertPoint::End(block), literal(4), &[user_values[0]], 0);
		let (_, body) = rw
			.create_region(isolated, RegionKind::FunctionBody, &[TypeKnowledge::Unknown])
			.expect("the region is created");
		let inner_argument = rw.ir().block(body).expect("the block exists").args[0];
		let (isolated_user, _) = add(rw, InsertPoint::End(body), literal(5), &[inner_argument], 0);
		(
			a[0],
			later[0],
			shared,
			isolated,
			user,
			user_values[0],
			second,
			isolated_user,
			argument,
			inner_argument,
		)
	});
	assert!(ir.dominates(a_value, user), "an enclosing block's earlier op dominates");
	assert!(!ir.dominates(a_value, isolated_user), "a value never reaches into an isolated region");
	assert!(!ir.dominates(later_value, user), "a definition after the enclosing op does not");
	assert!(ir.dominates(a_value, isolated), "the op that owns the isolated region still reads it");
	assert!(ir.dominates(argument, user));
	assert!(ir.dominates(argument, second));
	assert!(!ir.dominates(argument, isolated_user));
	assert!(ir.dominates(inner_argument, isolated_user), "a block argument reaches its own block");
	assert!(!ir.dominates(inner_argument, user));
	assert!(ir.dominates(user_value, second), "an earlier op of the same block dominates");
	assert!(!ir.dominates(user_value, user), "an op does not dominate itself");
	assert!(!ir.dominates(user_value, shared), "a value does not escape its region");
	let removed = edit(&mut ir, |rw| {
		let (op, _) = add(rw, InsertPoint::End(top), literal(6), &[], 1);
		rw.erase(op, EliminationReason::DeadPure).expect("the op is erased");
		op
	});
	assert!(!ir.dominates(a_value, removed), "a dangling user is dominated by nothing");
}

#[test]
fn value_table_never_shares_opaque_keys_or_across_statements() {
	let mut ir = request();
	let top = top_block(&ir);
	let (first, second, values) = edit(&mut ir, |rw| {
		let (first, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (second, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let values: Vec<ValueId> =
			(0..4).map(|at| add(rw, InsertPoint::End(top), literal(at), &[], 1).1[0]).collect();
		(first, second, values)
	});
	let key = |digest: u8, frame: u16, opaque: Option<OpId>| ValueKey {
		tag: literal(0).tag(),
		attrs: AttrDigest([digest; 16]),
		operands: SmallVec::new(),
		tokens: SmallVec::new(),
		frame: FrameId::new(frame),
		snapshot: SnapshotId::new(0),
		epoch: None,
		opaque,
	};
	let mut table = ValueTable::new();
	assert!(table.is_empty() && table.statement().is_none());
	table.enter_statement(Some(first));
	assert_eq!(table.lookup_or_insert(key(1, 0, None), values[0], |_| true), None);
	assert_eq!(table.lookup_or_insert(key(1, 0, None), values[1], |_| true), Some(values[0]));
	assert_eq!(
		table.lookup_or_insert(key(1, 1, None), values[2], |_| true),
		None,
		"frame is part of the key"
	);
	assert_eq!(table.lookup_or_insert(key(2, 0, None), values[3], |_| true), None);
	assert_eq!(table.len(), 3);
	for _ in 0..2 {
		assert_eq!(table.lookup_or_insert(key(3, 0, Some(first)), values[0], |_| true), None);
		assert_eq!(table.lookup_or_insert(key(0xFF, 0, None), values[0], |_| true), None);
	}
	assert_eq!(table.len(), 3, "an opaque key is never held");
	table.enter_statement(Some(first));
	assert_eq!(table.len(), 3, "re-entering the current statement keeps its keys");
	table.enter_statement(Some(second));
	assert!(table.is_empty());
	assert_eq!(table.statement(), Some(second));
	assert_eq!(table.lookup_or_insert(key(1, 0, None), values[1], |_| true), None);
	assert_eq!(table.lookup_or_insert(key(1, 0, None), values[2], |_| true), Some(values[1]));
}

/// A digest hit is a match only when the ops' attributes are equal. Under one forced key, as two
/// colliding digests give, `1`, `1f` and `1dec` are three values, and a second `1` is the first.
#[test]
fn value_table_confirms_a_digest_hit_by_attributes() {
	let mut ir = request();
	let top = top_block(&ir);
	let number = |n: Number| OpKind::Expr(ExprOp::Literal(Box::new(Value::Number(n))));
	let (stmt, values) = edit(&mut ir, |rw| {
		let (stmt, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let values: Vec<ValueId> = [
			number(Number::Int(1)),
			number(Number::Float(1.0)),
			number(Number::Decimal(rust_decimal::Decimal::ONE)),
			number(Number::Int(1)),
		]
		.into_iter()
		.map(|kind| add(rw, InsertPoint::End(top), kind, &[], 1).1[0])
		.collect();
		(stmt, values)
	});
	let collided = ValueKey {
		tag: literal(0).tag(),
		attrs: AttrDigest([1; 16]),
		operands: SmallVec::new(),
		tokens: SmallVec::new(),
		frame: FrameId::new(0),
		snapshot: SnapshotId::new(0),
		epoch: None,
		opaque: None,
	};
	let kind_of = |ir: &Ir, value: ValueId| match ir.value(value).map(|data| &data.def) {
		Some(ValueDef::OpResult {
			op,
			..
		}) => ir.op(*op).map(|data| data.kind.clone()),
		_ => None,
	};
	let mut table = ValueTable::new();
	table.enter_statement(Some(stmt));
	for (at, value) in values.iter().enumerate() {
		let kind = kind_of(&ir, *value).expect("a literal");
		let held = table.lookup_or_insert(collided.clone(), *value, |earlier| {
			kind_of(&ir, earlier).is_some_and(|earlier| earlier.attrs_eq(&kind))
		});
		let expected = (at == 3).then_some(values[0]);
		assert_eq!(held, expected, "value {at}");
	}
	assert_eq!(table.len(), 3);
}

#[test]
fn value_table_forgets_the_keys_of_a_region_it_leaves() {
	let mut regions: slotmap::SlotMap<RegionId, ()> = slotmap::SlotMap::with_key();
	let (outer, left, right) = (regions.insert(()), regions.insert(()), regions.insert(()));
	let mut values: slotmap::SlotMap<ValueId, ()> = slotmap::SlotMap::with_key();
	let held: Vec<ValueId> = (0..4).map(|_| values.insert(())).collect();
	let key = |digest: u8| ValueKey {
		tag: literal(0).tag(),
		attrs: AttrDigest([digest; 16]),
		operands: SmallVec::new(),
		tokens: SmallVec::new(),
		frame: FrameId::new(0),
		snapshot: SnapshotId::new(0),
		epoch: None,
		opaque: None,
	};
	let mut table = ValueTable::new();
	table.enter_statement(Some(OpId::default()));
	table.enter_regions(&[outer]);
	assert_eq!(table.lookup_or_insert(key(1), held[0], |_| true), None);
	table.enter_regions(&[outer, left]);
	assert_eq!(
		table.lookup_or_insert(key(1), held[1], |_| true),
		Some(held[0]),
		"an outer key is seen"
	);
	assert_eq!(table.lookup_or_insert(key(2), held[1], |_| true), None);
	table.enter_regions(&[outer, right]);
	assert_eq!(
		table.lookup_or_insert(key(2), held[2], |_| true),
		None,
		"a sibling's key is forgotten"
	);
	table.enter_regions(&[outer]);
	assert_eq!(
		table.lookup_or_insert(key(2), held[3], |_| true),
		None,
		"a left region's key is forgotten"
	);
	assert_eq!(table.lookup_or_insert(key(1), held[3], |_| true), Some(held[0]));
}

// ---------------------------------------------------------------------------------------------
// The verifier's structural rules
// ---------------------------------------------------------------------------------------------
//
// Each rule's case starts from a container that verifies, so a negative case names exactly the
// edit that breaks it. Operand counts, region kinds and terminators are the ones the dialects
// declare, so each case depends only on those declarations.

struct NoCatalog;

impl SemanticCatalog for NoCatalog {}

fn check_against(ir: &Ir, target: &LegalityTarget) -> Result<(), VerifyError> {
	let registry = FunctionRegistry::with_builtins();
	let frames = FrameTable::default();
	let cx = EffectCx {
		registry: &registry,
		catalog: &NoCatalog,
		frames: &frames,
	};
	verify(ir, &mut FactCache::new(), VerifyAt::AfterPass("test"), target, &cx)
}

fn check(ir: &Ir) -> Result<(), VerifyError> {
	check_against(ir, &LOGICAL_PHASE)
}

#[track_caller]
fn assert_valid(ir: &Ir) {
	if let Err(error) = check(ir) {
		panic!("the container fails verification: {error}");
	}
}

#[track_caller]
fn assert_rule(ir: &Ir, rule: VerifyRule) {
	match check(ir) {
		Err(error) => assert_eq!(error.rule, rule, "{error}"),
		Ok(()) => panic!("the container verifies, expected a {rule:?} violation"),
	}
}

/// Creates `kind` at `at` with the results its arity declares, after fresh literals that pad its
/// operands to the arity's minimum. Answers the op and its results.
fn conform(rw: &mut Rewriter<'_>, at: InsertPoint, kind: OpKind) -> (OpId, SmallVec<[ValueId; 2]>) {
	use crate::exec::ir::op_kind::OperandRule;

	let arity = kind.arity();
	let count = match arity.operands {
		OperandRule::Exactly(count) | OperandRule::AtLeast(count) => count,
		OperandRule::Grouped(sizes) => sizes.first().copied().unwrap_or(0),
	};
	let operands: SmallVec<[ValueId; 4]> =
		(0..count).map(|n| add(rw, at, literal(i64::from(n)), &[], 1).1[0]).collect();
	add(rw, at, kind, &operands, usize::from(arity.results))
}

/// A `logical.stmt` at the end of `block` with its open `Statement` region; answers the op and
/// the region's block.
fn open_statement(rw: &mut Rewriter<'_>, block: BlockId) -> (OpId, BlockId) {
	let (statement, _) = conform(rw, InsertPoint::End(block), stmt_kind());
	let (_, body) = rw
		.create_region(statement, RegionKind::Statement(StatementCommit::Auto), &[timestamp_arg()])
		.expect("the region is created");
	(statement, body)
}

/// Appends a yield of `values` to `block`.
fn close_with(rw: &mut Rewriter<'_>, block: BlockId, values: &[ValueId]) -> OpId {
	add(rw, InsertPoint::End(block), yield_kind(), values, 0).0
}

/// One statement whose body reads a literal and yields it.
struct Fixture {
	ir: Ir,
	top: BlockId,
	statement: OpId,
	body: BlockId,
	value: ValueId,
	literal: OpId,
	terminator: OpId,
}

fn fixture() -> Fixture {
	let mut ir = request();
	let top = top_block(&ir);
	let (statement, body, value, literal_op, terminator) = edit(&mut ir, |rw| {
		let (statement, body) = open_statement(rw, top);
		let (literal_op, values) = add(rw, InsertPoint::End(body), literal(1), &[], 1);
		let terminator = close_with(rw, body, &[values[0]]);
		(statement, body, values[0], literal_op, terminator)
	});
	Fixture {
		ir,
		top,
		statement,
		body,
		value,
		literal: literal_op,
		terminator,
	}
}

#[test]
fn a_well_formed_container_verifies() {
	assert_valid(&fixture().ir);
	assert_valid(&request());
}

#[test]
fn request_block_may_hold_no_terminator() {
	let mut f = fixture();
	assert_valid(&f.ir);
	let top = f.top;
	edit(&mut f.ir, |rw| close_with(rw, top, &[]));
	assert_valid(&f.ir);
	edit(&mut f.ir, |rw| {
		add(rw, InsertPoint::End(top), literal(2), &[], 1);
	});
	assert_rule(&f.ir, VerifyRule::Terminator);
}

#[test]
fn use_list_rejects_a_use_the_list_does_not_hold() {
	let mut f = fixture();
	assert_valid(&f.ir);
	f.ir.forget_last_use(f.value);
	assert_rule(&f.ir, VerifyRule::UseList);
}

#[test]
fn use_list_rejects_an_entry_the_op_does_not_make() {
	let mut f = fixture();
	let (statement, value) = (f.statement, f.value);
	if let Some(held) = f.ir.values.get_mut(value) {
		held.uses.push(Use {
			user: statement,
			operand: 3,
		});
	}
	assert_rule(&f.ir, VerifyRule::UseList);
	let mut f = fixture();
	let duplicate = f.ir.uses(f.value)[0];
	if let Some(held) = f.ir.values.get_mut(f.value) {
		held.uses.push(duplicate);
	}
	assert_rule(&f.ir, VerifyRule::UseList);
}

#[test]
fn dominance_rejects_an_operand_defined_later() {
	let mut f = fixture();
	let (body, terminator) = (f.body, f.terminator);
	let later = edit(&mut f.ir, |rw| add(rw, InsertPoint::End(body), literal(9), &[], 1).1[0]);
	edit(&mut f.ir, |rw| rw.set_operand(terminator, 0, later)).expect("the operand is set");
	assert_rule(&f.ir, VerifyRule::Dominance);
}

#[test]
fn v7_a_value_of_a_nested_region_is_not_visible_after_it() {
	let mut f = fixture();
	let terminator = f.terminator;
	let inner = edit(&mut f.ir, |rw| {
		let (owner, _) =
			conform(rw, InsertPoint::Before(terminator), OpKind::Logical(LogOp::Block));
		let (_, inner) =
			rw.create_region(owner, RegionKind::Block, &[]).expect("the region is created");
		let value = add(rw, InsertPoint::End(inner), literal(5), &[], 1).1[0];
		close_with(rw, inner, &[value]);
		value
	});
	assert_valid(&f.ir);
	edit(&mut f.ir, |rw| rw.set_operand(terminator, 0, inner)).expect("the operand is set");
	assert_rule(&f.ir, VerifyRule::Dominance);
}

#[test]
fn v8_an_isolated_region_reads_only_its_own_arguments() {
	let mut f = fixture();
	let terminator = f.terminator;
	let value = f.value;
	let inner = edit(&mut f.ir, |rw| {
		let (owner, _) = conform(rw, InsertPoint::Before(terminator), closure_kind());
		let (_, inner) = rw
			.create_region(owner, RegionKind::ClosureBody, &[timestamp_arg()])
			.expect("the region is created");
		let argument = rw.ir().block(inner).expect("the block exists").args[0];
		close_with(rw, inner, &[argument]);
		inner
	});
	assert_valid(&f.ir);
	let read = last_op_of(&f.ir, inner);
	edit(&mut f.ir, |rw| rw.set_operand(read, 0, value)).expect("the operand is set");
	assert_rule(&f.ir, VerifyRule::Dominance);
}

/// The last op of `block`.
fn last_op_of(ir: &Ir, block: BlockId) -> OpId {
	*ir.block(block).expect("the block exists").ops.last().expect("the block holds an op")
}

#[test]
fn v11_a_statement_cannot_read_another_statements_value() {
	let mut f = fixture();
	let (top, value) = (f.top, f.value);
	edit(&mut f.ir, |rw| {
		let (_, body) = open_statement(rw, top);
		close_with(rw, body, &[]);
	});
	assert_valid(&f.ir);
	edit(&mut f.ir, |rw| {
		let (_, body) = open_statement(rw, top);
		close_with(rw, body, &[value])
	});
	assert_rule(&f.ir, VerifyRule::Dominance);
}

#[test]
fn terminator_rejects_a_missing_and_a_second_terminator() {
	let mut f = fixture();
	assert_valid(&f.ir);
	let body = f.body;
	edit(&mut f.ir, |rw| close_with(rw, body, &[]));
	assert_rule(&f.ir, VerifyRule::Terminator);
	let mut f = fixture();
	let terminator = f.terminator;
	edit(&mut f.ir, |rw| rw.erase(terminator, EliminationReason::DeadPure)).expect("erased");
	assert_rule(&f.ir, VerifyRule::Terminator);
}

#[test]
fn v2a_a_return_lands_in_the_statement_that_holds_it() {
	let mut f = fixture();
	let (body, terminator) = (f.body, f.terminator);
	edit(&mut f.ir, |rw| {
		rw.erase(terminator, EliminationReason::DeadPure).expect("erased");
		conform(rw, InsertPoint::End(body), OpKind::Logical(LogOp::Return));
	});
	assert_valid(&f.ir);
	let ret = last_op_of(&f.ir, body);
	assert_eq!(control_target(&f.ir, ret), ControlTarget::Region(f.ir.enclosing_region(ret)));
}

#[test]
fn v2b_a_return_may_not_reach_the_request() {
	let mut f = fixture();
	let top = f.top;
	edit(&mut f.ir, |rw| {
		conform(rw, InsertPoint::End(top), OpKind::Logical(LogOp::Return));
	});
	assert_rule(&f.ir, VerifyRule::TerminatorTarget);
}

/// A `FOR` loop before the terminator of `f`'s statement body, with the open `StatementFor`
/// region over the element and its position; answers the region's block.
fn for_loop(f: &mut Fixture) -> BlockId {
	let terminator = f.terminator;
	let looped = OpKind::Logical(LogOp::Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		order: SourceOrder::Positional,
		step: None,
	});
	edit(&mut f.ir, |rw| {
		let (owner, _) = conform(rw, InsertPoint::Before(terminator), looped);
		// The body of a `FOR` takes the element and its position.
		let args = [TypeKnowledge::Unknown, TypeKnowledge::Unknown];
		rw.create_region(owner, RegionKind::StatementFor, &args).expect("the region is created").1
	})
}

fn closure_kind() -> OpKind {
	OpKind::Expr(ExprOp::ClosureCreate(Box::new(ClosureAttrs {
		params: Box::new([]),
		returns: None,
		body_sql: "".into(),
		captures: Box::new([]),
		reads_row: false,
		reads_root: false,
		literal: Default::default(),
	})))
}

#[test]
fn v3a_a_break_needs_the_loop_it_is_written_in() {
	let mut f = fixture();
	let inner = for_loop(&mut f);
	edit(&mut f.ir, |rw| {
		conform(rw, InsertPoint::End(inner), OpKind::Logical(LogOp::Break));
	});
	assert_valid(&f.ir);

	let mut f = fixture();
	let (body, terminator) = (f.body, f.terminator);
	edit(&mut f.ir, |rw| {
		rw.erase(terminator, EliminationReason::DeadPure).expect("erased");
		conform(rw, InsertPoint::End(body), OpKind::Logical(LogOp::Break));
	});
	assert_rule(&f.ir, VerifyRule::TerminatorTarget);
}

#[test]
fn v3a_a_break_through_a_closure_meets_a_barrier() {
	let mut f = fixture();
	let inner = for_loop(&mut f);
	edit(&mut f.ir, |rw| {
		let (closure, _) = conform(rw, InsertPoint::End(inner), closure_kind());
		let (_, closure_body) = rw
			.create_region(closure, RegionKind::ClosureBody, &[timestamp_arg()])
			.expect("the region is created");
		// A break the query wrote meets the barrier; one a rewrite synthesized passes through it
		// to its loop.
		add_located(rw, closure_body, OpKind::Logical(LogOp::Break), LocOrigin::Query);
		conform(rw, InsertPoint::End(inner), OpKind::Logical(LogOp::Next));
	});
	assert_rule(&f.ir, VerifyRule::TerminatorTarget);
}

#[test]
fn v1_region_kinds_follow_the_declaration() {
	let mut f = fixture();
	let (literal_op, terminator) = (f.literal, f.terminator);
	edit(&mut f.ir, |rw| {
		let (owner, _) =
			conform(rw, InsertPoint::Before(terminator), OpKind::Logical(LogOp::Block));
		let (_, inner) =
			rw.create_region(owner, RegionKind::Block, &[]).expect("the region is created");
		close_with(rw, inner, &[]);
	});
	assert_valid(&f.ir);
	edit(&mut f.ir, |rw| {
		let (_, inner) =
			rw.create_region(literal_op, RegionKind::Block, &[]).expect("the region is created");
		close_with(rw, inner, &[]);
	});
	assert_rule(&f.ir, VerifyRule::RegionKinds);
}

/// A physical `If` states with `value` which twin its arms are, so an arm of the other twin is a
/// region-kind violation, though core admits either twin for an op that does not state it.
#[test]
fn a_physical_if_holds_the_arm_kind_its_value_states() {
	for (value, arm, admitted) in [
		(true, RegionKind::ValueIf, true),
		(true, RegionKind::StatementIf, false),
		(false, RegionKind::StatementIf, true),
		(false, RegionKind::ValueIf, false),
	] {
		let mut f = fixture();
		let terminator = f.terminator;
		edit(&mut f.ir, |rw| {
			let (owner, _) = conform(
				rw,
				InsertPoint::Before(terminator),
				OpKind::Phys(PhysOp::If {
					value,
				}),
			);
			for _ in 0..2 {
				let (_, inner) = rw.create_region(owner, arm, &[]).expect("the region is created");
				close_with(rw, inner, &[]);
			}
		});
		assert_eq!(
			logical_violation(&f.ir, VerifyRule::RegionKinds).is_none(),
			admitted,
			"value: {value}, arms: {arm:?}"
		);
	}
}

#[test]
fn a_region_listed_twice_is_reported_and_not_walked_forever() {
	let mut f = fixture();
	let statement = f.statement;
	let region = f.ir.op(statement).expect("the op exists").regions[0];
	if let Some(held) = f.ir.ops.get_mut(statement) {
		held.regions.push(region);
	}
	assert_rule(&f.ir, VerifyRule::RegionKinds);
}

#[test]
fn a_live_op_no_block_lists_is_reported() {
	let mut f = fixture();
	let (terminator, body) = (f.terminator, f.body);
	let extra =
		edit(&mut f.ir, |rw| add(rw, InsertPoint::Before(terminator), literal(2), &[], 1).0);
	assert!(logical_violation(&f.ir, VerifyRule::RegionKinds).is_none());
	if let Some(held) = f.ir.blocks.get_mut(body) {
		held.ops.retain(|op| *op != extra);
	}
	let failure = logical_violation(&f.ir, VerifyRule::RegionKinds).expect("the op is an orphan");
	assert_eq!(failure.op, Some(extra));
}

#[test]
fn an_op_listed_twice_does_not_hide_an_orphan() {
	let mut f = fixture();
	let (terminator, body, listed) = (f.terminator, f.body, f.literal);
	let extra =
		edit(&mut f.ir, |rw| add(rw, InsertPoint::Before(terminator), literal(2), &[], 1).0);
	if let Some(held) = f.ir.blocks.get_mut(body) {
		held.ops.retain(|op| *op != extra);
		held.ops.insert(0, listed);
	}
	assert!(logical_violation(&f.ir, VerifyRule::RegionKinds).is_some());
}

#[test]
fn statement_structure_places_transactions_and_statements() {
	// A session transaction is the only top-level region of its fragment.
	let mut ir = request();
	let top = top_block(&ir);
	let transaction = OpKind::Logical(LogOp::Txn {
		mode: TxnRegionMode::Session {
			id: 0,
		},
	});
	let inner = edit(&mut ir, |rw| {
		let (owner, _) = conform(rw, InsertPoint::End(top), transaction.clone());
		let (_, inner) = rw
			.create_region(owner, RegionKind::Transaction(TxnKind::Session), &[])
			.expect("the region is created");
		let (_, body) = open_statement(rw, inner);
		close_with(rw, body, &[]);
		inner
	});
	assert_valid(&ir);
	edit(&mut ir, |rw| {
		let (owner, _) = conform(rw, InsertPoint::End(inner), transaction);
		rw.create_region(owner, RegionKind::Transaction(TxnKind::Session), &[])
			.expect("the region is created");
	});
	assert_rule(&ir, VerifyRule::StatementStructure);
}

#[test]
fn statement_structure_rejects_a_statement_under_a_block() {
	let mut f = fixture();
	let terminator = f.terminator;
	edit(&mut f.ir, |rw| {
		let (owner, _) =
			conform(rw, InsertPoint::Before(terminator), OpKind::Logical(LogOp::Block));
		let (_, inner) =
			rw.create_region(owner, RegionKind::Block, &[]).expect("the region is created");
		let (_, body) = open_statement(rw, inner);
		close_with(rw, body, &[]);
		close_with(rw, inner, &[]);
	});
	assert_rule(&f.ir, VerifyRule::StatementStructure);
}

#[test]
fn arity_holds_the_declared_counts() {
	let kind = literal(3);
	let arity = kind.arity();
	let mut f = fixture();
	let terminator = f.terminator;
	edit(&mut f.ir, |rw| {
		add(rw, InsertPoint::Before(terminator), kind.clone(), &[], usize::from(arity.results));
	});
	assert_valid(&f.ir);
	edit(&mut f.ir, |rw| {
		add(rw, InsertPoint::Before(terminator), kind, &[], usize::from(arity.results) + 1);
	});
	assert_rule(&f.ir, VerifyRule::Arity);
}

#[test]
fn frame_snapshot_ids_reject_an_id_outside_the_tables() {
	for (frame, snapshot) in [(9, 0), (0, 9)] {
		let mut f = fixture();
		let terminator = f.terminator;
		edit(&mut f.ir, |rw| {
			let mut stray = spec(literal(4), &[], 1);
			stray.frame = FrameId::new(frame);
			stray.snapshot = SnapshotId::new(snapshot);
			rw.create(InsertPoint::Before(terminator), stray).expect("the op is created");
		});
		assert_rule(&f.ir, VerifyRule::FrameSnapshotIds);
	}
}

#[test]
fn frame_snapshot_ids_reject_a_version_naming_a_dead_value() {
	let mut f = fixture();
	let terminator = f.terminator;
	let dead = edit(&mut f.ir, |rw| {
		let (gone, values) = add(rw, InsertPoint::Before(terminator), literal(3), &[], 1);
		rw.erase(gone, EliminationReason::DeadPure).expect("the op is erased");
		values[0]
	});
	let snapshot = edit(&mut f.ir, |rw| rw.intern_snapshot(Snapshot::Version(dead)))
		.expect("the snapshot is interned");
	assert_valid(&f.ir);
	edit(&mut f.ir, |rw| {
		let mut read = spec(literal(4), &[], 1);
		read.snapshot = snapshot;
		rw.create(InsertPoint::Before(terminator), read).expect("the op is created");
	});
	assert_rule(&f.ir, VerifyRule::FrameSnapshotIds);
}

#[test]
fn v9_an_inheriting_region_carries_its_owners_frame() {
	let other = AuthorityFrame {
		eval: EvalFrame::permission(None),
		unit: UnitFrame::unprincipled(),
	};
	let mut f = fixture();
	let terminator = f.terminator;
	let frame = edit(&mut f.ir, |rw| rw.intern_frame(other)).expect("the frame is interned");
	let stray = |frame, origin| {
		let mut described = spec(literal(4), &[], 1);
		described.frame = frame;
		described.loc.origin = origin;
		described
	};
	edit(&mut f.ir, |rw| {
		rw.create(
			InsertPoint::Before(terminator),
			stray(
				frame,
				LocOrigin::Catalog(Box::new((
					CatalogObjectKey {
						kind: CatalogKind::Table,
						ns: None,
						db: None,
						table: None,
						name: None,
					},
					ObjectVersion::Inline,
				))),
			),
		)
		.expect("the op is created");
	});
	assert_valid(&f.ir);
	edit(&mut f.ir, |rw| {
		rw.create(InsertPoint::Before(terminator), stray(frame, LocOrigin::Query))
			.expect("the op is created");
	});
	assert_rule(&f.ir, VerifyRule::FrameSnapshotIds);
}

#[test]
fn v9_a_permission_filter_opens_a_frame_of_the_same_principal() {
	let mut f = fixture();
	let terminator = f.terminator;
	let unit = f.ir.frame(FrameId::new(0)).expect("the request's frame").unit.clone();
	let other = AuthorityFrame {
		eval: EvalFrame::permission(None),
		unit,
	};
	let frame = edit(&mut f.ir, |rw| rw.intern_frame(other)).expect("the frame is interned");
	assert_ne!(frame, FrameId::new(0));
	let value = f.value;
	let filter = edit(&mut f.ir, |rw| {
		let mut described = spec(
			OpKind::Logical(LogOp::PermissionFilter {
				guard: crate::expr::logical::GuardKind::Read,
				computed: None,
			}),
			&[value],
			1,
		);
		described.frame = frame;
		described.loc.origin = LocOrigin::Query;
		rw.create(InsertPoint::Before(terminator), described).expect("the op is created")
	});
	assert!(logical_violation(&f.ir, VerifyRule::FrameSnapshotIds).is_none());
	edit(&mut f.ir, |rw| {
		let mut described = spec(literal(4), &[], 1);
		described.frame = frame;
		described.loc.origin = LocOrigin::Query;
		rw.create(InsertPoint::Before(terminator), described).expect("the op is created")
	});
	let failure =
		logical_violation(&f.ir, VerifyRule::FrameSnapshotIds).expect("a literal opens no frame");
	assert_ne!(failure.op, Some(filter));
}

/// Whether the verifier admits a permission filter of `guard` evaluating as `opened`, in a
/// statement whose request evaluates as `inherited`, both for the request's principal.
fn admits_filter_frame(inherited: EvalFrame, opened: EvalFrame, guard: GuardKind) -> bool {
	let mut f = fixture();
	f.ir.frames.frames[0].eval = inherited;
	let terminator = f.terminator;
	let unit = f.ir.frame(FrameId::new(0)).expect("the request's frame").unit.clone();
	let frame = edit(&mut f.ir, |rw| {
		rw.intern_frame(AuthorityFrame {
			eval: opened,
			unit,
		})
	})
	.expect("the frame is interned");
	let value = f.value;
	edit(&mut f.ir, |rw| {
		let mut described = spec(
			OpKind::Logical(LogOp::PermissionFilter {
				guard,
				computed: None,
			}),
			&[value],
			1,
		);
		described.frame = frame;
		described.loc.origin = LocOrigin::Query;
		rw.create(InsertPoint::Before(terminator), described).expect("the op is created")
	});
	logical_violation(&f.ir, VerifyRule::FrameSnapshotIds).is_none()
}

#[test]
fn v9_an_opened_frame_keeps_or_narrows_the_inherited_auth_limit() {
	use crate::iam::{AuthLimit, Level, Role};

	let db = |role| Some(AuthLimit::new(Level::Database("n".into(), "d".into()), Some(role)));
	let caller = |auth| EvalFrame {
		auth,
		..EvalFrame::caller()
	};
	let filter = |inherited, opened| {
		admits_filter_frame(caller(inherited), EvalFrame::permission(opened), GuardKind::Read)
	};
	// The inherited limit kept, or narrowed by role or by level.
	assert!(filter(db(Role::Editor), db(Role::Editor)));
	assert!(filter(db(Role::Editor), db(Role::Viewer)));
	let record =
		AuthLimit::new(Level::Record("n".into(), "d".into(), "r".into()), Some(Role::Viewer));
	assert!(filter(db(Role::Editor), Some(record)));
	// No inherited limit: any limit narrows.
	assert!(filter(None, db(Role::Owner)));
	// The inherited limit dropped, or widened by role or by level.
	assert!(!filter(db(Role::Editor), None));
	assert!(!filter(db(Role::Editor), db(Role::Owner)));
	let ns = AuthLimit::new(Level::Namespace("n".into()), Some(Role::Editor));
	assert!(!filter(db(Role::Editor), Some(ns)));
}

#[test]
fn v9_an_opened_frame_keeps_the_inherited_permission_state() {
	let (read, write) = (GuardKind::Read, GuardKind::Write);
	// A filter evaluates in the frame of its guard.
	assert!(admits_filter_frame(EvalFrame::caller(), EvalFrame::mutable_permission(None), write));
	assert!(!admits_filter_frame(EvalFrame::caller(), EvalFrame::mutable_permission(None), read));
	assert!(!admits_filter_frame(EvalFrame::caller(), EvalFrame::permission(None), write));
	// A frame that refuses writes opens none that performs them.
	assert!(admits_filter_frame(EvalFrame::computed(None), EvalFrame::permission(None), read));
	assert!(!admits_filter_frame(
		EvalFrame::computed(None),
		EvalFrame::mutable_permission(None),
		write
	));
	// A filter opens no record-id column frame.
	let column = EvalFrame {
		key_part: true,
		..EvalFrame::permission(None)
	};
	assert!(!admits_filter_frame(EvalFrame::caller(), column, read));
}

#[test]
fn v9_a_region_that_sets_its_own_frame_is_not_compared() {
	let other = AuthorityFrame {
		eval: EvalFrame::permission(None),
		unit: UnitFrame::unprincipled(),
	};
	let mut f = fixture();
	let terminator = f.terminator;
	let frame = edit(&mut f.ir, |rw| rw.intern_frame(other)).expect("the frame is interned");
	edit(&mut f.ir, |rw| {
		let (owner, _) =
			conform(rw, InsertPoint::Before(terminator), OpKind::Logical(LogOp::Block));
		let (_, inner) = rw
			.create_region(owner, RegionKind::StoredClause(StoredRole::Value), &[])
			.expect("the region is created");
		let mut described = spec(yield_kind(), &[], 0);
		described.frame = frame;
		rw.create(InsertPoint::End(inner), described).expect("the op is created");
	});
	if let Err(error) = check(&f.ir) {
		assert_ne!(error.rule, VerifyRule::FrameSnapshotIds, "{error}");
	}
}

#[test]
fn legality_follows_the_target_of_the_phase() {
	let f = fixture();
	assert_valid(&f.ir);
	let failure = check_against(&f.ir, &PHYSICAL_PHASE).expect_err("a logical op is illegal");
	assert_eq!(failure.rule, VerifyRule::Legality);
	let phys = {
		let mut ir = request();
		let top = top_block(&ir);
		edit(&mut ir, |rw| {
			let (_, body) = {
				let (owner, _) = conform(
					rw,
					InsertPoint::End(top),
					OpKind::Phys(PhysOp::Stmt {
						commit: StatementCommit::Auto,
						control: None,
					}),
				);
				rw.create_region(
					owner,
					RegionKind::Statement(StatementCommit::Auto),
					&[timestamp_arg()],
				)
				.expect("the region is created")
			};
			close_with(rw, body, &[]);
		});
		ir
	};
	assert_eq!(
		check_against(&phys, &LOGICAL_PHASE).expect_err("a physical op is illegal").rule,
		VerifyRule::Legality
	);
	assert!(check_against(&phys, &PHYSICAL_PHASE).is_ok());
}

#[test]
fn the_lowest_rule_is_reported_when_several_fail() {
	let mut f = fixture();
	let terminator = f.terminator;
	let (surplus, stray) = edit(&mut f.ir, |rw| {
		let surplus = rw
			.create(
				InsertPoint::Before(terminator),
				spec(literal(6), &[], usize::from(literal(6).arity().results) + 1),
			)
			.expect("the op is created");
		let mut described = spec(literal(7), &[], usize::from(literal(7).arity().results));
		described.frame = FrameId::new(9);
		let stray = rw.create(InsertPoint::Before(terminator), described).expect("created");
		(surplus, stray)
	});
	let target = PHYSICAL_PHASE;
	let rule = |ir: &Ir| check_against(ir, &target).expect_err("the container fails").rule;
	assert_eq!(rule(&f.ir), VerifyRule::Arity);
	edit(&mut f.ir, |rw| rw.erase(surplus, EliminationReason::DeadPure)).expect("erased");
	assert_eq!(rule(&f.ir), VerifyRule::FrameSnapshotIds);
	edit(&mut f.ir, |rw| rw.erase(stray, EliminationReason::DeadPure)).expect("erased");
	assert_eq!(rule(&f.ir), VerifyRule::Legality);
}

// ---------------------------------------------------------------------------------------------
// The verifier's semantic and phase-boundary rules
// ---------------------------------------------------------------------------------------------
//
// These cases read the violation a rule records, whatever rules rank below it, so each names the
// one edit that breaks its rule and does not depend on what the dialects declare.

/// The violation of `rule` after a pass, in the logical phase.
fn logical_violation(ir: &Ir, rule: VerifyRule) -> Option<VerifyError> {
	violation_of(ir, &mut FactCache::new(), VerifyAt::AfterPass("test"), &LOGICAL_PHASE, rule)
}

/// The violation of `rule` at the boundary that ends the physical phase.
fn physical_boundary_violation(ir: &Ir, rule: VerifyRule) -> Option<VerifyError> {
	violation_of(
		ir,
		&mut FactCache::new(),
		VerifyAt::PhaseBoundary(Phase::Physical),
		&PHYSICAL_PHASE,
		rule,
	)
}

#[test]
fn fallibility_consistency_admits_the_top_answers() {
	let f = fixture();
	assert!(logical_violation(&f.ir, VerifyRule::FallibilityConsistency).is_none());
}

/// A statement body holding `owner` and, under `owner`, one `ErrorScope` region that holds a
/// literal; answers the container and the ids the cases below move around.
struct Scoped {
	ir: Ir,
	body: BlockId,
	owner: OpId,
	region: RegionId,
	scope_block: BlockId,
	inside: OpId,
}

fn scoped() -> Scoped {
	let mut ir = request();
	let top = top_block(&ir);
	let (body, owner, region, scope_block, inside) = edit(&mut ir, |rw| {
		let (_, body) = open_statement(rw, top);
		let (owner, _) = add(rw, InsertPoint::End(body), stmt_kind(), &[], 0);
		let (region, scope_block) = rw
			.create_region(owner, RegionKind::ErrorScope(ErrorScope::ShapeToNone), &[])
			.expect("the region is created");
		let (inside, _) = add(rw, InsertPoint::End(scope_block), literal(1), &[], 1);
		(body, owner, region, scope_block, inside)
	});
	Scoped {
		ir,
		body,
		owner,
		region,
		scope_block,
		inside,
	}
}

#[test]
fn error_scope_admits_an_op_that_stays_in_its_scope() {
	let s = scoped();
	assert!(logical_violation(&s.ir, VerifyRule::ErrorScope).is_none());
}

#[test]
fn error_scope_rejects_a_scope_whose_owner_is_another_op() {
	let mut s = scoped();
	let (body, region, inside) = (s.body, s.region, s.inside);
	let other = edit(&mut s.ir, |rw| {
		let (other, _) = add(rw, InsertPoint::End(body), stmt_kind(), &[], 0);
		rw.move_region(region, other).expect("the region moves");
		other
	});
	assert_ne!(other, s.owner);
	let failure = logical_violation(&s.ir, VerifyRule::ErrorScope).expect("the scope changed");
	assert_eq!(failure.op, Some(inside));
}

#[test]
fn error_scope_admits_an_owner_that_stands_for_the_recorded_one() {
	let mut s = scoped();
	let (body, region, owner) = (s.body, s.region, s.owner);
	edit(&mut s.ir, |rw| {
		let other =
			rw.derived(&[owner], |rw| add(rw, InsertPoint::End(body), stmt_kind(), &[], 0).0);
		rw.move_region(region, other).expect("the region moves");
	});
	assert!(logical_violation(&s.ir, VerifyRule::ErrorScope).is_none());
}

#[test]
fn error_scope_rejects_an_op_moved_into_a_scope() {
	let mut s = scoped();
	let (body, scope_block) = (s.body, s.scope_block);
	let moved = edit(&mut s.ir, |rw| {
		let (carrier, _) = add(rw, InsertPoint::End(body), stmt_kind(), &[], 0);
		let (carried, carried_block) =
			rw.create_region(carrier, RegionKind::Block, &[]).expect("the region is created");
		let moved = add(rw, InsertPoint::End(carried_block), literal(2), &[], 1).0;
		let (host, _) = add(rw, InsertPoint::End(scope_block), stmt_kind(), &[], 0);
		rw.move_region(carried, host).expect("the region moves");
		moved
	});
	let failure = logical_violation(&s.ir, VerifyRule::ErrorScope).expect("the op left its scope");
	assert_eq!(failure.op, Some(moved));
}

#[test]
fn error_scope_checks_every_id_a_derived_op_stands_for() {
	let mut s = scoped();
	let (body, inside) = (s.body, s.inside);
	edit(&mut s.ir, |rw| {
		rw.derived(&[inside], |rw| add(rw, InsertPoint::End(body), literal(3), &[], 1).0)
	});
	let failure = logical_violation(&s.ir, VerifyRule::ErrorScope)
		.expect("an op derived from a scoped op stands outside the scope");
	assert_ne!(failure.op, Some(inside));
}

fn arm(guard: Guard, body: ArmBody) -> ArmSpec {
	ArmSpec {
		guard,
		body,
		label: ArmLabel("arm"),
	}
}

fn guarded() -> ArmSpec {
	arm(Guard::ValueKindIs(ValueKindClass::Array), ArmBody::PreLowered)
}

fn otherwise() -> ArmSpec {
	arm(Guard::Otherwise, ArmBody::PreLowered)
}

fn choose_spec(arms: Vec<ArmSpec>, generic: GenericArm, origin: ChooseOrigin) -> ChooseSpec {
	ChooseSpec {
		granularity: ChooseGranularity::Statement,
		arms: arms.into_iter().collect(),
		generic,
		equivalence: ArmEquivalence::SameRowsSameOrder,
		origin,
	}
}

/// A statement holding a `Choose` in the dialect of `physical` that owns one region of each of
/// `kinds`, each closed.
fn choose_container(spec: ChooseSpec, physical: bool, kinds: &[RegionKind]) -> Ir {
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		let (_, body) = open_statement(rw, top);
		let spec = Box::new(spec);
		let kind = if physical {
			OpKind::Phys(PhysOp::Choose(spec))
		} else {
			OpKind::Logical(LogOp::Choose(spec))
		};
		let (choice, _) = conform(rw, InsertPoint::End(body), kind);
		for region in kinds {
			let (_, block) = rw.create_region(choice, *region, &[]).expect("the region is created");
			close_with(rw, block, &[]);
		}
		close_with(rw, body, &[]);
	});
	ir
}

fn choose_violation(spec: ChooseSpec, kinds: &[RegionKind]) -> Option<VerifyError> {
	logical_violation(&choose_container(spec, false, kinds), VerifyRule::Choose)
}

const ARMS: [RegionKind; 2] = [RegionKind::ChooseArm, RegionKind::ChooseArm];

#[test]
fn choose_admits_a_well_formed_choice_in_either_dialect() {
	let spec = || {
		choose_spec(
			vec![guarded(), otherwise()],
			GenericArm::Present,
			ChooseOrigin::PhysicalGuarded,
		)
	};
	assert!(choose_violation(spec(), &ARMS).is_none());
	let physical = choose_container(spec(), true, &ARMS);
	assert!(
		violation_of(
			&physical,
			&mut FactCache::new(),
			VerifyAt::AfterPass("test"),
			&PHYSICAL_PHASE,
			VerifyRule::Choose
		)
		.is_none()
	);
}

#[test]
fn choose_holds_between_one_and_the_most_arms() {
	let none = choose_spec(Vec::new(), GenericArm::Present, ChooseOrigin::PhysicalGuarded);
	assert!(choose_violation(none, &[]).is_some());
	let one = choose_spec(vec![otherwise()], GenericArm::Present, ChooseOrigin::PhysicalGuarded);
	assert!(choose_violation(one, &[RegionKind::ChooseArm]).is_none());
	let mut arms: Vec<ArmSpec> = (1..MAX_CHOOSE_ARMS).map(|_| guarded()).collect();
	arms.push(otherwise());
	let most = choose_spec(arms.clone(), GenericArm::Present, ChooseOrigin::PhysicalGuarded);
	assert!(choose_violation(most, &[RegionKind::ChooseArm; MAX_CHOOSE_ARMS]).is_none());
	arms.insert(0, guarded());
	let over = choose_spec(arms, GenericArm::Present, ChooseOrigin::PhysicalGuarded);
	assert!(choose_violation(over, &[RegionKind::ChooseArm; MAX_CHOOSE_ARMS + 1]).is_some());
}

#[test]
fn choose_holds_one_arm_region_per_arm() {
	let spec = || {
		choose_spec(
			vec![guarded(), otherwise()],
			GenericArm::Present,
			ChooseOrigin::PhysicalGuarded,
		)
	};
	assert!(choose_violation(spec(), &[RegionKind::ChooseArm]).is_some());
	assert!(choose_violation(spec(), &[RegionKind::ChooseArm, RegionKind::Block]).is_some());
}

#[test]
fn choose_ends_in_the_generic_guard_and_has_it_nowhere_else() {
	let missing =
		choose_spec(vec![guarded(), guarded()], GenericArm::Present, ChooseOrigin::PhysicalGuarded);
	assert!(choose_violation(missing, &ARMS).is_some());
	let early = choose_spec(
		vec![otherwise(), guarded()],
		GenericArm::ExhaustiveBy(ExhaustiveFact::TypeFact(Kind::Any)),
		ChooseOrigin::PhysicalGuarded,
	);
	assert!(choose_violation(early, &ARMS).is_some());
	let exhaustive = choose_spec(
		vec![guarded(), guarded()],
		GenericArm::ExhaustiveBy(ExhaustiveFact::TypeFact(Kind::Any)),
		ChooseOrigin::PhysicalGuarded,
	);
	assert!(choose_violation(exhaustive, &ARMS).is_none());
}

#[test]
fn choose_confines_unit_loads_to_dynamic_targets() {
	let loaded = || {
		vec![
			arm(
				Guard::ValueKindIs(ValueKindClass::Table),
				ArmBody::UnitLoad(UnitKind::DynamicTargetArm),
			),
			otherwise(),
		]
	};
	let dispatch = choose_spec(loaded(), GenericArm::Present, ChooseOrigin::LogicalKindDispatch);
	assert!(choose_violation(dispatch, &ARMS).is_some());
	let guarded_physical =
		choose_spec(loaded(), GenericArm::Present, ChooseOrigin::PhysicalGuarded);
	assert!(choose_violation(guarded_physical, &ARMS).is_some());
	let dynamic = choose_spec(loaded(), GenericArm::Present, ChooseOrigin::LogicalDynamicTarget);
	assert!(choose_violation(dynamic, &ARMS).is_none());
}

/// A physical container holding one op of `kind` and nothing else.
fn phys_op_container(kind: OpKind) -> Ir {
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		add(rw, InsertPoint::End(top), kind, &[], 0);
	});
	ir
}

fn gated_kinds() -> [OpKind; 2] {
	[
		OpKind::Phys(PhysOp::Prefetch(Box::new(PrefetchKeys {
			points: SmallVec::new(),
			ranges: SmallVec::new(),
		}))),
		OpKind::Phys(PhysOp::Materialize {
			image: MaterializeImage::KeyAndValue,
		}),
	]
}

#[test]
fn phase_gated_rejects_each_gated_op_and_only_those() {
	for kind in gated_kinds() {
		let ir = phys_op_container(kind);
		assert!(logical_violation(&ir, VerifyRule::PhaseGated).is_some());
		assert!(physical_boundary_violation(&ir, VerifyRule::PhaseGated).is_some());
	}
	let ir = phys_op_container(OpKind::Phys(PhysOp::Yield));
	assert!(physical_boundary_violation(&ir, VerifyRule::PhaseGated).is_none());
	assert!(logical_violation(&request(), VerifyRule::PhaseGated).is_none());
}

#[test]
fn provenance_holds_a_logical_id_held_by_a_live_op() {
	let mut ir = request();
	let top = top_block(&ir);
	let logical = edit(&mut ir, |rw| {
		let (logical, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		rw.derived(&[logical], |rw| {
			add(rw, InsertPoint::End(top), literal(1), &[], 1);
		});
		rw.erase(logical, EliminationReason::DeadPure).expect("the op is erased");
		logical
	});
	assert!(ir.eliminated().iter().all(|(id, _)| *id != logical), "a live op holds the id");
	assert!(physical_boundary_violation(&ir, VerifyRule::Provenance).is_none());
}

#[test]
fn provenance_accepts_an_eliminated_logical_id_and_rejects_an_unaccounted_one() {
	let mut ir = request();
	let top = top_block(&ir);
	let logical = edit(&mut ir, |rw| {
		let (logical, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		rw.erase(logical, EliminationReason::DeadPure).expect("the op is erased");
		logical
	});
	assert!(physical_boundary_violation(&ir, VerifyRule::Provenance).is_none());
	ir.eliminated.clear();
	let failure = physical_boundary_violation(&ir, VerifyRule::Provenance)
		.expect("the id is neither held nor eliminated");
	assert_eq!(failure.op, Some(logical));
	assert!(logical_violation(&ir, VerifyRule::Provenance).is_none(), "only the boundary checks");
}

fn pipeline_kind() -> OpKind {
	OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
		id: PipelineId(0),
		edges: SmallVec::new(),
		logical: None,
	})))
}

#[test]
fn provenance_requires_a_decision_for_every_pipeline() {
	let ir = phys_op_container(pipeline_kind());
	let failure = physical_boundary_violation(&ir, VerifyRule::Provenance)
		.expect("a pipeline stands with no decision");
	assert!(failure.op.is_some());
	assert!(logical_violation(&ir, VerifyRule::Provenance).is_none());
	assert!(physical_boundary_violation(&request(), VerifyRule::Provenance).is_none());
}

fn loop_facts() -> LoopFacts {
	LoopFacts {
		mode: LoopMode::Independent,
		apply: ApplyCut::None,
		exit: Exit::None,
		sink: LoopSink::Output,
		delivery: SourceDelivery::Stream,
		multiplicity: Multiplicity::Distinct,
		key_path: ElemPath::Id,
		compute_reads_data: false,
		cut_at_marks: true,
	}
}

#[test]
fn legality_recompute_admits_an_absent_and_an_empty_snapshot() {
	let ir = request();
	assert!(physical_boundary_violation(&ir, VerifyRule::LegalityRecompute).is_none());
	let mut facts = FactCache::new();
	facts.set_legality_snapshot(BTreeMap::new());
	let at = VerifyAt::PhaseBoundary(Phase::Physical);
	assert!(
		violation_of(&ir, &mut facts, at, &PHYSICAL_PHASE, VerifyRule::LegalityRecompute).is_none()
	);
}

#[test]
fn legality_recompute_requires_every_snapshot_loop_to_be_covered() {
	let mut ir = request();
	let top = top_block(&ir);
	let loop_op = edit(&mut ir, |rw| {
		let (loop_op, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		rw.erase(loop_op, EliminationReason::DeadPure).expect("the op is erased");
		loop_op
	});
	let mut facts = FactCache::new();
	facts.set_legality_snapshot(BTreeMap::from([(loop_op, loop_facts())]));
	let at = VerifyAt::PhaseBoundary(Phase::Physical);
	let rule = VerifyRule::LegalityRecompute;
	assert!(violation_of(&ir, &mut facts, at, &PHYSICAL_PHASE, rule).is_none());
	ir.eliminated.clear();
	let failure = violation_of(&ir, &mut facts, at, &PHYSICAL_PHASE, rule)
		.expect("the snapshot names a loop nothing holds");
	assert_eq!(failure.op, Some(loop_op));
	let after_pass = VerifyAt::AfterPass("test");
	assert!(violation_of(&ir, &mut facts, after_pass, &PHYSICAL_PHASE, rule).is_none());
}

/// The physical facts of a read-only scan that splits.
fn scan_facts() -> PhysicalFacts {
	PhysicalFacts {
		source: SourceKind::Scan,
		enforce: false,
		writes: false,
		limits: 0,
		output: true,
		sink: SinkKind::Output,
		binds_row: false,
		fetches_row: false,
		distincts: 0,
		nested: NestedUse::None,
		host_blocks: false,
		paging_replayable: false,
		estimate: None,
	}
}

/// A container holding one pipeline that stands for the eliminated logical loop `loop_op`, placed
/// as `decision` over a splittable scan; answers the container, the pipeline and the loop.
fn placed_pipeline(decision: Decision) -> (Ir, OpId, OpId) {
	placed_pipeline_over(decision, scan_facts())
}

/// [`placed_pipeline`] over the physical facts `physical`.
fn placed_pipeline_over(decision: Decision, physical: PhysicalFacts) -> (Ir, OpId, OpId) {
	let mut ir = request();
	let top = top_block(&ir);
	let (loop_op, pipeline) = edit(&mut ir, |rw| {
		let (loop_op, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		rw.erase(loop_op, EliminationReason::DeadPure).expect("the op is erased");
		let kind = OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
			id: PipelineId(0),
			edges: SmallVec::new(),
			logical: Some(loop_op),
		})));
		let (pipeline, _) = add(rw, InsertPoint::End(top), kind, &[], 0);
		(loop_op, pipeline)
	});
	let legality = Legality {
		source: SourceSplit::Range,
		chain: ChainLegality::RowLocal,
		sink: SinkCombine::Concat,
		paging: Paging::None,
		arrival: Arrival::Serial,
		ordered_fold: false,
		error_order: ErrorOrder::FirstByInputPosition,
		may_raise: false,
	};
	place_for_test(&mut ir, pipeline, legality, decision, physical);
	(ir, pipeline, loop_op)
}

fn concat_morsels() -> Decision {
	Decision::Morsels {
		input: MorselInput::Range,
		combine: Combine::Concat,
		paged: false,
		prunes: false,
	}
}

#[test]
fn legality_recompute_rejects_a_fan_out_its_loop_does_not_admit() {
	let (ir, pipeline, loop_op) = placed_pipeline(concat_morsels());
	let at = VerifyAt::PhaseBoundary(Phase::Physical);
	let rule = VerifyRule::LegalityRecompute;
	let judge = |facts: LoopFacts| {
		let mut cache = FactCache::new();
		cache.set_legality_snapshot(BTreeMap::from([(loop_op, facts)]));
		violation_of(&ir, &mut cache, at, &PHYSICAL_PHASE, rule)
	};
	assert!(judge(loop_facts()).is_none(), "an independent loop with no exit admits the fan-out");
	let ordered = LoopFacts {
		exit: Exit::Ordered(ExitCause::Break),
		..loop_facts()
	};
	let failure = judge(ordered).expect("an ordered-exit loop placed as morsels is refused");
	assert_eq!(failure.rule, rule);
	assert_eq!(failure.op, Some(pipeline));
	let carried = LoopFacts {
		mode: LoopMode::Serial(LoopRefusal::Carried),
		..loop_facts()
	};
	assert!(judge(carried).is_some(), "a serial loop is not placed as morsels");
	let mut absent = FactCache::new();
	assert!(violation_of(&ir, &mut absent, at, &PHYSICAL_PHASE, rule).is_none());
}

/// A pipeline that stands for no loop has no loop verdict and is judged on its own source and sink.
#[test]
fn admission_judges_a_pipeline_that_stands_for_no_loop_on_its_own_facts() {
	use crate::exec::ir::phys::placement::admission_gap;

	let writing = PhysicalFacts {
		writes: true,
		..scan_facts()
	};
	let (ir, pipeline, _) = placed_pipeline_over(concat_morsels(), writing);
	let placed = ir.placement(pipeline).expect("the pipeline is placed");
	assert!(
		admission_gap(placed, None).is_some(),
		"a fan-out whose facts state a write is refused"
	);
	let (ir, pipeline, _) = placed_pipeline(concat_morsels());
	let placed = ir.placement(pipeline).expect("the pipeline is placed");
	assert_eq!(admission_gap(placed, None), None);
}

/// A pipeline that holds a loop op the legality snapshot says nothing of has the serial verdict, so
/// a loop the logical boundary did not judge is never split.
#[test]
fn a_loop_the_snapshot_omits_has_the_serial_verdict() {
	use crate::exec::ir::phys::placement::loop_verdict;

	let (mut ir, pipeline, _) = placed_pipeline(concat_morsels());
	let mut cache = FactCache::new();
	cache.set_legality_snapshot(BTreeMap::new());
	assert!(loop_verdict(&ir, &cache, pipeline).is_none(), "a pipeline with no loop has none");
	edit(&mut ir, |rw| {
		let (_, block) =
			rw.create_region(pipeline, RegionKind::Pipeline, &[]).expect("the region is created");
		let kind = OpKind::Phys(PhysOp::Loop {
			kind: LoopKind::Select,
			first: false,
			batch: UNBATCHED,
			unroll: 1,
			value: false,
			step: None,
		});
		add(rw, InsertPoint::End(block), kind, &[], 0);
	});
	let verdict = loop_verdict(&ir, &cache, pipeline).expect("the loop op has a verdict");
	assert!(matches!(verdict.mode, LoopMode::Serial(_)), "{:?}", verdict.mode);
}

#[test]
fn legality_recompute_admits_a_serial_decision_for_any_loop() {
	let (ir, _, loop_op) = placed_pipeline(Decision::Serial);
	let ordered = LoopFacts {
		exit: Exit::Ordered(ExitCause::Break),
		..loop_facts()
	};
	let mut cache = FactCache::new();
	cache.set_legality_snapshot(BTreeMap::from([(loop_op, ordered)]));
	let at = VerifyAt::PhaseBoundary(Phase::Physical);
	assert!(
		violation_of(&ir, &mut cache, at, &PHYSICAL_PHASE, VerifyRule::LegalityRecompute).is_none()
	);
}

/// A statement whose `StoredClause` region holds one literal; answers the container and the
/// literal.
fn stored() -> (Ir, BlockId, OpId) {
	let mut ir = request();
	let top = top_block(&ir);
	let (block, inside) = edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, block) = rw
			.create_region(owner, RegionKind::StoredClause(StoredRole::Value), &[])
			.expect("the region is created");
		let (inside, values) = add(rw, InsertPoint::End(block), literal(1), &[], 1);
		close_with(rw, block, &[values[0]]);
		(block, inside)
	});
	(ir, block, inside)
}

#[test]
fn legality_exempts_the_interior_of_a_stored_clause() {
	let mut ir = request();
	let top = top_block(&ir);
	let inside = edit(&mut ir, |rw| {
		let owner_kind = OpKind::Phys(PhysOp::Stmt {
			commit: StatementCommit::Auto,
			control: None,
		});
		let (owner, _) = add(rw, InsertPoint::End(top), owner_kind, &[], 0);
		let (_, clause) = rw
			.create_region(owner, RegionKind::StoredClause(StoredRole::Value), &[])
			.expect("the region is created");
		let (inside, _) = add(rw, InsertPoint::End(clause), stmt_kind(), &[], 0);
		close_with(rw, clause, &[]);
		inside
	});
	assert!(physical_boundary_violation(&ir, VerifyRule::Legality).is_none());
	edit(&mut ir, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0));
	let failure = physical_boundary_violation(&ir, VerifyRule::Legality)
		.expect("a logical op outside stored code is illegal");
	assert_ne!(failure.op, Some(inside));
}

#[test]
fn stored_clause_admits_ops_created_before_bind_completes() {
	let (mut ir, block, _) = stored();
	assert!(logical_violation(&ir, VerifyRule::StoredClause).is_none());
	edit(&mut ir, |rw| {
		let at = InsertPoint::Before(last_op_of(rw.ir(), block));
		add(rw, at, literal(2), &[], 1);
	});
	assert!(logical_violation(&ir, VerifyRule::StoredClause).is_none());
}

#[test]
fn stored_clause_admits_a_clause_left_as_it_was_sealed() {
	let (mut ir, _, _) = stored();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	assert!(logical_violation(&ir, VerifyRule::StoredClause).is_none());
}

#[test]
fn stored_clause_rejects_an_op_edited_after_bind() {
	let (mut ir, _, inside) = stored();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	edit(&mut ir, |rw| rw.update_kind(inside, |kind| *kind = literal(9))).expect("kind updated");
	let failure = logical_violation(&ir, VerifyRule::StoredClause).expect("the seal is broken");
	assert_eq!(failure.op, Some(inside));
}

#[test]
fn stored_clause_rejects_an_op_created_after_bind() {
	let (mut ir, block, _) = stored();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	let added = edit(&mut ir, |rw| {
		let at = InsertPoint::Before(last_op_of(rw.ir(), block));
		add(rw, at, literal(2), &[], 1).0
	});
	let failure = logical_violation(&ir, VerifyRule::StoredClause).expect("no seal was recorded");
	assert_eq!(failure.op, Some(added));
}

#[test]
fn stored_clause_rejects_an_op_removed_after_bind() {
	let (mut ir, block, _) = stored();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	let last = last_op_of(&ir, block);
	edit(&mut ir, |rw| rw.erase(last, EliminationReason::DeadPure)).expect("the op is erased");
	let failure = logical_violation(&ir, VerifyRule::StoredClause).expect("the region lost an op");
	assert!(failure.detail.contains("stored-clause region"));
}

#[test]
fn stored_clause_rejects_a_clause_inlined_after_bind() {
	let (mut ir, block, _) = stored();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	let region = ir.block(block).expect("the block exists").parent;
	let top = top_block(&ir);
	edit(&mut ir, |rw| rw.inline_region(region, InsertPoint::End(top), &[]))
		.expect("the region is inlined");
	let failure =
		logical_violation(&ir, VerifyRule::StoredClause).expect("the region left its owner");
	assert!(failure.detail.contains("left its owner"));
}

#[test]
fn stored_clause_admits_the_erase_of_the_op_that_owns_it() {
	let (mut ir, _, _) = stored();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	let owner = ops_in(&ir, ir.top()).next().expect("the owner exists");
	edit(&mut ir, |rw| rw.erase(owner, EliminationReason::DeadPure)).expect("the owner is erased");
	assert!(ir.sealed_regions.iter().next().is_none());
	assert!(logical_violation(&ir, VerifyRule::StoredClause).is_none());
}

#[test]
fn stored_clause_leaves_ops_outside_the_clause_free_to_change() {
	let (mut ir, _, _) = stored();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	let top = top_block(&ir);
	edit(&mut ir, |rw| add(rw, InsertPoint::End(top), literal(5), &[], 1));
	assert!(logical_violation(&ir, VerifyRule::StoredClause).is_none());
}

// ---------------------------------------------------------------------------------------------
// The greedy driver
// ---------------------------------------------------------------------------------------------

/// The integer a literal op holds.
fn literal_int(ir: &Ir, op: OpId) -> Option<i64> {
	if let Some(data) = ir.op(op)
		&& let OpKind::Expr(ExprOp::Literal(held)) = &data.kind
		&& let Value::Number(Number::Int(n)) = **held
	{
		Some(n)
	} else {
		None
	}
}

fn set_literal(rw: &mut Rewriter<'_>, op: OpId, to: i64) -> Result<(), IrError> {
	rw.update_kind(op, |kind| *kind = literal(to))
}

/// A container whose top block holds one literal per entry of `values`.
fn literals(values: &[i64]) -> (Ir, Vec<OpId>) {
	let mut ir = request();
	let block = top_block(&ir);
	let ops = edit(&mut ir, |rw| {
		values.iter().map(|v| add(rw, InsertPoint::End(block), literal(*v), &[], 1).0).collect()
	});
	(ir, ops)
}

/// Raises every literal below `limit` by one.
struct CountUp {
	limit: i64,
}

impl Pattern<ExprDialect> for CountUp {
	fn name(&self) -> &'static str {
		"count_up"
	}

	fn root(&self) -> ExprTag {
		ExprTag::Literal
	}

	fn benefit(&self) -> u16 {
		1
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		match literal_int(rw.ir(), op) {
			Some(held) if held < self.limit => {
				set_literal(rw, op, held + 1)?;
				Ok(true)
			}
			_ => Ok(false),
		}
	}
}

#[tokio::test]
async fn greedy_reaches_a_fixed_point() {
	let (mut ir, ops) = literals(&[0]);
	let mut facts = FactCache::new();
	let mut set = PatternSet::<ExprDialect>::new();
	set.add(Box::new(CountUp {
		limit: 3,
	}));
	let report = apply_greedy(
		&mut ir,
		&mut facts,
		&set,
		&NotYetFolder,
		crate::exec::ir::analysis::cx::builtin_registry(),
		GREEDY_DEFAULT,
	)
	.await
	.expect("the driver answers");
	assert_eq!((report.passes, report.rewrites, report.stopped_by), (4, 3, None));
	assert_eq!(literal_int(&ir, ops[0]), Some(3));
	assert_valid(&ir);
}

#[tokio::test]
async fn greedy_stops_at_the_limits_and_reports_them() {
	let mut set = PatternSet::<ExprDialect>::new();
	set.add(Box::new(CountUp {
		limit: i64::MAX,
	}));
	let (mut ir, _) = literals(&[0]);
	let mut facts = FactCache::new();
	let limits = GreedyLimits {
		max_passes: 3,
		max_rewrites: GREEDY_DEFAULT.max_rewrites,
	};
	let report = apply_greedy(
		&mut ir,
		&mut facts,
		&set,
		&NotYetFolder,
		crate::exec::ir::analysis::cx::builtin_registry(),
		limits,
	)
	.await
	.expect("a limit is not an error");
	assert_eq!((report.passes, report.rewrites, report.stopped_by), (3, 3, Some(Limit::Passes)));
	let limits = GreedyLimits {
		max_passes: u8::MAX,
		max_rewrites: 5,
	};
	let report = apply_greedy(
		&mut ir,
		&mut facts,
		&set,
		&NotYetFolder,
		crate::exec::ir::analysis::cx::builtin_registry(),
		limits,
	)
	.await
	.expect("a limit is not an error");
	assert_eq!((report.passes, report.rewrites, report.stopped_by), (5, 5, Some(Limit::Rewrites)));
	assert_valid(&ir);
}

/// Erases the literal after the one it is offered, and records every op it is offered.
struct EraseNext {
	offered: std::sync::Arc<Mutex<Vec<OpId>>>,
}

impl Pattern<ExprDialect> for EraseNext {
	fn name(&self) -> &'static str {
		"erase_next"
	}

	fn root(&self) -> ExprTag {
		ExprTag::Literal
	}

	fn benefit(&self) -> u16 {
		1
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		self.offered.lock().expect("the log is not poisoned").push(op);
		let Some(parent) = rw.ir().op(op).map(|data| data.parent) else {
			return Ok(false);
		};
		let next = rw.ir().block(parent).and_then(|block| {
			let at = block.ops.iter().position(|held| *held == op)?;
			block.ops.get(at + 1).copied()
		});
		match next {
			Some(next) => {
				rw.erase(next, EliminationReason::DeadPure)?;
				Ok(true)
			}
			None => Ok(false),
		}
	}
}

#[tokio::test]
async fn greedy_skips_an_op_erased_earlier_in_the_pass() {
	let (mut ir, ops) = literals(&[1, 2]);
	let mut facts = FactCache::new();
	let offered = std::sync::Arc::new(Mutex::new(Vec::new()));
	let mut set = PatternSet::<ExprDialect>::new();
	set.add(Box::new(EraseNext {
		offered: std::sync::Arc::clone(&offered),
	}));
	let report = apply_greedy(
		&mut ir,
		&mut facts,
		&set,
		&NotYetFolder,
		crate::exec::ir::analysis::cx::builtin_registry(),
		GREEDY_DEFAULT,
	)
	.await
	.expect("the driver answers");
	assert_eq!((report.rewrites, report.stopped_by), (1, None));
	assert!(ir.op(ops[1]).is_none(), "the second literal is erased");
	let offered = offered.lock().expect("the log is not poisoned").clone();
	assert_eq!(offered, vec![ops[0], ops[0]], "the erased op is never offered");
}

/// Records its name when offered an op, and answers `applies`.
struct Named {
	name: &'static str,
	benefit: u16,
	applies: bool,
	log: std::sync::Arc<Mutex<Vec<&'static str>>>,
}

impl Pattern<ExprDialect> for Named {
	fn name(&self) -> &'static str {
		self.name
	}

	fn root(&self) -> ExprTag {
		ExprTag::Literal
	}

	fn benefit(&self) -> u16 {
		self.benefit
	}

	fn apply(&self, _op: OpId, _rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		self.log.lock().expect("the log is not poisoned").push(self.name);
		Ok(self.applies)
	}
}

#[tokio::test]
async fn greedy_never_offers_an_op_inside_a_stored_clause() {
	let (mut ir, _, _) = stored();
	let top = top_block(&ir);
	edit(&mut ir, |rw| add(rw, InsertPoint::End(top), literal(5), &[], 1));
	let log = std::sync::Arc::new(Mutex::new(Vec::new()));
	let mut set = PatternSet::<ExprDialect>::new();
	set.add(Box::new(Named {
		name: "named",
		benefit: 1,
		applies: false,
		log: std::sync::Arc::clone(&log),
	}));
	apply_greedy(
		&mut ir,
		&mut FactCache::new(),
		&set,
		&NotYetFolder,
		crate::exec::ir::analysis::cx::builtin_registry(),
		GREEDY_DEFAULT,
	)
	.await
	.expect("the driver answers");
	assert_eq!(log.lock().expect("the log is not poisoned").len(), 1);
}

#[tokio::test]
async fn greedy_tries_patterns_by_benefit() {
	let order = [("low", 1, false), ("high", 5, false), ("tied", 5, false)];
	for (first_applies, expected, rewrites) in
		[(false, vec!["high", "tied", "low"], 0), (true, vec!["high"], 1)]
	{
		let log = std::sync::Arc::new(Mutex::new(Vec::new()));
		let mut set = PatternSet::<ExprDialect>::new();
		for (name, benefit, _) in order {
			set.add(Box::new(Named {
				name,
				benefit,
				applies: first_applies && name == "high",
				log: std::sync::Arc::clone(&log),
			}));
		}
		let (mut ir, _) = literals(&[0]);
		let limits = GreedyLimits {
			max_passes: 1,
			max_rewrites: 1,
		};
		let report = apply_greedy(
			&mut ir,
			&mut FactCache::new(),
			&set,
			&NotYetFolder,
			crate::exec::ir::analysis::cx::builtin_registry(),
			limits,
		)
		.await
		.expect("the driver answers");
		assert_eq!(report.rewrites, rewrites);
		assert_eq!(*log.lock().expect("the log is not poisoned"), expected);
	}
}

/// Folds every request to the constant 42 and counts the requests it answers.
struct ConstantFolder {
	calls: AtomicUsize,
}

impl PlanTimeFolder for ConstantFolder {
	fn fold<'a>(
		&'a self,
		_ir: &'a Ir,
		_cx: &'a EffectCx<'a>,
		_req: FoldRequest,
	) -> Pin<Box<dyn Future<Output = FoldOutcome> + Send + 'a>> {
		self.calls.fetch_add(1, Ordering::SeqCst);
		Box::pin(ready(FoldOutcome::Constant {
			value: Value::from(42),
			premises: PremiseSet::new(),
			depth: 0,
		}))
	}
}

/// Replaces a literal by the constant its fold answers: it asks on the first pass and reads the
/// outcome on a later one.
struct UsesFold;

impl Pattern<ExprDialect> for UsesFold {
	fn name(&self) -> &'static str {
		"uses_fold"
	}

	fn root(&self) -> ExprTag {
		ExprTag::Literal
	}

	fn benefit(&self) -> u16 {
		1
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		let Some(result) = rw.ir().op(op).and_then(|data| data.results.first().copied()) else {
			return Ok(false);
		};
		match rw.fold_outcome(result).cloned() {
			None => {
				rw.request_fold(FoldRequest {
					value: result,
					consumer: FoldConsumer::AlgebraicRule,
				});
				Ok(false)
			}
			Some(FoldOutcome::Constant {
				value: Value::Number(Number::Int(folded)),
				..
			}) if literal_int(rw.ir(), op) != Some(folded) => {
				set_literal(rw, op, folded)?;
				Ok(true)
			}
			Some(_) => Ok(false),
		}
	}
}

#[tokio::test]
async fn greedy_awaits_folds_between_passes() {
	let (mut ir, ops) = literals(&[1]);
	let mut facts = FactCache::new();
	let mut set = PatternSet::<ExprDialect>::new();
	set.add(Box::new(UsesFold));
	let folder = ConstantFolder {
		calls: AtomicUsize::new(0),
	};
	let report = apply_greedy(
		&mut ir,
		&mut facts,
		&set,
		&folder,
		crate::exec::ir::analysis::cx::builtin_registry(),
		GREEDY_DEFAULT,
	)
	.await
	.expect("the driver answers");
	assert_eq!((report.passes, report.rewrites, report.stopped_by), (3, 1, None));
	assert_eq!(folder.calls.load(Ordering::SeqCst), 1);
	assert_eq!(literal_int(&ir, ops[0]), Some(42));
	assert_eq!(facts.pending_folds().count(), 0);
}

/// Creates `kind` before the op it is offered, once per pass.
struct Creates {
	kind: OpKind,
}

impl Creates {
	fn make(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		rw.create(InsertPoint::Before(op), spec(self.kind.clone(), &[], 0))?;
		Ok(true)
	}
}

impl Pattern<LogicalDialect> for Creates {
	fn name(&self) -> &'static str {
		"creates"
	}

	fn root(&self) -> LogTag {
		LogTag::Stmt
	}

	fn benefit(&self) -> u16 {
		1
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		self.make(op, rw)
	}
}

impl Pattern<PhysDialect> for Creates {
	fn name(&self) -> &'static str {
		"creates"
	}

	fn root(&self) -> PhysTag {
		PhysTag::Yield
	}

	fn benefit(&self) -> u16 {
		1
	}

	fn apply(&self, op: OpId, rw: &mut Rewriter<'_>) -> Result<bool, IrError> {
		self.make(op, rw)
	}
}

fn one_op(kind: OpKind) -> Ir {
	let mut ir = request();
	let block = top_block(&ir);
	edit(&mut ir, |rw| add(rw, InsertPoint::End(block), kind, &[], 0));
	ir
}

#[track_caller]
fn assert_legality(result: Result<super::pattern::GreedyReport, IrError>) {
	match result {
		Err(IrError::Verify(error)) => assert_eq!(error.rule, VerifyRule::Legality, "{error}"),
		other => panic!("expected a legality violation, got {other:?}"),
	}
}

#[tokio::test]
async fn a_logical_pattern_creating_a_phys_op_is_rejected() {
	let limits = GreedyLimits {
		max_passes: 2,
		max_rewrites: 1,
	};
	let phys = || OpKind::Phys(PhysOp::Yield);
	let mut ir = one_op(stmt_kind());
	let mut set = PatternSet::<LogicalDialect>::new();
	set.add(Box::new(Creates {
		kind: phys(),
	}));
	assert_legality(
		apply_greedy(
			&mut ir,
			&mut FactCache::new(),
			&set,
			&NotYetFolder,
			crate::exec::ir::analysis::cx::builtin_registry(),
			limits,
		)
		.await,
	);

	let mut ir = one_op(phys());
	let mut set = PatternSet::<PhysDialect>::new();
	set.add(Box::new(Creates {
		kind: stmt_kind(),
	}));
	assert_legality(
		apply_greedy(
			&mut ir,
			&mut FactCache::new(),
			&set,
			&NotYetFolder,
			crate::exec::ir::analysis::cx::builtin_registry(),
			limits,
		)
		.await,
	);

	// Expression ops are creatable by every dialect's patterns.
	let mut ir = one_op(stmt_kind());
	let mut set = PatternSet::<LogicalDialect>::new();
	set.add(Box::new(Creates {
		kind: literal(1),
	}));
	let report = apply_greedy(
		&mut ir,
		&mut FactCache::new(),
		&set,
		&NotYetFolder,
		crate::exec::ir::analysis::cx::builtin_registry(),
		limits,
	)
	.await
	.expect("a logical pattern may create an expression op");
	assert_eq!(report.stopped_by, Some(Limit::Rewrites));
}

// ---------------------------------------------------------------------------------------------
// The conversion driver
// ---------------------------------------------------------------------------------------------

struct NoPhysical;

impl PhysicalCatalog for NoPhysical {}

/// Runs `convert` to the physical target with an empty catalog and `folder`.
async fn convert_with(
	ir: &mut Ir,
	facts: &mut FactCache,
	set: &ConversionSet,
	folder: &dyn PlanTimeFolder,
) -> Result<ConversionReport, IrError> {
	let registry = FunctionRegistry::with_builtins();
	let frames = FrameTable::default();
	let cx = PhysCx {
		effects: EffectCx {
			registry: &registry,
			catalog: &NoCatalog,
			frames: &frames,
		},
		physical: &NoPhysical,
		config: crate::exec::ir::iface::catalog::default_config(),
	};
	convert(ir, facts, set, &cx, folder, &PHYSICAL_PHASE).await
}

async fn run_convert(ir: &mut Ir, set: &ConversionSet) -> Result<ConversionReport, IrError> {
	convert_with(ir, &mut FactCache::new(), set, &NotYetFolder).await
}

/// Converts `logical.stmt` as its fields say: creates `create` before the op, erases the op when
/// `erase` is set, and answers `answer`.
struct ConvertStmt {
	answer: Converted,
	create: Option<OpKind>,
	erase: bool,
}

impl ConversionPattern for ConvertStmt {
	fn name(&self) -> &'static str {
		"convert_stmt"
	}

	fn source(&self) -> LogTag {
		LogTag::Stmt
	}

	fn convert(
		&self,
		op: OpId,
		rw: &mut Rewriter<'_>,
		_cx: &PhysCx<'_>,
	) -> Result<Converted, IrError> {
		if let Some(kind) = &self.create {
			rw.create(InsertPoint::Before(op), spec(kind.clone(), &[], 0))?;
		}
		if self.erase {
			rw.erase(op, EliminationReason::DeadPure)?;
		}
		Ok(self.answer)
	}
}

fn converting(
	answer: Converted,
	create: Option<OpKind>,
	erase: bool,
) -> Box<dyn ConversionPattern> {
	Box::new(ConvertStmt {
		answer,
		create,
		erase,
	})
}

#[tokio::test]
async fn convert_records_provenance_through_derived() {
	let mut ir = one_op(stmt_kind());
	let stmt = ops_in(&ir, ir.top()).next().expect("the statement exists");
	let mut set = ConversionSet::new();
	set.add(converting(Converted::Replaced, Some(OpKind::Phys(PhysOp::Yield)), true));
	let report = run_convert(&mut ir, &set).await.expect("the conversion runs");
	assert_eq!((report.converted, report.eliminated, report.stopped_by), (1, 0, None));
	assert!(ir.op(stmt).is_none());
	let converted: Vec<OpId> = ops_in(&ir, ir.top()).collect();
	assert_eq!(converted.len(), 1);
	assert_eq!(provenance_of(&ir, converted[0]), from(&[stmt]));
	assert!(ir.eliminated().is_empty(), "the physical op holds the logical id");
	assert_eq!(ir.phase(), Phase::Bound, "the driver never changes the phase");
}

/// Converts `logical.stmt` to a `phys.yield` with one result, which takes the statement's.
struct ReplaceWithResult;

impl ConversionPattern for ReplaceWithResult {
	fn name(&self) -> &'static str {
		"replace_with_result"
	}

	fn source(&self) -> LogTag {
		LogTag::Stmt
	}

	fn convert(
		&self,
		op: OpId,
		rw: &mut Rewriter<'_>,
		_cx: &PhysCx<'_>,
	) -> Result<Converted, IrError> {
		let made = rw.create(InsertPoint::Before(op), spec(OpKind::Phys(PhysOp::Yield), &[], 1))?;
		let result = rw.ir().op(made).ok_or(IrError::Stale("op"))?.results[0];
		rw.replace_op(op, &[result])?;
		Ok(Converted::Replaced)
	}
}

#[tokio::test]
async fn convert_logged_answers_the_values_the_patterns_replaced() {
	let mut ir = request();
	let top = top_block(&ir);
	let (_, replaced) = edit(&mut ir, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 1));
	let mut set = ConversionSet::new();
	set.add(Box::new(ReplaceWithResult));
	let registry = FunctionRegistry::with_builtins();
	let frames = FrameTable::default();
	let cx = PhysCx {
		effects: EffectCx {
			registry: &registry,
			catalog: &NoCatalog,
			frames: &frames,
		},
		physical: &NoPhysical,
		config: crate::exec::ir::iface::catalog::default_config(),
	};
	let (report, log) =
		convert_logged(&mut ir, &mut FactCache::new(), &set, &cx, &NotYetFolder, &PHYSICAL_PHASE)
			.await
			.expect("the conversion runs");
	assert_eq!((report.converted, report.stopped_by), (1, None));
	let [(from, to)] = log[..] else {
		panic!("one replacement, found {log:?}");
	};
	assert_eq!(from, replaced[0]);
	let defined = match ir.value(to).map(|value| value.def) {
		Some(crate::exec::ir::core::value::ValueDef::OpResult {
			op,
			..
		}) => ir.op(op).map(|data| data.kind.clone()),
		_ => None,
	};
	assert!(matches!(defined, Some(OpKind::Phys(PhysOp::Yield))));

	// A run that converts nothing logs nothing, and `convert` answers the same report.
	let mut none = one_op(stmt_kind());
	let (report, log) = convert_logged(
		&mut none,
		&mut FactCache::new(),
		&ConversionSet::new(),
		&cx,
		&NotYetFolder,
		&PHYSICAL_PHASE,
	)
	.await
	.expect("the run answers");
	assert!(log.is_empty());
	assert_eq!(report.stopped_by, Some(Limit::Iterations));
}

#[tokio::test]
async fn convert_reports_iterations_when_no_pattern_applies() {
	let mut ir = one_op(stmt_kind());
	let mut set = ConversionSet::new();
	set.add(converting(Converted::NotApplicable, None, false));
	let report = run_convert(&mut ir, &set).await.expect("a limit is not an error");
	assert_eq!((report.converted, report.eliminated), (0, 0));
	assert_eq!(report.stopped_by, Some(Limit::Iterations));
	assert_eq!(ops_in(&ir, ir.top()).count(), 1);
	let violation = physical_boundary_violation(&ir, VerifyRule::Legality);
	assert!(violation.is_some(), "the boundary verification reports the op left behind");

	let mut ir = one_op(stmt_kind());
	let report = run_convert(&mut ir, &ConversionSet::new()).await.expect("the run answers");
	assert_eq!(report.stopped_by, Some(Limit::Iterations));

	let mut ir = one_op(literal(1));
	let report = run_convert(&mut ir, &ConversionSet::new()).await.expect("the run answers");
	assert_eq!((report.converted, report.stopped_by), (0, None));
}

#[tokio::test]
async fn eliminated_conversion_erases_and_records() {
	let mut ir = one_op(stmt_kind());
	let stmt = ops_in(&ir, ir.top()).next().expect("the statement exists");
	let mut set = ConversionSet::new();
	set.add(converting(Converted::Eliminated(EliminationReason::Unreachable), None, false));
	let report = run_convert(&mut ir, &set).await.expect("the conversion runs");
	assert_eq!((report.converted, report.eliminated, report.stopped_by), (0, 1, None));
	assert!(ir.op(stmt).is_none());
	assert_eq!(ir.eliminated().to_vec(), vec![(stmt, EliminationReason::Unreachable)]);

	// A pattern that erased the op itself is not asked to erase it twice.
	let mut ir = one_op(stmt_kind());
	let mut set = ConversionSet::new();
	set.add(converting(Converted::Eliminated(EliminationReason::DeadPure), None, true));
	let report = run_convert(&mut ir, &set).await.expect("the conversion runs");
	assert_eq!((report.eliminated, report.stopped_by), (1, None));

	// An op the pattern says it replaced must be gone.
	let mut ir = one_op(stmt_kind());
	let mut set = ConversionSet::new();
	set.add(converting(Converted::Replaced, None, false));
	let result = run_convert(&mut ir, &set).await;
	assert!(
		matches!(
			result,
			Err(IrError::Invariant {
				what: "convert: Replaced left the op"
			})
		),
		"{result:?}"
	);
}

#[tokio::test]
async fn a_conversion_that_creates_and_answers_not_applicable_is_rejected() {
	let mut ir = one_op(stmt_kind());
	let mut set = ConversionSet::new();
	set.add(converting(Converted::NotApplicable, Some(OpKind::Phys(PhysOp::Yield)), false));
	let result = run_convert(&mut ir, &set).await;
	assert!(
		matches!(
			result,
			Err(IrError::Invariant {
				what: "convert: NotApplicable after creating ops"
			})
		),
		"{result:?}"
	);
}

#[tokio::test]
async fn a_conversion_creating_a_logical_op_is_rejected() {
	let mut ir = one_op(stmt_kind());
	let mut set = ConversionSet::new();
	set.add(converting(Converted::Replaced, Some(stmt_kind()), true));
	match run_convert(&mut ir, &set).await {
		Err(IrError::Verify(error)) => assert_eq!(error.rule, VerifyRule::Legality, "{error}"),
		other => panic!("expected a legality violation, got {other:?}"),
	}
}

/// Records the ops it is offered, in order, and applies to none.
struct Offered {
	log: std::sync::Arc<Mutex<Vec<OpId>>>,
}

impl ConversionPattern for Offered {
	fn name(&self) -> &'static str {
		"offered"
	}

	fn source(&self) -> LogTag {
		LogTag::Stmt
	}

	fn convert(
		&self,
		op: OpId,
		_rw: &mut Rewriter<'_>,
		_cx: &PhysCx<'_>,
	) -> Result<Converted, IrError> {
		self.log.lock().expect("the log is not poisoned").push(op);
		Ok(Converted::NotApplicable)
	}
}

#[tokio::test]
async fn convert_visits_the_ops_of_a_region_before_their_owner() {
	let mut ir = request();
	let top = top_block(&ir);
	let (outer, inner) = edit(&mut ir, |rw| {
		let (outer, body) = open_statement(rw, top);
		let (inner, _) = add(rw, InsertPoint::End(body), stmt_kind(), &[], 0);
		(outer, inner)
	});
	let log = std::sync::Arc::new(Mutex::new(Vec::new()));
	let mut set = ConversionSet::new();
	set.add(Box::new(Offered {
		log: std::sync::Arc::clone(&log),
	}));
	let report = run_convert(&mut ir, &set).await.expect("the run answers");
	assert_eq!(report.stopped_by, Some(Limit::Iterations));
	assert_eq!(*log.lock().expect("the log is not poisoned"), vec![inner, outer]);
}

#[tokio::test]
async fn convert_never_offers_an_op_inside_a_stored_clause() {
	let mut ir = request();
	let top = top_block(&ir);
	let owner = edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, clause) = rw
			.create_region(owner, RegionKind::StoredClause(StoredRole::Value), &[])
			.expect("the region is created");
		add(rw, InsertPoint::End(clause), stmt_kind(), &[], 0);
		close_with(rw, clause, &[]);
		owner
	});
	let log = std::sync::Arc::new(Mutex::new(Vec::new()));
	let mut set = ConversionSet::new();
	set.add(Box::new(Offered {
		log: std::sync::Arc::clone(&log),
	}));
	let report = run_convert(&mut ir, &set).await.expect("the run answers");
	assert_eq!(report.stopped_by, Some(Limit::Iterations));
	assert_eq!(*log.lock().expect("the log is not poisoned"), vec![owner]);
}

/// Converts `logical.stmt` once the fold of a value is stored: it asks for the fold first.
struct NeedsFold {
	value: ValueId,
}

impl ConversionPattern for NeedsFold {
	fn name(&self) -> &'static str {
		"needs_fold"
	}

	fn source(&self) -> LogTag {
		LogTag::Stmt
	}

	fn convert(
		&self,
		op: OpId,
		rw: &mut Rewriter<'_>,
		_cx: &PhysCx<'_>,
	) -> Result<Converted, IrError> {
		if rw.fold_outcome(self.value).is_none() {
			rw.request_fold(FoldRequest {
				value: self.value,
				consumer: FoldConsumer::AlgebraicRule,
			});
			return Ok(Converted::NotApplicable);
		}
		rw.create(InsertPoint::Before(op), spec(OpKind::Phys(PhysOp::Yield), &[], 0))?;
		rw.erase(op, EliminationReason::DeadPure)?;
		Ok(Converted::Replaced)
	}
}

#[tokio::test]
async fn convert_awaits_folds_between_rounds() {
	let mut ir = request();
	let top = top_block(&ir);
	let value = edit(&mut ir, |rw| {
		let (_, values) = add(rw, InsertPoint::End(top), literal(1), &[], 1);
		add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		values[0]
	});
	let mut set = ConversionSet::new();
	set.add(Box::new(NeedsFold {
		value,
	}));
	let folder = ConstantFolder {
		calls: AtomicUsize::new(0),
	};
	let mut facts = FactCache::new();
	let report = convert_with(&mut ir, &mut facts, &set, &folder).await.expect("the run answers");
	assert_eq!((report.converted, report.stopped_by), (1, None));
	assert_eq!(folder.calls.load(Ordering::SeqCst), 1);
	assert_eq!(facts.pending_folds().count(), 0);
}

// ---------------------------------------------------------------------------------------------
// Text form
// ---------------------------------------------------------------------------------------------

/// The attribute syntax of the text tests: a literal prints `int <n>` or `str <text>`, and the
/// other ops the tests use have no attributes.
struct TextSyntax;

static TEXT_SYNTAX: TextSyntax = TextSyntax;

impl AttrSyntax for TextSyntax {
	fn print_attrs(&self, kind: &OpKind, out: &mut String) {
		let OpKind::Expr(ExprOp::Literal(value)) = kind else {
			return;
		};
		match &**value {
			Value::Number(Number::Int(n)) => out.push_str(&format!("int {n}")),
			Value::String(text) => {
				out.push_str("str ");
				out.push_str(text.as_str());
			}
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Date(_)
			| Value::Time(_)
			| Value::Timestamp(_)
			| Value::Interval(_)
			| Value::Uuid(_)
			| Value::Array(_)
			| Value::Set(_)
			| Value::Object(_)
			| Value::Geometry(_)
			| Value::Bytes(_)
			| Value::Table(_)
			| Value::RecordId(_)
			| Value::File(_)
			| Value::Regex(_)
			| Value::Range(_)
			| Value::Closure(_) => out.push_str("unsupported"),
		}
	}

	fn parse_attrs(&self, tag: OpTag, text: &str) -> Result<OpKind, ParseError> {
		let unreadable = || ParseError {
			line: 0,
			detail: format!("unreadable attributes `{text}`"),
		};
		if tag == OpTag::Expr(ExprTag::Literal) {
			if let Some(n) = text.strip_prefix("int ") {
				return n.parse::<i64>().map(literal).map_err(|_| unreadable());
			}
			if let Some(s) = text.strip_prefix("str ") {
				return Ok(OpKind::Expr(ExprOp::Literal(Box::new(Value::String(Strand::from(s))))));
			}
			return Err(unreadable());
		}
		if !text.is_empty() {
			return Err(unreadable());
		}
		match tag {
			OpTag::Expr(ExprTag::Yield) => Ok(yield_kind()),
			OpTag::Expr(ExprTag::Subquery) => {
				Ok(OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)))
			}
			OpTag::Expr(ExprTag::If) => Ok(OpKind::Expr(ExprOp::If)),
			OpTag::Expr(_) | OpTag::Logical(_) | OpTag::Phys(_) => Err(unreadable()),
		}
	}
}

/// A table that knows the expression syntax of the text tests only.
fn text_registry() -> AttrSyntaxes {
	let mut registry = AttrSyntaxes::NONE;
	registry.0[0] = Some(&TEXT_SYNTAX);
	registry
}

fn text_of(ir: &Ir, registry: &AttrSyntaxes) -> String {
	print_with(ir, ir.top(), PrintOpts::default(), registry)
}

/// The container `text` describes, read without the verification [`parse`] runs, so a fixture
/// that keeps no phase's rules still reads back.
fn reparse(text: &str, registry: &AttrSyntaxes) -> Result<Ir, ParseError> {
	parse_unverified(
		text,
		IrScope::Request {
			fragment: None,
		},
		registry,
	)
}

fn op_count(ir: &Ir) -> usize {
	let mut count = 0;
	ir.walk(ir.top(), WalkOrder::PreOrder, &mut |_| {
		count += 1;
		Walk::Continue
	});
	count
}

/// A spec whose results carry the given types.
fn typed(kind: OpKind, operands: &[ValueId], results: Vec<TypeKnowledge>) -> OpSpec {
	let mut built = spec(kind, operands, 0);
	built.results = results.into_iter().collect();
	built
}

#[test]
fn print_numbers_values_in_print_order() {
	let registry = text_registry();
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		// Created first, printed second: the numbers follow the text and never the ids.
		let (second, _) = add(rw, InsertPoint::End(top), literal(2), &[], 1);
		add(rw, InsertPoint::Before(second), literal(1), &[], 1);
		let (owner, _) = add(
			rw,
			InsertPoint::End(top),
			OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
			&[],
			1,
		);
		let (_, block) = rw
			.create_region(owner, RegionKind::Subquery, &[TypeKnowledge::Unknown])
			.expect("the region is created");
		add(rw, InsertPoint::End(block), literal(3), &[], 1);
	});
	let text = text_of(&ir, &registry);
	let lines: Vec<&str> = text.lines().collect();
	assert_eq!(lines[0], "ir request");
	assert!(lines[1].starts_with("#f0 = "), "{text}");
	assert_eq!(lines[2], "#s0 = current");
	assert_eq!(
		lines[3..].to_vec(),
		vec![
			r#"%0 = expr.literal() attrs("int 1")"#,
			r#"%1 = expr.literal() attrs("int 2")"#,
			"%2 = expr.subquery() {subquery ^bb(%3):",
			"\t%4 = expr.literal() attrs(\"int 3\")",
			"}",
		]
	);
	assert!(text.ends_with('\n'));
}

#[test]
fn a_value_outside_the_printed_region_prints_as_unresolved() {
	let registry = text_registry();
	let mut ir = request();
	let top = top_block(&ir);
	let region = edit(&mut ir, |rw| {
		let (_, outer) = add(rw, InsertPoint::End(top), literal(1), &[], 1);
		let (owner, _) = add(
			rw,
			InsertPoint::End(top),
			OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
			&[],
			1,
		);
		let (region, block) =
			rw.create_region(owner, RegionKind::Subquery, &[]).expect("the region is created");
		add(rw, InsertPoint::End(block), yield_kind(), &[outer[0]], 0);
		region
	});
	let text = print_with(&ir, region, PrintOpts::default(), &registry);
	assert_eq!(text, "{subquery ^bb():\n\texpr.yield(%?0)\n}\n");
	assert!(reparse(&text, &registry).is_err(), "a region alone is not a program");
}

#[test]
fn print_parse_round_trip_with_regions_and_block_args() {
	let registry = text_registry();
	let int = TypeKnowledge::Fact {
		kind: Kind::Int,
		source: FactSource::Literal,
	};
	let person = TypeKnowledge::Hint {
		kind: Kind::Record(vec![TableName::from("person")]),
		source: HintSource::DeclaredField {
			table: TableName::from("person"),
		},
	};
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		let a = rw
			.create(InsertPoint::End(top), typed(literal(1), &[], vec![int.clone()]))
			.expect("the op is created");
		let a_value = rw.ir().op(a).expect("the op exists").results[0];
		let owner = rw
			.create(
				InsertPoint::End(top),
				typed(
					OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
					&[a_value],
					vec![person.clone()],
				),
			)
			.expect("the op is created");
		let param = TypeKnowledge::Fact {
			kind: Kind::Int,
			source: FactSource::FunctionParam,
		};
		let (_, block) = rw
			.create_region(owner, RegionKind::Subquery, &[param, TypeKnowledge::Unknown])
			.expect("the region is created");
		let args = rw.ir().block(block).expect("the block exists").args.clone();
		add(rw, InsertPoint::End(block), literal(7), &[], 1);
		add(rw, InsertPoint::End(block), yield_kind(), &[args[0]], 0);

		let (branch, _) = add(rw, InsertPoint::End(top), OpKind::Expr(ExprOp::If), &[a_value], 0);
		let (_, then_block) =
			rw.create_region(branch, RegionKind::ValueIf, &[]).expect("the region is created");
		add(rw, InsertPoint::End(then_block), yield_kind(), &[a_value], 0);
		rw.create_region(branch, RegionKind::ValueIf, &[]).expect("the region is created");
	});

	let text = text_of(&ir, &registry);
	assert!(text.contains("^bb(%2: fact(int,param), %3):"), "{text}");
	assert!(text.contains(" : fact(int,literal)\n"), "{text}");
	assert!(text.contains(" : hint(record<person>,declared_field(\"person\"))\n"), "{text}");
	let parsed = reparse(&text, &registry).unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	assert_eq!(text_of(&parsed, &registry), text);
	assert_eq!(op_count(&parsed), op_count(&ir));

	let first = ops_in(&parsed, parsed.top()).next().expect("the first op exists");
	let value = parsed.op(first).expect("the op exists").results[0];
	assert_eq!(parsed.value(value).expect("the value exists").ty, int);
	assert_eq!(parsed.uses(value).len(), 3, "the owner, the branch and the arm read it");
}

#[test]
fn types_with_commas_and_quotes_round_trip() {
	let results = vec![
		TypeKnowledge::Fact {
			kind: Kind::Array(Box::new(Kind::Int), Some(3)),
			source: FactSource::StrictRowMigrated {
				table: TableName::from("we,ird\"x"),
			},
		},
		TypeKnowledge::Hint {
			kind: Kind::Either(vec![Kind::Int, Kind::String]),
			source: HintSource::Annotation,
		},
		TypeKnowledge::Unknown,
	];
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		rw.create(InsertPoint::End(top), typed(OpKind::Expr(ExprOp::Array), &[], results.clone()))
			.expect("the op is created");
	});
	let mut with_array_syntax = text_registry();
	with_array_syntax.0[0] = Some(&ARRAY_SYNTAX);
	let text = text_of(&ir, &with_array_syntax);
	assert!(text.contains("fact(array<int, 3>,strict_row_migrated(\"we,ird\\\"x\"))"), "{text}");
	let parsed =
		reparse(&text, &with_array_syntax).unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	let id = ops_in(&parsed, parsed.top()).next().expect("the op exists");
	let held: Vec<TypeKnowledge> = parsed
		.op(id)
		.expect("the op exists")
		.results
		.iter()
		.map(|value| parsed.value(*value).expect("the value exists").ty.clone())
		.collect();
	assert_eq!(held, results);
	assert_eq!(text_of(&parsed, &with_array_syntax), text);
}

/// Reads `expr.array` besides what [`TextSyntax`] reads.
struct ArraySyntax;

static ARRAY_SYNTAX: ArraySyntax = ArraySyntax;

impl AttrSyntax for ArraySyntax {
	fn print_attrs(&self, kind: &OpKind, out: &mut String) {
		TEXT_SYNTAX.print_attrs(kind, out);
	}

	fn parse_attrs(&self, tag: OpTag, text: &str) -> Result<OpKind, ParseError> {
		if tag == OpTag::Expr(ExprTag::Array) && text.is_empty() {
			return Ok(OpKind::Expr(ExprOp::Array));
		}
		TEXT_SYNTAX.parse_attrs(tag, text)
	}
}

#[test]
fn locations_and_provenance_print_only_when_asked_and_read_back() {
	let registry = text_registry();
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		let (a, _) = add(rw, InsertPoint::End(top), literal(1), &[], 1);
		let mut located = spec(literal(2), &[], 1);
		located.loc = Location {
			stmt: 3,
			span: Some(Span {
				start: 4,
				end: 9,
			}),
			origin: LocOrigin::Query,
		};
		rw.derived(&[a], |rw| rw.create(InsertPoint::End(top), located))
			.expect("the op is created");
		let (gone, _) = add(rw, InsertPoint::End(top), literal(3), &[], 1);
		rw.derived(&[gone], |rw| add(rw, InsertPoint::End(top), literal(4), &[], 1));
		rw.erase(gone, EliminationReason::DeadPure).expect("the op is erased");
	});
	let plain = text_of(&ir, &registry);
	assert!(!plain.contains("loc(") && !plain.contains("prov("), "{plain}");

	let opts = PrintOpts {
		locations: true,
		provenance: true,
		facts: false,
	};
	let text = print_with(&ir, ir.top(), opts, &registry);
	assert!(text.contains(" loc(s0 synth) prov(bound)\n"), "{text}");
	assert!(text.contains(" loc(s3:4..9 query) prov(@0)\n"), "{text}");
	assert!(text.contains(" prov(@?0)\n"), "{text}");
	let parsed = reparse(&text, &registry).unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	assert_eq!(print_with(&parsed, parsed.top(), opts, &registry), text);
	assert_eq!(text_of(&parsed, &registry), plain);

	let facts = PrintOpts {
		facts: true,
		..PrintOpts::default()
	};
	let with_facts = print_with(&ir, ir.top(), facts, &registry);
	assert!(with_facts.contains(" ; facts("), "{with_facts}");
	let read = reparse(&with_facts, &registry).expect("a facts comment is skipped");
	assert_eq!(text_of(&read, &registry), plain);
}

#[test]
fn a_parsed_container_keeps_its_phase_and_is_verified() {
	let registry = text_registry();
	let scope = || IrScope::Request {
		fragment: None,
	};
	let mut ir = request();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	let text = text_of(&ir, &registry);
	assert!(text.starts_with("ir request phase(logical)\n"), "{text}");
	let parsed = parse(&text, scope(), &registry).unwrap_or_else(|failure| panic!("{failure}"));
	assert_eq!(parsed.phase(), Phase::Logical);
	assert_eq!(text_of(&parsed, &registry), text);
	assert!(text_of(&request(), &registry).starts_with("ir request\n"), "bound prints no phase");

	// A logical op is outside the dialects the physical phase admits.
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0));
	edit(&mut ir, |rw| rw.advance_phase(Phase::Physical)).expect("the phase advances");
	let registry = crate::exec::ir::registry::attr_syntaxes();
	let text = text_of(&ir, registry);
	let failure = parse(&text, scope(), registry).expect_err("the container does not verify");
	assert!(failure.detail.contains("does not verify"), "{failure}");
	let read = parse_unverified(&text, scope(), registry).expect("the text itself reads");
	assert_eq!(read.phase(), Phase::Physical);
}

#[test]
fn an_op_id_in_the_attributes_prints_as_provenance_names_it() {
	let registry = crate::exec::ir::registry::attr_syntaxes();
	let pipeline = |logical: OpId| {
		OpKind::Phys(PhysOp::Pipeline(Box::new(PipelineAttrs {
			id: PipelineId(0),
			edges: SmallVec::new(),
			logical: Some(logical),
		})))
	};
	let mut ir = request();
	let top = top_block(&ir);
	let held = edit(&mut ir, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0);
	let gone = edit(&mut ir, |rw| add(rw, InsertPoint::End(top), literal(1), &[], 1).0);
	edit(&mut ir, |rw| rw.erase(gone, EliminationReason::DeadPure)).expect("the op is erased");
	edit(&mut ir, |rw| {
		add(rw, InsertPoint::End(top), pipeline(held), &[], 0);
		add(rw, InsertPoint::End(top), pipeline(gone), &[], 0);
	});
	let text = text_of(&ir, registry);
	assert!(text.contains("logical: some(@0)"), "{text}");
	assert!(text.contains("logical: some(@?0)"), "{text}");
	let read = reparse(&text, registry).unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	assert_eq!(text_of(&read, registry), text);
	let ops: Vec<OpId> = ops_in(&read, read.top()).collect();
	let named = match read.op(ops[1]).map(|data| &data.kind) {
		Some(OpKind::Phys(PhysOp::Pipeline(attrs))) => attrs.logical,
		other => panic!("not a pipeline: {other:?}"),
	};
	assert_eq!(named, Some(ops[0]), "the name reads back as the op it names");
}

#[test]
fn a_provenance_clause_names_only_earlier_ops_that_stand_for_themselves() {
	let registry = text_registry();
	let head = text_of(&request(), &registry);
	let literal_line = "%0 = expr.literal() attrs(\"int 1\")";
	let cases = [
		(format!("{literal_line} prov(@0)\n"), "@0 is not an op built before this one"),
		(format!("{literal_line}\n{literal_line} prov(@0)\n"), "defined twice"),
		(format!("{literal_line} prov(@?1)\n"), "numbered in order of first appearance"),
	];
	for (body, detail) in cases {
		let failure = reparse(&format!("{head}{body}"), &registry).expect_err(&body);
		assert!(failure.detail.contains(detail), "{body}: {failure}");
	}
}

#[test]
fn a_snapshot_and_a_frame_clause_round_trip() {
	let registry = text_registry();
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		let (_, values) = add(rw, InsertPoint::End(top), literal(5), &[], 1);
		let snapshot =
			rw.intern_snapshot(Snapshot::Version(values[0])).expect("the snapshot is interned");
		let mut versioned = spec(literal(6), &[], 1);
		versioned.snapshot = snapshot;
		rw.create(InsertPoint::End(top), versioned).expect("the op is created");
	});
	let text = text_of(&ir, &registry);
	assert!(text.contains("#s1 = version(%0)\n"), "{text}");
	assert!(text.contains(" [frame=#f0 snap=#s1]"), "{text}");
	let parsed = reparse(&text, &registry).unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	assert_eq!(text_of(&parsed, &registry), text);
	assert!(parsed.snapshot(SnapshotId::new(1)).is_some());
}

#[test]
fn every_frame_prints_as_canonical_text_and_reads_back() {
	let registry = text_registry();
	let mut ir = request();
	let top = top_block(&ir);
	let limited = AuthorityFrame {
		eval: EvalFrame {
			mode: crate::expr::logical::EvalMode::Computed,
			auth: Some(crate::iam::AuthLimit::new(
				crate::iam::Level::Database("n\"s".to_owned(), "db".to_owned()),
				Some(crate::iam::Role::Editor),
			)),
			key_part: true,
		},
		unit: UnitFrame::from_parts(
			0x8000_0000_0000_0001,
			Some("ns".to_owned()),
			None,
			true,
			false,
		),
	};
	edit(&mut ir, |rw| {
		for frame in [
			AuthorityFrame {
				eval: EvalFrame::permission(None),
				unit: UnitFrame::unprincipled(),
			},
			limited.clone(),
		] {
			let other = rw.intern_frame(frame).expect("the frame is interned");
			let mut framed = spec(literal(1), &[], 1);
			framed.frame = other;
			rw.create(InsertPoint::End(top), framed).expect("the op is created");
		}
	});
	let text = text_of(&ir, &registry);
	assert!(!text.contains("AuthorityFrame"), "{text}");
	assert!(
		text.contains(
			"#f2 = eval(computed, limit(db(\"n\\\"s\", \"db\"), editor), key_part) \
			 unit(0x8000000000000001, \"ns\", none, suppressed, unchecked)\n"
		),
		"{text}"
	);
	let parsed = reparse(&text, &registry).unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	assert_eq!(text_of(&parsed, &registry), text);
	assert_eq!(parsed.frames().frames, ir.frames().frames);
	assert_eq!(parsed.frame(FrameId::new(2)), Some(&limited));

	let repeated = text.replace("#f2 = eval(computed", "#f2 = eval(permission");
	let repeated = repeated.replacen(
		"limit(db(\"n\\\"s\", \"db\"), editor), key_part) unit(0x8000000000000001, \"ns\", none, suppressed, unchecked)",
		"none, whole) unit(0x0000000000000000, none, none, enforced, checked)",
		1,
	);
	let failure = reparse(&repeated, &registry).expect_err("a frame is declared once");
	assert!(failure.detail.contains("repeats"), "{failure}");

	let head = text_of(&request(), &registry);
	let undefined = format!("{head}expr.yield() [frame=#f9 snap=#s0]\n");
	let failure = reparse(&undefined, &registry).expect_err("an undefined frame is rejected");
	assert_eq!(failure.line, 4);
}

#[test]
fn parse_rejects_unknown_mnemonic_with_line() {
	let registry = text_registry();
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		add(rw, InsertPoint::End(top), literal(1), &[], 1);
	});
	let text = text_of(&ir, &registry);
	let lines = u32::try_from(text.lines().count()).expect("a short text");
	let failure = reparse(&format!("{text}expr.nope()\n"), &registry)
		.expect_err("an unknown mnemonic is rejected");
	assert_eq!(failure.line, lines + 1);
	assert!(failure.detail.contains("expr.nope"), "{failure}");

	for text in ["", "; print: not yet\n"] {
		let failure = reparse(text, &registry).expect_err("a text without a header is rejected");
		assert_eq!(failure.line, 1, "{text:?}");
	}

	let failure = reparse(&text, &AttrSyntaxes::NONE).expect_err("no syntax reads the attributes");
	assert_eq!(failure.line, 4);
	assert_eq!(failure.detail, "no attribute syntax for expr");
	let unread = print_with(&ir, ir.top(), PrintOpts::default(), &AttrSyntaxes::NONE);
	assert!(unread.contains("attrs(\"?\")"), "{unread}");
}

#[test]
fn parse_reports_the_line_of_a_malformed_op() {
	let registry = text_registry();
	let head = text_of(&request(), &registry);
	assert_eq!(head.lines().count(), 3);
	let literal_line = "%0 = expr.literal() attrs(\"int 1\")";
	let cases = [
		(format!("{literal_line}\n{literal_line}\n"), 5, "defined twice"),
		("expr.yield(%?0)\n".to_owned(), 4, "outside the text"),
		("expr.yield(%7)\n".to_owned(), 4, "%7 is not defined"),
		("%0 = expr.subquery() {subquery ^bb():\n".to_owned(), 5, "is not closed"),
		("}\n".to_owned(), 4, "closes no region"),
		("expr.literal() attrs(\"int 1\n".to_owned(), 4, "unterminated"),
		(format!("{literal_line} : fact(int,bogus)\n"), 4, "unknown fact source `bogus`"),
		(format!("{literal_line} : unknown,unknown\n"), 4, "2 types for 1 results"),
		(format!("{literal_line} trailing\n"), 4, "unexpected `trailing`"),
	];
	for (body, line, detail) in cases {
		let failure = reparse(&format!("{head}{body}"), &registry).expect_err(&body);
		assert_eq!(failure.line, line, "{body}: {failure}");
		assert!(failure.detail.contains(detail), "{body}: {failure}");
	}
}

#[test]
fn quoted_attrs_round_trip_escapes() {
	let registry = text_registry();
	let raw = "a\"b\\c\nd; (e) {f}";
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		let value = Value::String(Strand::from(raw));
		add(rw, InsertPoint::End(top), OpKind::Expr(ExprOp::Literal(Box::new(value))), &[], 1);
	});
	let text = text_of(&ir, &registry);
	assert!(text.contains(r#"attrs("str a\"b\\c\nd; (e) {f}")"#), "{text}");
	assert_eq!(text.lines().count(), 4, "{text}");
	let parsed = reparse(&text, &registry).unwrap_or_else(|failure| panic!("{failure}\n{text}"));
	let id = ops_in(&parsed, parsed.top()).next().expect("the op exists");
	match &parsed.op(id).expect("the op exists").kind {
		OpKind::Expr(ExprOp::Literal(value)) => {
			assert_eq!(**value, Value::String(Strand::from(raw)));
		}
		other @ (OpKind::Expr(_) | OpKind::Logical(_) | OpKind::Phys(_)) => panic!("{other:?}"),
	}
	assert_eq!(text_of(&parsed, &registry), text);
}

#[test]
fn every_region_kind_name_round_trips() {
	let registry = text_registry();
	let mut seen = std::collections::HashSet::new();
	for kind in ALL_REGION_KINDS {
		let name = region_kind_name(*kind);
		assert!(seen.insert(name), "{name} names two kinds");
		assert_eq!(region_kind_from_name(name), Some(*kind), "{name}");
		let ir = Ir::new(
			IrScope::Request {
				fragment: None,
			},
			*kind,
		);
		let text = text_of(&ir, &registry);
		assert!(text.starts_with(&format!("ir {name}\n")), "{text}");
		let parsed = reparse(&text, &registry).unwrap_or_else(|failure| panic!("{failure}"));
		assert_eq!(parsed.region(parsed.top()).map(|region| region.kind), Some(*kind), "{name}");
	}
	assert_eq!(region_kind_from_name("no.such.kind"), None);
}

// ---------------------------------------------------------------------------------------------
// Statement copy, adoption and region clone
// ---------------------------------------------------------------------------------------------

fn int_fact() -> TypeKnowledge {
	TypeKnowledge::Fact {
		kind: Kind::Int,
		source: FactSource::Literal,
	}
}

/// A request that holds an outside literal and an outside non-literal with a typed result, and a
/// statement that reads both beside two literals of its own, one of them unused.
struct Reader {
	ir: Ir,
	statement: OpId,
	body: BlockId,
	outside_literal: ValueId,
	outside_other: ValueId,
	other: OpId,
	kept: OpId,
	array: OpId,
	dead: OpId,
}

fn reader() -> Reader {
	let mut ir = request();
	let top = top_block(&ir);
	let (statement, body, outside_literal, outside_other, other, kept, array, dead) =
		edit(&mut ir, |rw| {
			let (_, literal_values) = add(rw, InsertPoint::End(top), literal(7), &[], 1);
			let other = rw
				.create(
					InsertPoint::End(top),
					typed(OpKind::Expr(ExprOp::Array), &[literal_values[0]], vec![int_fact()]),
				)
				.expect("the op is created");
			let other_value = rw.ir().op(other).expect("the op exists").results[0];
			let (statement, body) = open_statement(rw, top);
			let (kept, kept_values) = add(rw, InsertPoint::End(body), literal(2), &[], 1);
			let (array, array_values) = add(
				rw,
				InsertPoint::End(body),
				OpKind::Expr(ExprOp::Array),
				&[literal_values[0], other_value, kept_values[0]],
				1,
			);
			let (dead, _) = add(rw, InsertPoint::End(body), literal(3), &[], 1);
			close_with(rw, body, &[array_values[0]]);
			(statement, body, literal_values[0], other_value, other, kept, array, dead)
		});
	Reader {
		ir,
		statement,
		body,
		outside_literal,
		outside_other,
		other,
		kept,
		array,
		dead,
	}
}

#[test]
fn adopt_statement_round_trip() {
	let registry = text_registry();
	let mut f = reader();
	let before = text_of(&f.ir, &registry);
	let uses_before = (f.ir.uses(f.outside_literal).len(), f.ir.uses(f.outside_other).len());
	let (copy, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	assert_eq!(text_of(&f.ir, &registry), before, "a copy leaves the original as it was");
	f.ir.adopt_statement(copy, &map).expect("the copy is adopted");
	assert_eq!(text_of(&f.ir, &registry), before);
	assert!(f.ir.op(f.statement).is_some(), "the statement keeps its id");
	assert_eq!(
		(f.ir.uses(f.outside_literal).len(), f.ir.uses(f.outside_other).len()),
		uses_before,
		"the adopted ops read the original outside values"
	);
	assert!(f.ir.op(f.array).is_none(), "the adopted ops have fresh ids");
	let adopted = f.ir.block(f.body).expect("the block exists").ops.clone();
	assert_eq!(adopted.len(), 4);
	for op in &adopted {
		assert_ne!(*op, f.kept);
		assert_eq!(f.ir.generation(*op), 1);
	}
	assert_eq!(provenance_of(&f.ir, adopted[0]), from(&[f.kept]));
	assert_eq!(provenance_of(&f.ir, adopted[1]), from(&[f.array]));
	for id in [f.kept, f.array, f.dead] {
		assert_eq!(f.ir.holders.get(&id), Some(&1));
	}
	assert!(f.ir.eliminated().is_empty());
	assert!(logical_violation(&f.ir, VerifyRule::UseList).is_none());
	assert!(logical_violation(&f.ir, VerifyRule::Dominance).is_none());
}

#[test]
fn adopt_statement_maps_eliminated_entries_back() {
	let registry = text_registry();
	let mut f = reader();
	let before = text_of(&f.ir, &registry);
	let (mut copy, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	let dead = map.map_op(f.dead).expect("the op is copied");
	assert_eq!(map.original_op(dead), Some(f.dead));
	edit(&mut copy, |rw| rw.erase(dead, EliminationReason::FoldedConstant))
		.expect("the copy's op is erased");
	assert_eq!(copy.eliminated().to_vec(), vec![(dead, EliminationReason::FoldedConstant)]);
	f.ir.adopt_statement(copy, &map).expect("the copy is adopted");
	assert_eq!(f.ir.eliminated().to_vec(), vec![(f.dead, EliminationReason::FoldedConstant)]);
	assert!(!f.ir.holders.contains_key(&f.dead));
	let after = text_of(&f.ir, &registry);
	assert_eq!(after.lines().count() + 1, before.lines().count(), "{after}");
}

#[test]
fn adopting_the_backup_removes_ids_a_surviving_op_holds_again() {
	let registry = text_registry();
	let mut f = reader();
	let before = text_of(&f.ir, &registry);
	let (backup, backup_map) = f.ir.copy_statement(f.statement).expect("the backup is copied");
	let (mut target, map) = f.ir.copy_statement(f.statement).expect("the target is copied");
	let dead = map.map_op(f.dead).expect("the op is copied");
	edit(&mut target, |rw| rw.erase(dead, EliminationReason::DeadPure)).expect("the op is erased");
	f.ir.adopt_statement(target, &map).expect("the target is adopted");
	assert_eq!(f.ir.eliminated().len(), 1);
	f.ir.adopt_statement(backup, &backup_map).expect("the backup is adopted");
	assert!(f.ir.eliminated().is_empty(), "the backup's op holds the id again");
	assert_eq!(f.ir.holders.get(&f.dead), Some(&1));
	assert_eq!(text_of(&f.ir, &registry), before);
}

#[test]
fn adoption_remaps_the_provenance_of_ops_the_copy_created() {
	let mut f = reader();
	let originals = f.ir.block(f.body).expect("the block exists").ops.clone();
	let (mut copy, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	let copied_kept = map.map_op(f.kept).expect("the op is copied");
	let copy_block = copy.region(copy.top()).expect("the top region exists").blocks[0];
	let derived_op = edit(&mut copy, |rw| {
		let (derived_op, _) = rw.derived(&[copied_kept], |rw| {
			add(rw, InsertPoint::Before(copied_kept), literal(40), &[], 1)
		});
		let (born_op, _) = add(rw, InsertPoint::End(copy_block), literal(41), &[], 1);
		let (_, _) =
			rw.derived(&[born_op], |rw| add(rw, InsertPoint::End(copy_block), literal(42), &[], 1));
		derived_op
	});
	assert_eq!(provenance_of(&copy, derived_op), from(&[copied_kept]));
	f.ir.adopt_statement(copy, &map).expect("the copy is adopted");
	let ops = f.ir.block(f.body).expect("the block exists").ops.clone();
	let adopted_derived = ops[0];
	assert_eq!(provenance_of(&f.ir, adopted_derived), from(&[f.kept]));
	for (original, adopted) in originals.iter().zip(&ops[1..]) {
		assert_eq!(
			provenance_of(&f.ir, *adopted),
			from(&[*original]),
			"an adopted copy stands for exactly its original"
		);
	}
	let adopted_born = ops[ops.len() - 2];
	let adopted_derived_from_born = ops[ops.len() - 1];
	assert_eq!(provenance_of(&f.ir, adopted_born), Provenance::Bound);
	assert_eq!(provenance_of(&f.ir, adopted_derived_from_born), from(&[adopted_born]));
	assert_eq!(f.ir.holders.get(&adopted_born), Some(&2), "itself and the op derived from it");
	assert!(f.ir.bind_scope.contains_key(adopted_born));
}

#[test]
fn copy_keeps_its_ops_standing_for_themselves() {
	let f = reader();
	let (copy, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	for original in [f.kept, f.array, f.dead] {
		let copied = map.map_op(original).expect("the op is copied");
		assert_eq!(map.original_op(copied), Some(original));
		assert_eq!(provenance_of(&copy, copied), Provenance::Bound);
		assert_eq!(copy.holders.get(&copied), Some(&1));
	}
	assert_eq!(
		map.original_region(copy.top()),
		Some(f.ir.op(f.statement).expect("the statement exists").regions[0]),
	);
}

#[test]
fn copy_gives_an_uncopied_provenance_id_a_key_that_names_no_op() {
	let mut f = reader();
	let derived_op = edit(&mut f.ir, |rw| {
		rw.derived(&[f.other], |rw| add(rw, InsertPoint::Before(f.dead), literal(11), &[], 1).0)
	});
	assert_eq!(provenance_of(&f.ir, derived_op), from(&[f.other]));
	let (copy, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	let copied = map.map_op(derived_op).expect("the op is copied");
	let Provenance::From(ids) = provenance_of(&copy, copied) else {
		panic!("the copy of a derived op is derived");
	};
	assert_eq!(ids.len(), 1);
	assert!(copy.op(ids[0]).is_none());
	assert_eq!(map.original_op(ids[0]), Some(f.other));
	assert_eq!(copy.holders.get(&ids[0]), Some(&1));
	f.ir.adopt_statement(copy, &map).expect("the copy is adopted");
	let adopted = f.ir.block(f.body).expect("the block exists").ops.clone();
	assert_eq!(provenance_of(&f.ir, adopted[2]), from(&[f.other]));
	assert_eq!(f.ir.holders.get(&f.other), Some(&2), "its own id and the adopted op");
}

#[test]
fn copy_brings_literal_definitions_of_outside_values() {
	let f = reader();
	let (copy, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	assert_eq!(copy.phase(), f.ir.phase());
	assert_eq!(copy.scope(), f.ir.scope());
	assert_eq!(
		copy.region(copy.top()).map(|region| region.kind),
		Some(RegionKind::Statement(StatementCommit::Auto))
	);
	let block = copy.block(top_block(&copy)).expect("the top block exists");
	assert_eq!(block.ops.len(), 5, "the literal leads the copied statement");
	let lead = block.ops[0];
	assert!(map.is_outside_copy(lead));
	assert!(matches!(
		&copy.op(lead).expect("the op exists").kind,
		OpKind::Expr(ExprOp::Literal(value)) if **value == Value::from(7_i64)
	));
	assert_eq!(block.args.len(), 2, "the statement's timestamp, then the non-literal");
	let argument = block.args[1];
	assert_eq!(copy.value(argument).expect("the value exists").ty, int_fact());
	assert_eq!(map.original_value(argument), Some(f.outside_other));
	assert_eq!(map.map_op(f.other), None, "the non-literal's op is not copied");
	let array = map.map_op(f.array).expect("the op is copied");
	let lead_value = copy.op(lead).expect("the op exists").results[0];
	let kept_value = copy.op(map.map_op(f.kept).expect("copied")).expect("exists").results[0];
	assert_eq!(
		copy.op(array).expect("the op exists").operands.to_vec(),
		vec![lead_value, argument, kept_value]
	);
	assert_eq!(map.original_value(lead_value), Some(f.outside_literal));
	assert!(f.ir.op(f.other).is_some());
}

#[test]
fn copy_statement_needs_a_statement_op() {
	let f = reader();
	let top_ops: Vec<OpId> = ops_in(&f.ir, f.ir.top()).collect();
	let failure = f.ir.copy_statement(top_ops[0]);
	assert!(matches!(failure, Err(IrError::Refused("statement op"))));
	let failure = f.ir.copy_statement(f.kept);
	assert!(matches!(failure, Err(IrError::Refused("statement op"))));
}

#[test]
fn a_failed_adoption_changes_nothing() {
	let registry = text_registry();
	let mut f = reader();
	let before = text_of(&f.ir, &registry);
	let shape = (f.ir.ops.len(), f.ir.values.len(), f.ir.holders.len(), f.ir.eliminated().len());
	let (_, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	let other = Ir::new(
		IrScope::Request {
			fragment: None,
		},
		RegionKind::Request,
	);
	let failure = f.ir.adopt_statement(other, &map);
	assert!(matches!(failure, Err(IrError::Refused("adopt region kind"))));
	assert_eq!(text_of(&f.ir, &registry), before);
	assert_eq!(
		(f.ir.ops.len(), f.ir.values.len(), f.ir.holders.len(), f.ir.eliminated().len()),
		shape
	);
	assert!(logical_violation(&f.ir, VerifyRule::UseList).is_none());
}

/// An adoption that fails while it wires the adopted ops leaves the container as it was: the
/// frame and snapshot an op wired before the failure interned are gone with the rest.
#[test]
fn an_adoption_that_fails_in_wiring_changes_nothing() {
	let registry = text_registry();
	let mut f = reader();
	let (mut copy, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	// The copy reads `outside_other` as an outside value; once its definer is gone, the value
	// resolves to nothing here and wiring the array that reads it fails.
	let (other, outside_other, outside_literal) = (f.other, f.outside_other, f.outside_literal);
	edit(&mut f.ir, |rw| {
		rw.replace_all_uses(outside_other, outside_literal).expect("the uses are replaced");
		rw.erase(other, EliminationReason::DeadPure).expect("the op is erased");
	});
	// The copy of `kept`, which is wired before the array, names a frame and a snapshot that are
	// new to the container.
	let first = map.map_op(f.kept).expect("the kept literal is copied");
	let read = copy.op(first).expect("the op exists").results[0];
	let (frame, snapshot) = edit(&mut copy, |rw| {
		let frame = rw
			.intern_frame(AuthorityFrame {
				eval: EvalFrame::computed(None),
				unit: UnitFrame::unprincipled(),
			})
			.expect("the frame is interned");
		(frame, rw.intern_snapshot(Snapshot::Version(read)).expect("interned"))
	});
	if let Some(held) = copy.ops.get_mut(first) {
		held.frame = frame;
		held.snapshot = snapshot;
	}
	let before = text_of(&f.ir, &registry);
	let shape = (f.ir.ops.len(), f.ir.values.len(), f.ir.holders.len(), f.ir.eliminated().len());
	let tables = (f.ir.frames.frames.len(), f.ir.snapshots.len());
	let failure = f.ir.adopt_statement(copy, &map);
	assert!(matches!(failure, Err(IrError::Refused("adopt value"))), "{failure:?}");
	assert_eq!(text_of(&f.ir, &registry), before);
	assert_eq!(
		(f.ir.ops.len(), f.ir.values.len(), f.ir.holders.len(), f.ir.eliminated().len()),
		shape
	);
	assert_eq!((f.ir.frames.frames.len(), f.ir.snapshots.len()), tables);
	assert!(logical_violation(&f.ir, VerifyRule::UseList).is_none());
}

#[test]
fn the_rewriters_adoption_drops_the_cached_facts_of_the_replaced_code() {
	let mut f = reader();
	let (copy, map) = f.ir.copy_statement(f.statement).expect("the statement is copied");
	let value = f.ir.op(f.kept).expect("the op exists").results[0];
	let mut facts = FactCache::new();
	{
		let mut rw = Rewriter::new(&mut f.ir, &mut facts)
			.with_registry(crate::exec::ir::analysis::cx::builtin_registry());
		rw.request_fold(FoldRequest {
			value,
			consumer: FoldConsumer::AlgebraicRule,
		});
		let _ = rw.facts(f.kept);
	}
	let generation = f.ir.generation(f.kept);
	assert_eq!(facts.pending_folds().count(), 1);
	assert!(facts.get(f.kept, generation, &default_key(&f.ir)).is_some());
	Rewriter::new(&mut f.ir, &mut facts).adopt_statement(copy, &map).expect("the copy is adopted");
	assert_eq!(facts.pending_folds().count(), 0);
	assert!(facts.get(f.kept, generation, &default_key(&f.ir)).is_none());
}

/// A statement whose body holds an op that owns a stored clause; answers the container, the
/// statement, its body block and the clause's literal.
fn statement_with_a_stored_clause() -> (Ir, OpId, BlockId, OpId) {
	let mut ir = request();
	let top = top_block(&ir);
	let (statement, body, inside) = edit(&mut ir, |rw| {
		let (statement, body) = open_statement(rw, top);
		let (owner, _) = add(rw, InsertPoint::End(body), stmt_kind(), &[], 0);
		let (_, clause) = rw
			.create_region(owner, RegionKind::StoredClause(StoredRole::Value), &[])
			.expect("the region is created");
		let (inside, values) = add(rw, InsertPoint::End(clause), literal(1), &[], 1);
		close_with(rw, clause, &[values[0]]);
		close_with(rw, body, &[]);
		(statement, body, inside)
	});
	(ir, statement, body, inside)
}

#[test]
fn adopted_stored_clause_still_verifies() {
	let (mut ir, statement, _, inside) = statement_with_a_stored_clause();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	assert!(logical_violation(&ir, VerifyRule::StoredClause).is_none());
	let (copy, map) = ir.copy_statement(statement).expect("the statement is copied");
	assert_eq!(copy.phase(), Phase::Logical);
	let copied = map.map_op(inside).expect("the op is copied");
	assert_eq!(copy.sealed.get(copied), Some(&copy.generation(copied)));
	ir.adopt_statement(copy, &map).expect("the copy is adopted");
	assert!(logical_violation(&ir, VerifyRule::StoredClause).is_none());
	assert!(ir.op(inside).is_none());
	assert_eq!(ir.sealed.len(), 2, "the clause's literal and yield are sealed again");
	for (op, seal) in ir.sealed.iter() {
		assert_eq!(*seal, ir.generation(op));
	}
}

#[test]
fn an_edit_in_the_copy_breaks_the_seal_of_the_adopted_op() {
	let (mut ir, statement, _, inside) = statement_with_a_stored_clause();
	edit(&mut ir, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	let (mut copy, map) = ir.copy_statement(statement).expect("the statement is copied");
	let copied = map.map_op(inside).expect("the op is copied");
	edit(&mut copy, |rw| rw.update_kind(copied, |kind| *kind = literal(9))).expect("kind updated");
	ir.adopt_statement(copy, &map).expect("the copy is adopted");
	let failure = logical_violation(&ir, VerifyRule::StoredClause).expect("the seal is broken");
	assert_eq!(
		literal_int(&ir, failure.op.expect("the violation names an op")),
		Some(9),
		"{failure}"
	);
}

// ---------------------------------------------------------------------------------------------
// A region as a unit of its own
// ---------------------------------------------------------------------------------------------

fn free_param(name: &str) -> OpKind {
	OpKind::Expr(ExprOp::FreeParam {
		name: Strand::new(name),
	})
}

/// A request whose statement holds an op that owns a region of `kind` with one argument. The
/// region reads an outside literal, an outside free parameter (an op with no operand that is no
/// literal), an outside array (an op with an operand), its own argument and a literal of its own,
/// and yields their array.
struct Unit {
	ir: Ir,
	region: RegionId,
	argument: ValueId,
	outside_literal: ValueId,
	outside_free: ValueId,
	outside_array: ValueId,
	timestamp: ValueId,
}

fn unit_of(kind: RegionKind) -> Unit {
	let mut ir = request();
	let top = top_block(&ir);
	let (region, argument, outside_literal, outside_free, outside_array, timestamp) =
		edit(&mut ir, |rw| {
			let (statement, _) = conform(rw, InsertPoint::End(top), stmt_kind());
			let (_, body) = rw
				.create_region(
					statement,
					RegionKind::Statement(StatementCommit::Auto),
					&[TypeKnowledge::Unknown],
				)
				.expect("the region is created");
			let timestamp = rw.ir().block(body).expect("the block exists").args[0];
			let (_, literal_values) = add(rw, InsertPoint::End(body), literal(7), &[], 1);
			let (_, free_values) = add(rw, InsertPoint::End(body), free_param("x"), &[], 1);
			let array = rw
				.create(
					InsertPoint::End(body),
					typed(OpKind::Expr(ExprOp::Array), &[literal_values[0]], vec![int_fact()]),
				)
				.expect("the op is created");
			let array_value = rw.ir().op(array).expect("the op exists").results[0];
			let (owner, _) = add(
				rw,
				InsertPoint::End(body),
				OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
				&[],
				1,
			);
			let (region, inner) =
				rw.create_region(owner, kind, &[int_fact()]).expect("the region is created");
			let argument = rw.ir().block(inner).expect("the block exists").args[0];
			let (_, own) = add(rw, InsertPoint::End(inner), literal(2), &[], 1);
			let (_, read) = add(
				rw,
				InsertPoint::End(inner),
				OpKind::Expr(ExprOp::Array),
				&[literal_values[0], free_values[0], array_value, argument, own[0], timestamp],
				1,
			);
			close_with(rw, inner, &[read[0]]);
			close_with(rw, body, &[]);
			(region, argument, literal_values[0], free_values[0], array_value, timestamp)
		});
	Unit {
		ir,
		region,
		argument,
		outside_literal,
		outside_free,
		outside_array,
		timestamp,
	}
}

#[test]
fn a_region_unit_takes_its_own_arguments_then_the_values_it_reads_from_outside() {
	let registry = text_registry();
	let f = unit_of(RegionKind::Subquery);
	let before = text_of(&f.ir, &registry);
	let unit = f.ir.copy_region(f.region).expect("the region is copied");
	assert_eq!(text_of(&f.ir, &registry), before, "a copy leaves the original as it was");
	assert_eq!(unit.ir.region(unit.ir.top()).map(|region| region.kind), Some(RegionKind::Subquery));
	assert_eq!(
		unit.inputs,
		vec![f.outside_free, f.outside_array, f.timestamp],
		"every outside value but a literal is an input, in the order the code reads them"
	);
	let args = unit.ir.block(top_block(&unit.ir)).expect("the block exists").args.clone();
	assert_eq!(args.len(), 4);
	assert_eq!(unit.map.original_value(args[0]), Some(f.argument));
	for (arg, input) in args[1..].iter().zip(&unit.inputs) {
		assert_eq!(unit.map.original_value(*arg), Some(*input));
		assert!(matches!(
			unit.ir.value(*arg).map(|value| value.def),
			Some(super::value::ValueDef::BlockArg { .. })
		));
	}
	assert!(logical_violation(&unit.ir, VerifyRule::UseList).is_none());
	assert!(logical_violation(&unit.ir, VerifyRule::Dominance).is_none());
}

#[test]
fn the_inputs_of_a_region_are_the_inputs_of_the_unit_it_is_copied_as() {
	let f = unit_of(RegionKind::Subquery);
	let unit = f.ir.copy_region(f.region).expect("the region is copied");
	assert_eq!(f.ir.region_inputs(f.region).expect("the inputs are read"), unit.inputs);
	assert!(matches!(f.ir.region_inputs(f.ir.top()), Err(IrError::Refused("unit region"))));
}

#[test]
fn a_region_unit_copies_an_outside_literal_and_never_a_computation() {
	let f = unit_of(RegionKind::Loop);
	let unit = f.ir.copy_region(f.region).expect("the region is copied");
	let copied = unit.map.map_value(f.outside_literal).expect("the literal is copied");
	let definer = match unit.ir.value(copied).map(|value| value.def) {
		Some(super::value::ValueDef::OpResult {
			op,
			..
		}) => op,
		other => panic!("the literal is defined by an op, not {other:?}"),
	};
	assert_eq!(literal_int(&unit.ir, definer), Some(7));
	assert!(unit.map.is_outside_copy(definer));
	assert!(!unit.inputs.contains(&f.outside_literal));
	let free = unit.map.map_value(f.outside_free).expect("the free parameter is mapped");
	assert!(matches!(
		unit.ir.value(free).map(|value| value.def),
		Some(super::value::ValueDef::BlockArg { .. })
	));
	assert_eq!(
		ops_in(&unit.ir, unit.ir.top())
			.filter(|op| matches!(
				unit.ir.op(*op).map(|data| &data.kind),
				Some(OpKind::Expr(ExprOp::FreeParam { .. }))
			))
			.count(),
		0,
		"the unit holds no op that computes a value the requester computed"
	);
}

#[test]
fn a_region_unit_reads_nothing_of_the_container_it_was_copied_from() {
	let f = unit_of(RegionKind::ClosureBody);
	let readers = f.ir.uses(f.outside_free).len();
	let unit = f.ir.copy_region(f.region).expect("the region is copied");
	assert_eq!(f.ir.uses(f.outside_free).len(), readers, "the original keeps its readers");
	let copied = unit.map.map_value(f.outside_free).expect("the value is mapped");
	assert_eq!(unit.ir.uses(copied).len(), 1);
	assert_eq!(unit.ir.phase(), f.ir.phase());
	assert_eq!(unit.ir.scope(), f.ir.scope());
	let held: Vec<OpId> = ops_in(&unit.ir, unit.ir.top()).collect();
	assert!(held.iter().all(|op| unit.map.original_op(*op).is_some()));
	for op in held {
		let original = unit.map.original_op(op).expect("the op has an original");
		assert!(f.ir.op(original).is_some());
	}
}

/// A request whose statement body defines a literal and an array of it, and holds a region that
/// reads the literal and, when `computed`, the array too.
fn reroot_fixture(computed: bool) -> (Ir, RegionId) {
	let mut ir = request();
	let top = top_block(&ir);
	let region = edit(&mut ir, |rw| {
		let (statement, _) = conform(rw, InsertPoint::End(top), stmt_kind());
		let (_, body) = rw
			.create_region(
				statement,
				RegionKind::Statement(StatementCommit::Auto),
				&[TypeKnowledge::Unknown],
			)
			.expect("the region is created");
		let (_, literal_values) = add(rw, InsertPoint::End(body), literal(7), &[], 1);
		let array = rw
			.create(
				InsertPoint::End(body),
				typed(OpKind::Expr(ExprOp::Array), &[literal_values[0]], vec![int_fact()]),
			)
			.expect("the op is created");
		let array_value = rw.ir().op(array).expect("the op exists").results[0];
		let (owner, _) = add(
			rw,
			InsertPoint::End(body),
			OpKind::Expr(ExprOp::Subquery(NestedStrategy::Apply)),
			&[],
			1,
		);
		let (region, inner) = rw
			.create_region(owner, RegionKind::Subquery, &[int_fact()])
			.expect("the region is created");
		let reads = if computed {
			vec![literal_values[0], array_value]
		} else {
			vec![literal_values[0]]
		};
		let (_, read) = add(rw, InsertPoint::End(inner), OpKind::Expr(ExprOp::Array), &reads, 1);
		close_with(rw, inner, &[read[0]]);
		close_with(rw, body, &[]);
		region
	});
	(ir, region)
}

#[test]
fn a_reroot_moves_the_outside_literal_a_region_reads_and_keeps_its_ops() {
	let (mut ir, region) = reroot_fixture(false);
	let inside: Vec<OpId> = ops_in(&ir, region).collect();
	let freed = ir.reroot(region).expect("the region stands beneath the top").expect("it is read");
	assert_eq!(freed.moved, 1, "the literal moves into the region");
	assert_eq!(ir.top(), region);
	let held: Vec<OpId> = ops_in(&ir, region).collect();
	assert_eq!(held.len(), inside.len() + 1);
	assert_eq!(literal_int(&ir, held[0]), Some(7), "the moved literal leads the block");
	assert_eq!(&held[1..], inside.as_slice(), "the ops inside keep their ids");
	assert!(logical_violation(&ir, VerifyRule::UseList).is_none());
	assert!(logical_violation(&ir, VerifyRule::Dominance).is_none());
}

#[test]
fn a_reroot_answers_outside_read_for_a_computed_value_and_changes_nothing() {
	let registry = text_registry();
	let (mut ir, region) = reroot_fixture(true);
	let before = text_of(&ir, &registry);
	assert!(matches!(ir.reroot(region), Ok(Err(OutsideRead))));
	assert_eq!(text_of(&ir, &registry), before);
}

#[test]
fn only_a_region_that_has_an_owner_is_a_unit() {
	let f = unit_of(RegionKind::Subquery);
	assert!(matches!(f.ir.copy_region(f.ir.top()), Err(IrError::Refused("unit region"))));
	assert!(matches!(f.ir.copy_region(RegionId::default()), Err(IrError::Stale("region"))));
}

/// A catalog container with one definition: an op that owns a region of `kind` holding a literal
/// and a yield, in a non-default frame and at a version snapshot over the region's argument.
/// Answers the container, the region, the op in it and the argument.
fn definition(kind: RegionKind) -> (Ir, RegionId, OpId, ValueId) {
	let mut ir = Ir::new(
		IrScope::CatalogVersion {
			version: CatalogVersionVector::new(),
		},
		RegionKind::Request,
	);
	let top = top_block(&ir);
	let (region, inside, argument) = edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (region, block) = rw
			.create_region(owner, kind, &[TypeKnowledge::Unknown])
			.expect("the region is created");
		let argument = rw.ir().block(block).expect("the block exists").args[0];
		let frame = rw
			.intern_frame(AuthorityFrame {
				eval: EvalFrame::computed(None),
				unit: UnitFrame::unprincipled(),
			})
			.expect("the frame is interned");
		let snapshot = rw.intern_snapshot(Snapshot::Version(argument)).expect("interned");
		let mut described = spec(literal(1), &[], 1);
		described.frame = frame;
		described.snapshot = snapshot;
		described.loc.origin = LocOrigin::Query;
		described.loc.span = Some(Span {
			start: 3,
			end: 5,
		});
		let inside = rw.create(InsertPoint::End(block), described).expect("the op is created");
		let value = rw.ir().op(inside).expect("the op exists").results[0];
		close_with(rw, block, &[value, argument]);
		(region, inside, argument)
	});
	(ir, region, inside, argument)
}

#[test]
fn clone_region_from_reinterns_frames() {
	let (src, region, inside, argument) = definition(RegionKind::FunctionBody);
	let mut dst = request();
	let top = top_block(&dst);
	let owner = edit(&mut dst, |rw| {
		rw.intern_frame(AuthorityFrame {
			eval: EvalFrame::permission(None),
			unit: UnitFrame::unprincipled(),
		})
		.expect("the frame is interned");
		add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0
	});
	let ops_before = dst.ops.len();
	let mut facts = FactCache::new();
	let mut rw = Rewriter::new(&mut dst, &mut facts);
	let (cloned, map) = rw.clone_region_from(&src, region, owner).expect("the region is cloned");
	let created = rw.take_created();
	drop(rw);
	assert_eq!(created.len(), 2);
	assert_eq!(dst.ops.len(), ops_before + 2);
	assert_eq!(dst.op(owner).expect("the owner exists").regions.to_vec(), vec![cloned]);
	assert_eq!(dst.parent_op(cloned), Some(owner));
	assert_eq!(map.original_region(cloned), Some(region));
	let copied = map.map_op(inside).expect("the op is cloned");
	assert_eq!(created[0], copied);
	let data = dst.op(copied).expect("the op exists");
	let original = src.op(inside).expect("the op exists");
	assert_eq!(dst.frame(data.frame), src.frame(original.frame));
	assert_ne!(data.frame, original.frame, "the frame has another index here");
	assert_eq!(data.frame, FrameId::new(2));
	assert_eq!(data.loc, original.loc);
	assert_eq!(data.provenance, Provenance::Bound);
	assert_eq!(dst.holders.get(&copied), Some(&1));
	let new_argument = map.map_value(argument).expect("the argument is cloned");
	assert_eq!(dst.snapshot(data.snapshot), Some(&Snapshot::Version(new_argument)));
	assert_eq!(dst.op_outside_tables(), None);
	let block = dst.region(cloned).expect("the region exists").blocks[0];
	assert_eq!(dst.block(block).expect("the block exists").args.to_vec(), vec![new_argument]);
	assert_eq!(dst.generation(copied), 1);
	assert!(dst.generation(owner) > 0);
	assert!(logical_violation(&dst, VerifyRule::UseList).is_none());
}

#[test]
fn clone_region_from_refuses_a_read_outside_the_region() {
	let (mut src, region, _, _) = definition(RegionKind::FunctionBody);
	let top = top_block(&src);
	let outside = edit(&mut src, |rw| add(rw, InsertPoint::End(top), literal(5), &[], 1).1[0]);
	let last = edit(&mut src, |rw| {
		let block = rw.ir().region(region).expect("the region exists").blocks[0];
		add(rw, InsertPoint::Before(last_op_of(rw.ir(), block)), literal(6), &[outside], 1).0
	});
	assert!(src.op(last).is_some());
	let mut dst = request();
	let top = top_block(&dst);
	let owner = edit(&mut dst, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0);
	let shape = (dst.ops.len(), dst.values.len(), dst.regions.len(), dst.blocks.len());
	let tables = (dst.frames.frames.len(), dst.snapshots.len());
	let failure = edit(&mut dst, |rw| rw.clone_region_from(&src, region, owner));
	assert!(matches!(failure, Err(IrError::Refused("clone value"))));
	assert_eq!((dst.ops.len(), dst.values.len(), dst.regions.len(), dst.blocks.len()), shape);
	// The first op interned a frame and a snapshot new to `dst` before the read outside failed.
	assert_eq!((dst.frames.frames.len(), dst.snapshots.len()), tables);
	assert!(dst.op(owner).expect("the owner exists").regions.is_empty());
	let gone = edit(&mut dst, |rw| add(rw, InsertPoint::End(top), literal(8), &[], 1).0);
	edit(&mut dst, |rw| rw.erase(gone, EliminationReason::DeadPure)).expect("the op is erased");
	let dangling = edit(&mut dst, |rw| rw.clone_region_from(&src, region, gone));
	assert!(matches!(dangling, Err(IrError::Stale("region owner"))));
}

#[test]
fn cloned_stored_clause_still_verifies() {
	let (src, region, _, _) = definition(RegionKind::StoredClause(StoredRole::Value));
	let (mut dst, _, inside) = stored();
	edit(&mut dst, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	let top = top_block(&dst);
	let owner = edit(&mut dst, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0);
	let (cloned, map) = edit(&mut dst, |rw| rw.clone_region_from(&src, region, owner))
		.expect("the region is cloned");
	assert!(logical_violation(&dst, VerifyRule::StoredClause).is_none());
	let sealed: Vec<OpId> = ops_in(&dst, cloned).collect();
	assert_eq!(sealed.len(), 2);
	for op in &sealed {
		assert_eq!(dst.sealed.get(*op), Some(&dst.generation(*op)));
	}
	assert!(map.original_op(sealed[0]).is_some());
	edit(&mut dst, |rw| rw.update_kind(sealed[0], |kind| *kind = literal(9)))
		.expect("kind updated");
	assert!(logical_violation(&dst, VerifyRule::StoredClause).is_some());

	let (block_source, block_region, _, _) = definition(RegionKind::Block);
	let (inner, _) = edit(&mut dst, |rw| rw.clone_region_from(&block_source, block_region, inside))
		.expect("the region is cloned");
	let nested: Vec<OpId> = ops_in(&dst, inner).collect();
	assert_eq!(nested.len(), 2);
	for op in nested {
		assert_eq!(dst.sealed.get(op), Some(&dst.generation(op)), "inside a stored clause");
	}
	assert_ne!(
		dst.sealed.get(inside),
		Some(&dst.generation(inside)),
		"the owner of the clone is edited, which a seal reports"
	);
}

#[test]
fn a_clone_before_the_phase_moves_is_sealed_when_it_does() {
	let (src, region, _, _) = definition(RegionKind::StoredClause(StoredRole::Value));
	let mut dst = request();
	let top = top_block(&dst);
	let owner = edit(&mut dst, |rw| add(rw, InsertPoint::End(top), stmt_kind(), &[], 0).0);
	let (cloned, _) = edit(&mut dst, |rw| rw.clone_region_from(&src, region, owner))
		.expect("the region is cloned");
	assert!(dst.sealed.is_empty(), "nothing is sealed while the container is bound");
	edit(&mut dst, |rw| rw.advance_phase(Phase::Logical)).expect("the phase advances");
	for op in ops_in(&dst, cloned) {
		assert_eq!(dst.sealed.get(op), Some(&dst.generation(op)));
	}
	assert!(logical_violation(&dst, VerifyRule::StoredClause).is_none());
}

/// The key of the default effect context of `ir`, which the rewriter's facts are filed under.
fn default_key(ir: &Ir) -> crate::exec::ir::iface::catalog::CxKey {
	crate::exec::ir::analysis::cx::default_cx(ir).memo_key()
}

#[test]
fn a_judged_container_whose_region_holds_a_construct_it_does_not_admit_fails_region_constraints() {
	use crate::exec::ir::iface::region_kind::ClauseRole;
	let mut ir = request();
	let top = top_block(&ir);
	edit(&mut ir, |rw| {
		let (owner, _) = add(rw, InsertPoint::End(top), stmt_kind(), &[], 0);
		let (_, block) = rw
			.create_region(owner, RegionKind::Clause(ClauseRole::Where), &[])
			.expect("the region is created");
		let mut written = spec(OpKind::Logical(LogOp::Sleep), &[], 0);
		written.loc.origin = LocOrigin::Query;
		rw.create(InsertPoint::End(block), written).expect("the op is created");
	});
	assert!(
		logical_violation(&ir, VerifyRule::RegionConstraints).is_none(),
		"a container the constraints do not judge is not checked"
	);
	ir.judge_constraints();
	assert!(logical_violation(&ir, VerifyRule::RegionConstraints).is_some());
}
