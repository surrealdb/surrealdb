//! Tests of the permission and `WHERE` passes. The containers are built by hand: the binder
//! emits no field-guard region and no permission over a catalog that holds none.

use smallvec::SmallVec;
use surrealdb_strand::Strand;

use super::driver::{LoCx, LoPass, NoObserver, PassConfig, PassReport};
use super::perm_where::{PushBelowLayer, PushBelowPermission, SpecialiseGuards};
use super::tests_support::{Env, create, empty_ir, literal, ops_of_tag};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::rewriter::{InsertPoint, Provenance, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::iface::ids::{BlockId, OpId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, LogOp, LogTag, ProjectAttrs};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::BinaryOperator;
use crate::expr::logical::{GuardKind, OutputPath};
use crate::val::Value;

const PERMISSION: OpTag = OpTag::Logical(LogTag::PermissionFilter);
const FILTER: OpTag = OpTag::Logical(LogTag::Filter);
const FIELD: OpTag = OpTag::Expr(ExprTag::Field);

/// The arguments of an isolated region: the statement timestamp and the row.
const ISOLATED_ARGS: [TypeKnowledge; 2] = [TypeKnowledge::Unknown, TypeKnowledge::Unknown];

/// Runs `pass` over `ir` under `cfg`, without the driver's verification, so a container built by
/// hand is judged by the pass alone.
async fn run(pass: &dyn LoPass, ir: &mut Ir, cfg: &PassConfig) -> PassReport {
	let env = Env::new();
	let effects = env.cx();
	let mut facts = FactCache::new();
	let mut observer = NoObserver;
	let mut cx = LoCx {
		ir,
		facts: &mut facts,
		folder: &NotYetFolder,
		effects: &effects,
		cfg,
		observer: &mut observer,
	};
	pass.run(&mut cx).await.expect("the pass runs")
}

fn top_block(ir: &Ir) -> BlockId {
	ir.region(ir.top()).expect("the top region exists").blocks[0]
}

fn reasons(ir: &Ir) -> Vec<EliminationReason> {
	ir.eliminated().iter().map(|(_, reason)| *reason).collect()
}

fn boolean(flag: bool) -> OpKind {
	OpKind::Expr(ExprOp::Literal(Box::new(Value::Bool(flag))))
}

fn add() -> OpKind {
	OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Add,
	})
}

fn field(name: &str) -> OpKind {
	OpKind::Expr(ExprOp::Field {
		name: Strand::new(name),
	})
}

fn yield_op() -> OpKind {
	OpKind::Expr(ExprOp::Yield)
}

/// The argument `index` of the block `block`.
fn arg(ir: &Ir, block: BlockId, index: usize) -> ValueId {
	ir.block(block).expect("the block exists").args[index]
}

/// The op that defines `value`.
fn definer(ir: &Ir, value: ValueId) -> OpId {
	match ir.value(value).expect("the value exists").def {
		ValueDef::OpResult {
			op,
			..
		} => op,
		ValueDef::BlockArg {
			..
		} => panic!("the value is a block argument"),
	}
}

/// A permission filter over `row` whose predicate yields what `verdict` builds. Returns the filter
/// op and its result value.
fn permission(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	row: ValueId,
	verdict: &dyn Fn(&mut Rewriter<'_>, InsertPoint) -> ValueId,
) -> (OpId, ValueId) {
	let kind = OpKind::Logical(LogOp::PermissionFilter {
		guard: GuardKind::Read,
		computed: None,
	});
	let (op, out) = create(rw, at, kind, &[row], 1);
	let (_, body) = rw
		.create_region(op, RegionKind::PermissionFrame, &ISOLATED_ARGS)
		.expect("the predicate region is created");
	let value = verdict(rw, InsertPoint::End(body));
	create(rw, InsertPoint::End(body), yield_op(), &[value], 0);
	(op, out[0])
}

/// A `WHERE` filter over `input` whose clause yields what `conjunct` builds from the row.
fn where_filter(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	input: ValueId,
	conjunct: &dyn Fn(&mut Rewriter<'_>, InsertPoint, ValueId) -> ValueId,
) -> (OpId, ValueId) {
	let (op, out) = create(rw, at, OpKind::Logical(LogOp::Filter), &[input], 1);
	let (_, body) = rw
		.create_region(op, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])
		.expect("the clause region is created");
	let row = rw.ir().block(body).expect("the block exists").args[0];
	let value = conjunct(rw, InsertPoint::End(body), row);
	create(rw, InsertPoint::End(body), yield_op(), &[value], 0);
	(op, out[0])
}

/// `truthy(1)`: a conjunct that is infallible and reads nothing.
fn pure_conjunct(rw: &mut Rewriter<'_>, at: InsertPoint, _row: ValueId) -> ValueId {
	let (_, one) = create(rw, at, literal(1), &[], 1);
	let (_, truth) = create(rw, at, OpKind::Expr(ExprOp::Truthy), &[one[0]], 1);
	truth[0]
}

/// A literal `true` verdict.
fn admit(rw: &mut Rewriter<'_>, at: InsertPoint) -> ValueId {
	create(rw, at, boolean(true), &[], 1).1[0]
}

/// A literal `false` verdict.
fn deny(rw: &mut Rewriter<'_>, at: InsertPoint) -> ValueId {
	create(rw, at, boolean(false), &[], 1).1[0]
}

/// `1 + 1`: a verdict that may fail.
fn fallible(rw: &mut Rewriter<'_>, at: InsertPoint) -> ValueId {
	let (_, one) = create(rw, at, literal(1), &[], 1);
	create(rw, at, add(), &[one[0], one[0]], 1).1[0]
}

/// A permission filter whose predicate yields the literal `true` goes, and its reader reads its
/// input.
#[tokio::test]
async fn literal_true_table_permission_is_erased() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (_, out) = permission(&mut rw, at, row[0], &admit);
	let (reader, _) = create(&mut rw, at, add(), &[out, out], 1);

	let report = run(&SpecialiseGuards, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 1);
	assert!(ops_of_tag(&ir, PERMISSION).is_empty());
	assert_eq!(ir.op(reader).expect("the reader stays").operands.as_slice(), [row[0], row[0]]);
	let erased = reasons(&ir);
	assert!(!erased.is_empty());
	assert!(erased.iter().all(|reason| *reason == EliminationReason::FoldedConstant));
}

/// A permission filter whose predicate yields the literal `false` stays: its rows are all hidden,
/// and the access path plans that.
#[tokio::test]
async fn literal_false_table_permission_is_kept() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (_, out) = permission(&mut rw, at, row[0], &deny);
	create(&mut rw, at, add(), &[out, out], 1);

	let report = run(&SpecialiseGuards, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(ops_of_tag(&ir, PERMISSION).len(), 1);
}

/// A conjunct that is infallible and reads nothing moves below a total permission predicate, and
/// the filter it leaves is replaced by its input.
#[tokio::test]
async fn push_below_permission_moves_a_pure_prefix() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (permit, hidden) = permission(&mut rw, at, row[0], &admit);
	let (_, kept) = where_filter(&mut rw, at, hidden, &pure_conjunct);
	let (reader, _) = create(&mut rw, at, add(), &[kept, kept], 1);

	let report = run(&PushBelowPermission, &mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 1);
	let filters = ops_of_tag(&ir, FILTER);
	assert_eq!(filters.len(), 1, "the emptied filter is replaced by its input");
	let lower = filters[0];
	assert_eq!(ir.op(lower).expect("the filter exists").operands.as_slice(), [row[0]]);
	let lower_out = ir.op(lower).expect("the filter exists").results[0];
	assert_eq!(ir.op(permit).expect("the permission stays").operands.as_slice(), [lower_out]);
	let permit_out = ir.op(permit).expect("the permission stays").results[0];
	assert_eq!(ir.op(reader).expect("the reader stays").operands.as_slice(), [permit_out; 2]);
}

/// A permission filter that derives a computed field lists the predicate first and the field's
/// body after it, and a conjunct still moves below it when every region is total.
#[tokio::test]
async fn push_below_permission_reads_the_predicate_of_a_filter_that_derives() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (permit, hidden) = permission(&mut rw, at, row[0], &admit);
	let (_, derived) = rw
		.create_region(permit, RegionKind::FieldFrame(FieldRole::Computed), &ISOLATED_ARGS)
		.expect("the field region is created");
	let value = pure_conjunct(&mut rw, InsertPoint::End(derived), row[0]);
	create(&mut rw, InsertPoint::End(derived), yield_op(), &[value], 0);
	where_filter(&mut rw, at, hidden, &pure_conjunct);

	let report = run(&PushBelowPermission, &mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 1);
	assert_eq!(ops_of_tag(&ir, FILTER).len(), 1, "the emptied filter is replaced by its input");
}

/// A permission predicate that may fail is never skipped for the rows a moved conjunct rejects.
#[tokio::test]
async fn push_below_permission_refused_when_permission_not_total() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (_, hidden) = permission(&mut rw, at, row[0], &fallible);
	where_filter(&mut rw, at, hidden, &pure_conjunct);

	let report = run(&PushBelowPermission, &mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(ops_of_tag(&ir, FILTER).len(), 1);
}

/// A conjunct that may fail stays above a total permission predicate.
#[tokio::test]
async fn push_below_permission_refused_for_a_fallible_conjunct() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (_, hidden) = permission(&mut rw, at, row[0], &admit);
	let fallible_conjunct =
		|rw: &mut Rewriter<'_>, at: InsertPoint, _row: ValueId| fallible(rw, at);
	where_filter(&mut rw, at, hidden, &fallible_conjunct);

	let report = run(&PushBelowPermission, &mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 0);
}

/// Each pass is switched by its own flag of the configuration.
#[tokio::test]
async fn the_passes_follow_their_switches() {
	let cfg = PassConfig::RELEASE;
	assert!(SpecialiseGuards.enabled(&cfg));
	assert!(PushBelowLayer.enabled(&cfg));
	assert!(!PushBelowPermission.enabled(&cfg));
	assert!(PushBelowLayer.enabled(&PassConfig::ALL_ON));
	assert!(PushBelowPermission.enabled(&PassConfig::ALL_ON));
	assert!(!SpecialiseGuards.enabled(&PassConfig::ALL_OFF));
}

/// A layer over `row` with one guarded column `a`, whose guard yields `verdict` and whose field
/// frame yields a read of the stored field, and a filter above it that reads `a`. Returns the
/// filter op, the op that yields its clause result and the stored row value.
fn guarded_layer(ir: &mut Ir, facts: &mut FactCache, verdict: bool) -> (OpId, OpId, ValueId) {
	let at = InsertPoint::End(top_block(ir));
	let mut rw = Rewriter::new(ir, facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let attrs = ProjectAttrs {
		fields: FieldsShape::Select(Box::new([FieldShape::Single {
			alias: Some(OutputPath::field("a")),
			kind: None,
		}])),
		layer: true,
		omit: 0,
		names: Vec::new(),
		computed: Vec::new(),
		sort_keys: Vec::new(),
		judged: None,
	};
	let (layer, delivered) =
		create(&mut rw, at, OpKind::Logical(LogOp::Project(Box::new(attrs))), &[row[0]], 1);
	let (_, guard) = rw
		.create_region(layer, RegionKind::PermissionFrame, &ISOLATED_ARGS)
		.expect("the guard region is created");
	let literal_verdict = create(&mut rw, InsertPoint::End(guard), boolean(verdict), &[], 1).1[0];
	create(&mut rw, InsertPoint::End(guard), yield_op(), &[literal_verdict], 0);
	let (_, frame) = rw
		.create_region(layer, RegionKind::FieldFrame(FieldRole::Computed), &ISOLATED_ARGS)
		.expect("the field frame is created");
	let frame_row = rw.ir().block(frame).expect("the block exists").args[1];
	let stored = create(&mut rw, InsertPoint::End(frame), field("a"), &[frame_row], 1).1[0];
	create(&mut rw, InsertPoint::End(frame), yield_op(), &[stored], 0);
	let (filter, _) = where_filter(&mut rw, at, delivered[0], &|rw, at, masked| {
		create(rw, at, field("a"), &[masked], 1).1[0]
	});
	let clause = rw.ir().op(filter).expect("the filter exists").regions[0];
	let block = rw.ir().region(clause).expect("the region exists").blocks[0];
	let yielded = *rw.ir().block(block).expect("the block exists").ops.last().expect("a yield");
	(filter, yielded, row[0])
}

/// A guard that yields `false` makes every read of its masked field read `NONE`. The `NONE` stands
/// for the read it replaces: its provenance holds the read, which is therefore not recorded as
/// eliminated.
#[tokio::test]
async fn a_false_field_guard_hides_its_reads() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let (_, yielded, _) = guarded_layer(&mut ir, &mut facts, false);
	// The read of the stored field in the field frame stays; only the filter's read is masked.
	let reads = ops_of_tag(&ir, FIELD);
	assert_eq!(reads.len(), 2);

	let report = run(&SpecialiseGuards, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 1);
	let kept = ops_of_tag(&ir, FIELD);
	let [hidden] =
		reads.iter().copied().filter(|read| !kept.contains(read)).collect::<Vec<_>>()[..]
	else {
		panic!("one read is hidden: {reads:?} became {kept:?}");
	};
	let value = ir.op(yielded).expect("the yield stays").operands[0];
	let none = ir.op(definer(&ir, value)).expect("the definer stays");
	assert!(
		matches!(&none.kind, OpKind::Expr(ExprOp::Literal(held)) if matches!(**held, Value::None)),
		"{:?}",
		none.kind
	);
	assert!(
		matches!(&none.provenance, Provenance::From(held) if held.contains(&hidden)),
		"{:?}",
		none.provenance
	);
	assert!(ir.eliminated().iter().all(|(op, _)| *op != hidden));
}

/// A guard that yields `true` over a field frame that only reads the stored field points the
/// masked reads at the stored row.
#[tokio::test]
async fn a_true_field_guard_reads_the_stored_row() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let (_, yielded, stored) = guarded_layer(&mut ir, &mut facts, true);

	let report = run(&SpecialiseGuards, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 1);
	let value = ir.op(yielded).expect("the yield stays").operands[0];
	let read = ir.op(definer(&ir, value)).expect("the read stays");
	assert_eq!(read.operands.as_slice(), [stored]);
}

/// A container with no guard region matches nothing.
#[tokio::test]
async fn specialise_guards_matches_nothing_without_guards() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let attrs = ProjectAttrs {
		fields: FieldsShape::Value {
			alias: None,
		},
		layer: true,
		omit: 0,
		names: Vec::new(),
		computed: Vec::new(),
		sort_keys: Vec::new(),
		judged: None,
	};
	create(&mut rw, at, OpKind::Logical(LogOp::Project(Box::new(attrs))), &[row[0]], 1);
	let before = SmallVec::<[usize; 4]>::from_slice(&[
		ops_of_tag(&ir, FILTER).len(),
		ops_of_tag(&ir, PERMISSION).len(),
	]);

	let report = run(&SpecialiseGuards, &mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 0);
	let after = SmallVec::<[usize; 4]>::from_slice(&[
		ops_of_tag(&ir, FILTER).len(),
		ops_of_tag(&ir, PERMISSION).len(),
	]);
	assert_eq!(before, after);
}
