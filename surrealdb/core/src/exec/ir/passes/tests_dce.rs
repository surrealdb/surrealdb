//! Tests of dead-code elimination: what each reason removes, and what is never removed.

use surrealdb_strand::{Strand, TableName};

use super::dce::Dce;
use super::driver::{LoCx, LoPass, NoObserver, PassConfig, PassReport};
use super::tests_support::{Env, bind, create, empty_ir, literal, ops_of_tag, verify_logical};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::expr::ops::{ComputedReadAttrs, ExprOp, ExprTag, NowWhich};
use crate::exec::ir::iface::CatalogObjectKey;
use crate::exec::ir::iface::conversion::EliminationReason;
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::iface::ids::BlockId;
use crate::exec::ir::iface::region_kind::{FieldRole, RegionKind};
use crate::exec::ir::logical::ops::{FieldsShape, LogOp, LogTag, ProjectAttrs};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::logical::footprint::CatalogKind;
use crate::exec::vm::compile_cache::ObjectVersion;
use crate::expr::BinaryOperator;
use crate::val::Value;

const LITERAL: OpTag = OpTag::Expr(ExprTag::Literal);
const BINARY: OpTag = OpTag::Expr(ExprTag::Binary);
const COMPUTED_READ: OpTag = OpTag::Expr(ExprTag::ComputedRead);

/// Runs the pass over `ir` under `cfg`, without the driver's verification, so a container built
/// by hand is judged by the pass alone.
async fn dce(ir: &mut Ir, cfg: &PassConfig) -> PassReport {
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
	Dce.run(&mut cx).await.expect("the pass runs")
}

fn top_block(ir: &Ir) -> BlockId {
	ir.region(ir.top()).expect("the top region exists").blocks[0]
}

fn count(ir: &Ir, tag: OpTag) -> usize {
	ops_of_tag(ir, tag).len()
}

/// The reasons of every erase the container recorded.
fn reasons(ir: &Ir) -> Vec<EliminationReason> {
	ir.eliminated().iter().map(|(_, reason)| *reason).collect()
}

fn add() -> OpKind {
	OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Add,
	})
}

/// A read of the computed field `a` of table `t`.
fn computed_read() -> OpKind {
	OpKind::Expr(ExprOp::ComputedRead(Box::new(ComputedReadAttrs {
		field: Strand::new("a"),
		def: (
			CatalogObjectKey {
				kind: CatalogKind::Field,
				ns: None,
				db: None,
				table: Some(TableName::new(Strand::new("t"))),
				name: Some(Strand::new("a")),
			},
			ObjectVersion::Inline,
		),
	})))
}

/// Unread infallible reads go, with what only they read; an unread op that may fail stays, and so
/// do the operands it reads.
#[tokio::test]
async fn dead_pure_removes_infallible_reads() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = top_block(&ir);
	let at = InsertPoint::End(block);
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let now = OpKind::Expr(ExprOp::Now {
		which: NowWhich::Datetime,
	});
	create(&mut rw, at, now, &[], 1);
	let (_, one) = create(&mut rw, at, literal(1), &[], 1);
	create(&mut rw, at, OpKind::Expr(ExprOp::Truthy), &[one[0]], 1);
	let (_, two) = create(&mut rw, at, literal(2), &[], 1);
	create(&mut rw, at, add(), &[two[0], two[0]], 1);

	let report = dce(&mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 3);
	assert_eq!(count(&ir, OpTag::Expr(ExprTag::Now)), 0);
	assert_eq!(count(&ir, OpTag::Expr(ExprTag::Truthy)), 0);
	assert_eq!(count(&ir, LITERAL), 1);
	assert_eq!(count(&ir, BINARY), 1);
	assert_eq!(reasons(&ir), [EliminationReason::DeadPure; 3]);
}

/// A computed read nothing shadows is kept: its definition may draw, write or raise, and the
/// derivation happens at the read.
#[tokio::test]
async fn computed_read_that_draws_is_kept() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = top_block(&ir);
	let at = InsertPoint::End(block);
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	create(&mut rw, at, computed_read(), &[row[0]], 1);

	let report = dce(&mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(count(&ir, COMPUTED_READ), 1);
}

/// A second read of the same field of the same row, after one that stays, is answered by the
/// memo the first filled, so it goes when unread.
#[tokio::test]
async fn computed_read_shadowed_by_an_earlier_read_is_removed() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = top_block(&ir);
	let at = InsertPoint::End(block);
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (_, first) = create(&mut rw, at, computed_read(), &[row[0]], 1);
	create(&mut rw, at, add(), &[first[0], row[0]], 1);
	let (later, _) = create(&mut rw, at, computed_read(), &[row[0]], 1);

	dce(&mut ir, &PassConfig::RELEASE).await;
	assert_eq!(count(&ir, COMPUTED_READ), 1);
	assert!(ir.op(later).is_none());
	assert_eq!(count(&ir, BINARY), 1);
}

/// An op after a terminator never runs and goes with its operands.
#[tokio::test]
async fn unreachable_after_raise() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = top_block(&ir);
	let at = InsertPoint::End(block);
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, message) = create(&mut rw, at, literal(1), &[], 1);
	create(&mut rw, at, OpKind::Expr(ExprOp::Throw), &[message[0]], 0);
	let (_, two) = create(&mut rw, at, literal(2), &[], 1);
	create(&mut rw, at, add(), &[two[0], two[0]], 1);

	dce(&mut ir, &PassConfig::RELEASE).await;
	assert_eq!(count(&ir, OpTag::Expr(ExprTag::Throw)), 1);
	assert_eq!(count(&ir, LITERAL), 1);
	assert_eq!(count(&ir, BINARY), 0);
	assert_eq!(reasons(&ir), [EliminationReason::Unreachable; 2]);
}

/// A region whose `RETURN` lands outside it is not dead, though every op in it is pure: a
/// statement `IF` passes a `RETURN` outward, where a block would catch it.
#[tokio::test]
async fn control_escaping_region_is_kept() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = top_block(&ir);
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (_, condition) = create(&mut rw, InsertPoint::End(block), literal(1), &[], 1);
	let (owner, _) =
		create(&mut rw, InsertPoint::End(block), OpKind::Logical(LogOp::If), &[condition[0]], 1);
	let (_, inner) =
		rw.create_region(owner, RegionKind::StatementIf, &[]).expect("the region is created");
	let (_, held) = create(&mut rw, InsertPoint::End(inner), literal(1), &[], 1);
	create(&mut rw, InsertPoint::End(inner), OpKind::Logical(LogOp::Return), &[held[0]], 0);

	let report = dce(&mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 0);
	assert!(ir.op(owner).is_some());
}

/// A statement's region stays even when everything in it is pure.
#[tokio::test]
async fn statements_are_never_removed() {
	let mut ir = bind("RETURN 1; RETURN 2").await;
	let statements = count(&ir, OpTag::Logical(LogTag::Stmt));
	assert_eq!(statements, 2);
	dce(&mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(count(&ir, OpTag::Logical(LogTag::Stmt)), statements);
	assert_eq!(count(&ir, OpTag::Logical(LogTag::Return)), 2);
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
}

/// A value-position `IF`, block or `FOR` that writes stays when the `LET` that read it goes, and
/// its regions keep their value kind: the container still verifies.
#[tokio::test]
async fn a_written_value_whose_reader_goes_keeps_its_value_regions() {
	for sql in [
		"{ LET $x = IF true { CREATE t:1; 1 } ELSE { 2 }; RETURN 5 };",
		"{ LET $x = { CREATE t:1; 3 }; RETURN 5 };",
		"{ LET $x = FOR $i IN [1] { CREATE t:1; }; RETURN 5 };",
	] {
		let mut ir = bind(sql).await;
		dce(&mut ir, &PassConfig::ALL_ON).await;
		assert_eq!(count(&ir, OpTag::Logical(LogTag::Let)), 0, "{sql}");
		let env = Env::new();
		verify_logical(&ir, &mut FactCache::new(), &env).expect(sql);
	}
}

/// In an explicit transaction, an earlier session `LET` that a certain raise cancels is removed
/// under `PassConfig::ALL_ON`. Under `PassConfig::RELEASE` the pass keeps it.
#[tokio::test]
async fn certain_error_removes_an_undone_session_let() {
	let sql = "BEGIN; LET $x = 1; THROW 'nope'; COMMIT;";
	let mut ir = bind(sql).await;
	assert_eq!(count(&ir, OpTag::Logical(LogTag::Let)), 1);
	dce(&mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(count(&ir, OpTag::Logical(LogTag::Let)), 0);
	assert_eq!(count(&ir, OpTag::Expr(ExprTag::Throw)), 1);
	assert!(reasons(&ir).contains(&EliminationReason::DeadBeforeCertainError));
	let env = Env::new();
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let mut ir = bind(sql).await;
	dce(&mut ir, &PassConfig::RELEASE).await;
	assert_eq!(count(&ir, OpTag::Logical(LogTag::Let)), 1);
	assert!(!reasons(&ir).contains(&EliminationReason::DeadBeforeCertainError));
}

/// An op that may fail is never removed ahead of a raise: the first error wins.
#[tokio::test]
async fn certain_error_keeps_fallible_predecessors() {
	let mut ir = bind("BEGIN; LET $x = <int>'z'; THROW 'nope'; COMMIT;").await;
	assert_eq!(count(&ir, OpTag::Expr(ExprTag::Convert)), 1);
	dce(&mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(count(&ir, OpTag::Expr(ExprTag::Convert)), 1);
	assert_eq!(count(&ir, OpTag::Expr(ExprTag::Throw)), 1);
}

/// A write is never removed, whatever the raise after it cancels.
#[tokio::test]
async fn writes_are_never_removed() {
	let mut ir = bind("BEGIN; CREATE a:1; THROW 'nope'; COMMIT;").await;
	let writes = count(&ir, OpTag::Logical(LogTag::Write));
	assert_eq!(writes, 1);
	dce(&mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(count(&ir, OpTag::Logical(LogTag::Write)), writes);
}

/// A computed read whose permission guard yields the literal `false` is removed. An op that reads
/// its result sees `NONE`.
#[tokio::test]
async fn invisible_reads_see_none() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = top_block(&ir);
	let at = InsertPoint::End(block);
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
	let project = OpKind::Logical(LogOp::Project(Box::new(attrs)));
	let (owner, out) = create(&mut rw, at, project, &[row[0]], 1);
	create(&mut rw, at, add(), &[out[0], out[0]], 1);

	let (_, guard) =
		rw.create_region(owner, RegionKind::PermissionFrame, &[]).expect("the guard is created");
	let denied = OpKind::Expr(ExprOp::Literal(Box::new(Value::Bool(false))));
	let (_, verdict) = create(&mut rw, InsertPoint::End(guard), denied, &[], 1);
	create(&mut rw, InsertPoint::End(guard), OpKind::Expr(ExprOp::Yield), &[verdict[0]], 0);

	let (_, frame) = rw
		.create_region(owner, RegionKind::FieldFrame(FieldRole::Computed), &[])
		.expect("the field frame is created");
	let (_, derived) = create(&mut rw, InsertPoint::End(frame), computed_read(), &[row[0]], 1);
	let (close, _) =
		create(&mut rw, InsertPoint::End(frame), OpKind::Expr(ExprOp::Yield), &[derived[0]], 0);

	dce(&mut ir, &PassConfig::RELEASE).await;
	assert_eq!(count(&ir, COMPUTED_READ), 0);
	assert!(reasons(&ir).contains(&EliminationReason::InvisibleToPrincipal));
	let operand = ir.op(close).expect("the yield stays").operands[0];
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(operand).map(|data| data.def)
	else {
		panic!("the yield reads an op result");
	};
	let kind = &ir.op(op).expect("the definition stays").kind;
	assert!(
		matches!(kind, OpKind::Expr(ExprOp::Literal(held)) if matches!(**held, Value::None)),
		"{kind:?}"
	);
}
