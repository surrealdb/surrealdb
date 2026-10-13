//! Tests of loop unswitching. The containers are built by hand, so each shape the pass accepts or
//! refuses is stated exactly: a `SELECT` loop over a literal whose `WHERE` filter holds one
//! conjunct, with the surroundings the test varies.

use super::driver::{LoCx, LoPass, NoObserver, PassConfig, PassReport};
use super::reg::lo_passes;
use super::tests_support::{Env, create, empty_ir, kind_of, literal, ops_of_tag};
use super::unswitch::{UNSWITCH_MAX_OPS, Unswitch};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::print::{PrintOpts, print};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, RegionKind, StatementCommit};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{LogOp, LogTag, LoopKind, LoopSink, SourceOrder};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::BinaryOperator;
use crate::val::Value;

const IF: OpTag = OpTag::Logical(LogTag::If);
const LOOP: OpTag = OpTag::Logical(LogTag::Loop);
const FILTER: OpTag = OpTag::Logical(LogTag::Filter);
const TRUTHY: OpTag = OpTag::Expr(ExprTag::Truthy);

/// What reads the value of the loop in the statement.
#[derive(Clone, Copy)]
enum Reader {
	/// The yield that closes the statement's block.
	Yield,
	/// An expression op, so the loop stands as a value.
	Value,
}

/// How a container is built.
struct Shape<'a> {
	kind: LoopKind,
	/// The conjunct of the `WHERE` clause, built from the row the clause receives.
	conjunct: &'a dyn Fn(&mut Rewriter<'_>, InsertPoint, ValueId) -> ValueId,
	/// When set, the clause yields a value defined in the statement before the loop, and
	/// `conjunct` is not used.
	outside: bool,
	/// Ops added to the loop's body before it closes.
	body: &'a dyn Fn(&mut Rewriter<'_>, BlockId),
	reader: Reader,
}

/// A container built from a [`Shape`].
struct Built {
	ir: Ir,
	/// The region of the statement.
	statement: RegionId,
}

fn yield_op() -> OpKind {
	OpKind::Expr(ExprOp::Yield)
}

fn no_body(_rw: &mut Rewriter<'_>, _body: BlockId) {}

/// `truthy(1)`: a value that cannot raise and is the same for every row.
fn constant_truth(rw: &mut Rewriter<'_>, at: InsertPoint) -> ValueId {
	let (_, one) = create(rw, at, literal(1), &[], 1);
	let (_, truth) = create(rw, at, OpKind::Expr(ExprOp::Truthy), &[one[0]], 1);
	truth[0]
}

/// A conjunct that cannot raise and is the same for every row.
fn invariant_conjunct(rw: &mut Rewriter<'_>, at: InsertPoint, _row: ValueId) -> ValueId {
	constant_truth(rw, at)
}

/// `truthy(row)`: a conjunct that varies with the row.
fn row_conjunct(rw: &mut Rewriter<'_>, at: InsertPoint, row: ValueId) -> ValueId {
	create(rw, at, OpKind::Expr(ExprOp::Truthy), &[row], 1).1[0]
}

/// `1 + 1`: a conjunct that is the same for every row and may raise.
fn fallible_conjunct(rw: &mut Rewriter<'_>, at: InsertPoint, _row: ValueId) -> ValueId {
	let (_, one) = create(rw, at, literal(1), &[], 1);
	let add = OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Add,
	});
	create(rw, at, add, &[one[0], one[0]], 1).1[0]
}

/// A body addition of `count` literals that nothing reads.
fn filler(count: usize) -> impl Fn(&mut Rewriter<'_>, BlockId) {
	move |rw, body| {
		for _ in 0..count {
			create(rw, InsertPoint::End(body), literal(0), &[], 1);
		}
	}
}

fn shape(kind: LoopKind) -> Shape<'static> {
	Shape {
		kind,
		conjunct: &invariant_conjunct,
		outside: false,
		body: &no_body,
		reader: Reader::Yield,
	}
}

fn build(shape: &Shape<'_>) -> Built {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let commit = StatementCommit::Auto;
	let (stmt, _) = create(
		&mut rw,
		InsertPoint::End(top),
		OpKind::Logical(LogOp::Stmt {
			commit,
			control: None,
		}),
		&[],
		0,
	);
	let (statement, block) = rw
		.create_region(stmt, RegionKind::Statement(commit), &[TypeKnowledge::Unknown])
		.expect("the statement region is created");
	let at = InsertPoint::End(block);
	let outer = shape.outside.then(|| constant_truth(&mut rw, at));
	let (_, source) = create(&mut rw, at, literal(1), &[], 1);
	let kind = OpKind::Logical(LogOp::Loop {
		kind: shape.kind,
		sink: LoopSink::Output,
		order: SourceOrder::Delivery,
		step: None,
	});
	let (loop_op, produced) = create(&mut rw, at, kind, &[source[0]], 1);
	let (_, body) = rw
		.create_region(loop_op, RegionKind::Loop, &[TypeKnowledge::Unknown, TypeKnowledge::Unknown])
		.expect("the body region is created");
	let row = rw.ir().block(body).expect("the body exists").args[0];
	let (filter, passed) =
		create(&mut rw, InsertPoint::End(body), OpKind::Logical(LogOp::Filter), &[row], 1);
	let (_, clause) = rw
		.create_region(filter, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])
		.expect("the clause region is created");
	let clause_row = rw.ir().block(clause).expect("the clause exists").args[0];
	let predicate = match outer {
		Some(value) => value,
		None => (shape.conjunct)(&mut rw, InsertPoint::End(clause), clause_row),
	};
	create(&mut rw, InsertPoint::End(clause), yield_op(), &[predicate], 0);
	(shape.body)(&mut rw, body);
	create(&mut rw, InsertPoint::End(body), OpKind::Logical(LogOp::Yield), &[passed[0]], 0);
	match shape.reader {
		Reader::Yield => {
			create(&mut rw, at, OpKind::Logical(LogOp::Yield), &[produced[0]], 0);
		}
		Reader::Value => {
			create(&mut rw, at, OpKind::Expr(ExprOp::Truthy), &[produced[0]], 1);
		}
	}
	Built {
		ir,
		statement,
	}
}

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

fn printed(ir: &Ir) -> String {
	print(ir, ir.top(), PrintOpts::default())
}

/// The loops that stand directly in `region`.
fn loops_in(ir: &Ir, region: RegionId) -> Vec<OpId> {
	ops_in(ir, region).filter(|op| kind_of(ir, *op).tag() == LOOP).collect()
}

/// The boolean the `WHERE` clause of `loop_op` yields when it yields a boolean literal.
fn where_literal(ir: &Ir, loop_op: OpId) -> Option<bool> {
	let mut found = None;
	for region in ir.op(loop_op)?.regions.iter().copied() {
		ir.walk(region, WalkOrder::PreOrder, &mut |op| {
			if kind_of(ir, op).tag() == FILTER {
				found = Some(op);
				return Walk::Stop;
			}
			Walk::Continue
		});
	}
	let clause = *ir.op(found?)?.regions.first()?;
	let block = *ir.region(clause)?.blocks.first()?;
	let last = *ir.block(block)?.ops.last()?;
	let predicate = *ir.op(last)?.operands.first()?;
	let ValueDef::OpResult {
		op,
		..
	} = ir.value(predicate)?.def
	else {
		return None;
	};
	match &ir.op(op)?.kind {
		OpKind::Expr(ExprOp::Literal(value)) => match **value {
			Value::Bool(truth) => Some(truth),
			_ => None,
		},
		_ => None,
	}
}

/// The one `logical.if` of `ir`.
fn the_branch(ir: &Ir) -> OpId {
	let branches = ops_of_tag(ir, IF);
	let [branch] = branches.as_slice() else {
		panic!("one branch, found {branches:?}");
	};
	*branch
}

/// A `SELECT` loop with an invariant conjunct that cannot raise becomes an `if` on a copy of the
/// conjunct, each arm holding the source and a copy of the loop whose conjunct is the arm's
/// literal.
#[tokio::test]
async fn unswitch_copies_select_loop_under_if() {
	let mut built = build(&shape(LoopKind::Select));
	let report = run(&Unswitch, &mut built.ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 1);

	let ir = &built.ir;
	let branch = the_branch(ir);
	let data = ir.op(branch).expect("the branch exists");
	assert_eq!(data.regions.len(), 2);
	assert!(loops_in(ir, built.statement).is_empty(), "the original loop is gone");

	let condition = data.operands[0];
	let ValueDef::OpResult {
		op: definer,
		..
	} = ir.value(condition).expect("the condition exists").def
	else {
		panic!("the condition is an op result");
	};
	assert_eq!(kind_of(ir, definer).tag(), TRUTHY);
	assert_eq!(
		ir.op(definer).expect("the definer exists").parent,
		ir.op(branch).expect("the branch exists").parent,
		"the condition is computed where the loop stood"
	);

	for (region, truth) in data.regions.iter().zip([true, false]) {
		assert_eq!(ir.region(*region).expect("the arm exists").kind, RegionKind::StatementIf);
		let loops = loops_in(ir, *region);
		let [copy] = loops.as_slice() else {
			panic!("one loop in the arm, found {loops:?}");
		};
		assert_eq!(where_literal(ir, *copy), Some(truth));
		assert_eq!(ops_in(ir, *region).count(), 3, "the source, the loop and the yield");
	}
	assert_eq!(ops_of_tag(ir, LOOP).len(), 2);

	let again = run(&Unswitch, &mut built.ir, &PassConfig::ALL_ON).await;
	assert_eq!(again.rewrites, 0, "an arm's loop has no conjunct left to branch on");
}

/// A loop that stands as a value gets value arms.
#[tokio::test]
async fn unswitch_gives_a_value_loop_value_arms() {
	let mut built = build(&Shape {
		reader: Reader::Value,
		..shape(LoopKind::Select)
	});
	let report = run(&Unswitch, &mut built.ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 1);
	let ir = &built.ir;
	let data = ir.op(the_branch(ir)).expect("the branch exists");
	for region in &data.regions {
		assert_eq!(ir.region(*region).expect("the arm exists").kind, RegionKind::ValueIf);
	}
}

/// A conjunct that varies with the row, a conjunct that may raise and the subject loop of a write
/// are each left as they are.
#[tokio::test]
async fn unswitch_refuses_fallible_and_write_loops() {
	let shapes = [
		(
			"row-dependent",
			Shape {
				conjunct: &row_conjunct,
				..shape(LoopKind::Select)
			},
		),
		(
			"fallible",
			Shape {
				conjunct: &fallible_conjunct,
				..shape(LoopKind::Select)
			},
		),
		("write", shape(LoopKind::Write)),
		("for", shape(LoopKind::For)),
	];
	for (name, held) in &shapes {
		let mut built = build(held);
		let before = printed(&built.ir);
		let report = run(&Unswitch, &mut built.ir, &PassConfig::ALL_ON).await;
		assert_eq!(report.rewrites, 0, "{name}");
		assert_eq!(printed(&built.ir), before, "{name}");
		assert!(ops_of_tag(&built.ir, IF).is_empty(), "{name}");
	}
}

/// A condition defined before the loop is read by the loop's regions, which cannot be cloned, so
/// the loop is left as it is.
#[tokio::test]
async fn unswitch_refuses_a_condition_defined_outside_the_loop() {
	let mut built = build(&Shape {
		outside: true,
		..shape(LoopKind::Select)
	});
	let before = printed(&built.ir);
	let report = run(&Unswitch, &mut built.ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(printed(&built.ir), before);
}

/// A `RETURN` inside the loop that lands outside it keeps the loop whole.
#[tokio::test]
async fn unswitch_refuses_a_loop_that_returns_out_of_itself() {
	let leaves = |rw: &mut Rewriter<'_>, body: BlockId| {
		let (_, value) = create(rw, InsertPoint::End(body), literal(1), &[], 1);
		create(rw, InsertPoint::End(body), OpKind::Logical(LogOp::Return), &[value[0]], 0);
	};
	let mut built = build(&Shape {
		body: &leaves,
		..shape(LoopKind::Select)
	});
	let before = printed(&built.ir);
	let report = run(&Unswitch, &mut built.ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(printed(&built.ir), before);
}

/// A loop whose copy would hold more than the bound is left as it is, and the same loop under the
/// bound is unswitched.
#[tokio::test]
async fn unswitch_respects_the_op_bound() {
	let small = filler(8);
	let mut under = build(&Shape {
		body: &small,
		..shape(LoopKind::Select)
	});
	assert_eq!(run(&Unswitch, &mut under.ir, &PassConfig::ALL_ON).await.rewrites, 1);

	let large = filler(UNSWITCH_MAX_OPS);
	let mut over = build(&Shape {
		body: &large,
		..shape(LoopKind::Select)
	});
	let before = printed(&over.ir);
	assert_eq!(run(&Unswitch, &mut over.ir, &PassConfig::ALL_ON).await.rewrites, 0);
	assert_eq!(printed(&over.ir), before);
}

/// The pass follows its own switch, which only the full configuration turns on.
#[test]
fn the_pass_follows_its_switch() {
	assert!(Unswitch.enabled(&PassConfig::ALL_ON));
	assert!(!Unswitch.enabled(&PassConfig::RELEASE));
	assert!(!Unswitch.enabled(&PassConfig::ALL_OFF));
}

/// The pass runs before the passes that place memos, so no memo is ever copied into an arm.
#[test]
fn unswitch_is_registered_before_the_memo_passes() {
	let names: Vec<&'static str> = lo_passes().iter().map(|pass| pass.name()).collect();
	let position = |wanted: &str| {
		names.iter().position(|name| *name == wanted).expect("the pass is registered")
	};
	assert!(position("unswitch") < position("hoist_leading_conjuncts"));
	assert!(position("hoist_leading_conjuncts") < position("licm"));
}
