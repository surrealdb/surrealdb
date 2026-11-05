//! Tests of loop unrolling. The containers are built by hand, so each shape the pass accepts or
//! refuses is stated exactly: one loop in a statement, over a literal or a computed source, with
//! the body and the sink the test varies.

use super::driver::{FoldObserver, LoCx, LoPass, NoObserver, PassConfig, PassReport};
use super::loop_unroll::LoopUnroll;
use super::reg::lo_passes;
use super::tests_support::{
	Env, RecordingObserver, create, empty_ir, kind_of, literal, ops_of_tag,
};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::print::{PrintOpts, print};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::core::walk::ops_in;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::bounds::{UNROLL_MAX_ITERATIONS, UNROLL_MAX_OPS};
use crate::exec::ir::iface::fold::{FoldConsumer, FoldOutcome, NotFoldable, NotYetFolder};
use crate::exec::ir::iface::ids::{BlockId, OpId, RegionId, ValueId};
use crate::exec::ir::iface::premise::PremiseSet;
use crate::exec::ir::iface::region_kind::{RegionKind, StatementCommit};
use crate::exec::ir::iface::statements::InfoSubject;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{
	DecisiveKind, LetAttrs, LetBinding, LogOp, LogTag, LoopKind, LoopSink, SourceOrder, StepAttrs,
};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::val::{Array, Set, Value};

const LOOP: OpTag = OpTag::Logical(LogTag::Loop);
const BLOCK: OpTag = OpTag::Logical(LogTag::Block);
const LET: OpTag = OpTag::Logical(LogTag::Let);
const IF: OpTag = OpTag::Logical(LogTag::If);
const YIELD: OpTag = OpTag::Logical(LogTag::Yield);
const TRUTHY: OpTag = OpTag::Expr(ExprTag::Truthy);
const ARRAY: OpTag = OpTag::Expr(ExprTag::Array);

type SourceFn<'a> = &'a dyn Fn(&mut Rewriter<'_>, InsertPoint) -> ValueId;
type BodyFn<'a> = &'a dyn Fn(&mut Rewriter<'_>, BlockId, [ValueId; 2]);

/// A loop to build: its kind and sink, how its source is made and how its body is filled and
/// closed. The body receives the element and the position.
struct Loop<'a> {
	kind: LoopKind,
	sink: LoopSink,
	source: SourceFn<'a>,
	body: BodyFn<'a>,
}

/// A loop added to a block.
struct Added {
	op: OpId,
	source: ValueId,
	produced: ValueId,
}

/// A container built from a [`Loop`].
struct Built {
	ir: Ir,
	/// The region of the statement.
	statement: RegionId,
	added: Added,
}

fn ints(items: &[i64]) -> Value {
	Value::Array(Array(items.iter().map(|item| Value::from(*item)).collect()))
}

fn int_set(items: &[i64]) -> Value {
	let mut set = Set::new();
	for item in items {
		set.insert(Value::from(*item));
	}
	Value::Set(set)
}

/// A source that is the literal `value`.
fn literal_source(value: Value) -> impl Fn(&mut Rewriter<'_>, InsertPoint) -> ValueId {
	move |rw: &mut Rewriter<'_>, at: InsertPoint| {
		let kind = OpKind::Expr(ExprOp::Literal(Box::new(value.clone())));
		create(rw, at, kind, &[], 1).1[0]
	}
}

/// Closes `body` with a `logical.yield` of `value`, when there is one.
fn close(rw: &mut Rewriter<'_>, body: BlockId, value: Option<ValueId>) {
	let operands: Vec<ValueId> = value.into_iter().collect();
	create(rw, InsertPoint::End(body), OpKind::Logical(LogOp::Yield), &operands, 0);
}

/// A body that reads the element and yields nothing.
fn read_element(rw: &mut Rewriter<'_>, body: BlockId, args: [ValueId; 2]) {
	create(rw, InsertPoint::End(body), OpKind::Expr(ExprOp::Truthy), &[args[0]], 1);
	close(rw, body, None);
}

/// A body that reads the position and yields nothing.
fn read_position(rw: &mut Rewriter<'_>, body: BlockId, args: [ValueId; 2]) {
	create(rw, InsertPoint::End(body), OpKind::Expr(ExprOp::Truthy), &[args[1]], 1);
	close(rw, body, None);
}

/// A body that yields the truthiness of the element.
fn yield_element(rw: &mut Rewriter<'_>, body: BlockId, args: [ValueId; 2]) {
	let (_, truth) =
		create(rw, InsertPoint::End(body), OpKind::Expr(ExprOp::Truthy), &[args[0]], 1);
	close(rw, body, Some(truth[0]));
}

/// A body of `count` literals that nothing reads, then the yield of nothing.
fn filler(count: usize) -> impl Fn(&mut Rewriter<'_>, BlockId, [ValueId; 2]) {
	move |rw: &mut Rewriter<'_>, body: BlockId, _args: [ValueId; 2]| {
		for _ in 0..count {
			create(rw, InsertPoint::End(body), literal(0), &[], 1);
		}
		close(rw, body, None);
	}
}

/// A body that holds an `IF` on a literal whose then arm ends in `terminator` and whose else arm
/// yields nothing, then yields nothing itself.
fn branch_with(terminator: LogOp) -> impl Fn(&mut Rewriter<'_>, BlockId, [ValueId; 2]) {
	move |rw: &mut Rewriter<'_>, body: BlockId, _args: [ValueId; 2]| {
		let (_, cond) = create(rw, InsertPoint::End(body), literal(1), &[], 1);
		let (branch, _) =
			create(rw, InsertPoint::End(body), OpKind::Logical(LogOp::If), &[cond[0]], 1);
		let (_, then_block) = rw
			.create_region(branch, RegionKind::StatementIf, &[])
			.expect("the then arm is created");
		create(rw, InsertPoint::End(then_block), OpKind::Logical(terminator.clone()), &[], 0);
		let (_, else_block) = rw
			.create_region(branch, RegionKind::StatementIf, &[])
			.expect("the else arm is created");
		close(rw, else_block, None);
		close(rw, body, None);
	}
}

/// A body whose only terminator is `terminator`.
fn ends_with(terminator: LogOp) -> impl Fn(&mut Rewriter<'_>, BlockId, [ValueId; 2]) {
	move |rw: &mut Rewriter<'_>, body: BlockId, _args: [ValueId; 2]| {
		create(rw, InsertPoint::End(body), OpKind::Logical(terminator.clone()), &[], 0);
	}
}

/// A body that sleeps, which is a barrier, then yields nothing.
fn sleeps(rw: &mut Rewriter<'_>, body: BlockId, _args: [ValueId; 2]) {
	let (_, duration) = create(rw, InsertPoint::End(body), literal(1), &[], 1);
	create(rw, InsertPoint::End(body), OpKind::Logical(LogOp::Sleep), &[duration[0]], 0);
	close(rw, body, None);
}

/// Adds the loop `spec` at `at`.
fn add_loop(rw: &mut Rewriter<'_>, at: InsertPoint, spec: &Loop<'_>) -> Added {
	let source = (spec.source)(rw, at);
	let kind = OpKind::Logical(LogOp::Loop {
		kind: spec.kind,
		sink: spec.sink.clone(),
		order: SourceOrder::Positional,
		step: None,
	});
	let (op, produced) = create(rw, at, kind, &[source], 1);
	let body_kind = if spec.kind == LoopKind::For {
		RegionKind::StatementFor
	} else {
		RegionKind::Loop
	};
	let (_, body) = rw
		.create_region(op, body_kind, &[TypeKnowledge::Unknown, TypeKnowledge::Unknown])
		.expect("the body region is created");
	let args = rw.ir().block(body).expect("the body exists").args.clone();
	(spec.body)(rw, body, [args[0], args[1]]);
	Added {
		op,
		source,
		produced: produced[0],
	}
}

/// A statement that holds the loop `spec` and closes with the loop's value when its sink
/// collects, and with nothing otherwise.
fn build(spec: &Loop<'_>) -> Built {
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
	let added = add_loop(&mut rw, InsertPoint::End(block), spec);
	let value = (spec.sink != LoopSink::Discard).then_some(added.produced);
	close(&mut rw, block, value);
	Built {
		ir,
		statement,
		added,
	}
}

/// Runs the pass over `ir` under `cfg` with the given facts and observer, without the driver's
/// verification, so a container built by hand is judged by the pass alone.
async fn run_with(
	ir: &mut Ir,
	facts: &mut FactCache,
	observer: &mut dyn FoldObserver,
	cfg: &PassConfig,
) -> PassReport {
	let env = Env::new();
	let effects = env.cx();
	let mut cx = LoCx {
		ir,
		facts,
		folder: &NotYetFolder,
		effects: &effects,
		cfg,
		observer,
	};
	LoopUnroll.run(&mut cx).await.expect("the pass runs")
}

async fn run(ir: &mut Ir, cfg: &PassConfig) -> PassReport {
	run_with(ir, &mut FactCache::new(), &mut NoObserver, cfg).await
}

fn printed(ir: &Ir) -> String {
	print(ir, ir.top(), PrintOpts::default())
}

/// The `logical.block` ops that stand directly in `region`, in order.
fn blocks_in(ir: &Ir, region: RegionId) -> Vec<OpId> {
	ops_in(ir, region).filter(|op| kind_of(ir, *op).tag() == BLOCK).collect()
}

/// The first region of `op`.
fn region_of(ir: &Ir, op: OpId) -> RegionId {
	ir.op(op).expect("the op exists").regions[0]
}

/// The ops of the first region of `op`, in order.
fn inner_ops(ir: &Ir, op: OpId) -> Vec<OpId> {
	ops_in(ir, region_of(ir, op)).collect()
}

/// The value of `op` when it is an `expr.literal`.
fn literal_value(ir: &Ir, op: OpId) -> Option<Value> {
	if let OpKind::Expr(ExprOp::Literal(value)) = kind_of(ir, op) {
		Some((**value).clone())
	} else {
		None
	}
}

/// The literals the first region of `op` holds directly, in order.
fn literals_in(ir: &Ir, op: OpId) -> Vec<Value> {
	inner_ops(ir, op).into_iter().filter_map(|inner| literal_value(ir, inner)).collect()
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
		} => panic!("{value:?} is a block argument"),
	}
}

/// A 3-element literal `FOR` becomes three statement blocks in order, each holding a literal of
/// its element that the copy of the body reads, and the loop's value becomes `NONE`.
#[tokio::test]
async fn unroll_literal_for() {
	let source = literal_source(ints(&[10, 20, 30]));
	let mut built = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &read_element,
	});
	let report = run(&mut built.ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 1);

	let ir = &built.ir;
	assert!(ops_of_tag(ir, LOOP).is_empty(), "the loop is gone");
	let blocks = blocks_in(ir, built.statement);
	assert_eq!(blocks.len(), 3);
	for (block, element) in blocks.iter().zip([10_i64, 20, 30]) {
		let region = ir.region(region_of(ir, *block)).expect("the region exists");
		assert_eq!(region.kind, RegionKind::Block);
		assert_eq!(literals_in(ir, *block), vec![Value::from(element)], "only the element is read");
		let ops = inner_ops(ir, *block);
		let element_value = ir.op(ops[0]).expect("the literal exists").results[0];
		let truthy = ops.iter().find(|op| kind_of(ir, **op).tag() == TRUTHY).expect("a read");
		assert_eq!(ir.op(*truthy).expect("the read exists").operands[0], element_value);
		let last = *ops.last().expect("the block is not empty");
		assert_eq!(kind_of(ir, last).tag(), YIELD, "the body's own yield closes the block");
	}
	let statement: Vec<Value> =
		ops_in(ir, built.statement).filter_map(|op| literal_value(ir, op)).collect();
	assert!(statement.contains(&Value::None), "a discarding loop's value is NONE");
}

/// The position is a literal too, and only a body that reads it gets one.
#[tokio::test]
async fn unroll_gives_the_position_a_literal() {
	let source = literal_source(ints(&[7, 8, 9]));
	let mut built = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &read_position,
	});
	assert_eq!(run(&mut built.ir, &PassConfig::ALL_ON).await.rewrites, 1);
	let ir = &built.ir;
	let blocks = blocks_in(ir, built.statement);
	assert_eq!(blocks.len(), 3);
	for (block, position) in blocks.iter().zip([0_i64, 1, 2]) {
		assert_eq!(literals_in(ir, *block), vec![Value::from(position)]);
	}
}

/// An `Output` sink yields an `expr.array` of the blocks' values in element order, and the blocks
/// are value blocks.
#[tokio::test]
async fn unroll_output_collects_into_an_array() {
	let source = literal_source(ints(&[1, 2]));
	let mut built = build(&Loop {
		kind: LoopKind::Closure,
		sink: LoopSink::Output,
		source: &source,
		body: &yield_element,
	});
	assert_eq!(run(&mut built.ir, &PassConfig::ALL_ON).await.rewrites, 1);

	let ir = &built.ir;
	assert!(ops_of_tag(ir, LOOP).is_empty());
	let blocks = blocks_in(ir, built.statement);
	assert_eq!(blocks.len(), 2);
	for block in &blocks {
		let region = ir.region(region_of(ir, *block)).expect("the region exists");
		assert_eq!(region.kind, RegionKind::ValueBlock);
	}
	let arrays = ops_of_tag(ir, ARRAY);
	let [array] = arrays.as_slice() else {
		panic!("one array, found {arrays:?}");
	};
	let data = ir.op(*array).expect("the array exists");
	let definers: Vec<OpId> = data.operands.iter().map(|value| definer(ir, *value)).collect();
	assert_eq!(definers, blocks, "the array holds the blocks' values in order");
	let closing = *ops_in(ir, built.statement).collect::<Vec<_>>().last().expect("a closing op");
	assert_eq!(kind_of(ir, closing).tag(), YIELD);
	assert_eq!(
		ir.op(closing).expect("the yield exists").operands[0],
		data.results[0],
		"the loop's reader reads the array"
	);
}

/// An `Output` loop over an empty array is the empty array.
#[tokio::test]
async fn unroll_of_an_empty_array_is_an_empty_array() {
	let source = literal_source(ints(&[]));
	let mut built = build(&Loop {
		kind: LoopKind::Closure,
		sink: LoopSink::Output,
		source: &source,
		body: &yield_element,
	});
	assert_eq!(run(&mut built.ir, &PassConfig::ALL_ON).await.rewrites, 1);
	let ir = &built.ir;
	assert!(ops_of_tag(ir, LOOP).is_empty());
	assert!(blocks_in(ir, built.statement).is_empty());
	let statement: Vec<Value> =
		ops_in(ir, built.statement).filter_map(|op| literal_value(ir, op)).collect();
	assert!(statement.contains(&Value::Array(Array(Vec::new()))));
}

/// A set is unrolled under a discarding sink, in its own order, and left alone under `Output`.
#[tokio::test]
async fn unroll_takes_a_set_only_under_discard() {
	let source = literal_source(int_set(&[2, 1]));
	let mut discards = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &read_element,
	});
	assert_eq!(run(&mut discards.ir, &PassConfig::ALL_ON).await.rewrites, 1);
	let blocks = blocks_in(&discards.ir, discards.statement);
	let elements: Vec<Vec<Value>> =
		blocks.iter().map(|block| literals_in(&discards.ir, *block)).collect();
	assert_eq!(elements, vec![vec![Value::from(1_i64)], vec![Value::from(2_i64)]]);

	let mut collects = build(&Loop {
		kind: LoopKind::Closure,
		sink: LoopSink::Output,
		source: &source,
		body: &yield_element,
	});
	let before = printed(&collects.ir);
	assert_eq!(run(&mut collects.ir, &PassConfig::ALL_ON).await.rewrites, 0);
	assert_eq!(printed(&collects.ir), before);
}

/// A sink that filters, decides or folds, and a loop kind that is not a `FOR`, closure or element
/// filter, are left as they are.
#[tokio::test]
async fn unroll_refuses_other_sinks_and_kinds() {
	let source = literal_source(ints(&[1, 2]));
	let shapes = [
		("filter", LoopKind::ElementFilter, LoopSink::Filter),
		("decisive", LoopKind::Closure, LoopSink::Decisive(DecisiveKind::Any)),
		("select", LoopKind::Select, LoopSink::Output),
		("write", LoopKind::Write, LoopSink::Output),
		("broadcast", LoopKind::Broadcast, LoopSink::Output),
	];
	for (name, kind, sink) in shapes {
		let mut built = build(&Loop {
			kind,
			sink,
			source: &source,
			body: &yield_element,
		});
		let before = printed(&built.ir);
		let report = run(&mut built.ir, &PassConfig::ALL_ON).await;
		assert_eq!(report.rewrites, 0, "{name}");
		assert_eq!(printed(&built.ir), before, "{name}");
	}
}

/// A `BREAK` that leaves the loop, a `CONTINUE` that lands on the body and a body that ends in a
/// terminator other than its yield each keep the loop whole.
#[tokio::test]
async fn unroll_refuses_break_and_continue() {
	let source = literal_source(ints(&[1, 2, 3]));
	let breaks = branch_with(LogOp::Break);
	let continues = branch_with(LogOp::Next);
	let ends_in_break = ends_with(LogOp::Break);
	let shapes: [(&str, BodyFn<'_>); 3] =
		[("break", &breaks), ("continue", &continues), ("ends in break", &ends_in_break)];
	for (name, body) in shapes {
		let mut built = build(&Loop {
			kind: LoopKind::For,
			sink: LoopSink::Discard,
			source: &source,
			body,
		});
		let before = printed(&built.ir);
		let report = run(&mut built.ir, &PassConfig::ALL_ON).await;
		assert_eq!(report.rewrites, 0, "{name}");
		assert_eq!(printed(&built.ir), before, "{name}");
		assert_eq!(ops_of_tag(&built.ir, LOOP).len(), 1, "{name}");
	}
}

/// A body with an `IF` whose arms close normally is copied with its regions, and a body that
/// sleeps has a barrier and stays a loop.
#[tokio::test]
async fn unroll_copies_nested_regions_and_refuses_a_barrier() {
	let source = literal_source(ints(&[1, 2]));
	let nested = branch_with(LogOp::Yield);
	let mut built = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &nested,
	});
	assert_eq!(run(&mut built.ir, &PassConfig::ALL_ON).await.rewrites, 1);
	let ir = &built.ir;
	let blocks = blocks_in(ir, built.statement);
	assert_eq!(blocks.len(), 2);
	for block in &blocks {
		let branches: Vec<OpId> =
			inner_ops(ir, *block).into_iter().filter(|op| kind_of(ir, *op).tag() == IF).collect();
		let [branch] = branches.as_slice() else {
			panic!("one branch in the block, found {branches:?}");
		};
		let data = ir.op(*branch).expect("the branch exists");
		assert_eq!(data.regions.len(), 2);
		for arm in &data.regions {
			assert_eq!(ir.region(*arm).expect("the arm exists").kind, RegionKind::StatementIf);
			assert_eq!(ops_in(ir, *arm).count(), 1, "each arm holds its terminator");
		}
		let condition = definer(ir, data.operands[0]);
		assert_eq!(
			ir.op(condition).expect("the condition exists").parent,
			ir.op(*branch).expect("the branch exists").parent,
			"the condition is copied beside the branch"
		);
	}
	assert_eq!(ops_of_tag(ir, IF).len(), 2);

	let mut asleep = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &sleeps,
	});
	let before = printed(&asleep.ir);
	assert_eq!(run(&mut asleep.ir, &PassConfig::ALL_ON).await.rewrites, 0);
	assert_eq!(printed(&asleep.ir), before);
}

/// The bound on the number of iterations and the bound on the ops the iterations hold are
/// inclusive, and a loop past either is left as it is.
#[tokio::test]
async fn unroll_respects_the_bounds() {
	let trips = i64::from(UNROLL_MAX_ITERATIONS);
	let full: Vec<i64> = (0..trips).collect();
	let over: Vec<i64> = (0..=trips).collect();
	let small = filler(1);
	for (name, items, expected) in
		[("at the iteration bound", &full, 1), ("past the iteration bound", &over, 0)]
	{
		let source = literal_source(ints(items));
		let mut built = build(&Loop {
			kind: LoopKind::For,
			sink: LoopSink::Discard,
			source: &source,
			body: &small,
		});
		let before = printed(&built.ir);
		assert_eq!(run(&mut built.ir, &PassConfig::ALL_ON).await.rewrites, expected, "{name}");
		if expected == 0 {
			assert_eq!(printed(&built.ir), before, "{name}");
		}
	}

	let per_iteration =
		usize::try_from(UNROLL_MAX_OPS / UNROLL_MAX_ITERATIONS).expect("the bound fits in a usize");
	let source = literal_source(ints(&full));
	let exact = filler(per_iteration - 1);
	let mut at_bound = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &exact,
	});
	assert_eq!(
		run(&mut at_bound.ir, &PassConfig::ALL_ON).await.rewrites,
		1,
		"the elements times the ops of the body equal the bound"
	);
	let heavy = filler(per_iteration);
	let mut past_bound = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &heavy,
	});
	let before = printed(&past_bound.ir);
	assert_eq!(run(&mut past_bound.ir, &PassConfig::ALL_ON).await.rewrites, 0);
	assert_eq!(printed(&past_bound.ir), before);
}

/// A source that is not a literal is unrolled when the folder answered a constant for it, and the
/// observer is told of the source while its defining op still exists. A refused fold, and no fold
/// at all, keep the loop.
#[tokio::test]
async fn unroll_uses_a_constant_fold() {
	let computed = |rw: &mut Rewriter<'_>, at: InsertPoint| {
		let (_, one) = create(rw, at, literal(1), &[], 1);
		let (_, two) = create(rw, at, literal(2), &[], 1);
		create(rw, at, OpKind::Expr(ExprOp::Array), &[one[0], two[0]], 1).1[0]
	};
	let shape = Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &computed,
		body: &read_element,
	};

	let mut unfolded = build(&shape);
	let before = printed(&unfolded.ir);
	assert_eq!(run(&mut unfolded.ir, &PassConfig::ALL_ON).await.rewrites, 0);
	assert_eq!(printed(&unfolded.ir), before, "an unfolded source is not a literal");

	let mut refused = build(&shape);
	let mut facts = FactCache::new();
	facts.store_fold(refused.added.source, FoldOutcome::NotFoldable(NotFoldable::NotYet));
	let report = run_with(&mut refused.ir, &mut facts, &mut NoObserver, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 0);

	let mut folded = build(&shape);
	let mut facts = FactCache::new();
	facts.store_fold(
		folded.added.source,
		FoldOutcome::Constant {
			value: ints(&[1, 2]),
			premises: PremiseSet::new(),
			depth: 0,
		},
	);
	let mut observer = RecordingObserver::default();
	let report = run_with(&mut folded.ir, &mut facts, &mut observer, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 1);
	assert_eq!(observer.seen, vec![(folded.added.source, FoldConsumer::Unroll)]);
	assert_eq!(observer.defined, vec![true], "the observer is told before the source is touched");
	assert!(ops_of_tag(&folded.ir, LOOP).is_empty());
	assert_eq!(blocks_in(&folded.ir, folded.statement).len(), 2);
}

/// A literal source needs no fold, so the observer is not told of it.
#[tokio::test]
async fn unroll_of_a_literal_does_not_tell_the_observer() {
	let source = literal_source(ints(&[1]));
	let mut built = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &read_element,
	});
	let mut observer = RecordingObserver::default();
	let report =
		run_with(&mut built.ir, &mut FactCache::new(), &mut observer, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 1);
	assert!(observer.seen.is_empty());
}

/// Loops inside a body are unrolled first, so the outer loop copies their iterations: two outer
/// iterations over two inner ones leave six blocks and no loop.
#[tokio::test]
async fn unroll_visits_inner_loops_first() {
	let inner_source = literal_source(ints(&[3, 4]));
	let inner = Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &inner_source,
		body: &read_element,
	};
	let nest = |rw: &mut Rewriter<'_>, body: BlockId, _args: [ValueId; 2]| {
		add_loop(rw, InsertPoint::End(body), &inner);
		close(rw, body, None);
	};
	let source = literal_source(ints(&[1, 2]));
	let mut built = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &nest,
	});
	assert_eq!(run(&mut built.ir, &PassConfig::ALL_ON).await.rewrites, 2);
	let ir = &built.ir;
	assert!(ops_of_tag(ir, LOOP).is_empty());
	assert_eq!(ops_of_tag(ir, BLOCK).len(), 6);
	let outer = blocks_in(ir, built.statement);
	assert_eq!(outer.len(), 2);
	for block in outer {
		let nested: Vec<OpId> =
			inner_ops(ir, block).into_iter().filter(|op| kind_of(ir, *op).tag() == BLOCK).collect();
		assert_eq!(nested.len(), 2, "each outer iteration holds both inner iterations");
	}
}

/// The pass follows its own switch, which only the full configuration turns on.
#[test]
fn the_pass_follows_its_switch() {
	assert!(LoopUnroll.enabled(&PassConfig::ALL_ON));
	assert!(!LoopUnroll.enabled(&PassConfig::RELEASE));
	assert!(!LoopUnroll.enabled(&PassConfig::ALL_OFF));
}

/// The pass runs after the push-below-permission pass and before unswitching and the memo
/// passes, so no memo is ever copied into an iteration.
#[test]
fn unroll_is_registered_before_the_memo_passes() {
	let names: Vec<&'static str> = lo_passes().iter().map(|pass| pass.name()).collect();
	let position = |wanted: &str| {
		names.iter().position(|name| *name == wanted).expect("the pass is registered")
	};
	assert!(position("push_below_permission") < position("loop_unroll"));
	assert!(position("loop_unroll") < position("unswitch"));
	assert!(position("unswitch") < position("licm"));
}

/// A `FOR` loop that binds a named parameter (`StepAttrs::Param`) emits a local `logical.let` for
/// that parameter in each unrolled iteration block and renames uses of the element block argument
/// to the `let`'s result.
#[tokio::test]
async fn unroll_for_with_param_binds_local_let_in_each_block() {
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
	let source = literal_source(ints(&[10, 20]))(&mut rw, InsertPoint::End(block));
	let (loop_op, _) = create(
		&mut rw,
		InsertPoint::End(block),
		OpKind::Logical(LogOp::Loop {
			kind: LoopKind::For,
			sink: LoopSink::Discard,
			order: SourceOrder::Positional,
			step: Some(Box::new(StepAttrs::Param("x".into()))),
		}),
		&[source],
		1,
	);
	let (_, body) = rw
		.create_region(
			loop_op,
			RegionKind::StatementFor,
			&[TypeKnowledge::Unknown, TypeKnowledge::Unknown],
		)
		.expect("the body region is created");
	let args = rw.ir().block(body).expect("the body exists").args.clone();
	read_element(&mut rw, body, [args[0], args[1]]);
	close(&mut rw, block, None);

	let report = run(&mut ir, &PassConfig::ALL_ON).await;
	assert_eq!(report.rewrites, 1);
	let blocks = blocks_in(&ir, statement);
	assert_eq!(blocks.len(), 2);
	for (block_op, expected) in blocks.iter().zip([10_i64, 20]) {
		let ops = inner_ops(&ir, *block_op);
		assert_eq!(literal_value(&ir, ops[0]), Some(Value::from(expected)));
		let lit_val = ir.op(ops[0]).expect("literal exists").results[0];
		assert_eq!(kind_of(&ir, ops[1]).tag(), LET);
		if let OpKind::Logical(LogOp::Let(attrs)) = kind_of(&ir, ops[1]) {
			assert_eq!(attrs.name.as_str(), "x");
			assert_eq!(attrs.binding, LetBinding::Local);
		} else {
			panic!("expected LogOp::Let");
		}
		assert_eq!(ir.op(ops[1]).expect("let exists").operands[0], lit_val);
		let let_val = ir.op(ops[1]).expect("let exists").results[0];
		let truthy = ops.iter().find(|op| kind_of(&ir, **op).tag() == TRUTHY).expect("a read");
		assert_eq!(ir.op(*truthy).expect("truthy exists").operands[0], let_val);
	}
}

/// A `FOR` loop whose `StatementFor` body discards a statement (`Collect::Discard`) is not
/// unrolled, whereas a statement consumed by a `LET` (`Collect::Let`) may be unrolled.
#[tokio::test]
async fn unroll_refuses_discarded_statement_in_for_body_and_allows_consumed_statement() {
	let source = literal_source(ints(&[1, 2]));
	let discarded_info = |rw: &mut Rewriter<'_>, body: BlockId, _args: [ValueId; 2]| {
		create(
			rw,
			InsertPoint::End(body),
			OpKind::Logical(LogOp::Info {
				subject: InfoSubject::Root,
				structure: false,
			}),
			&[],
			1,
		);
		close(rw, body, None);
	};
	let mut discarded = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &discarded_info,
	});
	let before = printed(&discarded.ir);
	assert_eq!(run(&mut discarded.ir, &PassConfig::ALL_ON).await.rewrites, 0);
	assert_eq!(printed(&discarded.ir), before);

	let consumed_info = |rw: &mut Rewriter<'_>, body: BlockId, _args: [ValueId; 2]| {
		let (_, info) = create(
			rw,
			InsertPoint::End(body),
			OpKind::Logical(LogOp::Info {
				subject: InfoSubject::Root,
				structure: false,
			}),
			&[],
			1,
		);
		create(
			rw,
			InsertPoint::End(body),
			OpKind::Logical(LogOp::Let(Box::new(LetAttrs {
				name: "info".into(),
				binding: LetBinding::Local,
				ty: None,
			}))),
			&[info[0]],
			1,
		);
		close(rw, body, None);
	};
	let mut consumed = build(&Loop {
		kind: LoopKind::For,
		sink: LoopSink::Discard,
		source: &source,
		body: &consumed_info,
	});
	assert_eq!(run(&mut consumed.ir, &PassConfig::ALL_ON).await.rewrites, 1);
}
