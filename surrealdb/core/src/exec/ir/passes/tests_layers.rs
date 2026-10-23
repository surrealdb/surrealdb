//! Tests of the passes that move a filter below the projection layers of a table, and of the
//! erasure of the layer columns nothing reads. The containers are built by hand: the binder emits
//! a layer only over a catalog that declares one.

use surrealdb_strand::Strand;

use super::dce::Dce;
use super::driver::{LoCx, LoPass, NoObserver, PassConfig, PassReport};
use super::perm_where::PushBelowLayer;
use super::rehome::rebuild_and_chain;
use super::tests_support::{Env, create, empty_ir, literal, ops_of_tag, spec};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::expr::ops::{ExprOp, ExprTag, ShortCircuitOp};
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::iface::ids::{BlockId, FrameId, OpId, ValueId};
use crate::exec::ir::iface::region_kind::{ClauseRole, FieldRole, RegionKind};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{FieldShape, FieldsShape, LogOp, LogTag, ProjectAttrs};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::BinaryOperator;
use crate::expr::logical::{EvalFrame, OutputPath};

const FILTER: OpTag = OpTag::Logical(LogTag::Filter);
const LAYER_OR_PROJECT: OpTag = OpTag::Logical(LogTag::Project);

/// A column of a layer: its name, whether it reads one element of the record id, and the field its
/// frame reads otherwise.
struct Column {
	name: &'static str,
	key: bool,
	reads: &'static str,
}

fn plain(name: &'static str) -> Column {
	Column {
		name,
		key: false,
		reads: name,
	}
}

fn key(name: &'static str) -> Column {
	Column {
		name,
		key: true,
		reads: "id",
	}
}

fn field(name: &str) -> OpKind {
	OpKind::Expr(ExprOp::Field {
		name: Strand::new(name),
	})
}

fn yield_op() -> OpKind {
	OpKind::Expr(ExprOp::Yield)
}

fn top_block(ir: &Ir) -> BlockId {
	ir.region(ir.top()).expect("the top region exists").blocks[0]
}

/// Runs `pass` over `ir` under the configuration with every pass on.
async fn run(pass: &dyn LoPass, ir: &mut Ir) -> PassReport {
	let env = Env::new();
	let effects = env.cx();
	let mut facts = FactCache::new();
	let mut observer = NoObserver;
	let mut cx = LoCx {
		ir,
		facts: &mut facts,
		folder: &NotYetFolder,
		effects: &effects,
		cfg: &PassConfig::ALL_ON,
		observer: &mut observer,
	};
	pass.run(&mut cx).await.expect("the pass runs")
}

/// A layer over `row` with the columns `columns`: one stored read each, then one field frame each.
/// The frame of a key column reads the element of the record id under the key-part frame.
fn layer_over(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	row: ValueId,
	columns: &[Column],
	key_frame: FrameId,
) -> (OpId, ValueId) {
	let mut shape = vec![FieldShape::All];
	shape.extend(columns.iter().map(|column| FieldShape::Single {
		alias: Some(OutputPath::field(column.name)),
		kind: None,
	}));
	let attrs = ProjectAttrs {
		fields: FieldsShape::Select(shape.into_boxed_slice()),
		layer: true,
		omit: 0,
		names: Vec::new(),
		computed: Vec::new(),
		sort_keys: Vec::new(),
		judged: None,
	};
	let (layer, out) = create(rw, at, OpKind::Logical(LogOp::Project(Box::new(attrs))), &[row], 1);
	for column in columns {
		let (_, body) = rw
			.create_region(
				layer,
				RegionKind::Clause(ClauseRole::Projection),
				&[TypeKnowledge::Unknown],
			)
			.expect("the stored read is created");
		let stored_row = rw.ir().block(body).expect("the block exists").args[0];
		let read = create(rw, InsertPoint::End(body), field(column.name), &[stored_row], 1).1[0];
		create(rw, InsertPoint::End(body), yield_op(), &[read], 0);
	}
	for column in columns {
		let (_, body) = rw
			.create_region(
				layer,
				RegionKind::FieldFrame(FieldRole::Computed),
				&[TypeKnowledge::Unknown, TypeKnowledge::Unknown, TypeKnowledge::Unknown],
			)
			.expect("the field frame is created");
		let frame_row = rw.ir().block(body).expect("the block exists").args[1];
		let at_end = InsertPoint::End(body);
		let read = if column.key {
			let make = |rw: &mut Rewriter<'_>, kind: OpKind, operands: &[ValueId]| {
				let mut made = spec(kind, operands, 1);
				made.frame = key_frame;
				let op = rw.create(at_end, made).expect("the op is created");
				rw.ir().op(op).expect("the op exists").results[0]
			};
			let id = make(rw, field("id"), &[frame_row]);
			let zero = make(rw, literal(0), &[]);
			make(rw, OpKind::Expr(ExprOp::Index), &[id, zero])
		} else {
			create(rw, at_end, field(column.reads), &[frame_row], 1).1[0]
		};
		let mut closing = spec(yield_op(), &[read], 0);
		if column.key {
			closing.frame = key_frame;
		}
		rw.create(at_end, closing).expect("the yield is created");
	}
	(layer, out[0])
}

/// The id of a frame that marks a column as one element of the record id.
fn key_part_frame(rw: &mut Rewriter<'_>) -> FrameId {
	let mut frame = rw.ir().frame(FrameId::new(0)).expect("the default frame exists").clone();
	frame.eval = EvalFrame::key_part();
	rw.intern_frame(frame).expect("the frame is interned")
}

/// `field(row, name) = 1` over the row of a clause.
fn compares(rw: &mut Rewriter<'_>, at: InsertPoint, row: ValueId, name: &str) -> ValueId {
	let read = create(rw, at, field(name), &[row], 1).1[0];
	let one = create(rw, at, literal(1), &[], 1).1[0];
	let equal = OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Equal,
	});
	create(rw, at, equal, &[read, one], 1).1[0]
}

/// `field(row, name) + 1`: a conjunct that reads a field and may raise.
fn may_raise(rw: &mut Rewriter<'_>, at: InsertPoint, row: ValueId, name: &str) -> ValueId {
	let read = create(rw, at, field(name), &[row], 1).1[0];
	let one = create(rw, at, literal(1), &[], 1).1[0];
	let add = OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Add,
	});
	create(rw, at, add, &[read, one], 1).1[0]
}

/// A `WHERE` filter over `input` whose predicate is the chain of the conjuncts `build` makes, in
/// order. Returns the filter and its result.
fn where_filter(
	rw: &mut Rewriter<'_>,
	at: InsertPoint,
	input: ValueId,
	build: &dyn Fn(&mut Rewriter<'_>, InsertPoint, ValueId) -> Vec<ValueId>,
) -> (OpId, ValueId) {
	let (op, out) = create(rw, at, OpKind::Logical(LogOp::Filter), &[input], 1);
	let (_, body) = rw
		.create_region(op, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])
		.expect("the clause region is created");
	let row = rw.ir().block(body).expect("the block exists").args[0];
	let conjuncts = build(rw, InsertPoint::End(body), row);
	let chain = rebuild_and_chain(rw, InsertPoint::End(body), &conjuncts).expect("the chain");
	create(rw, InsertPoint::End(body), yield_op(), &[chain], 0);
	(op, out[0])
}

/// A layer with `columns` over a stored row, and a filter over it holding the conjuncts `build`
/// makes. Returns the layer, the filter and the stored row.
fn layered_filter(
	ir: &mut Ir,
	columns: &[Column],
	build: &dyn Fn(&mut Rewriter<'_>, InsertPoint, ValueId) -> Vec<ValueId>,
) -> (OpId, OpId, ValueId) {
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(ir));
	let mut rw = Rewriter::new(ir, &mut facts);
	let frame = key_part_frame(&mut rw);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (layer, delivered) = layer_over(&mut rw, at, row[0], columns, frame);
	let (filter, _) = where_filter(&mut rw, at, delivered, build);
	(layer, filter, row[0])
}

/// How many field reads of `name` `ir` holds.
fn reads_of(ir: &Ir, name: &str) -> usize {
	ops_of_tag(ir, OpTag::Expr(ExprTag::Field))
		.into_iter()
		.filter(|op| {
			matches!(&ir.op(*op).expect("the op exists").kind,
				OpKind::Expr(ExprOp::Field { name: read }) if read.as_str() == name)
		})
		.count()
}

/// A conjunct that reads no column of the layer moves below it; one that reads a column stays.
#[tokio::test]
async fn a_conjunct_that_reads_no_column_moves_below_the_layer() {
	let mut ir = empty_ir();
	let (layer, filter, row) = layered_filter(&mut ir, &[plain("a")], &|rw, at, row| {
		vec![compares(rw, at, row, "b"), compares(rw, at, row, "a")]
	});
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 1);
	let filters = ops_of_tag(&ir, FILTER);
	assert_eq!(filters.len(), 2, "the conjunct on `a` stays above the layer");
	let below = ir.op(layer).expect("the layer stays").operands[0];
	assert_ne!(below, row, "the layer now reads the new filter");
	let kept = ir.op(filter).expect("the filter stays");
	assert_eq!(kept.operands.as_slice(), [ir.op(layer).expect("the layer").results[0]]);
}

/// A conjunct that stays above the layer and may raise keeps every later conjunct above it too,
/// since `AND` reaches the one that stays first.
#[tokio::test]
async fn a_conjunct_after_one_that_may_raise_stays() {
	let mut ir = empty_ir();
	let (layer, _, row) = layered_filter(&mut ir, &[plain("a")], &|rw, at, row| {
		vec![may_raise(rw, at, row, "a"), compares(rw, at, row, "b")]
	});
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(ir.op(layer).expect("the layer stays").operands[0], row);

	// A conjunct that stays and cannot raise holds nothing back when the later conjunct cannot
	// raise either.
	let mut ir = empty_ir();
	let (layer, _, row) = layered_filter(&mut ir, &[plain("a")], &|rw, at, row| {
		vec![compares(rw, at, row, "a"), compares(rw, at, row, "b")]
	});
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 1);
	assert_ne!(ir.op(layer).expect("the layer stays").operands[0], row);

	// A conjunct that may raise stays above the layer once an earlier conjunct stays, since moving
	// it below the layer would evaluate it before the earlier conjunct.
	let mut ir = empty_ir();
	let (layer, _, row) = layered_filter(&mut ir, &[plain("a")], &|rw, at, row| {
		vec![compares(rw, at, row, "a"), may_raise(rw, at, row, "b")]
	});
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(ir.op(layer).expect("the layer stays").operands[0], row);

	// A leading conjunct that may raise still moves when nothing before it stays.
	let mut ir = empty_ir();
	let (layer, _, row) = layered_filter(&mut ir, &[plain("a")], &|rw, at, row| {
		vec![may_raise(rw, at, row, "b"), compares(rw, at, row, "a")]
	});
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 1);
	assert_ne!(ir.op(layer).expect("the layer stays").operands[0], row);

	// A short-circuit conjunct whose lazy arm references a fallible op in the outer block is
	// recognized as fallible, keeping later conjuncts above the layer.
	let mut ir = empty_ir();
	let (layer, _, row) = layered_filter(&mut ir, &[plain("a")], &|rw, at, row| {
		let lhs = compares(rw, at, row, "a");
		let rhs_outer = may_raise(rw, at, row, "a");
		let (or_op, or_out) = create(
			rw,
			at,
			OpKind::Expr(ExprOp::ShortCircuit {
				op: ShortCircuitOp::Or,
			}),
			&[lhs],
			1,
		);
		let (_, lazy_block) =
			rw.create_region(or_op, RegionKind::LazyOperand, &[]).expect("the lazy region");
		create(rw, InsertPoint::End(lazy_block), yield_op(), &[rhs_outer], 0);
		vec![or_out[0], compares(rw, at, row, "b")]
	});
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(ir.op(layer).expect("the layer stays").operands[0], row);

	// A short-circuit conjunct whose lazy arm only performs a plain field read off the row is
	// recognized as infallible over the row, allowing a later infallible conjunct to move.
	let mut ir = empty_ir();
	let (layer, _, row) = layered_filter(&mut ir, &[plain("a")], &|rw, at, row| {
		let lhs = compares(rw, at, row, "a");
		let (or_op, or_out) = create(
			rw,
			at,
			OpKind::Expr(ExprOp::ShortCircuit {
				op: ShortCircuitOp::Or,
			}),
			&[lhs],
			1,
		);
		let (_, lazy_block) =
			rw.create_region(or_op, RegionKind::LazyOperand, &[]).expect("the lazy region");
		let rhs = compares(rw, InsertPoint::End(lazy_block), row, "a");
		create(rw, InsertPoint::End(lazy_block), yield_op(), &[rhs], 0);
		vec![or_out[0], compares(rw, at, row, "b")]
	});
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 1);
	assert_ne!(ir.op(layer).expect("the layer stays").operands[0], row);
}

/// A read of a key column is the read of the element of the record id the column stands for: the
/// conjunct moves below the layer with the read replaced by a copy of the column's frame.
#[tokio::test]
async fn a_conjunct_over_a_key_column_moves_as_a_read_of_the_id() {
	let mut ir = empty_ir();
	let (layer, filter, row) =
		layered_filter(&mut ir, &[key("city")], &|rw, at, row| vec![compares(rw, at, row, "city")]);
	assert_eq!(reads_of(&ir, "id"), 1);
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 1);
	assert_ne!(ir.op(layer).expect("the layer stays").operands[0], row);
	assert!(ir.op(filter).is_none(), "the filter whose conjuncts all moved is replaced");
	// The moved conjunct reads the id off the stored row; only the layer's own stored read of the
	// column still names `city`.
	assert_eq!(reads_of(&ir, "id"), 2);
	assert_eq!(reads_of(&ir, "city"), 1);
}

/// A conjunct that holds an index operator or runs code the expression does not hold stays above
/// the layer, which may hide what that code reads.
#[tokio::test]
async fn a_conjunct_that_runs_unseen_code_stays_above_the_layer() {
	let mut ir = empty_ir();
	let (layer, _, row) = layered_filter(&mut ir, &[plain("a")], &|rw, at, row| {
		let read = create(rw, at, field("b"), &[row], 1).1[0];
		vec![create(rw, at, OpKind::Expr(ExprOp::Invoke), &[read], 1).1[0]]
	});
	let report = run(&PushBelowLayer, &mut ir).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(ir.op(layer).expect("the layer stays").operands[0], row);
}

/// A layer whose column nothing reads goes, and its readers read its input.
#[tokio::test]
async fn a_layer_whose_columns_nothing_reads_is_erased() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let frame = key_part_frame(&mut rw);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (_, delivered) = layer_over(&mut rw, at, row[0], &[plain("a")], frame);
	let (reader, _) =
		where_filter(&mut rw, at, delivered, &|rw, at, row| vec![compares(rw, at, row, "b")]);
	let report = run(&Dce, &mut ir).await;
	assert_eq!(report.rewrites, 1);
	assert!(ops_of_tag(&ir, LAYER_OR_PROJECT).is_empty());
	let _ = reader;
	assert_eq!(ops_of_tag(&ir, FILTER).len(), 1);
	let filter = ops_of_tag(&ir, FILTER)[0];
	assert_eq!(ir.op(filter).expect("the filter stays").operands[0], row[0]);
}

/// Only the columns nothing reads go; a column another kept column reads stays, and a reader that
/// takes the whole row keeps every column.
#[tokio::test]
async fn a_layer_column_that_is_read_stays() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let frame = key_part_frame(&mut rw);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let columns = [
		plain("a"),
		plain("dead"),
		Column {
			name: "c",
			key: false,
			reads: "a",
		},
	];
	let (_, delivered) = layer_over(&mut rw, at, row[0], &columns, frame);
	where_filter(&mut rw, at, delivered, &|rw, at, row| vec![compares(rw, at, row, "c")]);
	let report = run(&Dce, &mut ir).await;
	assert_eq!(report.rewrites, 1);
	let layer = ops_of_tag(&ir, LAYER_OR_PROJECT)[0];
	let OpKind::Logical(LogOp::Project(attrs)) = &ir.op(layer).expect("the layer stays").kind
	else {
		panic!("a projection");
	};
	let FieldsShape::Select(shape) = &attrs.fields else {
		panic!("a field list");
	};
	assert_eq!(shape.len(), 3, "`*`, `a` and `c` remain");
	assert_eq!(ir.op(layer).expect("the layer").regions.len(), 4);

	// A reader of the whole row demands every column.
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let at = InsertPoint::End(top_block(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let frame = key_part_frame(&mut rw);
	let (_, row) = create(&mut rw, at, literal(1), &[], 1);
	let (_, delivered) = layer_over(&mut rw, at, row[0], &[plain("a")], frame);
	where_filter(&mut rw, at, delivered, &|rw, at, row| {
		vec![create(rw, at, OpKind::Expr(ExprOp::Invoke), &[row], 1).1[0]]
	});
	let report = run(&Dce, &mut ir).await;
	assert_eq!(report.rewrites, 0);
}
