//! Tests of common-subexpression elimination, logical and physical.

use super::cse::{Cse, Scope, merge_equal_values};
use super::driver::{LoPass, NoObserver, PassConfig, run_passes};
use super::phys_cse;
use super::tests_support::{
	Env, bind, create, empty_ir, literal, ops_of_tag, spec, verify_logical,
};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::print::{PrintOpts, print};
use crate::exec::ir::core::rewriter::{InsertPoint, OpSpec, Provenance, Rewriter};
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::bounds::KEYED_LITERAL_MAX_BYTES;
use crate::exec::ir::iface::conversion::Phase;
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::iface::frame::Snapshot;
use crate::exec::ir::iface::ids::{FrameId, OpId};
use crate::exec::ir::iface::region_kind::{ClauseRole, ErrorScope, RegionKind, StatementCommit};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{LogOp, LogTag};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::BinaryOperator;
use crate::val::Value;

const BINARY: OpTag = OpTag::Expr(ExprTag::Binary);
const LITERAL: OpTag = OpTag::Expr(ExprTag::Literal);
const SHORT_CIRCUIT: OpTag = OpTag::Expr(ExprTag::ShortCircuit);
const ARRAY: OpTag = OpTag::Expr(ExprTag::Array);

fn count(ir: &Ir, tag: OpTag) -> usize {
	ops_of_tag(ir, tag).len()
}

fn add() -> OpKind {
	OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Add,
	})
}

/// Runs [`Cse`] alone over `ir` and returns the number of rewrites it reports.
async fn run_cse(ir: &mut Ir, env: &Env) -> u32 {
	let passes: Vec<Box<dyn LoPass>> = vec![Box::new(Cse)];
	let mut facts = FactCache::new();
	let reports = run_passes(
		&passes,
		ir,
		&mut facts,
		&NotYetFolder,
		&env.cx(),
		&PassConfig::RELEASE,
		&mut NoObserver,
	)
	.await
	.expect("the pass runs");
	reports[0].1.rewrites
}

/// Two equal sums are one: the later one is erased, its readers read the earlier one, and the
/// earlier one stands for both in provenance.
#[tokio::test]
async fn cse_merges_equal_pure_values() {
	let env = Env::new();
	let mut ir = bind("RETURN [1 + 2, 1 + 2]").await;
	let sums = ops_of_tag(&ir, BINARY);
	let [first, second] = sums.as_slice() else {
		panic!("the array binds two sums, found {sums:?}");
	};
	let (first, second) = (*first, *second);
	assert!(run_cse(&mut ir, &env).await >= 1);
	assert_eq!(count(&ir, BINARY), 1);
	assert!(ir.op(second).is_none(), "the later sum is erased");
	let kept = ir.op(first).expect("the earlier sum stays");
	match &kept.provenance {
		Provenance::From(ids) => assert!(ids.contains(&second), "the sum stands for both"),
		Provenance::Bound => panic!("a merge unions the provenance of the op it removes"),
	}
	let array = ops_of_tag(&ir, ARRAY)[0];
	let operands = &ir.op(array).expect("the array stays").operands;
	assert_eq!(operands[0], operands[1], "both elements read the one sum");
}

/// `a AND b` and `a AND c` have equal keys over their operands, but their right operands are
/// regions the key does not hold, so neither is merged.
#[tokio::test]
async fn cse_never_merges_region_owners() {
	let env = Env::new();
	let mut ir = bind("RETURN [$a AND $b, $a AND $c]").await;
	assert_eq!(count(&ir, SHORT_CIRCUIT), 2);
	run_cse(&mut ir, &env).await;
	assert_eq!(count(&ir, SHORT_CIRCUIT), 2);
}

/// Two reads of one parameter around a statement-level `DEFINE PARAM` answer different values, so
/// the sums that read them stay two.
#[tokio::test]
async fn cse_keeps_catalog_reads_apart_after_a_change() {
	let env = Env::new();
	let mut ir = bind(
		"DEFINE PARAM $q VALUE 1; \
		 SELECT VALUE [$q + 1, (DEFINE PARAM OVERWRITE $q VALUE $q + 5), $q + 1] FROM [1, 2];",
	)
	.await;
	let sums = count(&ir, BINARY);
	run_cse(&mut ir, &env).await;
	assert_eq!(count(&ir, BINARY), sums);
}

/// A read the evaluation context supplies is its own evaluation at each position: two reads of one
/// parameter are two ops, and the sums over them stay two.
#[tokio::test]
async fn cse_never_merges_a_read_the_context_supplies() {
	let env = Env::new();
	let mut ir = bind("RETURN [$a + 1, $a + 1]").await;
	let reads = ops_of_tag(&ir, OpTag::Expr(ExprTag::FreeParam)).len();
	assert_eq!(reads, 2);
	run_cse(&mut ir, &env).await;
	assert_eq!(ops_of_tag(&ir, OpTag::Expr(ExprTag::FreeParam)).len(), reads);
	assert_eq!(count(&ir, BINARY), 2);
}

/// Stored code is bound and analysed and never rewritten: two equal sums in a field's `VALUE`
/// clause stay two.
#[tokio::test]
async fn cse_never_rewrites_a_stored_clause() {
	let env = Env::new();
	let mut ir = bind("DEFINE FIELD f ON t VALUE ($value + 1) + ($value + 1)").await;
	let sums = count(&ir, BINARY);
	assert!(sums >= 3, "the clause binds its three sums, found {sums}");
	assert_eq!(run_cse(&mut ir, &env).await, 0);
	assert_eq!(count(&ir, BINARY), sums);
}

/// The first op of the first statement's block.
fn first_in_statement(ir: &Ir) -> OpId {
	let statement = ops_of_tag(ir, OpTag::Logical(LogTag::Stmt))[0];
	let region = ir.op(statement).expect("the statement exists").regions[0];
	let block = ir.region(region).expect("the region exists").blocks[0];
	ir.block(block).expect("the block exists").ops[0]
}

/// A literal of the given frame, whose result nothing reads.
fn literal_in(frame: u16) -> OpSpec {
	OpSpec {
		frame: FrameId::new(frame),
		..spec(literal(7), &[], 1)
	}
}

/// A statement-shaped owner under which one `ErrorScope(ShapeToNone)` region holds the ops of a
/// test. The owner is a `Stmt` only so that the scope has an owner op; the ops of the region are
/// grouped with the enclosing statement.
fn scope_owner() -> OpKind {
	OpKind::Logical(LogOp::Stmt {
		commit: StatementCommit::Auto,
		control: None,
	})
}

/// Builds, before the first op of the statement, a sum over the literals `7` and `8` outside any
/// scope and an owner whose error scope holds `inside` sums of the same operands. Answers the sum
/// outside the scope, the sums inside it and the scope block.
fn sums_around_an_error_scope(
	ir: &mut Ir,
	facts: &mut FactCache,
	inside: usize,
) -> (OpId, Vec<OpId>, crate::exec::ir::iface::ids::BlockId) {
	let at = InsertPoint::Before(first_in_statement(ir));
	let mut rw = Rewriter::new(ir, facts);
	let (_, one) = create(&mut rw, at, literal(7), &[], 1);
	let (_, two) = create(&mut rw, at, literal(8), &[], 1);
	let operands = [one[0], two[0]];
	let (outside, _) = create(&mut rw, at, add(), &operands, 1);
	let (owner, _) = create(&mut rw, at, scope_owner(), &[], 0);
	let (_, scope) = rw
		.create_region(owner, RegionKind::ErrorScope(ErrorScope::ShapeToNone), &[])
		.expect("the scope is created");
	let inner = (0..inside)
		.map(|_| create(&mut rw, InsertPoint::End(scope), add(), &operands, 1).0)
		.collect();
	(outside, inner, scope)
}

/// An op bound in an error scope is not merged into an equal op outside it: the merge would give
/// the outside op the scope's provenance, which the outside op does not stand in.
#[tokio::test]
async fn cse_never_merges_an_op_out_of_its_error_scope() {
	let mut ir = bind("RETURN 0").await;
	let mut facts = FactCache::new();
	let (_, inner, _) = sums_around_an_error_scope(&mut ir, &mut facts, 1);
	let env = Env::new();
	let merged =
		merge_equal_values(&mut ir, &mut facts, &env.cx(), Scope::Logical).expect("the merge runs");
	assert_eq!(merged, 0);
	assert!(ir.op(inner[0]).is_some(), "the op inside the scope stays");
	assert_eq!(count(&ir, BINARY), 2);
}

/// Equal sums inside one error scope still merge with each other, though not with the sum outside.
#[tokio::test]
async fn cse_merges_equal_ops_within_one_error_scope() {
	let mut ir = bind("RETURN 0").await;
	let mut facts = FactCache::new();
	let (_, inner, _) = sums_around_an_error_scope(&mut ir, &mut facts, 2);
	let env = Env::new();
	let merged =
		merge_equal_values(&mut ir, &mut facts, &env.cx(), Scope::Logical).expect("the merge runs");
	assert_eq!(
		merged, 1,
		"the second inner sum merges into the first; the outer sum is out of scope"
	);
	assert!(ir.op(inner[1]).is_none());
	assert!(ir.op(inner[0]).is_some());
}

/// Equal sums in two statements are two, and two equal literals of different frames are two while
/// two of one frame are one.
#[tokio::test]
async fn cse_never_across_frames_or_statements() {
	let env = Env::new();
	let mut ir = bind("RETURN 1 + 2; RETURN 1 + 2;").await;
	assert_eq!(run_cse(&mut ir, &env).await, 0);
	assert_eq!(count(&ir, BINARY), 2);

	let mut ir = bind("RETURN 1").await;
	let mut facts = FactCache::new();
	let at = InsertPoint::Before(first_in_statement(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	for frame in [0, 0, 1] {
		rw.create(at, literal_in(frame)).expect("the literal is created");
	}
	let literals = count(&ir, LITERAL);
	let merged =
		merge_equal_values(&mut ir, &mut facts, &env.cx(), Scope::Logical).expect("the merge runs");
	assert_eq!(merged, 1);
	assert_eq!(count(&ir, LITERAL), literals - 1);
}

/// A literal text of `bytes` bytes.
fn text_literal(bytes: usize) -> OpKind {
	OpKind::Expr(ExprOp::Literal(Box::new(Value::from("x".repeat(bytes)))))
}

/// Two equal literals under the keyed size are one; two equal literals over it stay two, and no
/// digest of either is built.
#[tokio::test]
async fn cse_leaves_literals_over_the_keyed_size() {
	let env = Env::new();
	for (bytes, merged) in [(KEYED_LITERAL_MAX_BYTES, 1), (KEYED_LITERAL_MAX_BYTES + 1, 0)] {
		let mut ir = bind("RETURN 1").await;
		let mut facts = FactCache::new();
		let at = InsertPoint::Before(first_in_statement(&ir));
		let mut rw = Rewriter::new(&mut ir, &mut facts);
		for _ in 0..2 {
			rw.create(at, spec(text_literal(bytes), &[], 1)).expect("the literal is created");
		}
		let literals = count(&ir, LITERAL);
		let answer = merge_equal_values(&mut ir, &mut facts, &env.cx(), Scope::Logical)
			.expect("the merge runs");
		assert_eq!(answer, merged, "{bytes} bytes");
		assert_eq!(count(&ir, LITERAL), literals - merged as usize);
	}
}

/// Two equal literals that two snapshots name as their versions are one: both snapshots name the
/// surviving literal, every op reading either finds a live version value, and the container
/// verifies.
#[tokio::test]
async fn cse_keeps_version_snapshots_on_live_values() {
	let env = Env::new();
	let mut ir = bind("RETURN 1").await;
	let mut facts = FactCache::new();
	let at = InsertPoint::Before(first_in_statement(&ir));
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (first, first_value) = create(&mut rw, at, literal(7), &[], 1);
	let (second, second_value) = create(&mut rw, at, literal(7), &[], 1);
	let mut readers = Vec::new();
	for (value, kind) in [(first_value[0], literal(8)), (second_value[0], literal(9))] {
		let snapshot =
			rw.intern_snapshot(Snapshot::Version(value)).expect("the snapshot is interned");
		let reader = rw
			.create(
				at,
				OpSpec {
					snapshot,
					..spec(kind, &[], 1)
				},
			)
			.expect("the reader is created");
		readers.push(reader);
	}
	let merged =
		merge_equal_values(&mut ir, &mut facts, &env.cx(), Scope::Logical).expect("the merge runs");
	assert_eq!(merged, 1);
	assert!(ir.op(first).is_some());
	assert!(ir.op(second).is_none());
	for reader in readers {
		let data = ir.op(reader).expect("the reader stands");
		assert_eq!(ir.snapshot(data.snapshot), Some(&Snapshot::Version(first_value[0])));
	}
	verify_logical(&ir, &mut facts, &env).expect("the container verifies");
}

/// A physical container with a filter whose `WHERE` clause holds two equal sums, an array of them
/// and a literal nothing reads.
fn physical_filter() -> Ir {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let top = ir.region(ir.top()).expect("the top region exists").blocks[0];
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let commit = StatementCommit::Auto;
	let (stmt, _) = create(
		&mut rw,
		InsertPoint::End(top),
		OpKind::Phys(PhysOp::Stmt {
			commit,
			control: None,
		}),
		&[],
		0,
	);
	let (_, block) = rw
		.create_region(stmt, RegionKind::Statement(commit), &[TypeKnowledge::Unknown])
		.expect("the statement region is created");
	let (_, input) = create(&mut rw, InsertPoint::End(block), literal(0), &[], 1);
	let (filter, kept) =
		create(&mut rw, InsertPoint::End(block), OpKind::Phys(PhysOp::Filter), &[input[0]], 1);
	create(&mut rw, InsertPoint::End(block), OpKind::Expr(ExprOp::Yield), &[kept[0]], 0);
	let (_, clause) = rw
		.create_region(filter, RegionKind::Clause(ClauseRole::Where), &[TypeKnowledge::Unknown])
		.expect("the clause region is created");
	let at = InsertPoint::End(clause);
	let mut sums = Vec::new();
	for _ in 0..2 {
		let (_, one) = create(&mut rw, at, literal(1), &[], 1);
		let (_, two) = create(&mut rw, at, literal(2), &[], 1);
		let (_, sum) = create(&mut rw, at, add(), &[one[0], two[0]], 1);
		sums.push(sum[0]);
	}
	create(&mut rw, at, literal(9), &[], 1);
	let (_, both) = create(&mut rw, at, OpKind::Expr(ExprOp::Array), &sums, 1);
	create(&mut rw, at, OpKind::Expr(ExprOp::Yield), &[both[0]], 0);
	rw.advance_phase(Phase::Physical).expect("the phase advances");
	ir
}

/// The equal sums of a filter's clause are one, and the literal nothing reads is erased.
#[test]
fn phys_cse_merges_projection_exprs() {
	let env = Env::new();
	let mut ir = physical_filter();
	let mut facts = FactCache::new();
	let report = phys_cse::run(&mut ir, &mut facts, &env.cx()).expect("the pass runs");
	assert!(report.rewrites >= 2, "{report:?}");
	assert_eq!(count(&ir, BINARY), 1);
	assert_eq!(count(&ir, LITERAL), 3, "the input, 1 and 2 stay; 9 is dead");
	let array = ops_of_tag(&ir, ARRAY)[0];
	let operands = &ir.op(array).expect("the array stays").operands;
	assert_eq!(operands[0], operands[1]);
}

/// A container that is not physical is left as it is.
#[tokio::test]
async fn phys_cse_leaves_a_logical_container_alone() {
	let env = Env::new();
	let mut ir = bind("RETURN [1 + 2, 1 + 2]").await;
	let before = print(&ir, ir.top(), PrintOpts::default());
	let mut facts = FactCache::new();
	let report = phys_cse::run(&mut ir, &mut facts, &env.cx()).expect("the pass runs");
	assert_eq!(report, Default::default());
	assert_eq!(print(&ir, ir.top(), PrintOpts::default()), before);
}
