//! Tests of the memo candidates, of the conjunct reorder and of the memo pass. The statements the
//! binder produces from SQL are used where it binds the shape; a permission predicate and a field
//! guard are built by hand, because the binder emits neither over an empty catalog.

use super::driver::{LoCx, LoPass, NoObserver, PassConfig, PassReport, run_passes};
use super::licm::{
	HoistLeadingConjuncts, Licm, MAX_MEMOS_PER_STATEMENT, MemoCandidate, candidates,
};
use super::rehome::and_chain;
use super::tests_support::{
	Env, bind, create, empty_ir, kind_of, literal, ops_of_tag, thread_tokens, verify_logical,
};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::{Ir, Walk, WalkOrder};
use crate::exec::ir::core::print::{PrintOpts, print};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::core::value::ValueDef;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::iface::ids::{BlockId, OpId, ValueId};
use crate::exec::ir::iface::region_kind::{RegionKind, StatementCommit};
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::{
	FieldsShape, LogOp, LogTag, LoopKind, LoopSink, ProjectAttrs, SourceOrder,
};
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::expr::BinaryOperator;
use crate::expr::logical::GuardKind;

const STMT: OpTag = OpTag::Logical(LogTag::Stmt);
const FILTER: OpTag = OpTag::Logical(LogTag::Filter);
const LOOP: OpTag = OpTag::Logical(LogTag::Loop);
const BINARY: OpTag = OpTag::Expr(ExprTag::Binary);
const CONVERT: OpTag = OpTag::Expr(ExprTag::Convert);
const ACCESS: OpTag = OpTag::Expr(ExprTag::Access);
const DEREF: OpTag = OpTag::Expr(ExprTag::Deref);
const SESSION_PARAM: OpTag = OpTag::Expr(ExprTag::SessionParam);

/// A `WHERE` whose first conjunct depends on the row and whose second is invariant and raises.
const FIRST_USE: &str = "SELECT VALUE $this FROM [1] WHERE $this = 2 AND <int>'x' = 1";

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

fn printed(ir: &Ir) -> String {
	print(ir, ir.top(), PrintOpts::default())
}

fn top_block(ir: &Ir) -> BlockId {
	ir.region(ir.top()).expect("the top region exists").blocks[0]
}

/// The candidates of every statement of `ir`.
fn all_candidates(ir: &Ir) -> Vec<MemoCandidate> {
	let env = Env::new();
	let effects = env.cx();
	let mut facts = FactCache::new();
	ops_of_tag(ir, STMT)
		.into_iter()
		.flat_map(|stmt| candidates(ir, &mut facts, &effects, stmt))
		.collect()
}

/// The conjuncts of the one `WHERE` filter of `ir`, left to right.
fn where_chain(ir: &Ir) -> Vec<ValueId> {
	let filters = ops_of_tag(ir, FILTER);
	let [filter] = filters.as_slice() else {
		panic!("the statement binds one filter, found {filters:?}");
	};
	let region = ir.op(*filter).expect("the filter exists").regions[0];
	let block = ir.region(region).expect("the clause exists").blocks[0];
	let yielded = *ir.block(block).expect("the block exists").ops.last().expect("a yield");
	let predicate = ir.op(yielded).expect("the yield exists").operands[0];
	and_chain(ir, predicate).to_vec()
}

/// Whether an op of `tag` computes `value`: it defines it, defines an operand of it, or stands in
/// a region of one of them.
fn computes(ir: &Ir, value: ValueId, tag: OpTag) -> bool {
	let Some(ValueDef::OpResult {
		op,
		..
	}) = ir.value(value).map(|data| data.def)
	else {
		return false;
	};
	let data = ir.op(op).expect("the op exists");
	if data.kind.tag() == tag || data.operands.iter().any(|operand| computes(ir, *operand, tag)) {
		return true;
	}
	let mut found = false;
	for region in data.regions.iter().copied() {
		ir.walk(region, WalkOrder::PreOrder, &mut |inner| {
			if kind_of(ir, inner).tag() == tag {
				found = true;
				return Walk::Stop;
			}
			Walk::Continue
		});
	}
	found
}

fn yield_op() -> OpKind {
	OpKind::Expr(ExprOp::Yield)
}

fn add() -> OpKind {
	OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Add,
	})
}

/// `1 + 1`: an invariant value that may fail.
fn fallible(rw: &mut Rewriter<'_>, at: InsertPoint) -> ValueId {
	let (_, one) = create(rw, at, literal(1), &[], 1);
	create(rw, at, add(), &[one[0], one[0]], 1).1[0]
}

/// A statement holding a loop over a literal, with the loop's body block and its row argument.
fn loop_in_statement(ir: &mut Ir, facts: &mut FactCache) -> (OpId, OpId, BlockId, ValueId) {
	let top = top_block(ir);
	let mut rw = Rewriter::new(ir, facts);
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
	let (_, block) = rw
		.create_region(stmt, RegionKind::Statement(commit), &[TypeKnowledge::Unknown])
		.expect("the statement region is created");
	let at = InsertPoint::End(block);
	let (_, source) = create(&mut rw, at, literal(1), &[], 1);
	let kind = OpKind::Logical(LogOp::Loop {
		kind: LoopKind::Select,
		sink: LoopSink::Output,
		order: SourceOrder::Delivery,
		step: None,
	});
	let (loop_op, _) = create(&mut rw, at, kind, &[source[0]], 1);
	let (_, body) = rw
		.create_region(loop_op, RegionKind::Loop, &[TypeKnowledge::Unknown, TypeKnowledge::Unknown])
		.expect("the body region is created");
	let row = rw.ir().block(body).expect("the body exists").args[0];
	(stmt, loop_op, body, row)
}

/// The invariant conjunct of a first-use `WHERE` is the one candidate: the largest invariant
/// subexpression, whose reader depends on the row.
#[tokio::test]
async fn the_invariant_conjunct_is_the_one_candidate() {
	let ir = bind(FIRST_USE).await;
	let found = all_candidates(&ir);
	let [candidate] = found.as_slice() else {
		panic!("one candidate, found {found:?}");
	};
	assert_eq!(candidate.levels, 1);
	assert!(candidate.ops > 1);
	assert_eq!(kind_of(&ir, candidate.root).tag(), BINARY);
	assert_eq!(kind_of(&ir, candidate.scope).tag(), LOOP);
	let value = ir.op(candidate.root).expect("the root exists").results[0];
	assert!(computes(&ir, value, CONVERT));
}

/// Values that draw entropy or advance state are never candidates.
#[tokio::test]
async fn nothing_that_varies_between_reaches_is_a_candidate() {
	for sql in [
		"SELECT VALUE $this FROM [1] WHERE $this = 1 AND rand::float() > 0.5",
		"SELECT VALUE $this FROM [1] WHERE $this = 1 AND sequence::nextval('s') > 0",
	] {
		let ir = bind(sql).await;
		assert!(all_candidates(&ir).is_empty(), "{sql}");
	}
}

/// A catalog read is never a candidate in a statement that changes the catalog.
#[tokio::test]
async fn catalog_reads_are_not_candidates_where_the_catalog_changes() {
	let sql = "DEFINE PARAM $q VALUE 1; \
		SELECT VALUE [$q + 1, (DEFINE PARAM OVERWRITE $q VALUE $q + 5), $q + 1] FROM [1, 2]";
	let ir = bind(sql).await;
	assert!(all_candidates(&ir).is_empty());
}

/// A function call and a stored parameter read in a loop of a statement that changes the catalog
/// are not candidates, wherever the change stands in the loop body: the next iteration sees it.
#[tokio::test]
async fn a_catalog_read_before_a_loop_carried_change_is_not_a_candidate() {
	let sql = "DEFINE FUNCTION fn::k($a: int) { RETURN $a + 1 }; \
		DEFINE PARAM $p VALUE 1; \
		FOR $i IN [1, 2] { \
			CREATE t SET v = [fn::k(1), $p + 10]; \
			DEFINE FUNCTION OVERWRITE fn::k($a: int) { RETURN $a + 100 }; \
			DEFINE PARAM OVERWRITE $p VALUE 5; \
		}; \
		SELECT VALUE v FROM t ORDER BY v;";
	let ir = bind(sql).await;
	assert!(all_candidates(&ir).is_empty());
}

/// A read of stored data that a write of the statement reaches is never a candidate.
#[tokio::test]
async fn a_read_a_write_of_the_statement_reaches_is_not_a_candidate() {
	let sql = "CREATE t:1 SET n = 1; \
		SELECT VALUE [t:1.n, (UPDATE t:1 SET n += 1 RETURN NONE)] FROM [1, 2];";
	let ir = bind(sql).await;
	for candidate in all_candidates(&ir) {
		let value = ir.op(candidate.root).expect("the root exists").results[0];
		assert!(
			!computes(&ir, value, ACCESS) && !computes(&ir, value, DEREF),
			"{candidate:?} reads `t:1.n`"
		);
	}
}

/// An op in a function body stands in code that runs on its own, so the caller's loop never
/// carries it.
#[tokio::test]
async fn an_op_in_a_function_body_is_not_a_candidate() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let (stmt, _, body, row) = loop_in_statement(&mut ir, &mut facts);
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let kind = OpKind::Logical(LogOp::PermissionFilter {
		guard: GuardKind::Read,
		computed: None,
	});
	let (owner, _) = create(&mut rw, InsertPoint::End(body), kind, &[row], 1);
	let (_, function) = rw
		.create_region(owner, RegionKind::FunctionBody, &ISOLATED_ARGS)
		.expect("the function body region is created");
	let verdict = fallible(&mut rw, InsertPoint::End(function));
	create(&mut rw, InsertPoint::End(function), yield_op(), &[verdict], 0);

	thread_tokens(&mut ir);
	let env = Env::new();
	assert!(candidates(&ir, &mut FactCache::new(), &env.cx(), stmt).is_empty());
}

/// The table permission predicate a record user's statement binds holds a candidate:
/// `$auth = NONE OR …` is the same for every row. A field step off `$auth` fetches the record and
/// derives its computed fields through bodies it does not show, so it is no candidate.
#[cfg(feature = "kv-mem")]
#[tokio::test]
async fn the_bound_table_predicate_holds_the_auth_candidate() {
	let setup = "DEFINE ACCESS user ON DATABASE TYPE RECORD; \
		DEFINE TABLE vault PERMISSIONS FOR select WHERE $auth = NONE OR <int>'x' = 1;";
	let reader = crate::dbs::Session::for_record(
		"test",
		"test",
		"user",
		crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader")),
	);
	let ir = crate::exec::ir::tests_plan_request::bound_as(
		&reader,
		setup,
		"SELECT VALUE id FROM vault;",
	)
	.await
	.ir;
	let found = all_candidates(&ir);
	let in_predicate: Vec<&MemoCandidate> = found
		.iter()
		.filter(|candidate| {
			let region = ir.enclosing_region(candidate.root);
			ir.region(region).is_some_and(|held| held.kind == RegionKind::PermissionFrame)
		})
		.collect();
	let [candidate] = in_predicate.as_slice() else {
		panic!("one candidate in the predicate, found {found:?}");
	};
	let value = ir.op(candidate.root).expect("the root exists").results[0];
	assert!(computes(&ir, value, SESSION_PARAM));
	assert_eq!(kind_of(&ir, candidate.scope).tag(), LOOP);
}

/// A statement offers at most the bound, the largest first.
#[tokio::test]
async fn a_statement_offers_at_most_the_bound() {
	let elements = vec!["<int>'x' = 1"; MAX_MEMOS_PER_STATEMENT + 44].join(", ");
	let ir = bind(&format!("SELECT VALUE [{elements}, $this] FROM [1]")).await;
	let found = all_candidates(&ir);
	assert_eq!(found.len(), MAX_MEMOS_PER_STATEMENT);
	assert!(found.windows(2).all(|pair| pair[0].ops >= pair[1].ops));
}

/// An invariant value inside the table permission predicate is a candidate.
#[tokio::test]
async fn the_table_permission_predicate_holds_candidates() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let (stmt, loop_op, body, row) = loop_in_statement(&mut ir, &mut facts);
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let kind = OpKind::Logical(LogOp::PermissionFilter {
		guard: GuardKind::Read,
		computed: None,
	});
	let (permit, _) = create(&mut rw, InsertPoint::End(body), kind, &[row], 1);
	let (_, frame) = rw
		.create_region(permit, RegionKind::PermissionFrame, &ISOLATED_ARGS)
		.expect("the predicate region is created");
	let verdict = fallible(&mut rw, InsertPoint::End(frame));
	create(&mut rw, InsertPoint::End(frame), yield_op(), &[verdict], 0);

	thread_tokens(&mut ir);
	let env = Env::new();
	let found = candidates(&ir, &mut FactCache::new(), &env.cx(), stmt);
	let [candidate] = found.as_slice() else {
		panic!("one candidate, found {found:?}");
	};
	assert_eq!(candidate.scope, loop_op);
	assert_eq!(candidate.levels, 1);
	assert_eq!(candidate.ops, 2);
	assert_eq!(kind_of(&ir, candidate.root).tag(), BINARY);
}

/// A field guard of a projection layer holds no candidate.
#[tokio::test]
async fn a_field_guard_holds_no_candidate() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let (stmt, _, body, row) = loop_in_statement(&mut ir, &mut facts);
	let mut rw = Rewriter::new(&mut ir, &mut facts);
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
	let layer = OpKind::Logical(LogOp::Project(Box::new(attrs)));
	let (layer, _) = create(&mut rw, InsertPoint::End(body), layer, &[row], 1);
	let (_, guard) = rw
		.create_region(layer, RegionKind::PermissionFrame, &ISOLATED_ARGS)
		.expect("the guard region is created");
	let verdict = fallible(&mut rw, InsertPoint::End(guard));
	create(&mut rw, InsertPoint::End(guard), yield_op(), &[verdict], 0);

	thread_tokens(&mut ir);
	let env = Env::new();
	assert!(candidates(&ir, &mut FactCache::new(), &env.cx(), stmt).is_empty());
}

/// Whether `value` or any operand of the op defining `value` is a block argument (the row).
fn reads_row(ir: &Ir, value: ValueId) -> bool {
	match ir.value(value).map(|data| data.def) {
		Some(ValueDef::BlockArg {
			..
		}) => true,
		Some(ValueDef::OpResult {
			op,
			..
		}) => ir
			.op(op)
			.is_some_and(|data| data.operands.iter().any(|operand| reads_row(ir, *operand))),
		None => false,
	}
}

/// An infallible invariant conjunct moves in front of a total row conjunct written before it,
/// rebuilding the predicate as a lazy `AND` chain.
#[tokio::test]
async fn hoist_leading_reorders_infallible_invariant_ahead_of_total_row_conjunct() {
	let env = Env::new();
	let mut ir = bind("SELECT VALUE $this FROM [1] WHERE $this = 2 AND 1 = 1").await;
	let before = where_chain(&ir);
	assert_eq!(before.len(), 2);
	assert!(reads_row(&ir, before[0]));
	assert!(!reads_row(&ir, before[1]));

	let report = run(&HoistLeadingConjuncts, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 1);
	let after = where_chain(&ir);
	assert_eq!(after.len(), 2);
	assert!(!reads_row(&ir, after[0]));
	assert!(reads_row(&ir, after[1]));
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");

	let again = run(&HoistLeadingConjuncts, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(again.rewrites, 0, "a reordered chain is left as it is");
}

/// With the pass switched off the conjuncts keep their written order.
#[tokio::test]
async fn hoist_leading_off_keeps_written_order() {
	let env = Env::new();
	let cx = env.cx();
	let mut ir = bind("SELECT VALUE $this FROM [1] WHERE $this = 2 AND 1 = 1").await;
	let before = printed(&ir);
	let cfg = PassConfig {
		hoist_leading: false,
		..PassConfig::RELEASE
	};
	let passes: Vec<Box<dyn LoPass>> = vec![Box::new(HoistLeadingConjuncts)];
	let reports = run_passes(
		&passes,
		&mut ir,
		&mut FactCache::new(),
		&NotYetFolder,
		&cx,
		&cfg,
		&mut NoObserver,
	)
	.await
	.expect("the passes run");
	assert_eq!(reports, vec![("hoist_leading_conjuncts", PassReport::default())]);
	assert_eq!(printed(&ir), before);
}

/// An invariant conjunct that cannot raise moves ahead of an earlier total row conjunct, while a
/// subsequent fallible invariant conjunct stays behind the row conjunct so a rejected row never
/// raises its error.
#[tokio::test]
async fn hoist_leading_keeps_a_guarding_conjunct_ahead() {
	let env = Env::new();
	let mut ir =
		bind("SELECT VALUE $this FROM [1] WHERE $this = 2 AND 1 = 0 AND <int>'x' = 1").await;
	let before = where_chain(&ir);
	assert_eq!(before.len(), 3);

	let report = run(&HoistLeadingConjuncts, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 1);
	let after = where_chain(&ir);
	assert_eq!(after.len(), 3);
	assert!(!reads_row(&ir, after[0]));
	assert!(!computes(&ir, after[0], CONVERT));
	assert!(reads_row(&ir, after[1]));
	assert!(computes(&ir, after[2], CONVERT));
	verify_logical(&ir, &mut FactCache::new(), &env).expect("the container verifies");
}

/// A fallible invariant conjunct written after a per-row conjunct stays behind it so a row the
/// per-row conjunct rejects does not raise.
#[tokio::test]
async fn hoist_leading_leaves_a_fallible_conjunct_behind_row_conjunct() {
	let mut ir = bind(FIRST_USE).await;
	let before = printed(&ir);
	let report = run(&HoistLeadingConjuncts, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(printed(&ir), before);
}

/// A row conjunct that may raise (directly or inside a nested region) stays ahead of every
/// invariant conjunct written after it.
#[tokio::test]
async fn hoist_leading_stops_at_a_fallible_row_conjunct() {
	for query in [
		"SELECT VALUE $this FROM [1] WHERE 1 + $this = 2 AND 1 = 1",
		"SELECT VALUE $this FROM [1] WHERE ($this = 2 OR <int>$this = 1) AND 1 = 1",
	] {
		let mut ir = bind(query).await;
		let before = printed(&ir);
		let report = run(&HoistLeadingConjuncts, &mut ir, &PassConfig::RELEASE).await;
		assert_eq!(report.rewrites, 0, "query: {query}");
		assert_eq!(printed(&ir), before, "query: {query}");
	}
}

/// Without a memo op the pass places nothing and counts every candidate.
#[tokio::test]
async fn licm_applies_nothing_without_a_memo_op() {
	let mut ir = bind(FIRST_USE).await;
	let expected = all_candidates(&ir).len();
	assert!(expected > 0);
	let before = printed(&ir);

	let report = run(&Licm, &mut ir, &PassConfig::RELEASE).await;
	assert_eq!(report.rewrites, 0);
	assert_eq!(usize::try_from(report.memo_candidates_unapplied).expect("a small count"), expected);
	assert_eq!(printed(&ir), before);
}

/// Each pass is switched by its own flag of the configuration.
#[test]
fn the_passes_follow_their_switches() {
	assert!(HoistLeadingConjuncts.enabled(&PassConfig::RELEASE));
	assert!(Licm.enabled(&PassConfig::RELEASE));
	assert!(!HoistLeadingConjuncts.enabled(&PassConfig::ALL_OFF));
	assert!(!Licm.enabled(&PassConfig::ALL_OFF));
}
