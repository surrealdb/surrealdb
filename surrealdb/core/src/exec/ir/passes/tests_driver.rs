//! Tests of the loop optimiser's driver and of the rehoming helpers.

use std::sync::{Arc, Mutex};

use super::driver::{
	LoCx, LoPass, NoObserver, PassConfig, PassReport, PassRun, run_lo, run_passes,
};
use super::reg::{PHYS_CSE, lo_passes, register};
use super::rehome::{RehomeMode, and_chain, rebuild_and_chain, rehome_tree};
use super::tests_support::{
	Env, all_ops, bind, create, empty_ir, literal, ops_of_tag, verify_logical,
};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::print::{PrintOpts, print};
use crate::exec::ir::core::rewriter::{InsertPoint, Provenance, Rewriter};
use crate::exec::ir::error::IrError;
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::conversion::{EliminationReason, Phase};
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::iface::ids::ValueId;
use crate::exec::ir::op_kind::{OpKind, OpTag};
use crate::exec::ir::registry::Registry;
use crate::expr::BinaryOperator;

/// The order the loop optimiser runs its passes in. The registered passes must appear in this
/// list as a subsequence, in the same order. The test checks this as passes are added.
const LO_ORDER: [&str; 13] = [
	"specialise_guards",
	"inline_calls",
	"fold",
	"dce",
	"cse",
	"push_into_subquery",
	"push_below_layer",
	"push_below_permission",
	"loop_unroll",
	"unswitch",
	"hoist_leading_conjuncts",
	"licm",
	"dce",
];

/// A pass that records that it ran and reports one rewrite, enabled by `enabled`.
struct Recorder {
	name: &'static str,
	enabled: fn(&PassConfig) -> bool,
	log: Arc<Mutex<Vec<&'static str>>>,
}

impl LoPass for Recorder {
	fn name(&self) -> &'static str {
		self.name
	}

	fn enabled(&self, cfg: &PassConfig) -> bool {
		(self.enabled)(cfg)
	}

	fn run<'a, 'b>(&'a self, _cx: &'a mut LoCx<'b>) -> PassRun<'a> {
		self.log.lock().expect("the log is not poisoned").push(self.name);
		PassRun::done(Ok(PassReport {
			rewrites: 1,
			..PassReport::default()
		}))
	}
}

fn recorders(log: &Arc<Mutex<Vec<&'static str>>>) -> Vec<Box<dyn LoPass>> {
	let on = |cfg: &PassConfig| cfg.fold;
	let off = |_: &PassConfig| false;
	vec![
		Box::new(Recorder {
			name: "first",
			enabled: on,
			log: Arc::clone(log),
		}),
		Box::new(Recorder {
			name: "skipped",
			enabled: off,
			log: Arc::clone(log),
		}),
		Box::new(Recorder {
			name: "last",
			enabled: on,
			log: Arc::clone(log),
		}),
	]
}

fn printed(ir: &Ir) -> String {
	print(ir, ir.top(), PrintOpts::default())
}

#[test]
fn the_registered_passes_follow_the_lo_order() {
	let names: Vec<&str> = lo_passes().iter().map(|pass| pass.name()).collect();
	let mut order = LO_ORDER.iter();
	for name in &names {
		assert!(order.any(|expected| expected == name), "{name} is out of order in {names:?}");
	}
	let mut registry = Registry::empty();
	register(&mut registry);
	let registered: Vec<&str> = registry.passes.0.iter().map(|pass| pass.name()).collect();
	let mut expected = names.clone();
	expected.push(PHYS_CSE);
	assert_eq!(registered, expected);
	let (physical, logical) = registry.passes.0.split_last().expect("passes are registered");
	assert_eq!(physical.phase(), Phase::Physical);
	assert!(logical.iter().all(|pass| pass.phase() == Phase::Logical));
}

#[test]
fn pass_config_release_matches_the_cut_line() {
	let release = PassConfig::RELEASE;
	assert!(release.specialise_guards && release.fold && release.dce && release.cse);
	assert!(release.hoist_leading && release.licm && release.push_into_subquery);
	assert!(!release.dce_certain_error && release.push_below_layer);
	assert!(!release.push_below_permission && !release.unswitch);
	assert!(!release.unroll && !release.fold_phase_b);
	assert_ne!(PassConfig::ALL_ON, PassConfig::ALL_OFF);
}

#[tokio::test]
async fn run_lo_runs_passes_in_order() {
	let env = Env::new();
	let cx = env.cx();
	let mut ir = bind("RETURN 1").await;
	let mut facts = FactCache::new();
	let phase = ir.phase();
	let log = Arc::new(Mutex::new(Vec::new()));
	let passes = recorders(&log);
	let reports = run_passes(
		&passes,
		&mut ir,
		&mut facts,
		&NotYetFolder,
		&cx,
		&PassConfig::RELEASE,
		&mut NoObserver,
	)
	.await
	.expect("the passes run");
	assert_eq!(*log.lock().expect("the log is not poisoned"), ["first", "last"]);
	let names: Vec<&str> = reports.iter().map(|(name, _)| *name).collect();
	assert_eq!(names, ["first", "skipped", "last"]);
	assert_eq!(reports[0].1.rewrites, 1);
	assert_eq!(reports[1].1, PassReport::default());
	assert_eq!(ir.phase(), phase);
}

#[tokio::test]
async fn a_disabled_pass_touches_nothing() {
	let env = Env::new();
	let cx = env.cx();
	let mut ir = bind("SELECT * FROM t WHERE a = 1 AND b = 2").await;
	let before = printed(&ir);
	let mut facts = FactCache::new();
	let reports =
		run_lo(&mut ir, &mut facts, &NotYetFolder, &cx, &PassConfig::ALL_OFF, &mut NoObserver)
			.await
			.expect("the run answers");
	assert!(reports.iter().all(|(_, report)| *report == PassReport::default()));
	assert_eq!(printed(&ir), before);

	let log = Arc::new(Mutex::new(Vec::new()));
	let passes = recorders(&log);
	run_passes(
		&passes,
		&mut ir,
		&mut facts,
		&NotYetFolder,
		&cx,
		&PassConfig::ALL_OFF,
		&mut NoObserver,
	)
	.await
	.expect("the passes run");
	assert!(log.lock().expect("the log is not poisoned").is_empty());
	assert_eq!(printed(&ir), before);
}

fn add() -> OpKind {
	OpKind::Expr(ExprOp::Binary {
		op: BinaryOperator::Add,
	})
}

/// `(1 + 2) * 3`'s sum, moved after the literal `3`, keeps its operands, the user's operand and
/// the logical ids of the ops it replaces.
#[tokio::test]
async fn rehome_moves_a_tree_and_keeps_provenance() {
	let env = Env::new();
	let mut ir = bind("RETURN (1 + 2) * 3").await;
	let mut facts = FactCache::new();
	let binaries = ops_of_tag(&ir, OpTag::Expr(ExprTag::Binary));
	let [sum, product] = binaries.as_slice() else {
		panic!("the expression binds two binary ops, found {binaries:?}");
	};
	let (sum, product) = (*sum, *product);
	let literals = ops_of_tag(&ir, OpTag::Expr(ExprTag::Literal)).len();
	let eliminated = ir.eliminated().len();

	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let moved = rehome_tree(
		&mut rw,
		sum,
		InsertPoint::Before(product),
		RehomeMode::Move,
		EliminationReason::DeadPure,
	)
	.expect("the tree moves");
	assert_eq!(moved.len(), 1);

	assert!(ir.op(sum).is_none(), "the original root is erased");
	assert_eq!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Literal)).len(), literals);
	assert_eq!(ir.eliminated().len(), eliminated, "no logical id is lost");
	let product = ir.op(product).expect("the user stays");
	assert_eq!(product.operands[0], moved[0], "the user reads the moved result");
	let new_sum = ops_of_tag(&ir, OpTag::Expr(ExprTag::Binary))
		.into_iter()
		.find(|op| ir.op(*op).is_some_and(|data| data.results.contains(&moved[0])))
		.expect("the moved root is live");
	match &ir.op(new_sum).expect("the op exists").provenance {
		Provenance::From(ids) => assert!(ids.contains(&sum), "provenance names the original"),
		Provenance::Bound => panic!("a re-created op stands for the op it replaces"),
	}
	verify_logical(&ir, &mut facts, &env).expect("the container verifies");
}

/// A tree that reads a value standing at or after the insert point is refused, and the container
/// is left as it was.
#[test]
fn rehome_refuses_an_escaping_tree() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (first, x) = create(&mut rw, InsertPoint::End(block), literal(1), &[], 1);
	let (_, y) = create(&mut rw, InsertPoint::End(block), literal(2), &[], 1);
	let (sum, z) = create(&mut rw, InsertPoint::End(block), add(), &[x[0], y[0]], 1);
	let _ = create(&mut rw, InsertPoint::End(block), add(), &[x[0], x[0]], 1);
	let before = printed(&ir);
	let count = all_ops(&ir).len();

	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let refused = rehome_tree(
		&mut rw,
		sum,
		InsertPoint::Before(first),
		RehomeMode::Move,
		EliminationReason::DeadPure,
	);
	assert!(matches!(refused, Err(IrError::Refused("rehome escapes"))), "{refused:?}");
	assert_eq!(all_ops(&ir).len(), count);
	assert_eq!(printed(&ir), before);
	assert!(ir.uses(z[0]).is_empty());
}

/// Reversing the conjuncts of `a AND b AND c` through `and_chain` and `rebuild_and_chain` yields a
/// chain of the same three conjuncts in the new order, keeping the lazy ones lazy.
#[tokio::test]
async fn and_chain_round_trips() {
	let env = Env::new();
	let mut ir = bind("RETURN (1 + 1) AND (2 + 2) AND (3 + 3)").await;
	let mut facts = FactCache::new();
	let gates = ops_of_tag(&ir, OpTag::Expr(ExprTag::ShortCircuit));
	let outer = *gates.last().expect("the chain binds short-circuits");
	let root: ValueId = ir.op(outer).expect("the op exists").results[0];
	let conjuncts = and_chain(&ir, root);
	assert_eq!(conjuncts.len(), 3);
	let sums = ops_of_tag(&ir, OpTag::Expr(ExprTag::Binary)).len();

	let reversed: Vec<ValueId> = conjuncts.iter().rev().copied().collect();
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let rebuilt = rebuild_and_chain(&mut rw, InsertPoint::Before(outer), &reversed)
		.expect("the chain rebuilds");
	rw.replace_all_uses(root, rebuilt).expect("the uses move");
	for gate in gates.into_iter().rev() {
		rw.erase(gate, EliminationReason::DeadPure).expect("the old chain is erased");
	}

	let again = and_chain(&ir, rebuilt);
	assert_eq!(again.len(), 3);
	assert_eq!(ops_of_tag(&ir, OpTag::Expr(ExprTag::Binary)).len(), sums);
	verify_logical(&ir, &mut facts, &env).expect("the container verifies");
}

/// Statements from the language corpus. The loop optimiser's passes must not change their
/// answers. Each entry is a request of one or more statements.
const CORPUS: [&str; 17] = [
	"SELECT VALUE $this FROM [1] WHERE $this = 2 AND <int>'x' = 1;",
	"SELECT VALUE $this FROM [1] WHERE $this = 2 AND (1 + 'a') = 1;",
	"LET $p = 'x'; SELECT VALUE $this FROM [1] WHERE $this = 2 AND <int>$p = 1;",
	"SELECT VALUE $this FROM [1, 2] WHERE $this = 2 AND <int>'x' = 1;",
	"SELECT VALUE $this FROM [] WHERE <int>'x' = 1; SELECT VALUE [$this, <int>'x'] FROM [];",
	"SELECT VALUE IF $this = 2 { <int>'x' } ELSE { 0 } FROM [1];",
	"SELECT count(<int>'z') AS c FROM [{a:1}] GROUP ALL;",
	"SELECT count(1 + 'x') AS c FROM [{a:1}] GROUP ALL;",
	"DEFINE PARAM $p VALUE 1; FOR $i IN [1, 2] { CREATE t SET v = [$p + 10]; DEFINE PARAM OVERWRITE $p VALUE 5; };",
	"DEFINE PARAM $p VALUE 0; FOR $i IN [1, 2, 3] { DEFINE PARAM OVERWRITE $p VALUE $i; CREATE t SET v = $p + 10 };",
	"DEFINE PARAM $q VALUE 1; SELECT VALUE [$q + 1, (DEFINE PARAM OVERWRITE $q VALUE $q + 5), $q + 1] FROM [1, 2];",
	"DEFINE SEQUENCE s; SELECT VALUE sequence::nextval('s') FROM [1, 2, 3];",
	"SELECT VALUE rand::uuid() FROM [1, 2];",
	"LET $p = 'x'; FOR $i IN [1, 2] { IF $i = 3 { RETURN <int>$p } };",
	"SELECT VALUE (SELECT VALUE count() FROM t GROUP ALL)[0] FROM [1, 2, 3];",
	"RETURN [0 AND false, '' AND false, NONE AND false, 1 OR true];",
	"DEFINE SEQUENCE s; DEFINE FIELD c ON doc COMPUTED sequence::nextval('s'); SELECT VALUE { LET $u = c; 1 } FROM doc;",
];

/// Running the loop optimiser with every pass off and with the release set both leave a container
/// that verifies, for each statement of the corpus. That the two answer alike is checked where
/// the language tests run.
#[tokio::test]
async fn pass_config_off_equals_on() {
	let env = Env::new();
	let cx = env.cx();
	for sql in CORPUS {
		for cfg in [PassConfig::ALL_OFF, PassConfig::RELEASE] {
			let mut ir = bind(sql).await;
			let mut facts = FactCache::new();
			run_lo(&mut ir, &mut facts, &NotYetFolder, &cx, &cfg, &mut NoObserver)
				.await
				.unwrap_or_else(|error| panic!("{sql} runs under {cfg:?}: {error}"));
			let mut facts = FactCache::new();
			verify_logical(&ir, &mut facts, &env)
				.unwrap_or_else(|error| panic!("{sql} verifies under {cfg:?}: {error:?}"));
		}
	}
}
