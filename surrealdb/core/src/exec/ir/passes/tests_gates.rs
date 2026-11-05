//! Tests of the census gates: each gated pass is skipped on a container without its tags, a run
//! forced anyway rewrites nothing, and the gated run of the production passes prints what the
//! ungated run prints.

use super::driver::{LoCx, LoPass, NoObserver, PassConfig, PassReport, run_lo, ungated};
use super::inline::InlineCalls;
use super::licm::{HoistLeadingConjuncts, Licm};
use super::loop_unroll::LoopUnroll;
use super::perm_where::{PushBelowLayer, PushBelowPermission, SpecialiseGuards};
use super::push_subquery::PushIntoSubquery;
use super::tests_support::{Env, bind, create, empty_ir};
use super::unswitch::Unswitch;
use crate::exec::ir::core::census::{Census, Flag};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::print::{PrintOpts, print};
use crate::exec::ir::core::rewriter::{InsertPoint, Rewriter};
use crate::exec::ir::expr::ops::{ExprOp, ExprTag};
use crate::exec::ir::iface::fold::NotYetFolder;
use crate::exec::ir::iface::region_kind::RegionKind;
use crate::exec::ir::iface::types::TypeKnowledge;
use crate::exec::ir::logical::ops::LogTag;
use crate::exec::ir::op_kind::OpKind;
use crate::exec::ir::phys::ops::PhysTag;
use crate::expr::Function;

fn printed(ir: &Ir) -> String {
	print(ir, ir.top(), PrintOpts::default())
}

/// Runs `pass` over `ir` whatever the census says.
async fn run_anyway(pass: &dyn LoPass, ir: &mut Ir) -> (PassReport, FactCache) {
	let env = Env::new();
	let effects = env.cx();
	let mut facts = FactCache::new();
	let mut observer = NoObserver;
	let cfg = PassConfig::ALL_ON;
	let report = {
		let mut cx = LoCx {
			ir,
			facts: &mut facts,
			folder: &NotYetFolder,
			effects: &effects,
			cfg: &cfg,
			observer: &mut observer,
		};
		pass.run(&mut cx).await.expect("the pass runs")
	};
	(report, facts)
}

/// Every gated pass, with the statement none of whose tags the pass needs.
async fn assert_gate_is_sound(pass: &dyn LoPass, sql: &str) {
	let mut ir = bind(sql).await;
	assert!(!pass.needs(&Census::of(&ir)), "{} is gated on `{sql}`", pass.name());
	let before = printed(&ir);
	let (report, facts) = run_anyway(pass, &mut ir).await;
	assert_eq!(report, PassReport::default(), "{} on `{sql}`", pass.name());
	assert_eq!(printed(&ir), before, "{} on `{sql}`", pass.name());
	assert_eq!(
		format!("{facts:?}"),
		format!("{:?}", FactCache::new()),
		"{} touched the facts on `{sql}`",
		pass.name()
	);
}

#[tokio::test]
async fn a_pass_without_its_tags_is_gated_and_rewrites_nothing() {
	for sql in ["RETURN 1", "UPDATE t:1 SET a = 1", "SELECT * FROM t:1"] {
		let loopless = !Census::of(&bind(sql).await).has(LogTag::Loop);
		for pass in [
			&InlineCalls as &dyn LoPass,
			&SpecialiseGuards,
			&PushBelowLayer,
			&PushBelowPermission,
			&PushIntoSubquery,
		] {
			assert_gate_is_sound(pass, sql).await;
		}
		if loopless {
			for pass in [&LoopUnroll as &dyn LoPass, &Unswitch, &Licm, &HoistLeadingConjuncts] {
				assert_gate_is_sound(pass, sql).await;
			}
		}
	}
	// A loop and a filter without a permission, a layer or a subquery.
	let sql = "SELECT * FROM t WHERE a = 1";
	for pass in [
		&InlineCalls as &dyn LoPass,
		&SpecialiseGuards,
		&PushBelowLayer,
		&PushBelowPermission,
		&PushIntoSubquery,
	] {
		assert_gate_is_sound(pass, sql).await;
	}
}

#[tokio::test]
async fn the_gates_open_on_the_tags_they_need() {
	let census = Census::of(&bind("SELECT * FROM (SELECT * FROM t WHERE a = 1) WHERE b = 2").await);
	assert!(PushIntoSubquery.needs(&census));
	assert!(census.has(LogTag::Subquery) && census.has(LogTag::Filter));
	let census = Census::of(&bind("SELECT * FROM t WHERE a = 1").await);
	assert!(census.has(LogTag::Loop));
	assert!(LoopUnroll.needs(&census) && Unswitch.needs(&census) && Licm.needs(&census));
	assert!(HoistLeadingConjuncts.needs(&census));
}

/// A call that owns a `FunctionBody` region opens the inlining gate.
#[test]
fn a_call_with_a_body_opens_the_inline_gate() {
	let mut ir = empty_ir();
	let mut facts = FactCache::new();
	let block = ir.region(ir.top()).expect("the top region exists").blocks[0];
	let mut rw = Rewriter::new(&mut ir, &mut facts);
	let (call, _) = create(
		&mut rw,
		InsertPoint::End(block),
		OpKind::Expr(ExprOp::call(Function::Custom("k".into()))),
		&[],
		1,
	);
	assert!(!InlineCalls.needs(&Census::of(rw.ir())));
	let args = [TypeKnowledge::Unknown, TypeKnowledge::Unknown];
	rw.create_region(call, RegionKind::FunctionBody, &args).expect("the body is created");
	let census = Census::of(&ir);
	assert!(census.flag(Flag::FunctionBodyCall));
	assert!(InlineCalls.needs(&census));
}

#[tokio::test]
async fn the_census_states_tags_and_flags() {
	let census = Census::of(&bind("RETURN 1").await);
	assert!(census.has(ExprTag::Literal));
	assert!(!census.has(LogTag::Loop) && !census.has(PhysTag::TableScan));
	assert!(!census.flag(Flag::StoredClause) && !census.flag(Flag::PermissionFrame));
	let wide = Census::of(&bind("SELECT * FROM t WHERE a = 1").await);
	assert!(wide.covers(&wide) && !census.covers(&wide));
}

/// The statements whose production run the gated and ungated drivers must print alike.
const CORPUS: [&str; 7] = [
	"RETURN 1",
	"SELECT * FROM t WHERE a = 1",
	"SELECT * FROM t WHERE id = t:1",
	"UPDATE t:1 SET a = 1",
	"FOR $i IN [1, 2] { CREATE t SET v = $i };",
	"SELECT * FROM (SELECT * FROM t WHERE a = 1) WHERE b = 2",
	"SELECT VALUE (SELECT VALUE count() FROM t GROUP ALL)[0] FROM [1, 2, 3];",
];

#[tokio::test]
async fn gated_and_ungated_runs_print_alike() {
	let env = Env::new();
	let cx = env.cx();
	for cfg in [PassConfig::PRODUCTION, PassConfig::ALL_ON] {
		for sql in CORPUS {
			let mut gated = bind(sql).await;
			let reports = run_lo(
				&mut gated,
				&mut FactCache::new(),
				&NotYetFolder,
				&cx,
				&cfg,
				&mut NoObserver,
			)
			.await
			.expect("the gated run answers");
			let mut forced = bind(sql).await;
			let forced_reports = {
				let _guard = ungated::force();
				run_lo(
					&mut forced,
					&mut FactCache::new(),
					&NotYetFolder,
					&cx,
					&cfg,
					&mut NoObserver,
				)
				.await
				.expect("the ungated run answers")
			};
			assert_eq!(printed(&gated), printed(&forced), "{sql}");
			assert_eq!(reports, forced_reports, "{sql}");
		}
	}
}
