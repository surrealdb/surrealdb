//! The e-graph's place in a build without the `egraph` feature.
//!
//! The optimiser and the `egg` crate it is built on are left out. Every statement is planned as
//! though the e-graph declined it: the plan the binder, the loop passes and the physical boundary
//! leave is lowered as it stands, and [`plan_statement`] answers an outcome that skips the
//! statement with [`SkipReason::NotBuilt`]. The plain data and the predicates the rest of the
//! planner shares with the e-graph ([`decl`], [`guards`], [`SeedHost`]) are the same source files
//! the full module builds from.

#[expect(dead_code, reason = "the e-graph that reads the rule declarations is left out")]
#[path = "egraph/decl.rs"]
pub(crate) mod decl;
#[path = "egraph/guards.rs"]
pub(crate) mod guards;
#[expect(dead_code, reason = "the e-graph that runs and reads these outcomes is left out")]
#[path = "egraph/outcome.rs"]
mod outcome;

pub(crate) use self::outcome::{EgraphOutcome, SeedFuture, SeedHost, SkipReason};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::iface::catalog::EffectCx;
use crate::exec::ir::iface::ids::OpId;

/// What planning a statement reads in a build with the e-graph: the effect context and the physical
/// layer's answers for seeded mode. Neither is read here.
#[expect(dead_code, reason = "the e-graph that reads the context is left out")]
pub(crate) struct PlanStmtCx<'a> {
	pub(crate) effects: EffectCx<'a>,
	pub(crate) seed: Option<&'a dyn SeedHost>,
}

impl<'a> PlanStmtCx<'a> {
	/// The context a request plans one statement with.
	pub(crate) fn for_request(effects: EffectCx<'a>) -> PlanStmtCx<'a> {
		PlanStmtCx {
			effects,
			seed: None,
		}
	}
}

/// Leaves the statement `stmt` of `ir` exactly as it is and answers that the e-graph did not run
/// on it.
pub(crate) async fn plan_statement(
	_ir: &mut Ir,
	_facts: &mut FactCache,
	_stmt: OpId,
	_cx: &PlanStmtCx<'_>,
) -> EgraphOutcome {
	EgraphOutcome::skipped(SkipReason::NotBuilt)
}
