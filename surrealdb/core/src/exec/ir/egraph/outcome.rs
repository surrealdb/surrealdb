//! What planning a statement through the e-graph answers, and what seeded mode asks of the
//! physical layer.
//!
//! Plain data and a trait over plain data: it names no `egg` type, so a build without the `egraph`
//! feature carries it, and a statement it plans is answered [`SkipReason::NotBuilt`].

use std::future::Future;
use std::pin::Pin;

use surrealdb_strand::TableName;

use super::decl::{CostHook, OrderSig, PhysTableFacts};
use crate::exec::ir::core::facts::FactCache;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::core::pattern::Limit;
use crate::exec::ir::error::IrError;
use crate::exec::ir::iface::ids::{OpId, SnapshotId, ValueId};
use crate::exec::ir::phys::ops::PhysOp;
use crate::expr::With;

/// What planning one statement did: the counters and the reason for a skip or a fallback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EgraphOutcome {
	/// `Err` when the e-graph did not run, with why.
	pub(crate) ran: Result<(), SkipReason>,
	/// The limit the run stopped at, or `None` when every round saturated.
	pub(crate) stopped_by: Option<Limit>,
	pub(crate) mode: Option<Mode>,
	pub(crate) rounds: usize,
	pub(crate) iterations: usize,
	pub(crate) nodes: u32,
	/// A candidate other than the incumbent was written back.
	pub(crate) chose_alternative: bool,
	/// Why the statement keeps its unrewritten plan after the e-graph ran.
	pub(crate) fallback: Option<FallbackReason>,
	/// The unions the guards refused.
	pub(crate) union_refused: u32,
}

impl EgraphOutcome {
	/// Reports the run to the compile timers; a statement the e-graph did not run on reports
	/// nothing.
	#[cfg(feature = "compile-timers")]
	pub(crate) fn report(&self) {
		if self.ran.is_ok() {
			crate::exec::ir::timers::compile_egraph!(self.nodes, self.iterations);
		}
	}

	/// The outcome of a statement the e-graph did not run on.
	pub(crate) fn skipped(reason: SkipReason) -> Self {
		Self {
			ran: Err(reason),
			stopped_by: None,
			mode: None,
			rounds: 0,
			iterations: 0,
			nodes: 0,
			chose_alternative: false,
			fallback: None,
			union_refused: 0,
		}
	}
}

/// Why the e-graph did not run on a statement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum SkipReason {
	/// The statement's program was served from a cache. Reserved: no program cache produces it.
	CacheHit,
	NoChoicePoint,
	NoApplicableRule,
	NeedsTotalOrder,
	/// A test forced the e-graph off.
	Forced,
	/// A hook the run needs has no implementation.
	NotYet,
	/// The build leaves the e-graph out (the `egraph` feature).
	#[cfg(not(feature = "egraph"))]
	NotBuilt,
}

/// Why a statement keeps its unrewritten plan after the e-graph ran on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FallbackReason {
	NoPhysRoot,
	SeedConversionFailed,
	WritebackAborted,
	WallBeforeSeeding,
	MatchCapHit,
	LogicalVerifyFailed,
}

/// What an e-graph run holds and writes back.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Mode {
	/// Logical rewrites only; the statement stays logical.
	LogicalOnly,
	/// Logical and physical nodes in one graph, seeded from the default conversion.
	Seeded,
}

/// Whether the e-graph runs on a statement, and in which mode.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Gate {
	Run(Mode),
	Skip(SkipReason),
}

/// The boxed future of a [`SeedHost`] call that reads the physical layer.
pub(crate) type SeedFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, IrError>> + Send + 'a>>;

/// What seeded mode asks of the physical layer: the facts of the row sources, the conversion of a
/// statement copy, the delivery order of a physical op, whether a row producer's order may change,
/// and the cost function extraction minimises.
///
/// A host answers for the catalog version the planning session is bound to, and every future it
/// returns is `Send`, so the planner may hold the e-graph across the await.
pub(crate) trait SeedHost: Send + Sync {
	/// The physical facts of each of `sources`, a table with its `WITH` clause and the snapshot it
	/// reads. A source with no admissible access candidate has no entry.
	fn table_facts<'a>(
		&'a self,
		sources: &'a [(TableName, Option<With>, SnapshotId)],
	) -> SeedFuture<'a, Vec<PhysTableFacts>>;

	/// Converts the statement copy `copy` to the physical dialect in place, and answers the
	/// conversion's replacement log: each `(from, to)` is a value of the copy and the value that
	/// replaced it.
	fn convert_copy<'a>(
		&'a self,
		copy: &'a mut Ir,
		facts: &'a mut FactCache,
	) -> SeedFuture<'a, Vec<(ValueId, ValueId)>>;

	/// The order `op` delivers its rows in, given the order of its row input.
	fn order_sig(&self, op: &PhysOp, input: OrderSig) -> OrderSig;

	/// Whether the row producer `source` may change the order it delivers rows in: the nearest
	/// order-sensitive consumer above it orders by a total key, and every op between them is
	/// infallible and deterministic.
	fn order_change_admissible(&self, ir: &Ir, facts: &mut FactCache, source: OpId) -> bool;

	/// The cost function extraction minimises over physical plans.
	fn cost(&self) -> &dyn CostHook;
}
