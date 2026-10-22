//! Plan-time folding: the request a pattern files, the outcome the folder answers and the folder
//! interface.
//!
//! Folding happens only when a decision consumes the value. The evaluator is the VM running the
//! expression in a restricted host; the planner never computes a value itself. A pattern that
//! needs a folded value files a request and is left unrewritten in that pass; the driver awaits
//! every pending request between passes, and the pattern reads the outcome on the next pass.

use std::future::{Future, ready};
use std::pin::Pin;

use super::catalog::EffectCx;
use super::fallibility::{ContextDep, FallibilityClass};
use super::ids::ValueId;
use super::premise::PremiseSet;
use crate::exec::ir::core::ir::Ir;
use crate::exec::ir::expr::ops::FoldedError;
use crate::expr::logical::EffectSet;
use crate::val::Value;

/// A request to fold one value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FoldRequest {
	pub(crate) value: ValueId,
	pub(crate) consumer: FoldConsumer,
}

/// The decision that consumes a folded value. A value is folded only for one of these.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum FoldConsumer {
	DynamicTargetTable,
	IndexChoice,
	BranchChoice,
	TypeFact,
	Unroll,
	AlgebraicRule,
}

/// What folding a value answered.
#[derive(Clone, Debug)]
pub(crate) enum FoldOutcome {
	/// The value is `value`, provided every premise holds. `depth` is the computation depth the
	/// folded evaluation required, which the consumer charges at the site it replaces.
	Constant {
		value: Value,
		premises: PremiseSet,
		depth: u32,
	},
	/// Evaluating the value raises a value-class error: the consumer may replace the
	/// subexpression by a raise of the same error in place.
	Raises {
		error: FoldedError,
		premises: PremiseSet,
		depth: u32,
	},
	NotFoldable(NotFoldable),
}

/// Why a value is not folded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum NotFoldable {
	/// The expression may fail in a class folding does not admit.
	Fallibility(FallibilityClass),
	/// The expression may fail depending on a context no premise covers.
	ContextDep(ContextDep),
	/// The expression has an effect folding does not admit.
	Effect(EffectSet),
	/// The expression reads the statement clock, which is a per-execution operand and never a
	/// literal.
	Clock,
	ReadsRecords,
	NotConstantInputs,
	/// The restricted host refused the named capability.
	HostRefused(&'static str),
	FuelExhausted,
	ResourceError,
	CapabilityDenied,
	/// The encoded result exceeds the fold size bound.
	TooLarge,
	/// No folder is available.
	NotYet,
}

/// Evaluates a value at plan time.
///
/// Two-phase: patterns request through the rewriter, the driver awaits between passes, and
/// patterns read the outcome on the next pass. Async because the evaluator is the VM.
pub(crate) trait PlanTimeFolder: Send + Sync {
	fn fold<'a>(
		&'a self,
		ir: &'a Ir,
		cx: &'a EffectCx<'a>,
		req: FoldRequest,
	) -> Pin<Box<dyn Future<Output = FoldOutcome> + Send + 'a>>;

	/// Evaluates every request of `reqs`, answering one outcome per request in order.
	///
	/// Each outcome is the one [`PlanTimeFolder::fold`] answers for that request alone: the
	/// requests are independent, and no request's answer depends on another's or on the order.
	/// The default folds the requests one after another.
	fn fold_all<'a>(
		&'a self,
		ir: &'a Ir,
		cx: &'a EffectCx<'a>,
		reqs: &'a [FoldRequest],
	) -> Pin<Box<dyn Future<Output = Vec<FoldOutcome>> + Send + 'a>> {
		Box::pin(async move {
			let mut outcomes = Vec::with_capacity(reqs.len());
			for req in reqs {
				outcomes.push(self.fold(ir, cx, *req).await);
			}
			outcomes
		})
	}
}

/// A folder that folds nothing: every request is answered [`NotFoldable::NotYet`], so every
/// consumer applies its rule for an unfolded value.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct NotYetFolder;

impl PlanTimeFolder for NotYetFolder {
	fn fold<'a>(
		&'a self,
		_ir: &'a Ir,
		_cx: &'a EffectCx<'a>,
		_req: FoldRequest,
	) -> Pin<Box<dyn Future<Output = FoldOutcome> + Send + 'a>> {
		Box::pin(ready(FoldOutcome::NotFoldable(NotFoldable::NotYet)))
	}
}
