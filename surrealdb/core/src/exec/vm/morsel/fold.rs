//! The terms of an aggregate's float folds, kept by each morsel for the merge to fold in order.
//!
//! The state of a sum, a mean, a variance or a deviation of floats depends on the order its terms
//! are folded in, so a morsel's state folded into the statement's is not the state the serial run
//! reaches. A pipeline whose plan names accumulators to fold in order
//! ([`MorselPlan::ordered_fold`](crate::exec::planner::placement::MorselPlan::ordered_fold)) has
//! each morsel keep, for each of them, the numbers its rows hand over and the first value it
//! cannot fold ([`Groups::log_folds`]). The morsel's accumulators for them stay empty, and every
//! other accumulator of the aggregate folds in the morsel and combines in the merge. The merge
//! takes the morsels in the order of the serial run and folds the kept values into the
//! statement's accumulators as it takes each morsel ([`Groups::absorb`]), so an accumulator folds
//! the values of every row in the order of the serial run and reaches the serial run's state, bit
//! for bit.
//!
//! The terms are kept in blocks, each allocated at its full size before the rows it holds the
//! terms of run, so a term is never moved once kept. A morsel over a key range opens a block for
//! each chunk it reads and charges its bytes to the run's lease as it opens them; a batch opens
//! one block for its rows, charged with the batch's other bytes. What a morsel has charged stays
//! charged until the merge has folded its terms.
//!
//! The kept terms count with the output when a morsel decides whether it holds as much as its
//! window allows ([`Fold::logged_bytes`]), so the morsel that is not at the front parks before it
//! holds more than a window of them.

use std::any::Any;
use std::sync::Arc;

use super::super::interp::{Groups, State};
use super::super::program::Pipeline;
use super::merge::BodyBreakers;
use super::run::MorselResult;
use crate::exec::planner::placement::{MorselPlan, Parallel};

/// The accumulators of an aggregate whose terms the morsels of a pipeline keep.
#[derive(Clone)]
pub(super) struct Fold {
	/// The aggregates the body fills, whose groups keep the terms.
	aggregates: Arc<[u16]>,
	/// The places of the accumulators, among the calls of an aggregate's functions, that keep
	/// terms.
	slots: Arc<[usize]>,
}

impl Fold {
	/// The fold `pipeline`'s plan states for the aggregates `breakers` names, or `None` when its
	/// morsels fold every accumulator themselves.
	pub(super) fn of(pipeline: &Pipeline, breakers: &BodyBreakers) -> Option<Self> {
		let Parallel::Morsels(MorselPlan {
			ordered_fold,
			..
		}) = &pipeline.parallel
		else {
			return None;
		};
		if ordered_fold.is_empty() {
			return None;
		}
		debug_assert!(
			!breakers.aggregates.is_empty(),
			"terms are kept for a body with no aggregate"
		);
		Some(Self {
			aggregates: breakers.aggregates.as_slice().into(),
			slots: ordered_fold.as_slice().into(),
		})
	}

	/// Make the groups of `state`, a morsel's, keep the terms of the accumulators the plan names.
	pub(super) fn log(&self, state: &mut State) {
		for aggregate in self.aggregates.iter() {
			if let Some(groups) = state.groups[*aggregate as usize].as_mut() {
				groups.log_folds(&self.slots);
			}
		}
	}

	/// Open a block for the terms of the next `numbers` rows of `state`, and answer the bytes it
	/// reserves.
	pub(super) fn open_block(&self, state: &mut State, numbers: usize) -> usize {
		let mut reserved = 0;
		for aggregate in self.aggregates.iter() {
			if let Some(groups) = state.groups[*aggregate as usize].as_mut() {
				reserved += groups.open_log_block(numbers);
			}
		}
		reserved
	}

	/// The bytes a block of `numbers` terms for each kept accumulator reserves.
	pub(super) fn block_bytes(&self, numbers: usize) -> usize {
		Groups::block_bytes(self.slots.len(), numbers).saturating_mul(self.aggregates.len())
	}

	/// What the terms `state` keeps for the merge hold, in bytes.
	pub(super) fn logged_bytes(&self, state: &State) -> usize {
		let mut kept = 0;
		for aggregate in self.aggregates.iter() {
			if let Some(groups) = state.groups[*aggregate as usize].as_ref() {
				kept += groups.logged_bytes();
			}
		}
		kept
	}

	/// Keep `hold`, what charges the memory of the terms `state` keeps, until the merge has folded
	/// them.
	pub(super) fn hold(&self, state: &mut State, hold: Box<dyn Any + Send + Sync>) {
		Self::hold_in(&self.aggregates, &mut state.groups, hold);
	}

	/// [`Self::hold`], for the terms a finished morsel hands the merge.
	pub(super) fn hold_result(&self, result: &mut MorselResult, hold: Box<dyn Any + Send + Sync>) {
		Self::hold_in(&self.aggregates, &mut result.groups, hold);
	}

	fn hold_in(
		aggregates: &[u16],
		groups: &mut [Option<Groups>],
		hold: Box<dyn Any + Send + Sync>,
	) {
		for aggregate in aggregates {
			if let Some(groups) = groups[*aggregate as usize].as_mut() {
				groups.hold_log(hold);
				return;
			}
		}
	}
}
