//! A statement a write performs on its own behalf — a delete's cascades, a
//! view's re-read — as a subroutine: the one group of a program of its own,
//! run to completion each time the write reaches it, over a state kept from
//! one run to the next.
//!
//! The state holds the statement's write state as a block statement's group
//! holds it ([`super::host::run_group`]): the stage programs it lowered for
//! each table and the catalog its write contexts read are kept for as long as
//! the transaction's catalog is unchanged, so each run pays only for its own
//! records. What the writes compile — their write contexts' catalog reads and
//! the nested statements they run — is shared by every state and with the
//! scope that asked for the subroutine, so the levels of a cascade lower each
//! statement they reach once.
//!
//! One run holds one state. A run reached while another is in flight — a
//! cascade that cascades back into the same statement — takes a state of its
//! own, so the states in flight are a stack, one per level, and each level's
//! record, handlers, savepoints and `ONLY` count are its own. A state goes
//! back to be kept only when its run succeeded.
//!
//! A subroutine is held by the operator tree's write scope
//! (`operators::write::scope::NestedPlan`), which lowers it when its write
//! first reaches the statement. No [`Program`] holds one: the states it keeps
//! are run-time state of that scope, not part of a lowered program.

use std::sync::{Arc, Mutex};

use super::interp::PooledState;
use super::program::Program;
use crate::exec::operators::write::scope::KeptContexts;
use crate::exec::{ExecutionContext, FlowResult};
use crate::val::Value;

/// A nested statement lowered as a subroutine; see the module.
pub(crate) struct Subroutine {
	program: Arc<Program>,
	kept: Mutex<Kept>,
}

/// The states kept between runs, what their writes compiled, and the
/// transaction's catalog stamp both were kept under.
struct Kept {
	/// The stamp the states were kept under, `None` before the first run,
	/// when what `contexts` holds is the asking scope's.
	stamp: Option<Option<u64>>,
	states: Vec<PooledState>,
	/// Shared by every state's write, so a level a cascade reaches first
	/// takes what another level compiled.
	contexts: Arc<KeptContexts>,
}

impl std::fmt::Debug for Subroutine {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Subroutine").finish_non_exhaustive()
	}
}

impl Subroutine {
	/// `program`, a statement's one group, as a subroutine whose writes keep
	/// what they compile in `contexts`, or in contexts of its own.
	pub(super) fn new(program: Program, contexts: Option<Arc<KeptContexts>>) -> Self {
		Self {
			program: Arc::new(program),
			kept: Mutex::new(Kept {
				stamp: None,
				states: Vec::new(),
				contexts: contexts.unwrap_or_default(),
			}),
		}
	}

	/// A state for a run, whose write keeps what it compiles in `contexts`.
	fn fresh(
		&self,
		frame: &ExecutionContext,
		contexts: Arc<KeptContexts>,
	) -> FlowResult<PooledState> {
		let mut state = super::host::statement_state(&self.program, frame)?;
		if let Some(spec) = self.program.groups.first().and_then(|group| group.write.as_ref()) {
			let mut write = Box::new(super::write::WriteState::new(spec, frame)?);
			write.keeping_in(contexts);
			write.group = Some(0);
			state.group_writes[0] = Some(write);
		}
		Ok(state)
	}

	/// Run the statement in `frame` and answer its rows.
	pub(crate) async fn run(&self, frame: &ExecutionContext) -> FlowResult<Vec<Value>> {
		let stamp = frame.try_txn().map(|txn| txn.catalog_stamps());
		let (state, contexts) = {
			let mut kept = self.kept.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
			match kept.stamp {
				Some(kept_under) if kept_under != stamp => {
					kept.states.clear();
					kept.contexts = Arc::default();
				}
				_ => {}
			}
			kept.stamp = Some(stamp);
			(kept.states.pop(), Arc::clone(&kept.contexts))
		};
		let mut state = match state {
			Some(state) => state,
			None => self.fresh(frame, contexts)?,
		};
		state.permissions_bypassed = frame.root().skip_fetch_perms;
		let program = Arc::clone(&self.program);
		let running =
			super::dispatch::with_dispatch(&program, frame).unwrap_or_else(|| frame.clone());
		let (state, rows, ran) = crate::exec::trampoline::nest(async move {
			let (rows, ran) = super::host::run_group(&program, 0, &mut state, &running).await;
			(state, rows, ran)
		})
		.await;
		ran?;
		// A run that changed the catalog leaves its state behind.
		let after = frame.try_txn().map(|txn| txn.catalog_stamps());
		let mut kept = self.kept.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
		if after == stamp && kept.stamp == Some(stamp) {
			kept.states.push(state);
		}
		Ok(rows)
	}
}
