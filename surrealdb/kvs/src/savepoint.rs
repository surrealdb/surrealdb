//! Savepoint bookkeeping for backends built on a native engine savepoint.

use crate::err::{Error, Result};

/// Tracks the engine savepoints backing each open logical savepoint.
///
/// The engines behind the local backends expose only "set a savepoint" and
/// "roll back to the most recent savepoint", where rolling back consumes the
/// savepoint it reverts to. Neither offers a release operation — a way to
/// discard a savepoint marker while keeping the writes made after it.
///
/// Release still has to leave those writes undoable by the enclosing
/// savepoint, so a released savepoint stays on the engine's stack and its
/// unwind cost transfers to the scope that encloses it: rolling that scope
/// back reverts the engine once per savepoint the scope has absorbed, walking
/// the engine's stack down to the right point in one go.
///
/// Each element counts the engine savepoints one open scope must unwind, with
/// the innermost scope last.
///
/// The accounting invariant is `scopes.iter().sum() == live engine savepoints`:
/// [`Self::open`] adds one to each side, [`Self::release`] moves a count
/// between scopes without changing the total (or, at the outermost scope, drops
/// it and leaves the engine savepoints behind), and [`Self::take`] removes a
/// count whose savepoints the caller then consumes. Savepoints left behind by a
/// release are always a bottom prefix of the engine's stack, never interleaved
/// with live ones, so [`Self::take`] can never return more than the engine
/// holds. That is what makes the caller's unwind loop safe: it cannot run the
/// engine past the end of its stack.
///
/// Two costs follow from emulating release rather than performing it, and both
/// would go away if the engines gained a native pop-without-restore:
///
/// - A released savepoint is left on the engine's stack for the rest of the transaction. What that
///   retains is engine-specific: a counter on surrealkv, a lock-tracker allocation on RocksDB, a
///   full writeset snapshot on surrealmx.
/// - Unwinding a scope that absorbed `n` releases costs `n + 1` engine rollbacks, and each one
///   restores whole-transaction state rather than one scope's worth.
#[derive(Debug, Default)]
pub struct SavepointStack {
	scopes: Vec<usize>,
}

impl SavepointStack {
	/// Record that an engine savepoint was set for a new scope.
	pub fn open(&mut self) {
		self.scopes.push(1);
	}

	/// Release the innermost scope, transferring its unwind cost to the
	/// enclosing scope.
	///
	/// The engine's stack is left alone: the released savepoint is what a
	/// rollback of the enclosing scope has to reach back past. Releasing the
	/// outermost scope leaves its engine savepoints unreachable, which is
	/// harmless — a transaction discards the whole stack when it ends.
	pub fn release(&mut self) {
		if let Some(absorbed) = self.scopes.pop()
			&& let Some(enclosing) = self.scopes.last_mut()
		{
			*enclosing += absorbed;
		}
	}

	/// Take the innermost scope, returning how many times the engine must be
	/// rolled back to revert it.
	///
	/// Errors with [`Error::NoSavepoint`] when no scope is open, in which case
	/// the caller must not roll the engine back at all: the engine may still be
	/// holding savepoints for scopes that were released, and reverting to one
	/// of those would discard writes the release was meant to keep.
	pub fn take(&mut self) -> Result<usize> {
		self.scopes.pop().ok_or(Error::NoSavepoint)
	}
}

#[cfg(test)]
mod tests {
	use super::SavepointStack;

	#[test]
	fn one_scope_unwinds_one_engine_savepoint() {
		let mut stack = SavepointStack::default();
		stack.open();
		assert_eq!(stack.take().unwrap(), 1);
		assert!(stack.take().is_err());
	}

	#[test]
	fn released_scope_is_absorbed_by_the_enclosing_scope() {
		let mut stack = SavepointStack::default();
		stack.open();
		stack.open();
		stack.release();
		// The enclosing scope must now walk back past both engine savepoints,
		// or its rollback would stop at the released one.
		assert_eq!(stack.take().unwrap(), 2);
		assert!(stack.take().is_err());
	}

	#[test]
	fn releases_accumulate_across_siblings() {
		let mut stack = SavepointStack::default();
		stack.open();
		for _ in 0..3 {
			stack.open();
			stack.release();
		}
		assert_eq!(stack.take().unwrap(), 4);
	}

	#[test]
	fn rollback_leaves_enclosing_scopes_intact() {
		let mut stack = SavepointStack::default();
		stack.open();
		stack.open();
		assert_eq!(stack.take().unwrap(), 1);
		assert_eq!(stack.take().unwrap(), 1);
		assert!(stack.take().is_err());
	}

	#[test]
	fn releasing_the_outermost_scope_closes_the_stack() {
		let mut stack = SavepointStack::default();
		stack.open();
		stack.release();
		// Nothing is open, so a rollback must not touch the engine even though
		// the engine still holds the released savepoint.
		assert!(stack.take().is_err());
	}

	#[test]
	fn unbalanced_release_is_inert() {
		let mut stack = SavepointStack::default();
		stack.release();
		assert!(stack.take().is_err());
	}
}
