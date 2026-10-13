//! This module stores the TiKV savepoint types.

use surrealdb_kvs::Val;

/// An operation that can be undone during savepoint rollback
#[derive(Debug)]
pub(super) enum Operation {
	/// Delete a key that was inserted
	DeleteKey(Vec<u8>),
	/// Restore a key's previous value, whether it was overwritten or deleted
	RestoreValue(Vec<u8>, Val),
}

/// The undo log backing savepoint support on TiKV.
///
/// TiKV exposes no native savepoint, so a rollback is performed by replaying
/// an inverse operation for every write made since the savepoint was taken.
/// Capturing an inverse operation requires the key's pre-image, which costs a
/// round trip whenever the write buffer doesn't already hold it, so writes
/// only capture one while a savepoint is active.
///
/// Each entry on the stack holds the inverse operations for one open scope,
/// oldest first, with the innermost scope last. Writes record into the
/// innermost scope, and with no scope open there is nowhere to record — which
/// is what keeps a write from paying for a pre-image read that no rollback
/// could ever consume.
#[derive(Debug, Default)]
pub(super) struct UndoLog {
	/// Inverse operations per open scope, outermost scope first
	savepoints: Vec<Vec<Operation>>,
}

impl UndoLog {
	/// Whether writes must capture the pre-image of the keys they touch.
	///
	/// This is the gate for the extra read on every write path, so it is
	/// false whenever no savepoint is active.
	pub(super) fn is_tracking(&self) -> bool {
		!self.savepoints.is_empty()
	}

	/// Record the inverse of a write that has just succeeded.
	///
	/// Dropped when no savepoint is active, as no rollback could reach it.
	pub(super) fn record(&mut self, operation: Operation) {
		if let Some(scope) = self.savepoints.last_mut() {
			scope.push(operation);
		}
	}

	/// Take a savepoint, opening a scope that subsequent writes record into.
	pub(super) fn new_savepoint(&mut self) {
		self.savepoints.push(Vec::new());
	}

	/// Release the innermost savepoint, keeping the scope's writes.
	///
	/// A rollback to an enclosing savepoint must still undo those writes, so
	/// the released scope's operations are appended to the enclosing scope,
	/// preserving the order they were recorded in. Releasing the outermost
	/// savepoint drops them instead: no rollback can reach them, and keeping
	/// them would make every later write capture a pre-image again.
	pub(super) fn release_savepoint(&mut self) {
		if let Some(scope) = self.savepoints.pop()
			&& let Some(enclosing) = self.savepoints.last_mut()
		{
			enclosing.extend(scope);
		}
	}

	/// Take the innermost savepoint's operations for a rollback, leaving the
	/// enclosing scope as the innermost.
	///
	/// Operations are returned oldest first; the caller undoes the scope by
	/// applying them in reverse. Returns `None` when no savepoint is active.
	pub(super) fn take_savepoint(&mut self) -> Option<Vec<Operation>> {
		self.savepoints.pop()
	}
}

#[cfg(test)]
mod tests {
	use super::{Operation, UndoLog};

	/// Stands in for the `set` / `del` / `delr` write paths, which read a
	/// key's pre-image if and only if the undo log is tracking.
	///
	/// This mirrors those paths rather than driving them — the real ones need
	/// a live cluster — so it pins the log's own behaviour, not the call
	/// sites'. Counting the reads a write sequence would issue is still the
	/// most direct statement of the contract: the log must not ask for a
	/// pre-image once no savepoint is active.
	#[derive(Default)]
	struct Writer {
		undo: UndoLog,
		reads: usize,
	}

	impl Writer {
		/// Overwrite a key, mirroring the pre-image read and the record the
		/// `set` path performs.
		fn set(&mut self, key: &[u8], old: &[u8]) {
			if self.undo.is_tracking() {
				self.reads += 1;
			}
			self.undo.record(Operation::RestoreValue(key.to_vec(), old.to_vec()));
		}
	}

	/// Summarise a log as the keys it would restore, in undo order (the
	/// reverse of the order they were recorded in).
	fn undo_order(operations: &[Operation]) -> Vec<Vec<u8>> {
		operations
			.iter()
			.rev()
			.map(|op| match op {
				Operation::DeleteKey(key) | Operation::RestoreValue(key, _) => key.clone(),
			})
			.collect()
	}

	#[test]
	fn no_reads_without_a_savepoint() {
		let mut w = Writer::default();
		w.set(b"a", b"0");
		w.set(b"b", b"0");
		// Nothing can roll these back, so no pre-image is needed.
		assert_eq!(w.reads, 0);
		assert!(!w.undo.is_tracking());
	}

	#[test]
	fn no_reads_after_releasing_the_last_savepoint() {
		let mut w = Writer::default();
		// One savepoint's worth of writes, released on success — the shape
		// every INSERT/UPSERT document takes.
		w.undo.new_savepoint();
		w.set(b"a", b"0");
		assert_eq!(w.reads, 1);
		w.undo.release_savepoint();
		// With no savepoint active the remaining writes must not read.
		let reads_before = w.reads;
		w.set(b"b", b"0");
		w.set(b"c", b"0");
		assert_eq!(w.reads, reads_before, "writes read a pre-image with no savepoint active");
		assert!(!w.undo.is_tracking());
		assert!(w.undo.take_savepoint().is_none(), "released operations outlived their savepoint");
	}

	#[test]
	fn repeated_savepoint_cycles_do_not_accumulate() {
		let mut w = Writer::default();
		// Each cycle is one document in a multi-record INSERT: the reads it
		// costs must not grow with the number of documents already written.
		for i in 0..4u8 {
			w.undo.new_savepoint();
			w.set(&[i], b"0");
			w.undo.release_savepoint();
		}
		assert_eq!(w.reads, 4);
		assert!(!w.undo.is_tracking());
	}

	#[test]
	fn release_merges_into_the_enclosing_savepoint() {
		let mut log = UndoLog::default();
		log.new_savepoint();
		log.record(Operation::DeleteKey(b"outer".to_vec()));
		log.new_savepoint();
		log.record(Operation::DeleteKey(b"inner".to_vec()));
		log.release_savepoint();
		// The enclosing savepoint is still active, and a rollback to it undoes
		// both scopes' writes, innermost first. This is the path a sync
		// `DEFINE EVENT` takes: its statement opens a nested savepoint and
		// releases it, inside the enclosing per-document savepoint.
		assert!(log.is_tracking());
		let operations = log.take_savepoint().expect("the enclosing savepoint is still active");
		assert_eq!(undo_order(&operations), vec![b"inner".to_vec(), b"outer".to_vec()]);
		assert!(!log.is_tracking());
	}

	#[test]
	fn rollback_restores_the_enclosing_savepoints_log() {
		let mut log = UndoLog::default();
		log.new_savepoint();
		log.record(Operation::DeleteKey(b"outer".to_vec()));
		log.new_savepoint();
		log.record(Operation::DeleteKey(b"inner".to_vec()));
		// Rolling back the inner scope hands back only its own operations.
		let operations = log.take_savepoint().expect("the inner savepoint is active");
		assert_eq!(undo_order(&operations), vec![b"inner".to_vec()]);
		// The enclosing scope keeps its operations and stays active.
		assert!(log.is_tracking());
		let operations = log.take_savepoint().expect("the enclosing savepoint is active");
		assert_eq!(undo_order(&operations), vec![b"outer".to_vec()]);
		assert!(!log.is_tracking());
	}

	#[test]
	fn take_savepoint_without_a_savepoint_is_none() {
		let mut log = UndoLog::default();
		assert!(log.take_savepoint().is_none());
	}

	#[test]
	fn unbalanced_release_is_inert() {
		let mut log = UndoLog::default();
		log.release_savepoint();
		assert!(!log.is_tracking());
		assert!(log.take_savepoint().is_none());
	}
}
