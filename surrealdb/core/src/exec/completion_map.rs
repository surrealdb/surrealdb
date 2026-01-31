//! CompletionMap - synchronization primitive for parallel statement execution.
//!
//! The CompletionMap tracks completion status and results for all statements
//! in a script, enabling statements to wait for their dependencies.

use parking_lot::Mutex;
use tokio::sync::watch;

use crate::exec::statement::{StatementId, StatementOutput};

/// Error returned when waiting for a statement fails.
#[derive(Debug, Clone)]
pub enum CompletionError {
	/// The statement ID is out of bounds.
	InvalidStatementId(StatementId),
	/// Execution was aborted (channel closed).
	ExecutionAborted,
	/// The statement failed with an error.
	StatementFailed(String),
}

impl std::fmt::Display for CompletionError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::InvalidStatementId(id) => write!(f, "Invalid statement ID: {}", id),
			Self::ExecutionAborted => write!(f, "Execution was aborted"),
			Self::StatementFailed(msg) => write!(f, "Statement failed: {}", msg),
		}
	}
}

impl std::error::Error for CompletionError {}

/// Tracks completion status and results for all statements in a script.
///
/// This is the synchronization primitive that enables parallel execution:
/// - Statements signal completion via `complete()`
/// - Dependent statements wait via `wait_for()`
/// - Results are stored for later retrieval
pub struct CompletionMap {
	/// Number of statements
	size: usize,

	/// Completed outputs (None until complete, Some(Ok) on success, Some(Err) on failure)
	outputs: Vec<Mutex<Option<Result<StatementOutput, String>>>>,

	/// Completion signals - each sender signals when a statement completes
	senders: Vec<watch::Sender<bool>>,

	/// Receivers for waiting on completion
	receivers: Vec<watch::Receiver<bool>>,
}

impl CompletionMap {
	/// Create a new CompletionMap for the given number of statements.
	pub fn new(size: usize) -> Self {
		let (senders, receivers): (Vec<_>, Vec<_>) =
			(0..size).map(|_| watch::channel(false)).unzip();

		Self {
			size,
			outputs: (0..size).map(|_| Mutex::new(None)).collect(),
			senders,
			receivers,
		}
	}

	/// Wait for a statement to complete and get its output.
	///
	/// Returns the output on success, or an error if:
	/// - The statement ID is invalid
	/// - Execution was aborted (channel closed)
	/// - The statement failed with an error
	pub async fn wait_for(&self, id: StatementId) -> Result<StatementOutput, CompletionError> {
		let idx = id.0;
		if idx >= self.size {
			return Err(CompletionError::InvalidStatementId(id));
		}

		// Wait for completion signal
		let mut rx = self.receivers[idx].clone();
		while !*rx.borrow() {
			rx.changed().await.map_err(|_| CompletionError::ExecutionAborted)?;
		}

		// Get the output
		let guard = self.outputs[idx].lock();
		match guard.as_ref() {
			Some(Ok(output)) => Ok(output.clone()),
			Some(Err(msg)) => Err(CompletionError::StatementFailed(msg.clone())),
			None => Err(CompletionError::ExecutionAborted),
		}
	}

	/// Mark a statement as successfully completed with its output.
	pub fn complete(&self, id: StatementId, output: StatementOutput) {
		let idx = id.0;
		if idx >= self.size {
			return;
		}

		*self.outputs[idx].lock() = Some(Ok(output));
		let _ = self.senders[idx].send(true);
	}

	/// Mark a statement as failed with an error message.
	pub fn fail(&self, id: StatementId, error: String) {
		let idx = id.0;
		if idx >= self.size {
			return;
		}

		*self.outputs[idx].lock() = Some(Err(error));
		let _ = self.senders[idx].send(true);
	}

	/// Check if a statement is complete (non-blocking).
	pub fn is_complete(&self, id: StatementId) -> bool {
		let idx = id.0;
		if idx >= self.size {
			return false;
		}
		*self.receivers[idx].borrow()
	}

	/// Get the output of a completed statement without waiting.
	///
	/// Returns None if the statement hasn't completed yet.
	pub fn get_output(&self, id: StatementId) -> Option<Result<StatementOutput, String>> {
		let idx = id.0;
		if idx >= self.size {
			return None;
		}
		self.outputs[idx].lock().clone()
	}
}

impl std::fmt::Debug for CompletionMap {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("CompletionMap")
			.field("size", &self.size)
			.field(
				"completed",
				&(0..self.size).filter(|&i| self.is_complete(StatementId(i))).count(),
			)
			.finish()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	// Note: Full integration tests with actual StatementOutput require
	// a complete execution context with Transaction. These are covered
	// in the language-tests framework. Here we test the basic synchronization
	// primitives.

	#[test]
	fn test_completion_map_creation() {
		let map = CompletionMap::new(5);
		assert!(!map.is_complete(StatementId(0)));
		assert!(!map.is_complete(StatementId(4)));
		// Out of bounds should return false
		assert!(!map.is_complete(StatementId(10)));
	}

	#[test]
	fn test_completion_map_fail() {
		let map = CompletionMap::new(3);
		map.fail(StatementId(0), "test error".to_string());
		assert!(map.is_complete(StatementId(0)));

		// Get the output should be an error
		let output = map.get_output(StatementId(0));
		assert!(output.is_some());
		assert!(output.unwrap().is_err());
	}

	#[test]
	fn test_completion_map_invalid_id() {
		let map = CompletionMap::new(3);
		// Operations on invalid IDs should be safe (no-op or return None/false)
		assert!(!map.is_complete(StatementId(100)));
		assert!(map.get_output(StatementId(100)).is_none());
	}

	#[test]
	fn test_debug_impl() {
		let map = CompletionMap::new(3);
		let debug_str = format!("{:?}", map);
		assert!(debug_str.contains("CompletionMap"));
		assert!(debug_str.contains("size"));
	}
}
