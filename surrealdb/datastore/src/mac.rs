//! Macros the transaction layer uses.

/// Pauses and yields execution to the tokio runtime.
///
/// The sequence allocator's batch loop can run long enough to starve other tasks
/// on the same worker, so it yields between batches.
macro_rules! yield_now {
	() => {
		if tokio::runtime::Handle::try_current().is_ok() {
			tokio::task::consume_budget().await;
		}
	};
}
