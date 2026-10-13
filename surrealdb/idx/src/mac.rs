//! Macros the index engines use.

/// Pauses and yields execution to the tokio runtime.
///
/// The engines' batch loops — cache eviction, compaction, count scans, term-file
/// loading — can run long enough to starve other tasks on the same worker, so
/// they yield between batches.
macro_rules! yield_now {
	() => {
		if tokio::runtime::Handle::try_current().is_ok() {
			tokio::task::consume_budget().await;
		}
	};
}
