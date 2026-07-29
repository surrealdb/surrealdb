/// Whether a DEFINE EVENT runs inline with the triggering write or is
/// dispatched asynchronously, and the retry/nesting budget when async.

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum EventKind {
	Sync,
	Async {
		/// Maximum retry count for async events (0 disables retries; event still runs once).
		retry: u16,
		/// Maximum async event nesting depth for this event (0 allows top-level only).
		max_depth: u16,
	},
}

impl EventKind {
	/// Retry budget applied to `ASYNC` events that do not name one.
	pub const DEFAULT_RETRY: u16 = 1;
	/// Nesting budget applied to `ASYNC` events that do not name one.
	pub const DEFAULT_MAX_DEPTH: u16 = 3;
}
