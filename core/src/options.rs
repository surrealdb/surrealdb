/// Configuration for the engine behaviour
/// The defaults are optimal so please only modify these if you know deliberately why you are modifying them.
#[derive(Clone, Copy, Debug)]
pub struct EngineOptions {
	/// The maximum number of live queries that can be created in a single transaction
	pub new_live_queries_per_transaction: u32,
}

impl Default for EngineOptions {
	fn default() -> Self {
		Self {
			new_live_queries_per_transaction: 100,
		}
	}
}
