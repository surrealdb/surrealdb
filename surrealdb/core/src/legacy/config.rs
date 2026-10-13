//! The legacy evaluator's concurrency budget.
//!
//! The recursive value walk evaluates the branches of an idiom over an array or
//! an object concurrently through `try_join_all_buffered`, which needs a bound
//! on how many of those futures may be in flight at once; without one, a wide
//! collection spawns one future per element.
//!
//! The legacy evaluator owns this knob because it is its only reader — the
//! streaming executor bounds concurrency through operator buffering instead —
//! so the knob is scoped to the legacy walk and goes away with it.

use surrealdb_cnf as cnf;

/// The concurrency limit of the legacy evaluator.
#[derive(Clone, Debug)]
pub(crate) struct LegacyConfig {
	/// Specifies how many concurrent jobs can be buffered in the worker channel
	pub max_concurrent_tasks: usize,
}

impl Default for LegacyConfig {
	fn default() -> Self {
		Self {
			#[cfg(not(target_family = "wasm"))]
			max_concurrent_tasks: 64,
			#[cfg(target_family = "wasm")]
			max_concurrent_tasks: 1,
		}
	}
}

impl cnf::Config for LegacyConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("max_concurrent_tasks", &mut self.max_concurrent_tasks);
	}
}
