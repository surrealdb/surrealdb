//! Ceilings applied to the embedded JavaScript runtime.
//!
//! The three knobs bound one invocation of a scripting function: the interpreter
//! stack it may grow, the heap it may allocate, and the wall-clock time it may
//! spend before the interrupt handler cancels it. This layer owns them because
//! it is the only place a JavaScript runtime is created, and each value is
//! installed on that runtime — a stack or memory ceiling cannot be changed once
//! the runtime exists.
//!
//! The time limit is an upper bound only: the same interrupt handler also
//! observes the query's cancellation, so a script stops at whichever comes
//! first.

use std::time::Duration;

use surrealdb_cnf as cnf;

/// Resource ceilings of the JavaScript function runtime.
#[derive(Clone, Debug)]
pub(crate) struct ScriptConfig {
	/// The maximum stack size of the JavaScript function runtime (default: 256 KiB)
	pub scripting_max_stack_size: usize,
	/// The maximum memory limit of the JavaScript function runtime (default: 2 MiB)
	pub scripting_max_memory_limit: usize,
	/// The maximum amount of time that a JavaScript function can run (default: 5
	/// seconds)
	pub scripting_max_time_limit: Duration,
}

impl Default for ScriptConfig {
	fn default() -> Self {
		Self {
			scripting_max_stack_size: 256 * 1024,
			scripting_max_memory_limit: 2 << 20,
			scripting_max_time_limit: Duration::from_secs(5),
		}
	}
}

impl cnf::Config for ScriptConfig {
	fn parse(&mut self, map: &cnf::ConfigMap) {
		map.parse_key("scripting_max_stack_size", &mut self.scripting_max_stack_size)
			.parse_key("scripting_max_memory_limit", &mut self.scripting_max_memory_limit)
			.parse_key_with("scripting_max_time_limit", &mut self.scripting_max_time_limit, |x| {
				x.parse().map(Duration::from_millis).ok()
			});
	}
}
