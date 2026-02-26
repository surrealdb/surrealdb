pub(crate) mod dynamic;

use std::sync::atomic::{AtomicUsize, Ordering};

pub use surrealdb_cfg::*;

// ---------------------------------------------------------------------------
// True constants (no env vars)
// ---------------------------------------------------------------------------

/// The publicly visible name of the server
pub const SERVER_NAME: &str = "SurrealDB";

/// The characters which are supported in server record IDs
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Specifies the names of parameters which can not be specified in a query
pub const PROTECTED_PARAM_NAMES: &[&str] = &["access", "auth", "token", "session"];

// ---------------------------------------------------------------------------
// Global atomics set from CoreConfig during Datastore initialization
// ---------------------------------------------------------------------------

/// The memory usage threshold before tasks are forced to exit (default: 0 = disabled).
/// Set from `CoreConfig.limits.memory_threshold` during datastore init.
pub static MEMORY_THRESHOLD: AtomicUsize = AtomicUsize::new(0);

/// The number of computed regexes cached in the engine (default: 1000).
/// Set from `CoreConfig.caches.regex_cache_size` during datastore init.
pub static REGEX_CACHE_SIZE: AtomicUsize = AtomicUsize::new(1_000);

/// Apply the relevant fields from a `CoreConfig` to the global atomics.
pub fn apply_config(config: &CoreConfig) {
	MEMORY_THRESHOLD.store(config.limits.memory_threshold, Ordering::Relaxed);
	REGEX_CACHE_SIZE.store(config.caches.regex_cache_size, Ordering::Relaxed);
}
