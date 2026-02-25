pub(crate) mod dynamic;

use std::sync::LazyLock;

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
// Statics that stay global (genuinely no path to receive config)
// ---------------------------------------------------------------------------

/// The memory usage threshold before tasks are forced to exit (default: 0
/// bytes). The default 0 bytes means that there is no memory threshold.
/// Any other user-set memory threshold will default to at least 1 MiB.
pub static MEMORY_THRESHOLD: LazyLock<usize> = LazyLock::new(|| {
	let n = std::env::var("SURREAL_MEMORY_THRESHOLD")
		.map(|s| s.parse::<usize>().unwrap_or(0))
		.unwrap_or(0);
	match n {
		default @ 0 => default,
		specified => std::cmp::max(specified, 1024 * 1024),
	}
});

/// Specifies the number of computed regexes which can be cached in the engine
/// (default: 1000). Kept global because it governs a process-wide thread-local
/// regex cache used from parsing, deserialization, and query execution.
pub static REGEX_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_REGEX_CACHE_SIZE", usize, 1_000);
