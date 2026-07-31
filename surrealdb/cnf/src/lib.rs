//! # Surrealdb Config
//!
//! Server-wide configuration constants, environment-variable defaults, and the
//! [`Config`]/[`ConfigMap`] mechanism each layer uses to declare and load its
//! own settings.
//!
//! <section class="warning">
//! <h3>Unstable!</h3>
//! This crate is <b>SurrealDB internal API</b>. It does not adhere to SemVer and its API is
//! free to change and break code even between patch versions. If you are looking for a stable
//! interface to the SurrealDB library please have a look at
//! <a href="https://crates.io/crates/surrealdb">the Rust SDK</a>.
//! </section>

#[macro_use]
extern crate tracing;
#[macro_use]
extern crate common;

pub mod config;
pub mod dynamic;

use std::fs;
use std::path::PathBuf;
use std::sync::LazyLock;

use common::str::ParseBytes;
pub use config::{Config, ConfigMap, format_duration, parse_duration};
pub use dynamic::DynamicConfiguration;
use path_clean::PathClean;

/// The publicly visible name of the server
pub const SERVER_NAME: &str = "SurrealDB";

/// The characters which are supported in server record IDs
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// Specifies the names of parameters which can not be specified in a query
pub const PROTECTED_PARAM_NAMES: &[&str] = &["access", "auth", "token", "session"];

/// Default capacity for the bounded channel used to deliver live-query
/// notifications from the datastore to subscribers.
pub const NOTIFICATIONS_CHANNEL_SIZE: usize = 15_000;

/// Parse an allowlist configuration string into a list of paths.
///
/// The string is split on the platform path delimiter (`:` on Unix, `;` on
/// Windows), each entry is cleaned, and (when `canonicalize` is set) resolved
/// to its canonical form; entries that fail to canonicalize are dropped with a
/// warning. `subject` names the allowlist for log messages (e.g. `"file"`).
/// Enforcement of a resolved path against the allowlist lives in
/// `surrealdb_core::iam::file::check_is_path_allowed`.
pub fn extract_allowed_paths(input: &str, canonicalize: bool, subject: &str) -> Vec<PathBuf> {
	let delimiter = if cfg!(target_os = "windows") {
		";"
	} else {
		":"
	};
	// Split the allowlist string, canonicalize each path, and collect valid paths.
	input
		.split(delimiter)
		.filter_map(|s| {
			let trimmed = s.trim();
			if trimmed.is_empty() {
				None
			} else {
				let path = PathBuf::from(trimmed).clean();
				let path = if canonicalize {
					let Ok(path) = fs::canonicalize(&path) else {
						warn!("Failed to canonicalize {subject} path: {}", path.to_string_lossy());
						return None;
					};

					path
				} else {
					path
				};

				debug!("Allowed {subject} path: {}", path.to_string_lossy());
				Some(path)
			}
		})
		.collect()
}

//FIXME: These configuration values should be removed.
// We advertise that we are embeddable, but configuring solely through environment variables is not
// acceptable for an embeddable database.
// Currently these cannot be removed without a major restructure.

// Used in the memory allocator global, so hard to remove.

/// The memory usage threshold before tasks are forced to exit (default: 0
/// bytes). The default 0 bytes means that there is no memory threshold.
/// Any other user-set memory threshold will default to at least 1 MiB.
pub static MEMORY_THRESHOLD: LazyLock<usize> = LazyLock::new(|| {
	std::env::var("SURREAL_MEMORY_THRESHOLD")
		.ok()
		.and_then(|x| parse_memory_threshold(&x))
		.unwrap_or(0)
});

/// Parse a `SURREAL_MEMORY_THRESHOLD` value into a byte count. Accepts a plain
/// byte count or a human-readable size suffix (`b`/`kb`/`kib`/`mb`/`mib`/
/// `gb`/`gib`, case-insensitive). Returns `None` for unparseable values;
/// `Some(0)` for `"0"` (disables the threshold); otherwise `Some(n)` floored
/// to 1 MiB.
fn parse_memory_threshold(value: &str) -> Option<usize> {
	value.parse_bytes::<usize>().ok().map(|x| match x {
		0 => 0,
		x => x.max(1024 * 1024),
	})
}

/// Optional fixed seed for the HNSW level-assignment RNG.
///
/// Unset (the default) seeds the RNG from entropy, so every index build produces
/// a different graph. Set `SURREAL_HNSW_BUILD_SEED=<u64>` to build a
/// *deterministic* graph (the structure then depends only on insertion order and
/// the vectors), which makes HNSW search benchmarks reproducible across runs — a
/// prerequisite for a clean before/after comparison of search-path changes. It
/// only affects graph construction, never search behaviour, results, or recall.
///
/// Read once at first use, like the other knobs here: the benchmark harness sets
/// the variable out-of-process before launch, so a read-once `LazyLock` is
/// sufficient and avoids any in-process `set_var`.
pub static HNSW_BUILD_SEED: LazyLock<Option<u64>> = LazyLock::new(|| {
	std::env::var("SURREAL_HNSW_BUILD_SEED").ok().and_then(|s| s.parse::<u64>().ok())
});

/// Optional fixed seed for the deterministic data-generation RNG (see
/// `surrealdb_core::rnd`).
///
/// Unset (the default) leaves `rand::*` and generated record ids drawing from
/// the per-thread RNG, exactly as in production. Set `SURREAL_RAND_SEED=<u64>`
/// to route them through a single seeded RNG so benchmark datasets are identical
/// across runs. TEST AND BENCHMARK USE ONLY — never set it on a shared or
/// multi-tenant deployment, where it makes record ids and `rand::*` values
/// predictable process-wide.
///
/// Read once at first use, like the other knobs here. A value that is set but
/// not a valid `u64` is reported via `tracing::warn!` and ignored, rather than
/// silently falling back to the default.
pub static RAND_SEED: LazyLock<Option<u64>> =
	LazyLock::new(|| match std::env::var("SURREAL_RAND_SEED") {
		Ok(v) => match v.parse::<u64>() {
			Ok(seed) => Some(seed),
			Err(_) => {
				warn!("Ignoring invalid SURREAL_RAND_SEED value `{v}`; expected a u64");
				None
			}
		},
		Err(_) => None,
	});

/// Initial (and minimum) window size for the DiskANN filtered-KNN record
/// prefetch. The committed-graph search prefetches candidate records in
/// distance-ascending windows that grow geometrically (doubling, capped by
/// [`DISKANN_FILTER_PREFETCH_MAX_CHUNK`]); this is the first window's size and
/// the floor. A smaller value bounds the over-fetch tighter when the result
/// builder fills early (non-selective filters); a larger value amortises each
/// window's multi-get over more candidates. Read once at first use.
pub static DISKANN_FILTER_PREFETCH_MIN_CHUNK: LazyLock<usize> = LazyLock::new(|| {
	std::env::var("SURREAL_DISKANN_FILTER_PREFETCH_MIN_CHUNK")
		.ok()
		.and_then(|s| s.parse::<usize>().ok())
		.filter(|n| *n > 0)
		.unwrap_or(64)
});

/// Upper bound on the DiskANN filtered-KNN record-prefetch window (the geometric
/// growth is capped here). Read once at first use.
pub static DISKANN_FILTER_PREFETCH_MAX_CHUNK: LazyLock<usize> = LazyLock::new(|| {
	std::env::var("SURREAL_DISKANN_FILTER_PREFETCH_MAX_CHUNK")
		.ok()
		.and_then(|s| s.parse::<usize>().ok())
		.filter(|n| *n > 0)
		.unwrap_or(4096)
});

// Used in a lot of surrealql functions which randomly access this limit as well as casting
// functions Both of which cannot be changed without massive restructuring.

/// Used to limit allocation for builtin functions. Default: 2^20 (1 MiB),
/// can be as large as 28 (2^28, 256 MiB)
pub static GENERATION_ALLOCATION_LIMIT: LazyLock<usize> = LazyLock::new(|| {
	let n = std::env::var("SURREAL_GENERATION_ALLOCATION_LIMIT")
		.map(|s| s.parse::<u32>().unwrap_or(20))
		.unwrap_or(20);
	2usize.pow(n.min(28))
});

// Used in a lot of surrealql functions which randomly access this limit.
// Which cannot be changed without massive restructuring the planner.

/// The maximum input string length for similarity/distance functions (default:
/// 16384 bytes)
pub static STRING_SIMILARITY_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_STRING_SIMILARITY_LIMIT", usize, 16384);

// Used in global regex cache, we would first need to make that cache non-global.

/// The maximum size of a compiled regular expression (default: 10 MiB)
pub static REGEX_SIZE_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_REGEX_SIZE_LIMIT", usize, 10 * 1024 * 1024);

/// Specifies the number of computed regexes which can be cached in the engine
/// (default: 1000)
pub static REGEX_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_REGEX_CACHE_SIZE", usize, 1_000);

/// Drained-entry budget for one range branch of a bitmap candidate plan
/// (default: 250000). A non-anchor range branch that drains more index
/// entries than this is abandoned — its predicate is instead enforced by the
/// residual WHERE filter — so an unselective range cannot make the bitmap
/// plan slower than the streaming plan it replaced. `0` disables the budget.
pub static BITMAP_BRANCH_BUDGET: LazyLock<usize> =
	lazy_env_parse!("SURREAL_BITMAP_BRANCH_BUDGET", usize, 250_000);

// A limit belongs on the owning layer's config struct rather than in a static
// here whenever its readers already hold a configuration in hand: that keeps it
// per-datastore and settable programmatically, not only through an env var. The
// statics above are the cases where no such handle reaches the reader.

#[cfg(test)]
mod tests {
	use super::*;

	/// `SURREAL_MEMORY_THRESHOLD` must accept human-readable byte suffixes
	/// (regression for #6860, which dropped suffix parsing and silently
	/// disabled the guard for values like `1792mb`).
	#[test]
	fn memory_threshold_parses_byte_suffixes() {
		// Human-readable suffix is honoured (the regressed case).
		assert_eq!(parse_memory_threshold("1792mb"), Some(1792 * 1024 * 1024));
		assert_eq!(parse_memory_threshold("1g"), Some(1024 * 1024 * 1024));
		// A plain byte count still works.
		assert_eq!(parse_memory_threshold("1879048192"), Some(1792 * 1024 * 1024));
		// `0` disables the threshold.
		assert_eq!(parse_memory_threshold("0"), Some(0));
		// Any non-zero value is floored to at least 1 MiB.
		assert_eq!(parse_memory_threshold("10"), Some(1024 * 1024));
		// An unparseable value returns None; callers map that to disabled (0).
		assert_eq!(parse_memory_threshold("garbage"), None);
	}
}
