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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

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
/// `surrealdb_idx::file::check_is_path_allowed`.
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

/// The tracked-memory ceiling, in bytes, above which queries are refused
/// (`SURREAL_MEMORY_THRESHOLD`). `0` — the default — disables the guard
/// entirely; any other value is floored to 1 MiB.
///
/// # What the threshold is compared against
///
/// The guard compares this value against *tracked* bytes, not process RSS: for
/// each live Rust-heap allocation the size class the allocator reserved for it,
/// plus every registered external memory reporter (the RocksDB block/row/blob
/// cache). Tracked bytes are a lower bound on the process's real footprint. Not
/// counted, and therefore requiring headroom below a container memory limit:
///
/// - allocator arena metadata, slab-level fragmentation, and pages the allocator retains rather
///   than returning to the operating system
/// - RocksDB memory outside its cache — per-SST table readers, compaction and flush buffers,
///   iterator readahead, and an open transaction's pending writes
/// - WebAssembly linear memory, which is mapped rather than heap-allocated (bound it with
///   `SURREAL_SURREALISM_MAX_MEMORY`)
/// - thread stacks and the binary's own text
///
/// Because the heap term is the reserved size class rather than the requested
/// size, it exceeds the sum of the requests — by more, proportionally, the
/// smaller the allocations are. A value carried over from a release that
/// compared requested bytes therefore refuses queries at a lighter workload
/// than it used to, and wants raising to match.
///
/// Set this *below* the container's memory limit so the guard sheds load before
/// the kernel's OOM killer intervenes, and *above* the configured RocksDB block
/// cache size (`SURREAL_ROCKSDB_BLOCK_CACHE_SIZE`, which defaults to half of
/// system memory less 1 GiB): a warm cache occupies its full capacity
/// indefinitely, so a threshold at or below that size refuses every query once
/// the cache fills and does not recover without a restart.
///
/// # When the guard is inert
///
/// Enforcement needs the `allocation-tracking` feature, which the server
/// enables by default. A build without it reports no tracked memory and never
/// refuses a query, whatever this is set to.
///
/// # Lifetime
///
/// Read once, on first use. Changing the environment variable takes effect only
/// in a fresh process.
pub static MEMORY_THRESHOLD: LazyLock<usize> = LazyLock::new(|| {
	let raw = match std::env::var("SURREAL_MEMORY_THRESHOLD") {
		Ok(raw) => raw,
		// Absent is the documented default: the guard is off.
		Err(std::env::VarError::NotPresent) => return 0,
		// Set to something that is not UTF-8. Reported rather than treated as
		// absent, because the operator did configure it and it will not apply.
		Err(std::env::VarError::NotUnicode(_)) => {
			warn!(
				"Ignoring SURREAL_MEMORY_THRESHOLD: the value is not valid UTF-8. The memory \
				 threshold is DISABLED: queries will not be refused on memory pressure."
			);
			return 0;
		}
	};
	// An empty value is how a deployment spells "leave this unset", so it takes
	// the default without being reported as a mistake.
	if raw.trim().is_empty() {
		return 0;
	}
	match parse_memory_threshold(&raw) {
		Some(v) => v,
		None => {
			warn!(
				"Ignoring invalid SURREAL_MEMORY_THRESHOLD value `{raw}`; expected a byte count \
				 with an optional k/kb/kib/m/mb/mib/g/gb/gib suffix (Kubernetes-style `Gi`/`Mi` \
				 quantities are not accepted). The memory threshold is DISABLED: queries will \
				 not be refused on memory pressure."
			);
			0
		}
	}
});

/// Parse a `SURREAL_MEMORY_THRESHOLD` value into a byte count. Accepts a plain
/// byte count or a human-readable size suffix (`b`, `k`/`kb`/`kib`,
/// `m`/`mb`/`mib`, `g`/`gb`/`gib`, case-insensitive; all multipliers are
/// binary). Returns `None` for unparseable values — note that
/// Kubernetes-style `Gi`/`Mi` suffixes are *not* accepted, so a resource
/// quantity cannot be passed through verbatim. Returns `Some(0)` for `"0"`
/// (disables the threshold); otherwise `Some(n)` floored to 1 MiB.
fn parse_memory_threshold(value: &str) -> Option<usize> {
	value.parse_bytes::<usize>().ok().map(|x| match x {
		0 => 0,
		x => x.max(1024 * 1024),
	})
}

/// `SURREAL_HNSW_BUILD_SEED`, read once at first use. A value that is set but not
/// a valid `u64` is reported via `tracing::warn!` and ignored, rather than
/// silently falling back to entropy — a typo'd seed would otherwise look like a
/// pinned build while producing a different graph on every run.
static HNSW_BUILD_SEED_ENV: LazyLock<Option<u64>> =
	LazyLock::new(|| match std::env::var("SURREAL_HNSW_BUILD_SEED") {
		Ok(v) => match v.parse::<u64>() {
			Ok(seed) => Some(seed),
			Err(_) => {
				warn!("Ignoring invalid SURREAL_HNSW_BUILD_SEED value `{v}`; expected a u64");
				None
			}
		},
		Err(_) => None,
	});

/// In-process fallback seed, installed by [`set_default_hnsw_build_seed`]. Read
/// only when [`HNSW_BUILD_SEED_ENV`] is absent.
static HNSW_BUILD_SEED_DEFAULT: AtomicU64 = AtomicU64::new(0);

/// Whether [`HNSW_BUILD_SEED_DEFAULT`] holds an installed seed. Separate from the
/// seed itself so every `u64` remains a usable seed value.
static HNSW_BUILD_SEED_DEFAULT_SET: AtomicBool = AtomicBool::new(false);

/// The seed for the HNSW level-assignment RNG, or `None` to seed from entropy.
///
/// Resolution order, highest priority first:
///
/// 1. `SURREAL_HNSW_BUILD_SEED=<u64>`.
/// 2. An in-process default installed by [`set_default_hnsw_build_seed`].
/// 3. Neither: entropy, so every index build produces a different graph.
///
/// A fixed seed builds a *deterministic* graph — its structure then depends only
/// on insertion order and the vectors — which makes HNSW build and search
/// benchmarks reproducible across runs. Without one, graph-construction variance
/// alone moves an HNSW search benchmark by far more than any regression worth
/// catching, so a fixed seed is a prerequisite for a clean before/after
/// comparison. The seed only affects graph construction, never search behaviour,
/// results, or recall.
pub fn hnsw_build_seed() -> Option<u64> {
	if let Some(seed) = *HNSW_BUILD_SEED_ENV {
		return Some(seed);
	}
	// `Acquire` pairs with the `Release` store in the setter, so the seed written
	// before the flag is visible to any thread that observes the flag.
	HNSW_BUILD_SEED_DEFAULT_SET
		.load(Ordering::Acquire)
		.then(|| HNSW_BUILD_SEED_DEFAULT.load(Ordering::Relaxed))
}

/// Install the process-wide fallback HNSW build seed used when
/// `SURREAL_HNSW_BUILD_SEED` is unset, so a caller that wants reproducible graphs
/// gets them without depending on its launcher exporting the variable. The
/// environment variable still wins, which is what lets a single run be re-seeded
/// from outside the process.
///
/// Only takes effect for indexes built after the call, and only affects graph
/// construction. BENCHMARK AND TEST USE ONLY — the benchmark harness calls this
/// at startup; a server has no reason to pin the seed.
pub fn set_default_hnsw_build_seed(seed: u64) {
	HNSW_BUILD_SEED_DEFAULT.store(seed, Ordering::Relaxed);
	HNSW_BUILD_SEED_DEFAULT_SET.store(true, Ordering::Release);
}

/// Optional fixed seed for the deterministic data-generation RNG (see
/// `surrealdb_expr::val::rnd`).
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

/// Master switch for pre-filtered vector search (#548, default: true).
/// When `false`, KNN queries keep evaluating their WHERE residual inside the
/// ANN traversal via per-candidate record fetches, exactly as before the
/// allow-list bitmap prefilter existed.
pub static KNN_PREFILTER_ENABLED: LazyLock<bool> =
	lazy_env_parse!("SURREAL_KNN_PREFILTER_ENABLED", bool, true);

/// Allow-list size at or below which a pre-filtered KNN search skips the
/// graph entirely and scores the members exactly (default: 2000). Below this
/// cardinality a bounded fetch-and-score over the bitmap is cheaper than a
/// graph traversal and — unlike filtered ANN search under a highly selective
/// filter — guaranteed to find the true top-K.
pub static KNN_PREFILTER_EXACT_THRESHOLD: LazyLock<u64> =
	lazy_env_parse!("SURREAL_KNN_PREFILTER_EXACT_THRESHOLD", u64, 2_000);

/// Allow-list size above which the gated ANN traversal stops boosting `ef`
/// (default: 100000). Large allow-lists barely reject candidates on typical
/// tables, so widening the search buys no recall; admission stays gated on
/// bitmap membership at every size — an absolute cardinality says nothing
/// about how near-universe the predicate really is, and an unrestricted
/// search with a post-check would truncate results whenever the nearest
/// overall neighbours are excluded.
pub static KNN_PREFILTER_EF_BOOST_THRESHOLD: LazyLock<u64> =
	lazy_env_parse!("SURREAL_KNN_PREFILTER_EF_BOOST_THRESHOLD", u64, 100_000);

/// `ef` multiplier for allow-list-gated ANN traversal (default: 4). A
/// selective allow-list rejects most candidates, effectively thinning the
/// graph; boosting the search width counteracts the recall loss. Applied as
/// `max(ef, ef * boost)` capped by [`KNN_PREFILTER_EF_MAX`].
pub static KNN_PREFILTER_EF_BOOST: LazyLock<u32> =
	lazy_env_parse!("SURREAL_KNN_PREFILTER_EF_BOOST", u32, 4);

/// Ceiling for the boosted `ef` of an allow-list-gated ANN traversal
/// (default: 1024). Never clamps below the user-requested `ef`.
pub static KNN_PREFILTER_EF_MAX: LazyLock<u32> =
	lazy_env_parse!("SURREAL_KNN_PREFILTER_EF_MAX", u32, 1_024);

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

	/// Kubernetes resource quantities are not accepted. A deployment that
	/// templates a container memory limit straight into the environment
	/// variable disables the guard rather than setting it, so these forms must
	/// keep returning `None` and be reported to the operator.
	#[test]
	fn memory_threshold_rejects_kubernetes_quantities() {
		// Binary IEC suffixes as Kubernetes spells them.
		assert_eq!(parse_memory_threshold("8Gi"), None);
		assert_eq!(parse_memory_threshold("8192Mi"), None);
		// Decimal fractions and percentages are not a supported spelling.
		assert_eq!(parse_memory_threshold("1.5g"), None);
		assert_eq!(parse_memory_threshold("85%"), None);
		// Terabyte suffixes are outside the accepted set.
		assert_eq!(parse_memory_threshold("1tb"), None);
	}

	/// The in-process HNSW seed default is off until installed, then answers every
	/// later resolution. This is the only test that touches that process-global
	/// state, so it can assert the uninstalled state without synchronising.
	#[test]
	fn hnsw_build_seed_honours_installed_default() {
		// `SURREAL_HNSW_BUILD_SEED` outranks the default by design, so with it set
		// there is nothing here for the default to answer.
		if std::env::var_os("SURREAL_HNSW_BUILD_SEED").is_some() {
			return;
		}

		assert_eq!(hnsw_build_seed(), None, "no seed until one is installed");

		set_default_hnsw_build_seed(0);
		assert_eq!(hnsw_build_seed(), Some(0), "zero is a seed, not 'unset'");

		set_default_hnsw_build_seed(u64::MAX);
		assert_eq!(hnsw_build_seed(), Some(u64::MAX), "a later install replaces the default");
	}
}
