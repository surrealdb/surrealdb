//! # Surrealdb Config
//!
//! Server-wide configuration constants, environment-variable defaults, and
//! the [`CommonConfig`] struct used to tune SurrealDB's runtime limits.
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

use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;
use std::{fmt, fs};

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

/// Default value for [`CommonConfig::scan_batch_size`] — the number of records
/// each scan operator buffers before yielding a batch downstream. Read at
/// runtime from config (overridable via `SURREAL_SCAN_BATCH_SIZE`).
pub const DEFAULT_SCAN_BATCH_SIZE: usize = 1000;

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

/// Selects which live-query execution engine the datastore uses.
///
/// See `surrealdb_core::lq` for the architecture. Defaults to
/// [`LiveQueryEngine::Inline`] (the historical behaviour) so the new pipeline
/// can be rolled out behind this flag without changing existing deployments.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LiveQueryEngine {
	/// Legacy engine: per-subscriber matching, permission checks, and projection
	/// run inline on the mutator's transaction path before commit, so write cost
	/// scales with the number of live subscribers on the written table.
	#[default]
	Inline,
	/// Inverted engine: the mutator only persists before/after values, and a
	/// per-node router performs per-subscriber matching off the write path, so
	/// write throughput is independent of the subscriber count.
	Router,
}

impl fmt::Display for LiveQueryEngine {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Inline => f.write_str("inline"),
			Self::Router => f.write_str("router"),
		}
	}
}

impl FromStr for LiveQueryEngine {
	type Err = String;

	fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
		match s.to_lowercase().as_str() {
			"inline" => Ok(Self::Inline),
			"router" => Ok(Self::Router),
			v => Err(format!("Invalid live query engine: '{v}'. Expected 'inline' or 'router'")),
		}
	}
}

#[derive(Debug)]
pub struct CommonConfig {
	pub memory_threshold: usize,
	/// Specifies how many concurrent jobs can be buffered in the worker channel
	pub max_concurrent_tasks: usize,
	/// Specifies how deep recursive computation will go before erroring (default:
	/// 120)
	pub max_computation_depth: u32,
	/// Specifies how deep the parser will parse nested objects and arrays (default:
	/// 100)
	pub max_object_parsing_depth: u32,
	/// Specifies how deep the parser will parse recursive queries (default: 20)
	pub max_query_parsing_depth: u32,
	/// Specifies how deep the parser will build an expression operator tree
	/// before erroring. Bounds left-associative operator spines (e.g.
	/// `1 + 1 + 1 + ...`) and prefix/postfix chains, which are otherwise
	/// unbounded and overflow the call stack when the resulting tree is later
	/// walked recursively (e.g. dropped, formatted, or lowered to `expr::Expr`).
	/// Kept low enough that even those recursive walks stay well within a
	/// conservative worker-thread stack (default: 128)
	pub max_expression_parsing_depth: u32,
	/// The maximum recursive idiom path depth allowed (default: 256)
	pub idiom_recursion_limit: u32,
	/// The maximum size of a compiled regular expression (default: 10 MiB)
	pub regex_size_limit: usize,
	/// Specifies the number of computed regexes which can be cached in the engine
	/// (default: 1000)
	pub regex_cache_size: usize,
	/// Specifies the number of items which can be cached within a single
	/// transaction (default: 512)
	pub transaction_cache_size: usize,
	/// Specifies the number of definitions which can be cached across transactions
	/// (default: 1,000)
	pub datastore_cache_size: usize,
	/// The maximum number of keys that should be scanned at once for export queries
	/// (default: 1000)
	pub export_batch_size: u32,
	/// Batch size used when allocating sequence-based document IDs for a table's
	/// shared doc-ID space (used by full-text, HNSW and DiskANN indexes). Larger
	/// batches reduce coordination on the distributed sequence at the cost of
	/// larger gaps when a node is lost before exhausting its current batch.
	/// (default: 1000)
	pub table_doc_ids_batch_size: u32,
	/// The number of batches each operator buffers ahead of downstream demand.
	/// Set to 0 to disable operator-level pipeline buffering.
	/// (default: 2)
	pub operator_buffer_size: usize,
	/// Default batch size for scan operators that collect records before
	/// yielding downstream (table/index/record-id/graph/reference scans).
	/// Memory-constrained deployments can reduce this to lower per-pipeline
	/// in-flight memory at the cost of slightly more per-batch dispatch
	/// overhead. Per-batch unit is values, not bytes. (default: 1000)
	pub scan_batch_size: usize,
	/// The maximum size of the priority queue triggering usage of the priority
	/// queue for the result collector.
	pub max_order_limit_priority_queue_size: u32,
	/// Whether eligible `ORDER BY … LIMIT` table scans may skip record decode
	/// for rows that cannot beat the current top-K threshold (default: true)
	pub topk_threshold_pushdown_enabled: bool,
	/// Maximum number of build-side rows a GQL `MATCH` hash join (and the
	/// whole-row `Distinct` dedup that rides the same budget) may hold in memory
	/// before failing the query (default: 1,000,000). Bounds the in-memory
	/// build/seen set for GQL v2 binding-table execution; spill to disk is a
	/// future change (matching the `Aggregate` stance). Errors that trip this
	/// guard name the env knob (`SURREAL_GQL_MAX_JOIN_BUILD_ROWS`).
	pub gql_max_join_build_rows: usize,
	/// Maximum number of paths a single GQL `MATCH` `PathExpand` (variable-length
	/// / quantified edge traversal) may have live on its DFS stack plus already
	/// emitted, per source row, before failing the query (default: 1,000,000).
	/// Bounds the worst-case path explosion of a quantified pattern over a dense
	/// or cyclic graph; edge-uniqueness-within-path guarantees termination but the
	/// number of distinct paths can still be very large. Errors that trip this
	/// guard name the env knob (`SURREAL_GQL_MAX_PATH_ROWS`).
	pub gql_max_path_rows: usize,
	/// Maximum number of rows a single GQL `MATCH` fan-out operator (`HashJoin` —
	/// including the `Cross` cartesian product — and single-hop `Expand`) may
	/// emit, cumulatively across all batches, before failing the query (default:
	/// 1,000,000). Unlike `gql_max_join_build_rows` (which bounds the in-memory
	/// build/seen set), this bounds the *output* product: a cross join of a
	/// bounded build side against a streaming probe side, or a high-fan-out
	/// expand, can emit unboundedly many rows while holding only a small build
	/// set. Errors that trip this guard name the env knob
	/// (`SURREAL_GQL_MAX_OUTPUT_ROWS`).
	pub gql_max_output_rows: usize,
	/// The maximum stack size of the JavaScript function runtime (default: 256 KiB)
	pub scripting_max_stack_size: usize,
	/// The maximum memory limit of the JavaScript function runtime (default: 2 MiB)
	pub scripting_max_memory_limit: usize,
	/// The maximum amount of time that a JavaScript function can run (default: 5
	/// seconds)
	pub scripting_max_time_limit: Duration,
	/// The maximum number of HTTP redirects allowed within http functions (default:
	/// 10)
	pub max_http_redirects: usize,
	/// The maximum number of idle HTTP connections to maintain per host (default: 128)
	pub max_http_idle_connections_per_host: usize,
	/// The maximum number of total idle HTTP connections to maintain (default: 1000)
	pub max_http_idle_connections: usize,
	/// The timeout for idle HTTP connections before closing (default: 90 seconds)
	pub http_idle_timeout_secs: u64,
	/// The timeout for connecting to HTTP endpoints (default: 30 seconds)
	pub http_connect_timeout_secs: u64,
	/// Forward all authentication errors to the client. Do not use in production
	/// (default: false)
	pub insecure_forward_access_errors: bool,
	/// The number of result records which will trigger on-disk sorting (default:
	/// 50,000)
	pub external_sorting_buffer_limit: usize,
	/// Used to limit allocation for builtin functions. Default: 2^20 (1 MiB),
	/// can be as large as 28 (2^28, 256 MiB)
	pub generation_allocation_limit: usize,
	/// The maximum input string length for similarity/distance functions (default:
	/// 16384 bytes)
	pub string_similarity_limit: usize,
	/// Specifies a list of paths in which files can be accessed (default: empty)
	pub file_allowlist: Vec<PathBuf>,
	/// Specify the name of a global bucket for file data (default: None)
	pub global_bucket: Option<String>,
	/// Whether to enforce a global bucket for file data (default: false)
	pub global_bucket_enforced: bool,
	/// Specify the USER-AGENT string used by HTTP requests
	pub surrealdb_user_agent: String,
	/// The maximum total size of the HNSW ANN cache (default: 256 MiB)
	pub hnsw_cache_size: u64,
	/// The maximum total size of the DiskANN ANN cache (default: 256 MiB)
	pub diskann_cache_size: u64,
	/// Specifies the number of surrealism modules which can be cached across transactions
	/// (default: 100)
	pub surrealism_cache_size: usize,
	/// Per-module WASM linear memory ceiling in bytes (default: none / unlimited).
	/// When set, each WASM store is limited via `StoreLimits`. Effective limit is
	/// `min(this, module_config.max_memory_bytes)` when both are set.
	pub surrealism_max_memory: Option<usize>,
	/// Per-invocation execution time ceiling in milliseconds for Surrealism WASM modules
	/// (default: none / unlimited). Combined with module config and query context timeout
	/// via `min()` to produce the effective deadline.
	pub surrealism_max_execution_time: Option<u64>,

	/// Per-module KV store entry count ceiling for Surrealism WASM modules (default: none /
	/// unlimited). Effective limit is `min(this, module_config.max_kv_entries)` when both are
	/// set.
	pub surrealism_max_kv_entries: Option<usize>,
	/// Per-module KV store maximum value size in bytes for Surrealism WASM modules
	/// (default: none / unlimited). Effective limit is
	/// `min(this, module_config.max_kv_value_bytes)` when both are set.
	pub surrealism_max_kv_value_bytes: Option<usize>,
	/// Maximum aggregate size in bytes for attached filesystem entries in `.surli` archives
	/// (default: 100 MiB). Applied when unpacking module archives during `DEFINE MODULE` or
	/// eager loading.
	pub surrealism_max_fs_bytes: u64,
	/// Per-module controller pool size ceiling for Surrealism WASM modules (default: 8).
	/// Each pooled controller holds an instantiated WASM store. Effective pool size is
	/// `min(this, module_config.max_pool_size.unwrap_or(this))`.
	pub surrealism_max_pool_size: usize,
	/// Per-module controller pool size ceiling for Surrealism WASM modules (default: 8).
	/// Each pooled controller holds an instantiated WASM store. Effective pool size is
	/// `min(this, module_config.max_pool_size.unwrap_or(this))`.
	pub surrealism_log_level: String,
	/// Selects the live-query execution engine (default: [`LiveQueryEngine::Inline`]).
	pub live_query_engine: LiveQueryEngine,
	/// Retention window for the dedicated live-query event keyspace (only used when
	/// `live_query_engine` is `Router`). Sized to cover subscriber reconnect and
	/// rolling-upgrade windows during which a subscriber may need to replay missed
	/// events. Independent of any user-defined `CHANGEFEED` retention (default: 1h).
	pub live_query_retention: Duration,
}

impl Default for CommonConfig {
	fn default() -> Self {
		Self {
			memory_threshold: 0,
			#[cfg(not(target_family = "wasm"))]
			max_concurrent_tasks: 64,
			#[cfg(target_family = "wasm")]
			max_concurrent_tasks: 1,
			max_computation_depth: 120,
			max_object_parsing_depth: 100,
			max_query_parsing_depth: 20,
			max_expression_parsing_depth: 128,
			idiom_recursion_limit: 256,
			regex_size_limit: 10 * 1024 * 1024,
			regex_cache_size: 1_000,
			transaction_cache_size: 512,
			datastore_cache_size: 1_000,
			export_batch_size: 1000,
			table_doc_ids_batch_size: 1000,
			operator_buffer_size: 2,
			scan_batch_size: DEFAULT_SCAN_BATCH_SIZE,
			max_order_limit_priority_queue_size: 1000,
			topk_threshold_pushdown_enabled: true,
			gql_max_join_build_rows: 1_000_000,
			gql_max_path_rows: 1_000_000,
			gql_max_output_rows: 1_000_000,
			scripting_max_stack_size: 256 * 1024,
			scripting_max_memory_limit: 2 << 20,
			scripting_max_time_limit: Duration::from_secs(5),
			max_http_redirects: 10,
			max_http_idle_connections_per_host: 128,
			max_http_idle_connections: 1000,
			http_idle_timeout_secs: 90,
			http_connect_timeout_secs: 30,
			insecure_forward_access_errors: false,
			external_sorting_buffer_limit: 50_000,
			generation_allocation_limit: 2 << 20,
			string_similarity_limit: 16384,
			file_allowlist: Vec::new(),
			global_bucket: None,
			global_bucket_enforced: false,
			surrealdb_user_agent: "SurrealDB".to_string(),
			hnsw_cache_size: 256 * 1024 * 1024,
			diskann_cache_size: 256 * 1024 * 1024,
			surrealism_cache_size: 100,
			surrealism_max_memory: None,
			surrealism_max_execution_time: None,
			surrealism_max_kv_entries: None,
			surrealism_max_kv_value_bytes: None,
			surrealism_max_fs_bytes: 100 * 1024 * 1024,
			surrealism_max_pool_size: 8,
			surrealism_log_level: "debug".to_string(),
			live_query_engine: LiveQueryEngine::Inline,
			live_query_retention: Duration::from_secs(3600),
		}
	}
}

impl Config for CommonConfig {
	fn parse(&mut self, map: &ConfigMap) {
		map.parse_key_with("memory_threshold", &mut self.memory_threshold, parse_memory_threshold)
			.parse_key("max_concurrent_tasks", &mut self.max_concurrent_tasks)
			.parse_key("max_computation_depth", &mut self.max_computation_depth)
			.parse_key("max_object_parsing_depth", &mut self.max_object_parsing_depth)
			.parse_key("max_query_parsing_depth", &mut self.max_query_parsing_depth)
			.parse_key("max_expression_parsing_depth", &mut self.max_expression_parsing_depth)
			.parse_key("regex_size_limit", &mut self.regex_size_limit)
			.parse_key("regex_cache_size", &mut self.regex_cache_size)
			.parse_key("transaction_cache_size", &mut self.transaction_cache_size)
			.parse_key("datastore_cache_size", &mut self.datastore_cache_size)
			.parse_key("surrealism_cache_size", &mut self.surrealism_cache_size)
			.parse_key("export_batch_size", &mut self.export_batch_size)
			.parse_key("table_doc_ids_batch_size", &mut self.table_doc_ids_batch_size)
			.parse_key("operator_buffer_size", &mut self.operator_buffer_size)
			.parse_key("scan_batch_size", &mut self.scan_batch_size)
			.parse_key(
				"max_order_limit_priority_queue_size",
				&mut self.max_order_limit_priority_queue_size,
			)
			.parse_key("topk_threshold_pushdown_enabled", &mut self.topk_threshold_pushdown_enabled)
			.parse_key("gql_max_join_build_rows", &mut self.gql_max_join_build_rows)
			.parse_key("gql_max_path_rows", &mut self.gql_max_path_rows)
			.parse_key("gql_max_output_rows", &mut self.gql_max_output_rows)
			.parse_key("scripting_max_stack_size", &mut self.scripting_max_stack_size)
			.parse_key("scripting_max_memory_limit", &mut self.scripting_max_memory_limit)
			.parse_key_with("scripting_max_time_limit", &mut self.scripting_max_time_limit, |x| {
				x.parse().map(Duration::from_millis).ok()
			})
			.parse_key("max_http_redirects", &mut self.max_http_redirects)
			.parse_key(
				"max_http_idle_connections_per_host",
				&mut self.max_http_idle_connections_per_host,
			)
			.parse_key("max_http_idle_connections", &mut self.max_http_idle_connections)
			.parse_key("http_idle_timeout_secs", &mut self.http_idle_timeout_secs)
			.parse_key("http_connect_timeout_secs", &mut self.http_connect_timeout_secs)
			.parse_key("insecure_forward_access_errors", &mut self.insecure_forward_access_errors)
			.parse_key("external_sorting_buffer_limit", &mut self.external_sorting_buffer_limit)
			.parse_key_with(
				"generation_allocation_limit",
				&mut self.generation_allocation_limit,
				|x| x.parse::<usize>().ok().map(|x| 2 << x.min(28)),
			)
			.parse_key("string_similarity_limit", &mut self.string_similarity_limit)
			.parse_key("hnsw_cache_size", &mut self.hnsw_cache_size)
			.parse_key("diskann_cache_size", &mut self.diskann_cache_size)
			.parse_key_with("file_allowlist", &mut self.file_allowlist, |x| {
				// FIXME: We really shouldn't be doing random, faillable, IO when reading
				// configuration values. But no way to fix it without restructuring the
				// datastore entirely.
				Some(extract_allowed_paths(x, true, "file"))
			})
			.parse_key("surrealdb_user_agent", &mut self.surrealdb_user_agent)
			.parse_key_option("surrealism_max_memory", &mut self.surrealism_max_memory)
			.parse_key_option(
				"surrealism_max_execution_time",
				&mut self.surrealism_max_execution_time,
			)
			.parse_key_option("surrealism_max_kv_entries", &mut self.surrealism_max_kv_entries)
			.parse_key_option(
				"surrealism_max_kv_value_bytes",
				&mut self.surrealism_max_kv_value_bytes,
			)
			.parse_key("surrealism_max_fs_bytes", &mut self.surrealism_max_fs_bytes)
			.parse_key_with("surrealism_log_level", &mut self.surrealism_log_level, |s| {
				Some(s.to_string())
			})
			.parse_key("live_query_engine", &mut self.live_query_engine)
			.parse_key_with("live_query_retention", &mut self.live_query_retention, |x| {
				config::parse_duration(x).ok()
			});
	}
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

/// Per-module controller pool size ceiling for Surrealism WASM modules (default: 8).
/// Each pooled controller holds an instantiated WASM store. Effective pool size is
/// `min(this, module_config.max_pool_size.unwrap_or(this))`.
pub static SURREALISM_MAX_POOL_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SURREALISM_MAX_POOL_SIZE", usize, 8);

// The GQL v2 MATCH resource limits (`gql_max_join_build_rows`,
// `gql_max_path_rows`, `gql_max_output_rows`) live on `CommonConfig` above, not
// as global statics: every operator that reads them already has the execution
// `CommonConfig` in hand (`ctx.root().ctx.config`), so they are per-datastore
// and settable programmatically (not only via `SURREAL_GQL_*` env vars).

#[cfg(test)]
mod tests {
	use super::*;

	/// The TopK threshold pushdown kill switch must default on and parse off
	/// from the config map (`SURREAL_TOPK_THRESHOLD_PUSHDOWN_ENABLED=false`).
	/// Disabling it routes planning through the same code path as "no ORDER
	/// BY opportunity" (TopKPushdownRequest::NotApplicable), which the
	/// topk_pushdown language tests cover.
	#[test]
	fn topk_threshold_pushdown_kill_switch_parses() {
		let mut config = CommonConfig::default();
		assert!(config.topk_threshold_pushdown_enabled, "feature defaults on");
		let map = ConfigMap::empty().with_key_value("topk_threshold_pushdown_enabled", "false");
		config.parse(&map);
		assert!(!config.topk_threshold_pushdown_enabled, "config map disables the feature");
	}

	/// The GQL v2 MATCH resource limits live on `CommonConfig` (not as global
	/// statics): they default to 1M and parse from the config map under the same
	/// keys `ConfigMap::from_env` derives from `SURREAL_GQL_MAX_*`, so the env
	/// vars keep working and embedded callers can set them programmatically.
	#[test]
	fn gql_match_limits_parse_from_config_map() {
		let mut config = CommonConfig::default();
		assert_eq!(config.gql_max_join_build_rows, 1_000_000);
		assert_eq!(config.gql_max_path_rows, 1_000_000);
		assert_eq!(config.gql_max_output_rows, 1_000_000);

		let map = ConfigMap::empty()
			.with_key_value("gql_max_join_build_rows", "5")
			.with_key_value("gql_max_path_rows", "7")
			.with_key_value("gql_max_output_rows", "9");
		config.parse(&map);
		assert_eq!(config.gql_max_join_build_rows, 5);
		assert_eq!(config.gql_max_path_rows, 7);
		assert_eq!(config.gql_max_output_rows, 9);
	}

	/// `memory_threshold` in the config map must accept human-readable byte
	/// suffixes (the config-map counterpart to the `SURREAL_MEMORY_THRESHOLD`
	/// env-var fix in `parse_memory_threshold`).  A previous agent only fixed
	/// the legacy env-var path; this test guards the configmap path.
	#[test]
	fn memory_threshold_configmap_parses_byte_suffixes() {
		let mut config = CommonConfig::default();
		assert_eq!(config.memory_threshold, 0, "default is no threshold");

		// Human-readable suffix is honoured (the previously-regressed case).
		let map = ConfigMap::empty().with_key_value("memory_threshold", "1792mb");
		config.parse(&map);
		assert_eq!(config.memory_threshold, 1792 * 1024 * 1024);

		// Another suffix variant.
		let map = ConfigMap::empty().with_key_value("memory_threshold", "1g");
		config.parse(&map);
		assert_eq!(config.memory_threshold, 1024 * 1024 * 1024);

		// Plain byte count still works.
		let map = ConfigMap::empty().with_key_value("memory_threshold", "1879048192");
		config.parse(&map);
		assert_eq!(config.memory_threshold, 1792 * 1024 * 1024);

		// Any non-zero value is floored to at least 1 MiB.
		let map = ConfigMap::empty().with_key_value("memory_threshold", "10");
		config.parse(&map);
		assert_eq!(config.memory_threshold, 1024 * 1024);

		// An unparseable value leaves the field unchanged rather than panicking.
		config.memory_threshold = 0;
		let map = ConfigMap::empty().with_key_value("memory_threshold", "garbage");
		config.parse(&map);
		assert_eq!(config.memory_threshold, 0, "unparseable value must not change the field");
	}

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
