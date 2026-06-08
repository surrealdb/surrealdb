pub(crate) mod dynamic;

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::Duration;

use crate::iam::file::extract_allowed_paths;

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

/// A map with a set of configuration values stored as pairs of strings.
#[derive(Clone, Debug)]
pub struct ConfigMap {
	values: HashMap<String, String>,
}

impl Default for ConfigMap {
	fn default() -> Self {
		Self::empty()
	}
}

impl ConfigMap {
	/// Returns an empty map.
	pub fn empty() -> Self {
		ConfigMap {
			values: HashMap::new(),
		}
	}

	/// Adds a new key and value into the config map, will overwrite existing values.
	pub fn with_key_value<K, V>(mut self, key: K, value: V) -> Self
	where
		String: From<K>,
		String: From<V>,
	{
		self.values.insert(key.into(), value.into());
		self
	}

	/// Creates a config map from all the environment variables prefixed with `SURREAL_`
	pub fn from_env() -> Self {
		Self::from_env_prefix("SURREAL_")
	}

	/// Creates a config map from all the environment variables prefixed with the specific prefix.
	pub fn from_env_prefix(prefix: &str) -> Self {
		let mut values = HashMap::new();
		for (k, v) in std::env::vars() {
			let Some(x) = k.strip_prefix(prefix) else {
				continue;
			};

			let key_name = x.to_lowercase();
			values.insert(key_name, v);
		}
		ConfigMap {
			values,
		}
	}

	/// Map all the keys in the config map with the given closure.
	pub fn map_keys<F: FnMut(String) -> String>(self, mut f: F) -> Self {
		Self {
			values: self.values.into_iter().map(|(k, v)| (f(k), v)).collect(),
		}
	}

	/// Creates a config map from all the environment variables
	pub fn from_config_string(s: &str) -> Self {
		let values = s
			.split('&')
			.filter_map(|x| {
				let (k, v) = x.split_once('=')?;
				Some((k.to_lowercase(), v.to_string()))
			})
			.collect();

		ConfigMap {
			values,
		}
	}

	/// Join two config maps together prefering the values not in self.
	pub fn join(mut self, other: ConfigMap) -> Self {
		for (k, v) in other.values {
			self.values.insert(k, v);
		}
		self
	}

	/// Load a config type from the map.
	pub fn load<C: Config>(&self) -> C {
		let mut def = C::default();
		def.parse(self);
		def
	}

	/// Parse a value out of the map if it exists.
	///
	/// If either the key does not exist or the parsing values the value is unaltered.
	pub fn parse_key<S: FromStr>(&self, key: &str, value: &mut S) -> &Self {
		self.parse_key_with(key, value, |x| S::from_str(x).ok())
	}

	pub fn parse_key_option<S: FromStr>(&self, key: &str, value: &mut Option<S>) -> &Self {
		self.parse_key_with(key, value, |x| S::from_str(x).ok().map(Some))
	}

	/// Parse a boolean out of the map if it exists.
	///
	/// If either the key does not exist or the parsing values the value is unaltered.
	pub fn parse_key_bool(&self, key: &str, value: &mut bool) -> &Self {
		self.parse_key_with(key, value, |x| {
			if x.eq_ignore_ascii_case("true") || x == "1" {
				Some(true)
			} else if x.eq_ignore_ascii_case("false") || x == "0" {
				Some(false)
			} else {
				None
			}
		})
	}

	/// Parse a value out of the map if it exists.
	/// Takes a closure which can be used to define how to parse the string
	///
	/// If either the key does not exist or the parsing closure returns `None` the value is
	/// unaltered.
	pub fn parse_key_with<R, F: FnOnce(&str) -> Option<R>>(
		&self,
		key: &str,
		value: &mut R,
		f: F,
	) -> &Self {
		let Some(v) = self.values.get(key) else {
			return self;
		};

		let Some(v) = f(v) else {
			warn!("Could not parse configuration value for key `{}`", key.to_uppercase());
			return self;
		};

		*value = v;
		self
	}

	pub fn has_key(&self, key: &str) -> bool {
		self.values.contains_key(key)
	}
}

/// Trait for types which contain configureation information.
pub trait Config: Default {
	fn parse(&mut self, map: &ConfigMap);
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
			idiom_recursion_limit: 256,
			regex_size_limit: 10 * 1024 * 1024,
			regex_cache_size: 1_000,
			transaction_cache_size: 512,
			datastore_cache_size: 1_000,
			export_batch_size: 1000,
			operator_buffer_size: 2,
			scan_batch_size: crate::exec::operators::scan::common::DEFAULT_SCAN_BATCH_SIZE,
			max_order_limit_priority_queue_size: 1000,
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
		}
	}
}

impl Config for CommonConfig {
	fn parse(&mut self, map: &ConfigMap) {
		map.parse_key_with("memory_threshold", &mut self.memory_threshold, |x| {
			x.parse::<usize>().map(|x| x.max(1024 * 1024)).ok()
		})
		.parse_key("max_concurrent_tasks", &mut self.max_concurrent_tasks)
		.parse_key("max_computation_depth", &mut self.max_computation_depth)
		.parse_key("max_object_parsing_depth", &mut self.max_object_parsing_depth)
		.parse_key("max_query_parsing_depth", &mut self.max_query_parsing_depth)
		.parse_key("regex_size_limit", &mut self.regex_size_limit)
		.parse_key("regex_cache_size", &mut self.regex_cache_size)
		.parse_key("transaction_cache_size", &mut self.transaction_cache_size)
		.parse_key("datastore_cache_size", &mut self.datastore_cache_size)
		.parse_key("surrealism_cache_size", &mut self.surrealism_cache_size)
		.parse_key("export_batch_size", &mut self.export_batch_size)
		.parse_key("operator_buffer_size", &mut self.operator_buffer_size)
		.parse_key("scan_batch_size", &mut self.scan_batch_size)
		.parse_key(
			"max_order_limit_priority_queue_size",
			&mut self.max_order_limit_priority_queue_size,
		)
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
		.parse_key_with("generation_allocation_limit", &mut self.generation_allocation_limit, |x| {
			x.parse::<usize>().ok().map(|x| 2 << x.min(28))
		})
		.parse_key("string_similarity_limit", &mut self.string_similarity_limit)
		.parse_key("hnsw_cache_size", &mut self.hnsw_cache_size)
		.parse_key("diskann_cache_size", &mut self.diskann_cache_size)
		.parse_key_with("file_allowlist", &mut self.file_allowlist, |x| {
			// FIXME: We really shouldn't be doing random, faillable, IO when reading configuration
			// values. But no way to fix it without restructuring the datastore entirely.
			Some(extract_allowed_paths(x, true, "file"))
		})
		.parse_key("surrealdb_user_agent", &mut self.surrealdb_user_agent)
		.parse_key_option("surrealism_max_memory", &mut self.surrealism_max_memory)
		.parse_key_option("surrealism_max_execution_time", &mut self.surrealism_max_execution_time)
		.parse_key_option("surrealism_max_kv_entries", &mut self.surrealism_max_kv_entries)
		.parse_key_option("surrealism_max_kv_value_bytes", &mut self.surrealism_max_kv_value_bytes)
		.parse_key("surrealism_max_fs_bytes", &mut self.surrealism_max_fs_bytes)
		.parse_key_with("surrealism_log_level", &mut self.surrealism_log_level, |s| {
			Some(s.to_string())
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
	let n = std::env::var("SURREAL_MEMORY_THRESHOLD")
		.map(|s| s.parse::<usize>().unwrap_or(0))
		.unwrap_or(0);
	match n {
		default @ 0 => default,
		specified => std::cmp::max(specified, 1024 * 1024),
	}
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

/// Number of worker threads in the shared KVS blocking threadpool
/// (`surrealdb-threadpool`) used by the `kv-mem`, `kv-rocksdb`, and
/// `kv-surrealkv` storage backends to run synchronous storage work off the
/// tokio runtime.
///
/// Default: `num_cpus::get()` on hosts with at least 16 logical cores
/// (matching the legacy `thread_per_core` behaviour with one pinned
/// worker per core), `16` on smaller hosts. Override with
/// `SURREAL_KVS_THREADPOOL_SIZE=<N>` (minimum `4`) to oversubscribe (more
/// concurrent blocking-IO slots, useful when many workers stall on disk
/// reads or fsyncs) or undersubscribe (cap blocking concurrency below
/// core count).
///
/// Explicit overrides drop the per-core CPU pinning that the default
/// applies on >=16-core hosts — pinning only makes sense when the
/// worker count exactly matches the core count.
///
/// **Minimum: 4.** Some kvs operations always run on this pool — read-only
/// `count` with sharded fan-out, `compact`, writable scans — and below ~4
/// workers their throughput collapses (sharded `COUNT(*)` becomes serial,
/// `compact` blocks all other always-pool work). Values below 4, non-numeric
/// values, and an empty string are reported via `tracing::warn!` and the
/// computed default is used instead.
#[cfg(any(feature = "kv-mem", feature = "kv-rocksdb", feature = "kv-surrealkv"))]
#[cfg(not(target_family = "wasm"))]
pub static KVS_THREADPOOL_SIZE: LazyLock<usize> = LazyLock::new(|| {
	let default = || {
		let cores = num_cpus::get();
		if cores >= 16 {
			cores
		} else {
			16
		}
	};
	const MINIMUM_OVERRIDE: usize = 4;
	match std::env::var("SURREAL_KVS_THREADPOOL_SIZE") {
		Err(_) => default(),
		Ok(s) if s.is_empty() => default(),
		Ok(s) => match s.parse::<usize>() {
			Ok(n) if n >= MINIMUM_OVERRIDE => n,
			Ok(n) => {
				tracing::warn!(
					target: "surrealdb::kvs::threadpool",
					"SURREAL_KVS_THREADPOOL_SIZE={n} is below the minimum of {MINIMUM_OVERRIDE}; using default",
				);
				default()
			}
			Err(_) => {
				tracing::warn!(
					target: "surrealdb::kvs::threadpool",
					"SURREAL_KVS_THREADPOOL_SIZE={s:?} is not a valid integer; using default",
				);
				default()
			}
		},
	}
});
