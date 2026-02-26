use std::path::PathBuf;

#[cfg(feature = "cli")]
use crate::parsers::{parse_generation_alloc_limit, parse_memory_threshold, parse_path_list};

// ---------------------------------------------------------------------------
// LimitsConfig
// ---------------------------------------------------------------------------

const DEFAULT_MAX_COMPUTATION_DEPTH: u32 = 120;
const DEFAULT_MAX_OBJECT_PARSING_DEPTH: u32 = 100;
const DEFAULT_MAX_QUERY_PARSING_DEPTH: u32 = 20;
const DEFAULT_IDIOM_RECURSION_LIMIT: usize = 256;
const DEFAULT_REGEX_SIZE_LIMIT: usize = 10 * 1024 * 1024;
const DEFAULT_MAX_CONCURRENT_TASKS: usize = 64;
const DEFAULT_GENERATION_ALLOCATION_LIMIT: usize = 1_048_576; // 2^20
const DEFAULT_STRING_SIMILARITY_LIMIT: usize = 16384;
const DEFAULT_MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE: u32 = 1000;
const DEFAULT_OPERATOR_BUFFER_SIZE: usize = 2;
const DEFAULT_EXTERNAL_SORTING_BUFFER_LIMIT: usize = 50_000;
const DEFAULT_MEMORY_THRESHOLD: usize = 0;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct LimitsConfig {
	/// The memory usage threshold before tasks are forced to exit (default: 0 = disabled).
	/// Non-zero values are clamped to at least 1 MiB.
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MEMORY_THRESHOLD",
		long = "memory-threshold",
		default_value_t = DEFAULT_MEMORY_THRESHOLD,
		hide = true,
		value_parser = parse_memory_threshold,
	))]
	pub memory_threshold: usize,
	/// Specifies how deep recursive computation will go before erroring
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MAX_COMPUTATION_DEPTH",
		long = "max-computation-depth",
		default_value_t = DEFAULT_MAX_COMPUTATION_DEPTH,
		hide = true,
	))]
	pub max_computation_depth: u32,
	/// Specifies how deep the parser will parse nested objects and arrays
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MAX_OBJECT_PARSING_DEPTH",
		long = "max-object-parsing-depth",
		default_value_t = DEFAULT_MAX_OBJECT_PARSING_DEPTH,
		hide = true,
	))]
	pub max_object_parsing_depth: u32,
	/// Specifies how deep the parser will parse recursive queries
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MAX_QUERY_PARSING_DEPTH",
		long = "max-query-parsing-depth",
		default_value_t = DEFAULT_MAX_QUERY_PARSING_DEPTH,
		hide = true,
	))]
	pub max_query_parsing_depth: u32,
	/// The maximum recursive idiom path depth allowed
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_IDIOM_RECURSION_LIMIT",
		long = "idiom-recursion-limit",
		default_value_t = DEFAULT_IDIOM_RECURSION_LIMIT,
		hide = true,
	))]
	pub idiom_recursion_limit: usize,
	/// The maximum size of a compiled regular expression (bytes)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_REGEX_SIZE_LIMIT",
		long = "regex-size-limit",
		default_value_t = DEFAULT_REGEX_SIZE_LIMIT,
		hide = true,
	))]
	pub regex_size_limit: usize,
	/// Specifies how many concurrent jobs can be buffered in the worker channel
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MAX_CONCURRENT_TASKS",
		long = "max-concurrent-tasks",
		default_value_t = DEFAULT_MAX_CONCURRENT_TASKS,
		hide = true,
	))]
	pub max_concurrent_tasks: usize,
	/// Used to limit allocation for builtin functions (accepts exponent N, stores 2^N clamped to
	/// 2^28)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_GENERATION_ALLOCATION_LIMIT",
		long = "generation-allocation-limit",
		default_value_t = DEFAULT_GENERATION_ALLOCATION_LIMIT,
		hide = true,
		value_parser = parse_generation_alloc_limit,
	))]
	pub generation_allocation_limit: usize,
	/// The maximum input string length for similarity/distance functions
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_STRING_SIMILARITY_LIMIT",
		long = "string-similarity-limit",
		default_value_t = DEFAULT_STRING_SIMILARITY_LIMIT,
		hide = true,
	))]
	pub string_similarity_limit: usize,
	/// The threshold triggering priority-queue-based result collection
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE",
		long = "max-order-limit-priority-queue-size",
		default_value_t = DEFAULT_MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE,
		hide = true,
	))]
	pub max_order_limit_priority_queue_size: u32,
	/// The number of batches each operator buffers ahead of downstream demand
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_OPERATOR_BUFFER_SIZE",
		long = "operator-buffer-size",
		default_value_t = DEFAULT_OPERATOR_BUFFER_SIZE,
		hide = true,
	))]
	pub operator_buffer_size: usize,
	/// The number of result records which will trigger on-disk sorting
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_EXTERNAL_SORTING_BUFFER_LIMIT",
		long = "external-sorting-buffer-limit",
		default_value_t = DEFAULT_EXTERNAL_SORTING_BUFFER_LIMIT,
		hide = true,
	))]
	pub external_sorting_buffer_limit: usize,
}

impl Default for LimitsConfig {
	fn default() -> Self {
		Self {
			memory_threshold: DEFAULT_MEMORY_THRESHOLD,
			max_computation_depth: DEFAULT_MAX_COMPUTATION_DEPTH,
			max_object_parsing_depth: DEFAULT_MAX_OBJECT_PARSING_DEPTH,
			max_query_parsing_depth: DEFAULT_MAX_QUERY_PARSING_DEPTH,
			idiom_recursion_limit: DEFAULT_IDIOM_RECURSION_LIMIT,
			regex_size_limit: DEFAULT_REGEX_SIZE_LIMIT,
			max_concurrent_tasks: DEFAULT_MAX_CONCURRENT_TASKS,
			generation_allocation_limit: DEFAULT_GENERATION_ALLOCATION_LIMIT,
			string_similarity_limit: DEFAULT_STRING_SIMILARITY_LIMIT,
			max_order_limit_priority_queue_size: DEFAULT_MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE,
			operator_buffer_size: DEFAULT_OPERATOR_BUFFER_SIZE,
			external_sorting_buffer_limit: DEFAULT_EXTERNAL_SORTING_BUFFER_LIMIT,
		}
	}
}

// ---------------------------------------------------------------------------
// ScriptingConfig
// ---------------------------------------------------------------------------

const DEFAULT_SCRIPTING_MAX_STACK_SIZE: usize = 256 * 1024;
const DEFAULT_SCRIPTING_MAX_MEMORY_LIMIT: usize = 2 << 20;
const DEFAULT_SCRIPTING_MAX_TIME_LIMIT: usize = 5 * 1000;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct ScriptingConfig {
	/// The maximum stack size of the JavaScript function runtime (bytes)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SCRIPTING_MAX_STACK_SIZE",
		long = "scripting-max-stack-size",
		default_value_t = DEFAULT_SCRIPTING_MAX_STACK_SIZE,
		hide = true,
	))]
	pub max_stack_size: usize,
	/// The maximum memory limit of the JavaScript function runtime (bytes)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SCRIPTING_MAX_MEMORY_LIMIT",
		long = "scripting-max-memory-limit",
		default_value_t = DEFAULT_SCRIPTING_MAX_MEMORY_LIMIT,
		hide = true,
	))]
	pub max_memory_limit: usize,
	/// The maximum amount of time that a JavaScript function can run (ms)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SCRIPTING_MAX_TIME_LIMIT",
		long = "scripting-max-time-limit",
		default_value_t = DEFAULT_SCRIPTING_MAX_TIME_LIMIT,
		hide = true,
	))]
	pub max_time_limit: usize,
}

impl Default for ScriptingConfig {
	fn default() -> Self {
		Self {
			max_stack_size: DEFAULT_SCRIPTING_MAX_STACK_SIZE,
			max_memory_limit: DEFAULT_SCRIPTING_MAX_MEMORY_LIMIT,
			max_time_limit: DEFAULT_SCRIPTING_MAX_TIME_LIMIT,
		}
	}
}

// ---------------------------------------------------------------------------
// HttpClientConfig
// ---------------------------------------------------------------------------

const DEFAULT_MAX_HTTP_REDIRECTS: usize = 10;
const DEFAULT_MAX_HTTP_IDLE_CONNECTIONS_PER_HOST: usize = 128;
const DEFAULT_MAX_HTTP_IDLE_CONNECTIONS: usize = 1000;
const DEFAULT_HTTP_IDLE_TIMEOUT_SECS: u64 = 90;
const DEFAULT_HTTP_CONNECT_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct HttpClientConfig {
	/// The maximum number of HTTP redirects allowed
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MAX_HTTP_REDIRECTS",
		long = "max-http-redirects",
		default_value_t = DEFAULT_MAX_HTTP_REDIRECTS,
		hide = true,
	))]
	pub max_redirects: usize,
	/// The maximum number of idle HTTP connections to maintain per host
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MAX_HTTP_IDLE_CONNECTIONS_PER_HOST",
		long = "max-http-idle-connections-per-host",
		default_value_t = DEFAULT_MAX_HTTP_IDLE_CONNECTIONS_PER_HOST,
		hide = true,
	))]
	pub max_idle_connections_per_host: usize,
	/// The maximum number of total idle HTTP connections to maintain
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_MAX_HTTP_IDLE_CONNECTIONS",
		long = "max-http-idle-connections",
		default_value_t = DEFAULT_MAX_HTTP_IDLE_CONNECTIONS,
		hide = true,
	))]
	pub max_idle_connections: usize,
	/// The timeout for idle HTTP connections before closing (seconds)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_HTTP_IDLE_TIMEOUT_SECS",
		long = "http-idle-timeout-secs",
		default_value_t = DEFAULT_HTTP_IDLE_TIMEOUT_SECS,
		hide = true,
	))]
	pub idle_timeout_secs: u64,
	/// The timeout for connecting to HTTP endpoints (seconds)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_HTTP_CONNECT_TIMEOUT_SECS",
		long = "http-connect-timeout-secs",
		default_value_t = DEFAULT_HTTP_CONNECT_TIMEOUT_SECS,
		hide = true,
	))]
	pub connect_timeout_secs: u64,
	/// The USER-AGENT string used by HTTP requests
	#[cfg_attr(
		feature = "cli",
		arg(
			env = "SURREAL_USER_AGENT",
			long = "user-agent",
			default_value = "SurrealDB",
			hide = true,
		)
	)]
	pub user_agent: String,
}

impl Default for HttpClientConfig {
	fn default() -> Self {
		Self {
			max_redirects: DEFAULT_MAX_HTTP_REDIRECTS,
			max_idle_connections_per_host: DEFAULT_MAX_HTTP_IDLE_CONNECTIONS_PER_HOST,
			max_idle_connections: DEFAULT_MAX_HTTP_IDLE_CONNECTIONS,
			idle_timeout_secs: DEFAULT_HTTP_IDLE_TIMEOUT_SECS,
			connect_timeout_secs: DEFAULT_HTTP_CONNECT_TIMEOUT_SECS,
			user_agent: "SurrealDB".to_string(),
		}
	}
}

// ---------------------------------------------------------------------------
// CacheConfig
// ---------------------------------------------------------------------------

const DEFAULT_TRANSACTION_CACHE_SIZE: usize = 10_000;
const DEFAULT_DATASTORE_CACHE_SIZE: usize = 1_000;
const DEFAULT_SURREALISM_CACHE_SIZE: usize = 100;
const DEFAULT_HNSW_CACHE_SIZE: u64 = 256 * 1024 * 1024;
const DEFAULT_REGEX_CACHE_SIZE: usize = 1_000;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct CacheConfig {
	/// The number of computed regexes cached in the engine
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_REGEX_CACHE_SIZE",
		long = "regex-cache-size",
		default_value_t = DEFAULT_REGEX_CACHE_SIZE,
		hide = true,
	))]
	pub regex_cache_size: usize,
	/// The number of items cached within a single transaction
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_TRANSACTION_CACHE_SIZE",
		long = "transaction-cache-size",
		default_value_t = DEFAULT_TRANSACTION_CACHE_SIZE,
		hide = true,
	))]
	pub transaction_cache_size: usize,
	/// The number of definitions cached across transactions
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_DATASTORE_CACHE_SIZE",
		long = "datastore-cache-size",
		default_value_t = DEFAULT_DATASTORE_CACHE_SIZE,
		hide = true,
	))]
	pub datastore_cache_size: usize,
	/// The number of surrealism modules cached across transactions
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_SURREALISM_CACHE_SIZE",
		long = "surrealism-cache-size",
		default_value_t = DEFAULT_SURREALISM_CACHE_SIZE,
		hide = true,
	))]
	pub surrealism_cache_size: usize,
	/// The maximum size of the HNSW vector cache (bytes)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_HNSW_CACHE_SIZE",
		long = "hnsw-cache-size",
		default_value_t = DEFAULT_HNSW_CACHE_SIZE,
		hide = true,
	))]
	pub hnsw_cache_size: u64,
}

impl Default for CacheConfig {
	fn default() -> Self {
		Self {
			regex_cache_size: DEFAULT_REGEX_CACHE_SIZE,
			transaction_cache_size: DEFAULT_TRANSACTION_CACHE_SIZE,
			datastore_cache_size: DEFAULT_DATASTORE_CACHE_SIZE,
			surrealism_cache_size: DEFAULT_SURREALISM_CACHE_SIZE,
			hnsw_cache_size: DEFAULT_HNSW_CACHE_SIZE,
		}
	}
}

// ---------------------------------------------------------------------------
// BatchConfig
// ---------------------------------------------------------------------------

const DEFAULT_NORMAL_FETCH_SIZE: u32 = 500;
const DEFAULT_EXPORT_BATCH_SIZE: u32 = 1000;
const DEFAULT_COUNT_BATCH_SIZE: u32 = 50_000;
const DEFAULT_INDEXING_BATCH_SIZE: u32 = 250;

#[derive(Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct BatchConfig {
	/// The maximum number of keys scanned at once in general queries
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_NORMAL_FETCH_SIZE",
		long = "normal-fetch-size",
		default_value_t = DEFAULT_NORMAL_FETCH_SIZE,
		hide = true,
	))]
	pub normal_fetch_size: u32,
	/// The maximum number of keys scanned at once for export queries
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_EXPORT_BATCH_SIZE",
		long = "export-batch-size",
		default_value_t = DEFAULT_EXPORT_BATCH_SIZE,
		hide = true,
	))]
	pub export_batch_size: u32,
	/// The maximum number of keys scanned at once for count queries
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_COUNT_BATCH_SIZE",
		long = "count-batch-size",
		default_value_t = DEFAULT_COUNT_BATCH_SIZE,
		hide = true,
	))]
	pub count_batch_size: u32,
	/// The maximum number of keys to scan at once per concurrent indexing batch
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_INDEXING_BATCH_SIZE",
		long = "indexing-batch-size",
		default_value_t = DEFAULT_INDEXING_BATCH_SIZE,
		hide = true,
	))]
	pub indexing_batch_size: u32,
}

impl Default for BatchConfig {
	fn default() -> Self {
		Self {
			normal_fetch_size: DEFAULT_NORMAL_FETCH_SIZE,
			export_batch_size: DEFAULT_EXPORT_BATCH_SIZE,
			count_batch_size: DEFAULT_COUNT_BATCH_SIZE,
			indexing_batch_size: DEFAULT_INDEXING_BATCH_SIZE,
		}
	}
}

// ---------------------------------------------------------------------------
// SecurityConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct SecurityConfig {
	/// Forward all authentication errors to the client. Do not use in production.
	#[cfg_attr(
		feature = "cli",
		arg(
			env = "SURREAL_INSECURE_FORWARD_ACCESS_ERRORS",
			long = "insecure-forward-access-errors",
			default_value_t = false,
			hide = true,
		)
	)]
	pub insecure_forward_access_errors: bool,
}

// ---------------------------------------------------------------------------
// FileConfig
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "cli", derive(clap::Args))]
pub struct FileConfig {
	/// Paths in which files can be accessed (colon-separated, semicolon on Windows)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_FILE_ALLOWLIST",
		long = "file-allowlist",
		default_value = "",
		hide = true,
		value_parser = parse_path_list,
	))]
	pub file_allowlist: Vec<PathBuf>,
	/// Paths in which bucket folders can be accessed (colon-separated, semicolon on Windows)
	#[cfg_attr(feature = "cli", arg(
		env = "SURREAL_BUCKET_FOLDER_ALLOWLIST",
		long = "bucket-folder-allowlist",
		default_value = "",
		hide = true,
		value_parser = parse_path_list,
	))]
	pub bucket_folder_allowlist: Vec<PathBuf>,
	/// The name of a global bucket for file data
	#[cfg_attr(
		feature = "cli",
		arg(env = "SURREAL_GLOBAL_BUCKET", long = "global-bucket", hide = true,)
	)]
	pub global_bucket: Option<String>,
	/// Whether to enforce a global bucket for file data
	#[cfg_attr(
		feature = "cli",
		arg(
			env = "SURREAL_GLOBAL_BUCKET_ENFORCED",
			long = "global-bucket-enforced",
			default_value_t = false,
			hide = true,
		)
	)]
	pub global_bucket_enforced: bool,
}
