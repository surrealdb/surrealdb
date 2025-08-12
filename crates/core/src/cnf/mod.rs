use std::path::PathBuf;
use std::sync::LazyLock;

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

/// The memory usage threshold before tasks are forced to exit (default: 0
/// bytes)
pub static MEMORY_THRESHOLD: LazyLock<usize> =
	lazy_env_parse!(bytes, "SURREAL_MEMORY_THRESHOLD", usize, 0);

/// Specifies how many concurrent jobs can be buffered in the worker channel
#[cfg(not(target_family = "wasm"))]
pub static MAX_CONCURRENT_TASKS: LazyLock<usize> =
	lazy_env_parse!("SURREAL_MAX_CONCURRENT_TASKS", usize, 64);

/// Specifies how deep recursive computation will go before erroring (default:
/// 120)
pub static MAX_COMPUTATION_DEPTH: LazyLock<u32> =
	lazy_env_parse!("SURREAL_MAX_COMPUTATION_DEPTH", u32, 120);

/// Specifies how deep the parser will parse nested objects and arrays (default:
/// 100)
pub static MAX_OBJECT_PARSING_DEPTH: LazyLock<u32> =
	lazy_env_parse!("SURREAL_MAX_OBJECT_PARSING_DEPTH", u32, 100);

/// Specifies how deep the parser will parse recursive queries (default: 20)
pub static MAX_QUERY_PARSING_DEPTH: LazyLock<u32> =
	lazy_env_parse!("SURREAL_MAX_QUERY_PARSING_DEPTH", u32, 20);

/// The maximum recursive idiom path depth allowed (default: 256)
pub static IDIOM_RECURSION_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_IDIOM_RECURSION_LIMIT", usize, 256);

/// The maximum size of a compiled regular expression (default: 10 MiB)
pub static REGEX_SIZE_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_REGEX_SIZE_LIMIT", usize, 10 * 1024 * 1024);

/// Specifies the number of computed regexes which can be cached in the engine
/// (default: 1000)
pub static REGEX_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_REGEX_CACHE_SIZE", usize, 1_000);

/// Specifies the number of items which can be cached within a single
/// transaction (default: 10,000)
pub static TRANSACTION_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_TRANSACTION_CACHE_SIZE", usize, 10_000);

/// Specifies the number of definitions which can be cached across transactions
/// (default: 1,000)
pub static DATASTORE_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_DATASTORE_CACHE_SIZE", usize, 1_000);

/// The maximum number of keys that should be scanned at once in general queries
/// (default: 500)
pub static NORMAL_FETCH_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_NORMAL_FETCH_SIZE", u32, 500);

/// The maximum number of keys that should be scanned at once for export queries
/// (default: 1000)
pub static EXPORT_BATCH_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_EXPORT_BATCH_SIZE", u32, 1000);

/// The maximum number of keys that should be scanned at once for count queries
/// (default: 10,000)
pub static COUNT_BATCH_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_COUNT_BATCH_SIZE", u32, 10_000);

/// The maximum number of keys to scan at once per concurrent indexing batch
/// (default: 250)
pub static INDEXING_BATCH_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_INDEXING_BATCH_SIZE", u32, 250);

/// The maximum size of the priority queue triggering usage of the priority
/// queue for the result collector.
pub static MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE", u32, 1000);

/// The maximum stack size of the JavaScript function runtime (default: 256 KiB)
pub static SCRIPTING_MAX_STACK_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SCRIPTING_MAX_STACK_SIZE", usize, 256 * 1024);

/// The maximum memory limit of the JavaScript function runtime (default: 2 MiB)
pub static SCRIPTING_MAX_MEMORY_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SCRIPTING_MAX_MEMORY_LIMIT", usize, 2 << 20);

/// The maximum amount of time that a JavaScript function can run (default: 5
/// seconds)
pub static SCRIPTING_MAX_TIME_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SCRIPTING_MAX_TIME_LIMIT", usize, 5 * 1000);

/// The maximum number of HTTP redirects allowed within http functions (default:
/// 10)
pub static MAX_HTTP_REDIRECTS: LazyLock<usize> =
	lazy_env_parse!("SURREAL_MAX_HTTP_REDIRECTS", usize, 10);

/// Forward all authentication errors to the client. Do not use in production
/// (default: false)
pub static INSECURE_FORWARD_ACCESS_ERRORS: LazyLock<bool> =
	lazy_env_parse!("SURREAL_INSECURE_FORWARD_ACCESS_ERRORS", bool, false);

/// The number of result records which will trigger on-disk sorting (default:
/// 50,000)
#[cfg(storage)]
pub static EXTERNAL_SORTING_BUFFER_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_EXTERNAL_SORTING_BUFFER_LIMIT", usize, 50_000);

/// Used to limit allocation for builtin functions
pub static GENERATION_ALLOCATION_LIMIT: LazyLock<usize> = LazyLock::new(|| {
	let n = std::env::var("SURREAL_GENERATION_ALLOCATION_LIMIT")
		.map(|s| s.parse::<u32>().unwrap_or(20))
		.unwrap_or(20);
	2usize.pow(n)
});

/// Specifies a list of paths in which files can be accessed (default: empty)
pub static FILE_ALLOWLIST: LazyLock<Vec<PathBuf>> = LazyLock::new(|| {
	std::env::var("SURREAL_FILE_ALLOWLIST")
		.map(|input| extract_allowed_paths(&input, true, "file"))
		.unwrap_or_default()
});

/// Specifies a list of paths in which files can be accessed (default: empty)
pub static BUCKET_FOLDER_ALLOWLIST: LazyLock<Vec<PathBuf>> = LazyLock::new(|| {
	std::env::var("SURREAL_BUCKET_FOLDER_ALLOWLIST")
		.map(|input| extract_allowed_paths(&input, false, "bucket folder"))
		.unwrap_or_default()
});

/// Specify the name of a global bucket for file data (default: None)
pub static GLOBAL_BUCKET: LazyLock<Option<String>> =
	lazy_env_parse!("SURREAL_GLOBAL_BUCKET", Option<String>);

/// Whether to enforce a global bucket for file data (default: false)
pub static GLOBAL_BUCKET_ENFORCED: LazyLock<bool> =
	lazy_env_parse!("SURREAL_GLOBAL_BUCKET_ENFORCED", bool, false);

/// Whether to output in a form readable for devices like screen and braille
/// readers For example, by showing ⟨ and ⟩ as `
pub static ACCESSIBLE_OUTPUT: LazyLock<bool> =
	lazy_env_parse!("SURREAL_ACCESSIBLE_OUTPUT", bool, false);

/// Specify the USER-AGENT string used by HTTP requests
pub static SURREALDB_USER_AGENT: LazyLock<String> =
	LazyLock::new(|| std::env::var("SURREAL_USER_AGENT").unwrap_or("SurrealDB".to_string()));
