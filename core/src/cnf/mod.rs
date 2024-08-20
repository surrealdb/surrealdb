use once_cell::sync::Lazy;

/// The characters which are supported in server record IDs.
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// The publicly visible name of the server
pub const SERVER_NAME: &str = "SurrealDB";

/// Specifies the names of parameters which can not be specified in a query.
pub const PROTECTED_PARAM_NAMES: &[&str] = &["access", "auth", "token", "session"];

/// Specifies how many concurrent jobs can be buffered in the worker channel.
#[cfg(not(target_arch = "wasm32"))]
pub static MAX_CONCURRENT_TASKS: Lazy<usize> =
	lazy_env_parse!("SURREAL_MAX_CONCURRENT_TASKS", usize, 64);

/// Specifies how deep computation recursive call will go before en error is returned.
pub static MAX_COMPUTATION_DEPTH: Lazy<u32> =
	lazy_env_parse!("SURREAL_MAX_COMPUTATION_DEPTH", u32, 120);

/// Specifies the number of items which can be cached within a single transaction.
pub static TRANSACTION_CACHE_SIZE: Lazy<usize> =
	lazy_env_parse!("SURREAL_TRANSACTION_CACHE_SIZE", usize, 10_000);

/// The maximum number of keys that should be scanned at once in general queries.
pub static NORMAL_FETCH_SIZE: Lazy<u32> = lazy_env_parse!("SURREAL_NORMAL_FETCH_SIZE", u32, 50);

/// The maximum number of keys that should be scanned at once for export queries.
pub static EXPORT_BATCH_SIZE: Lazy<u32> = lazy_env_parse!("SURREAL_EXPORT_BATCH_SIZE", u32, 1000);

/// The maximum number of keys that should be fetched when streaming range scans in a Scanner.
pub static MAX_STREAM_BATCH_SIZE: Lazy<u32> =
	lazy_env_parse!("SURREAL_MAX_STREAM_BATCH_SIZE", u32, 1000);

/// The maximum number of keys that should be scanned at once per concurrent indexing batch.
pub static INDEXING_BATCH_SIZE: Lazy<u32> =
	lazy_env_parse!("SURREAL_INDEXING_BATCH_SIZE", u32, 250);

/// Forward all signup/signin/authenticate query errors to a client performing authentication. Do not use in production.
pub static INSECURE_FORWARD_ACCESS_ERRORS: Lazy<bool> =
	lazy_env_parse!("SURREAL_INSECURE_FORWARD_ACCESS_ERRORS", bool, false);

#[cfg(any(
	feature = "kv-mem",
	feature = "kv-surrealkv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
))]
/// Specifies the buffer limit for external sorting.
/// If the environment variable is not present or cannot be parsed, a default value of 50,000 is used.
pub static EXTERNAL_SORTING_BUFFER_LIMIT: Lazy<usize> =
	lazy_env_parse!("SURREAL_EXTERNAL_SORTING_BUFFER_LIMIT", usize, 50_000);

pub static GRAPHQL_ENABLE: Lazy<bool> =
	lazy_env_parse!("SURREAL_EXPERIMENTAL_GRAPHQL", bool, false);

/// Enable experimental bearer access and stateful access grant management. Still under active development.
/// Using this experimental feature may introduce risks related to breaking changes and security issues.
#[cfg(not(test))]
pub static EXPERIMENTAL_BEARER_ACCESS: Lazy<bool> =
	lazy_env_parse!("SURREAL_EXPERIMENTAL_BEARER_ACCESS", bool, false);
// Run tests with bearer access enabled as it introduces new functionality that needs to be tested.
#[cfg(test)]
pub static EXPERIMENTAL_BEARER_ACCESS: Lazy<bool> = Lazy::new(|| true);

/// Used to limit allocation for builtin functions
pub static FUNCTION_ALLOCATION_LIMIT: Lazy<usize> = once_cell::sync::Lazy::new(|| {
	let n = std::env::var("SURREAL_FUNCTION_ALLOCATION_LIMIT")
		.map(|s| s.parse::<u32>().unwrap_or(20))
		.unwrap_or(20);
	2usize.pow(n)
});
