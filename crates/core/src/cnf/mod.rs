use std::sync::LazyLock;

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
#[cfg(not(target_family = "wasm"))]
pub static MAX_CONCURRENT_TASKS: LazyLock<usize> =
	lazy_env_parse!("SURREAL_MAX_CONCURRENT_TASKS", usize, 64);

/// Specifies how deep computation recursive call will go before en error is returned.
pub static MAX_COMPUTATION_DEPTH: LazyLock<u32> =
	lazy_env_parse!("SURREAL_MAX_COMPUTATION_DEPTH", u32, 120);

/// Specifies how deep the parser will parse nested objects and arrays in a query.
pub static MAX_OBJECT_PARSING_DEPTH: LazyLock<u32> =
	lazy_env_parse!("SURREAL_MAX_OBJECT_PARSING_DEPTH", u32, 100);

/// Specifies how deep the parser will parse recursive queries (queries within queries).
pub static MAX_QUERY_PARSING_DEPTH: LazyLock<u32> =
	lazy_env_parse!("SURREAL_MAX_QUERY_PARSING_DEPTH", u32, 20);

/// Specifies the number of computed regexes which can be cached in the engine.
pub static REGEX_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_REGEX_CACHE_SIZE", usize, 1_000);

/// Specifies the number of items which can be cached within a single transaction.
pub static TRANSACTION_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_TRANSACTION_CACHE_SIZE", usize, 10_000);

/// Specifies the number of definitions which can be cached across transactions.
pub static DATASTORE_CACHE_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_DATASTORE_CACHE_SIZE", usize, 1_000);

/// The maximum number of keys that should be scanned at once in general queries.
pub static NORMAL_FETCH_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_NORMAL_FETCH_SIZE", u32, 500);

/// The maximum number of keys that should be scanned at once for export queries.
pub static EXPORT_BATCH_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_EXPORT_BATCH_SIZE", u32, 1000);

/// The maximum number of keys that should be scanned at once for count queries.
pub static COUNT_BATCH_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_COUNT_BATCH_SIZE", u32, 10_000);

/// The maximum size of the priority queue triggering usage of the priority queue for the result collector.
pub static MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_MAX_ORDER_LIMIT_PRIORITY_QUEUE_SIZE", u32, 1000);

/// The maximum number of keys that should be scanned at once per concurrent indexing batch.
pub static INDEXING_BATCH_SIZE: LazyLock<u32> =
	lazy_env_parse!("SURREAL_INDEXING_BATCH_SIZE", u32, 250);

/// The maximum stack size of the JavaScript function runtime (defaults to 256 KiB)
pub static SCRIPTING_MAX_STACK_SIZE: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SCRIPTING_MAX_STACK_SIZE", usize, 256 * 1024);

/// The maximum memory limit of the JavaScript function runtime (defaults to 2 MiB).
pub static SCRIPTING_MAX_MEMORY_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_SCRIPTING_MAX_MEMORY_LIMIT", usize, 2 << 20);

/// Forward all signup/signin/authenticate query errors to a client performing authentication. Do not use in production.
pub static INSECURE_FORWARD_ACCESS_ERRORS: LazyLock<bool> =
	lazy_env_parse!("SURREAL_INSECURE_FORWARD_ACCESS_ERRORS", bool, false);

#[cfg(storage)]
/// Specifies the buffer limit for external sorting.
pub static EXTERNAL_SORTING_BUFFER_LIMIT: LazyLock<usize> =
	lazy_env_parse!("SURREAL_EXTERNAL_SORTING_BUFFER_LIMIT", usize, 50_000);

/// Used to limit allocation for builtin functions
pub static GENERATION_ALLOCATION_LIMIT: LazyLock<usize> = LazyLock::new(|| {
	let n = std::env::var("SURREAL_GENERATION_ALLOCATION_LIMIT")
		.map(|s| s.parse::<u32>().unwrap_or(20))
		.unwrap_or(20);
	2usize.pow(n)
});

/// Used to limit allocation for builtin functions
pub static IDIOM_RECURSION_LIMIT: LazyLock<usize> = LazyLock::new(|| {
	std::env::var("SURREAL_IDIOM_RECURSION_LIMIT")
		.map(|s| s.parse::<usize>().unwrap_or(256))
		.unwrap_or(256)
});

pub static MEMORY_THRESHOLD: LazyLock<usize> = std::sync::LazyLock::new(|| {
	std::env::var("SURREAL_MEMORY_THRESHOLD")
		.map(|input| {
			// Trim the input of any spaces
			let input = input.trim();
			// Check if this contains a suffix
			let split = input.find(|c: char| !c.is_ascii_digit());
			// Split the value into number and suffix
			let parts = match split {
				Some(index) => input.split_at(index),
				None => (input, ""),
			};
			// Parse the number as a positive number
			let number = parts.0.parse::<usize>().unwrap_or_default();
			// Parse the supplied suffix as a multiplier
			let suffix = match parts.1.trim().to_lowercase().as_str() {
				"" | "b" => 1,
				"k" | "kb" | "kib" => 1024,
				"m" | "mb" | "mib" => 1024 * 1024,
				"g" | "gb" | "gib" => 1024 * 1024 * 1024,
				_ => 1,
			};
			// Multiply the input by the suffix
			let bytes = number.checked_mul(suffix).unwrap_or_default();
			// Log the parsed memory threshold
			debug!("Memory threshold guide: {input} ({bytes} bytes)");
			// Return the total byte threshold
			bytes
		})
		.unwrap_or(0)
});
