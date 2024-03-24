use once_cell::sync::Lazy;
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use std::path::PathBuf;

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
/// Specifies how many concurrent jobs can be buffered in the worker channel.
pub const MAX_CONCURRENT_TASKS: usize = 64;

/// Specifies how deep various forms of computation will go before the query fails
/// with [`crate::err::Error::ComputationDepthExceeded`].
///
/// For reference, use ~15 per MiB of stack in release mode.
///
/// During query parsing, the total depth of calls to parse values (including arrays, expressions,
/// functions, objects, sub-queries), Javascript values, and geometry collections count against
/// this limit.
///
/// During query execution, all potentially-recursive code paths count against this limit. Whereas
/// parsing assigns equal weight to each recursion, certain expensive code paths are allowed to
/// count for more than one unit of depth during execution.
pub static MAX_COMPUTATION_DEPTH: Lazy<u8> = Lazy::new(|| {
	option_env!("SURREAL_MAX_COMPUTATION_DEPTH").and_then(|s| s.parse::<u8>().ok()).unwrap_or(120)
});

/// Specifies the names of parameters which can not be specified in a query.
pub const PROTECTED_PARAM_NAMES: &[&str] = &["auth", "scope", "token", "session"];

/// The characters which are supported in server record IDs.
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// The publicly visible name of the server
pub const SERVER_NAME: &str = "SurrealDB";

/// Datastore processor batch size for scan operations
pub const PROCESSOR_BATCH_SIZE: u32 = 50;

/// Forward all signup/signin query errors to a client trying authenticate to a scope. Do not use in production.
pub static INSECURE_FORWARD_SCOPE_ERRORS: Lazy<bool> = Lazy::new(|| {
	option_env!("SURREAL_INSECURE_FORWARD_SCOPE_ERRORS")
		.and_then(|s| s.parse::<bool>().ok())
		.unwrap_or(false)
});

/// Specifies the path of the temporary directory used by SurrealDB.
/// If not specified, SurrealDB attempts to make a temporary directory inside `env::temp_dir()`.
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
pub(crate) static TEMPORARY_DIRECTORY: Lazy<Option<PathBuf>> =
	Lazy::new(|| option_env!("SURREAL_TEMPORARY_DIRECTORY").map(PathBuf::from));

/// Specifies the buffer limit for external sorting.
/// If the environment variable is not present or cannot be parsed, a default value of 50,000 is used.
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
pub static EXTERNAL_SORTING_BUFFER_LIMIT: Lazy<usize> = Lazy::new(|| {
	option_env!("SURREAL_EXTERNAL_SORTING_BUFFER_LIMIT")
		.and_then(|s| s.parse::<usize>().ok())
		.unwrap_or(50_000)
});
