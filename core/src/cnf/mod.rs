use once_cell::sync::Lazy;

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
pub static MAX_COMPUTATION_DEPTH: Lazy<u32> =
	lazy_env_parse!("SURREAL_MAX_COMPUTATION_DEPTH", u32, 120);

/// Specifies the names of parameters which can not be specified in a query.
pub const PROTECTED_PARAM_NAMES: &[&str] = &["access", "auth", "token", "session"];

/// The characters which are supported in server record IDs.
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];

/// The publicly visible name of the server
pub const SERVER_NAME: &str = "SurrealDB";

/// Datastore processor batch size for scan operations
pub const PROCESSOR_BATCH_SIZE: u32 = 50;

/// Forward all signup/signin query errors to a client performing record access. Do not use in production.
pub static INSECURE_FORWARD_RECORD_ACCESS_ERRORS: Lazy<bool> =
	lazy_env_parse!("SURREAL_INSECURE_FORWARD_RECORD_ACCESS_ERRORS", bool, false);

#[cfg(not(target_arch = "wasm32"))]
/// Specifies the buffer limit for external sorting.
/// If the environment variable is not present or cannot be parsed, a default value of 50,000 is used.
pub static EXTERNAL_SORTING_BUFFER_LIMIT: Lazy<usize> =
	lazy_env_parse!("SURREAL_EXTERNAL_SORTING_BUFFER_LIMIT", usize, 50_000);
