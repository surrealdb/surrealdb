use once_cell::sync::Lazy;

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
/// Specifies how many concurrent jobs can be buffered in the worker channel.
pub const MAX_CONCURRENT_TASKS: usize = 64;

/// Specifies how deep various forms of computation will go before the query fails
/// with [`crate::error::Db::ComputationDepthExceeded`].
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
	let default = false;
	std::env::var("SURREAL_INSECURE_FORWARD_SCOPE_ERRORS")
		.map(|v| v.parse::<bool>().unwrap_or(default))
		.unwrap_or(default)
});
