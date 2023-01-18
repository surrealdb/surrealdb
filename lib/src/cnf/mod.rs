#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
/// Specifies how many concurrent jobs can be buffered in the worker channel.
pub const MAX_CONCURRENT_TASKS: usize = 64;

/// Specifies how deep various forms of computation will go before the query fails
/// with [`Error::ComputationDepthExceeded`].
///
/// During query parsing, the total depth of calls to parse values (including arrays, expressions,
/// functions, objects, sub-queries), Javascript values, and geometry collections count against
/// this limit.
///
/// During query execution, all potentially-recursive code paths count against this limit. Whereas
/// parsing assigns equal weight to each recursion, certain expensive code paths are allowed to
/// count for more than one unit of depth during execution.
pub const MAX_COMPUTATION_DEPTH: u8 = 30;

/// Specifies the names of parameters which can not be specified in a query.
pub const PROTECTED_PARAM_NAMES: &[&str] = &["auth", "scope", "token", "session"];

/// The characters which are supported in server record IDs.
pub const ID_CHARS: [char; 36] = [
	'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i',
	'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
];
