/// Debug purposes only. It may be used in logs. Not for key handling in implementation code.
use crate::kvs::Key;

/// Helpers for debugging keys

/// sprint_key converts a key to an escaped string.
/// This is used for logging and debugging tests and should not be used in implementation code.
pub fn sprint_key(key: &Key) -> String {
	key.iter()
		.flat_map(|&byte| std::ascii::escape_default(byte))
		.map(|byte| byte as char)
		.collect::<String>()
}
