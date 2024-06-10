/// Debug purposes only. It may be used in logs. Not for key handling in implementation code.

/// Helpers for debugging keys

/// sprint_key converts a key to an escaped string.
/// This is used for logging and debugging tests and should not be used in implementation code.
pub fn sprint<T>(key: &T) -> String
where
	T: AsRef<[u8]>,
{
	key.as_ref()
		.iter()
		.flat_map(|&byte| std::ascii::escape_default(byte))
		.map(|byte| byte as char)
		.collect::<String>()
}
