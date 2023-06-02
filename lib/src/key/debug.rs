use crate::kvs::Key;

/// Helpers for debugging keys

pub fn sprint_key(key: &Key) -> String {
	key.clone()
		.iter()
		.flat_map(|&byte| std::ascii::escape_default(byte))
		.map(|byte| byte as char)
		.collect::<String>()
}
