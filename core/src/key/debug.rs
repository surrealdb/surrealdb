/// Displays a key in a human-readable format.
#[cfg(debug_assertions)]
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
