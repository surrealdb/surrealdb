//! Debugging utilities for the `key` module.

use std::ops::Range;

/// A trait for types that can be converted to a string representation for
/// readability.
pub trait Sprintable {
	/// Converts the implementing type to a string representation.
	fn sprint(&self) -> String;
}

impl Sprintable for &str {
	fn sprint(&self) -> String {
		(*self).to_string()
	}
}

impl Sprintable for String {
	fn sprint(&self) -> String {
		self.to_string()
	}
}

impl Sprintable for Vec<u8> {
	fn sprint(&self) -> String {
		self.iter()
			.flat_map(|&byte| std::ascii::escape_default(byte))
			.map(|byte| byte as char)
			.collect::<String>()
	}
}

impl Sprintable for &[u8] {
	fn sprint(&self) -> String {
		self.iter()
			.flat_map(|&byte| std::ascii::escape_default(byte))
			.map(|byte| byte as char)
			.collect::<String>()
	}
}

impl<T> Sprintable for Vec<T>
where
	T: Sprintable,
{
	fn sprint(&self) -> String {
		self.iter().map(Sprintable::sprint).collect::<Vec<_>>().join(" + ")
	}
}

impl<T> Sprintable for Range<T>
where
	T: Sprintable,
{
	fn sprint(&self) -> String {
		format!("{}..{}", self.start.sprint(), self.end.sprint())
	}
}
