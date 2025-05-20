use core::fmt;
use std::{
	borrow::Borrow,
	hash::{Hash, Hasher},
	ops::Deref,
};

use crate::expr::Value;

/// Path represents a normalized file path in the object store.
#[derive(Clone, Debug)]
pub struct ObjectKey(String);

impl ObjectKey {
	/// Create a new path, ensuring it starts with "/"
	pub fn new(path: impl Into<String>) -> Self {
		let path_str = path.into();
		let normalized = if path_str.starts_with('/') {
			path_str
		} else {
			format!("/{}", path_str)
		};
		Self(normalized)
	}

	/// Join this path with another, returning a new Path
	pub fn join(&self, right: &ObjectKey) -> Self {
		let mut path = self.0.clone();
		if path.ends_with('/') {
			path.pop();
		}

		path.push_str(&right.0);
		Self(path)
	}

	/// Remove a prefix from this path, returning a new Path.
	/// If this path doesn't start with the given prefix, returns None.
	pub fn strip_prefix(&self, prefix: impl AsRef<str>) -> Option<Self> {
		let prefix_str = prefix.as_ref();
		let normalized_prefix = if prefix_str.starts_with('/') {
			prefix_str.to_string()
		} else {
			format!("/{}", prefix_str)
		};

		// Ensure the prefix ends without a trailing slash for comparison
		let normalized_prefix = normalized_prefix.trim_end_matches('/');

		if self.0.starts_with(normalized_prefix) {
			// Get the substring after the prefix, ensuring we keep the leading slash
			let remaining = &self.0[normalized_prefix.len()..];
			let result = if remaining.is_empty() || remaining.starts_with('/') {
				remaining.to_string()
			} else {
				format!("/{}", remaining)
			};

			Some(Self(result))
		} else {
			None
		}
	}

	/// Get the path as a string slice
	pub fn as_str(&self) -> &str {
		&self.0
	}
}

impl From<String> for ObjectKey {
	fn from(value: String) -> Self {
		ObjectKey::new(value)
	}
}

impl From<&str> for ObjectKey {
	fn from(value: &str) -> Self {
		ObjectKey::new(value)
	}
}

impl From<ObjectKey> for Value {
	fn from(val: ObjectKey) -> Self {
		Value::from(val.0)
	}
}

impl Deref for ObjectKey {
	type Target = str;

	fn deref(&self) -> &Self::Target {
		self.as_str()
	}
}

impl fmt::Display for ObjectKey {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl AsRef<str> for ObjectKey {
	fn as_ref(&self) -> &str {
		self.as_str()
	}
}

impl Borrow<str> for ObjectKey {
	fn borrow(&self) -> &str {
		self.as_str()
	}
}

impl PartialEq for ObjectKey {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}

impl Eq for ObjectKey {}

impl Hash for ObjectKey {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.hash(state)
	}
}

impl PartialOrd for ObjectKey {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for ObjectKey {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.0.cmp(&other.0)
	}
}
