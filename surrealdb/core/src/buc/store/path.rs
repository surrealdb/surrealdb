use core::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use crate::val::Value;

/// Path represents a normalized file path in the object store.
#[derive(Clone, Debug)]
pub struct ObjectKey(String);

impl Default for ObjectKey {
	fn default() -> Self {
		ObjectKey("/".to_owned())
	}
}

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
	pub fn strip_prefix(&self, prefix: &str) -> Option<Self> {
		let normalized_prefix = if prefix.starts_with('/') {
			prefix.to_string()
		} else {
			format!("/{}", prefix)
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

	/// Reject any key that contains a path-traversal segment. Without this
	/// check a key like `/../other.txt` (direct bucket) or
	/// `/../../../other_ns/other_db/other_bucket/secret` (global PrefixedStore
	/// bucket) would resolve outside the logical bucket prefix.
	pub fn check_no_traversal(&self) -> Result<(), String> {
		// Split on both separators: on Windows a backslash is a path separator,
		// so a key such as `..\other` is a traversal there, and the filesystem
		// backend's `path_clean` would collapse it. Rejecting the segment on
		// every platform keeps the check independent of where the key is later
		// resolved.
		for segment in self.0.split(['/', '\\']) {
			if segment == ".." || segment == "." {
				return Err(format!("object key '{}' contains a path-traversal segment", self.0));
			}
		}
		Ok(())
	}

	/// Reject a key that a filesystem backend would resolve to the same file
	/// as a differently spelled key. Path resolution drops empty segments and
	/// a trailing separator, so `//private/secret`, `/private//secret` and
	/// `/private/secret/` all name `/private/secret`; where
	/// `backslash_is_separator` holds (Windows), `/private\secret` does too.
	/// A bucket permission predicate judges the key as written, so an alias
	/// would reach a file the predicate denies under its canonical spelling.
	///
	/// The bucket root `/` is accepted. `allow_trailing_separator` admits a
	/// single trailing `/`, which a list prefix may carry to name a directory;
	/// an object key never may.
	pub fn check_canonical(
		&self,
		allow_trailing_separator: bool,
		backslash_is_separator: bool,
	) -> Result<(), String> {
		if backslash_is_separator && self.0.contains('\\') {
			return Err(format!("object key '{}' contains a backslash separator", self.0));
		}
		let rest = self.0.strip_prefix('/').unwrap_or(&self.0);
		if rest.is_empty() {
			return Ok(());
		}
		let rest = if allow_trailing_separator {
			rest.strip_suffix('/').unwrap_or(rest)
		} else {
			rest
		};
		if rest.split('/').any(str::is_empty) {
			return Err(format!(
				"object key '{}' contains an empty segment or a trailing separator",
				self.0
			));
		}
		Ok(())
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

#[cfg(test)]
mod tests {
	use super::ObjectKey;

	#[test]
	fn rejects_forward_slash_traversal() {
		assert!(ObjectKey::new("/a/../b").check_no_traversal().is_err());
		assert!(ObjectKey::new("/../secret").check_no_traversal().is_err());
		assert!(ObjectKey::new("/a/./b").check_no_traversal().is_err());
	}

	#[test]
	fn rejects_backslash_traversal() {
		// On Windows a backslash separates segments, so these are traversals
		// there; rejecting them on every platform keeps the key check
		// independent of where it is resolved (GHSA-6c37-x4jm-529x).
		assert!(ObjectKey::new("/a\\..\\b").check_no_traversal().is_err());
		assert!(ObjectKey::new("/..\\secret").check_no_traversal().is_err());
	}

	#[test]
	fn allows_ordinary_keys() {
		assert!(ObjectKey::new("/dir/file.txt").check_no_traversal().is_ok());
		assert!(ObjectKey::new("/a.b/c_d/e-f.txt").check_no_traversal().is_ok());
	}

	#[test]
	fn canonical_object_keys_reject_aliases() {
		for alias in ["//private/secret", "/private//secret", "/private/secret/", "//"] {
			assert!(
				ObjectKey::new(alias).check_canonical(false, false).is_err(),
				"{alias} must be rejected as an object key"
			);
		}
		assert!(ObjectKey::new("/").check_canonical(false, false).is_ok());
		assert!(ObjectKey::new("/private/secret").check_canonical(false, false).is_ok());
	}

	#[test]
	fn canonical_prefixes_allow_one_trailing_separator() {
		assert!(ObjectKey::new("/").check_canonical(true, false).is_ok());
		assert!(ObjectKey::new("/dir/").check_canonical(true, false).is_ok());
		assert!(ObjectKey::new("/dir").check_canonical(true, false).is_ok());
		assert!(ObjectKey::new("//dir/").check_canonical(true, false).is_err());
		assert!(ObjectKey::new("/dir//").check_canonical(true, false).is_err());
		assert!(ObjectKey::new("//").check_canonical(true, false).is_err());
	}

	#[test]
	fn canonical_keys_reject_backslash_only_where_it_separates() {
		let key = ObjectKey::new("/private\\secret");
		assert!(key.check_canonical(false, true).is_err());
		assert!(key.check_canonical(false, false).is_ok());
	}
}
