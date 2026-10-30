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
	/// Create a key in its one canonical spelling.
	///
	/// Leading `/` added, runs of `/` collapsed, and a trailing `/` removed.
	/// The root is `/`.
	///
	/// One spelling per object is load-bearing, not tidiness. The key reaches
	/// three consumers that must agree on it: the `PERMISSIONS` clause, which
	/// sees it as `$file`; the presigned URL's signature, which covers a path
	/// re-encoded from it; and the store, which resolves it to an object. If
	/// `/a//b` and `/a/b` were distinct keys here while the signed path spelled
	/// both the same way, a URL authorised for one would redeem against the
	/// other.
	pub fn new(path: impl Into<String>) -> Self {
		let path_str = path.into();

		// The common key is already canonical, so recognise it and keep the
		// allocation the caller already made.
		let canonical = path_str.starts_with('/')
			&& !path_str.contains("//")
			&& (path_str.len() == 1 || !path_str.ends_with('/'));
		if canonical {
			return Self(path_str);
		}

		let mut out = String::with_capacity(path_str.len() + 1);
		for segment in path_str.split('/').filter(|s| !s.is_empty()) {
			out.push('/');
			out.push_str(segment);
		}
		if out.is_empty() {
			out.push('/');
		}
		Self(out)
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

		// The prefix must end on a segment boundary. `starts_with` alone would
		// make `/1/1/logs` a prefix of `/1/1/logs-archive`, so one bucket's
		// keys would strip into another's and be reported as its own.
		let boundary = self.0.len() == normalized_prefix.len()
			|| self.0[normalized_prefix.len()..].starts_with('/');

		if self.0.starts_with(normalized_prefix) && boundary {
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
	fn a_key_has_one_spelling() {
		// The property the presign scheme rests on: every spelling of an object
		// collapses to the string that gets signed, permission-checked and
		// stored.
		for (input, want) in [
			("/a/b", "/a/b"),
			("a/b", "/a/b"),
			("/a//b", "/a/b"),
			("//a///b//", "/a/b"),
			("/a/b/", "/a/b"),
		] {
			assert_eq!(ObjectKey::new(input).as_str(), want, "{input}");
		}
	}

	#[test]
	fn an_empty_key_is_the_root() {
		for input in ["", "/", "//", "///"] {
			assert_eq!(ObjectKey::new(input).as_str(), "/", "{input:?}");
		}
		assert_eq!(ObjectKey::default().as_str(), "/");
	}

	#[test]
	fn a_prefix_must_end_on_a_separator() {
		let key = ObjectKey::new("/1/1/logs-archive/payroll.csv");
		// A sibling bucket whose name extends this one must not match.
		assert!(key.strip_prefix("/1/1/logs").is_none());
		assert_eq!(
			key.strip_prefix("/1/1/logs-archive").map(|k| k.as_str().to_owned()),
			Some("/payroll.csv".to_owned())
		);
	}

	#[test]
	fn stripping_the_whole_key_leaves_the_root() {
		let key = ObjectKey::new("/1/1/logs");
		assert_eq!(
			key.strip_prefix("/1/1/logs").map(|k| k.as_str().to_owned()),
			Some(String::new())
		);
	}

	#[test]
	fn traversal_is_rejected_after_normalisation() {
		assert!(ObjectKey::new("/a/../b").check_no_traversal().is_err());
		assert!(ObjectKey::new("//..//b").check_no_traversal().is_err());
		assert!(ObjectKey::new("/a/b").check_no_traversal().is_ok());
	}

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
				ObjectKey(alias.to_owned()).check_canonical(false, false).is_err(),
				"{alias} must be rejected as an object key"
			);
		}
		assert!(ObjectKey::new("/").check_canonical(false, false).is_ok());
		assert!(ObjectKey::new("/private/secret").check_canonical(false, false).is_ok());
	}

	#[test]
	fn canonical_prefixes_allow_one_trailing_separator() {
		assert!(ObjectKey("/".to_owned()).check_canonical(true, false).is_ok());
		assert!(ObjectKey("/dir/".to_owned()).check_canonical(true, false).is_ok());
		assert!(ObjectKey("/dir".to_owned()).check_canonical(true, false).is_ok());
		assert!(ObjectKey("//dir/".to_owned()).check_canonical(true, false).is_err());
		assert!(ObjectKey("/dir//".to_owned()).check_canonical(true, false).is_err());
		assert!(ObjectKey("//".to_owned()).check_canonical(true, false).is_err());
	}

	#[test]
	fn canonical_keys_reject_backslash_only_where_it_separates() {
		let key = ObjectKey::new("/private\\secret");
		assert!(key.check_canonical(false, true).is_err());
		assert!(key.check_canonical(false, false).is_ok());
	}
}
