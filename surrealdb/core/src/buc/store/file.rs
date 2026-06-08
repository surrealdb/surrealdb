use std::future::Future;
use std::path::{Path as OsPath, PathBuf};
use std::pin::Pin;

use bytes::Bytes;
use path_clean::PathClean;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use url::Url;
use web_time::SystemTime;

use super::{ListOptions, ObjectKey, ObjectMeta, ObjectStore};
use crate::buc::Config;
use crate::err::Error;

/// Options for configuring the FileStore
#[derive(Clone, Debug)]
pub struct FileStoreOptions {
	root: ObjectKey,
	lowercase_paths: bool,
}

/// A store implementation that uses the local filesystem
#[derive(Clone, Debug)]
pub struct FileStore {
	options: FileStoreOptions,
	config: Config,
}

impl FileStore {
	/// Create a new FileStore with the given options
	pub fn new(options: FileStoreOptions, config: Config) -> Self {
		FileStore {
			options,
			config,
		}
	}

	/// Parse a URL into FileStoreOption
	pub async fn parse_url(
		url_str: &str,
		config: &Config,
	) -> Result<Option<FileStoreOptions>, Error> {
		let Ok(url) = Url::parse(url_str) else {
			return Ok(None);
		};

		if url.scheme() != "file" {
			return Ok(None);
		}

		// Whether to fold object keys to lowercase when mapping them onto the
		// host filesystem. Defaults to `false` so keys round-trip with their
		// original case: `file::put(type::file($b, "MixedCase.txt"), …)` lands
		// at `<root>/MixedCase.txt` on disk, matching the key the handle and
		// `file::list` / `file::get` report back. Opt in with
		// `?lowercase_paths=true` (or bare `?lowercase_paths`) when storing on
		// case-insensitive filesystems where folding before persistence avoids
		// collisions between keys that only differ by case.
		let lowercase_paths: bool = url
			.query_pairs()
			.find(|(key, _)| key == "lowercase_paths")
			.map(|(_, value)| {
				if value.is_empty() {
					Ok(true)
				} else {
					value.parse()
				}
			})
			.transpose()
			.map_err(|_| {
				Error::InvalidBucketUrl(
					"Expected to find a bool for query option `lowercase_paths`".to_string(),
				)
			})?
			.unwrap_or(false);

		// Get the path from the URL.
		// The root is a host filesystem path and must preserve its original case;
		// `lowercase_paths` only affects object keys (handled by `to_os_path`).
		// The mutability is needed to remove the leading slash on Windows.
		#[allow(unused_mut)]
		let mut path_from_url = url.path().to_string();

		// Handle Windows-specific path formatting
		#[cfg(windows)]
		{
			// Handle URL paths like "file:///C:/path" -> "/C:/path"
			if path_from_url.starts_with('/')
				&& path_from_url.len() > 2
				&& path_from_url.as_bytes()[1].is_ascii_alphabetic()
				&& path_from_url.as_bytes()[2] == b':'
			{
				path_from_url.remove(0); // Remove the leading slash
			}
		}

		// Create a PathBuf from the path, and clean it
		let path_buf = PathBuf::from(&path_from_url).clean();

		// File backends only support absolute paths as the base
		if !path_buf.is_absolute() {
			return Err(Error::InvalidBucketUrl(format!(
				"File path '{}' (derived from URL path '{}') is not absolute.",
				path_buf.display(),
				path_from_url
			)));
		}

		// Check if the path is allowed
		if !is_path_allowed(&path_buf, lowercase_paths, &config.bucket_list) {
			return Err(Error::FileAccessDenied(path_from_url.clone()));
		}

		// Check if the path exists
		let metadata = tokio::fs::metadata(&path_buf).await;

		if let Ok(metadata) = metadata {
			if !metadata.is_dir() {
				return Err(Error::InvalidBucketUrl(format!(
					"Path '{}' is not a directory.",
					path_buf.display()
				)));
			}
		} else {
			// Create directory and its parents if they don't exist
			tokio::fs::create_dir_all(&path_buf).await.map_err(|e| {
				Error::InvalidBucketUrl(format!(
					"Failed to create directory '{}': {}",
					path_buf.display(),
					e
				))
			})?;
		};

		Ok(Some(FileStoreOptions {
			root: ObjectKey::new(path_from_url),
			lowercase_paths,
		}))
	}

	/// Check if a path exists on disk
	async fn path_exists(path: &OsPath) -> Result<bool, String> {
		tokio::fs::try_exists(path)
			.await
			.map_err(|e| format!("Failed to check if path exists: {}", e))
	}

	/// Convert a Path to an OsPath, checking against the allowlist
	async fn to_os_path(&self, path: &ObjectKey) -> Result<PathBuf, String> {
		// The mutability is needed to remove the leading slash on Windows
		#[allow(unused_mut)]
		let mut root_str = self.options.root.as_str();

		// Handle Windows-specific path formatting
		#[cfg(windows)]
		{
			// Fix paths with leading slash before drive letter like "/C:/foo"
			if root_str.starts_with('/')
				&& root_str.len() > 2
				&& root_str.as_bytes()[1] != b'/' // Ensure it's not a UNC path
				&& root_str.as_bytes()[2] == b':'
			{
				root_str = &root_str[1..];
			}
		}

		let root_path = PathBuf::from(root_str).clean();

		// First canonicalize the root (which should exist)
		let canonical_root = tokio::fs::canonicalize(&root_path).await.map_err(|e| {
			format!("Failed to canonicalize root path '{}': {}", root_path.display(), e)
		})?;

		// Get the relative path components
		let relative_path_str = path.as_str().trim_start_matches('/');

		// Handle case sensitivity for the relative part
		let relative_path = if self.options.lowercase_paths {
			relative_path_str.to_lowercase()
		} else {
			relative_path_str.to_string()
		};

		// Combine the canonical root with the relative path
		let full_path = canonical_root.join(&relative_path).clean();

		// SECURITY: First ensure the resolved path stays inside *this* bucket's
		// canonical root. `path_clean::clean` collapses `..` segments, so a key
		// like `/../other.txt` (direct bucket) or `/../../../other_ns/...`
		// (global PrefixedStore bucket) would otherwise escape upward and only
		// the global allowlist check below would gate it — letting one bucket
		// read or write another bucket's files under the same allowlisted root.
		if !full_path.starts_with(&canonical_root) {
			return Err(format!("Path escapes the bucket root: {}", full_path.display()));
		}

		// Verify the path is within the allowlist
		if !is_path_allowed(&full_path, self.options.lowercase_paths, &self.config.bucket_list) {
			return Err(format!(
				"Path is not inside the allowed bucket directories: {}",
				full_path.display()
			));
		}

		Ok(full_path)
	}

	/// Create parent directories for a path if they don't exist
	async fn ensure_parent_dirs(path: &OsPath) -> Result<(), String> {
		if let Some(parent) = path.parent() {
			tokio::fs::create_dir_all(parent)
				.await
				.map_err(|e| format!("Failed to create directories: {}", e))?;
		}
		Ok(())
	}
}

/// Check if a path is allowed according to the allowlist
fn is_path_allowed(
	path_to_check: &std::path::Path,
	lowercase_paths: bool,
	allowed: &[PathBuf],
) -> bool {
	if !lowercase_paths {
		// Case-sensitive comparison goes component-wise, which already handles
		// non-UTF-8 bytes correctly (each component is compared as an `OsStr`).
		return allowed.iter().any(|allowed_path| path_to_check.starts_with(allowed_path));
	}

	// Reject paths that aren't valid UTF-8: `to_string_lossy` would collapse
	// every invalid byte onto the same `U+FFFD` replacement character, so two
	// distinct paths that differ only in their invalid bytes would compare
	// equal and slip past the prefix check.
	let Some(raw_path) = path_to_check.to_str() else {
		return false;
	};

	// Windows canonical paths often carry a `\\?\` prefix; after lowercasing
	// and normalising separators it becomes `//?/`. Strip it so callers can
	// pass either form interchangeably.
	const WINDOWS_CANONICAL_PATH_PREFIX: &str = "//?/";
	let normalized = raw_path.to_lowercase().replace('\\', "/");
	let path_str = normalized.strip_prefix(WINDOWS_CANONICAL_PATH_PREFIX).unwrap_or(&normalized);

	allowed.iter().any(|allowed_path| {
		let Some(raw_allowed) = allowed_path.to_str() else {
			return false;
		};
		let allowed_str = raw_allowed.to_lowercase().replace('\\', "/");
		// Strip a trailing separator so an entry like `/srv/data/` accepts
		// `/srv/data` itself, not just children.
		let allowed_str = allowed_str.trim_end_matches('/');
		// Require the prefix to land on a component boundary: bare
		// `str::starts_with` would let `/srv/data-evil` match `/srv/data`.
		let Some(rest) = path_str.strip_prefix(allowed_str) else {
			return false;
		};
		rest.is_empty() || rest.starts_with('/')
	})
}

impl ObjectStore for FileStore {
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let os_path = self.to_os_path(key).await?;
			Self::ensure_parent_dirs(&os_path).await?;

			let mut file = File::create(&os_path)
				.await
				.map_err(|e| format!("Failed to create file: {}", e))?;

			file.write_all(&data).await.map_err(|e| format!("Failed to write to file: {}", e))?;

			file.flush().await.map_err(|e| format!("Failed to flush file: {}", e))?;

			Ok(())
		})
	}

	fn put_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let os_path = self.to_os_path(key).await?;

			// Check if the file already exists
			if Self::path_exists(&os_path).await? {
				return Ok(());
			}

			Self::ensure_parent_dirs(&os_path).await?;

			let mut file = File::create(&os_path)
				.await
				.map_err(|e| format!("Failed to create file: {}", e))?;

			file.write_all(&data).await.map_err(|e| format!("Failed to write to file: {}", e))?;

			file.flush().await.map_err(|e| format!("Failed to flush file: {}", e))?;

			Ok(())
		})
	}

	fn get<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<Bytes>, String>> + Send + 'a>> {
		Box::pin(async move {
			let os_path = self.to_os_path(key).await?;

			// Check if the file exists
			if !Self::path_exists(&os_path).await? {
				return Ok(None);
			}

			let data = tokio::fs::read(&os_path)
				.await
				.map_err(|e| format!("Failed to read file: {}", e))?;

			Ok(Some(Bytes::from(data)))
		})
	}

	fn head<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			let os_path = self.to_os_path(key).await?;

			// Check if the file exists
			if !Self::path_exists(&os_path).await? {
				return Ok(None);
			}

			let metadata = tokio::fs::metadata(&os_path)
				.await
				.map_err(|e| format!("Failed to get metadata: {}", e))?;

			let size = metadata.len();

			// Get modified time if available
			let updated = metadata.modified().unwrap_or_else(|_| SystemTime::now()).into();

			Ok(Some(ObjectMeta {
				size,
				updated,
				key: key.to_owned(),
			}))
		})
	}

	fn delete<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let os_path = self.to_os_path(key).await?;

			// Check if the file exists
			if !Self::path_exists(&os_path).await? {
				return Ok(());
			}

			tokio::fs::remove_file(&os_path)
				.await
				.map_err(|e| format!("Failed to delete file: {}", e))?;

			Ok(())
		})
	}

	fn exists<'a>(
		&'a self,
		key: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<bool, String>> + Send + 'a>> {
		Box::pin(async move {
			let os_path = self.to_os_path(key).await?;
			Self::path_exists(&os_path).await
		})
	}

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let source_key = self.to_os_path(key).await?;
			let target_key = self.to_os_path(target).await?;

			// Check if the source file exists
			if !Self::path_exists(&source_key).await? {
				return Err(format!("Source key does not exist: {}", source_key.display()));
			}

			Self::ensure_parent_dirs(&target_key).await?;

			tokio::fs::copy(&source_key, &target_key)
				.await
				.map_err(|e| format!("Failed to copy file: {}", e))?;

			Ok(())
		})
	}

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let source_key = self.to_os_path(key).await?;
			let target_key = self.to_os_path(target).await?;

			// Check if target already exists
			if Self::path_exists(&target_key).await? {
				return Ok(());
			}

			// Check if the source file exists
			if !Self::path_exists(&source_key).await? {
				// Silently ignore operations on non-existent source files
				return Ok(());
			}

			Self::ensure_parent_dirs(&target_key).await?;

			tokio::fs::copy(&source_key, &target_key)
				.await
				.map_err(|e| format!("Failed to copy file: {}", e))?;

			Ok(())
		})
	}

	fn rename<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let source_key = self.to_os_path(key).await?;
			let target_key = self.to_os_path(target).await?;

			// Check if the source file exists
			if !Self::path_exists(&source_key).await? {
				return Err(format!("Source file does not exist: {}", source_key.display()));
			}

			Self::ensure_parent_dirs(&target_key).await?;

			tokio::fs::rename(&source_key, &target_key)
				.await
				.map_err(|e| format!("Failed to rename file: {}", e))?;

			Ok(())
		})
	}

	fn rename_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let source_key = self.to_os_path(key).await?;
			let target_key = self.to_os_path(target).await?;

			// Check if target already exists
			if Self::path_exists(&target_key).await? {
				return Ok(());
			}

			// Check if the source file exists
			if !Self::path_exists(&source_key).await? {
				return Err(format!("Source file does not exist: {}", source_key.display()));
			}

			Self::ensure_parent_dirs(&target_key).await?;

			tokio::fs::rename(&source_key, &target_key)
				.await
				.map_err(|e| format!("Failed to rename file: {}", e))?;

			Ok(())
		})
	}

	fn list<'a>(
		&'a self,
		opts: &'a ListOptions,
	) -> Pin<Box<dyn Future<Output = Result<Vec<ObjectMeta>, String>> + Send + 'a>> {
		Box::pin(async move {
			// If a prefix is provided, combine it with the store prefix
			// If not, just use the store's prefix
			let base_key = opts.prefix.clone().unwrap_or_default();
			let os_path = self.to_os_path(&base_key).await?;

			// Check if the directory exists
			if !Self::path_exists(&os_path).await? {
				return Ok(Vec::new());
			}

			// Check if it's a file or directory
			let metadata = tokio::fs::metadata(&os_path)
				.await
				.map_err(|e| format!("Failed to get metadata: {}", e))?;

			// If it's a file, return it as a single item
			if metadata.is_file() {
				// If a start key is provided and our base_key is less than it, return empty
				if let Some(ref start_key) = opts.start
					&& base_key.to_string() < start_key.to_string()
				{
					return Ok(Vec::new());
				}

				let size = metadata.len();
				let updated = metadata.modified().unwrap_or_else(|_| SystemTime::now()).into();
				return Ok(vec![ObjectMeta {
					key: base_key,
					size,
					updated,
				}]);
			}

			// If it's a directory, read its contents
			let mut read_dir = tokio::fs::read_dir(&os_path)
				.await
				.map_err(|e| format!("Failed to read directory: {}", e))?;

			// Collect all entries first so we can sort and paginate them
			let mut all_entries = Vec::new();

			// Process each entry in the directory
			while let Ok(Some(entry)) = read_dir.next_entry().await {
				let path = entry.path();
				let metadata = match tokio::fs::metadata(&path).await {
					Ok(md) => md,
					Err(e) => {
						// Skip entries we can't get metadata for
						error!("Failed to get metadata for {}: {}", path.display(), e);
						continue;
					}
				};

				// Skip directories if we're only listing files
				if metadata.is_dir() {
					continue;
				}

				// Convert the path to a relative Key
				let rel_path = path
					.strip_prefix(&os_path)
					.map_err(|e| format!("Failed to get relative path: {}", e))?;
				let rel_str = rel_path.to_string_lossy();
				let entry_key = base_key.join(&ObjectKey::new(rel_str.into_owned()));

				all_entries.push((entry_key, metadata));
			}

			// Sort entries by key to ensure consistent ordering
			all_entries.sort_by_key(|(key, _)| key.to_string());

			// Filter by start key if provided
			let filtered_entries = if let Some(ref start_key) = opts.start {
				all_entries
					.into_iter()
					.filter(|(key, _)| key.to_string() > start_key.to_string())
					.collect()
			} else {
				all_entries
			};

			// Apply limit if specified
			let limited_entries = if let Some(limit_val) = opts.limit {
				filtered_entries.into_iter().take(limit_val).collect::<Vec<_>>()
			} else {
				filtered_entries
			};

			// Convert to ObjectMeta
			let objects = limited_entries
				.into_iter()
				.map(|(entry_key, metadata)| {
					let size = metadata.len();
					let updated = metadata.modified().unwrap_or_else(|_| SystemTime::now()).into();
					ObjectMeta {
						key: entry_key,
						size,
						updated,
					}
				})
				.collect();

			Ok(objects)
		})
	}
}

#[cfg(test)]
mod tests {
	use temp_dir::TempDir;

	use super::*;
	use crate::buc::Config;

	/// macOS canonicalises `/var/folders/...` to `/private/var/folders/...`,
	/// so `to_os_path` and the allowlist disagree about the bucket root when
	/// the URL path is the non-canonical form. Push the canonical path through
	/// the URL and the allowlist so both checks see the same string.
	async fn canonical(dir: &OsPath) -> PathBuf {
		tokio::fs::canonicalize(dir).await.unwrap()
	}

	fn build_url(dir: &OsPath, query: &str) -> String {
		let path = dir.to_string_lossy();
		if query.is_empty() {
			format!("file://{path}")
		} else {
			format!("file://{path}?{query}")
		}
	}

	async fn open_store(dir: &OsPath, query: &str) -> (FileStore, PathBuf) {
		let root = canonical(dir).await;
		let cfg = Config::for_test(vec![root.clone()]);
		let opts = FileStore::parse_url(&build_url(&root, query), &cfg)
			.await
			.expect("parse_url should succeed for an allowlisted path")
			.expect("file:// URL should resolve to a FileStore");
		(FileStore::new(opts, cfg), root)
	}

	/// Read the raw filenames the filesystem reports inside `dir`. Going through
	/// `read_dir` rather than `Path::exists` is what lets these tests assert on
	/// the *actual case-preserved name on disk* even on case-insensitive
	/// filesystems like macOS APFS, where `MixedCase.txt` and `mixedcase.txt`
	/// would otherwise both report as existing.
	fn on_disk_names(dir: &OsPath) -> Vec<String> {
		let mut names: Vec<String> = std::fs::read_dir(dir)
			.unwrap()
			.filter_map(|entry| entry.ok())
			.filter_map(|entry| entry.file_name().into_string().ok())
			.collect();
		names.sort();
		names
	}

	/// Regression for surrealdb/surrealdb#7309: by default the filesystem
	/// bucket backend must persist objects under the key the caller supplied
	/// — without silently lowercasing — so writes round-trip through `list`,
	/// `get`, `exists`, and `delete`.
	#[tokio::test]
	async fn default_preserves_case_end_to_end() {
		let dir = TempDir::new().unwrap();
		let (store, root) = open_store(dir.path(), "").await;

		let key = ObjectKey::new("/CaseProbe_XYZ.tmp".to_string());
		store.put(&key, Bytes::from_static(b"hello")).await.unwrap();

		// The directory entry on disk preserves the original case, so direct
		// disk tooling (cp/rsync/ls) and SurrealQL agree on the filename. On
		// case-insensitive filesystems `Path::exists` would happily report
		// both casings as present, so we go through `read_dir` to assert on
		// the actual stored name.
		assert_eq!(on_disk_names(&root), vec!["CaseProbe_XYZ.tmp"]);

		// Round-trip via the public ObjectStore API using the same casing.
		assert!(store.exists(&key).await.unwrap());
		assert_eq!(store.get(&key).await.unwrap().as_deref(), Some(&b"hello"[..]));

		// `list` reports the case that was written, not a folded variant.
		let listed = store.list(&ListOptions::default()).await.unwrap();
		let keys: Vec<_> = listed.into_iter().map(|m| m.key.to_string()).collect();
		assert_eq!(keys, vec!["/CaseProbe_XYZ.tmp"]);

		// And the same casing successfully removes the file.
		store.delete(&key).await.unwrap();
		assert!(!store.exists(&key).await.unwrap());
		assert!(on_disk_names(&root).is_empty());
	}

	/// `?lowercase_paths=true` remains available for callers that need the
	/// old folding behaviour (e.g. case-insensitive filesystems where two
	/// keys differing only by case should converge on one object).
	#[tokio::test]
	async fn opt_in_lowercase_paths_still_folds() {
		let dir = TempDir::new().unwrap();
		let (store, root) = open_store(dir.path(), "lowercase_paths=true").await;

		let key = ObjectKey::new("/MixedCase.txt".to_string());
		store.put(&key, Bytes::from_static(b"hello")).await.unwrap();

		// On disk the file is folded to lowercase, regardless of how the key
		// was casing-supplied.
		assert_eq!(on_disk_names(&root), vec!["mixedcase.txt"]);

		// Both casings resolve to the same folded path under opt-in.
		let lower = ObjectKey::new("/mixedcase.txt".to_string());
		assert!(store.exists(&key).await.unwrap());
		assert!(store.exists(&lower).await.unwrap());
	}

	/// Bare `?lowercase_paths` (no value) keeps the documented shorthand for
	/// "enable folding" so the option stays usable for callers who relied on
	/// the old default.
	#[tokio::test]
	async fn bare_lowercase_paths_query_enables_folding() {
		let dir = TempDir::new().unwrap();
		let (store, root) = open_store(dir.path(), "lowercase_paths").await;

		let key = ObjectKey::new("/MixedCase.txt".to_string());
		store.put(&key, Bytes::from_static(b"hello")).await.unwrap();

		assert_eq!(on_disk_names(&root), vec!["mixedcase.txt"]);
	}

	#[test]
	fn case_sensitive_allowlist_matches_prefix_components() {
		let allowed = vec![PathBuf::from("/srv/data")];
		assert!(is_path_allowed(OsPath::new("/srv/data/file.txt"), false, &allowed));
		assert!(!is_path_allowed(OsPath::new("/srv/other/file.txt"), false, &allowed));
	}

	#[test]
	fn lowercase_allowlist_matches_case_insensitively() {
		let allowed = vec![PathBuf::from("/srv/Data")];
		assert!(is_path_allowed(OsPath::new("/SRV/data/file.txt"), true, &allowed));
	}

	/// Regression: bare `str::starts_with` would let a sibling directory
	/// whose name extends the allowlisted prefix (`/srv/data-evil`) match
	/// `/srv/data`. The component-boundary check rejects that case while
	/// still accepting the exact entry and any path nested below it.
	#[test]
	fn lowercase_allowlist_requires_component_boundary() {
		let allowed = vec![PathBuf::from("/srv/data")];
		assert!(!is_path_allowed(OsPath::new("/srv/data-evil/secret"), true, &allowed));
		assert!(is_path_allowed(OsPath::new("/srv/data"), true, &allowed));
		assert!(is_path_allowed(OsPath::new("/srv/data/file.txt"), true, &allowed));
	}

	/// A trailing separator on the allowlist entry should still accept the
	/// entry itself, not just children, so `/srv/data/` is equivalent to
	/// `/srv/data`.
	#[test]
	fn lowercase_allowlist_accepts_trailing_separator() {
		let allowed = vec![PathBuf::from("/srv/data/")];
		assert!(is_path_allowed(OsPath::new("/srv/data"), true, &allowed));
		assert!(is_path_allowed(OsPath::new("/srv/data/file.txt"), true, &allowed));
		assert!(!is_path_allowed(OsPath::new("/srv/data-evil"), true, &allowed));
	}

	/// Regression: `to_string_lossy` previously collapsed every invalid UTF-8
	/// byte onto `U+FFFD`, so two distinct non-UTF-8 paths could compare equal
	/// and bypass the allowlist. With the lossless `Path::to_str` check, any
	/// non-UTF-8 candidate is rejected outright when `lowercase_paths` is on.
	#[cfg(unix)]
	#[test]
	fn lowercase_mode_rejects_non_utf8_paths() {
		use std::ffi::OsStr;
		use std::os::unix::ffi::OsStrExt;

		let allowed = vec![PathBuf::from("/srv/data")];
		let bad_path = OsPath::new(OsStr::from_bytes(b"/srv/data/\xFFsecret"));
		assert!(!is_path_allowed(bad_path, true, &allowed));
	}

	#[cfg(unix)]
	#[test]
	fn lowercase_mode_rejects_non_utf8_allowed_entry() {
		use std::ffi::OsStr;
		use std::os::unix::ffi::OsStrExt;

		let allowed = vec![PathBuf::from(OsStr::from_bytes(b"/srv/data/\xFF"))];
		assert!(!is_path_allowed(OsPath::new("/srv/data/file.txt"), true, &allowed));
	}
}
