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
use crate::buc::{Config, Error};

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

	/// Resolve an object key to the host path an operation will use; see
	/// [`Self::resolve_key`].
	async fn to_os_path(&self, path: &ObjectKey) -> Result<PathBuf, String> {
		self.resolve_key(path, false).await
	}

	/// Resolve a list prefix to the host directory or file it names; see
	/// [`Self::resolve_key`]. Unlike an object key, a prefix may end in `/`.
	async fn to_os_prefix_path(&self, prefix: &ObjectKey) -> Result<PathBuf, String> {
		self.resolve_key(prefix, true).await
	}

	/// Resolve a key to its host path, confined to this bucket's canonical
	/// root. The containment holds for the filesystem state at call time only;
	/// see the residual TOCTOU note at the symlink check below for the
	/// concurrency limitation the returned path still carries.
	async fn resolve_key(&self, path: &ObjectKey, is_prefix: bool) -> Result<PathBuf, String> {
		// A bucket permission predicate judges the key as written, so the key
		// must name exactly one file: no traversal segment, and no spelling
		// that path resolution would collapse onto another key. A global
		// bucket's `PrefixedStore` rejects traversal segments before it
		// prepends the bucket prefix, but a direct `file://` bucket reaches
		// this with the raw key, so both rules are enforced here. Without them
		// `/public/../private/secret` or `//private/secret` resolves to
		// `<root>/private/secret` while the predicate saw a key outside
		// `/private/`.
		path.check_no_traversal()?;
		path.check_canonical(is_prefix, cfg!(windows))?;

		let canonical_root = self.canonical_root().await?;

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

		// SECURITY: `path_clean` only collapses `..` lexically; it does not
		// follow symlinks. A symlink planted inside the bucket (e.g.
		// `<root>/link -> /etc`) would let the key `/link/secret` pass the
		// lexical check above and then escape the root when the real filesystem
		// operation follows the link. Resolve the deepest existing ancestor of
		// the target (a write target itself does not exist yet) and confirm the
		// resolved path is still inside the canonical root.
		let resolved = canonicalize_deepest_existing(&full_path).await?;
		if !resolved.starts_with(&canonical_root) {
			return Err(format!(
				"Path escapes the bucket root through a symlink: {}",
				full_path.display()
			));
		}

		// SECURITY (residual TOCTOU): the containment checked above - the
		// lexical `..` collapse, the `starts_with` root check, and the symlink
		// resolution of the deepest existing ancestor - describes the filesystem
		// only as it stands at this instant. `full_path` is returned unresolved
		// and the caller's filesystem operation (`File::create`, `tokio::fs::read`,
		// `copy`, `rename`, ...) re-resolves it by name a moment later. A process
		// that can write inside the bucket tree can, in that gap, replace an
		// in-root ancestor component with an out-of-root symlink, so the
		// operation follows the link and escapes the root after the check
		// passed. This time-of-check/time-of-use window is not closeable with a
		// path-based check: closing it needs the operation itself to run against
		// a pinned directory handle with beneath or no-follow semantics
		// (`openat2(RESOLVE_BENEATH)`, per-component `O_NOFOLLOW`, or a
		// `cap-std`/`openat`-style opened-handle API), an fd-relative filesystem
		// primitive this crate does not currently depend on. Until then the
		// static containment above is the enforced boundary, and callers must
		// treat a bucket root that another process can write to as outside the
		// trust boundary.

		// Verify the path is within the allowlist
		if !is_path_allowed(&full_path, self.options.lowercase_paths, &self.config.bucket_list) {
			return Err(format!(
				"Path is not inside the allowed bucket directories: {}",
				full_path.display()
			));
		}

		Ok(full_path)
	}

	/// The bucket root as the host resolves it, with every symlink followed.
	/// Every containment check compares against this path.
	async fn canonical_root(&self) -> Result<PathBuf, String> {
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

		// The root should exist
		tokio::fs::canonicalize(&root_path).await.map_err(|e| {
			format!("Failed to canonicalize root path '{}': {}", root_path.display(), e)
		})
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

/// Windows spells the same location `C:\dir` and `\\?\C:\dir`, and
/// canonicalisation always yields the second. Returns `path` in the first
/// spelling, with `\\?\UNC\server\share` as `\\server\share`; every other
/// path is returned as is.
///
/// A path on any other platform never begins `\\?\` while absolute, so it is
/// never altered there.
fn without_verbatim_prefix(path: &std::path::Path) -> std::borrow::Cow<'_, std::path::Path> {
	const VERBATIM: &str = r"\\?\";
	const VERBATIM_UNC: &str = r"\\?\UNC\";
	let Some(raw) = path.to_str() else {
		return std::borrow::Cow::Borrowed(path);
	};
	if let Some(rest) = raw.strip_prefix(VERBATIM_UNC) {
		std::borrow::Cow::Owned(PathBuf::from(format!(r"\\{rest}")))
	} else if let Some(rest) = raw.strip_prefix(VERBATIM) {
		std::borrow::Cow::Owned(PathBuf::from(rest))
	} else {
		std::borrow::Cow::Borrowed(path)
	}
}

/// Whether `allowed` is a bare root (`/`), which names every path on the
/// platform. On Windows such an entry carries no drive, so it must cover every
/// drive rather than none.
fn is_bare_root(allowed: &std::path::Path) -> bool {
	allowed.has_root() && allowed.components().count() == 1
}

/// Canonicalize the deepest ancestor of `path` that exists, resolving every
/// symlink along the way. A write target does not exist yet, so the whole path
/// cannot always be canonicalized; its deepest existing ancestor can, and that
/// is where any symlink redirection would be, since the remaining components
/// are about to be created under it.
///
/// Fails closed on a dangling symlink. A symlink whose target does not exist is
/// not canonicalizable, so the deepest-existing walk would otherwise pop past
/// it and resolve a safe ancestor - yet the real filesystem operation follows
/// the link and lands wherever it points, which may be outside the root. Each
/// component that `canonicalize` cannot resolve is stat'd without following it
/// (`symlink_metadata`): a component that is itself a symlink is refused,
/// because its destination cannot be proven in-root; a component that simply
/// does not exist (the write target, or a directory about to be created) is
/// expected and the walk continues.
async fn canonicalize_deepest_existing(path: &OsPath) -> Result<PathBuf, String> {
	let mut current = PathBuf::from(path);
	loop {
		if let Ok(resolved) = tokio::fs::canonicalize(&current).await {
			return Ok(resolved);
		}
		if let Ok(meta) = tokio::fs::symlink_metadata(&current).await
			&& meta.file_type().is_symlink()
		{
			return Err(format!(
				"Path escapes the bucket root through an unresolvable symlink: {}",
				current.display()
			));
		}
		if !current.pop() {
			return Err(format!("Failed to resolve any ancestor of path: {}", path.display()));
		}
	}
}

/// Whether `path_to_check` falls within one of the allowlisted bucket
/// directories, comparing component-wise so a prefix only matches on a
/// directory boundary.
fn is_path_allowed(
	path_to_check: &std::path::Path,
	lowercase_paths: bool,
	allowed: &[PathBuf],
) -> bool {
	if !lowercase_paths {
		// Case-sensitive comparison goes component-wise, which already handles
		// non-UTF-8 bytes correctly (each component is compared as an `OsStr`).
		let path_to_check = without_verbatim_prefix(path_to_check);
		for allowed_path in allowed {
			let allowed = without_verbatim_prefix(allowed_path);
			// `Path::starts_with("")` holds for every path, so an empty entry
			// (or bare `\\?\`) must never be allowed to match.
			if allowed.as_os_str().is_empty() {
				continue;
			}
			if is_bare_root(allowed_path) || path_to_check.starts_with(&allowed) {
				return true;
			}
		}
		return false;
	}

	// Reject paths that aren't valid UTF-8: `to_string_lossy` would collapse
	// every invalid byte onto the same `U+FFFD` replacement character, so two
	// distinct paths that differ only in their invalid bytes would compare
	// equal and slip past the prefix check.
	let path_to_check = without_verbatim_prefix(path_to_check);
	let Some(raw_path) = path_to_check.to_str() else {
		return false;
	};

	let normalize = |raw: &str| raw.to_lowercase().replace('\\', "/");
	let path_str = normalize(raw_path);

	for allowed_path in allowed {
		let allowed = without_verbatim_prefix(allowed_path);
		let Some(raw_allowed) = allowed.to_str() else {
			continue;
		};
		let allowed_str = normalize(raw_allowed);
		if allowed_str.is_empty() {
			continue;
		}
		// Strip a trailing separator so an entry like `/srv/data/` accepts
		// `/srv/data` itself, not just children.
		let allowed_str = allowed_str.trim_end_matches('/');
		// Only a bare root normalises to nothing and names every path.
		if allowed_str.is_empty() {
			if is_bare_root(allowed_path) {
				return true;
			}
			continue;
		}
		// Require the prefix to land on a component boundary: bare
		// `str::starts_with` would let `/srv/data-evil` match `/srv/data`.
		let Some(rest) = path_str.strip_prefix(allowed_str) else {
			continue;
		};
		if rest.is_empty() || rest.starts_with('/') {
			return true;
		}
	}
	false
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

			// Copying a file onto itself must be a no-op; `tokio::fs::copy` opens
			// the destination with `O_TRUNC`, which would zero out the source file.
			if source_key == target_key {
				return Ok(());
			}
			if Self::path_exists(&target_key).await?
				&& let (Ok(src_canon), Ok(dst_canon)) = (
					tokio::fs::canonicalize(&source_key).await,
					tokio::fs::canonicalize(&target_key).await,
				) && src_canon == dst_canon
			{
				return Ok(());
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
			let os_path = self.to_os_prefix_path(&base_key).await?;

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

			// An entry that is a symlink is listed only if it resolves inside the
			// root, so the listing reports no object that `get` or `head` refuses.
			let canonical_root = self.canonical_root().await?;

			// Collect all entries first so we can sort and paginate them
			let mut all_entries = Vec::new();

			// Process each entry in the directory
			while let Ok(Some(entry)) = read_dir.next_entry().await {
				let path = entry.path();
				let Ok(file_type) = entry.file_type().await else {
					continue;
				};
				if file_type.is_symlink() {
					match tokio::fs::canonicalize(&path).await {
						Ok(target) if target.starts_with(&canonical_root) => {}
						_ => continue,
					}
				}
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
	/// A canonical Windows path (`\\?\C:\...`) matches an allowlist entry
	/// spelled either way, and a bare `/` covers every drive.
	#[test]
	fn lowercase_allowlist_matches_windows_spellings() {
		let canonical = OsPath::new(r"\\?\C:\Users\me\bucket\Cargo.toml");
		for entry in [r"C:\Users\me", "C:/Users/me/", r"\\?\C:\Users\me", "C:/", "/"] {
			let allowed = vec![PathBuf::from(entry)];
			assert!(is_path_allowed(canonical, true, &allowed), "entry {entry}");
		}
		let allowed = vec![PathBuf::from("C:/Users/other")];
		assert!(!is_path_allowed(canonical, true, &allowed));
		assert!(is_path_allowed(OsPath::new("C:/Users/me/bucket"), true, &[PathBuf::from("/")]));

		let unc = OsPath::new(r"\\?\UNC\srv\share\bucket\Cargo.toml");
		for entry in [r"\\srv\share\bucket", r"\\?\UNC\srv\share\bucket", "//srv/share/bucket", "/"]
		{
			let allowed = vec![PathBuf::from(entry)];
			assert!(is_path_allowed(unc, true, &allowed), "UNC entry {entry}");
		}
		assert!(is_path_allowed(
			OsPath::new(r"\\srv\share\bucket\Cargo.toml"),
			true,
			&[PathBuf::from(r"\\?\UNC\srv\share\bucket")],
		));
		assert!(!is_path_allowed(unc, true, &[PathBuf::from(r"\\srv\share\other")]));
	}

	/// An empty entry or a bare verbatim prefix names no directory, so it
	/// allows nothing.
	#[test]
	fn an_empty_allowlist_entry_matches_nothing() {
		for lowercase in [false, true] {
			let allowed = vec![PathBuf::new(), PathBuf::from(r"\\?\")];
			assert!(!is_path_allowed(OsPath::new("/srv/data"), lowercase, &allowed));
			assert!(!is_path_allowed(OsPath::new(r"\\?\C:\srv\data"), lowercase, &allowed));
		}
	}

	#[test]
	fn verbatim_prefix_is_removed_from_drive_and_unc_paths() {
		assert_eq!(
			without_verbatim_prefix(OsPath::new(r"\\?\C:\a\b")).as_ref(),
			OsPath::new(r"C:\a\b")
		);
		assert_eq!(
			without_verbatim_prefix(OsPath::new(r"\\?\UNC\srv\share\a")).as_ref(),
			OsPath::new(r"\\srv\share\a")
		);
		assert_eq!(
			without_verbatim_prefix(OsPath::new("/srv/data")).as_ref(),
			OsPath::new("/srv/data")
		);
		assert!(is_path_allowed(
			OsPath::new(r"\\?\UNC\srv\share\bucket"),
			false,
			&[PathBuf::from(r"\\srv\share\bucket")],
		));
		assert!(is_path_allowed(
			OsPath::new(r"\\srv\share\bucket"),
			false,
			&[PathBuf::from(r"\\?\UNC\srv\share\bucket")],
		));
	}

	/// A case-sensitive allowlist treats the canonical and plain spellings of a
	/// Windows path alike, and a bare root covers every drive.
	#[cfg(windows)]
	#[test]
	fn case_sensitive_allowlist_matches_windows_spellings() {
		let canonical = OsPath::new(r"\\?\C:\Users\me\bucket\Cargo.toml");
		for entry in [r"C:\Users\me", "C:/Users/me/", r"\\?\C:\Users\me", "C:/", "/"] {
			let allowed = vec![PathBuf::from(entry)];
			assert!(is_path_allowed(canonical, false, &allowed), "entry {entry}");
		}
		let allowed = vec![PathBuf::from("C:/Users/other")];
		assert!(!is_path_allowed(canonical, false, &allowed));

		let unc = OsPath::new(r"\\?\UNC\srv\share\bucket\Cargo.toml");
		for entry in [r"\\srv\share\bucket", r"\\?\UNC\srv\share\bucket", "/"] {
			let allowed = vec![PathBuf::from(entry)];
			assert!(is_path_allowed(unc, false, &allowed), "UNC entry {entry}");
		}
		assert!(!is_path_allowed(unc, false, &[PathBuf::from(r"\\srv\share\other")]));
	}

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

	/// A direct `file://` bucket reaches the backend with the raw key, so a
	/// key crossing a logical prefix with `..` must be refused before it is
	/// resolved; otherwise it crosses a key-based ACL boundary the permission
	/// predicate drew on the un-normalised key (GHSA-cph5-pm57-9w9c).
	#[tokio::test]
	async fn direct_bucket_rejects_a_traversal_key() {
		let dir = TempDir::new().unwrap();
		let (store, _root) = open_store(dir.path(), "").await;

		let key = ObjectKey::new("/public/../secret.txt".to_string());
		assert!(store.put(&key, Bytes::from_static(b"x")).await.is_err());
		assert!(store.get(&key).await.is_err());
		assert!(store.exists(&key).await.is_err());
	}

	/// A symlink planted inside the bucket root must not let an operation reach
	/// a file outside the root: `path_clean` collapses `..` but does not follow
	/// symlinks, so the resolved target has to be re-checked against the root
	/// (GHSA-7pqw-x84x-frxc).
	#[cfg(unix)]
	#[tokio::test]
	async fn an_in_bucket_symlink_cannot_escape_the_root() {
		let root_dir = TempDir::new().unwrap();
		let outside_dir = TempDir::new().unwrap();
		std::fs::write(outside_dir.path().join("secret.txt"), b"top secret").unwrap();
		std::os::unix::fs::symlink(outside_dir.path(), root_dir.path().join("link")).unwrap();

		let (store, _root) = open_store(root_dir.path(), "").await;

		// Reading a file outside the root through the symlink is refused.
		let read_key = ObjectKey::new("/link/secret.txt".to_string());
		assert!(
			store.get(&read_key).await.is_err(),
			"reading a file through an in-bucket symlink must be refused"
		);

		// Writing through the symlink is refused, and nothing lands outside.
		let write_key = ObjectKey::new("/link/evil.txt".to_string());
		assert!(
			store.put(&write_key, Bytes::from_static(b"x")).await.is_err(),
			"writing through an in-bucket symlink must be refused"
		);
		assert!(
			!outside_dir.path().join("evil.txt").exists(),
			"the refused write must not have landed outside the root"
		);
	}

	/// A dangling symlink inside the root - one whose target does not yet exist
	/// outside it - must not let a write escape. `canonicalize` cannot resolve
	/// the missing target, so the deepest-existing walk must notice the link
	/// itself rather than popping to a safe ancestor and allowing the write,
	/// which would follow the link and create the target outside the root
	/// (GHSA-7pqw-x84x-frxc).
	#[cfg(unix)]
	#[tokio::test]
	async fn a_dangling_in_bucket_symlink_cannot_escape_the_root() {
		let root_dir = TempDir::new().unwrap();
		let outside_dir = TempDir::new().unwrap();
		// The target does not exist yet, so this is a dangling symlink.
		let outside_target = outside_dir.path().join("planted.txt");
		std::os::unix::fs::symlink(&outside_target, root_dir.path().join("link")).unwrap();

		let (store, _root) = open_store(root_dir.path(), "").await;

		// Writing to the dangling symlink is refused, and nothing is created at
		// its out-of-root target.
		let write_key = ObjectKey::new("/link".to_string());
		assert!(
			store.put(&write_key, Bytes::from_static(b"x")).await.is_err(),
			"writing to a dangling in-bucket symlink must be refused"
		);
		assert!(
			!outside_target.exists(),
			"the refused write must not have created the symlink's out-of-root target"
		);
	}

	/// Keys that path resolution collapses onto another key are refused, so a
	/// permission predicate that denies `/private/` cannot be sidestepped by
	/// spelling the same file `//private/secret` or `/private/secret/`. A list
	/// prefix may still end in `/`.
	#[tokio::test]
	async fn direct_bucket_rejects_an_aliased_key() {
		let dir = TempDir::new().unwrap();
		let (store, _root) = open_store(dir.path(), "").await;
		let canonical = ObjectKey::new("/private/secret".to_string());
		store.put(&canonical, Bytes::from_static(b"top secret")).await.unwrap();

		for alias in ["//private/secret", "/private//secret", "/private/secret/"] {
			let key = ObjectKey::new(alias.to_string());
			assert!(store.get(&key).await.is_err(), "get {alias} must be refused");
			assert!(store.head(&key).await.is_err(), "head {alias} must be refused");
			assert!(
				store.put(&key, Bytes::from_static(b"x")).await.is_err(),
				"put {alias} must be refused"
			);
			assert!(store.copy(&canonical, &key).await.is_err(), "copy to {alias} must be refused");
		}
		assert_eq!(
			store.get(&canonical).await.unwrap().unwrap(),
			Bytes::from_static(b"top secret")
		);

		let opts = ListOptions {
			prefix: Some(ObjectKey::new("/private/".to_string())),
			..Default::default()
		};
		let listed = store.list(&opts).await.unwrap();
		assert_eq!(listed.len(), 1);
	}

	/// On Windows a backslash separates path segments, so `/private\secret`
	/// would name `/private/secret` and is refused.
	#[cfg(windows)]
	#[tokio::test]
	async fn direct_bucket_rejects_a_backslash_separated_key() {
		let dir = TempDir::new().unwrap();
		let (store, _root) = open_store(dir.path(), "").await;
		let key = ObjectKey::new("/private\\secret".to_string());
		assert!(store.put(&key, Bytes::from_static(b"x")).await.is_err());
		assert!(store.get(&key).await.is_err());
	}

	/// Listing must not report a symlink that resolves outside the root: `head`
	/// and `get` refuse it, so its size and modification time are not the
	/// bucket's to disclose. A symlink that stays inside the root is listed.
	#[cfg(unix)]
	#[tokio::test]
	async fn list_omits_a_symlink_that_escapes_the_root() {
		let root_dir = TempDir::new().unwrap();
		let outside_dir = TempDir::new().unwrap();
		std::fs::write(outside_dir.path().join("secret.txt"), b"top secret").unwrap();
		std::fs::write(root_dir.path().join("real.txt"), b"real").unwrap();
		std::os::unix::fs::symlink(
			outside_dir.path().join("secret.txt"),
			root_dir.path().join("escape.txt"),
		)
		.unwrap();
		std::os::unix::fs::symlink(
			root_dir.path().join("real.txt"),
			root_dir.path().join("inner.txt"),
		)
		.unwrap();

		let (store, _root) = open_store(root_dir.path(), "").await;

		let listed = store.list(&ListOptions::default()).await.unwrap();
		let mut keys: Vec<String> = listed.iter().map(|m| m.key.to_string()).collect();
		keys.sort();
		assert_eq!(keys, vec!["/inner.txt".to_string(), "/real.txt".to_string()]);
	}

	#[tokio::test]
	async fn copy_onto_same_key_preserves_contents() {
		let dir = TempDir::new().unwrap();
		let (store, _root) = open_store(dir.path(), "").await;
		let key = ObjectKey::new("/doc.txt".to_string());
		let payload = Bytes::from_static(b"important payload");
		store.put(&key, payload.clone()).await.unwrap();

		store.copy(&key, &key).await.unwrap();
		assert_eq!(store.get(&key).await.unwrap(), Some(payload));
	}
}
