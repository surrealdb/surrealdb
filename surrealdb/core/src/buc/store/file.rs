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
use crate::err::Error;

/// Options for configuring the FileStore
#[derive(Clone, Debug)]
pub struct FileStoreOptions {
	root: ObjectKey,
	lowercase_paths: bool,
	bucket_folder_allowlist: Vec<PathBuf>,
}

/// A store implementation that uses the local filesystem
#[derive(Clone, Debug)]
pub struct FileStore {
	options: FileStoreOptions,
}

impl FileStore {
	/// Create a new FileStore with the given options
	pub fn new(options: FileStoreOptions) -> Self {
		FileStore {
			options,
		}
	}

	/// Parse a URL into FileStoreOption
	pub async fn parse_url(
		url_str: &str,
		bucket_folder_allowlist: Vec<PathBuf>,
	) -> Result<Option<FileStoreOptions>, Error> {
		let Ok(url) = Url::parse(url_str) else {
			return Ok(None);
		};

		if url.scheme() != "file" {
			return Ok(None);
		}

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
			.unwrap_or(true);

		// Get the path from the URL
		// The mutability is needed to remove the leading slash on Windows
		#[allow(unused_mut)]
		let mut path_from_url = if lowercase_paths {
			url.path().to_lowercase()
		} else {
			url.path().to_string()
		};

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
		if !is_path_allowed_with_list(&path_buf, lowercase_paths, &bucket_folder_allowlist) {
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
			bucket_folder_allowlist,
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

		// Verify the path is within the allowlist
		if !is_path_allowed_with_list(
			&full_path,
			self.options.lowercase_paths,
			&self.options.bucket_folder_allowlist,
		) {
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

fn is_path_allowed_with_list(
	path_to_check: &std::path::Path,
	lowercase_paths: bool,
	allowlist: &[PathBuf],
) -> bool {
	if allowlist.is_empty() {
		return false;
	}

	allowlist.iter().any(|allowed_path| {
		if lowercase_paths {
			// Windows canonical paths often have "\\?\" prefix that needs special handling
			// Convert to lowercase and normalize path separators for consistent comparison
			let mut path_str = path_to_check.to_string_lossy().to_lowercase().replace("\\", "/");

			// Strip Windows canonical path prefix if present (becomes "//?/" after normalization)
			const WINDOWS_CANONICAL_PATH_PREFIX: &str = "//?/";
			if path_str.starts_with(WINDOWS_CANONICAL_PATH_PREFIX) {
				path_str = path_str
					.strip_prefix(WINDOWS_CANONICAL_PATH_PREFIX)
					.unwrap_or(&path_str)
					.to_string();
			}

			// Normalize allowed path for comparison
			let allowed_str = allowed_path.to_string_lossy().to_lowercase().replace("\\", "/");

			path_str.starts_with(&allowed_str)
		} else {
			// Case-sensitive comparison (original behavior)
			path_to_check.starts_with(allowed_path)
		}
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
			all_entries.sort_by(|(key_a, _), (key_b, _)| key_a.to_string().cmp(&key_b.to_string()));

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
