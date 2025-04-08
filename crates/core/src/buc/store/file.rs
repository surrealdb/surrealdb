use std::{
	future::Future,
	path::{Path as OsPath, PathBuf},
	pin::Pin,
};

use bytes::Bytes;
use tokio::{fs::File, io::AsyncWriteExt};
use url::Url;

use crate::{cnf::FILE_ALLOWLIST, err::Error, sql::Datetime};

use super::{ListOptions, ObjectKey, ObjectMeta, ObjectStore};

/// Options for configuring the FileStore
#[derive(Clone, Debug)]
pub struct FileStoreOptions {
	root: ObjectKey,
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

	/// Parse a URL into FileStoreOptions
	pub fn parse_url(url: &str) -> Result<Option<FileStoreOptions>, Error> {
		let Ok(url) = Url::parse(url) else {
			return Ok(None);
		};

		if url.scheme() != "file" {
			return Ok(None);
		}

		// Get the path from the URL
		let path = url.path();

		// Create a PathBuf from the path
		let path_buf = PathBuf::from(path);

		// Check if the path is allowed
		if !is_path_allowed(&path_buf) {
			return Err(Error::FileAccessDenied(path.to_string()));
		}

		// Check if the path exists
		if !path_buf.exists() {
			// Create directory and its parents if they don't exist
			std::fs::create_dir_all(&path_buf).map_err(|e| {
				Error::InvalidBucketUrl(format!("Failed to create directory {}: {}", path, e))
			})?;
		} else if !path_buf.is_dir() {
			// If path exists but is not a directory, return an error
			return Err(Error::InvalidBucketUrl(format!("Path is not a directory: {}", path)));
		}

		Ok(Some(FileStoreOptions {
			root: ObjectKey::from(path.to_string()),
		}))
	}

	/// Check if a path exists on disk
	async fn path_exists(path: &OsPath) -> Result<bool, String> {
		tokio::fs::try_exists(path)
			.await
			.map_err(|e| format!("Failed to check if path exists: {}", e))
	}

	/// Convert a Path to an OsPath, checking against the allowlist
	fn to_os_path(&self, path: &ObjectKey) -> Result<PathBuf, String> {
		let root = PathBuf::from(self.options.root.as_str());

		// First canonicalize the root (which should exist)
		let canonical_root = std::fs::canonicalize(&root)
			.map_err(|e| format!("Failed to canonicalize root path: {}", e))?;

		// Get the relative path components
		let relative_path = path.as_str().trim_start_matches('/');

		// Combine the canonical root with the relative path
		let full_path = canonical_root.join(relative_path);

		// Verify the path is within the allowlist without canonicalizing
		// Since we're starting from a canonicalized root, the path should be valid
		if !is_path_allowed(&full_path) {
			return Err(format!("Path is not in the allowlist: {}", full_path.display()));
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
fn is_path_allowed(path: &std::path::Path) -> bool {
	// If the allowlist is empty, nothing is allowed
	if FILE_ALLOWLIST.is_empty() {
		return false;
	}

	// Check if the path is within any of the allowed paths
	FILE_ALLOWLIST.iter().any(|allowed| {
		if path.starts_with(allowed) {
			return true;
		}

		// Handle case sensitivity on Windows
		#[cfg(windows)]
		if path
			.to_string_lossy()
			.to_lowercase()
			.starts_with(&allowed.to_string_lossy().to_lowercase())
		{
			return true;
		}

		false
	})
}

impl ObjectStore for FileStore {
	fn put<'a>(
		&'a self,
		key: &'a ObjectKey,
		data: Bytes,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let os_path = self.to_os_path(key)?;
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
			let os_path = self.to_os_path(key)?;

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
			let os_path = self.to_os_path(key)?;

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
			let os_path = self.to_os_path(key)?;

			// Check if the file exists
			if !Self::path_exists(&os_path).await? {
				return Ok(None);
			}

			let metadata = tokio::fs::metadata(&os_path)
				.await
				.map_err(|e| format!("Failed to get metadata: {}", e))?;

			let size = metadata.len();

			// Get modified time if available
			let updated = metadata.modified().map(|time| Datetime(time.into())).unwrap_or_default();

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
			let os_path = self.to_os_path(key)?;

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
			let os_path = self.to_os_path(key)?;
			Self::path_exists(&os_path).await
		})
	}

	fn copy<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let source_key = self.to_os_path(key)?;
			let target_key = self.to_os_path(target)?;

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

	fn copy_if_not_exists<'a>(
		&'a self,
		key: &'a ObjectKey,
		target: &'a ObjectKey,
	) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
		Box::pin(async move {
			let source_key = self.to_os_path(key)?;
			let target_key = self.to_os_path(target)?;

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
			let source_key = self.to_os_path(key)?;
			let target_key = self.to_os_path(target)?;

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
			let source_key = self.to_os_path(key)?;
			let target_key = self.to_os_path(target)?;

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
			let base_key = opts.prefix.as_ref().cloned().unwrap_or_else(|| ObjectKey::from(""));
			let os_path = self.to_os_path(&base_key)?;

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
				if let Some(ref start_key) = opts.start {
					if base_key.to_string() < start_key.to_string() {
						return Ok(Vec::new());
					}
				}

				let size = metadata.len();
				let updated =
					metadata.modified().map(|time| Datetime(time.into())).unwrap_or_default();
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
				let entry_key = base_key.join(&ObjectKey::from(rel_str.to_string()));

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
					let updated =
						metadata.modified().map(|time| Datetime(time.into())).unwrap_or_default();
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
