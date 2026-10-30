use std::future::Future;
use std::io::SeekFrom;
use std::path::{Path as OsPath, PathBuf};
use std::pin::Pin;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use path_clean::PathClean;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter};
use tokio_util::io::ReaderStream;
use url::Url;
use uuid::Uuid;
use web_time::SystemTime;

use super::{
	ByteRange, ListEntry, ListOptions, Listing, ObjectKey, ObjectMeta, ObjectStore, ObjectStream,
	PutCondition, PutError, PutOptions, PutResult,
};
use crate::buc::{Config, Error};

/// Options for configuring the FileStore
#[derive(Clone, Debug)]
pub struct FileStoreOptions {
	root: String,
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
			root: path_from_url,
			lowercase_paths,
		}))
	}

	/// Check if a path exists on disk
	/// The path, name and metadata of a directory entry a listing may report
	/// or descend into, or `None` for one it must not.
	///
	/// Refused: a staged write in progress, which is not an object (it lives
	/// beside its target for the duration of an upload, and a crash mid-upload
	/// leaves one behind); a symlink resolving outside the root, since `get`
	/// and `head` refuse it; a symlink to a directory, which could form a
	/// cycle; and a name that is not valid UTF-8, which no key can spell.
	async fn listable(
		entry: &tokio::fs::DirEntry,
		canonical_root: &OsPath,
	) -> Option<(PathBuf, String, std::fs::Metadata)> {
		let path = entry.path();
		if StagedWrite::is_staged_name(&path) {
			return None;
		}
		let name = entry.file_name().into_string().ok()?;
		let file_type = entry.file_type().await.ok()?;
		if file_type.is_symlink() {
			match tokio::fs::canonicalize(&path).await {
				Ok(target) if target.starts_with(canonical_root) => {}
				_ => return None,
			}
		}
		let metadata = match tokio::fs::metadata(&path).await {
			Ok(md) => md,
			Err(e) => {
				error!("Failed to get metadata for {}: {}", path.display(), e);
				return None;
			}
		};
		if file_type.is_symlink() && metadata.is_dir() {
			return None;
		}
		Some((path, name, metadata))
	}

	/// Whether a directory holds at least one object a listing would report,
	/// at any depth. Stops at the first one.
	async fn holds_object(dir: PathBuf, canonical_root: &OsPath) -> bool {
		let mut pending = vec![dir];
		while let Some(dir) = pending.pop() {
			let Ok(mut read_dir) = tokio::fs::read_dir(&dir).await else {
				continue;
			};
			while let Ok(Some(entry)) = read_dir.next_entry().await {
				if let Some((path, _, metadata)) = Self::listable(&entry, canonical_root).await {
					if !metadata.is_dir() {
						return true;
					}
					pending.push(path);
				}
			}
		}
		false
	}

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
	/// [`Self::resolve_key`]. Unlike an object key, a prefix may end in `/` or
	/// resolve to the bucket root.
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

		// Refuse an object key that resolves to the bucket root, because the
		// root names a directory rather than an object. Without that,
		// `StagedWrite` would take the root's own *parent* as the directory to
		// stage in and write caller bytes outside the bucket root and outside
		// the allowlist. Only `list` (`is_prefix = true`) may resolve the root.
		if !is_prefix && full_path == canonical_root {
			return Err(format!("object key '{path}' does not name an object"));
		}

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

	/// Open a staged write for `os_path`, creating its parent directories.
	async fn stage(os_path: &OsPath) -> Result<StagedWrite, String> {
		Self::ensure_parent_dirs(os_path).await?;
		StagedWrite::open(os_path).await
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

/// A weak entity tag for a local file, from metadata `head` already reads.
///
/// **Weak, and it has to be.** The mtime granularity of a filesystem varies, so
/// two writes within one tick that produce the same size collide. A `file://`
/// bucket also exists so an operator can place files in the directory out of
/// band, where a restore can preserve both size and mtime while changing the
/// content.
///
/// A weak validator is correct for `If-None-Match`, which is what caching
/// needs. It must not back `If-Match` or `If-Range`, so a conditional write
/// keeps the existence check in `put_if_not_exists` instead.
///
/// Reading the bytes to hash them would make it strong, and would turn every
/// `head` into a full read of the object. That is the cost this trades away.
fn weak_etag(metadata: &std::fs::Metadata) -> Option<String> {
	let mtime = metadata.modified().ok()?.duration_since(std::time::UNIX_EPOCH).ok()?;
	Some(format!("W/\"{:x}-{:x}\"", metadata.len(), mtime.as_nanos()))
}

/// Bytes read from a local object per syscall.
///
/// `ReaderStream` defaults to 4 KiB, and `tokio::fs` turns each read into a
/// blocking-pool dispatch, so the default costs a round trip and an allocation
/// per 4 KiB of a served object.
const READ_CHUNK_SIZE: usize = 256 * 1024;

/// Bytes buffered before a staged write reaches the filesystem.
///
/// An inbound HTTP chunk is 8-64 KiB and each `write_all` is its own
/// blocking-pool dispatch and syscall, serialised behind the previous one.
const WRITE_BUFFER_SIZE: usize = 1024 * 1024;

/// A file being written to a temporary path in its target's own directory, to
/// be renamed into place once it is complete.
///
/// The rename makes the write atomic *with respect to a concurrent reader*: a
/// reader sees either the previous object or the whole new one, never a
/// half-written file at a live key. It is not a durability guarantee — nothing
/// is fsynced — and it does not need to be, since the property being protected
/// is what `head` and `get` report, not what survives a power cut.
///
/// A streamed upload can be abandoned at any point: the client disconnects, or
/// the signed size limit trips. Writing straight to the target would leave a
/// truncated file that reads back as a complete object.
struct StagedWrite {
	file: BufWriter<File>,
	temp: PathBuf,
	target: PathBuf,
	committed: bool,
}

impl StagedWrite {
	async fn open(target: &OsPath) -> Result<Self, String> {
		// A sibling of the target, so the rename stays within one filesystem and
		// is therefore atomic. The leading dot and the uuid keep it out of the
		// way of concurrent writers to the same key, and
		// [`Self::is_staged_name`] recognises the shape so `list` never reports
		// an upload in progress as an object.
		let name = target.file_name().unwrap_or_default().to_string_lossy().into_owned();
		let temp = match target.parent() {
			Some(parent) => parent.join(format!(".{name}.{}.tmp", Uuid::new_v4())),
			None => return Err(format!("Path has no parent directory: {}", target.display())),
		};

		let file =
			File::create(&temp).await.map_err(|e| format!("Failed to create file: {}", e))?;

		Ok(Self {
			file: BufWriter::with_capacity(WRITE_BUFFER_SIZE, file),
			temp,
			target: target.to_path_buf(),
			committed: false,
		})
	}

	/// Whether a directory entry is a staged write rather than an object.
	///
	/// Matches the shape [`Self::open`] creates. Kept beside it so the two
	/// cannot drift: a rename here without a rename there would publish
	/// half-written uploads into `list`.
	fn is_staged_name(path: &std::path::Path) -> bool {
		path.file_name()
			.and_then(|n| n.to_str())
			.is_some_and(|n| n.starts_with('.') && n.ends_with(".tmp"))
	}

	async fn write_all(&mut self, data: &[u8]) -> Result<(), String> {
		self.file.write_all(data).await.map_err(|e| format!("Failed to write to file: {}", e))
	}

	/// Flush the staged file and move it onto the target key.
	///
	/// The flush is what makes the buffering safe: every byte written has to
	/// reach the file before the rename publishes it.
	async fn commit(mut self) -> Result<(), String> {
		self.file.flush().await.map_err(|e| format!("Failed to flush file: {}", e))?;
		tokio::fs::rename(&self.temp, &self.target)
			.await
			.map_err(|e| format!("Failed to write to file: {}", e))?;
		self.committed = true;
		Ok(())
	}

	/// Flush and move onto the target only if the target is free.
	///
	/// The reservation is what makes this decidable: `create_new` fails if the
	/// path exists, and it is one syscall, so two uploads racing for a free key
	/// cannot both win it. The staged file is then renamed over the placeholder
	/// this call just made, which is why the rename that follows is allowed to
	/// clobber.
	///
	/// A plain `exists`-then-`rename` cannot uphold this: both writers would
	/// observe the key free and the second would silently replace the first.
	async fn commit_if_vacant(mut self) -> Result<(), PutError> {
		match tokio::fs::OpenOptions::new().write(true).create_new(true).open(&self.target).await {
			Ok(_) => {}
			Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
				return Err(PutError::AlreadyExists);
			}
			Err(e) => return Err(PutError::Failed(format!("Failed to write to file: {}", e))),
		}

		self.file
			.flush()
			.await
			.map_err(|e| PutError::Failed(format!("Failed to flush file: {}", e)))?;
		tokio::fs::rename(&self.temp, &self.target)
			.await
			.map_err(|e| PutError::Failed(format!("Failed to write to file: {}", e)))?;
		self.committed = true;
		Ok(())
	}
}

impl Drop for StagedWrite {
	fn drop(&mut self) {
		if self.committed {
			return;
		}
		// Blocking, deliberately. The common abandonment is a cancelled request
		// future, and a cancelled future's `Drop` cannot await. One `unlink` is
		// cheap enough to run inline, and spawning a task to do it would leak
		// the file whenever the runtime is already shutting down.
		let _ = std::fs::remove_file(&self.temp);
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

			let mut staged = Self::stage(&os_path).await?;
			staged.write_all(&data).await?;
			staged.commit().await
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

			let mut staged = Self::stage(&os_path).await?;
			staged.write_all(&data).await?;
			staged.commit().await
		})
	}

	fn supports_content_type(&self) -> bool {
		false
	}

	fn put_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		mut data: BoxStream<'static, Result<Bytes, String>>,
		opts: PutOptions,
	) -> Pin<Box<dyn Future<Output = Result<PutResult, PutError>> + Send + 'a>> {
		Box::pin(async move {
			// A filesystem has nowhere portable to keep a media type, and none
			// is inferred from the key, so a write that names one is refused
			// before anything is staged.
			if opts.content_type.is_some() {
				return Err(PutError::Failed(
					"the file backend cannot store a content type".to_owned(),
				));
			}
			let os_path = self.to_os_path(key).await?;
			let mut staged = Self::stage(&os_path).await?;

			let mut size = 0u64;
			while let Some(chunk) = data.next().await {
				// An error aborts the write: `staged` is dropped without
				// committing, so its temporary file is removed and the object
				// already at `key`, if any, is untouched.
				let chunk = chunk?;
				size += chunk.len() as u64;
				staged.write_all(&chunk).await?;
			}

			match opts.condition {
				PutCondition::Overwrite => staged.commit().await?,
				PutCondition::IfNotExists => staged.commit_if_vacant().await?,
			}

			// Stat after the rename, so the tag matches the one a later `head`
			// derives from the same file rather than from the staged copy.
			let etag = tokio::fs::metadata(&os_path).await.ok().as_ref().and_then(weak_etag);

			Ok(PutResult {
				size,
				etag,
			})
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

	fn get_streamed<'a>(
		&'a self,
		key: &'a ObjectKey,
		range: Option<ByteRange>,
	) -> Pin<Box<dyn Future<Output = Result<Option<ObjectStream>, String>> + Send + 'a>> {
		Box::pin(async move {
			let os_path = self.to_os_path(key).await?;

			let mut file = match File::open(&os_path).await {
				Ok(f) => f,
				Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
				Err(e) => return Err(format!("Failed to open file: {}", e)),
			};

			// Metadata from the open handle, not from the path. A write commits
			// by renaming a staged file onto the target, so a `stat` taken
			// before the `open` can describe an inode the `open` then replaces
			// -- and the size it reported has already been promised to the
			// client as `Content-Length`. Reading it from the descriptor
			// describes the bytes this stream will actually yield.
			let metadata =
				file.metadata().await.map_err(|e| format!("Failed to get metadata: {}", e))?;

			// A directory answers `metadata` but cannot be read as an object.
			// Reporting it absent matches `get`, which reads and fails.
			if !metadata.is_file() {
				return Ok(None);
			}

			let size = metadata.len();
			let range = match range {
				Some(r) => match r.resolve(size) {
					Some(r) => r,
					None => return Ok(None),
				},
				None => 0..size,
			};

			if range.start > 0 {
				file.seek(SeekFrom::Start(range.start))
					.await
					.map_err(|e| format!("Failed to seek in file: {}", e))?;
			}

			// `take` bounds the reader at the end of the range, so a file that
			// grows after the metadata was read does not yield more bytes than
			// the `Content-Length` already sent.
			let reader = file.take(range.end - range.start);
			// The default capacity is 4 KiB, which costs one blocking-pool
			// dispatch, one read syscall and one allocation per 4 KiB served.
			let stream = ReaderStream::with_capacity(reader, READ_CHUNK_SIZE)
				.map_err(|e| format!("Failed to read file: {e}"));

			let updated = metadata.modified().unwrap_or_else(|_| SystemTime::now()).into();

			Ok(Some(ObjectStream {
				meta: ObjectMeta {
					size,
					updated,
					key: key.to_owned(),
					etag: weak_etag(&metadata),
					content_type: None,
				},
				range,
				stream: stream.boxed(),
			}))
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
				etag: weak_etag(&metadata),
				content_type: None,
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
	) -> Pin<Box<dyn Future<Output = Result<Vec<ListEntry>, String>> + Send + 'a>> {
		Box::pin(async move {
			// Keys are listed as they are spelled on disk, so a bucket that
			// lowercases its paths matches a lowercased prefix and start.
			let lowered;
			let opts = if self.options.lowercase_paths {
				lowered = ListOptions {
					start: opts.start.as_ref().map(|s| s.to_lowercase()),
					prefix: opts.prefix.as_ref().map(|s| s.to_lowercase()),
					..opts.clone()
				};
				&lowered
			} else {
				opts
			};

			let directory = opts.directory().to_owned();
			let os_path = self.to_os_prefix_path(&ObjectKey::new(directory.as_str())).await?;
			if !Self::path_exists(&os_path).await?
				|| !tokio::fs::metadata(&os_path)
					.await
					.map_err(|e| format!("Failed to get metadata: {}", e))?
					.is_dir()
			{
				return Ok(Vec::new());
			}

			// An entry that is a symlink is listed only if it resolves inside the
			// root, so the listing reports no object that `get` or `head` refuses.
			let canonical_root = self.canonical_root().await?;
			let prefix = opts.prefix();

			let mut candidates = Vec::new();
			let mut pending = vec![(os_path, directory)];
			while let Some((dir, dir_key)) = pending.pop() {
				let mut read_dir = tokio::fs::read_dir(&dir)
					.await
					.map_err(|e| format!("Failed to read directory: {}", e))?;
				while let Ok(Some(entry)) = read_dir.next_entry().await {
					let Some((path, name, metadata)) =
						Self::listable(&entry, &canonical_root).await
					else {
						continue;
					};
					if metadata.is_dir() {
						let group = format!("{dir_key}{name}/");
						// Skip a directory no key of the prefix can be inside.
						if !group.starts_with(prefix) && !prefix.starts_with(group.as_str()) {
							continue;
						}
						if opts.delimiter && group.starts_with(prefix) {
							// A group stands for the objects in it, so a
							// directory holding none is not reported.
							if Self::holds_object(path, &canonical_root).await {
								candidates.push(ListEntry::Prefix(group));
							}
						} else {
							pending.push((path, group));
						}
						continue;
					}
					let updated = metadata.modified().unwrap_or_else(|_| SystemTime::now()).into();
					candidates.push(ListEntry::Object(ObjectMeta {
						key: ObjectKey::new(format!("{dir_key}{name}")),
						size: metadata.len(),
						updated,
						etag: weak_etag(&metadata),
						content_type: None,
					}));
				}
			}

			candidates.sort_by(|a, b| a.key().cmp(b.key()));
			let mut listing = Listing::new(opts);
			for candidate in candidates {
				let more = match candidate {
					ListEntry::Object(meta) => listing.push_object(meta),
					ListEntry::Prefix(group) => listing.push_prefix(group),
				};
				if !more {
					break;
				}
			}
			Ok(listing.finish())
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

	/// The file store, and a prefixed store over it, answer listings as every
	/// other backend does.
	#[tokio::test]
	async fn list_semantics_match_the_other_backends() {
		use std::sync::Arc;

		use super::super::list_conformance;

		let dir = TempDir::new().unwrap();
		list_conformance::check("file", &open_store(dir.path(), "").await.0).await;

		let shared_dir = TempDir::new().unwrap();
		let shared: Arc<dyn ObjectStore> = Arc::new(open_store(shared_dir.path(), "").await.0);
		list_conformance::check_prefixed("prefixed file", shared).await;
	}

	/// An empty directory is not a group: a group stands for objects.
	#[tokio::test]
	async fn list_omits_an_empty_directory_from_groups() {
		use super::super::ListEntry;

		let dir = TempDir::new().unwrap();
		let (store, root) = open_store(dir.path(), "").await;
		std::fs::create_dir_all(root.join("empty/deeper")).unwrap();
		store.put(&ObjectKey::new("/full/x.txt"), Bytes::from_static(b"x")).await.unwrap();

		let listed = store
			.list(&ListOptions {
				delimiter: true,
				..Default::default()
			})
			.await
			.unwrap();
		let keys: Vec<&str> = listed.iter().map(ListEntry::key).collect();
		assert_eq!(keys, vec!["/full/"]);
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
		let keys: Vec<_> = listed.into_iter().map(|m| m.key().to_string()).collect();
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

	#[tokio::test]
	async fn get_streamed_honours_the_contract() {
		let dir = TempDir::new().unwrap();
		let (store, _) = open_store(dir.path(), "").await;
		crate::buc::store::assert_get_streamed_contract(&store).await;
	}

	#[tokio::test]
	async fn get_streamed_reports_a_directory_as_absent() {
		// `metadata` succeeds on a directory, so without the file check a
		// directory would be opened and then fail mid-stream, after the status
		// line was already sent.
		let dir = TempDir::new().unwrap();
		let (store, root) = open_store(dir.path(), "").await;
		tokio::fs::create_dir(root.join("subdir")).await.unwrap();

		assert!(store.get_streamed(&ObjectKey::new("/subdir"), None).await.unwrap().is_none());
	}

	#[tokio::test]
	async fn put_streamed_honours_the_contract() {
		let dir = TempDir::new().unwrap();
		let (store, _) = open_store(dir.path(), "").await;
		crate::buc::store::assert_put_streamed_contract(&store).await;
	}

	#[tokio::test]
	async fn an_aborted_write_leaves_no_temporary_file() {
		// The partial object is staged beside its target, so a failed write that
		// forgot to clean up would leave a hidden file in the bucket directory
		// rather than a visible one. `list` would not show it and the disk would
		// fill anyway.
		let dir = TempDir::new().unwrap();
		let (store, root) = open_store(dir.path(), "").await;

		let aborted: futures::stream::BoxStream<'static, Result<Bytes, String>> =
			Box::pin(futures::stream::iter(vec![
				Ok(Bytes::from_static(b"partial")),
				Err("client went away".to_string()),
			]));
		store
			.put_streamed(&ObjectKey::new("/doomed.bin"), aborted, PutCondition::Overwrite.into())
			.await
			.unwrap_err();

		let left: Vec<String> = std::fs::read_dir(&root)
			.unwrap()
			.map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
			.collect();
		assert!(left.is_empty(), "the bucket directory should be empty, found {left:?}");
	}

	#[tokio::test]
	async fn a_write_is_not_visible_until_it_completes() {
		// `put` renames a staged file into place, so a reader never observes a
		// key that exists but holds fewer bytes than were written.
		let dir = TempDir::new().unwrap();
		let (store, _) = open_store(dir.path(), "").await;
		let key = ObjectKey::new("/nested/dir/object.bin");

		store.put(&key, Bytes::from_static(b"first")).await.unwrap();
		assert_eq!(store.get(&key).await.unwrap().as_deref(), Some(&b"first"[..]));

		store.put(&key, Bytes::from_static(b"second value")).await.unwrap();
		assert_eq!(store.get(&key).await.unwrap().as_deref(), Some(&b"second value"[..]));
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

	/// `ObjectKey::new` collapses separator aliases onto the canonical key
	/// before permission checks or the store see it, and keys that still carry
	/// a trailing separator (such as `prefix.join(&ObjectKey::default())`) or
	/// resolve to the bucket root are refused as object keys while accepted as
	/// list prefixes.
	#[tokio::test]
	async fn direct_bucket_rejects_an_aliased_key() {
		let dir = TempDir::new().unwrap();
		let (store, _root) = open_store(dir.path(), "").await;
		let canonical = ObjectKey::new("/private/secret".to_string());
		store.put(&canonical, Bytes::from_static(b"top secret")).await.unwrap();

		for alias in ["//private/secret", "/private//secret", "/private/secret/"] {
			assert_eq!(ObjectKey::new(alias), canonical, "{alias} must normalise to {canonical}");
		}

		let trailing = ObjectKey::new("/private/secret").join(&ObjectKey::default());
		let root_key = ObjectKey::default();
		for bad in [&trailing, &root_key] {
			assert!(store.get(bad).await.is_err(), "get {bad} must be refused");
			assert!(store.head(bad).await.is_err(), "head {bad} must be refused");
			assert!(
				store.put(bad, Bytes::from_static(b"x")).await.is_err(),
				"put {bad} must be refused"
			);
			assert!(store.copy(&canonical, bad).await.is_err(), "copy to {bad} must be refused");
		}
		assert_eq!(
			store.get(&canonical).await.unwrap().unwrap(),
			Bytes::from_static(b"top secret")
		);

		let opts = ListOptions {
			prefix: Some("/private/".to_owned()),
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
		let mut keys: Vec<String> = listed.iter().map(|m| m.key().to_string()).collect();
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
