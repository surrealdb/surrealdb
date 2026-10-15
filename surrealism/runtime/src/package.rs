//! `.surli` archive format: tar + zstd containing `mod.wasm`, the package file,
//! exports, an optional `surrealism/fs/` filesystem, a `surrealism/MANIFEST`
//! that lists every file with its hash, and an optional `surrealism/SIGNATURE`.
//! Only WASM components are accepted.
//!
//! The package file is `surrealism/package.toml`. The loader also accepts the
//! legacy `surrealism/surrealism.toml`, and reads it only when the archive has
//! no `surrealism/package.toml`. The manifest covers both when both are present.

use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};

use sha2::{Digest as _, Sha256};
use surrealism_types::err::{PrefixErr, SurrealismError, SurrealismResult};
use tar::Archive;
use tempfile::TempDir;
use zstd::stream::read::Decoder;

use crate::config::{AbiVersion, LEGACY_PACKAGE_FILE, PACKAGE_FILE, SurrealismConfig};
use crate::exports::ExportsManifest;
pub use crate::manifest::{Digest, MANIFEST_PATH, Manifest, SIGNATURE_PATH, validate_path};

/// The 8-byte preamble of a WASM component (layer 1, version 0x0d).
const COMPONENT_PREAMBLE: [u8; 8] = [0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];

/// PNG file signature (first 8 bytes of any valid PNG).
const PNG_SIGNATURE: [u8; 8] = [0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A];

/// Maximum allowed logo size: 256 KiB.
pub const MAX_LOGO_BYTES: usize = 256 * 1024;

/// Default aggregate size limit for attached filesystem entries: 100 MiB.
pub const DEFAULT_MAX_FS_BYTES: u64 = 100 * 1024 * 1024;

/// Verify that the WASM bytes represent a component (not a core module).
fn verify_component(wasm: &[u8]) -> SurrealismResult<()> {
	if wasm.len() < 8 || wasm[..8] != COMPONENT_PREAMBLE {
		return Err(SurrealismError::Other(anyhow::anyhow!(
			"expected a WASM component but found a core module. \
			 Core modules (WASI Preview 1) are no longer supported — \
			 compile with `--target wasm32-wasip2` to produce a component"
		)));
	}
	Ok(())
}

/// Validate that logo bytes are a valid PNG within the size limit.
pub fn verify_logo(bytes: &[u8]) -> SurrealismResult<()> {
	if bytes.len() > MAX_LOGO_BYTES {
		return Err(SurrealismError::Other(anyhow::anyhow!(
			"logo.png is too large ({} bytes, max {} bytes / {} KiB)",
			bytes.len(),
			MAX_LOGO_BYTES,
			MAX_LOGO_BYTES / 1024,
		)));
	}
	if bytes.len() < 8 || bytes[..8] != PNG_SIGNATURE {
		return Err(SurrealismError::Other(anyhow::anyhow!(
			"logo.png is not a valid PNG file (invalid file signature)"
		)));
	}
	Ok(())
}

/// Archive path of the package file.
const PACKAGE_ENTRY: &str = "surrealism/package.toml";

/// Archive path of the legacy package file.
const LEGACY_PACKAGE_ENTRY: &str = "surrealism/surrealism.toml";

/// The archive path of the attached filesystem's root directory.
const FS_ROOT: &str = "surrealism/fs";

/// The tar path prefix used for filesystem attachments inside the archive.
const FS_PREFIX: &str = "surrealism/fs/";

/// Extracted filesystem from a `.surli` archive. The `TempDir` keeps the
/// directory alive and cleans it up on drop. `root` points to the actual
/// filesystem root within the temp directory (the `surrealism/fs/` subtree).
pub struct AttachedFs {
	_dir: TempDir,
	root: PathBuf,
}

impl AttachedFs {
	pub fn path(&self) -> &Path {
		&self.root
	}
}

impl std::fmt::Debug for AttachedFs {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AttachedFs").field("root", &self.root).finish()
	}
}

pub struct SurrealismPackage {
	pub config: SurrealismConfig,
	pub wasm: Vec<u8>,
	/// Exported function signatures parsed from `surrealism/exports.toml`.
	pub exports: ExportsManifest,
	/// Extracted filesystem from the archive. Present when the archive
	/// contained `surrealism/fs/` entries.
	pub fs: Option<AttachedFs>,
	/// Optional logo image bundled as `surrealism/logo.png` in the archive.
	pub logo: Option<Vec<u8>>,
	/// `surrealism/MANIFEST` of the archive this package was read from, after
	/// the loader checked every file against it. `None` for a package that was
	/// assembled in memory and not read from an archive.
	pub manifest: Option<Manifest>,
	/// Raw bytes of `surrealism/SIGNATURE`, if the archive carries one. The
	/// loader does not interpret or verify them.
	pub signature: Option<Vec<u8>>,
}

/// Options controlling how `from_reader` extracts filesystem entries.
pub struct UnpackOptions<'a> {
	/// Base directory for creating temp dirs (e.g. server's `--temporary-directory`).
	/// Takes priority when set.
	pub temp_base: Option<&'a Path>,
	/// Prefix for the temp directory name (e.g. `SURREAL_MODFS_ns_db_mod_`).
	pub temp_prefix: &'a str,
	/// Maximum aggregate size in bytes for attached filesystem entries.
	/// Defaults to [`DEFAULT_MAX_FS_BYTES`] (100 MiB). Configurable via
	/// `SURREAL_SURREALISM_MAX_FS_BYTES` in server deployments.
	pub max_fs_bytes: u64,
}

impl Default for UnpackOptions<'_> {
	fn default() -> Self {
		Self {
			temp_base: None,
			temp_prefix: "SURREAL_MODFS_local_",
			max_fs_bytes: DEFAULT_MAX_FS_BYTES,
		}
	}
}

/// Create a `TempDir` following the priority: configured base > system temp.
/// Returns an error if neither succeeds.
fn create_temp_dir(opts: &UnpackOptions<'_>) -> SurrealismResult<TempDir> {
	let mut builder = tempfile::Builder::new();
	builder.prefix(opts.temp_prefix);
	if let Some(base) = opts.temp_base {
		match builder.tempdir_in(base) {
			Ok(dir) => return Ok(dir),
			Err(e) => {
				tracing::warn!(
					base = %base.display(),
					error = %e,
					"Configured temporary directory unusable, falling back to system temp"
				);
			}
		}
	}
	builder.tempdir().prefix_err(|| {
		"Failed to create temporary directory for module filesystem. \
		 Configure --temporary-directory or ensure the system temp directory is writable"
	})
}

/// `surrealism/MANIFEST` and `surrealism/SIGNATURE` of an archive, read
/// without unpacking or hashing any other file. See [`trust_files_from_reader`].
#[derive(Debug)]
pub struct TrustFiles {
	/// The parsed manifest. Its [`Manifest::digest`] is the archive's digest.
	pub manifest: Manifest,
	/// Raw bytes of `surrealism/SIGNATURE`, if the archive carries one.
	pub signature: Option<Vec<u8>>,
}

/// Read only `surrealism/MANIFEST` and `surrealism/SIGNATURE` from a `.surli`
/// archive.
///
/// Every entry is held to rules 2 to 4 of [`SurrealismPackage::from_reader`]
/// (no path twice, canonical paths, files and directories only), so the
/// manifest read here is the one the loader reads. Rule 1, that the files
/// match the manifest, is not checked: it needs every file hashed, and
/// [`SurrealismPackage::from_reader`] checks it before any code is handed out.
///
/// At most [`archive_byte_limit`]`(max_fs_bytes)` bytes are decompressed.
pub fn trust_files_from_reader<R: Read>(
	reader: R,
	max_fs_bytes: u64,
) -> SurrealismResult<TrustFiles> {
	let mut archive = open_archive(reader, archive_byte_limit(max_fs_bytes))?;

	let mut manifest: Option<Vec<u8>> = None;
	let mut signature: Option<Vec<u8>> = None;
	let mut seen: HashSet<String> = HashSet::new();
	for entry in archive.entries().prefix_err(|| "Failed to read archive entries")? {
		let mut entry = entry.prefix_err(|| "Failed to read archive entry")?;
		let (path, is_dir) = entry_path(&mut entry)?;
		if !seen.insert(path.clone()) {
			return Err(archive_err(format!("{path:?} appears more than once")));
		}
		if is_dir {
			continue;
		}
		match path.as_str() {
			MANIFEST_PATH => {
				manifest = Some(read_bounded(&mut entry, &path, MAX_MANIFEST_BYTES)?);
			}
			SIGNATURE_PATH => {
				signature = Some(read_bounded(&mut entry, &path, MAX_SIGNATURE_BYTES)?);
			}
			_ => {
				std::io::copy(&mut entry, &mut std::io::sink())
					.prefix_err(|| format!("Failed to skip archive entry: {path}"))?;
			}
		}
	}

	let Some(manifest) = manifest else {
		return Err(archive_err(format!(
			"{MANIFEST_PATH} not found; an archive built with an SDK older than ABI {} must be rebuilt",
			AbiVersion::CURRENT.0
		)));
	};
	Ok(TrustFiles {
		manifest: Manifest::parse(manifest)?,
		signature,
	})
}

/// Room in [`archive_byte_limit`] for everything but `mod.wasm` and `fs/`:
/// the TOML files, the logo, `MANIFEST`, `SIGNATURE` and the tar headers.
const ARCHIVE_OVERHEAD_BYTES: u64 = 64 * 1024 * 1024;

/// The most an archive may decompress to, given the server's bound on
/// attached files: [`MAX_WASM_BYTES`], `max_fs_bytes` and room for the rest.
/// It bounds the work a small, highly compressed archive can cause, including
/// for entries the reader skips or only hashes.
pub fn archive_byte_limit(max_fs_bytes: u64) -> u64 {
	MAX_WASM_BYTES.saturating_add(max_fs_bytes).saturating_add(ARCHIVE_OVERHEAD_BYTES)
}

/// A reader that fails once more than `limit` bytes have come through it.
struct LimitedReader<R> {
	inner: R,
	read: u64,
	limit: u64,
}

impl<R: Read> Read for LimitedReader<R> {
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
		let n = self.inner.read(buf)?;
		self.read += n as u64;
		if self.read > self.limit {
			return Err(std::io::Error::other(format!(
				"invalid archive: it decompresses to more than {} bytes",
				self.limit
			)));
		}
		Ok(n)
	}
}

/// The tar entries of a zstd-compressed archive, decompressing at most
/// `limit` bytes.
fn open_archive<R: Read>(reader: R, limit: u64) -> SurrealismResult<Archive<impl Read>> {
	let inner =
		Decoder::new(BufReader::new(reader)).prefix_err(|| "Failed to create zstd decoder")?;
	Ok(Archive::new(LimitedReader {
		inner,
		read: 0,
		limit,
	}))
}

/// Maximum size of `surrealism/MANIFEST`: 16 MiB.
pub const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;

/// Maximum size of `surrealism/mod.wasm`: 512 MiB.
///
/// The component is read into memory, so without a bound a small, highly
/// compressed archive could make the loader allocate without limit. The bound
/// leaves room for a component whose initial memory image is large.
pub const MAX_WASM_BYTES: u64 = 512 * 1024 * 1024;

/// Maximum size of `surrealism/SIGNATURE`: 1 MiB.
pub const MAX_SIGNATURE_BYTES: u64 = 1024 * 1024;

/// Length of the name field in a tar header. A GNU long-name entry leaves the
/// header holding the path truncated to this many bytes, less up to three bytes
/// backed off to a UTF-8 boundary.
const TAR_NAME_LEN: usize = 100;

fn archive_err(msg: impl std::fmt::Display) -> SurrealismError {
	SurrealismError::Other(anyhow::anyhow!("invalid archive: {msg}"))
}

/// A writer that hashes everything written through it.
struct HashingWriter<W> {
	inner: W,
	hasher: Sha256,
}

impl<W: Write> HashingWriter<W> {
	fn new(inner: W) -> Self {
		Self {
			inner,
			hasher: Sha256::new(),
		}
	}

	fn finish(self) -> Digest {
		Digest(self.hasher.finalize().into())
	}
}

impl<W: Write> Write for HashingWriter<W> {
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
		let n = self.inner.write(buf)?;
		self.hasher.update(&buf[..n]);
		Ok(n)
	}

	fn flush(&mut self) -> std::io::Result<()> {
		self.inner.flush()
	}
}

/// Resolve and check an entry's path: its type, its canonical form, and that
/// no extension header renamed it.
///
/// `tar` resolves a path from a GNU long-name entry first, then a PAX `path`
/// record, then the header's own name field. A resolved path that differs from
/// the header's name is accepted only as a GNU long name, where the header
/// holds the same path truncated to the name field. Any other difference would
/// let a reader that ignores extension headers see another file than this
/// loader does.
///
/// A PAX extension header may hold only a `path` record equal to the resolved
/// path. Any other record, `size` among them, would make a reader that honours
/// it see other bytes than one that does not.
///
/// `MANIFEST` and `SIGNATURE` are files: a directory with either name is
/// refused, so every reader finds them, or their absence, alike.
fn entry_path<R: Read>(entry: &mut tar::Entry<'_, R>) -> SurrealismResult<(String, bool)> {
	let resolved = entry.path_bytes().into_owned();
	let shown = String::from_utf8_lossy(&resolved).into_owned();

	let kind = entry.header().entry_type();
	let is_dir = kind.is_dir();
	if !kind.is_file() && !is_dir {
		return Err(archive_err(format!(
			"entry {shown:?} is a {kind:?}; only regular files and directories are allowed"
		)));
	}

	let header = entry.header().path_bytes().into_owned();
	if resolved != header {
		let truncated_long_name = resolved.len() > TAR_NAME_LEN
			&& header.len() + 3 >= TAR_NAME_LEN
			&& resolved.starts_with(&header);
		if !truncated_long_name {
			return Err(archive_err(format!(
				"entry {:?} is renamed to {shown:?} by an extension header",
				String::from_utf8_lossy(&header)
			)));
		}
	}
	if let Some(pax) = entry.pax_extensions().map_err(archive_err)? {
		for record in pax {
			let record = record.map_err(archive_err)?;
			if record.key_bytes() != b"path" {
				return Err(archive_err(format!(
					"entry {shown:?} carries a PAX {:?} record; only `path` is allowed",
					String::from_utf8_lossy(record.key_bytes())
				)));
			}
			if record.value_bytes() != resolved.as_slice() {
				return Err(archive_err(format!(
					"entry {shown:?} carries a PAX path that differs from its name"
				)));
			}
		}
	}

	let mut path = String::from_utf8(resolved)
		.map_err(|_| archive_err(format!("entry {shown:?} is not valid UTF-8")))?;
	// A directory entry may end in one `/`; the rest of its path is canonical.
	if is_dir && path.ends_with('/') {
		path.pop();
	}
	validate_path(&path).map_err(archive_err)?;
	if is_dir && (path == MANIFEST_PATH || path == SIGNATURE_PATH) {
		return Err(archive_err(format!("{path:?} is a directory; it must be a file")));
	}
	Ok((path, is_dir))
}

/// Read an entry into memory, refusing one larger than `max` bytes.
///
/// The size is the one `tar` reads the entry by. Reading stops one byte past
/// `max`, so an entry that holds more is refused rather than cut short.
fn read_bounded<R: Read>(
	entry: &mut tar::Entry<'_, R>,
	path: &str,
	max: u64,
) -> SurrealismResult<Vec<u8>> {
	let declared = entry.size();
	if declared > max {
		return Err(archive_err(format!("{path} is too large ({declared} bytes, max {max})")));
	}
	// The size is the archive's claim; grow to it as bytes arrive.
	let mut buffer = Vec::with_capacity(declared.min(64 * 1024) as usize);
	entry
		.take(max.saturating_add(1))
		.read_to_end(&mut buffer)
		.prefix_err(|| format!("Failed to read {path}"))?;
	if buffer.len() as u64 > max {
		return Err(archive_err(format!("{path} is larger than {max} bytes")));
	}
	Ok(buffer)
}

fn utf8(bytes: Vec<u8>, path: &str) -> SurrealismResult<String> {
	String::from_utf8(bytes).map_err(|_| archive_err(format!("{path} is not valid UTF-8")))
}

impl SurrealismPackage {
	pub fn from_file(file: PathBuf) -> SurrealismResult<Self> {
		if file.extension().and_then(|s| s.to_str()) != Some("surli") {
			return Err(SurrealismError::Other(anyhow::anyhow!("Only .surli files are supported")));
		}

		if !file.exists() {
			return Err(SurrealismError::Other(anyhow::anyhow!(
				"File not found: {}",
				file.display()
			)));
		}

		let archive_file = File::open(file).prefix_err(|| "Failed to open archive file")?;
		Self::from_reader(archive_file, &UnpackOptions::default())
	}

	/// `sha256(MANIFEST)`: the digest a definition pins. `None` for a package
	/// that was not read from an archive.
	pub fn digest(&self) -> Option<Digest> {
		self.manifest.as_ref().map(Manifest::digest)
	}

	/// Raw bytes of `surrealism/SIGNATURE`, if the archive carries one.
	pub fn signature(&self) -> Option<&[u8]> {
		self.signature.as_deref()
	}

	/// Read a `.surli` archive in one pass, hashing every file as it goes.
	///
	/// The package is returned only after the archive passes four rules:
	///
	/// 1. `surrealism/MANIFEST` lists every file except itself and `surrealism/SIGNATURE`, every
	///    path it lists is a file in the archive, and every hash matches.
	/// 2. No path appears twice.
	/// 3. Every path is canonical (see [`validate_path`]) and no extension header renames an entry.
	/// 4. Every entry is a regular file or a directory, and every directory holds a file the
	///    manifest lists.
	///
	/// At most [`archive_byte_limit`]`(opts.max_fs_bytes)` bytes are
	/// decompressed. `surrealism/fs/` entries are written to a temporary directory during the
	/// pass. If any rule fails, that directory is removed before the error
	/// returns.
	pub fn from_reader<R: Read>(reader: R, opts: &UnpackOptions<'_>) -> SurrealismResult<Self> {
		let mut archive = open_archive(reader, archive_byte_limit(opts.max_fs_bytes))?;

		let mut wasm: Option<Vec<u8>> = None;
		let mut package_toml: Option<String> = None;
		let mut legacy_package_toml: Option<String> = None;
		let mut exports: Option<ExportsManifest> = None;
		let mut fs_dir: Option<TempDir> = None;
		let mut logo: Option<Vec<u8>> = None;
		let mut manifest: Option<Vec<u8>> = None;
		let mut signature: Option<Vec<u8>> = None;
		let mut fs_bytes_total: u64 = 0;
		// Every path seen, files and directories, to refuse a second entry with the same path.
		let mut seen: HashSet<String> = HashSet::new();
		// Every file the manifest must list, with the hash of its contents.
		let mut hashed: BTreeMap<String, Digest> = BTreeMap::new();
		// Every directory entry, each of which must hold a listed file.
		let mut dirs: Vec<String> = Vec::new();

		for entry in archive.entries().prefix_err(|| "Failed to read archive entries")? {
			let mut entry = entry.prefix_err(|| "Failed to read archive entry")?;
			let (path, is_dir) = entry_path(&mut entry)?;
			if !seen.insert(path.clone()) {
				return Err(archive_err(format!("{path:?} appears more than once")));
			}

			// The fs root itself is only a directory; a file needs a name below it.
			let fs_relative = if is_dir && path == FS_ROOT {
				Some("")
			} else {
				path.strip_prefix(FS_PREFIX)
			};

			if is_dir {
				dirs.push(path.clone());
				if let Some(rel) = fs_relative {
					let dir = match fs_dir.as_ref() {
						Some(dir) => dir,
						None => fs_dir.insert(create_temp_dir(opts)?),
					};
					std::fs::create_dir_all(dir.path().join(FS_ROOT).join(rel))
						.prefix_err(|| format!("Failed to unpack fs directory: {path}"))?;
				}
				continue;
			}

			let hash = match path.as_str() {
				MANIFEST_PATH => {
					manifest = Some(read_bounded(&mut entry, &path, MAX_MANIFEST_BYTES)?);
					continue;
				}
				SIGNATURE_PATH => {
					signature = Some(read_bounded(&mut entry, &path, MAX_SIGNATURE_BYTES)?);
					continue;
				}
				"surrealism/mod.wasm" => {
					let buffer = read_bounded(&mut entry, &path, MAX_WASM_BYTES)?;
					verify_component(&buffer)?;
					let hash = Digest::of(&buffer);
					wasm = Some(buffer);
					hash
				}
				// Parsed after the pass, so the ignored legacy file is never parsed.
				PACKAGE_ENTRY | LEGACY_PACKAGE_ENTRY => {
					let buffer = read_bounded(&mut entry, &path, MAX_MANIFEST_BYTES)?;
					let hash = Digest::of(&buffer);
					let text = utf8(buffer, &path)?;
					if path == PACKAGE_ENTRY {
						package_toml = Some(text);
					} else {
						legacy_package_toml = Some(text);
					}
					hash
				}
				"surrealism/exports.toml" => {
					let buffer = read_bounded(&mut entry, &path, MAX_MANIFEST_BYTES)?;
					let hash = Digest::of(&buffer);
					exports = Some(
						ExportsManifest::parse(&utf8(buffer, &path)?)
							.prefix_err(|| "Failed to parse exports.toml")?,
					);
					hash
				}
				"surrealism/logo.png" => {
					let buffer = read_bounded(&mut entry, &path, MAX_LOGO_BYTES as u64)?;
					let hash = Digest::of(&buffer);
					logo = Some(buffer);
					hash
				}
				_ => match fs_relative {
					Some(rel) => {
						// The size `tar` reads the entry by, which bounds the copy below.
						fs_bytes_total = fs_bytes_total.saturating_add(entry.size());
						if fs_bytes_total > opts.max_fs_bytes {
							return Err(SurrealismError::Other(anyhow::anyhow!(
								"attached filesystem exceeds {} MiB limit ({} bytes)",
								opts.max_fs_bytes / (1024 * 1024),
								opts.max_fs_bytes,
							)));
						}
						let dir = match fs_dir.as_ref() {
							Some(dir) => dir,
							None => fs_dir.insert(create_temp_dir(opts)?),
						};
						// The path is canonical and no entry is a link, so the
						// target stays inside the temporary directory.
						let target = dir.path().join(FS_ROOT).join(rel);
						if let Some(parent) = target.parent() {
							std::fs::create_dir_all(parent)
								.prefix_err(|| format!("Failed to unpack fs entry: {path}"))?;
						}
						let file = File::create(&target)
							.prefix_err(|| format!("Failed to unpack fs entry: {path}"))?;
						let mut writer = HashingWriter::new(file);
						std::io::copy(&mut entry, &mut writer)
							.prefix_err(|| format!("Failed to unpack fs entry: {path}"))?;
						writer.finish()
					}
					// Not used by the loader, but still covered by the manifest.
					None => {
						let mut writer = HashingWriter::new(std::io::sink());
						std::io::copy(&mut entry, &mut writer)
							.prefix_err(|| format!("Failed to read archive entry: {path}"))?;
						writer.finish()
					}
				},
			};
			hashed.insert(path, hash);
		}

		let config = match (package_toml, legacy_package_toml) {
			(Some(toml), _) => Some(
				SurrealismConfig::parse(&toml)
					.prefix_err(|| format!("Failed to parse {PACKAGE_FILE}"))?,
			),
			(None, Some(toml)) => Some(
				SurrealismConfig::parse(&toml)
					.prefix_err(|| format!("Failed to parse {LEGACY_PACKAGE_FILE}"))?,
			),
			(None, None) => None,
		};

		let Some(manifest) = manifest else {
			// An archive from an older SDK has no manifest; its ABI says why.
			if let Some(config) = &config
				&& config.abi != AbiVersion::CURRENT
			{
				return Err(SurrealismError::UnsupportedAbi {
					expected: AbiVersion::CURRENT.0,
					got: config.abi.0,
				});
			}
			return Err(archive_err(format!("{MANIFEST_PATH} not found")));
		};
		let manifest = Manifest::parse(manifest)?;
		check_manifest(&manifest, &hashed)?;
		check_dirs(&manifest, &dirs)?;

		let wasm = wasm.ok_or_else(|| anyhow::anyhow!("mod.wasm not found in archive"))?;
		let config =
			config.ok_or_else(|| anyhow::anyhow!("{PACKAGE_FILE} not found in archive"))?;
		let exports =
			exports.ok_or_else(|| anyhow::anyhow!("exports.toml not found in archive"))?;

		if let Some(ref logo_bytes) = logo {
			verify_logo(logo_bytes)?;
		}

		let fs = fs_dir.map(|dir| {
			let root = dir.path().join(FS_ROOT);
			AttachedFs {
				_dir: dir,
				root,
			}
		});

		Ok(SurrealismPackage {
			config,
			wasm,
			exports,
			fs,
			logo,
			manifest: Some(manifest),
			signature,
		})
	}

	/// Write the package as a `.surli` archive with a `surrealism/MANIFEST`
	/// covering every file, and return the archive's digest.
	///
	/// `fs_dir` is attached as `surrealism/fs/`; symbolic links in it are
	/// followed. `signature`, when set, is written as `surrealism/SIGNATURE`
	/// unchanged; it is valid only if it was made for the digest this call
	/// returns. `manifest` is ignored and recomputed.
	///
	/// Entries are written in a fixed order with fixed metadata, so the same
	/// inputs produce the same archive bytes.
	#[allow(clippy::needless_pass_by_value)] // Public API: keeps caller ergonomics for `pack(out, …)`.
	pub fn pack(&self, output: PathBuf, fs_dir: Option<&Path>) -> SurrealismResult<Digest> {
		if output.extension().and_then(|s| s.to_str()) != Some("surli") {
			return Err(SurrealismError::Other(anyhow::anyhow!(
				"Output file must have .surli extension"
			)));
		}

		match (&self.config.attach.fs, &fs_dir) {
			(Some(cfg_fs), None) => {
				tracing::warn!(
					attach_fs = %cfg_fs,
					"config.attach.fs is set but no fs_dir was provided to pack()"
				);
			}
			(None, Some(dir)) => {
				tracing::warn!(
					fs_dir = %dir.display(),
					"fs_dir provided to pack() but config.attach.fs is not set"
				);
			}
			_ => {}
		}

		if let Some(ref logo_bytes) = self.logo {
			verify_logo(logo_bytes)?;
		}

		let config = self.config.to_toml().prefix_err(|| "Failed to serialize config")?;
		let exports =
			self.exports.to_toml().prefix_err(|| "Failed to serialize exports manifest")?;

		let mut files: Vec<(String, Vec<u8>)> = vec![
			("surrealism/mod.wasm".to_string(), self.wasm.clone()),
			(PACKAGE_ENTRY.to_string(), config.into_bytes()),
			("surrealism/exports.toml".to_string(), exports.into_bytes()),
		];
		if let Some(ref logo_bytes) = self.logo {
			files.push(("surrealism/logo.png".to_string(), logo_bytes.clone()));
		}
		let mut dirs: Vec<String> = Vec::new();
		if let Some(dir) = fs_dir {
			dirs.push(FS_ROOT.to_string());
			collect_fs(dir, FS_ROOT, &mut dirs, &mut files)?;
		}
		// The loader refuses a directory that holds no listed file, so an
		// empty one is not written.
		dirs.retain(|dir| {
			let prefix = format!("{dir}/");
			files.iter().any(|(path, _)| path.starts_with(&prefix))
		});

		// Built before anything is written, so a path the manifest cannot carry
		// fails without leaving a partial archive.
		let manifest = Manifest::from_entries(
			files.iter().map(|(path, bytes)| (path.clone(), Digest::of(bytes))),
		)?;

		let file = File::create(&output).prefix_err(|| "Failed to create output file")?;
		let encoder =
			zstd::stream::Encoder::new(file, 0).prefix_err(|| "Failed to create zstd encoder")?;
		let mut archive = tar::Builder::new(encoder);

		for path in &dirs {
			let mut header = tar::Header::new_gnu();
			header.set_entry_type(tar::EntryType::Directory);
			header.set_size(0);
			header.set_mode(0o755);
			archive
				.append_data(&mut header, path, std::io::empty())
				.prefix_err(|| format!("Failed to add {path} to archive"))?;
		}
		let trailer = std::iter::once((MANIFEST_PATH, manifest.as_bytes()))
			.chain(self.signature.as_deref().map(|sig| (SIGNATURE_PATH, sig)));
		for (path, bytes) in
			files.iter().map(|(path, bytes)| (path.as_str(), bytes.as_slice())).chain(trailer)
		{
			let mut header = tar::Header::new_gnu();
			header.set_size(bytes.len() as u64);
			header.set_mode(0o644);
			archive
				.append_data(&mut header, path, bytes)
				.prefix_err(|| format!("Failed to add {path} to archive"))?;
		}

		let encoder = archive.into_inner().prefix_err(|| "Failed to get encoder from archive")?;
		encoder.finish().prefix_err(|| "Failed to finish zstd encoder")?;

		Ok(manifest.digest())
	}
}

/// Compare the files found in the archive with the manifest.
/// Refuses a directory entry that holds no file the manifest lists.
fn check_dirs(manifest: &Manifest, dirs: &[String]) -> SurrealismResult<()> {
	// Every directory that holds a listed file: the proper ancestors of each
	// listed path. Paths are canonical, so `/` separates every segment.
	let mut holding: HashSet<&str> = HashSet::new();
	for (path, _) in manifest.entries() {
		let mut rest = path.as_str();
		while let Some(at) = rest.rfind('/') {
			rest = &rest[..at];
			if !holding.insert(rest) {
				break;
			}
		}
	}
	if let Some(dir) = dirs.iter().find(|dir| !holding.contains(dir.as_str())) {
		return Err(archive_err(format!(
			"directory {dir:?} holds no file listed in {MANIFEST_PATH}"
		)));
	}
	Ok(())
}

/// Check a `.surli` archive against rules 1 to 4 of
/// [`SurrealismPackage::from_reader`] by hashing every file, without
/// unpacking, parsing or compiling anything, and return its `MANIFEST` and
/// `SIGNATURE`.
///
/// This is what a signer runs before signing, so that it never signs a
/// manifest the loader would refuse to match against the archive's files. At
/// most [`archive_byte_limit`]`(max_fs_bytes)` bytes are decompressed.
pub fn verify_archive<R: Read>(reader: R, max_fs_bytes: u64) -> SurrealismResult<TrustFiles> {
	let mut archive = open_archive(reader, archive_byte_limit(max_fs_bytes))?;
	let mut manifest: Option<Vec<u8>> = None;
	let mut signature: Option<Vec<u8>> = None;
	let mut seen: HashSet<String> = HashSet::new();
	let mut hashed: BTreeMap<String, Digest> = BTreeMap::new();
	let mut dirs: Vec<String> = Vec::new();
	for entry in archive.entries().prefix_err(|| "Failed to read archive entries")? {
		let mut entry = entry.prefix_err(|| "Failed to read archive entry")?;
		let (path, is_dir) = entry_path(&mut entry)?;
		if !seen.insert(path.clone()) {
			return Err(archive_err(format!("{path:?} appears more than once")));
		}
		if is_dir {
			dirs.push(path);
			continue;
		}
		match path.as_str() {
			MANIFEST_PATH => {
				manifest = Some(read_bounded(&mut entry, &path, MAX_MANIFEST_BYTES)?);
			}
			SIGNATURE_PATH => {
				signature = Some(read_bounded(&mut entry, &path, MAX_SIGNATURE_BYTES)?);
			}
			_ => {
				let mut writer = HashingWriter::new(std::io::sink());
				std::io::copy(&mut entry, &mut writer)
					.prefix_err(|| format!("Failed to read archive entry: {path}"))?;
				hashed.insert(path, writer.finish());
			}
		}
	}
	let Some(manifest) = manifest else {
		return Err(archive_err(format!("{MANIFEST_PATH} not found")));
	};
	let manifest = Manifest::parse(manifest)?;
	check_manifest(&manifest, &hashed)?;
	check_dirs(&manifest, &dirs)?;
	Ok(TrustFiles {
		manifest,
		signature,
	})
}

fn check_manifest(manifest: &Manifest, hashed: &BTreeMap<String, Digest>) -> SurrealismResult<()> {
	for (path, hash) in hashed {
		match manifest.get(path) {
			None => {
				return Err(archive_err(format!("{path:?} is not listed in {MANIFEST_PATH}")));
			}
			Some(listed) if listed != *hash => {
				return Err(archive_err(format!(
					"{path:?} does not match {MANIFEST_PATH}: listed {listed}, found {hash}"
				)));
			}
			Some(_) => {}
		}
	}
	if let Some((path, _)) = manifest.entries().iter().find(|(path, _)| !hashed.contains_key(path))
	{
		return Err(archive_err(format!(
			"{path:?} is listed in {MANIFEST_PATH} but not a file in the archive"
		)));
	}
	Ok(())
}

/// Collect the files and directories under `dir` as archive paths below
/// `prefix`, in a fixed order.
fn collect_fs(
	dir: &Path,
	prefix: &str,
	dirs: &mut Vec<String>,
	files: &mut Vec<(String, Vec<u8>)>,
) -> SurrealismResult<()> {
	let mut children: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)
		.prefix_err(|| format!("Failed to read directory {}", dir.display()))?
		.collect::<Result<_, _>>()
		.prefix_err(|| format!("Failed to read directory {}", dir.display()))?;
	children.sort_by_key(std::fs::DirEntry::file_name);
	for child in children {
		let name = child.file_name();
		let name = name.to_str().ok_or_else(|| {
			archive_err(format!(
				"{} has a file name that is not valid UTF-8",
				child.path().display()
			))
		})?;
		let path = format!("{prefix}/{name}");
		validate_path(&path).map_err(archive_err)?;
		// Follows symbolic links, so the archive holds what they point at.
		let meta = std::fs::metadata(child.path())
			.prefix_err(|| format!("Failed to read {}", child.path().display()))?;
		if meta.is_dir() {
			dirs.push(path.clone());
			collect_fs(&child.path(), &path, dirs, files)?;
		} else if meta.is_file() {
			let bytes = std::fs::read(child.path())
				.prefix_err(|| format!("Failed to read {}", child.path().display()))?;
			files.push((path, bytes));
		} else {
			return Err(archive_err(format!(
				"{} is neither a regular file nor a directory",
				child.path().display()
			)));
		}
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use tar::EntryType;

	use super::*;

	const CONFIG: &str =
		"abi = 4\n\n[package]\norganisation = \"t\"\nname = \"t\"\nversion = \"0.1.0\"\n";

	/// One tar entry, written byte for byte so a test can express shapes that
	/// `tar::Builder` refuses to produce.
	struct Raw {
		name: Vec<u8>,
		kind: EntryType,
		data: Vec<u8>,
		/// PAX records written in an extension header before the entry.
		pax: Vec<(&'static str, String)>,
	}

	fn dir(name: &str) -> Raw {
		Raw {
			name: name.as_bytes().to_vec(),
			kind: EntryType::Directory,
			data: Vec::new(),
			pax: Vec::new(),
		}
	}

	fn file(name: &str, data: &[u8]) -> Raw {
		Raw {
			name: name.as_bytes().to_vec(),
			kind: EntryType::Regular,
			data: data.to_vec(),
			pax: Vec::new(),
		}
	}

	/// The files every valid archive needs.
	fn base_files() -> Vec<(String, Vec<u8>)> {
		vec![
			("surrealism/mod.wasm".into(), COMPONENT_PREAMBLE.to_vec()),
			(PACKAGE_ENTRY.into(), CONFIG.as_bytes().to_vec()),
			(
				"surrealism/exports.toml".into(),
				ExportsManifest::empty().to_toml().unwrap().into_bytes(),
			),
		]
	}

	fn manifest_of(files: &[(String, Vec<u8>)]) -> Vec<u8> {
		Manifest::from_entries(files.iter().map(|(p, b)| (p.clone(), Digest::of(b))))
			.unwrap()
			.as_bytes()
			.to_vec()
	}

	/// `files` followed by a manifest covering exactly `files`.
	fn listed(files: &[(String, Vec<u8>)]) -> Vec<Raw> {
		let mut raws: Vec<Raw> = files.iter().map(|(p, b)| file(p, b)).collect();
		raws.push(file(MANIFEST_PATH, &manifest_of(files)));
		raws
	}

	fn archive(entries: &[Raw]) -> Vec<u8> {
		let mut builder = tar::Builder::new(Vec::new());
		for e in entries {
			if !e.pax.is_empty() {
				let mut record = String::new();
				for (key, value) in &e.pax {
					let body = format!("{key}={value}\n");
					// A PAX record's length prefix counts itself and the space after it.
					let mut len = body.len() + 2;
					while len.to_string().len() + 1 + body.len() != len {
						len += 1;
					}
					record.push_str(&format!("{len} {body}"));
				}
				let mut h = tar::Header::new_ustar();
				h.set_entry_type(EntryType::XHeader);
				h.set_size(record.len() as u64);
				h.set_cksum();
				builder.append(&h, record.as_bytes()).unwrap();
			}
			let mut h = tar::Header::new_gnu();
			h.as_old_mut().name[..e.name.len()].copy_from_slice(&e.name);
			h.set_entry_type(e.kind);
			h.set_size(e.data.len() as u64);
			h.set_mode(0o644);
			if matches!(e.kind, EntryType::Symlink | EntryType::Link) {
				h.set_link_name("surrealism/mod.wasm").unwrap();
			}
			h.set_cksum();
			builder.append(&h, e.data.as_slice()).unwrap();
		}
		zstd::encode_all(builder.into_inner().unwrap().as_slice(), 0).unwrap()
	}

	fn load(bytes: &[u8]) -> SurrealismResult<SurrealismPackage> {
		SurrealismPackage::from_reader(bytes, &UnpackOptions::default())
	}

	fn load_err(entries: &[Raw]) -> String {
		match load(&archive(entries)) {
			Ok(_) => panic!("archive was accepted"),
			Err(e) => e.to_string(),
		}
	}

	#[test]
	fn round_trip() {
		let src = tempfile::tempdir().unwrap();
		std::fs::create_dir_all(src.path().join("data/empty")).unwrap();
		std::fs::write(src.path().join("data/model.bin"), b"weights").unwrap();
		// Longer than a tar name field, so the archive carries a GNU long name.
		let long = "n".repeat(120);
		std::fs::write(src.path().join(&long), b"long").unwrap();

		let mut logo = PNG_SIGNATURE.to_vec();
		logo.extend_from_slice(b"png");
		let package = SurrealismPackage {
			config: SurrealismConfig::parse(CONFIG).unwrap(),
			wasm: COMPONENT_PREAMBLE.to_vec(),
			exports: ExportsManifest::empty(),
			fs: None,
			logo: Some(logo.clone()),
			manifest: None,
			signature: None,
		};
		assert_eq!(package.digest(), None);

		let out = tempfile::tempdir().unwrap();
		let a = out.path().join("a.surli");
		let b = out.path().join("b.surli");
		let digest = package.pack(a.clone(), Some(src.path())).unwrap();
		assert_eq!(package.pack(b.clone(), Some(src.path())).unwrap(), digest);
		assert_eq!(std::fs::read(&a).unwrap(), std::fs::read(&b).unwrap(), "pack is deterministic");

		let mut loaded = SurrealismPackage::from_file(a).unwrap();
		assert_eq!(loaded.digest(), Some(digest));
		let manifest = loaded.manifest.as_ref().unwrap();
		assert_eq!(digest, Digest::of(manifest.as_bytes()));
		let paths: Vec<&str> = manifest.entries().iter().map(|(p, _)| p.as_str()).collect();
		let long_path = format!("surrealism/fs/{long}");
		assert_eq!(
			paths,
			[
				"surrealism/exports.toml",
				"surrealism/fs/data/model.bin",
				long_path.as_str(),
				"surrealism/logo.png",
				"surrealism/mod.wasm",
				"surrealism/package.toml",
			]
		);
		assert_eq!(loaded.wasm, COMPONENT_PREAMBLE);
		assert_eq!(loaded.logo.as_deref(), Some(logo.as_slice()));
		assert_eq!(loaded.signature(), None);
		let fs = loaded.fs.as_ref().unwrap().path().to_path_buf();
		assert_eq!(std::fs::read(fs.join("data/model.bin")).unwrap(), b"weights");
		assert_eq!(std::fs::read(fs.join(&long)).unwrap(), b"long");
		assert!(!fs.join("data/empty").exists(), "a directory holding no file is not packed");

		// A signature is carried as raw bytes and does not change the digest.
		loaded.signature = Some(b"-----BEGIN SSH SIGNATURE-----\n".to_vec());
		let signed = out.path().join("signed.surli");
		assert_eq!(loaded.pack(signed.clone(), Some(src.path())).unwrap(), digest);
		let signed = SurrealismPackage::from_file(signed).unwrap();
		assert_eq!(signed.digest(), Some(digest));
		assert_eq!(signed.signature(), Some(b"-----BEGIN SSH SIGNATURE-----\n".as_slice()));
	}

	#[test]
	fn accepts_minimal_archive() {
		let package = load(&archive(&listed(&base_files()))).unwrap();
		assert!(package.fs.is_none());
		assert!(package.digest().is_some());
	}

	#[test]
	fn rejects_missing_manifest() {
		let raws: Vec<Raw> = base_files().iter().map(|(p, b)| file(p, b)).collect();
		assert!(load_err(&raws).contains("MANIFEST not found"));
	}

	#[test]
	fn missing_manifest_on_older_abi_asks_for_rebuild() {
		let mut files = base_files();
		files[1].1 = CONFIG.replace("abi = 4", "abi = 3").into_bytes();
		let raws: Vec<Raw> = files.iter().map(|(p, b)| file(p, b)).collect();
		match load(&archive(&raws)) {
			Err(SurrealismError::UnsupportedAbi {
				expected: 4,
				got: 3,
			}) => {}
			other => panic!("unexpected result: {:?}", other.map(|_| ())),
		}
	}

	#[test]
	fn rule1_rejects_unlisted_file() {
		let mut raws = listed(&base_files());
		raws.push(file("surrealism/fs/extra.txt", b"sneaky"));
		assert!(load_err(&raws).contains("not listed"));
	}

	#[test]
	fn rule1_rejects_listed_file_that_is_missing() {
		let mut files = base_files();
		files.push(("surrealism/fs/gone.txt".into(), b"gone".to_vec()));
		let mut raws = listed(&files);
		raws.retain(|r| r.name != b"surrealism/fs/gone.txt");
		assert!(load_err(&raws).contains("not a file in the archive"));
	}

	#[test]
	fn rule1_rejects_changed_file() {
		let mut raws = listed(&base_files());
		// Still valid TOML, so only the hash check can refuse it.
		raws[2].data.extend_from_slice(b"\n# changed\n");
		assert!(load_err(&raws).contains("does not match"));
	}

	#[test]
	fn rule1_rejects_malformed_manifest() {
		let mut raws = listed(&base_files());
		let last = raws.len() - 1;
		raws[last].data.pop(); // drop the final LF
		assert!(load_err(&raws).contains("LF"));
	}

	#[test]
	fn signature_need_not_be_listed() {
		let mut raws = listed(&base_files());
		raws.push(file(SIGNATURE_PATH, b"{}"));
		let package = load(&archive(&raws)).unwrap();
		assert_eq!(package.signature(), Some(b"{}".as_slice()));
	}

	#[test]
	fn trust_files_read_the_manifest_the_loader_reads() {
		let mut raws = listed(&base_files());
		raws.push(file(SIGNATURE_PATH, b"{}"));
		let bytes = archive(&raws);
		let trust = trust_files_from_reader(bytes.as_slice(), DEFAULT_MAX_FS_BYTES).unwrap();
		let package = load(&bytes).unwrap();
		assert_eq!(Some(trust.manifest.digest()), package.digest());
		assert_eq!(trust.signature.as_deref(), Some(b"{}".as_slice()));
	}

	#[test]
	fn a_directory_must_hold_a_listed_file() {
		let mut files = base_files();
		files.push(("surrealism/fs/data/model.bin".into(), b"weights".to_vec()));
		let mut raws = listed(&files);
		raws.insert(0, dir("surrealism/fs"));
		raws.insert(1, dir("surrealism/fs/data"));
		load(&archive(&raws)).unwrap();

		raws.insert(2, dir("surrealism/fs/unlisted"));
		assert!(load_err(&raws).contains("holds no file listed"));
	}

	#[test]
	fn verify_archive_applies_the_loader_rules() {
		let raws = listed(&base_files());
		let files = verify_archive(archive(&raws).as_slice(), DEFAULT_MAX_FS_BYTES).unwrap();
		assert_eq!(Some(files.manifest.digest()), load(&archive(&raws)).unwrap().digest());

		let mut changed = listed(&base_files());
		changed[2].data.extend_from_slice(b"\n# changed\n");
		let err = verify_archive(archive(&changed).as_slice(), DEFAULT_MAX_FS_BYTES).unwrap_err();
		assert!(err.to_string().contains("does not match"), "{err}");

		let mut unlisted = listed(&base_files());
		unlisted.push(file("surrealism/fs/extra.txt", b"sneaky"));
		let err = verify_archive(archive(&unlisted).as_slice(), DEFAULT_MAX_FS_BYTES).unwrap_err();
		assert!(err.to_string().contains("not listed"), "{err}");

		let mut empty = listed(&base_files());
		empty.insert(0, dir("surrealism/fs"));
		let err = verify_archive(archive(&empty).as_slice(), DEFAULT_MAX_FS_BYTES).unwrap_err();
		assert!(err.to_string().contains("holds no file listed"), "{err}");
	}

	#[test]
	fn decompression_is_bounded() {
		let raws = listed(&base_files());
		let bytes = archive(&raws);
		let mut reader = open_archive(bytes.as_slice(), 512).unwrap();
		let err = reader
			.entries()
			.and_then(|mut entries| {
				entries.try_for_each(|entry| {
					std::io::copy(&mut entry?, &mut std::io::sink()).map(|_| ())
				})
			})
			.unwrap_err();
		assert!(err.to_string().contains("decompresses to more than 512 bytes"), "{err}");
		assert!(archive_byte_limit(DEFAULT_MAX_FS_BYTES) > MAX_WASM_BYTES);
	}

	#[test]
	fn trust_files_refuse_what_the_loader_refuses() {
		let raws: Vec<Raw> = base_files().iter().map(|(p, b)| file(p, b)).collect();
		let err = trust_files_from_reader(archive(&raws).as_slice(), DEFAULT_MAX_FS_BYTES)
			.unwrap_err()
			.to_string();
		assert!(err.contains("MANIFEST not found"), "{err}");

		let mut raws = listed(&base_files());
		raws.insert(0, file(MANIFEST_PATH, &manifest_of(&base_files())));
		let err = trust_files_from_reader(archive(&raws).as_slice(), DEFAULT_MAX_FS_BYTES)
			.unwrap_err()
			.to_string();
		assert!(err.contains("more than once"), "{err}");
	}

	#[test]
	fn rule2_rejects_duplicate_path() {
		// The manifest lists the first mod.wasm; a second one must not replace it.
		let mut raws = listed(&base_files());
		let mut evil = COMPONENT_PREAMBLE.to_vec();
		evil.push(0xff);
		raws.insert(1, file("surrealism/mod.wasm", &evil));
		assert!(load_err(&raws).contains("more than once"));
	}

	#[test]
	fn rule2_rejects_duplicate_directory() {
		let mut raws = listed(&base_files());
		raws.insert(0, dir("surrealism/fs/"));
		raws.insert(0, dir("surrealism/fs"));
		assert!(load_err(&raws).contains("more than once"));
	}

	#[test]
	fn rule3_rejects_non_canonical_paths() {
		for bad in [
			"./surrealism/fs/a",
			"surrealism/./fs/a",
			"surrealism/fs/../a",
			"/surrealism/fs/a",
			"surrealism//fs/a",
			"surrealism\\fs\\a",
		] {
			let mut raws = listed(&base_files());
			raws.push(file(bad, b"x"));
			let err = load_err(&raws);
			assert!(
				err.contains(bad.trim_start_matches("./")) || err.contains("path"),
				"{bad}: {err}"
			);
		}
	}

	#[test]
	fn rule3_rejects_pax_path_override() {
		let mut files = base_files();
		files.push(("surrealism/fs/a.txt".into(), b"a".to_vec()));
		let mut raws = listed(&files);
		raws[3].pax = vec![("path", "surrealism/fs/b.txt".into())];
		assert!(load_err(&raws).contains("renamed"));
	}

	#[test]
	fn rule3_rejects_pax_path_override_to_listed_file() {
		// A header naming an unused path, renamed by PAX onto mod.wasm.
		let mut raws = listed(&base_files());
		raws[0].name = b"surrealism/fs/harmless".to_vec();
		raws[0].pax = vec![("path", "surrealism/mod.wasm".into())];
		assert!(load_err(&raws).contains("renamed"));
	}

	/// Every reader of an archive, each of which must refuse it with `needle`.
	fn refused_by_every_reader(raws: &[Raw], needle: &str) {
		let bytes = archive(raws);
		let loaded = load(&bytes).err().map(|e| e.to_string()).unwrap_or_default();
		let trusted = trust_files_from_reader(bytes.as_slice(), DEFAULT_MAX_FS_BYTES)
			.err()
			.map(|e| e.to_string())
			.unwrap_or_default();
		let verified = verify_archive(bytes.as_slice(), DEFAULT_MAX_FS_BYTES)
			.err()
			.map(|e| e.to_string())
			.unwrap_or_default();
		for (reader, err) in [("loader", loaded), ("trust", trusted), ("verify", verified)] {
			assert!(err.contains(needle), "{reader}: {err:?} lacks {needle:?}");
		}
	}

	#[test]
	fn rule3_accepts_a_pax_path_equal_to_the_name() {
		let mut raws = listed(&base_files());
		raws[0].pax = vec![("path", "surrealism/mod.wasm".into())];
		load(&archive(&raws)).unwrap();
	}

	/// A PAX `size` record changes how many bytes `tar` reads for the entry,
	/// so a reader that honours it and one that does not see other files.
	#[test]
	fn rule3_rejects_a_pax_size_record() {
		let mut raws = listed(&base_files());
		let manifest = raws.len() - 1;
		raws[manifest].pax = vec![("size", "1".into())];
		refused_by_every_reader(&raws, "PAX \"size\" record");
	}

	#[test]
	fn rule3_rejects_pax_records_other_than_path() {
		for key in ["mtime", "linkpath", "uname", "SCHILY.xattr.user.x"] {
			let mut raws = listed(&base_files());
			raws[0].pax = vec![("path", "surrealism/mod.wasm".into()), (key, "x".into())];
			refused_by_every_reader(&raws, &format!("PAX {key:?} record"));
		}
	}

	#[test]
	fn manifest_and_signature_must_be_files() {
		for name in [MANIFEST_PATH, SIGNATURE_PATH] {
			let mut raws = listed(&base_files());
			raws.retain(|r| r.name != name.as_bytes());
			raws.insert(0, dir(name));
			refused_by_every_reader(&raws, "is a directory; it must be a file");
		}
	}

	/// An entry is read up to its size and no further, and one past the bound
	/// is refused rather than cut short.
	#[test]
	fn a_bounded_read_refuses_rather_than_truncates() {
		let read = |data: &[u8], max: u64| {
			let bytes = archive(&[file("surrealism/fs/blob", data)]);
			let mut archive = open_archive(bytes.as_slice(), u64::MAX).unwrap();
			let mut entries = archive.entries().unwrap();
			let mut entry = entries.next().unwrap().unwrap();
			read_bounded(&mut entry, "surrealism/fs/blob", max)
		};
		assert_eq!(read(b"12345", 5).unwrap(), b"12345");
		let err = read(b"123456", 5).unwrap_err().to_string();
		assert!(err.contains("too large (6 bytes, max 5)"), "{err}");
	}

	#[test]
	fn attached_files_are_counted_against_the_bound() {
		let mut files = base_files();
		files.push(("surrealism/fs/a.bin".into(), vec![0; 600]));
		files.push(("surrealism/fs/b.bin".into(), vec![0; 600]));
		let bytes = archive(&listed(&files));
		let small = UnpackOptions {
			max_fs_bytes: 1000,
			..Default::default()
		};
		let Err(err) = SurrealismPackage::from_reader(bytes.as_slice(), &small) else {
			panic!("attached files over the bound were accepted");
		};
		let err = err.to_string();
		assert!(err.contains("attached filesystem exceeds"), "{err}");
		let roomy = UnpackOptions {
			max_fs_bytes: 1200,
			..Default::default()
		};
		SurrealismPackage::from_reader(bytes.as_slice(), &roomy).unwrap();
	}

	#[test]
	fn a_directory_is_held_by_a_file_listed_beneath_it() {
		let mut files = base_files();
		files.push(("surrealism/fs/a/b/c/model.bin".into(), b"w".to_vec()));
		let mut raws = listed(&files);
		for dir_name in ["surrealism", "surrealism/fs", "surrealism/fs/a", "surrealism/fs/a/b"] {
			raws.insert(0, dir(dir_name));
		}
		load(&archive(&raws)).unwrap();
		// A sibling whose name shares a prefix with a holding directory holds nothing.
		raws.insert(0, dir("surrealism/fs/a/b2"));
		assert!(load_err(&raws).contains("holds no file listed"));
	}

	#[test]
	fn rule4_rejects_links() {
		for kind in [EntryType::Symlink, EntryType::Link] {
			let mut raws = listed(&base_files());
			raws.push(Raw {
				name: b"surrealism/fs/link".to_vec(),
				kind,
				data: Vec::new(),
				pax: Vec::new(),
			});
			assert!(load_err(&raws).contains("only regular files and directories"), "{kind:?}");
		}
	}

	#[test]
	fn rule4_rejects_other_entry_types() {
		let mut raws = listed(&base_files());
		raws.push(Raw {
			name: b"surrealism/fs/fifo".to_vec(),
			kind: EntryType::Fifo,
			data: Vec::new(),
			pax: Vec::new(),
		});
		assert!(load_err(&raws).contains("only regular files and directories"));
	}

	/// The base files with the package file replaced by `package_files`.
	fn with_package_files(package_files: &[(&str, &[u8])]) -> Vec<(String, Vec<u8>)> {
		let mut files = base_files();
		files.retain(|(p, _)| p != PACKAGE_ENTRY);
		files.extend(package_files.iter().map(|(p, b)| (p.to_string(), b.to_vec())));
		files
	}

	fn named(name: &str) -> String {
		CONFIG.replace("name = \"t\"", &format!("name = \"{name}\""))
	}

	#[test]
	fn archive_with_package_toml_only() {
		let current = named("current");
		let files = with_package_files(&[(PACKAGE_ENTRY, current.as_bytes())]);
		let pkg = load(&archive(&listed(&files))).unwrap();
		assert_eq!(pkg.config.meta.name, "current");
	}

	#[test]
	fn archive_with_surrealism_toml_only() {
		let legacy = named("legacy");
		let files = with_package_files(&[(LEGACY_PACKAGE_ENTRY, legacy.as_bytes())]);
		let pkg = load(&archive(&listed(&files))).unwrap();
		assert_eq!(pkg.config.meta.name, "legacy");
	}

	#[test]
	fn archive_with_both_prefers_package_toml() {
		let current = named("current");
		// Unparseable, so the test fails if the loader reads it.
		let legacy = b"not toml [";
		for order in [
			[(LEGACY_PACKAGE_ENTRY, &legacy[..]), (PACKAGE_ENTRY, current.as_bytes())],
			[(PACKAGE_ENTRY, current.as_bytes()), (LEGACY_PACKAGE_ENTRY, &legacy[..])],
		] {
			let files = with_package_files(&order);
			let pkg = load(&archive(&listed(&files))).unwrap();
			assert_eq!(pkg.config.meta.name, "current");
			// Both are covered: the ignored file is still part of what was signed.
			let manifest = pkg.manifest.as_ref().unwrap();
			assert!(manifest.get(LEGACY_PACKAGE_ENTRY).is_some());
			assert!(manifest.get(PACKAGE_ENTRY).is_some());
		}
	}

	#[test]
	fn archive_without_package_file_is_rejected() {
		let files = with_package_files(&[]);
		let err = load_err(&listed(&files));
		assert!(err.contains("package.toml not found"), "{err}");
	}

	#[test]
	fn pack_writes_package_toml() {
		let dir = tempfile::tempdir().unwrap();
		let out = dir.path().join("test.surli");
		let pkg = SurrealismPackage {
			config: SurrealismConfig::parse(&named("packed")).unwrap(),
			wasm: COMPONENT_PREAMBLE.to_vec(),
			exports: ExportsManifest::empty(),
			fs: None,
			logo: None,
			manifest: None,
			signature: None,
		};
		pkg.pack(out.clone(), None).unwrap();

		let mut archive = Archive::new(Decoder::new(File::open(&out).unwrap()).unwrap());
		let paths: Vec<String> = archive
			.entries()
			.unwrap()
			.map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned())
			.collect();
		assert!(paths.iter().any(|p| p == PACKAGE_ENTRY), "{paths:?}");
		assert!(!paths.iter().any(|p| p == LEGACY_PACKAGE_ENTRY), "{paths:?}");

		let unpacked = SurrealismPackage::from_file(out).unwrap();
		assert_eq!(unpacked.config.meta.name, "packed");
	}
}
