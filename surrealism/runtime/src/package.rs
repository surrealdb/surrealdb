//! `.surli` archive format: tar + zstd containing `mod.wasm`, config, exports,
//! and optional `surrealism/fs/` filesystem. Only WASM components are accepted.

use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use surrealism_types::err::{PrefixErr, SurrealismError, SurrealismResult};
use tar::Archive;
use tempfile::TempDir;
use zstd::stream::read::Decoder;

use crate::config::SurrealismConfig;
use crate::exports::ExportsManifest;

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

/// Read only `surrealism/exports.toml` from a `.surli` (zstd tar) archive without loading WASM,
/// config, or attached filesystem — avoids JIT compilation when only export metadata is needed.
pub fn exports_manifest_from_reader<R: Read>(reader: R) -> SurrealismResult<ExportsManifest> {
	let zstd_decoder =
		Decoder::new(BufReader::new(reader)).prefix_err(|| "Failed to create zstd decoder")?;
	let mut archive = Archive::new(zstd_decoder);

	for entry in archive.entries().prefix_err(|| "Failed to read archive entries")? {
		let mut entry = entry.prefix_err(|| "Failed to read archive entry")?;
		let entry_path = entry.path().prefix_err(|| "Failed to get entry path")?;
		let entry_str = entry_path.to_string_lossy().to_string();

		if entry_str == "surrealism/exports.toml" {
			let mut buffer = String::new();
			entry
				.read_to_string(&mut buffer)
				.prefix_err(|| "Failed to read exports file from archive")?;
			return ExportsManifest::parse(&buffer).prefix_err(|| "Failed to parse exports.toml");
		}

		// Discard this member's payload without buffering large entries (e.g. mod.wasm) in memory.
		std::io::copy(&mut entry, &mut std::io::sink())
			.prefix_err(|| format!("Failed to skip archive entry: {entry_str}"))?;
	}

	Err(SurrealismError::Other(anyhow::anyhow!("surrealism/exports.toml not found in archive")))
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

	pub fn from_reader<R: Read>(reader: R, opts: &UnpackOptions<'_>) -> SurrealismResult<Self> {
		let zstd_decoder =
			Decoder::new(BufReader::new(reader)).prefix_err(|| "Failed to create zstd decoder")?;
		let mut archive = Archive::new(zstd_decoder);

		let mut wasm: Option<Vec<u8>> = None;
		let mut config: Option<SurrealismConfig> = None;
		let mut exports: Option<ExportsManifest> = None;
		let mut fs_dir: Option<TempDir> = None;
		let mut logo: Option<Vec<u8>> = None;
		let mut fs_bytes_total: u64 = 0;

		for entry in archive.entries().prefix_err(|| "Failed to read archive entries")? {
			let mut entry = entry.prefix_err(|| "Failed to read archive entry")?;
			let entry_path = entry.path().prefix_err(|| "Failed to get entry path")?;
			let entry_str = entry_path.to_string_lossy().to_string();

			if entry_str == "surrealism/mod.wasm" {
				let mut buffer = Vec::new();
				entry
					.read_to_end(&mut buffer)
					.prefix_err(|| "Failed to read WASM file from archive")?;
				verify_component(&buffer)?;
				wasm = Some(buffer);
			} else if entry_str == "surrealism/surrealism.toml" {
				let mut buffer = String::new();
				entry
					.read_to_string(&mut buffer)
					.prefix_err(|| "Failed to read config file from archive")?;
				config = Some(
					SurrealismConfig::parse(&buffer)
						.prefix_err(|| "Failed to parse surrealism.toml")?,
				);
			} else if entry_str == "surrealism/exports.toml" {
				let mut buffer = String::new();
				entry
					.read_to_string(&mut buffer)
					.prefix_err(|| "Failed to read exports file from archive")?;
				exports = Some(
					ExportsManifest::parse(&buffer)
						.prefix_err(|| "Failed to parse exports.toml")?,
				);
			} else if entry_str == "surrealism/logo.png" {
				let declared = entry
					.header()
					.size()
					.prefix_err(|| "Failed to read surrealism/logo.png tar header")?;
				if declared > MAX_LOGO_BYTES as u64 {
					return Err(SurrealismError::Other(anyhow::anyhow!(
						"surrealism/logo.png is too large ({} bytes, max {} KiB)",
						declared,
						MAX_LOGO_BYTES / 1024
					)));
				}
				let mut buffer = Vec::with_capacity(declared as usize);
				entry
					.read_to_end(&mut buffer)
					.prefix_err(|| "Failed to read logo.png from archive")?;
				logo = Some(buffer);
			} else if entry_str.starts_with(FS_PREFIX) {
				let entry_size = entry
					.header()
					.size()
					.prefix_err(|| format!("Failed to read tar header for: {entry_str}"))?;
				fs_bytes_total += entry_size;
				if fs_bytes_total > opts.max_fs_bytes {
					return Err(SurrealismError::Other(anyhow::anyhow!(
						"attached filesystem exceeds {} MiB limit ({} bytes)",
						opts.max_fs_bytes / (1024 * 1024),
						opts.max_fs_bytes,
					)));
				}

				if fs_dir.is_none() {
					fs_dir = Some(create_temp_dir(opts)?);
				}
				let Some(dir) = fs_dir.as_ref() else {
					unreachable!("fs_dir is always Some after the block above");
				};

				let unpacked = entry
					.unpack_in(dir.path())
					.prefix_err(|| format!("Failed to unpack fs entry: {}", entry_str))?;
				if !unpacked {
					tracing::warn!(
						entry = %entry_str,
						"Skipped archive fs entry for safety"
					);
				}
			}
		}

		let wasm = wasm.ok_or_else(|| anyhow::anyhow!("mod.wasm not found in archive"))?;
		let config =
			config.ok_or_else(|| anyhow::anyhow!("surrealism.toml not found in archive"))?;
		let exports =
			exports.ok_or_else(|| anyhow::anyhow!("exports.toml not found in archive"))?;

		if let Some(ref logo_bytes) = logo {
			verify_logo(logo_bytes)?;
		}

		let fs = fs_dir.map(|dir| {
			let root = dir.path().join(FS_PREFIX.trim_end_matches('/'));
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
		})
	}

	pub fn pack(&self, output: PathBuf, fs_dir: Option<&Path>) -> SurrealismResult<()> {
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

		let file = File::create(&output).prefix_err(|| "Failed to create output file")?;
		let encoder =
			zstd::stream::Encoder::new(file, 0).prefix_err(|| "Failed to create zstd encoder")?;
		let mut archive = tar::Builder::new(encoder);

		let mut wasm_reader = std::io::Cursor::new(&self.wasm);
		let mut wasm_header = tar::Header::new_gnu();
		wasm_header.set_size(self.wasm.len() as u64);
		wasm_header.set_mode(0o644);
		archive
			.append_data(&mut wasm_header, "surrealism/mod.wasm", &mut wasm_reader)
			.prefix_err(|| "Failed to add mod.wasm to archive")?;

		let config_str = self.config.to_toml().prefix_err(|| "Failed to serialize config")?;
		let config_bytes = config_str.as_bytes();
		let mut config_reader = std::io::Cursor::new(config_bytes);
		let mut config_header = tar::Header::new_gnu();
		config_header.set_size(config_bytes.len() as u64);
		config_header.set_mode(0o644);
		archive
			.append_data(&mut config_header, "surrealism/surrealism.toml", &mut config_reader)
			.prefix_err(|| "Failed to add surrealism.toml to archive")?;

		let exports_str =
			self.exports.to_toml().prefix_err(|| "Failed to serialize exports manifest")?;
		let exports_bytes = exports_str.as_bytes();
		let mut exports_reader = std::io::Cursor::new(exports_bytes);
		let mut exports_header = tar::Header::new_gnu();
		exports_header.set_size(exports_bytes.len() as u64);
		exports_header.set_mode(0o644);
		archive
			.append_data(&mut exports_header, "surrealism/exports.toml", &mut exports_reader)
			.prefix_err(|| "Failed to add exports.toml to archive")?;

		if let Some(ref logo_bytes) = self.logo {
			verify_logo(logo_bytes)?;
			let mut logo_reader = std::io::Cursor::new(logo_bytes);
			let mut logo_header = tar::Header::new_gnu();
			logo_header.set_size(logo_bytes.len() as u64);
			logo_header.set_mode(0o644);
			archive
				.append_data(&mut logo_header, "surrealism/logo.png", &mut logo_reader)
				.prefix_err(|| "Failed to add logo.png to archive")?;
		}

		if let Some(dir) = fs_dir {
			archive
				.append_dir_all("surrealism/fs", dir)
				.prefix_err(|| "Failed to add filesystem directory to archive")?;
		}

		let encoder = archive.into_inner().prefix_err(|| "Failed to get encoder from archive")?;
		encoder.finish().prefix_err(|| "Failed to finish zstd encoder")?;

		Ok(())
	}
}
