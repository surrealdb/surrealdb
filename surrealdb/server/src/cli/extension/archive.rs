//! Client-side inspection of a `.surli` archive, in two steps.
//!
//! 1. [`preview_archive`] reads, without unpacking anything to disk, what the signature check and
//!    the prompt need: `MANIFEST` and `SIGNATURE` (through the loader's
//!    [`trust_files_from_reader`]), and `package.toml` and `exports.toml`, each held to the hash
//!    `MANIFEST` lists for it. `DIGEST` is `sha256(MANIFEST)`.
//! 2. Only once the signature is verified, [`unpack_archive`] runs the loader the server runs,
//!    [`SurrealismPackage::from_reader`], so an archive the client accepts is one the server's
//!    loader accepts. That loader refuses the archive unless:
//!    1. every file except `SIGNATURE` is in the manifest, and every manifest line is in the
//!       archive with a matching hash;
//!    2. no path appears twice;
//!    3. paths are canonical, and no tar extension header renames an entry;
//!    4. only regular files and directories.

use std::io::{Cursor, Read as _};

use anyhow::{Context as _, Result, bail, ensure};
use bytes::Bytes;
use semver::Version;
use surrealism_runtime::config::{PackageSigner, SurrealismConfig};
use surrealism_runtime::exports::ExportsManifest;
use surrealism_runtime::package::{
	Digest, Manifest, SurrealismPackage, UnpackOptions, trust_files_from_reader,
};
use surrealism_runtime::package_config::PackageConfig;
use tracing::instrument;

use super::capabilities::Capabilities;

const PACKAGE_PATH: &str = "surrealism/package.toml";
const LEGACY_PATH: &str = "surrealism/surrealism.toml";
const EXPORTS_PATH: &str = "surrealism/exports.toml";

/// The largest metadata file read in the preview.
const MAX_METADATA_BYTES: u64 = 1024 * 1024;

/// What the client learns from an archive before it shows the prompt.
#[derive(Clone, Debug)]
pub struct ArchiveFacts {
	/// `sha256(surrealism/MANIFEST)`: the `DIGEST`.
	pub digest: Digest,
	/// The raw `surrealism/SIGNATURE`, when the archive carries one.
	pub signature: Option<Vec<u8>>,
	pub organisation: String,
	pub name: String,
	pub version: Version,
	/// The optional `repository` the author declares in `package.toml`.
	pub repository: Option<String>,
	/// `[signer]` from `package.toml`: the one signer this package's archives
	/// may carry.
	pub declared_signer: Option<PackageSigner>,
	/// `[capabilities]` from `package.toml`, plus `write` when an export writes.
	pub requests: Capabilities,
	pub exports: Vec<Export>,
}

/// One exported function, as `exports.toml` declares it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Export {
	/// `None` for the package's default export.
	pub name: Option<String>,
	/// `(name, SurrealQL type)` per argument.
	pub args: Vec<(String, String)>,
	/// A SurrealQL type.
	pub returns: String,
	pub writes: bool,
	pub comment: Option<String>,
}

/// The most bytes an archive may unpack to, all entries together.
///
/// It bounds what a hostile archive (a decompression bomb) can make the
/// client decompress and write to a temporary directory. It sits well above
/// what the server accepts: the server takes at most 256 MiB of compressed
/// archive and, by default, 100 MiB of attached files
/// (`SURREAL_SURREALISM_MAX_FS_BYTES`), and an operator who raises that
/// limit stays far below 4 GiB in practice. The client applies no separate
/// limit on attached files, because it cannot know the server's setting.
pub const MAX_UNPACKED_BYTES: u64 = 4 * 1024 * 1024 * 1024;

/// [`preview_bytes`] on a blocking thread: decompressing a large archive must
/// not stall the async runtime.
#[instrument(level = "debug", skip_all, fields(len = bytes.len()))]
pub async fn preview_archive(bytes: Bytes) -> Result<ArchiveFacts> {
	tokio::task::spawn_blocking(move || preview_bytes(&bytes))
		.await
		.context("the archive inspection did not finish")?
}

/// Reads what the signature check and the prompt need, writing nothing to
/// disk: `MANIFEST`, `SIGNATURE`, and `package.toml` (or `surrealism.toml`)
/// and `exports.toml` checked against their `MANIFEST` hashes.
pub fn preview_bytes(bytes: &[u8]) -> Result<ArchiveFacts> {
	// MANIFEST and SIGNATURE, with the loader's path rules and its bound on
	// how much the archive may decompress to.
	let trust = trust_files_from_reader(Cursor::new(bytes), MAX_UNPACKED_BYTES)
		.map_err(anyhow::Error::from)?;
	let manifest = &trust.manifest;

	let decoder = zstd::stream::read::Decoder::new(Cursor::new(bytes))
		.context("the archive is not zstd-compressed")?;
	let mut archive = tar::Archive::new(decoder);
	let (mut package, mut legacy, mut exports) = (None, None, None);
	for entry in archive.entries().context("failed to read the archive")? {
		let mut entry = entry.context("failed to read an archive entry")?;
		let path = String::from_utf8_lossy(&entry.path_bytes()).into_owned();
		let slot = match path.as_str() {
			PACKAGE_PATH => &mut package,
			LEGACY_PATH => &mut legacy,
			EXPORTS_PATH => &mut exports,
			_ => continue,
		};
		let mut text = Vec::new();
		(&mut entry)
			.take(MAX_METADATA_BYTES + 1)
			.read_to_end(&mut text)
			.with_context(|| format!("failed to read {path}"))?;
		ensure!(
			text.len() as u64 <= MAX_METADATA_BYTES,
			"{path} is over {MAX_METADATA_BYTES} bytes"
		);
		*slot = Some(listed(manifest, &path, text)?);
	}

	// `package.toml` wins over `surrealism.toml`, as in the loader.
	let config: SurrealismConfig = PackageConfig::parse(
		&package.or(legacy).context("the archive has neither package.toml nor surrealism.toml")?,
	)
	.map_err(anyhow::Error::from)
	.context("the archive's package.toml is invalid")?
	.into_config();
	let exports = ExportsManifest::parse(&exports.context("the archive has no exports.toml")?)
		.map_err(anyhow::Error::from)
		.context("the archive's exports.toml is invalid")?;

	let exports: Vec<Export> = exports
		.functions
		.iter()
		.map(|f| Export {
			name: f.name.clone(),
			args: f.args.iter().map(|(name, kind)| (name.clone(), kind.to_string())).collect(),
			returns: f.returns.to_string(),
			writes: f.writeable,
			comment: f.comment.clone(),
		})
		.collect();
	let any_writes = exports.iter().any(|e| e.writes);
	let meta = &config.meta;
	Ok(ArchiveFacts {
		digest: manifest.digest(),
		signature: trust.signature,
		organisation: meta.organisation.clone(),
		name: meta.name.clone(),
		version: meta.version.clone(),
		repository: meta.repository.clone(),
		declared_signer: config.signer.clone(),
		requests: Capabilities::from_request(&config.capabilities, any_writes),
		exports,
	})
}

/// `text` as UTF-8, once it matches the hash `MANIFEST` lists for `path`.
fn listed(manifest: &Manifest, path: &str, text: Vec<u8>) -> Result<String> {
	match manifest.get(path) {
		Some(hash) if hash == Digest::of(&text) => {}
		Some(_) => bail!("{path} does not match surrealism/MANIFEST"),
		None => bail!("{path} is not listed in surrealism/MANIFEST"),
	}
	String::from_utf8(text).with_context(|| format!("{path} is not UTF-8"))
}

/// [`unpack_bytes`] on a blocking thread.
#[instrument(level = "debug", skip_all, fields(len = bytes.len()))]
pub async fn unpack_archive(bytes: Bytes, digest: Digest) -> Result<()> {
	tokio::task::spawn_blocking(move || unpack_bytes(&bytes, &digest))
		.await
		.context("the archive check did not finish")?
}

/// Runs the server's loader over the archive: every file hashed against
/// `MANIFEST`, attached files unpacked to a temporary directory that is
/// removed again. Call it only after the signature is verified. `digest` is
/// the preview's, which the loader must reproduce.
pub fn unpack_bytes(bytes: &[u8], digest: &Digest) -> Result<()> {
	// Count first, without writing anything, so a bomb is refused before the
	// loader extracts attached files to disk.
	let mut decoder = zstd::stream::read::Decoder::new(Cursor::new(bytes))
		.context("the archive is not zstd-compressed")?;
	let unpacked =
		std::io::copy(&mut (&mut decoder).take(MAX_UNPACKED_BYTES + 1), &mut std::io::sink())
			.context("failed to decompress the archive")?;
	ensure!(
		unpacked <= MAX_UNPACKED_BYTES,
		"the archive unpacks to more than {MAX_UNPACKED_BYTES} bytes"
	);
	let opts = UnpackOptions {
		max_fs_bytes: MAX_UNPACKED_BYTES,
		..UnpackOptions::default()
	};
	let package =
		SurrealismPackage::from_reader(Cursor::new(bytes), &opts).map_err(anyhow::Error::from)?;
	ensure!(
		package.digest().as_ref() == Some(digest),
		"the archive's MANIFEST changed between the preview and the check"
	);
	Ok(())
}

/// Both steps, for tests and for callers that verify nothing in between.
#[cfg(test)]
pub fn inspect_bytes(bytes: &[u8]) -> Result<ArchiveFacts> {
	let facts = preview_bytes(bytes)?;
	unpack_bytes(bytes, &facts.digest)?;
	Ok(facts)
}

/// Builds archives for tests, in the layout and manifest format the loader
/// reads.
#[cfg(test)]
pub mod fixture {
	use surrealism_runtime::package::{MANIFEST_PATH, Manifest, SIGNATURE_PATH};

	use super::*;

	/// The 8-byte preamble of a WASM component; enough for inspection.
	pub const COMPONENT_PREAMBLE: [u8; 8] = [0x00, 0x61, 0x73, 0x6d, 0x0d, 0x00, 0x01, 0x00];

	pub const PACKAGE_TOML: &str = r#"
[package]
organisation = "acme"
name = "color"
version = "1.0.0"
repository = "https://github.com/acme/color"

[capabilities]
allow_net = ["api.color.io:443"]
allow_functions = ["http::get"]
"#;

	/// `exports.toml` for two functions; `recolor` writes.
	pub fn exports_toml() -> String {
		use surrealdb_types::Kind;
		use surrealism_runtime::exports::{ExportsManifest, FunctionExport};
		let function = |name: &str, writeable: bool| FunctionExport {
			name: Some(name.into()),
			args: vec![],
			returns: Kind::String,
			args_text: None,
			returns_text: None,
			writeable,
			declares_transaction: false,
			comment: None,
		};
		ExportsManifest {
			functions: vec![function("blend", false), function("recolor", true)],
		}
		.to_toml()
		.expect("serialise the fixture exports")
	}

	/// The manifest for `files`.
	pub fn manifest(files: &[(&str, &[u8])]) -> Manifest {
		Manifest::from_entries(
			files.iter().map(|(path, data)| (path.to_string(), Digest::of(data))),
		)
		.expect("fixture paths are canonical")
	}

	/// A zstd tar holding `files`, their manifest, and `signature` if given.
	pub fn pack(files: &[(&str, &[u8])], signature: Option<&[u8]>) -> Vec<u8> {
		let manifest = manifest(files);
		let mut entries: Vec<(&str, &[u8])> = files.to_vec();
		entries.push((MANIFEST_PATH, manifest.as_bytes()));
		if let Some(sig) = signature {
			entries.push((SIGNATURE_PATH, sig));
		}
		pack_raw(&entries)
	}

	/// A zstd tar holding exactly `entries`, with no manifest added.
	pub fn pack_raw(entries: &[(&str, &[u8])]) -> Vec<u8> {
		let mut tar = tar::Builder::new(Vec::new());
		for (path, data) in entries {
			let mut header = tar::Header::new_ustar();
			header.set_size(data.len() as u64);
			header.set_mode(0o644);
			header.set_entry_type(tar::EntryType::Regular);
			header.set_path(path).expect("fixture paths fit a ustar header");
			header.set_cksum();
			tar.append(&header, *data).expect("append to an in-memory tar");
		}
		let tar = tar.into_inner().expect("finish the in-memory tar");
		zstd::stream::encode_all(Cursor::new(tar), 0).expect("compress the fixture")
	}

	/// The standard fixture package's files.
	pub fn files() -> Vec<(&'static str, &'static [u8])> {
		static EXPORTS: std::sync::LazyLock<String> = std::sync::LazyLock::new(exports_toml);
		vec![
			("surrealism/mod.wasm", &COMPONENT_PREAMBLE[..]),
			("surrealism/package.toml", PACKAGE_TOML.as_bytes()),
			("surrealism/exports.toml", EXPORTS.as_bytes()),
		]
	}

	/// The standard fixture package, unsigned.
	pub fn archive() -> Vec<u8> {
		pack(&files(), None)
	}
}

#[cfg(test)]
mod tests {
	use surrealism_runtime::package::MANIFEST_PATH;

	use super::fixture::*;
	use super::*;

	#[test]
	fn inspects_the_fixture() {
		let facts = inspect_bytes(&archive()).expect("a valid archive");
		assert_eq!(facts.digest, manifest(&files()).digest());
		assert_eq!(facts.organisation, "acme");
		assert_eq!(facts.name, "color");
		assert_eq!(facts.version, Version::new(1, 0, 0));
		assert_eq!(facts.repository.as_deref(), Some("https://github.com/acme/color"));
		assert_eq!(facts.requests.net, vec!["api.color.io:443".to_string()]);
		assert_eq!(facts.requests.functions, vec!["http::get".to_string()]);
		assert!(facts.requests.write, "an export that writes requests `write`");
		let names: Vec<_> = facts.exports.iter().map(|e| (e.name.as_deref(), e.writes)).collect();
		assert_eq!(names, vec![(Some("blend"), false), (Some("recolor"), true)]);
		assert!(facts.signature.is_none());
	}

	#[test]
	fn signature_does_not_change_the_digest() {
		let unsigned = inspect_bytes(&archive()).unwrap();
		let signed = inspect_bytes(&pack(&files(), Some(b"{}"))).unwrap();
		assert_eq!(unsigned.digest, signed.digest);
		assert_eq!(signed.signature.as_deref(), Some(&b"{}"[..]));
	}

	#[test]
	fn attached_files_over_the_server_default_are_not_refused() {
		// 101 MiB of zeros compresses to almost nothing.
		let big = vec![0u8; 101 * 1024 * 1024];
		let mut files = files();
		files.push(("surrealism/fs/model.bin", &big[..]));
		let facts = inspect_bytes(&pack(&files, None)).expect("the server may allow this size");
		assert_eq!(facts.digest, manifest(&files).digest());
	}

	#[tokio::test]
	async fn inspects_off_the_async_runtime() {
		let facts = preview_archive(Bytes::from(archive())).await.expect("a valid archive");
		unpack_archive(Bytes::from(archive()), facts.digest).await.expect("unpacks");
		assert_eq!(facts.name, "color");
	}

	#[test]
	fn the_preview_holds_metadata_to_the_manifest_and_writes_nothing() {
		// A tampered package.toml is refused by the preview, before any unpack.
		let mut entries = files();
		let manifest = manifest(&entries);
		let swapped = PACKAGE_TOML.replace("api.color.io", "evil.example");
		entries[1] = ("surrealism/package.toml", swapped.as_bytes());
		entries.push((surrealism_runtime::package::MANIFEST_PATH, manifest.as_bytes()));
		let err = preview_bytes(&pack_raw(&entries)).unwrap_err();
		assert!(format!("{err:#}").contains("does not match surrealism/MANIFEST"), "{err:#}");

		// A large attached file is only read, never written, by the preview.
		let big = vec![0u8; 8 * 1024 * 1024];
		let mut files = files();
		files.push(("surrealism/fs/model.bin", &big[..]));
		let facts = preview_bytes(&pack(&files, None)).expect("previews");
		assert_eq!(facts.name, "color");
	}

	#[test]
	fn the_loader_rules_apply() {
		let mut entries = files();
		let manifest = manifest(&entries);
		entries[0] = ("surrealism/mod.wasm", b"tampered");
		entries.push((MANIFEST_PATH, manifest.as_bytes()));
		assert!(inspect_bytes(&pack_raw(&entries)).is_err(), "a tampered file is refused");

		assert!(inspect_bytes(&pack_raw(&files())).is_err(), "an archive needs a MANIFEST");
	}
}
