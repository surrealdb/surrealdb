//! Reading `surrealism/MANIFEST` from a `.surli` archive and writing
//! `surrealism/SIGNATURE` back into it.
//!
//! The archive is copied at the tar-record level: every header block and data
//! block of every other entry is written out byte for byte, including GNU
//! long-name and PAX extension records. Only the zstd framing is re-encoded. The
//! manifest therefore stays byte-identical, and so does `DIGEST`.

use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use surrealism_runtime::config::{LEGACY_PACKAGE_FILE, PACKAGE_FILE};
pub use surrealism_runtime::manifest::{MANIFEST_PATH, Manifest, SIGNATURE_PATH};
use surrealism_runtime::package_config::PackageConfig;
use tar::{Archive, EntryType, Header, PaxExtensions};
use zstd::stream::read::Decoder;

/// One tar record as it appears in the archive: a header block, then its data.
struct Record {
	header: Header,
	data: Vec<u8>,
}

/// A logical entry: its extension records (GNU long name, PAX) followed by the
/// record that carries the file itself.
struct Entry {
	records: Vec<Record>,
	path: Vec<u8>,
}

impl Entry {
	fn file(&self) -> &Record {
		// `read_entries` never builds an entry without its final record.
		&self.records[self.records.len() - 1]
	}
}

/// Reads every logical entry of a `.surli` archive, keeping each record's raw
/// header so that it can be written back unchanged.
fn read_entries(path: &Path) -> Result<Vec<Entry>> {
	let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
	let decoder = Decoder::new(BufReader::new(file)).context("Failed to create zstd decoder")?;
	let mut archive = Archive::new(decoder);
	let mut entries = Vec::new();
	let mut pending: Vec<Record> = Vec::new();
	for raw in archive.entries().context("Failed to read archive entries")?.raw(true) {
		let mut raw = raw.context("Failed to read archive entry")?;
		let header = raw.header().clone();
		let mut data = Vec::new();
		raw.read_to_end(&mut data).context("Failed to read archive entry data")?;
		let record = Record {
			header,
			data,
		};
		if matches!(
			record.header.entry_type(),
			EntryType::GNULongName | EntryType::GNULongLink | EntryType::XHeader
		) {
			pending.push(record);
		} else {
			let path = effective_path(&pending, &record.header)?;
			pending.push(record);
			entries.push(Entry {
				records: std::mem::take(&mut pending),
				path,
			});
		}
	}
	ensure!(pending.is_empty(), "Archive ends with a dangling extension header");
	Ok(entries)
}

/// The path an entry has once its extension records apply: a PAX `path`
/// overrides a GNU long name, which overrides the header's own name.
fn effective_path(extensions: &[Record], header: &Header) -> Result<Vec<u8>> {
	let mut path = header.path_bytes().into_owned();
	for ext in extensions {
		let entry_type = ext.header.entry_type();
		if entry_type == EntryType::GNULongName {
			let name = ext.data.split(|b| *b == 0).next().unwrap_or_default();
			path = name.to_vec();
		} else if entry_type == EntryType::XHeader {
			for pax in PaxExtensions::new(&ext.data) {
				let pax = pax.context("Malformed PAX extension header")?;
				if pax.key_bytes() == b"path" {
					path = pax.value_bytes().to_vec();
				}
			}
		}
	}
	Ok(path)
}

/// Checks that the files of the archive at `path` match `manifest`, by the
/// rules the server's loader applies (`surrealism_runtime::package::verify_archive`).
/// A signature over a manifest the files do not match would vouch for code
/// the archive does not hold.
pub fn check_files(path: &Path, manifest: &Manifest) -> Result<()> {
	let file = File::open(path).with_context(|| format!("Failed to open {}", path.display()))?;
	// The signer reads its own archive, so no server bound on attached files
	// applies.
	let files = surrealism_runtime::package::verify_archive(BufReader::new(file), u64::MAX)
		.map_err(|e| anyhow::anyhow!("Refusing to sign {}: {e}", path.display()))?;
	ensure!(
		files.manifest.digest() == manifest.digest(),
		"Refusing to sign {}: its {MANIFEST_PATH} changed while it was read",
		path.display()
	);
	Ok(())
}

/// Reads the package file inside an archive: `surrealism/package.toml`, or
/// `surrealism/surrealism.toml` when there is none, as the server's loader
/// does.
pub fn read_package(path: &Path) -> Result<PackageConfig> {
	let entries = read_entries(path)?;
	for name in [PACKAGE_FILE, LEGACY_PACKAGE_FILE] {
		let entry_path = format!("surrealism/{name}");
		if let Some(entry) = entries.iter().find(|e| e.path == entry_path.as_bytes()) {
			let text = std::str::from_utf8(&entry.file().data)
				.map_err(|_| anyhow::anyhow!("{entry_path} is not UTF-8"))?;
			return PackageConfig::parse(text)
				.map_err(|e| anyhow::anyhow!("{entry_path} is not a valid package file: {e}"));
		}
	}
	bail!("The archive has no surrealism/{PACKAGE_FILE}")
}

/// Reads and parses `surrealism/MANIFEST` from an archive.
pub fn read_manifest(path: &Path) -> Result<Manifest> {
	let entries = read_entries(path)?;
	manifest_of(&entries)
}

fn manifest_of(entries: &[Entry]) -> Result<Manifest> {
	let mut found = entries.iter().filter(|e| e.path == MANIFEST_PATH.as_bytes());
	let Some(entry) = found.next() else {
		bail!(
			"The archive has no {MANIFEST_PATH}. Rebuild it with `surreal ext build` from \
			 SurrealDB 4.0 or later"
		);
	};
	ensure!(found.next().is_none(), "The archive holds more than one {MANIFEST_PATH}");
	let file = entry.file();
	ensure!(
		file.header.entry_type() == EntryType::Regular,
		"{MANIFEST_PATH} is not a regular file"
	);
	Manifest::parse(file.data.clone())
		.map_err(|e| anyhow::anyhow!("{MANIFEST_PATH} is malformed: {e}"))
}

/// Writes `signature` as `surrealism/SIGNATURE` into the archive at `input`,
/// and writes the result to `output` (which may be `input`).
///
/// Every existing `SIGNATURE` entry is dropped; the new one is appended after
/// all other entries. Every other record is copied unchanged. The write goes to
/// a temporary file next to `output`, which then replaces `output`, so a failure
/// leaves `output` as it was.
///
/// `signed` is the manifest the signature was made over. The archive is read
/// again here, so it is refused when its `MANIFEST` is no longer that one: a
/// signature is never written beside a manifest it does not cover.
pub fn write_signature(
	input: &Path,
	output: &Path,
	signed: &Manifest,
	signature: &[u8],
) -> Result<()> {
	let entries = read_entries(input)?;
	// Refuse an archive that cannot be signed before writing anything.
	ensure!(
		manifest_of(&entries)?.as_bytes() == signed.as_bytes(),
		"Refusing to sign {}: its {MANIFEST_PATH} changed while it was being signed",
		input.display()
	);

	let dir = match output.parent() {
		Some(p) if !p.as_os_str().is_empty() => p,
		_ => Path::new("."),
	};
	let tmp = tempfile::Builder::new()
		.prefix(".surli-sign-")
		.tempfile_in(dir)
		.with_context(|| format!("Failed to create a temporary file in {}", dir.display()))?;
	let encoder =
		zstd::stream::Encoder::new(tmp.as_file(), 0).context("Failed to create zstd encoder")?;
	let mut builder = tar::Builder::new(encoder);
	for entry in entries.iter().filter(|e| e.path != SIGNATURE_PATH.as_bytes()) {
		for record in &entry.records {
			builder
				.append(&record.header, record.data.as_slice())
				.context("Failed to copy archive entry")?;
		}
	}
	let mut header = Header::new_gnu();
	header.set_entry_type(EntryType::Regular);
	header.set_size(signature.len() as u64);
	header.set_mode(0o644);
	builder
		.append_data(&mut header, SIGNATURE_PATH, signature)
		.context("Failed to add SIGNATURE to archive")?;
	let mut encoder = builder.into_inner().context("Failed to finish tar stream")?;
	encoder.flush().context("Failed to flush archive")?;
	encoder.finish().context("Failed to finish zstd stream")?;
	// The temporary file is created owner-only. The signed archive keeps the
	// permissions of the file it replaces, or of the input when `output` is new.
	let permissions = std::fs::metadata(output)
		.or_else(|_| std::fs::metadata(input))
		.with_context(|| format!("Failed to read the permissions of {}", input.display()))?
		.permissions();
	tmp.as_file()
		.set_permissions(permissions)
		.with_context(|| format!("Failed to set the permissions of {}", output.display()))?;
	tmp.as_file().sync_all().context("Failed to sync archive")?;
	tmp.persist(output).with_context(|| format!("Failed to write {}", output.display()))?;
	Ok(())
}

#[cfg(test)]
pub(super) mod tests {
	use std::path::PathBuf;

	use surrealism_runtime::manifest::Digest;

	use super::*;

	/// Builds a `.surli` archive holding `files` plus their `MANIFEST`, and
	/// returns its path inside `dir`.
	pub fn archive_with_manifest(dir: &Path, files: &[(&str, &[u8])]) -> PathBuf {
		let manifest =
			Manifest::from_entries(files.iter().map(|(p, b)| (p.to_string(), Digest::of(b))))
				.unwrap();
		let mut all: Vec<(&str, &[u8])> = files.to_vec();
		all.push((MANIFEST_PATH, manifest.as_bytes()));
		write_archive(&dir.join("pkg.surli"), &all)
	}

	pub fn write_archive(path: &Path, files: &[(&str, &[u8])]) -> PathBuf {
		let file = File::create(path).unwrap();
		let encoder = zstd::stream::Encoder::new(file, 0).unwrap();
		let mut builder = tar::Builder::new(encoder);
		for (name, data) in files {
			let mut header = Header::new_gnu();
			header.set_size(data.len() as u64);
			header.set_mode(0o644);
			builder.append_data(&mut header, name, *data).unwrap();
		}
		builder.into_inner().unwrap().finish().unwrap();
		path.to_path_buf()
	}

	/// Every logical entry as (path, raw header bytes, data).
	pub fn dump(path: &Path) -> Vec<(String, Vec<u8>, Vec<u8>)> {
		read_entries(path)
			.unwrap()
			.into_iter()
			.flat_map(|e| {
				let p = String::from_utf8(e.path.clone()).unwrap();
				e.records
					.into_iter()
					.map(move |r| (p.clone(), r.header.as_bytes().to_vec(), r.data))
			})
			.collect()
	}

	const LONG: &str = "surrealism/fs/a/very/long/directory/name/that/does/not/fit/into/the/one/hundred/byte/ustar/name/field.bin";

	#[test]
	fn resigning_keeps_every_other_record_and_replaces_the_signature() {
		let dir = tempfile::tempdir().unwrap();
		let pkg = archive_with_manifest(
			dir.path(),
			&[
				("surrealism/mod.wasm", b"\0asm\x0d\0\x01\0"),
				("surrealism/package.toml", b"[meta]\nname = \"x\"\n"),
				(LONG, b"payload"),
			],
		);
		let before = dump(&pkg);
		let manifest = read_manifest(&pkg).unwrap();
		let digest = manifest.digest();

		write_signature(&pkg, &pkg, &manifest, b"first").unwrap();
		write_signature(&pkg, &pkg, &manifest, b"second").unwrap();

		let after = dump(&pkg);
		let (sigs, rest): (Vec<_>, Vec<_>) =
			after.into_iter().partition(|(p, _, _)| p == SIGNATURE_PATH);
		assert_eq!(rest, before);
		assert_eq!(sigs.len(), 1);
		assert_eq!(sigs[0].2, b"second");
		assert_eq!(read_manifest(&pkg).unwrap().digest(), digest);
	}

	/// Signing replaces the archive through a temporary file, which is created
	/// owner-only; the result keeps the mode of the archive it replaces, or of
	/// the input when written to a new path.
	#[cfg(unix)]
	#[test]
	fn signing_keeps_the_archive_permissions() {
		use std::os::unix::fs::PermissionsExt as _;

		let dir = tempfile::tempdir().unwrap();
		let pkg = archive_with_manifest(dir.path(), &[("surrealism/mod.wasm", b"\0asm")]);
		let manifest = read_manifest(&pkg).unwrap();
		let mode = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;

		std::fs::set_permissions(&pkg, std::fs::Permissions::from_mode(0o644)).unwrap();
		write_signature(&pkg, &pkg, &manifest, b"sig").unwrap();
		assert_eq!(mode(&pkg), 0o644);

		let copy = dir.path().join("signed.surli");
		std::fs::set_permissions(&pkg, std::fs::Permissions::from_mode(0o640)).unwrap();
		write_signature(&pkg, &copy, &manifest, b"sig").unwrap();
		assert_eq!(mode(&copy), 0o640);
	}

	#[test]
	fn an_archive_without_a_manifest_is_refused() {
		let dir = tempfile::tempdir().unwrap();
		let pkg = write_archive(&dir.path().join("old.surli"), &[("surrealism/mod.wasm", b"x")]);
		let manifest = Manifest::from_entries([]).unwrap();
		let err = write_signature(&pkg, &pkg, &manifest, b"sig").unwrap_err();
		assert!(err.to_string().contains("has no surrealism/MANIFEST"), "{err}");
	}

	/// A signature made over one MANIFEST is not written into an archive
	/// whose MANIFEST has since changed, and the archive is left as it was.
	#[test]
	fn a_signature_over_another_manifest_is_refused() {
		let dir = tempfile::tempdir().unwrap();
		let pkg =
			archive_with_manifest(dir.path(), &[("surrealism/mod.wasm", b"\0asm\x0d\0\x01\0")]);
		let original = std::fs::read(&pkg).unwrap();
		let other = Manifest::from_entries([(
			"surrealism/mod.wasm".to_string(),
			surrealism_runtime::manifest::Digest::of(b"other"),
		)])
		.unwrap();
		let err = write_signature(&pkg, &pkg, &other, b"sig").unwrap_err();
		assert!(err.to_string().contains("changed while it was being signed"), "{err}");
		assert_eq!(std::fs::read(&pkg).unwrap(), original);
	}

	#[test]
	fn two_manifests_are_refused() {
		let dir = tempfile::tempdir().unwrap();
		let pkg = write_archive(
			&dir.path().join("dup.surli"),
			&[(MANIFEST_PATH, b""), (MANIFEST_PATH, b"")],
		);
		assert!(read_manifest(&pkg).is_err());
	}
}
