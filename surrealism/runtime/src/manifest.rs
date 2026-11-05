//! `surrealism/MANIFEST`: the list of every file in a `.surli` archive with its
//! SHA-256 hash. The signature signs the manifest, and the manifest's own hash
//! is the archive's [`Digest`].
//!
//! The digest hashes the manifest's bytes, so the format is exact:
//!
//! ```text
//! sha256:<64 lowercase hex>  <path>\n
//! ```
//!
//! - UTF-8 with no byte-order mark; every line, the last included, ends with a single LF.
//! - Paths are relative to the archive root, use `/`, and are sorted by their raw bytes, with no
//!   duplicates.
//! - A path is canonical (see [`validate_path`]) and contains no LF, CR or NUL.
//! - No blank lines and no trailing spaces.
//!
//! [`Manifest::parse`] accepts exactly the bytes [`Manifest::from_entries`]
//! writes, so a parsed manifest re-renders to the same bytes and the same
//! digest.

use std::fmt;

use sha2::{Digest as _, Sha256};
use surrealism_types::err::{SurrealismError, SurrealismResult};

/// Path of the manifest inside the archive.
pub const MANIFEST_PATH: &str = "surrealism/MANIFEST";

/// Path of the signature inside the archive. It is the only file the manifest
/// does not list, besides the manifest itself.
pub const SIGNATURE_PATH: &str = "surrealism/SIGNATURE";

const HASH_PREFIX: &str = "sha256:";

/// A SHA-256 hash, rendered as `sha256:<64 lowercase hex>`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Digest(pub [u8; 32]);

impl Digest {
	/// Hash `bytes` with SHA-256.
	pub fn of(bytes: &[u8]) -> Self {
		Self(Sha256::digest(bytes).into())
	}
}

impl fmt::Display for Digest {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{HASH_PREFIX}{}", hex::encode(self.0))
	}
}

impl fmt::Debug for Digest {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		fmt::Display::fmt(self, f)
	}
}

/// A parsed `surrealism/MANIFEST`. Holds the exact bytes it was parsed from or
/// rendered to, because [`Manifest::digest`] hashes those bytes.
#[derive(Clone, PartialEq, Eq)]
pub struct Manifest {
	bytes: Vec<u8>,
	/// `(path, hash)`, strictly ascending by the path's raw bytes.
	entries: Vec<(String, Digest)>,
}

impl fmt::Debug for Manifest {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("Manifest")
			.field("digest", &self.digest())
			.field("entries", &self.entries.len())
			.finish()
	}
}

fn manifest_err(msg: impl fmt::Display) -> SurrealismError {
	SurrealismError::Other(anyhow::anyhow!("invalid {MANIFEST_PATH}: {msg}"))
}

/// Check that `path` is a canonical archive path: valid as written in the
/// manifest and unambiguous when extracted.
///
/// Rejected: an empty path, a leading `/`, an empty segment (`a//b`, a
/// trailing `/`), a `.` or `..` segment, a backslash, LF, CR or NUL, and a
/// leading or trailing space, which would make a manifest line ambiguous or
/// leave a trailing space on it.
///
/// Also rejected, because the loader extracts `fs/` entries below a
/// directory on whatever system the server runs on: a `:` anywhere, which on
/// Windows makes a segment such as `C:` a drive prefix that replaces the
/// extraction root when joined, or names an alternate data stream; and a
/// segment that is a Windows device name (`CON`, `PRN`, `AUX`, `NUL`, `COM1`
/// to `COM9`, `LPT1` to `LPT9`), with or without an extension and in any case,
/// which Windows opens as the device rather than as a file.
pub fn validate_path(path: &str) -> Result<(), String> {
	if path.is_empty() {
		return Err("empty path".into());
	}
	if let Some(c) = path.chars().find(|c| matches!(c, '\n' | '\r' | '\0' | ':')) {
		return Err(format!("path {path:?} contains {c:?}"));
	}
	if path.contains('\\') {
		return Err(format!("path {path:?} contains a backslash"));
	}
	if path.starts_with(' ') || path.ends_with(' ') {
		return Err(format!("path {path:?} starts or ends with a space"));
	}
	if path.starts_with('/') {
		return Err(format!("path {path:?} is absolute"));
	}
	for segment in path.split('/') {
		match segment {
			"" => return Err(format!("path {path:?} has an empty segment")),
			"." | ".." => return Err(format!("path {path:?} is not canonical")),
			_ if is_windows_device_name(segment) => {
				return Err(format!("path {path:?} names a Windows device"));
			}
			_ => {}
		}
	}
	Ok(())
}

/// Whether `segment`, ignoring case and anything from its first `.`, is a
/// name Windows reserves for a device.
fn is_windows_device_name(segment: &str) -> bool {
	let stem = segment.split('.').next().unwrap_or(segment).trim_end_matches(' ');
	let upper = stem.to_ascii_uppercase();
	match upper.as_str() {
		"CON" | "PRN" | "AUX" | "NUL" => true,
		_ => match upper.strip_prefix("COM").or_else(|| upper.strip_prefix("LPT")) {
			Some(n) => matches!(n, "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"),
			None => false,
		},
	}
}

impl Manifest {
	/// Render a manifest from `(path, hash)` pairs, in any order.
	///
	/// Fails on a non-canonical path or a path given twice.
	pub fn from_entries(
		entries: impl IntoIterator<Item = (String, Digest)>,
	) -> SurrealismResult<Self> {
		let mut entries: Vec<(String, Digest)> = entries.into_iter().collect();
		entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
		let mut bytes = Vec::with_capacity(entries.len() * 96);
		for (i, (path, hash)) in entries.iter().enumerate() {
			validate_path(path).map_err(manifest_err)?;
			if i > 0 && entries[i - 1].0 == *path {
				return Err(manifest_err(format!("duplicate path {path:?}")));
			}
			bytes.extend_from_slice(format!("{hash}  {path}\n").as_bytes());
		}
		Ok(Self {
			bytes,
			entries,
		})
	}

	/// Parse manifest bytes, rejecting anything [`Manifest::from_entries`]
	/// would not have written.
	pub fn parse(bytes: Vec<u8>) -> SurrealismResult<Self> {
		let text = std::str::from_utf8(&bytes).map_err(manifest_err)?;
		if !text.is_empty() && !text.ends_with('\n') {
			return Err(manifest_err("the last line does not end with LF"));
		}
		let mut entries: Vec<(String, Digest)> = Vec::new();
		// Every line ends with LF, so the text after the last LF is empty and skipped.
		for (n, line) in text.split_terminator('\n').enumerate() {
			let n = n + 1;
			let rest = line
				.strip_prefix(HASH_PREFIX)
				.ok_or_else(|| manifest_err(format!("line {n} does not start with `sha256:`")))?;
			let (hex_part, path) = rest
				.split_at_checked(64)
				.ok_or_else(|| manifest_err(format!("line {n} is too short")))?;
			if !hex_part.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
				return Err(manifest_err(format!("line {n}: hash is not 64 lowercase hex digits")));
			}
			let path = path.strip_prefix("  ").ok_or_else(|| {
				manifest_err(format!("line {n}: hash and path must be separated by two spaces"))
			})?;
			validate_path(path).map_err(|e| manifest_err(format!("line {n}: {e}")))?;
			if let Some((prev, _)) = entries.last()
				&& prev.as_bytes() >= path.as_bytes()
			{
				return Err(manifest_err(format!(
					"line {n}: path {path:?} is a duplicate or out of order"
				)));
			}
			let mut hash = [0u8; 32];
			hex::decode_to_slice(hex_part, &mut hash).map_err(manifest_err)?;
			entries.push((path.to_owned(), Digest(hash)));
		}
		Ok(Self {
			bytes,
			entries,
		})
	}

	/// The manifest's exact bytes.
	pub fn as_bytes(&self) -> &[u8] {
		&self.bytes
	}

	/// `sha256(MANIFEST)`: the archive's digest.
	pub fn digest(&self) -> Digest {
		Digest::of(&self.bytes)
	}

	/// `(path, hash)` pairs, ascending by the path's raw bytes.
	pub fn entries(&self) -> &[(String, Digest)] {
		&self.entries
	}

	/// The hash listed for `path`.
	pub fn get(&self, path: &str) -> Option<Digest> {
		self.entries
			.binary_search_by(|(p, _)| p.as_bytes().cmp(path.as_bytes()))
			.ok()
			.map(|i| self.entries[i].1)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn h(b: u8) -> Digest {
		Digest([b; 32])
	}

	#[test]
	fn renders_exact_format_sorted_by_bytes() {
		let m = Manifest::from_entries([
			("surrealism/mod.wasm".to_string(), h(0x77)),
			("surrealism/Z".to_string(), h(0x01)),
			("surrealism/exports.toml".to_string(), h(0xab)),
		])
		.unwrap();
		let expected = format!(
			"sha256:{}  surrealism/Z\nsha256:{}  surrealism/exports.toml\nsha256:{}  surrealism/mod.wasm\n",
			"01".repeat(32),
			"ab".repeat(32),
			"77".repeat(32),
		);
		assert_eq!(std::str::from_utf8(m.as_bytes()).unwrap(), expected);
		assert_eq!(m.digest(), Digest::of(expected.as_bytes()));
		let parsed = Manifest::parse(expected.into_bytes()).unwrap();
		assert_eq!(parsed, m);
		assert_eq!(parsed.get("surrealism/mod.wasm"), Some(h(0x77)));
		assert_eq!(parsed.get("surrealism/missing"), None);
	}

	#[test]
	fn digest_display() {
		assert_eq!(h(0xa5).to_string(), format!("sha256:{}", "a5".repeat(32)));
	}

	#[test]
	fn empty_manifest_is_empty_bytes() {
		let m = Manifest::from_entries([]).unwrap();
		assert!(m.as_bytes().is_empty());
		assert!(Manifest::parse(Vec::new()).unwrap().entries().is_empty());
	}

	#[test]
	fn writer_rejects_bad_paths() {
		for bad in [
			"",
			"/abs",
			"a//b",
			"a/",
			"./a",
			"a/./b",
			"a/../b",
			"..",
			"a\nb",
			"a\rb",
			"a\0b",
			"a\\b",
			" a",
			"a ",
			"C:/x",
			"fs/C:",
			"a:b",
			"fs/a.txt:stream",
			"fs/CON",
			"fs/nul.txt",
			"fs/Com1",
			"fs/lpt9.dat",
			"fs/aux/x",
		] {
			assert!(Manifest::from_entries([(bad.to_string(), h(0))]).is_err(), "accepted {bad:?}");
		}
		assert!(Manifest::from_entries([("a".into(), h(0)), ("a".into(), h(1))]).is_err());
	}

	#[test]
	fn names_near_a_device_name_are_accepted() {
		for good in ["fs/console.txt", "fs/COM10", "fs/com0", "fs/lpt", "fs/nul_", "fs/x.con"] {
			assert!(validate_path(good).is_ok(), "refused {good:?}");
		}
	}

	#[test]
	fn parser_rejects_non_canonical_text() {
		let line = |path: &str| format!("sha256:{}  {path}\n", "00".repeat(32));
		let ok = line("a");
		assert!(Manifest::parse(ok.clone().into_bytes()).is_ok());
		let cases = [
			format!("\u{feff}{ok}"),                   // BOM
			ok.trim_end().to_string(),                 // no final LF
			ok.replace('\n', "\r\n"),                  // CRLF
			format!("{ok}\n"),                         // blank line
			ok.replace("  a", "   a"),                 // three spaces
			ok.replace("  a", " a"),                   // one space
			ok.replace("\n", " \n"),                   // trailing space
			ok.replace("sha256:", "SHA256:"),          // prefix case
			ok.replace("00", "AA"),                    // uppercase hex
			format!("sha256:{}  a\n", "0".repeat(63)), // short hash
			format!("{}{}", line("b"), line("a")),     // unsorted
			format!("{}{}", line("a"), line("a")),     // duplicate
			line("./a"),                               // non-canonical
			line("a/../b"),                            // non-canonical
		];
		for case in cases {
			assert!(Manifest::parse(case.clone().into_bytes()).is_err(), "accepted {case:?}");
		}
		assert!(Manifest::parse(vec![0xff, b'\n']).is_err(), "accepted invalid UTF-8");
	}
}
