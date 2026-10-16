use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use semver::Version;
use serde::{Deserialize, Serialize};
use surrealism_types::err::{PrefixErr, SurrealismResult};

use crate::capabilities::SurrealismCapabilities;

/// Source language / toolchain the module is written in.
///
/// Only `Rust` is supported today. The field is used by `init` (to choose
/// which scaffold to generate) and `build` (to choose which toolchain to
/// invoke). Once compiled to WASM the target is irrelevant at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
	#[default]
	Rust,
}

impl std::fmt::Display for Target {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Target::Rust => write!(f, "rust"),
		}
	}
}

/// Which ABI version the plugin targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AbiVersion(pub u32);

impl AbiVersion {
	pub const CURRENT: Self = Self(4);
}

impl Serialize for AbiVersion {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_u32(self.0)
	}
}

impl<'de> Deserialize<'de> for AbiVersion {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		struct Visitor;
		impl serde::de::Visitor<'_> for Visitor {
			type Value = AbiVersion;

			fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
				write!(f, "an integer ABI version or legacy string (\"p1\", \"p2\")")
			}

			fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<AbiVersion, E> {
				u32::try_from(v)
					.map(AbiVersion)
					.map_err(|_| E::custom(format!("ABI version out of range: {v}")))
			}

			fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<AbiVersion, E> {
				u32::try_from(v)
					.map(AbiVersion)
					.map_err(|_| E::custom(format!("ABI version out of range: {v}")))
			}

			fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<AbiVersion, E> {
				match v {
					"p1" | "1" => Ok(AbiVersion(1)),
					"p2" | "2" => Ok(AbiVersion(2)),
					other => Err(E::custom(format!("unknown ABI version: {other}"))),
				}
			}
		}
		deserializer.deserialize_any(Visitor)
	}
}

/// File name of the package file, in a project directory and inside an archive
/// (as `surrealism/package.toml`).
pub const PACKAGE_FILE: &str = "package.toml";

/// Legacy file name of the package file. It is read only when no
/// [`PACKAGE_FILE`] is present beside it.
pub const LEGACY_PACKAGE_FILE: &str = "surrealism.toml";

/// The package file selected in a project directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PackageFile {
	/// The file to read: [`PACKAGE_FILE`] if it exists, else [`LEGACY_PACKAGE_FILE`].
	pub path: PathBuf,
	/// [`LEGACY_PACKAGE_FILE`] when it exists beside a [`PACKAGE_FILE`]. Its
	/// contents are never read; callers report it so the author can remove it.
	pub ignored: Option<PathBuf>,
}

impl PackageFile {
	/// Select the package file in `dir`. A name is present when anything is
	/// at that path, without following a symlink: a directory, a dangling
	/// symlink or an unreadable file under [`PACKAGE_FILE`] is selected, so
	/// reading it fails, rather than letting [`LEGACY_PACKAGE_FILE`] stand in
	/// for it. Returns `None` when neither name is present.
	pub fn locate(dir: &Path) -> Option<Self> {
		let current = dir.join(PACKAGE_FILE);
		let legacy = dir.join(LEGACY_PACKAGE_FILE);
		let present = |path: &Path| path.symlink_metadata().is_ok();
		match (present(&current), present(&legacy)) {
			(true, legacy_exists) => Some(Self {
				path: current,
				ignored: legacy_exists.then_some(legacy),
			}),
			(false, true) => Some(Self {
				path: legacy,
				ignored: None,
			}),
			(false, false) => None,
		}
	}
}

/// The contents of a package file. Read and written through
/// [`PackageConfig`](crate::package_config::PackageConfig), never directly.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SurrealismConfig {
	#[serde(default)]
	pub target: Target,
	#[serde(rename = "package")]
	pub meta: SurrealismMeta,
	#[serde(default)]
	pub capabilities: SurrealismCapabilities,
	#[serde(default)]
	pub abi: AbiVersion,
	#[serde(default)]
	pub attach: SurrealismAttach,
	/// Who may sign the package's archives. `surreal ext sign` refuses
	/// to sign as anyone else.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub signer: Option<PackageSigner>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct SurrealismAttach {
	/// Directories bundled into the archive and mounted read-only for the
	/// module. See [`FsMounts`].
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub fs: Option<FsMounts>,
}

/// Directories bundled under `surrealism/fs/` and seen by the module as one
/// read-only filesystem: each local directory, relative to the project root
/// or absolute, appears at its guest path.
///
/// Written as a string when the only mount is at `/`, the form older package
/// files use, and as a table of guest path to local directory otherwise:
///
/// ```toml
/// [attach]
/// fs = "fs"
///
/// [attach.fs]
/// "/" = "fs"
/// "/models" = "../models"
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FsMounts(BTreeMap<String, String>);

impl FsMounts {
	/// The guest path the root mount appears at.
	pub const ROOT: &str = "/";

	/// `local` mounted at `/`, the form a string `fs = "…"` reads as.
	pub fn root(local: impl Into<String>) -> Self {
		Self(BTreeMap::from([(Self::ROOT.to_string(), local.into())]))
	}

	/// Every mount, as (guest path, local directory), shortest guest path first.
	pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
		self.0.iter().map(|(guest, local)| (guest.as_str(), local.as_str()))
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Mount `local` at `guest`, replacing what was mounted there.
	pub(crate) fn insert(&mut self, guest: String, local: String) {
		self.0.insert(guest, local);
	}

	/// Unmount `guest`. Returns whether something was mounted there.
	pub(crate) fn remove(&mut self, guest: &str) -> bool {
		self.0.remove(guest).is_some()
	}

	/// `guest` in its stored form, `/` or `/a/b`, or why it is not a guest
	/// path: every segment must be one an archive path may hold.
	pub fn normalise_guest_path(guest: &str) -> Result<String, String> {
		let trimmed = guest.trim_matches('/');
		if trimmed.is_empty() {
			return Ok(Self::ROOT.to_string());
		}
		crate::manifest::validate_path(trimmed)
			.map_err(|e| format!("{guest:?} is not a mount path: {e}"))?;
		Ok(format!("/{trimmed}"))
	}
}

impl Serialize for FsMounts {
	fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		match self.0.iter().next() {
			Some((guest, local)) if self.0.len() == 1 && guest == Self::ROOT => {
				serializer.serialize_str(local)
			}
			_ => self.0.serialize(serializer),
		}
	}
}

impl<'de> Deserialize<'de> for FsMounts {
	fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		#[derive(Deserialize)]
		#[serde(untagged)]
		enum Form {
			Root(String),
			Mounts(BTreeMap<String, String>),
		}
		let mounts = match Form::deserialize(deserializer)? {
			Form::Root(local) => BTreeMap::from([(Self::ROOT.to_string(), local)]),
			Form::Mounts(mounts) => {
				let mut normalised = BTreeMap::new();
				for (guest, local) in mounts {
					let guest =
						Self::normalise_guest_path(&guest).map_err(serde::de::Error::custom)?;
					normalised.insert(guest, local);
				}
				normalised
			}
		};
		Ok(Self(mounts))
	}
}

/// `[signer]`: the one identity whose signatures this package's archives may
/// carry.
///
/// ```toml
/// [signer.sigstore]
/// issuer = "https://token.actions.githubusercontent.com"
/// identity = "https://github.com/acme/color/.github/workflows/release.yml"
/// repository_id = "812345678"
/// owner_id = "98765432"
/// ```
///
/// or
///
/// ```toml
/// [signer.ssh]
/// public_key = "ssh-ed25519 AAAA…"
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PackageSigner {
	Sigstore(SigstoreSigner),
	Ssh(SshSigner),
}

/// A Sigstore keyless signer: the certificate Fulcio issues to the CI job.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SigstoreSigner {
	/// The OIDC issuer, certificate extension `1.3.6.1.4.1.57264.1.8`.
	pub issuer: String,
	/// The workflow identity, the certificate's subject alternative name
	/// without its `@ref` suffix, so every ref of the workflow matches.
	pub identity: String,
	/// The repository's numeric id, extension `1.3.6.1.4.1.57264.1.15`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub repository_id: Option<String>,
	/// The repository owner's numeric id, extension `1.3.6.1.4.1.57264.1.17`.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub owner_id: Option<String>,
}

/// An SSH signer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SshSigner {
	/// The public key as `<type> <base64>`, without a comment.
	pub public_key: String,
}

impl PackageSigner {
	/// Refuse a signer no signature could ever match.
	pub fn validate(&self) -> Result<(), String> {
		match self {
			Self::Sigstore(s) => {
				if !s.issuer.starts_with("https://") {
					return Err(format!(
						"signer.sigstore.issuer {:?} is not an https URL",
						s.issuer
					));
				}
				if s.identity.trim().is_empty() || s.identity.trim() != s.identity {
					return Err(format!(
						"signer.sigstore.identity {:?} is empty or has surrounding spaces",
						s.identity
					));
				}
				// A workflow URI is named without its ref, so any ref matches.
				// Any other identity, such as an email address from an
				// interactive signing, is compared as written.
				if s.identity.starts_with("https://") && s.identity.contains('@') {
					return Err(format!(
						"signer.sigstore.identity {:?} names a ref; give the workflow without `@…`, \
						 so that every ref of it matches",
						s.identity
					));
				}
				for (field, id) in [("repository_id", &s.repository_id), ("owner_id", &s.owner_id)]
				{
					if let Some(id) = id
						&& (id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()))
					{
						return Err(format!("signer.sigstore.{field} {id:?} is not a numeric id"));
					}
				}
				Ok(())
			}
			Self::Ssh(s) => SshSigner::normalise(&s.public_key).map(|_| ()),
		}
	}
}

impl SshSigner {
	/// `key`, an `authorized_keys` line or the contents of a `.pub` file, as
	/// `<type> <base64>` with any comment dropped.
	pub fn normalise(key: &str) -> Result<String, String> {
		let mut parts = key.split_whitespace();
		let (Some(kind), Some(blob)) = (parts.next(), parts.next()) else {
			return Err(format!("{key:?} is not an SSH public key"));
		};
		if !(kind.starts_with("ssh-") || kind.starts_with("ecdsa-") || kind.starts_with("sk-")) {
			return Err(format!("{kind:?} is not an SSH key type"));
		}
		let base64 = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=');
		if blob.len() % 4 != 0 || !blob.bytes().all(base64) {
			return Err(format!("the {kind} key is not valid base64"));
		}
		Ok(format!("{kind} {blob}"))
	}
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct SurrealismMeta {
	pub organisation: String,
	pub name: String,
	pub version: Version,
	/// The source repository the author declares, e.g.
	/// `https://github.com/acme/color`. Install clients show whether it
	/// matches the repository a Sigstore signer names; nothing trusts it.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub repository: Option<String>,
}

impl SurrealismConfig {
	pub(crate) fn parse(s: &str) -> SurrealismResult<Self> {
		toml::from_str(s).prefix_err(|| "Failed to parse Surrealism config")
	}

	pub(crate) fn to_toml(&self) -> SurrealismResult<String> {
		toml::to_string(self).prefix_err(|| "Failed to serialize Surrealism config")
	}

	pub fn file_name(&self) -> String {
		format!("{}-{}-{}.surli", self.meta.organisation, self.meta.name, self.meta.version)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parse_minimal_config() {
		let toml = r#"
[package]
organisation = "test"
name = "demo"
version = "1.0.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.target, Target::Rust);
		assert_eq!(config.meta.organisation, "test");
		assert_eq!(config.meta.name, "demo");
		assert_eq!(config.meta.version, Version::new(1, 0, 0));
		assert_eq!(config.abi, AbiVersion::default());
	}

	#[test]
	fn parse_explicit_target() {
		let toml = r#"
target = "rust"

[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.target, Target::Rust);
	}

	#[test]
	fn parse_full_config() {
		let toml = r#"
abi = 4

[package]
organisation = "acme"
name = "widget"
version = "2.3.1"

[capabilities]
allow_scripting = true
allow_arbitrary_queries = true
allow_functions = ["http::*", "fn::check"]

[attach]
fs = "static"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.meta.name, "widget");
		assert_eq!(config.abi, AbiVersion::CURRENT);
		assert!(config.capabilities.allow_scripting);
		assert!(config.capabilities.allow_arbitrary_queries);
		assert!(config.capabilities.allow_functions.allows("http::get"));
		assert!(config.capabilities.allow_functions.allows("fn::check"));
		assert!(!config.capabilities.allow_functions.allows("string::len"));
		let mounts: Vec<_> = config.attach.fs.as_ref().unwrap().iter().collect();
		assert_eq!(mounts, [("/", "static")]);
	}

	#[test]
	fn abi_version_integer() {
		let toml = r#"
abi = 2

[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.abi, AbiVersion(2));
	}

	#[test]
	fn abi_version_legacy_string() {
		let toml = r#"
abi = "p2"

[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.abi, AbiVersion(2));
	}

	#[test]
	fn abi_version_legacy_p1() {
		let toml = r#"
abi = "p1"

[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert_eq!(config.abi, AbiVersion(1));
	}

	#[test]
	fn config_roundtrip() {
		let config = SurrealismConfig {
			target: Target::Rust,
			meta: SurrealismMeta {
				organisation: "test".to_string(),
				name: "roundtrip".to_string(),
				version: Version::new(0, 1, 0),
				repository: None,
			},
			capabilities: Default::default(),
			abi: AbiVersion::CURRENT,
			attach: SurrealismAttach {
				fs: Some(FsMounts::root("data")),
			},
			signer: None,
		};

		let toml_str = config.to_toml().unwrap();
		let parsed = SurrealismConfig::parse(&toml_str).unwrap();
		assert_eq!(parsed.target, Target::Rust);
		assert_eq!(parsed.meta.organisation, "test");
		assert_eq!(parsed.meta.name, "roundtrip");
		assert_eq!(parsed.abi, AbiVersion::CURRENT);
		assert_eq!(parsed.attach, config.attach);
		assert!(toml_str.contains("fs = \"data\""), "a root mount is written as a string");
	}

	#[test]
	fn file_name_format() {
		let config = SurrealismConfig {
			target: Target::default(),
			meta: SurrealismMeta {
				organisation: "surrealdb".to_string(),
				name: "demo".to_string(),
				version: Version::new(1, 2, 3),
				repository: None,
			},
			capabilities: Default::default(),
			abi: AbiVersion::CURRENT,
			attach: Default::default(),
			signer: None,
		};
		assert_eq!(config.file_name(), "surrealdb-demo-1.2.3.surli");
	}

	#[test]
	fn attach_default_is_none() {
		let toml = r#"
[package]
organisation = "t"
name = "t"
version = "0.1.0"
"#;
		let config = SurrealismConfig::parse(toml).unwrap();
		assert!(config.attach.fs.is_none());
	}

	#[test]
	fn locate_package_toml_only() {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(dir.path().join(PACKAGE_FILE), "").unwrap();
		let found = PackageFile::locate(dir.path()).unwrap();
		assert_eq!(found.path, dir.path().join(PACKAGE_FILE));
		assert_eq!(found.ignored, None);
	}

	#[test]
	fn locate_surrealism_toml_only() {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(dir.path().join(LEGACY_PACKAGE_FILE), "").unwrap();
		let found = PackageFile::locate(dir.path()).unwrap();
		assert_eq!(found.path, dir.path().join(LEGACY_PACKAGE_FILE));
		assert_eq!(found.ignored, None);
	}

	#[test]
	fn locate_both_prefers_package_toml() {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(dir.path().join(PACKAGE_FILE), "").unwrap();
		std::fs::write(dir.path().join(LEGACY_PACKAGE_FILE), "").unwrap();
		let found = PackageFile::locate(dir.path()).unwrap();
		assert_eq!(found.path, dir.path().join(PACKAGE_FILE));
		assert_eq!(found.ignored, Some(dir.path().join(LEGACY_PACKAGE_FILE)));
	}

	#[test]
	fn locate_selects_a_package_toml_that_is_not_a_file() {
		let dir = tempfile::tempdir().unwrap();
		std::fs::create_dir(dir.path().join(PACKAGE_FILE)).unwrap();
		std::fs::write(dir.path().join(LEGACY_PACKAGE_FILE), "").unwrap();
		let found = PackageFile::locate(dir.path()).unwrap();
		assert_eq!(found.path, dir.path().join(PACKAGE_FILE));
		assert!(std::fs::read_to_string(&found.path).is_err());
	}

	#[cfg(unix)]
	#[test]
	fn locate_selects_a_dangling_package_toml_symlink() {
		let dir = tempfile::tempdir().unwrap();
		std::os::unix::fs::symlink(dir.path().join("missing"), dir.path().join(PACKAGE_FILE))
			.unwrap();
		std::fs::write(dir.path().join(LEGACY_PACKAGE_FILE), "").unwrap();
		let found = PackageFile::locate(dir.path()).unwrap();
		assert_eq!(found.path, dir.path().join(PACKAGE_FILE));
	}

	#[test]
	fn locate_neither() {
		let dir = tempfile::tempdir().unwrap();
		assert_eq!(PackageFile::locate(dir.path()), None);
	}
}
