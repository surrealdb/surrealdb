//! The one way to read and write a package file.
//!
//! [`PackageConfig`] holds a package file twice: as the typed
//! [`SurrealismConfig`] callers read and change through the methods here, and
//! as the [`toml_edit`] document it was read from. Saving compares the typed
//! config with the one that was read and writes only the keys and tables that
//! differ into the document, so comments, ordering and formatting of
//! everything else survive. Opening and saving without a change rewrites the
//! file byte for byte.
//!
//! ```no_run
//! # use surrealism_runtime::package_config::PackageConfig;
//! let mut package = PackageConfig::open(std::path::Path::new("."))?;
//! package.capabilities_mut().add_net("api.color.io:443")?;
//! package.save()?;
//! # Ok::<(), surrealism_types::err::SurrealismError>(())
//! ```

use std::path::{Path, PathBuf};
use std::time::Duration;

use semver::Version;
use surrealism_types::err::{PrefixErr, SurrealismError, SurrealismResult};
use toml_edit::{DocumentMut, InlineTable, Item, Table, TableLike, Value};

use crate::capabilities::{FunctionTargets, SurrealismCapabilities};
use crate::config::{
	AbiVersion, FsMounts, PACKAGE_FILE, PackageFile, PackageSigner, SurrealismAttach,
	SurrealismConfig, SurrealismMeta, Target,
};

fn invalid(message: impl Into<String>) -> SurrealismError {
	SurrealismError::Other(anyhow::anyhow!(message.into()))
}

/// A package file: its typed contents and the document they were read from.
#[derive(Debug, Clone)]
pub struct PackageConfig {
	/// Where [`Self::save`] writes. `None` for a package file read from
	/// elsewhere, such as an archive.
	path: Option<PathBuf>,
	/// A legacy package file that was not read because a current one exists.
	ignored: Option<PathBuf>,
	/// The typed contents as read, or as last saved.
	saved: SurrealismConfig,
	/// The typed contents, with the changes made since.
	config: SurrealismConfig,
	/// The document as read, or as last saved.
	document: DocumentMut,
}

impl PackageConfig {
	/// The package file of the project in `dir`: `package.toml`, or
	/// `surrealism.toml` when there is no `package.toml`
	/// ([`PackageFile::locate`]).
	pub fn open(dir: &Path) -> SurrealismResult<Self> {
		let located = PackageFile::locate(dir).ok_or_else(|| {
			invalid(format!(
				"Neither {PACKAGE_FILE} nor {} found in {}",
				crate::config::LEGACY_PACKAGE_FILE,
				dir.display()
			))
		})?;
		let name = located.path.file_name().unwrap_or_default().to_string_lossy().into_owned();
		let text = std::fs::read_to_string(&located.path)
			.prefix_err(|| format!("Failed to read {name}"))?;
		let mut package = Self::parse(&text).prefix_err(|| format!("Invalid {name}"))?;
		package.path = Some(located.path);
		package.ignored = located.ignored;
		Ok(package)
	}

	/// A package file read from `text`, such as the one inside an archive.
	/// [`Self::save`] refuses it; [`Self::render`] gives its text.
	pub fn parse(text: &str) -> SurrealismResult<Self> {
		let document: DocumentMut =
			text.parse().prefix_err(|| "Failed to parse the package file")?;
		let config = SurrealismConfig::parse(text)?;
		if let Some(signer) = &config.signer {
			signer.validate().map_err(invalid)?;
		}
		Ok(Self {
			path: None,
			ignored: None,
			saved: config.clone(),
			config,
			document,
		})
	}

	/// The package file of an archive, as a server loading the archive reads
	/// it.
	///
	/// `[signer]` and `[attach]` are for the tools that sign and build an
	/// archive, and the server reads neither. A later tool may write either in
	/// a form this version does not know, so when the file does not parse
	/// with them, it is read again without them, and `[signer]` is never
	/// validated here: an archive never fails to load because of them.
	pub fn for_loader(text: &str) -> SurrealismResult<SurrealismConfig> {
		/// Sections only the build and sign tools use.
		const TOOL_SECTIONS: [&str; 2] = ["signer", "attach"];
		match SurrealismConfig::parse(text) {
			Ok(config) => Ok(config),
			Err(strict) => {
				let mut table: toml::Table = toml::from_str(text).map_err(|_| strict)?;
				for section in TOOL_SECTIONS {
					table.remove(section);
				}
				table.try_into().prefix_err(|| "Failed to parse Surrealism config")
			}
		}
	}

	/// A new package file for the project in `dir`, written on [`Self::save`].
	pub fn new(dir: &Path, meta: SurrealismMeta) -> SurrealismResult<Self> {
		identifier("package.organisation", &meta.organisation)?;
		identifier("package.name", &meta.name)?;
		let mut package = Self::from_config(SurrealismConfig {
			target: Target::Rust,
			meta,
			capabilities: SurrealismCapabilities::default(),
			abi: AbiVersion::LATEST,
			attach: SurrealismAttach::default(),
			signer: None,
			bindings: Default::default(),
		})?;
		package.path = Some(dir.join(PACKAGE_FILE));
		Ok(package)
	}

	/// A package file holding `config`, rendered in full.
	pub fn from_config(config: SurrealismConfig) -> SurrealismResult<Self> {
		let document = as_document(&config)?;
		Ok(Self {
			path: None,
			ignored: None,
			saved: config.clone(),
			config,
			document,
		})
	}

	/// The typed contents, with the changes made so far.
	pub fn config(&self) -> &SurrealismConfig {
		&self.config
	}

	/// The typed contents, with the changes made so far.
	pub fn into_config(self) -> SurrealismConfig {
		self.config
	}

	/// The value at the dotted `key`, such as `package.version` or
	/// `capabilities.allow_net`, defaults included, in TOML form; a string is
	/// given without quotes. `None` when no such key exists.
	pub fn get(&self, key: &str) -> SurrealismResult<Option<String>> {
		let path = key_path(key).map_err(invalid)?;
		let mut value = toml::Value::try_from(&self.config)
			.prefix_err(|| "Failed to read the package file's values")?;
		for segment in &path {
			let toml::Value::Table(mut table) = value else {
				return Ok(None);
			};
			match table.remove(segment) {
				Some(inner) => value = inner,
				None => return Ok(None),
			}
		}
		Ok(Some(match value {
			toml::Value::String(s) => s,
			// Rendered under its own path, so `get capabilities` reads
			// `[capabilities]` rather than bare keys.
			toml::Value::Table(table) => {
				let nested = path.iter().rev().fold(toml::Value::Table(table), |inner, segment| {
					toml::Value::Table(toml::Table::from_iter([(segment.clone(), inner)]))
				});
				toml::to_string(&nested).prefix_err(|| format!("Failed to render {key}"))?
			}
			other @ (toml::Value::Integer(_)
			| toml::Value::Float(_)
			| toml::Value::Boolean(_)
			| toml::Value::Datetime(_)
			| toml::Value::Array(_)) => other.to_string(),
		}))
	}

	/// The file [`Self::save`] writes, if any.
	pub fn path(&self) -> Option<&Path> {
		self.path.as_deref()
	}

	/// A `surrealism.toml` that was not read because a `package.toml` exists
	/// beside it.
	pub fn ignored(&self) -> Option<&Path> {
		self.ignored.as_deref()
	}

	pub fn set_organisation(&mut self, organisation: &str) -> SurrealismResult<()> {
		self.config.meta.organisation = identifier("package.organisation", organisation)?;
		Ok(())
	}

	pub fn set_name(&mut self, name: &str) -> SurrealismResult<()> {
		self.config.meta.name = identifier("package.name", name)?;
		Ok(())
	}

	pub fn set_version(&mut self, version: &str) -> SurrealismResult<()> {
		self.config.meta.version = Version::parse(version)
			.map_err(|e| invalid(format!("package.version {version:?} is not a version: {e}")))?;
		Ok(())
	}

	/// Stamp the ABI the package is built for.
	pub fn set_abi(&mut self, abi: AbiVersion) {
		self.config.abi = abi;
	}

	/// The capabilities the package asks for.
	pub fn capabilities_mut(&mut self) -> CapabilitiesMut<'_> {
		CapabilitiesMut(&mut self.config.capabilities)
	}

	/// Set or clear `[signer]`.
	pub fn set_signer(&mut self, signer: Option<PackageSigner>) -> SurrealismResult<()> {
		if let Some(signer) = &signer {
			signer.validate().map_err(invalid)?;
		}
		self.config.signer = signer;
		Ok(())
	}

	/// Mount the local directory `local` at guest path `guest`, replacing
	/// what was mounted there. `local` is relative to the project root, or
	/// absolute.
	pub fn add_fs(&mut self, local: &str, guest: &str) -> SurrealismResult<()> {
		if local.trim().is_empty() {
			return Err(invalid("the directory to mount is empty"));
		}
		let guest = FsMounts::normalise_guest_path(guest).map_err(invalid)?;
		self.config
			.attach
			.fs
			.get_or_insert_with(FsMounts::default)
			.insert(guest, local.to_string());
		Ok(())
	}

	/// Remove the mount at `guest`. Returns whether there was one.
	pub fn remove_fs(&mut self, guest: &str) -> SurrealismResult<bool> {
		let guest = FsMounts::normalise_guest_path(guest).map_err(invalid)?;
		let Some(mounts) = &mut self.config.attach.fs else {
			return Ok(false);
		};
		let removed = mounts.remove(&guest);
		if mounts.is_empty() {
			self.config.attach.fs = None;
		}
		Ok(removed)
	}

	/// The package file's text with the changes made so far.
	pub fn render(&self) -> SurrealismResult<String> {
		Ok(self.document_with_changes()?.to_string())
	}

	/// Write the changes made so far to [`Self::path`]. Keys and tables that
	/// did not change keep their text, comments and position.
	pub fn save(&mut self) -> SurrealismResult<()> {
		let Some(path) = &self.path else {
			return Err(invalid(
				"this package file was not read from a project, so it cannot be saved",
			));
		};
		let document = self.document_with_changes()?;
		// Written beside the file and renamed over it, so a failure leaves the
		// old file whole.
		let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
		let mut temporary = tempfile::NamedTempFile::new_in(dir)
			.prefix_err(|| format!("Failed to write {}", path.display()))?;
		std::io::Write::write_all(&mut temporary, document.to_string().as_bytes())
			.prefix_err(|| format!("Failed to write {}", path.display()))?;
		temporary
			.persist(path)
			.map_err(|e| invalid(format!("Failed to replace {}: {}", path.display(), e.error)))?;
		self.document = document;
		self.saved = self.config.clone();
		Ok(())
	}

	/// The document with every key that differs between [`Self::saved`] and
	/// [`Self::config`] written into it.
	fn document_with_changes(&self) -> SurrealismResult<DocumentMut> {
		let mut document = self.document.clone();
		if self.config == self.saved {
			return Ok(document);
		}
		let saved = as_document(&self.saved)?;
		let current = as_document(&self.config)?;
		apply_changes("", saved.as_table(), current.as_table(), document.as_table_mut(), false);
		Ok(document)
	}
}

/// `config` as the document a fresh serialisation produces.
fn as_document(config: &SurrealismConfig) -> SurrealismResult<DocumentMut> {
	config.to_toml()?.parse().prefix_err(|| "Failed to re-read the serialised package file")
}

/// Write into `target` every key of `current` whose value differs from the one
/// in `saved`, and remove every key `saved` has and `current` does not. A
/// table present on both sides is compared key by key, so an unchanged key
/// inside a changed table is not touched either.
///
/// `target` is the author's document at `path` (dotted, empty at the root),
/// a standard table or an inline one; `inline` says which, so a table this
/// adds inside an inline table is inline too. A key the author spelled
/// differently from the serialisation, such as the mount `models/` for
/// `/models`, is found by [`document_key`].
fn apply_changes(
	path: &str,
	saved: &Table,
	current: &Table,
	target: &mut dyn TableLike,
	inline: bool,
) {
	for (key, new) in current.iter() {
		let old = saved.get(key);
		if old.is_some_and(|old| same(old, new)) {
			continue;
		}
		let at = document_key(path, target, key);
		match new.as_table() {
			Some(new) => {
				if !target.get(&at).is_some_and(Item::is_table_like) {
					let item = if inline {
						Item::Value(Value::InlineTable(InlineTable::new()))
					} else {
						let mut table = Table::new();
						table.set_implicit(new.is_implicit());
						Item::Table(table)
					};
					target.insert(&at, item);
				}
				let child_inline = matches!(target.get(&at), Some(Item::Value(_)));
				let child = target
					.get_mut(&at)
					.and_then(Item::as_table_like_mut)
					.expect("a table was just inserted at this key");
				let empty = Table::new();
				let old = old.and_then(Item::as_table).unwrap_or(&empty);
				let child_path = if path.is_empty() {
					key.to_string()
				} else {
					format!("{path}.{key}")
				};
				apply_changes(&child_path, old, new, child, child_inline);
			}
			None => match (target.get_mut(&at), new.as_value()) {
				// Keep the comment and spacing around the old value.
				(Some(Item::Value(existing)), Some(value)) => {
					let decor = existing.decor().clone();
					*existing = value.clone();
					*existing.decor_mut() = decor;
				}
				_ => {
					target.insert(&at, new.clone());
				}
			},
		}
	}
	let removed: Vec<String> = saved
		.iter()
		.filter(|(key, _)| !current.contains_key(key))
		.map(|(key, _)| document_key(path, target, key))
		.collect();
	for key in removed {
		target.remove(&key);
	}
}

/// The key in the author's table at `path` that stands for the serialised
/// `key`: the mount whose guest path normalises to `key` under
/// `attach.fs`, and `key` itself everywhere else or when no key matches.
fn document_key(path: &str, target: &dyn TableLike, key: &str) -> String {
	if path == "attach.fs" {
		let normalised = |k: &str| FsMounts::normalise_guest_path(k).ok();
		if let Some((found, _)) = target.iter().find(|(k, _)| normalised(k).as_deref() == Some(key))
		{
			return found.to_string();
		}
	}
	key.to_string()
}

/// Whether two items hold the same keys and values, decoration aside. Tables
/// are compared key by key, sub-tables included, which their `Display` does
/// not show.
fn same(a: &Item, b: &Item) -> bool {
	match (a, b) {
		(Item::Value(a), Item::Value(b)) => {
			a.clone().decorated("", "").to_string() == b.clone().decorated("", "").to_string()
		}
		(Item::Table(a), Item::Table(b)) => {
			a.len() == b.len() && a.iter().all(|(key, a)| b.get(key).is_some_and(|b| same(a, b)))
		}
		(Item::None, Item::None) => true,
		_ => a.to_string() == b.to_string(),
	}
}

/// The size of a WebAssembly page, the unit linear memory grows by.
const WASM_PAGE: usize = 64 * 1024;

/// The segments of a dotted key such as `attach.fs."/models"`. A segment in
/// double quotes may hold dots.
fn key_path(key: &str) -> Result<Vec<String>, String> {
	let mut segments = Vec::new();
	let mut rest = key;
	loop {
		let (segment, after) = if let Some(quoted) = rest.strip_prefix('"') {
			let end = quoted.find('"').ok_or_else(|| format!("{key:?} has an unclosed quote"))?;
			(quoted[..end].to_string(), &quoted[end + 1..])
		} else {
			let end = rest.find('.').unwrap_or(rest.len());
			(rest[..end].to_string(), &rest[end..])
		};
		if segment.is_empty() {
			return Err(format!("{key:?} has an empty segment"));
		}
		segments.push(segment);
		match after.strip_prefix('.') {
			Some(next) => rest = next,
			None if after.is_empty() => return Ok(segments),
			None => return Err(format!("{key:?} has text after a quoted segment")),
		}
	}
}

/// `value`, refused unless it can name a package: ASCII letters, digits, `-`
/// and `_`.
fn identifier(field: &str, value: &str) -> SurrealismResult<String> {
	if value.is_empty()
		|| !value.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
	{
		return Err(invalid(format!(
			"{field} {value:?} must be ASCII letters, digits, `-` or `_`"
		)));
	}
	Ok(value.to_string())
}

/// The `[capabilities]` a package asks for, changed through validating
/// methods.
pub struct CapabilitiesMut<'a>(&'a mut SurrealismCapabilities);

impl CapabilitiesMut<'_> {
	/// Ask for network target `target`, such as `api.color.io:443` or
	/// `10.0.0.0/8`. Returns whether it was not asked for already.
	pub fn add_net(&mut self, target: &str) -> SurrealismResult<bool> {
		crate::net_allow::NetPolicy::parse(&[target.to_string()], crate::NetDeny::Nothing)
			.map_err(|e| invalid(format!("{target:?} is not a network target: {e}")))?;
		if self.0.allow_net.iter().any(|t| t == target) {
			return Ok(false);
		}
		self.0.allow_net.push(target.to_string());
		Ok(true)
	}

	/// Stop asking for network target `target`. Returns whether it was asked
	/// for.
	pub fn remove_net(&mut self, target: &str) -> bool {
		let before = self.0.allow_net.len();
		self.0.allow_net.retain(|t| t != target);
		self.0.allow_net.len() != before
	}

	/// Ask for function `pattern`: `*`, a family such as `http` or
	/// `http::*`, or one function such as `fn::lookup`. Returns whether it
	/// was not asked for already.
	pub fn add_function(&mut self, pattern: &str) -> SurrealismResult<bool> {
		if pattern == "*" {
			let changed = self.0.allow_functions != FunctionTargets::All;
			self.0.allow_functions = FunctionTargets::All;
			return Ok(changed);
		}
		let segments: Vec<&str> = pattern.split("::").collect();
		let ident =
			|s: &&str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_');
		let valid = match segments.split_last() {
			Some((&"*", family)) => !family.is_empty() && family.iter().all(ident),
			_ => segments.iter().all(ident),
		};
		if !valid {
			return Err(invalid(format!("{pattern:?} is not a function or a function family")));
		}
		match &mut self.0.allow_functions {
			FunctionTargets::All => Ok(false),
			FunctionTargets::Some(patterns) if patterns.iter().any(|p| p == pattern) => Ok(false),
			FunctionTargets::Some(patterns) => {
				patterns.push(pattern.to_string());
				Ok(true)
			}
			FunctionTargets::None => {
				self.0.allow_functions = FunctionTargets::Some(vec![pattern.to_string()]);
				Ok(true)
			}
		}
	}

	/// Stop asking for function `pattern`; `*` stops asking for all of them.
	/// Returns whether it was asked for.
	pub fn remove_function(&mut self, pattern: &str) -> bool {
		match &mut self.0.allow_functions {
			FunctionTargets::All if pattern == "*" => {
				self.0.allow_functions = FunctionTargets::None;
				true
			}
			FunctionTargets::Some(patterns) => {
				let before = patterns.len();
				patterns.retain(|p| p != pattern);
				let removed = patterns.len() != before;
				if patterns.is_empty() {
					self.0.allow_functions = FunctionTargets::None;
				}
				removed
			}
			FunctionTargets::None | FunctionTargets::All => false,
		}
	}

	pub fn set_queries(&mut self, queries: bool) {
		self.0.allow_arbitrary_queries = queries;
	}

	pub fn set_scripting(&mut self, scripting: bool) {
		self.0.allow_scripting = scripting;
	}

	pub fn set_write(&mut self, write: bool) {
		self.0.allow_write = write;
	}

	pub fn set_strict_timeout(&mut self, strict: bool) {
		self.0.strict_timeout = strict;
	}

	/// The most linear memory one invocation may use, at least one 64 KiB
	/// WebAssembly page; `None` leaves the server's ceiling.
	pub fn set_max_memory(&mut self, bytes: Option<usize>) -> SurrealismResult<()> {
		if let Some(bytes) = bytes
			&& bytes < WASM_PAGE
		{
			return Err(invalid(format!(
				"capabilities.max_memory_bytes {bytes} is less than one WebAssembly page \
				 ({WASM_PAGE} bytes)"
			)));
		}
		self.0.max_memory_bytes = bytes;
		Ok(())
	}

	/// The longest one invocation may run, at least a millisecond; `None`
	/// leaves the server's ceiling.
	pub fn set_max_execution_time(&mut self, time: Option<Duration>) -> SurrealismResult<()> {
		if let Some(time) = time
			&& time.as_millis() == 0
		{
			return Err(invalid("capabilities.max_execution_time must be at least 1 ms"));
		}
		self.0.max_execution_time = time;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::config::{SigstoreSigner, SshSigner};

	/// A package file written by hand: comments, blank lines, key order and an
	/// inline table that a fresh serialisation would not reproduce.
	const HAND_WRITTEN: &str = r#"# The colour extension.
abi = 4

[package]
name = "color"          # the name users type
organisation = "acme"
version = "1.2.0"

# What it needs at run time.
[capabilities]
allow_net = [ "api.color.io:443" ]   # the colour API
allow_functions = ["http::get"]

[attach]
fs = "fs"
"#;

	fn project(text: &str) -> tempfile::TempDir {
		let dir = tempfile::tempdir().unwrap();
		std::fs::write(dir.path().join(PACKAGE_FILE), text).unwrap();
		dir
	}

	fn saved(dir: &tempfile::TempDir) -> String {
		std::fs::read_to_string(dir.path().join(PACKAGE_FILE)).unwrap()
	}

	#[test]
	fn saving_without_a_change_rewrites_the_same_bytes() {
		let dir = project(HAND_WRITTEN);
		PackageConfig::open(dir.path()).unwrap().save().unwrap();
		assert_eq!(saved(&dir), HAND_WRITTEN);
	}

	#[test]
	fn one_change_touches_only_its_key() {
		let dir = project(HAND_WRITTEN);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.set_version("1.3.0").unwrap();
		package.save().unwrap();
		assert_eq!(saved(&dir), HAND_WRITTEN.replace("\"1.2.0\"", "\"1.3.0\""));

		// A second save without a change keeps the first save's text.
		let mut reopened = PackageConfig::open(dir.path()).unwrap();
		reopened.save().unwrap();
		assert_eq!(saved(&dir), HAND_WRITTEN.replace("\"1.2.0\"", "\"1.3.0\""));
	}

	#[test]
	fn a_changed_value_keeps_its_comment() {
		let dir = project(HAND_WRITTEN);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.set_name("colour").unwrap();
		package.save().unwrap();
		assert_eq!(saved(&dir), HAND_WRITTEN.replace("\"color\"", "\"colour\""));
	}

	#[test]
	fn a_list_change_rewrites_only_that_list() {
		let dir = project(HAND_WRITTEN);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		assert!(package.capabilities_mut().add_net("10.0.0.0/8").unwrap());
		assert!(!package.capabilities_mut().add_net("10.0.0.0/8").unwrap());
		package.save().unwrap();
		let text = saved(&dir);
		assert!(
			text.contains("allow_net = [\"api.color.io:443\", \"10.0.0.0/8\"]   # the colour API"),
			"{text}"
		);
		assert!(text.contains("# What it needs at run time.\n[capabilities]"), "{text}");
		assert!(text.contains("name = \"color\"          # the name users type"), "{text}");

		let mut package = PackageConfig::open(dir.path()).unwrap();
		assert!(package.capabilities_mut().remove_net("10.0.0.0/8"));
		package.save().unwrap();
		assert!(saved(&dir).contains("allow_net = [\"api.color.io:443\"]   # the colour API"));
	}

	#[test]
	fn a_new_table_is_added_and_a_removed_one_goes() {
		let dir = project(HAND_WRITTEN);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		let signer = PackageSigner::Ssh(SshSigner {
			public_key: SshSigner::normalise("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA== me@host")
				.unwrap(),
		});
		package.set_signer(Some(signer.clone())).unwrap();
		package.save().unwrap();
		let text = saved(&dir);
		assert!(text.starts_with(HAND_WRITTEN), "{text}");
		assert!(
			text.ends_with(
				"\n[signer.ssh]\npublic_key = \"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIA==\"\n"
			),
			"{text}"
		);
		assert_eq!(PackageConfig::open(dir.path()).unwrap().config().signer, Some(signer));

		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.set_signer(None).unwrap();
		package.save().unwrap();
		assert_eq!(saved(&dir), HAND_WRITTEN);
	}

	#[test]
	fn mounts_switch_between_the_string_and_the_table_form() {
		let dir = project(HAND_WRITTEN);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.add_fs("../models", "models/").unwrap();
		package.save().unwrap();
		let reopened = PackageConfig::open(dir.path()).unwrap();
		let mounts: Vec<_> = reopened.config().attach.fs.as_ref().unwrap().iter().collect();
		assert_eq!(mounts, [("/", "fs"), ("/models", "../models")]);

		let mut package = reopened;
		assert!(package.remove_fs("/models").unwrap());
		package.save().unwrap();
		assert!(saved(&dir).ends_with("[attach]\nfs = \"fs\"\n"), "{}", saved(&dir));
		assert!(package.add_fs("x", "/../etc").is_err());
	}

	#[test]
	fn mutators_refuse_what_cannot_be_written() {
		let dir = project(HAND_WRITTEN);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		assert!(package.set_version("one").is_err());
		assert!(package.set_name("").is_err());
		assert!(package.set_organisation("a b").is_err());
		assert!(package.capabilities_mut().add_net("not a target").is_err());
		assert!(package.capabilities_mut().add_function("http::").is_err());
		assert!(package.capabilities_mut().add_function("http::*").unwrap());
		assert!(package.capabilities_mut().add_function("fn::lookup").unwrap());
		assert!(package.capabilities_mut().remove_function("http::get"));
		package.capabilities_mut().set_write(true);
		package.capabilities_mut().set_queries(true);
		package.capabilities_mut().set_max_execution_time(Some(Duration::from_secs(5))).unwrap();
		assert!(package.capabilities_mut().set_max_execution_time(Some(Duration::ZERO)).is_err());
		assert!(package.capabilities_mut().set_max_memory(Some(0)).is_err());
		assert!(package.capabilities_mut().set_max_memory(Some(1024)).is_err());
		package.capabilities_mut().set_max_memory(Some(64 << 20)).unwrap();
		package.save().unwrap();
		let caps = PackageConfig::open(dir.path()).unwrap().config().capabilities.clone();
		assert!(caps.allow_write && caps.allow_arbitrary_queries);
		assert!(caps.allow_functions.allows("http::post"));
		assert_eq!(caps.max_execution_time, Some(Duration::from_secs(5)));
	}

	#[test]
	fn a_sigstore_signer_must_name_no_ref() {
		let mut signer = SigstoreSigner {
			issuer: "https://token.actions.githubusercontent.com".into(),
			identity: "https://github.com/acme/color/.github/workflows/release.yml".into(),
			repository_id: Some("812345678".into()),
			owner_id: Some("98765432".into()),
		};
		assert!(PackageSigner::Sigstore(signer.clone()).validate().is_ok());
		signer.identity.push_str("@refs/heads/main");
		assert!(PackageSigner::Sigstore(signer.clone()).validate().is_err());
		signer.identity = "https://github.com/acme/color/.github/workflows/release.yml".into();
		signer.owner_id = Some("acme".into());
		assert!(PackageSigner::Sigstore(signer).validate().is_err());
		assert!(
			PackageConfig::parse(
				"[package]\norganisation = \"a\"\nname = \"b\"\nversion = \"1.0.0\"\n\
				 [signer.ssh]\npublic_key = \"rsa nope\"\n"
			)
			.is_err()
		);
	}

	#[test]
	fn a_mount_is_added_to_a_file_without_attach() {
		let dir = project("[package]\norganisation = \"a\"\nname = \"b\"\nversion = \"1.0.0\"\n");
		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.add_fs("../models", "/models").unwrap();
		package
			.capabilities_mut()
			.set_max_execution_time(Some(Duration::from_millis(1500)))
			.unwrap();
		package.save().unwrap();
		let reopened = PackageConfig::open(dir.path()).unwrap();
		assert_eq!(reopened.config(), package.config(), "{:?}", saved(&dir));
		assert!(
			saved(&dir).ends_with("[attach.fs]\n\"/models\" = \"../models\"\n"),
			"{:?}",
			saved(&dir)
		);
	}

	/// Inline tables keep every key the change does not touch, and a table
	/// the change adds inside one is inline too.
	#[test]
	fn inline_tables_are_changed_key_by_key() {
		const INLINE: &str = r#"package = { organisation = "acme", name = "color", version = "1.0.0" }
capabilities = { allow_net = ["api.color.io:443"], allow_write = true }
attach = { fs = { "/" = "fs", "/models" = "../m" } }
"#;
		let dir = project(INLINE);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.add_fs("../data", "/data").unwrap();
		package.set_version("1.1.0").unwrap();
		package.capabilities_mut().add_net("10.0.0.0/8").unwrap();
		package.save().unwrap();

		let text = saved(&dir);
		let reopened = PackageConfig::open(dir.path()).unwrap();
		assert_eq!(reopened.config(), package.config(), "{text}");
		let mounts: Vec<_> = reopened.config().attach.fs.as_ref().unwrap().iter().collect();
		assert_eq!(mounts, [("/", "fs"), ("/data", "../data"), ("/models", "../m")], "{text}");
		assert!(
			text.contains("organisation = \"acme\", name = \"color\", version = \"1.1.0\""),
			"{text}"
		);
		assert!(text.contains("allow_write = true"), "{text}");
		assert_eq!(text.lines().count(), 3, "every table stays inline: {text}");
	}

	/// A mount the author wrote as `models/` is the mount `/models`: changing
	/// or removing it changes that key, not a second one beside it.
	#[test]
	fn a_mount_is_found_whatever_its_spelling() {
		let dir = project(&HAND_WRITTEN.replace(
			"[attach]\nfs = \"fs\"\n",
			"[attach.fs]\n\"/\" = \"fs\"\n\"models/\" = \"../m\"\n",
		));
		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.add_fs("../models", "/models").unwrap();
		package.save().unwrap();
		let text = saved(&dir);
		assert!(text.contains("\"models/\" = \"../models\""), "{text}");
		assert!(!text.contains("\"/models\""), "no second key: {text}");

		let mut package = PackageConfig::open(dir.path()).unwrap();
		assert!(package.remove_fs("models").unwrap());
		package.save().unwrap();
		let reopened = PackageConfig::open(dir.path()).unwrap();
		let mounts: Vec<_> = reopened.config().attach.fs.as_ref().unwrap().iter().collect();
		assert_eq!(mounts, [("/", "fs")], "{}", saved(&dir));
	}

	#[test]
	fn get_reads_quoted_segments_and_renders_tables_under_their_path() {
		let dir = project(HAND_WRITTEN);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.add_fs("../m", "/models.v2").unwrap();
		assert_eq!(package.get("attach.fs.\"/models.v2\"").unwrap().as_deref(), Some("../m"));
		assert_eq!(package.get("package.name").unwrap().as_deref(), Some("color"));
		let table = package.get("package").unwrap().unwrap();
		assert!(table.starts_with("[package]\n"), "{table}");
		let nested = package.get("attach.fs").unwrap().unwrap();
		assert!(nested.starts_with("[attach.fs]\n"), "{nested}");
		assert_eq!(package.get("package.nope").unwrap(), None);
		assert!(package.get("attach.\"fs").is_err());
	}

	/// A save renames its temporary file over the package file, leaving
	/// nothing else behind, and a new package file must have a valid name.
	#[test]
	fn save_replaces_the_file_whole() {
		let dir = project(HAND_WRITTEN);
		let mut package = PackageConfig::open(dir.path()).unwrap();
		package.set_version("2.0.0").unwrap();
		package.save().unwrap();
		let names: Vec<_> = std::fs::read_dir(dir.path())
			.unwrap()
			.map(|e| e.unwrap().file_name().into_string().unwrap())
			.collect();
		assert_eq!(names, [PACKAGE_FILE]);
		assert!(
			PackageConfig::new(
				dir.path(),
				SurrealismMeta {
					organisation: "a b".into(),
					name: "c".into(),
					version: Version::new(1, 0, 0),
					repository: None,
				},
			)
			.is_err()
		);
	}

	/// A signer kind or mount form a later tool writes does not stop a server
	/// loading the archive; the tools that use them still refuse it.
	#[test]
	fn the_loader_ignores_tool_sections_it_cannot_read() {
		let text = "[package]\norganisation = \"a\"\nname = \"b\"\nversion = \"1.0.0\"\n\
			[signer.future]\nfield = 1\n[attach]\nfs = 5\n";
		let config = PackageConfig::for_loader(text).unwrap();
		assert_eq!(config.meta.name, "b");
		assert_eq!(config.signer, None);
		assert!(PackageConfig::parse(text).is_err());

		// A readable one is kept, and is not validated.
		let text = "[package]\norganisation = \"a\"\nname = \"b\"\nversion = \"1.0.0\"\n\
			[signer.ssh]\npublic_key = \"rsa not-a-key\"\n";
		assert!(PackageConfig::for_loader(text).unwrap().signer.is_some());
		assert!(PackageConfig::for_loader("[package]\nname = 1\n").is_err());
	}

	#[test]
	fn a_new_package_file_is_written_in_full() {
		let dir = tempfile::tempdir().unwrap();
		let mut package = PackageConfig::new(
			dir.path(),
			SurrealismMeta {
				organisation: "acme".into(),
				name: "color".into(),
				version: Version::new(1, 0, 0),
				repository: None,
			},
		)
		.unwrap();
		package.save().unwrap();
		let reopened = PackageConfig::open(dir.path()).unwrap();
		assert_eq!(reopened.config(), package.config());
		assert_eq!(reopened.config().abi, AbiVersion::LATEST);
		assert_eq!(saved(&dir), package.config().to_toml().unwrap());
	}
}
