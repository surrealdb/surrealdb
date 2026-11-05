//! The trust and capability clauses of an extension definition.
//!
//! ```surql
//! DEFINE MODULE mod::color FROM f"modules:/color.surli"
//!     DIGEST "sha256:5d1a…"
//!     SIGNER { key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w… release@acme" }
//!     CAPABILITIES { net: ["api.color.io:443"], functions: ["http::get"] };
//! ```
//!
//! `DIGEST` pins the code, `SIGNER` names the anchor and identity rule a
//! signature over that code must satisfy (`UNSIGNED` waives the check), and
//! `CAPABILITIES` is everything the extension may do. These types hold what the
//! statement says after a syntactic check; whether a signature satisfies a
//! `SIGNER` is decided where archives are verified, not here.

use std::fmt::{self, Display};

use common::fmt::{EscapeObjectKey, QuoteStr};
use revision::revisioned;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::statements::info::InfoStructure;
use crate::val::stored::schema::Stored;
use crate::val::{Duration, Value};

/// `DIGEST "sha256:<64 lowercase hex digits>"`: the hash that pins an
/// extension's code.
///
/// A definition records it once, when it is written, so the capabilities
/// approved in the same statement always belong to exactly that code.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum ManifestDigest {
	/// A SHA-256 digest. Always 32 bytes: [`ManifestDigest::parse`] and
	/// [`ManifestDigest::sha256`] are the only constructors that callers use.
	#[stored(n = 1)]
	Sha256(Vec<u8>),
}

impl ManifestDigest {
	const SHA256_PREFIX: &'static str = "sha256:";

	pub fn sha256(bytes: [u8; 32]) -> Self {
		ManifestDigest::Sha256(bytes.to_vec())
	}

	/// Parses the textual form, `sha256:` followed by exactly 64 lowercase
	/// hexadecimal digits. Any other text is `None`.
	pub fn parse(text: &str) -> Option<Self> {
		let hex = text.strip_prefix(Self::SHA256_PREFIX)?;
		if hex.len() != 64 || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
			return None;
		}
		hex::decode(hex).ok().map(ManifestDigest::Sha256)
	}
}

impl Display for ManifestDigest {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			ManifestDigest::Sha256(bytes) => {
				write!(f, "{}{}", Self::SHA256_PREFIX, hex::encode(bytes))
			}
		}
	}
}

impl ToSql for ManifestDigest {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		QuoteStr(&self.to_string()).fmt_sql(f, fmt)
	}
}

/// `SIGNER { … }`: the trust anchor a signature must chain to, and for a
/// certificate authority the identity rule the certificate must match.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum ExtensionSigner {
	/// `{ sigstore: { … } }`.
	#[stored(n = 1)]
	Sigstore(SigstoreSigner),
	/// `{ key: "<type> <base64 key> [comment]" }`: an SSH public key line, kept
	/// as written. The key is the whole anchor, so there is no identity rule.
	#[stored(n = 2)]
	Key(String),
}

/// A Sigstore anchor and identity rule.
///
/// Every certificate and key is standard base64 of its DER encoding, as
/// Sigstore's trust root distributes them.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct SigstoreSigner {
	/// The certificate authority (Fulcio) chain: root first, then any
	/// intermediates. Never empty.
	#[stored(n = 2)]
	pub ca: Vec<String>,
	/// The transparency log's (Rekor's) public key, which signs the
	/// checkpoints that prove a signature was logged.
	#[stored(n = 4)]
	pub log: String,
	/// The RFC 3161 timestamp authority chain that dates the signature: its
	/// signing certificate and the certificates above it. Never empty: Rekor v2
	/// records no time, so a trusted timestamp is the only trusted signing
	/// time.
	#[stored(n = 6)]
	pub tsa: Vec<String>,
	/// The OIDC issuer the certificate must name.
	#[stored(n = 8)]
	pub issuer: String,
	/// The certificate's subject alternative name, such as a workflow URI.
	#[stored(n = 10)]
	pub identity: String,
	/// The source repository's numeric ID the certificate must carry (OID
	/// 1.3.6.1.4.1.57264.1.15), which unlike its name cannot be reused.
	#[stored(n = 12)]
	pub repository_id: Option<String>,
	/// The repository owner's numeric ID the certificate must carry (OID
	/// 1.3.6.1.4.1.57264.1.17).
	#[stored(n = 14)]
	pub owner_id: Option<String>,
}

fn fmt_str_list(f: &mut String, fmt: SqlFormat, list: &[String]) {
	f.push('[');
	for (i, s) in list.iter().enumerate() {
		if i > 0 {
			f.push_str(", ");
		}
		QuoteStr(s).fmt_sql(f, fmt);
	}
	f.push(']');
}

impl ToSql for SigstoreSigner {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push_str("{ ca: ");
		fmt_str_list(f, fmt, &self.ca);
		write_sql!(f, fmt, ", log: {}", QuoteStr(&self.log));
		if !self.tsa.is_empty() {
			f.push_str(", tsa: ");
			fmt_str_list(f, fmt, &self.tsa);
		}
		write_sql!(f, fmt, ", issuer: {}", QuoteStr(&self.issuer));
		write_sql!(f, fmt, ", identity: {}", QuoteStr(&self.identity));
		if let Some(id) = &self.repository_id {
			write_sql!(f, fmt, ", repository_id: {}", QuoteStr(id));
		}
		if let Some(id) = &self.owner_id {
			write_sql!(f, fmt, ", owner_id: {}", QuoteStr(id));
		}
		f.push_str(" }");
	}
}

impl ToSql for ExtensionSigner {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			ExtensionSigner::Sigstore(s) => {
				f.push_str("{ sigstore: ");
				s.fmt_sql(f, fmt);
				f.push_str(" }");
			}
			ExtensionSigner::Key(k) => {
				f.push_str("{ key: ");
				QuoteStr(k).fmt_sql(f, fmt);
				f.push_str(" }");
			}
		}
	}
}

impl InfoStructure for ExtensionSigner {
	fn structure(self) -> Value {
		let strs = |v: Vec<String>| Value::from(v.into_iter().map(Value::from).collect::<Vec<_>>());
		match self {
			ExtensionSigner::Sigstore(s) => Value::from(map! {
				"sigstore" => Value::from(map! {
					"ca" => strs(s.ca),
					"log" => s.log.into(),
					"tsa" => strs(s.tsa),
					"issuer" => s.issuer.into(),
					"identity" => s.identity.into(),
					"repository_id", if let Some(v) = s.repository_id => v.into(),
					"owner_id", if let Some(v) = s.owner_id => v.into(),
				}),
			}),
			ExtensionSigner::Key(k) => Value::from(map! {
				"key" => k.into(),
			}),
		}
	}
}

/// Which targets a capability grants.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub enum CapabilityTargets {
	/// `false`, `[]` or absent: nothing.
	#[default]
	#[stored(n = 1)]
	None,
	/// `true`: whatever the server allows.
	#[stored(n = 2)]
	All,
	/// `["…", …]`: these targets, as far as the server allows each of them.
	#[stored(n = 3)]
	Some(Vec<String>),
}

impl CapabilityTargets {
	fn is_none(&self) -> bool {
		matches!(self, CapabilityTargets::None)
	}
}

impl ToSql for CapabilityTargets {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			CapabilityTargets::None => f.push_str("false"),
			CapabilityTargets::All => f.push_str("true"),
			CapabilityTargets::Some(list) => fmt_str_list(f, fmt, list),
		}
	}
}

impl InfoStructure for CapabilityTargets {
	fn structure(self) -> Value {
		match self {
			CapabilityTargets::None => Value::Bool(false),
			CapabilityTargets::All => Value::Bool(true),
			CapabilityTargets::Some(list) => {
				Value::from(list.into_iter().map(Value::from).collect::<Vec<_>>())
			}
		}
	}
}

/// `CAPABILITIES { … }`: everything an extension may do.
///
/// The default, which `CAPABILITIES {}` and an absent clause both produce,
/// grants nothing and enforces timeouts. What an invocation may actually do is
/// this intersected with the server's own capabilities.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Stored)]
#[stored(since = "4.0")]
pub struct ExtensionCapabilities {
	/// Hosts and ports the extension may connect to.
	#[stored(n = 2)]
	pub net: CapabilityTargets,
	/// SurrealDB functions the extension may call, such as `"http::get"` or
	/// `"http::*"`.
	#[stored(n = 4)]
	pub functions: CapabilityTargets,
	/// Whether the extension may run arbitrary SurrealQL text.
	#[stored(n = 6)]
	pub queries: bool,
	/// Whether the extension may use embedded scripting.
	#[stored(n = 8)]
	pub scripting: bool,
	/// Whether the extension's functions that are marked as writing may write.
	#[stored(n = 10)]
	pub write: bool,
	/// Maximum linear memory per invocation, in bytes. `None` leaves the
	/// server's ceiling.
	#[stored(n = 12)]
	pub memory: Option<u64>,
	/// Maximum duration of one invocation. `None` leaves the server's ceiling.
	#[stored(n = 14)]
	pub timeout: Option<Duration>,
	/// `false` compiles the extension without timeout checks, so an invocation
	/// can run forever.
	#[stored(n = 16)]
	pub strict_timeout: bool,
}

impl Default for ExtensionCapabilities {
	fn default() -> Self {
		ExtensionCapabilities {
			net: CapabilityTargets::None,
			functions: CapabilityTargets::None,
			queries: false,
			scripting: false,
			write: false,
			memory: None,
			timeout: None,
			strict_timeout: true,
		}
	}
}

impl ExtensionCapabilities {
	/// The field names `CAPABILITIES` accepts, in rendering order.
	pub const FIELDS: &'static [&'static str] = &[
		"net",
		"functions",
		"queries",
		"scripting",
		"write",
		"memory",
		"timeout",
		"strict_timeout",
	];
}

/// Writes the fields that differ from the default, so `CAPABILITIES {}`
/// renders as written.
impl ToSql for ExtensionCapabilities {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let mut first = true;
		let mut sep = |f: &mut String| {
			f.push_str(if first {
				" "
			} else {
				", "
			});
			first = false;
		};
		f.push('{');
		if !self.net.is_none() {
			sep(f);
			write_sql!(f, fmt, "net: {}", self.net);
		}
		if !self.functions.is_none() {
			sep(f);
			write_sql!(f, fmt, "functions: {}", self.functions);
		}
		if self.queries {
			sep(f);
			f.push_str("queries: true");
		}
		if self.scripting {
			sep(f);
			f.push_str("scripting: true");
		}
		if self.write {
			sep(f);
			f.push_str("write: true");
		}
		if let Some(memory) = self.memory {
			sep(f);
			f.push_str("memory: ");
			f.push_str(&memory.to_string());
		}
		if let Some(timeout) = &self.timeout {
			sep(f);
			write_sql!(f, fmt, "timeout: {}", timeout);
		}
		if !self.strict_timeout {
			sep(f);
			f.push_str("strict_timeout: false");
		}
		if first {
			f.push('}');
		} else {
			f.push_str(" }");
		}
	}
}

/// Every field, defaults included, so the structure shows exactly what is
/// granted.
impl InfoStructure for ExtensionCapabilities {
	fn structure(self) -> Value {
		Value::from(map! {
			"net" => self.net.structure(),
			"functions" => self.functions.structure(),
			"queries" => self.queries.into(),
			"scripting" => self.scripting.into(),
			"write" => self.write.into(),
			"memory" => self.memory.map(Value::from).unwrap_or(Value::None),
			"timeout" => self.timeout.map(Value::Duration).unwrap_or(Value::None),
			"strict_timeout" => self.strict_timeout.into(),
		})
	}
}

/// Whether `text` is standard, padded base64 (RFC 4648 §4) with at least one
/// character. Only the alphabet and padding are checked, not what the bytes
/// encode.
pub fn is_base64(text: &str) -> bool {
	let bytes = text.as_bytes();
	if bytes.is_empty() || !bytes.len().is_multiple_of(4) {
		return false;
	}
	let body = bytes.len() - bytes.iter().rev().take_while(|&&b| b == b'=').count();
	if bytes.len() - body > 2 {
		return false;
	}
	bytes[..body].iter().all(|&b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

/// The SSH public key types `SIGNER { key }` accepts.
pub const SSH_KEY_TYPES: &[&str] = &["ssh-ed25519", "ecdsa-sha2-nistp256"];

/// Checks that `line` has the shape of an SSH public key line,
/// `<type> <base64 key> [comment]`, with a type from [`SSH_KEY_TYPES`].
/// Returns why it does not.
pub fn check_ssh_key_line(line: &str) -> Result<(), &'static str> {
	let mut parts = line.splitn(3, ' ');
	let kind = parts.next().unwrap_or_default();
	if !SSH_KEY_TYPES.contains(&kind) {
		return Err("The key type must be `ssh-ed25519` or `ecdsa-sha2-nistp256`");
	}
	match parts.next() {
		Some(key) if is_base64(key) => Ok(()),
		_ => Err("The key type must be followed by a space and the base64 public key"),
	}
}

/// `BINDINGS { name: expression, … }`: values an extension reads by name.
///
/// Each expression is evaluated when the extension first asks for its name
/// during an invocation, with permissions off and the caller's system level
/// capped at the definer's, so a definer can hand the extension a value the
/// caller may not read — a secret, a row of a locked table. The caller's own
/// parameters are not in scope. The extension's `package.toml` declares the
/// names and their kinds; the definition must give every name whose kind is
/// not `option<…>`, and no name the package does not declare.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ExtensionBindings(pub Vec<ExtensionBinding>);

/// One entry of [`ExtensionBindings`].
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExtensionBinding {
	pub name: String,
	pub value: crate::expr::Expr,
}

impl ExtensionBindings {
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// The expression bound to `name`, if the definition binds it.
	pub fn get(&self, name: &str) -> Option<&crate::expr::Expr> {
		self.0.iter().find(|b| b.name == name).map(|b| &b.value)
	}

	pub fn names(&self) -> impl Iterator<Item = &str> {
		self.0.iter().map(|b| b.name.as_str())
	}
}

impl ToSql for ExtensionBindings {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.0.is_empty() {
			f.push_str("{}");
			return;
		}
		f.push_str("{ ");
		for (i, binding) in self.0.iter().enumerate() {
			if i > 0 {
				f.push_str(", ");
			}
			write_sql!(
				f,
				fmt,
				"{}: {}",
				EscapeObjectKey(&binding.name),
				crate::expr::CoverStmts(&binding.value)
			);
		}
		f.push_str(" }");
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn digest_text_round_trips() {
		let text = format!("sha256:{}", "ab".repeat(32));
		let digest = ManifestDigest::parse(&text).unwrap();
		assert_eq!(digest.to_string(), text);
		assert_eq!(digest, ManifestDigest::sha256([0xab; 32]));
	}

	#[test]
	fn digest_refuses_other_forms() {
		for text in [
			"",
			"sha256:",
			&format!("sha256:{}", "AB".repeat(32)),
			&format!("sha256:{}", "ab".repeat(31)),
			&format!("sha512:{}", "ab".repeat(32)),
			&format!("SHA256:{}", "ab".repeat(32)),
			&format!("sha256:{} ", "ab".repeat(32)),
		] {
			assert!(ManifestDigest::parse(text).is_none(), "{text}");
		}
	}

	#[test]
	fn base64_shapes() {
		assert!(is_base64("AAAA"));
		assert!(is_base64("AA=="));
		assert!(is_base64("A+/="));
		assert!(!is_base64(""));
		assert!(!is_base64("AAA"));
		assert!(!is_base64("A==="));
		assert!(!is_base64("AA-_"));
		assert!(!is_base64("A=AA"));
	}

	#[test]
	fn ssh_key_lines() {
		check_ssh_key_line("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w release@acme").unwrap();
		check_ssh_key_line("ecdsa-sha2-nistp256 AAAA").unwrap();
		check_ssh_key_line("ssh-rsa AAAA").unwrap_err();
		check_ssh_key_line("ssh-ed25519").unwrap_err();
		check_ssh_key_line("ssh-ed25519  AAAA").unwrap_err();
	}

	#[test]
	fn empty_capabilities_render_as_written() {
		assert_eq!(ExtensionCapabilities::default().to_sql(), "{}");
		let caps = ExtensionCapabilities {
			net: CapabilityTargets::Some(vec!["api.color.io:443".into()]),
			functions: CapabilityTargets::All,
			memory: Some(1024),
			strict_timeout: false,
			..Default::default()
		};
		assert_eq!(
			caps.to_sql(),
			"{ net: ['api.color.io:443'], functions: true, memory: 1024, strict_timeout: false }"
		);
	}
}
