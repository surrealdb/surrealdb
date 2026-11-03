//! Lowering of a module definition's `DIGEST`, `SIGNER`, `CAPABILITIES` and
//! `BINDINGS` clauses.
//!
//! The parser takes `SIGNER`, `CAPABILITIES` and `BINDINGS` as expressions;
//! here each must be an object literal. `SIGNER` and `CAPABILITIES` hold
//! literal values with exactly the documented fields; `BINDINGS` holds any
//! expression under any name.
//! The checks are syntactic: a well-formed `SIGNER` says nothing about whether
//! any archive satisfies it.

use std::time::Duration;

use surrealdb_parser_next::ast::{Ast, Node, NodeId, Tag};
use surrealdb_parser_next::decode;
use surrealdb_parser_next::lex::decode_duration;

use super::{Lower, R, Reject};
use crate::expr::extension::{check_ssh_key_line, is_base64};
use crate::expr::{
	CapabilityTargets, ExtensionBinding, ExtensionBindings, ExtensionCapabilities, ExtensionSigner,
	ManifestDigest, SigstoreSigner,
};

const SIGSTORE_FIELDS: &[&str] =
	&["ca", "log", "tsa", "issuer", "identity", "repository_id", "owner_id"];

/// An object literal's entries: each key with its span and value node.
struct Entries(Vec<(String, (u32, u32), NodeId)>);

impl Entries {
	/// Takes the value of `key`, if present.
	fn take(&mut self, key: &str) -> Option<NodeId> {
		let at = self.0.iter().position(|(k, _, _)| k == key)?;
		Some(self.0.swap_remove(at).2)
	}
}

fn reject(n: &Node, msg: impl Into<std::borrow::Cow<'static, str>>) -> Reject {
	Reject::new(msg).at(n.start, n.end)
}

impl Lower<'_> {
	/// `DIGEST "sha256:…"`, from the clause's string node.
	pub(super) fn digest(&self, id: NodeId) -> R<ManifestDigest> {
		let n = self.n(id);
		let text = self.lit_str(id, "the digest")?;
		ManifestDigest::parse(&text).ok_or_else(|| {
			reject(n, "Invalid module digest")
				.with_label("Expected `sha256:` followed by 64 lowercase hexadecimal digits")
		})
	}

	/// `SIGNER { sigstore: { … } }` or `SIGNER { key: "…" }`.
	pub(super) fn signer(&self, id: NodeId) -> R<ExtensionSigner> {
		let n = self.n(id);
		let mut entries = self.lit_object(id, "`SIGNER`")?;
		let one_of = "Expected exactly one of `sigstore` and `key`";
		if entries.0.len() != 1 {
			return Err(reject(n, "Invalid `SIGNER`").with_label(one_of));
		}
		if let Some(key) = entries.take("key") {
			let line = self.lit_str(key, "`key`")?;
			check_ssh_key_line(&line)
				.map_err(|why| reject(self.n(key), "Invalid `SIGNER` key").with_label(why))?;
			return Ok(ExtensionSigner::Key(line));
		}
		let Some(sigstore) = entries.take("sigstore") else {
			let (key, (start, end), _) = &entries.0[0];
			return Err(Reject::new(format!("Unknown field `{key}` in `SIGNER`"))
				.at(*start, *end)
				.with_label(one_of));
		};
		let mut fields = self.lit_object(sigstore, "`sigstore`")?;
		let sigstore_node = self.n(sigstore);
		let mut required = |name: &'static str| {
			fields.take(name).ok_or_else(|| {
				reject(sigstore_node, format!("Missing field `{name}` in `SIGNER {{ sigstore }}`"))
			})
		};
		let ca = required("ca")?;
		let log = required("log")?;
		let issuer = required("issuer")?;
		let identity = required("identity")?;
		let tsa = required("tsa")?;
		let repository_id = fields.take("repository_id");
		let owner_id = fields.take("owner_id");
		if let Some((key, (start, end), _)) = fields.0.first() {
			return Err(Reject::new(format!("Unknown field `{key}` in `SIGNER {{ sigstore }}`"))
				.at(*start, *end)
				.with_label(format!("Expected one of {}", quoted_list(SIGSTORE_FIELDS))));
		}
		let ca_list = self.base64_list(ca, "`ca`")?;
		if ca_list.is_empty() {
			return Err(reject(self.n(ca), "Invalid `ca`")
				.with_label("Expected at least one certificate authority certificate"));
		}
		let tsa_list = self.base64_list(tsa, "`tsa`")?;
		if tsa_list.is_empty() {
			return Err(reject(self.n(tsa), "Invalid `tsa`").with_label(
				"Expected at least one timestamp authority certificate: Rekor v2 records no time, so the signing time comes from an RFC 3161 timestamp",
			));
		}
		Ok(ExtensionSigner::Sigstore(SigstoreSigner {
			ca: ca_list,
			log: self.base64(log, "`log`")?,
			tsa: tsa_list,
			issuer: self.non_empty_str(issuer, "`issuer`")?,
			identity: self.non_empty_str(identity, "`identity`")?,
			repository_id: repository_id
				.map(|v| self.numeric_id(v, "`repository_id`"))
				.transpose()?,
			owner_id: owner_id.map(|v| self.numeric_id(v, "`owner_id`")).transpose()?,
		}))
	}

	/// `BINDINGS { name: expression, … }`, in the order written. Which names
	/// the extension declares is checked against its archive, not here.
	pub(super) fn bindings(&self, id: NodeId) -> R<ExtensionBindings> {
		let entries = self.lit_object(id, "`BINDINGS`")?;
		let mut out = Vec::with_capacity(entries.0.len());
		for (name, _, value) in entries.0 {
			out.push(ExtensionBinding {
				name,
				value: self.expr(value)?,
			});
		}
		Ok(ExtensionBindings(out))
	}

	/// `CAPABILITIES { … }`. Absent fields keep their defaults, which grant
	/// nothing.
	pub(super) fn capabilities(&self, id: NodeId) -> R<ExtensionCapabilities> {
		let mut entries = self.lit_object(id, "`CAPABILITIES`")?;
		let mut caps = ExtensionCapabilities::default();
		if let Some(v) = entries.take("net") {
			caps.net = self.targets(v, "`net`")?;
		}
		if let Some(v) = entries.take("functions") {
			caps.functions = self.targets(v, "`functions`")?;
		}
		if let Some(v) = entries.take("queries") {
			caps.queries = self.lit_bool(v, "`queries`")?;
		}
		if let Some(v) = entries.take("scripting") {
			caps.scripting = self.lit_bool(v, "`scripting`")?;
		}
		if let Some(v) = entries.take("write") {
			caps.write = self.lit_bool(v, "`write`")?;
		}
		if let Some(v) = entries.take("memory") {
			let n = self.n(v);
			if n.tag != Tag::Int || Ast::int(n) <= 0 {
				return Err(reject(n, "Invalid `memory`")
					.with_label("Expected a positive integer number of bytes"));
			}
			caps.memory = Some(Ast::int(n) as u64);
		}
		if let Some(v) = entries.take("timeout") {
			let n = self.n(v);
			let invalid =
				|| reject(n, "Invalid `timeout`").with_label("Expected a positive duration");
			if n.tag != Tag::Duration {
				return Err(invalid());
			}
			let (s, ns) = decode_duration(self.text(v).as_bytes()).ok_or_else(invalid)?;
			let d = Duration::new(s, ns);
			if d.is_zero() {
				return Err(invalid());
			}
			caps.timeout = Some(d.into());
		}
		if let Some(v) = entries.take("strict_timeout") {
			caps.strict_timeout = self.lit_bool(v, "`strict_timeout`")?;
		}
		if let Some((key, (start, end), _)) = entries.0.first() {
			return Err(Reject::new(format!("Unknown field `{key}` in `CAPABILITIES`"))
				.at(*start, *end)
				.with_label(format!(
					"Expected one of {}",
					quoted_list(ExtensionCapabilities::FIELDS)
				)));
		}
		Ok(caps)
	}

	/// The entries of the object literal `id`, refusing anything else and any
	/// key given twice.
	fn lit_object(&self, id: NodeId, what: &'static str) -> R<Entries> {
		let n = self.n(id);
		if n.tag != Tag::Object {
			return Err(reject(n, format!("Expected an object for {what}")));
		}
		let mut out: Vec<(String, (u32, u32), NodeId)> = Vec::new();
		for kv in self.list(n.a).chunks(2) {
			let k = self.n(kv[0]);
			let key = self.name(kv[0])?;
			if out.iter().any(|(seen, _, _)| *seen == key) {
				return Err(
					Reject::new(format!("Duplicate field `{key}` in {what}")).at(k.start, k.end)
				);
			}
			out.push((key, (k.start, k.end), kv[1]));
		}
		Ok(Entries(out))
	}

	/// The plain string literal `id`.
	fn lit_str(&self, id: NodeId, what: &'static str) -> R<String> {
		let n = self.n(id);
		if n.tag != Tag::Str || !matches!(n.op, 0 | b's') {
			return Err(reject(n, format!("Expected a string for {what}")));
		}
		let mut s = String::new();
		if !decode::unescape(decode::strip_delims(self.text(id)), &mut s) {
			return Err(Reject::new("invalid escape"));
		}
		Ok(s)
	}

	fn non_empty_str(&self, id: NodeId, what: &'static str) -> R<String> {
		let s = self.lit_str(id, what)?;
		if s.is_empty() {
			return Err(reject(self.n(id), format!("Expected a non-empty string for {what}")));
		}
		Ok(s)
	}

	/// A decimal ID, kept as text so no width is assumed.
	fn numeric_id(&self, id: NodeId, what: &'static str) -> R<String> {
		let s = self.lit_str(id, what)?;
		if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
			return Err(reject(
				self.n(id),
				format!("Expected a string of decimal digits for {what}"),
			));
		}
		Ok(s)
	}

	fn base64(&self, id: NodeId, what: &'static str) -> R<String> {
		let s = self.lit_str(id, what)?;
		if !is_base64(&s) {
			return Err(reject(self.n(id), format!("Expected standard base64 for {what}")));
		}
		Ok(s)
	}

	fn base64_list(&self, id: NodeId, what: &'static str) -> R<Vec<String>> {
		let n = self.n(id);
		if n.tag != Tag::Array {
			return Err(reject(n, format!("Expected an array of base64 strings for {what}")));
		}
		self.list(n.a).iter().map(|&e| self.base64(e, what)).collect()
	}

	fn lit_bool(&self, id: NodeId, what: &'static str) -> R<bool> {
		let n = self.n(id);
		if n.tag == Tag::True {
			Ok(true)
		} else if n.tag == Tag::False {
			Ok(false)
		} else {
			Err(reject(n, format!("Expected `true` or `false` for {what}")))
		}
	}

	/// `true`, `false`, or an array of non-empty strings.
	fn targets(&self, id: NodeId, what: &'static str) -> R<CapabilityTargets> {
		let n = self.n(id);
		if n.tag == Tag::True {
			Ok(CapabilityTargets::All)
		} else if n.tag == Tag::False {
			Ok(CapabilityTargets::None)
		} else if n.tag == Tag::Array {
			let list = self
				.list(n.a)
				.iter()
				.map(|&e| self.non_empty_str(e, what))
				.collect::<R<Vec<_>>>()?;
			Ok(if list.is_empty() {
				CapabilityTargets::None
			} else {
				CapabilityTargets::Some(list)
			})
		} else {
			Err(reject(n, format!("Expected `true`, `false` or an array of strings for {what}")))
		}
	}
}

fn quoted_list(names: &[&str]) -> String {
	names.iter().map(|n| format!("`{n}`")).collect::<Vec<_>>().join(", ")
}
