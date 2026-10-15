//! The `SIGNER` clause: a trust anchor plus, for Sigstore, an identity rule.
//!
//! The client writes what the verifier proved into the statement, shows it in
//! the prompt as a label and fingerprint, and compares it on upgrade.

use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use sha2::{Digest as _, Sha256};
use surrealdb_types::Value;

use super::sql::quote;

/// A recorded signer. `None` in an `Option<Signer>` is `UNSIGNED`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Signer {
	Sigstore(SigstoreSigner),
	/// An SSH public key line: `<type> <base64 blob> [comment]`.
	Key(String),
}

/// A Sigstore anchor (`ca`, `log`) with the identity rule it vouches for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigstoreSigner {
	/// Base64 DER certificates of the Fulcio chain, root first.
	pub ca: Vec<String>,
	/// Base64 DER (SPKI) of the Rekor log's public key.
	pub log: String,
	/// Base64 DER certificates of the RFC 3161 timestamp authority; empty when
	/// the trust root names none.
	pub tsa: Vec<String>,
	/// The OIDC issuer the certificate records.
	pub issuer: String,
	/// The certificate's SAN, e.g.
	/// `https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0`.
	pub identity: String,
	/// Certificate extension 1.3.6.1.4.1.57264.1.15.
	pub repository_id: Option<String>,
	/// Certificate extension 1.3.6.1.4.1.57264.1.17.
	pub owner_id: Option<String>,
}

/// How a client names an anchor to the person approving it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnchorLabel {
	/// One of the entries of Sigstore's public-good root, checked online.
	PublicGood,
	/// Checked online and NOT in Sigstore's public-good root: a private
	/// Sigstore, or one that a statement names.
	CustomCa,
	/// A Sigstore anchor whose origin could not be checked online.
	Unchecked,
	/// A key anchor, which vouches only for itself.
	Key,
}

/// The result of comparing an installed signer with a new one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SignerMatch {
	Same,
	BothUnsigned,
	/// Every reason the two differ, for the warning.
	Different(Vec<String>),
}

impl Signer {
	/// The clause as SurrealQL, without the `SIGNER` keyword.
	pub fn to_sql(&self) -> String {
		match self {
			Signer::Key(key) => format!("{{ key: {} }}", quote(key)),
			Signer::Sigstore(s) => {
				let list = |items: &[String]| {
					items.iter().map(|c| quote(c)).collect::<Vec<_>>().join(", ")
				};
				let mut fields =
					vec![format!("ca: [{}]", list(&s.ca)), format!("log: {}", quote(&s.log))];
				if !s.tsa.is_empty() {
					fields.push(format!("tsa: [{}]", list(&s.tsa)));
				}
				fields.push(format!("issuer: {}", quote(&s.issuer)));
				fields.push(format!("identity: {}", quote(&s.identity)));
				if let Some(id) = &s.repository_id {
					fields.push(format!("repository_id: {}", quote(id)));
				}
				if let Some(id) = &s.owner_id {
					fields.push(format!("owner_id: {}", quote(id)));
				}
				format!("{{ sigstore: {{ {} }} }}", fields.join(", "))
			}
		}
	}

	/// The verifier's form: every certificate and key decoded from the base64
	/// the clause holds, as the server decodes it.
	pub fn to_verifier(&self) -> Result<surrealism_verify::Signer> {
		let der = |field: &str, value: &str| {
			STANDARD
				.decode(value)
				.with_context(|| format!("SIGNER field `{field}` is not valid base64"))
		};
		let ders = |field: &str, values: &[String]| {
			values.iter().map(|v| der(field, v)).collect::<Result<Vec<_>>>()
		};
		Ok(match self {
			Signer::Key(key) => surrealism_verify::Signer::Key(key.clone()),
			Signer::Sigstore(s) => {
				surrealism_verify::Signer::Sigstore(surrealism_verify::SigstoreSigner {
					ca: ders("ca", &s.ca)?,
					log: der("log", &s.log)?,
					tsa: ders("tsa", &s.tsa)?,
					issuer: s.issuer.clone(),
					identity: s.identity.clone(),
					repository_id: s.repository_id.clone(),
					owner_id: s.owner_id.clone(),
				})
			}
		})
	}

	/// Reads a `SIGNER` object as `INFO … STRUCTURE` returns it. `NONE` is
	/// `UNSIGNED`.
	pub fn from_value(value: &Value) -> Result<Option<Signer>> {
		let obj = match value {
			Value::None | Value::Null => return Ok(None),
			Value::Object(obj) => obj,
			_ => bail!("expected a SIGNER object"),
		};
		if let Some(Value::String(key)) = obj.get("key") {
			return Ok(Some(Signer::Key(key.clone())));
		}
		let Some(Value::Object(s)) = obj.get("sigstore") else {
			bail!("a SIGNER object needs `key` or `sigstore`");
		};
		let text = |k: &str| -> Result<Option<String>> {
			match s.get(k) {
				None | Some(Value::None) | Some(Value::Null) => Ok(None),
				Some(Value::String(v)) => Ok(Some(v.clone())),
				Some(_) => bail!("SIGNER field `{k}` must be a string"),
			}
		};
		let required = |k: &str| -> Result<String> {
			text(k)?.with_context(|| format!("SIGNER is missing `sigstore.{k}`"))
		};
		let certs = |k: &str| -> Result<Option<Vec<String>>> {
			match s.get(k) {
				None | Some(Value::None) | Some(Value::Null) => Ok(None),
				Some(Value::Array(items)) => items
					.iter()
					.map(|v| match v {
						Value::String(c) => Ok(c.clone()),
						_ => bail!("SIGNER field `{k}` must be an array of strings"),
					})
					.collect::<Result<Vec<_>>>()
					.map(Some),
				Some(_) => bail!("SIGNER field `{k}` must be an array of strings"),
			}
		};
		let ca = certs("ca")?.context("SIGNER is missing `sigstore.ca`")?;
		let tsa = certs("tsa")?.unwrap_or_default();
		Ok(Some(Signer::Sigstore(SigstoreSigner {
			ca,
			log: required("log")?,
			tsa,
			issuer: required("issuer")?,
			identity: required("identity")?,
			repository_id: text("repository_id")?,
			owner_id: text("owner_id")?,
		})))
	}
}

pub(crate) use crate::cli::module::sign::signer::identity_without_ref;

/// The key type and blob of an SSH public key line, without its comment.
pub(super) fn key_type_and_blob(key: &str) -> (&str, &str) {
	let mut parts = key.split_whitespace();
	(parts.next().unwrap_or(""), parts.next().unwrap_or(""))
}

/// Whether two certificate lists hold the same certificates. They form a
/// set: their order says nothing.
pub(super) fn same_certs(a: &[String], b: &[String]) -> bool {
	fn set(v: &[String]) -> std::collections::BTreeSet<&str> {
		v.iter().map(String::as_str).collect()
	}
	a.len() == b.len() && set(a) == set(b)
}

/// Compares two recorded signers as SSH compares host keys.
///
/// Sigstore signers are the same when anchor (`ca` and `log`), issuer, both
/// numeric IDs, and the identity without its `@<ref>` suffix are all equal.
/// Key signers are the same when type and blob are equal; the comment is
/// ignored.
pub fn compare(installed: Option<&Signer>, new: Option<&Signer>) -> SignerMatch {
	match (installed, new) {
		(None, None) => SignerMatch::BothUnsigned,
		(None, Some(_)) => SignerMatch::Different(vec!["the installed version is unsigned".into()]),
		(Some(_), None) => SignerMatch::Different(vec!["the new version is UNSIGNED".into()]),
		(Some(Signer::Key(a)), Some(Signer::Key(b))) => {
			if key_type_and_blob(a) == key_type_and_blob(b) {
				SignerMatch::Same
			} else {
				SignerMatch::Different(vec!["a different signing key".into()])
			}
		}
		(Some(Signer::Sigstore(a)), Some(Signer::Sigstore(b))) => {
			let mut reasons = Vec::new();
			if !same_certs(&a.ca, &b.ca) || a.log != b.log || !same_certs(&a.tsa, &b.tsa) {
				reasons.push("a different trust anchor".into());
			}
			if a.issuer != b.issuer {
				reasons.push(format!("issuer {} → {}", a.issuer, b.issuer));
			}
			if a.repository_id != b.repository_id {
				reasons.push(format!(
					"repository ID {} → {}",
					a.repository_id.as_deref().unwrap_or("none"),
					b.repository_id.as_deref().unwrap_or("none")
				));
			}
			if a.owner_id != b.owner_id {
				reasons.push(format!(
					"owner ID {} → {}",
					a.owner_id.as_deref().unwrap_or("none"),
					b.owner_id.as_deref().unwrap_or("none")
				));
			}
			let (ia, ib) = (identity_without_ref(&a.identity), identity_without_ref(&b.identity));
			if ia != ib {
				reasons.push(format!("identity {ia} → {ib}"));
			}
			if reasons.is_empty() {
				SignerMatch::Same
			} else {
				SignerMatch::Different(reasons)
			}
		}
		_ => SignerMatch::Different(vec!["one is a Sigstore signer, the other a key".into()]),
	}
}

/// `sha256:` and the first 12 hex digits of the SHA-256 of base64 DER bytes.
pub fn der_fingerprint(b64: &str) -> String {
	match STANDARD.decode(b64.trim()) {
		Ok(der) => short_sha256(&der),
		Err(_) => "sha256:<invalid base64>".into(),
	}
}

fn short_sha256(bytes: &[u8]) -> String {
	let digest = hex::encode(Sha256::digest(bytes));
	format!("sha256:{}…", &digest[..12])
}

/// The fingerprint `ssh-keygen -l` prints for a key line.
pub fn ssh_key_fingerprint(key: &str) -> String {
	let (_, blob) = key_type_and_blob(key);
	match STANDARD.decode(blob) {
		Ok(blob) => format!("SHA256:{}", STANDARD_NO_PAD.encode(Sha256::digest(blob))),
		Err(_) => "SHA256:<invalid key>".into(),
	}
}

/// The prompt's `Signed by` value, e.g.
/// `github.com/acme/color  (.github/workflows/release.yml @ v1.0.0)`.
pub fn describe_identity(identity: &str) -> String {
	let Some(rest) = identity.strip_prefix("https://") else {
		return identity.to_string();
	};
	let (path, git_ref) = match rest.rfind('@') {
		Some(i) if rest[..i].contains('/') => (&rest[..i], Some(&rest[i + 1..])),
		_ => (rest, None),
	};
	// GitLab separates the project from the CI file with `//`; GitHub
	// identities put the workflow under `.github/workflows/`.
	let split = path
		.find("//")
		.map(|i| (&path[..i], &path[i + 2..]))
		.or_else(|| path.find("/.github/").map(|i| (&path[..i], &path[i + 1..])));
	let short_ref = git_ref.map(|r| {
		r.strip_prefix("refs/tags/").or_else(|| r.strip_prefix("refs/heads/")).unwrap_or(r)
	});
	match (split, short_ref) {
		(Some((repo, file)), Some(r)) => format!("{repo}  ({file} @ {r})"),
		(Some((repo, file)), None) => format!("{repo}  ({file})"),
		(None, Some(r)) => format!("{path}  (@ {r})"),
		(None, None) => path.to_string(),
	}
}

/// The repository URL that a Sigstore identity names, for the comparison with
/// `package.toml`'s `repository`.
pub fn identity_repository(identity: &str) -> Option<String> {
	let rest = identity.strip_prefix("https://")?;
	let path = identity_without_ref(rest);
	let repo = path
		.find("//")
		.map(|i| &path[..i])
		.or_else(|| path.find("/.github/").map(|i| &path[..i]))?;
	Some(format!("https://{repo}"))
}

/// `--trust-signer`: the signer the person accepts without being asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TrustSigner {
	/// `any`: whichever signer the signature proves.
	Any,
	/// An SSH key, by the fingerprint the prompt shows (`SHA256:…`).
	Key(String),
	/// A Sigstore identity (the workflow URL; a ref, `@refs/…`, is matched
	/// only when given), and optionally its OIDC issuer after a space.
	Sigstore {
		identity: String,
		issuer: Option<String>,
	},
}

impl std::str::FromStr for TrustSigner {
	type Err = String;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let s = s.trim();
		if s == "any" {
			return Ok(Self::Any);
		}
		if s.starts_with("SHA256:") {
			return Ok(Self::Key(s.to_string()));
		}
		let (identity, issuer) = match s.split_once(char::is_whitespace) {
			Some((identity, issuer)) => (identity, Some(issuer.trim().to_string())),
			None => (s, None),
		};
		if !identity.contains("://") && !identity.contains('@') {
			return Err(format!(
				"`{s}` is not a signer: give `any`, an SSH key fingerprint (SHA256:…), or a Sigstore \
				 identity such as https://github.com/acme/color/.github/workflows/release.yml, \
				 optionally followed by its issuer"
			));
		}
		Ok(Self::Sigstore {
			identity: identity.to_string(),
			issuer,
		})
	}
}

impl TrustSigner {
	/// Whether `proved` is the signer this names.
	pub fn accepts(&self, proved: &Signer) -> bool {
		match (self, proved) {
			(Self::Any, _) => true,
			(Self::Key(fingerprint), Signer::Key(key)) => ssh_key_fingerprint(key) == *fingerprint,
			(
				Self::Sigstore {
					identity,
					issuer,
				},
				Signer::Sigstore(s),
			) => {
				// A ref given must match exactly; without one, any ref of the
				// workflow does.
				let same = if identity_without_ref(identity) == identity {
					identity_without_ref(&s.identity) == identity
				} else {
					s.identity == *identity
				};
				same && issuer.as_ref().is_none_or(|i| *i == s.issuer)
			}
			_ => false,
		}
	}
}

/// A proven signer in one line, for errors.
pub fn describe_signer(signer: &Signer) -> String {
	match signer {
		Signer::Key(key) => format!("key {}", ssh_key_fingerprint(key)),
		Signer::Sigstore(s) => format!("{} (issuer {})", s.identity, s.issuer),
	}
}

/// A `[signer]` from `package.toml` in one line, for errors and the prompt.
pub fn describe_declared(declared: &surrealism_runtime::config::PackageSigner) -> String {
	use surrealism_runtime::config::PackageSigner;
	match declared {
		PackageSigner::Sigstore(s) => describe_identity(&s.identity),
		PackageSigner::Ssh(s) => format!("key {}", ssh_key_fingerprint(&s.public_key)),
	}
}

/// The prompt lines for a signer, each `(label, value)`.
pub fn prompt_rows(signer: Option<&Signer>, label: AnchorLabel) -> Vec<(&'static str, String)> {
	match signer {
		None => vec![(
			"Signed by",
			"⚠ UNSIGNED (--unsigned): no signature is verified; DIGEST still pins the code".into(),
		)],
		Some(Signer::Key(key)) => {
			let (kind, _) = key_type_and_blob(key);
			vec![
				("Signed by", format!("key {kind} {}", ssh_key_fingerprint(key))),
				("Anchor", "the key itself (confirm its fingerprint with the publisher)".into()),
			]
		}
		Some(Signer::Sigstore(s)) => {
			let ca = s.ca.first().map(|c| der_fingerprint(c)).unwrap_or_else(|| "none".into());
			let log = der_fingerprint(&s.log);
			let anchor = match label {
				AnchorLabel::PublicGood => {
					format!("Sigstore public-good   (ca {ca}, log {log})")
				}
				AnchorLabel::Unchecked => format!(
					"⚠ not checked online  (ca {ca}, log {log}) — may not be Sigstore's public root"
				),
				_ => format!(
					"✗ custom CA            (ca {ca}, log {log}) — not Sigstore's public root"
				),
			};
			let mut rows = vec![("Signed by", describe_identity(&s.identity))];
			let mut ids = Vec::new();
			if let Some(id) = &s.repository_id {
				ids.push(format!("repository {id}"));
			}
			if let Some(id) = &s.owner_id {
				ids.push(format!("owner {id}"));
			}
			if !ids.is_empty() {
				rows.push(("IDs", ids.join(", ")));
			}
			rows.push(("Issuer", s.issuer.clone()));
			rows.push(("Anchor", anchor));
			rows
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const GH: &str = "https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0";

	fn sigstore(identity: &str) -> Signer {
		Signer::Sigstore(SigstoreSigner {
			ca: vec!["Y2Ffcm9vdA==".into(), "Y2FfaW50".into()],
			log: "bG9nX2tleQ==".into(),
			tsa: vec!["dHNh".into()],
			issuer: "https://token.actions.githubusercontent.com".into(),
			identity: identity.into(),
			repository_id: Some("812345678".into()),
			owner_id: Some("98765432".into()),
		})
	}

	fn with(f: impl FnOnce(&mut SigstoreSigner)) -> Signer {
		let Signer::Sigstore(mut s) = sigstore(GH) else {
			unreachable!()
		};
		f(&mut s);
		Signer::Sigstore(s)
	}

	#[test]
	fn strips_the_ref_suffix() {
		assert_eq!(
			identity_without_ref(GH),
			"https://github.com/acme/color/.github/workflows/release.yml"
		);
		assert_eq!(
			identity_without_ref("https://gitlab.com/acme/color//.gitlab-ci.yml@refs/heads/main"),
			"https://gitlab.com/acme/color//.gitlab-ci.yml"
		);
		// No ref, no path, or an e-mail identity: unchanged.
		assert_eq!(identity_without_ref("https://example.com/a/b"), "https://example.com/a/b");
		assert_eq!(identity_without_ref("https://user@example.com"), "https://user@example.com");
		assert_eq!(identity_without_ref("release@acme.com"), "release@acme.com");
		// A ref may itself hold `@`: the suffix starts at the first one.
		assert_eq!(
			identity_without_ref("https://github.com/a/b/.github/workflows/x.yml@refs/heads/fix@2"),
			"https://github.com/a/b/.github/workflows/x.yml"
		);
	}

	#[test]
	fn same_signer_across_tags() {
		let a = sigstore(GH);
		let b = sigstore(
			"https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.1.0",
		);
		assert_eq!(compare(Some(&a), Some(&b)), SignerMatch::Same);
	}

	#[test]
	fn different_signer_on_each_field() {
		let a = sigstore(GH);
		let cases = [
			(
				sigstore(
					"https://github.com/acm3/color/.github/workflows/release.yml@refs/tags/v1.2.0",
				),
				"identity",
			),
			(
				sigstore(
					"https://github.com/acme/color/.github/workflows/other.yml@refs/tags/v1.0.0",
				),
				"identity",
			),
			(with(|s| s.issuer = "https://gitlab.com".into()), "issuer"),
			(with(|s| s.repository_id = Some("1".into())), "repository ID"),
			(with(|s| s.owner_id = None), "owner ID"),
			(with(|s| s.log = "b3RoZXI=".into()), "trust anchor"),
			(with(|s| s.tsa.clear()), "trust anchor"),
			(with(|s| s.ca.pop().map(drop).unwrap_or(())), "trust anchor"),
		];
		for (b, reason) in cases {
			match compare(Some(&a), Some(&b)) {
				SignerMatch::Different(reasons) => assert!(
					reasons.iter().any(|r| r.contains(reason)),
					"expected `{reason}` in {reasons:?}"
				),
				other => panic!("expected a different signer for `{reason}`, got {other:?}"),
			}
		}
	}

	#[test]
	fn trust_signer_names_one_signer() {
		let any: TrustSigner = "any".parse().unwrap();
		assert!(any.accepts(&sigstore(GH)));
		let workflow: TrustSigner =
			"https://github.com/acme/color/.github/workflows/release.yml".parse().unwrap();
		assert!(workflow.accepts(&sigstore(GH)), "any ref of the workflow");
		assert!(!workflow.accepts(&sigstore(
			"https://github.com/acm3/color/.github/workflows/release.yml@refs/tags/v1"
		)));
		let pinned: TrustSigner =
			format!("{GH} https://token.actions.githubusercontent.com").parse().unwrap();
		assert!(pinned.accepts(&sigstore(GH)));
		let other_issuer: TrustSigner = format!("{GH} https://gitlab.com").parse().unwrap();
		assert!(!other_issuer.accepts(&sigstore(GH)));
		let key = Signer::Key("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w".into());
		let by_print: TrustSigner =
			ssh_key_fingerprint("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w").parse().unwrap();
		assert!(by_print.accepts(&key));
		assert!(!by_print.accepts(&sigstore(GH)));
		assert!("color".parse::<TrustSigner>().is_err());
	}

	#[test]
	fn certificate_order_does_not_change_the_signer() {
		let a = sigstore(GH);
		let b = with(|s| {
			s.ca.reverse();
		});
		assert_eq!(compare(Some(&a), Some(&b)), SignerMatch::Same);
	}

	#[test]
	fn renamed_repository_with_same_ids_is_still_different() {
		// The name in the identity moved while the numeric IDs stayed: the
		// identity rule is part of the decision, so it is a different signer.
		let b =
			sigstore("https://github.com/acme/colour/.github/workflows/release.yml@refs/tags/v2");
		assert!(matches!(compare(Some(&sigstore(GH)), Some(&b)), SignerMatch::Different(_)));
	}

	#[test]
	fn key_comparison_ignores_the_comment() {
		let a = Signer::Key("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w release@acme".into());
		let b = Signer::Key("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w laptop".into());
		let c = Signer::Key("ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIZZZ release@acme".into());
		assert_eq!(compare(Some(&a), Some(&b)), SignerMatch::Same);
		assert!(matches!(compare(Some(&a), Some(&c)), SignerMatch::Different(_)));
		assert!(matches!(compare(Some(&a), Some(&sigstore(GH))), SignerMatch::Different(_)));
	}

	#[test]
	fn unsigned_transitions() {
		let a = sigstore(GH);
		assert_eq!(compare(None, None), SignerMatch::BothUnsigned);
		assert!(matches!(compare(Some(&a), None), SignerMatch::Different(_)));
		assert!(matches!(compare(None, Some(&a)), SignerMatch::Different(_)));
	}

	#[test]
	fn renders_and_reads_back() {
		let s = sigstore(GH);
		assert_eq!(
			s.to_sql(),
			format!(
				r#"{{ sigstore: {{ ca: ['Y2Ffcm9vdA==', 'Y2FfaW50'], log: 'bG9nX2tleQ==', tsa: ['dHNh'], issuer: 'https://token.actions.githubusercontent.com', identity: '{GH}', repository_id: '812345678', owner_id: '98765432' }} }}"#
			)
		);
		let v = crate::core::syn::value(&s.to_sql()).expect("valid SurrealQL");
		assert_eq!(Signer::from_value(&v).expect("reads back"), Some(s));
		let k = Signer::Key("ssh-ed25519 AAAA release@acme".into());
		assert_eq!(k.to_sql(), r#"{ key: 'ssh-ed25519 AAAA release@acme' }"#);
		assert_eq!(Signer::from_value(&Value::None).expect("NONE"), None);
	}

	#[test]
	fn describes_identities() {
		assert_eq!(
			describe_identity(GH),
			"github.com/acme/color  (.github/workflows/release.yml @ v1.0.0)"
		);
		assert_eq!(
			describe_identity("https://gitlab.com/acme/color//.gitlab-ci.yml@refs/tags/v1.0.0"),
			"gitlab.com/acme/color  (.gitlab-ci.yml @ v1.0.0)"
		);
		assert_eq!(describe_identity("release@acme.com"), "release@acme.com");
		assert_eq!(identity_repository(GH).as_deref(), Some("https://github.com/acme/color"));
	}

	#[test]
	fn fingerprints() {
		// "Y2Ffcm9vdA==" is base64 for "ca_root".
		let expected = format!("sha256:{}…", &hex::encode(Sha256::digest(b"ca_root"))[..12]);
		assert_eq!(der_fingerprint("Y2Ffcm9vdA=="), expected);
		assert!(ssh_key_fingerprint("ssh-ed25519 AAAA x").starts_with("SHA256:"));
	}
}
