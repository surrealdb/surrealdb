//! `surrealism_verify::verify` against `sigstore-verify`, the reference
//! verifier, over every Sigstore fixture and a set of edits of them.
//!
//! Both must reach the same verdict. Where the two policies differ by design,
//! [`pre_checks`] applies this crate's stricter rule before the bundle reaches
//! `sigstore-verify`. A verifier built on `sigstore-verify` must keep every one
//! of them to accept exactly what this crate accepts:
//!
//! 1. A DSSE envelope is refused. `sigstore-verify` accepts one.
//! 2. A log entry other than `hashedrekord` 0.0.2 (Rekor v2) is refused. `sigstore-verify` accepts
//!    Rekor v1 entries too.
//! 3. Line breaks inside a base64 field are removed before decoding, as the protobuf JSON decoders
//!    that write bundles do. `sigstore-verify` refuses such a field.
//! 4. The leaf certificate holds exactly one subject alternative name, a URI or an email address.
//!    `sigstore-verify` takes the first of several.
//! 5. The issuer is read from extension `1.3.6.1.4.1.57264.1.8` only. `sigstore-verify` falls back
//!    to the legacy `1.3.6.1.4.1.57264.1.1`.
//!
//! The signer's `repository_id` and `owner_id` are not part of a
//! `sigstore-verify` policy; [`reference`] compares them with the
//! certificate's CI claims, as `verify` does: equal, or both absent.
//!
//! No fixture exercises rules 4 and 5, because no fixture's certificate holds
//! several names or only the legacy issuer, and the keys that issued the
//! fixtures no longer exist.

#![cfg(not(target_arch = "wasm32"))]
#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use sigstore_verify::trust_root::TrustedRoot;
use sigstore_verify::types::{Artifact, Bundle, Sha256Hash};
use sigstore_verify::{IdentityMatcher, VerificationPolicy};
use surrealism_verify::{ManifestDigest, Signer, SigstoreSigner, verify};
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::FromDer as _;

fn fixture(name: &str) -> Vec<u8> {
	let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name);
	fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn json(bytes: &[u8]) -> Value {
	serde_json::from_slice(bytes).unwrap()
}

/// A bundle, the digest it is checked against, the signer, and the trusted
/// root that holds the same anchors for `sigstore-verify`.
struct Case {
	name: String,
	digest: ManifestDigest,
	bundle: Value,
	signer: SigstoreSigner,
	root: Value,
}

impl Case {
	fn edited(&self, what: &str, edit: impl FnOnce(&mut Self)) -> Self {
		let mut case = Self {
			name: format!("{} ({what})", self.name),
			digest: self.digest,
			bundle: self.bundle.clone(),
			signer: self.signer.clone(),
			root: self.root.clone(),
		};
		edit(&mut case);
		case
	}

	fn ours(&self) -> bool {
		let bundle = serde_json::to_vec(&self.bundle).unwrap();
		verify(&self.digest, &bundle, &Signer::Sigstore(self.signer.clone())).is_ok()
	}

	fn theirs(&self) -> Result<(), String> {
		let mut bundle = self.bundle.clone();
		pre_checks(&mut bundle)?;
		reference(self, &bundle)
	}
}

// ---------------------------------------------------------------------------
// The pre-checks, and the reference verifier.
// ---------------------------------------------------------------------------

/// The fields of a bundle that hold base64.
const BASE64_FIELDS: &[&[&str]] = &[
	&["verificationMaterial", "certificate", "rawBytes"],
	&["messageSignature", "signature"],
	&["messageSignature", "messageDigest", "digest"],
];
const ENTRY_BASE64_FIELDS: &[&[&str]] =
	&[&["canonicalizedBody"], &["logId", "keyId"], &["inclusionProof", "rootHash"]];

/// This crate's rules that `sigstore-verify` does not apply; see the module
/// documentation.
fn pre_checks(bundle: &mut Value) -> Result<(), String> {
	// 1. DSSE.
	if bundle.get("dsseEnvelope").is_some() {
		return Err("DSSE".into());
	}
	// 2. Rekor v2 only.
	let entries = bundle["verificationMaterial"]["tlogEntries"].as_array_mut();
	for entry in entries.into_iter().flatten() {
		if entry["kindVersion"] != json!({"kind": "hashedrekord", "version": "0.0.2"}) {
			return Err(format!("log entry {}", entry["kindVersion"]));
		}
		// 3. Line breaks in base64, inside the entry.
		for path in ENTRY_BASE64_FIELDS {
			join_lines(at(entry, path));
		}
		for hash in entry["inclusionProof"]["hashes"].as_array_mut().into_iter().flatten() {
			join_lines(hash);
		}
	}
	// 3. Line breaks in base64, elsewhere.
	for path in BASE64_FIELDS {
		join_lines(at(bundle, path));
	}
	let timestamps =
		bundle["verificationMaterial"]["timestampVerificationData"]["rfc3161Timestamps"]
			.as_array_mut();
	for ts in timestamps.into_iter().flatten() {
		join_lines(&mut ts["signedTimestamp"]);
	}
	// 4 and 5. The leaf's names and issuer.
	let Some(leaf) = bundle["verificationMaterial"]["certificate"]["rawBytes"].as_str() else {
		return Err("no leaf certificate".into());
	};
	let der = BASE64.decode(leaf).map_err(|e| format!("leaf: {e}"))?;
	let (_, leaf) = x509_parser::certificate::X509Certificate::from_der(&der)
		.map_err(|e| format!("leaf: {e}"))?;
	let names = leaf.subject_alternative_name().map_err(|e| format!("SAN: {e}"))?;
	match names.map(|san| san.value.general_names.as_slice()) {
		Some([GeneralName::URI(_)] | [GeneralName::RFC822Name(_)]) => {}
		_ => return Err("not exactly one URI or email SAN".into()),
	}
	let issuer = x509_parser::oid_registry::Oid::from(&[1, 3, 6, 1, 4, 1, 57264, 1, 8]).unwrap();
	if !leaf.extensions().iter().any(|ext| ext.oid == issuer) {
		return Err("no issuer extension 1.3.6.1.4.1.57264.1.8".into());
	}
	Ok(())
}

fn at<'a>(mut value: &'a mut Value, path: &[&str]) -> &'a mut Value {
	for key in path {
		value = &mut value[*key];
	}
	value
}

fn join_lines(value: &mut Value) {
	if let Some(text) = value.as_str() {
		*value = text.replace(['\r', '\n'], "").into();
	}
}

/// `sigstore-verify` with the signer's rule as its policy, and the signer's
/// IDs compared with the certificate's CI claims.
fn reference(case: &Case, bundle: &Value) -> Result<(), String> {
	let root = TrustedRoot::from_json(&case.root.to_string()).map_err(|e| e.to_string())?;
	let bundle = Bundle::from_json(&bundle.to_string()).map_err(|e| e.to_string())?;
	// SCTs: a `SIGNER` names no CT log, so this crate checks none.
	let policy = VerificationPolicy::new(
		IdentityMatcher::Exact(case.signer.identity.clone()),
		case.signer.issuer.clone(),
	)
	.skip_sct();
	let artifact = Artifact::from_digest(Sha256Hash::new(case.digest));
	let result =
		sigstore_verify::verify(artifact, &bundle, &policy, &root).map_err(|e| e.to_string())?;
	let claims = &result.certificate().ok_or("no certificate")?.ci_claims;
	if claims.source_repository_identifier != case.signer.repository_id {
		return Err("repository_id".into());
	}
	if claims.source_repository_owner_identifier != case.signer.owner_id {
		return Err("owner_id".into());
	}
	Ok(())
}

// ---------------------------------------------------------------------------
// The fixtures.
// ---------------------------------------------------------------------------

mod conformance {
	pub const DIGEST: &str = "a0cfc71271d6e278e57cd332ff957c3f7043fdda354c4cbb190a30d56efa01bf";
	pub const ISSUER: &str = "https://token.actions.githubusercontent.com";
	pub const IDENTITY: &str = "https://github.com/sigstore-conformance/extremely-dangerous-public-oidc-beacon/.github/workflows/extremely-dangerous-oidc-beacon.yml@refs/heads/main";
	pub const REPOSITORY_ID: &str = "632596897";
	pub const OWNER_ID: &str = "131804563";
	pub const LOG_URL: &str = "https://log2025-alpha1.rekor.sigstage.dev";
}

fn certificates(chain: &Value) -> Vec<Vec<u8>> {
	chain["certChain"]["certificates"]
		.as_array()
		.unwrap()
		.iter()
		.map(|c| BASE64.decode(c["rawBytes"].as_str().unwrap()).unwrap())
		.collect()
}

/// The conformance signer, with anchors from `root` as `tests/conformance.rs`
/// takes them, and `root` reduced to exactly those anchors.
fn conformance_anchors(root: &Value) -> (SigstoreSigner, Value) {
	use conformance::*;
	let ca = root["certificateAuthorities"].as_array().unwrap().last().unwrap();
	let tsa = root["timestampAuthorities"].as_array().unwrap().last().unwrap();
	let log = root["tlogs"].as_array().unwrap().iter().find(|l| l["baseUrl"] == LOG_URL).unwrap();
	let signer = SigstoreSigner {
		ca: certificates(ca),
		log: BASE64.decode(log["publicKey"]["rawBytes"].as_str().unwrap()).unwrap(),
		tsa: certificates(tsa),
		issuer: ISSUER.into(),
		identity: IDENTITY.into(),
		repository_id: Some(REPOSITORY_ID.into()),
		owner_id: Some(OWNER_ID.into()),
	};
	let mut reduced = root.clone();
	reduced["certificateAuthorities"] = json!([ca]);
	reduced["timestampAuthorities"] = json!([tsa]);
	reduced["tlogs"] = json!([log]);
	(signer, reduced)
}

fn conformance_cases() -> Vec<Case> {
	let digest: ManifestDigest = *Sha256Hash::from_hex(conformance::DIGEST).unwrap().as_bytes();
	let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/conformance");
	let mut names: Vec<String> = fs::read_dir(dir)
		.unwrap()
		.map(|e| e.unwrap().file_name().into_string().unwrap())
		.filter_map(|n| n.strip_suffix(".sigstore.json").map(str::to_string))
		.collect();
	names.sort();
	assert!(names.len() >= 20, "conformance fixtures missing: {names:?}");
	names
		.into_iter()
		.map(|name| {
			// The cases about a short-lived TSA come with their own root.
			let root = if name.starts_with("timestamp-with-expired-cert-chain")
				|| name.starts_with("timestamp-outside-tsa-cert-validity")
			{
				"conformance/trusted_root_local_tsa.json"
			} else {
				"conformance/trusted_root.json"
			};
			let (signer, root) = conformance_anchors(&json(&fixture(root)));
			Case {
				bundle: json(&fixture(&format!("conformance/{name}.sigstore.json"))),
				name,
				digest,
				signer,
				root,
			}
		})
		.collect()
}

/// A trusted root that holds exactly the anchors of a private `signer.json`.
/// The log is named by the ID its entries record.
fn private_root(signer: &Value, log_id: &Value) -> Value {
	let certs = |list: &Value| -> Value {
		list.as_array().unwrap().iter().map(|c| json!({ "rawBytes": c })).collect()
	};
	let log = signer["log"].as_str().unwrap();
	// An Ed25519 SubjectPublicKeyInfo is 44 bytes; ECDSA P-256 is 91.
	let key_details = if BASE64.decode(log).unwrap().len() == 44 {
		"PKIX_ED25519"
	} else {
		"PKIX_ECDSA_P256_SHA_256"
	};
	let valid = json!({ "start": "2000-01-01T00:00:00Z" });
	json!({
		"mediaType": "application/vnd.dev.sigstore.trustedroot+json;version=0.1",
		"tlogs": [{
			"baseUrl": "https://log.surrealism.test",
			"hashAlgorithm": "SHA2_256",
			"publicKey": { "rawBytes": log, "keyDetails": key_details, "validFor": valid },
			"logId": log_id,
		}],
		"certificateAuthorities": [{
			"subject": { "organization": "surrealism.test", "commonName": "test-fulcio" },
			"uri": "https://fulcio.surrealism.test",
			"certChain": { "certificates": certs(&signer["ca"]) },
			"validFor": valid,
		}],
		"ctlogs": [],
		"timestampAuthorities": [{
			"subject": { "organization": "surrealism.test", "commonName": "test-tsa" },
			"uri": "https://tsa.surrealism.test",
			"certChain": { "certificates": certs(&signer["tsa"]) },
			"validFor": valid,
		}],
	})
}

fn private_cases() -> Vec<Case> {
	let digest: ManifestDigest = Sha256::digest(fixture("MANIFEST")).into();
	["ecdsa-log", "ed25519-log", "email-identity", "expired"]
		.into_iter()
		.map(|name| {
			let bundle = json(&fixture(&format!("private/{name}/bundle.sigstore.json")));
			let signer_json = json(&fixture(&format!("private/{name}/signer.json")));
			let bytes = |v: &Value| BASE64.decode(v.as_str().unwrap()).unwrap();
			let text = |v: &Value| v.as_str().map(str::to_string);
			let signer = SigstoreSigner {
				ca: signer_json["ca"].as_array().unwrap().iter().map(bytes).collect(),
				log: bytes(&signer_json["log"]),
				tsa: signer_json["tsa"].as_array().unwrap().iter().map(bytes).collect(),
				issuer: text(&signer_json["issuer"]).unwrap(),
				identity: text(&signer_json["identity"]).unwrap(),
				repository_id: text(&signer_json["repository_id"]),
				owner_id: text(&signer_json["owner_id"]),
			};
			let log_id = &bundle["verificationMaterial"]["tlogEntries"][0]["logId"];
			Case {
				root: private_root(&signer_json, log_id),
				name: format!("private/{name}"),
				digest,
				bundle,
				signer,
			}
		})
		.collect()
}

/// Edits of a valid bundle or signer, each of which one rule decides.
fn edits(case: &Case) -> Vec<Case> {
	fn entry(c: &mut Case) -> &mut Value {
		&mut c.bundle["verificationMaterial"]["tlogEntries"][0]
	}
	vec![
		case.edited("tampered digest", |c| c.digest[0] ^= 1),
		case.edited("other identity", |c| c.signer.identity.push('x')),
		case.edited("other issuer", |c| c.signer.issuer = "https://gitlab.com".into()),
		case.edited("other repository_id", |c| c.signer.repository_id = Some("1".into())),
		case.edited("no repository_id", |c| c.signer.repository_id = None),
		case.edited("other owner_id", |c| c.signer.owner_id = Some("1".into())),
		case.edited("base64 with line breaks", |c| {
			let cert = &mut c.bundle["verificationMaterial"]["certificate"]["rawBytes"];
			let text = cert.as_str().unwrap();
			let wrapped: Vec<&str> =
				text.as_bytes().chunks(64).map(|l| std::str::from_utf8(l).unwrap()).collect();
			*cert = wrapped.join("\n").into();
		}),
		case.edited("Rekor v1 entry", |c| entry(c)["kindVersion"]["version"] = "0.0.1".into()),
		case.edited("no timestamp", |c| {
			c.bundle["verificationMaterial"]["timestampVerificationData"]["rfc3161Timestamps"] =
				json!([]);
		}),
		case.edited("no log entry", |c| {
			c.bundle["verificationMaterial"]["tlogEntries"] = json!([])
		}),
		case.edited("tampered checkpoint", |c| {
			let envelope = &mut entry(c)["inclusionProof"]["checkpoint"]["envelope"];
			let text = envelope.as_str().unwrap();
			let mut lines: Vec<String> = text.split('\n').map(str::to_string).collect();
			lines[1] = format!("{}", lines[1].parse::<u64>().unwrap() + 1);
			*envelope = lines.join("\n").into();
		}),
		case.edited("tampered inclusion proof", |c| {
			entry(c)["inclusionProof"]["hashes"][0] = BASE64.encode([0u8; 32]).into();
		}),
		case.edited("other log key", |c| {
			let last = c.signer.log.len() - 1;
			c.signer.log[last] ^= 1;
			let key = &mut c.root["tlogs"][0]["publicKey"]["rawBytes"];
			*key = BASE64.encode(&c.signer.log).into();
		}),
		case.edited("CA anchor is the TSA chain", |c| {
			c.signer.ca = c.signer.tsa.clone();
			c.root["certificateAuthorities"][0]["certChain"] =
				c.root["timestampAuthorities"][0]["certChain"].clone();
		}),
		case.edited("TSA anchor is the CA chain", |c| {
			c.signer.tsa = c.signer.ca.clone();
			c.root["timestampAuthorities"][0]["certChain"] =
				c.root["certificateAuthorities"][0]["certChain"].clone();
		}),
	]
}

// ---------------------------------------------------------------------------
// The comparison.
// ---------------------------------------------------------------------------

fn compare(cases: &[Case]) -> usize {
	let mut accepted = 0;
	let mut disagreements = Vec::new();
	for case in cases {
		let ours = case.ours();
		let theirs = case.theirs();
		if ours != theirs.is_ok() {
			disagreements.push(format!("{}: ours {ours}, reference {theirs:?}", case.name));
		}
		accepted += usize::from(ours);
	}
	assert!(disagreements.is_empty(), "verdicts differ:\n{}", disagreements.join("\n"));
	accepted
}

#[test]
fn conformance_fixtures() {
	let cases = conformance_cases();
	let accepted = compare(&cases);
	// `happy-path`, seven variants and `timestamp-with-expired-cert-chain`;
	// `dsse-happy-path` is refused by rule 1.
	assert_eq!(accepted, 9, "valid conformance bundles");
}

#[test]
fn private_fixtures() {
	let cases = private_cases();
	let accepted = compare(&cases);
	// `expired` is refused.
	assert_eq!(accepted, 3, "valid private bundles");
}

#[test]
fn edited_fixtures() {
	let valid: Vec<Case> = conformance_cases()
		.into_iter()
		.filter(|c| c.name == "happy-path")
		.chain(private_cases().into_iter().filter(|c| c.name != "private/expired"))
		.collect();
	assert_eq!(valid.len(), 4);
	let cases: Vec<Case> = valid.iter().flat_map(edits).collect();
	let accepted = compare(&cases);
	// Each edit but the line breaks is refused, and "no repository_id" is
	// accepted for the email identity, whose certificate carries none.
	assert_eq!(accepted, valid.len() + 1, "accepted edits");
}
