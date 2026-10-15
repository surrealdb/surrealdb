//! Signature verification and the Sigstore trust root.
//!
//! The client runs the same verifier as the server, before it shows the
//! prompt, so the person never approves a signer the server would refuse.
//! The flows use [`claims`] to tell the signature's kind, then either
//! [`verify_envelope`] (the trust root the signature embeds) or
//! [`load_trust_root`] and [`prove_signer`] (a `--trust-root` override). An
//! SSH signature goes through [`prove_signer`] with its own key.

use std::path::Path;
use std::str::FromStr as _;

use anyhow::{Context as _, Result, bail};
use serde::Deserialize;
use surrealism_runtime::package::Digest;
pub use surrealism_verify::{Claims, VerifiedSigner};

use super::signer::{Signer, SigstoreSigner};

/// One Sigstore anchor a signature may verify against. Each list holds base64
/// DER certificates, in any order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigstoreAnchor {
	/// The Fulcio chain: the intermediate that issues signing certificates and
	/// the roots above it.
	pub ca: Vec<String>,
	/// Base64 DER `SubjectPublicKeyInfo` of a Rekor log key.
	pub log: String,
	/// The log's address (`baseUrl`), for the online lookup of the entry.
	/// Not part of the anchor: the statement does not record it.
	pub log_url: Option<String>,
	/// The RFC 3161 timestamp authorities: their signing certificates and
	/// the certificates above them.
	pub tsa: Vec<String>,
	/// When both the CA and the log key became valid, in seconds since the
	/// Unix epoch; `None` when the trust root gives no start.
	pub valid_from: Option<i64>,
	/// When the first of them was retired; `None` while both are current.
	pub valid_until: Option<i64>,
}

impl SigstoreAnchor {
	/// Whether a signature attested at `signed_at` falls in the window.
	pub(super) fn covers(&self, signed_at: i64) -> bool {
		self.valid_from.is_none_or(|from| from <= signed_at)
			&& self.valid_until.is_none_or(|until| signed_at <= until)
	}
}

/// The anchors of one Sigstore instance.
#[derive(Clone, Debug)]
pub struct TrustRoot {
	pub anchors: Vec<SigstoreAnchor>,
}

/// A `trusted_root.json` the person supplies with `--trust-root`, for a
/// private Sigstore or an air-gapped install. It replaces the root the
/// signature envelope embeds.
pub fn load_trust_root(path: &Path) -> Result<TrustRoot> {
	let text = std::fs::read_to_string(path)
		.with_context(|| format!("failed to read trust root {}", path.display()))?;
	parse_trusted_root(&text)
		.with_context(|| format!("{} is not a Sigstore trusted_root.json", path.display()))
}

/// The signer a signature envelope proves offline, from the trust root it
/// embeds.
///
/// `surrealism_verify::verify_offline` checks the bundle against the anchors
/// the envelope carries, with the identity its certificate claims; the
/// returned signer records exactly those, so the statement holds a `SIGNER`
/// the server's own verify accepts. Whether those anchors deserve trust is a
/// separate question: see [`super::online`].
pub fn verify_envelope(digest: &Digest, signature: &[u8]) -> Result<(Signer, VerifiedSigner)> {
	use base64::Engine as _;
	use base64::engine::general_purpose::STANDARD;
	let Claims::Sigstore {
		issuer,
		identity,
		repository_id,
		owner_id,
		anchors,
	} = claims(signature)?
	else {
		bail!("surrealism/SIGNATURE is not a Sigstore signature");
	};
	let Some(anchors) = anchors else {
		bail!(
			"surrealism/SIGNATURE is a bare Sigstore bundle, without the trust root to verify it \
			 against. Sign the archive again with this release's `surreal extension sign`, or pass \
			 --trust-root with the trusted_root.json it was signed under"
		);
	};
	let verified = surrealism_verify::verify_offline(&digest.0, signature).map_err(|e| {
		anyhow::anyhow!(
			"the signature, which claims Sigstore identity {identity} from {issuer}, does not \
			 verify against the trust root it carries: {e}"
		)
	})?;
	let b64 = |der: &Vec<u8>| STANDARD.encode(der);
	let signer = Signer::Sigstore(SigstoreSigner {
		ca: anchors.ca.iter().map(b64).collect(),
		log: STANDARD.encode(&anchors.log),
		tsa: anchors.tsa.iter().map(b64).collect(),
		issuer,
		identity,
		repository_id,
		owner_id,
	});
	Ok((signer, verified))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TrustedRootJson {
	#[serde(default)]
	tlogs: Vec<TlogJson>,
	#[serde(default)]
	certificate_authorities: Vec<ChainJson>,
	#[serde(default)]
	timestamp_authorities: Vec<ChainJson>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TlogJson {
	#[serde(default)]
	base_url: Option<String>,
	public_key: KeyJson,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct KeyJson {
	raw_bytes: String,
	#[serde(default)]
	valid_for: Option<ValidFor>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ChainJson {
	cert_chain: CertChainJson,
	#[serde(default)]
	valid_for: Option<ValidFor>,
}

#[derive(Deserialize)]
struct CertChainJson {
	certificates: Vec<RawBytes>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawBytes {
	raw_bytes: String,
}

#[derive(Deserialize)]
struct ValidFor {
	#[serde(default)]
	start: Option<String>,
	#[serde(default)]
	end: Option<String>,
}

/// An entry's `validFor` as seconds since the Unix epoch, `(start, end)`;
/// either bound may be open.
fn window(valid_for: &Option<ValidFor>) -> Result<(Option<i64>, Option<i64>)> {
	let at = |t: &Option<String>| -> Result<Option<i64>> {
		t.as_deref()
			.map(|t| {
				surrealdb_types::Datetime::from_str(t)
					.map(|d| d.timestamp())
					.map_err(|_| anyhow::anyhow!("`{t}` is not an RFC 3339 time"))
			})
			.transpose()
	};
	match valid_for {
		None => Ok((None, None)),
		Some(v) => Ok((at(&v.start)?, at(&v.end)?)),
	}
}

/// One anchor per (CA chain, log key) pair in a Sigstore `trusted_root.json`,
/// each with the certificates of every timestamp authority.
///
/// Retired entries, which carry a `validFor.end`, are kept: a key that was
/// rotated out still verifies a release signed while it was in use. Each
/// anchor records the window in which both its CA and its log key were
/// valid, and [`prove_signer`] accepts it only for a signature whose
/// attested signing time lies in that window.
pub fn parse_trusted_root(text: &str) -> Result<TrustRoot> {
	let json: TrustedRootJson = serde_json::from_str(text)?;
	let certs = |chain: &ChainJson| -> Vec<String> {
		chain.cert_chain.certificates.iter().map(|c| c.raw_bytes.clone()).collect()
	};
	let tsa: Vec<String> = json.timestamp_authorities.iter().flat_map(certs).collect();
	let mut anchors = Vec::new();
	for ca in &json.certificate_authorities {
		let (ca_from, ca_until) = window(&ca.valid_for)?;
		for tlog in &json.tlogs {
			let (log_from, log_until) = window(&tlog.public_key.valid_for)?;
			let valid_from = ca_from.max(log_from);
			let valid_until = match (ca_until, log_until) {
				(Some(a), Some(b)) => Some(a.min(b)),
				(a, b) => a.or(b),
			};
			// A CA and a log key that were never valid at the same time
			// cannot have produced one signature together.
			if let (Some(from), Some(until)) = (valid_from, valid_until)
				&& from > until
			{
				continue;
			}
			anchors.push(SigstoreAnchor {
				ca: certs(ca),
				log: tlog.public_key.raw_bytes.clone(),
				log_url: tlog.base_url.clone(),
				tsa: tsa.clone(),
				valid_from,
				valid_until,
			});
		}
	}
	if anchors.is_empty() {
		bail!("the trust root holds no certificate authority and log key");
	}
	Ok(TrustRoot {
		anchors,
	})
}

/// Reads the signer a `SIGNATURE` claims to come from, without checking
/// anything. The claims are untrusted until [`prove_signer`] verifies them.
pub fn claims(signature: &[u8]) -> Result<Claims> {
	surrealism_verify::claims(signature)
		.map_err(|e| anyhow::anyhow!("failed to read surrealism/SIGNATURE: {e}"))
}

/// Checks with the server's verifier that `signature` proves `signer` signed
/// `digest`.
pub fn verify_signature(
	digest: &Digest,
	signature: &[u8],
	signer: &Signer,
) -> Result<VerifiedSigner> {
	surrealism_verify::verify(&digest.0, signature, &signer.to_verifier()?)
		.map_err(|e| anyhow::anyhow!("{e}"))
}

/// The signer `signature` proves over `digest`, as a `SIGNER` clause records
/// it.
///
/// The claims name the signer; the anchor comes from the verifying side. An
/// SSH signature's anchor is its own key, which the person confirms by
/// fingerprint. A Sigstore signature's claimed identity is combined with each
/// anchor of `root` in turn, and the first combination that [`verify_signature`]
/// accepts is the signer. So only an anchor that proved the signature reaches
/// a statement, and the server, running the same verifier, accepts it.
/// Returns the signer with what the verification proved, the signing time
/// among it.
pub fn prove_signer(
	digest: &Digest,
	signature: &[u8],
	root: Option<&TrustRoot>,
) -> Result<(Signer, VerifiedSigner)> {
	let claimed = claims(signature)?;
	let describe = describe_claims(&claimed);
	let proved = match claimed {
		Claims::Key {
			key,
		} => {
			let signer = Signer::Key(key);
			verify_signature(digest, signature, &signer).map(|v| (signer, v))
		}
		Claims::Sigstore {
			issuer,
			identity,
			repository_id,
			owner_id,
			..
		} => {
			let root = root.context("a Sigstore signature needs a trust root")?;
			let mut last = None;
			let mut proved = None;
			for anchor in &root.anchors {
				let signer = Signer::Sigstore(SigstoreSigner {
					ca: anchor.ca.clone(),
					log: anchor.log.clone(),
					tsa: anchor.tsa.clone(),
					issuer: issuer.clone(),
					identity: identity.clone(),
					repository_id: repository_id.clone(),
					owner_id: owner_id.clone(),
				});
				match verify_signature(digest, signature, &signer) {
					Ok(VerifiedSigner::Sigstore {
						signed_at,
						..
					}) if !anchor.covers(signed_at) => {
						last = Some(anyhow::anyhow!(
							"it was signed at {signed_at} (Unix time), outside the validity of the trust-root entry that verifies it"
						));
					}
					Ok(verified) => {
						proved = Some((signer, verified));
						break;
					}
					Err(e) => last = Some(e),
				}
			}
			match proved {
				Some(proved) => Ok(proved),
				None => {
					Err(last.unwrap_or_else(|| anyhow::anyhow!("the trust root holds no anchor")))
				}
			}
		}
	};
	proved.with_context(|| format!("the signature, which claims {describe}, does not verify"))
}

/// The claims as one line for an error message.
fn describe_claims(claims: &Claims) -> String {
	match claims {
		Claims::Key {
			key,
		} => format!("SSH key {}", super::signer::ssh_key_fingerprint(key)),
		Claims::Sigstore {
			issuer,
			identity,
			..
		} => format!("Sigstore identity {identity} from {issuer}"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	const ROOT: &str = r#"{
	  "mediaType": "application/vnd.dev.sigstore.trustedroot+json;version=0.1",
	  "tlogs": [
	    { "baseUrl": "https://rekor.sigstore.dev",
	      "publicKey": { "rawBytes": "T0xE", "validFor": { "start": "2021-01-12T11:53:27Z", "end": "2025-01-01T00:00:00Z" } } },
	    { "baseUrl": "https://log2025-1.rekor.sigstore.dev",
	      "publicKey": { "rawBytes": "TE9H", "validFor": { "start": "2025-01-01T00:00:00Z" } } }
	  ],
	  "certificateAuthorities": [
	    { "certChain": { "certificates": [ { "rawBytes": "T0xEQ0E=" } ] },
	      "validFor": { "start": "2021-03-07T03:20:29Z", "end": "2022-12-31T23:59:59Z" } },
	    { "certChain": { "certificates": [ { "rawBytes": "SU5U" }, { "rawBytes": "Uk9PVA==" } ] },
	      "validFor": { "start": "2022-04-13T20:06:15Z" } }
	  ],
	  "ctlogs": [],
	  "timestampAuthorities": [
	    { "certChain": { "certificates": [ { "rawBytes": "VFNB" }, { "rawBytes": "VFNBUk9PVA==" } ] },
	      "validFor": { "start": "2025-07-04T00:00:00Z" } }
	  ]
	}"#;

	fn at(t: &str) -> i64 {
		surrealdb_types::Datetime::from_str(t).unwrap().timestamp()
	}

	#[test]
	fn keeps_retired_entries_with_their_windows() {
		let root = parse_trusted_root(ROOT).expect("valid trusted root");
		let tsa: Vec<String> = vec!["VFNB".into(), "VFNBUk9PVA==".into()];
		let new_ca: Vec<String> = vec!["SU5U".into(), "Uk9PVA==".into()];
		assert_eq!(
			root.anchors,
			vec![
				SigstoreAnchor {
					ca: vec!["T0xEQ0E=".into()],
					log: "T0xE".into(),
					log_url: Some("https://rekor.sigstore.dev".into()),
					tsa: tsa.clone(),
					valid_from: Some(at("2021-03-07T03:20:29Z")),
					valid_until: Some(at("2022-12-31T23:59:59Z")),
				},
				// The old CA and the new log key never overlapped: no anchor.
				SigstoreAnchor {
					ca: new_ca.clone(),
					log: "T0xE".into(),
					log_url: Some("https://rekor.sigstore.dev".into()),
					tsa: tsa.clone(),
					valid_from: Some(at("2022-04-13T20:06:15Z")),
					valid_until: Some(at("2025-01-01T00:00:00Z")),
				},
				SigstoreAnchor {
					ca: new_ca,
					log: "TE9H".into(),
					log_url: Some("https://log2025-1.rekor.sigstore.dev".into()),
					tsa,
					valid_from: Some(at("2025-01-01T00:00:00Z")),
					valid_until: None,
				},
			]
		);
		let retired = &root.anchors[0];
		assert!(retired.covers(at("2022-06-01T00:00:00Z")));
		assert!(!retired.covers(at("2024-06-01T00:00:00Z")), "signed after it was retired");
		assert!(root.anchors[2].covers(at("2030-01-01T00:00:00Z")));
	}

	#[test]
	fn a_root_without_anchors_is_refused() {
		let empty = ROOT.replace(
			r#""certificateAuthorities": ["#,
			r#""certificateAuthorities": [], "unused": ["#,
		);
		assert!(parse_trusted_root(&empty).is_err());
	}
}
