//! Sigstore bundles (`application/vnd.dev.sigstore.bundle.v0.3+json`): the
//! checks an imported bundle must pass.
//!
//! A bundle signs `MANIFEST` as a message: `messageSignature.messageDigest` is
//! `sha256(MANIFEST)` and the signature is ECDSA over the same bytes. This is
//! what `cosign sign-blob --bundle` produces for the extracted manifest.

use anyhow::{Context, Result, bail, ensure};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::Value;

const MEDIA_TYPE_PREFIX: &str = "application/vnd.dev.sigstore.bundle";

/// Checks that `bytes` is a Sigstore bundle that signs a message whose
/// SHA-256 is `digest`, and returns the bytes to store as `SIGNATURE`.
///
/// The bundle is stored as given, minus surrounding whitespace, so that its
/// first byte is `{`. The checks are:
/// - it is JSON with a Sigstore bundle media type;
/// - it carries a `messageSignature` (a DSSE envelope signs an attestation, not the manifest);
/// - `messageSignature.messageDigest` is SHA2_256 and equals `digest`;
/// - every `hashedrekord` log entry whose body this client can read records the same digest.
///
/// Signature, certificate and log proofs are left to the verifier, which holds
/// the trust anchors.
pub fn check_import(bytes: &[u8], digest: &[u8; 32]) -> Result<Vec<u8>> {
	let trimmed = bytes.trim_ascii();
	ensure!(trimmed.first() == Some(&b'{'), "The bundle is not a JSON object");
	let bundle: Value = serde_json::from_slice(trimmed).context("The bundle is not valid JSON")?;

	let media = bundle["mediaType"].as_str().unwrap_or_default();
	if !media.starts_with(MEDIA_TYPE_PREFIX) {
		bail!(
			"The file is not a Sigstore bundle (mediaType {media:?}). With cosign, sign with \
			 `cosign sign-blob --new-bundle-format --bundle bundle.json MANIFEST`"
		);
	}
	if bundle.get("dsseEnvelope").is_some() {
		bail!(
			"The bundle holds a DSSE envelope. Sign the extracted MANIFEST as a blob with \
			 `cosign sign-blob`, not `cosign attest-blob`"
		);
	}
	let md = &bundle["messageSignature"]["messageDigest"];
	ensure!(md.is_object(), "The bundle has no messageSignature.messageDigest");
	let algorithm = md["algorithm"].as_str().unwrap_or_default();
	ensure!(
		algorithm == "SHA2_256",
		"The bundle digest uses {algorithm:?}; extensions are signed with SHA2_256"
	);
	let bundled = decode_b64(&md["digest"]).context("messageDigest.digest is not base64")?;
	ensure_digest(&bundled, digest, "messageSignature.messageDigest")?;

	for entry in bundle["verificationMaterial"]["tlogEntries"].as_array().into_iter().flatten() {
		if let Some(logged) = logged_digest(entry)? {
			ensure_digest(&logged, digest, "the transparency log entry")?;
		}
	}
	Ok(trimmed.to_vec())
}

fn ensure_digest(found: &[u8], expected: &[u8; 32], what: &str) -> Result<()> {
	ensure!(
		found == expected,
		"The bundle does not sign this archive: {what} is sha256:{}, the MANIFEST is sha256:{}",
		hex::encode(found),
		hex::encode(expected)
	);
	Ok(())
}

/// The artifact digest a `hashedrekord` entry records, for the body versions
/// this client reads (0.0.1 from Rekor v1, 0.0.2 from Rekor v2). Returns
/// `None` for anything else.
fn logged_digest(entry: &Value) -> Result<Option<Vec<u8>>> {
	let Some(body) = entry["canonicalizedBody"].as_str() else {
		return Ok(None);
	};
	let body: Value = serde_json::from_slice(
		&B64.decode(body).context("A log entry's canonicalizedBody is not base64")?,
	)
	.context("A log entry's canonicalizedBody is not JSON")?;
	if body["kind"] != "hashedrekord" {
		return Ok(None);
	}
	match body["apiVersion"].as_str() {
		Some("0.0.1") => {
			let hash = &body["spec"]["data"]["hash"];
			if hash["algorithm"] != "sha256" {
				return Ok(None);
			}
			let hex = hash["value"].as_str().context("hashedrekord 0.0.1 entry has no hash")?;
			Ok(Some(hex::decode(hex).context("hashedrekord 0.0.1 hash is not hex")?))
		}
		Some("0.0.2") => {
			let data = &body["spec"]["hashedRekordV002"]["data"];
			if data["algorithm"] != "SHA2_256" {
				return Ok(None);
			}
			Ok(Some(decode_b64(&data["digest"]).context("hashedrekord 0.0.2 digest")?))
		}
		_ => Ok(None),
	}
}

fn decode_b64(v: &Value) -> Result<Vec<u8>> {
	Ok(B64.decode(v.as_str().context("expected a base64 string")?)?)
}

#[cfg(test)]
pub(super) mod tests {
	use serde_json::json;

	use super::*;

	pub const MEDIA_TYPE_V03: &str = "application/vnd.dev.sigstore.bundle.v0.3+json";

	/// A cosign-shaped v0.3 bundle over `digest`, with a Rekor v1 entry that
	/// records `logged`.
	pub fn cosign_bundle(digest: &[u8; 32], logged: &[u8; 32]) -> Vec<u8> {
		let body = json!({
			"apiVersion": "0.0.1",
			"kind": "hashedrekord",
			"spec": {
				"data": { "hash": { "algorithm": "sha256", "value": hex::encode(logged) } },
				"signature": { "content": "MEUCIQ==", "publicKey": { "content": "LS0t" } }
			}
		});
		let bundle = json!({
			"mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json",
			"verificationMaterial": {
				"certificate": { "rawBytes": "MIIC" },
				"tlogEntries": [{
					"logIndex": "1",
					"kindVersion": { "kind": "hashedrekord", "version": "0.0.1" },
					"canonicalizedBody": B64.encode(serde_json::to_vec(&body).unwrap())
				}]
			},
			"messageSignature": {
				"messageDigest": { "algorithm": "SHA2_256", "digest": B64.encode(digest) },
				"signature": "MEUCIQ=="
			}
		});
		let mut out = serde_json::to_vec_pretty(&bundle).unwrap();
		out.push(b'\n');
		out
	}

	#[test]
	fn a_matching_bundle_is_accepted_and_trimmed() {
		let d = [1u8; 32];
		let out = check_import(&cosign_bundle(&d, &d), &d).unwrap();
		assert_eq!(out.first(), Some(&b'{'));
		assert_eq!(out.last(), Some(&b'}'));
	}

	#[test]
	fn a_bundle_over_another_digest_is_refused() {
		let err = check_import(&cosign_bundle(&[1; 32], &[1; 32]), &[2; 32]).unwrap_err();
		assert!(err.to_string().contains("does not sign this archive"), "{err}");
	}

	#[test]
	fn a_log_entry_over_another_digest_is_refused() {
		let err = check_import(&cosign_bundle(&[1; 32], &[3; 32]), &[1; 32]).unwrap_err();
		assert!(err.to_string().contains("transparency log entry"), "{err}");
	}

	#[test]
	fn a_dsse_bundle_is_refused() {
		let b = json!({
			"mediaType": MEDIA_TYPE_V03,
			"verificationMaterial": {},
			"dsseEnvelope": {}
		});
		let err = check_import(&serde_json::to_vec(&b).unwrap(), &[0; 32]).unwrap_err();
		assert!(err.to_string().contains("DSSE"), "{err}");
	}

	#[test]
	fn a_legacy_cosign_bundle_is_refused() {
		let b = br#"{"base64Signature":"x","cert":"y","rekorBundle":{}}"#;
		let err = check_import(b, &[0; 32]).unwrap_err();
		assert!(err.to_string().contains("not a Sigstore bundle"), "{err}");
	}
}
