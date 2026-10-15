//! Real Rekor v2 bundles from the Sigstore conformance suite.
//!
//! Every bundle was produced by Sigstore's staging infrastructure for a GitHub
//! Actions workflow, so it exercises the real certificate chain, the real
//! Ed25519 checkpoint format and a real RFC 3161 response. The anchors are
//! read from the staging trusted root, as a client would read them from
//! Sigstore's TUF repository. See `fixtures/conformance/README.md`.

#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::Value;
use surrealism_verify::{Error, ManifestDigest, Signer, SigstoreSigner, VerifiedSigner, verify};

/// `sha256` of the artifact every bundle signs.
const DIGEST: &str = "a0cfc71271d6e278e57cd332ff957c3f7043fdda354c4cbb190a30d56efa01bf";
const ISSUER: &str = "https://token.actions.githubusercontent.com";
const IDENTITY: &str = "https://github.com/sigstore-conformance/extremely-dangerous-public-oidc-beacon/.github/workflows/extremely-dangerous-oidc-beacon.yml@refs/heads/main";
const REPOSITORY_ID: &str = "632596897";
const OWNER_ID: &str = "131804563";
/// The Rekor v2 staging log in the trusted root.
const LOG_URL: &str = "https://log2025-alpha1.rekor.sigstage.dev";

fn fixture(name: &str) -> Vec<u8> {
	let path =
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/conformance").join(name);
	fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn digest() -> ManifestDigest {
	let mut out = [0u8; 32];
	for (i, byte) in out.iter_mut().enumerate() {
		*byte = u8::from_str_radix(&DIGEST[i * 2..i * 2 + 2], 16).unwrap();
	}
	out
}

fn certificates(chain: &Value) -> Vec<Vec<u8>> {
	chain["certChain"]["certificates"]
		.as_array()
		.unwrap()
		.iter()
		.map(|c| BASE64.decode(c["rawBytes"].as_str().unwrap()).unwrap())
		.collect()
}

/// The signer a client writes for the beacon workflow, with anchors from
/// `root`.
fn signer_from(root: &str) -> SigstoreSigner {
	let root: Value = serde_json::from_slice(&fixture(root)).unwrap();
	let ca = root["certificateAuthorities"].as_array().unwrap().last().unwrap();
	let tsa = root["timestampAuthorities"].as_array().unwrap().last().unwrap();
	let log = root["tlogs"]
		.as_array()
		.unwrap()
		.iter()
		.find(|l| l["baseUrl"] == LOG_URL)
		.expect("staging Rekor v2 log in the trusted root");
	SigstoreSigner {
		ca: certificates(ca),
		log: BASE64.decode(log["publicKey"]["rawBytes"].as_str().unwrap()).unwrap(),
		tsa: certificates(tsa),
		issuer: ISSUER.into(),
		identity: IDENTITY.into(),
		repository_id: Some(REPOSITORY_ID.into()),
		owner_id: Some(OWNER_ID.into()),
	}
}

fn signer() -> SigstoreSigner {
	signer_from("trusted_root.json")
}

fn check(bundle: &str, signer: SigstoreSigner) -> Result<VerifiedSigner, Error> {
	verify(&digest(), &fixture(&format!("{bundle}.sigstore.json")), &Signer::Sigstore(signer))
}

#[test]
fn happy_path() {
	let verified = check("happy-path", signer()).unwrap();
	assert_eq!(
		verified,
		VerifiedSigner::Sigstore {
			issuer: ISSUER.into(),
			identity: IDENTITY.into(),
			repository_id: Some(REPOSITORY_ID.into()),
			owner_id: Some(OWNER_ID.into()),
			// 2025-06-12T12:02:20Z, from the timestamp authority.
			signed_at: 1_749_729_740,
			log_index: 735,
		}
	);
}

#[test]
fn valid_variants() {
	for bundle in [
		// Witness cosignatures sit next to the log's signature; unknown
		// signers are ignored, in any position.
		"checkpoint-cosigned",
		"checkpoint-multiple-cosigs",
		"checkpoint-origin-not-first",
		"checkpoint-two-sigs-cosigned",
		"checkpoint-two-sigs-from-origin",
		// Embedded TSA certificates are neither needed nor trusted.
		"timestamp-with-embedded-cert",
		"timestamp-without-embedded-cert",
	] {
		check(bundle, signer()).unwrap_or_else(|e| panic!("{bundle}: {e}"));
	}
}

/// The TSA chain has expired since, but was valid at the stamped time. No
/// clock is consulted, so the bundle stays valid.
#[test]
fn timestamp_chain_expired_after_signing() {
	check("timestamp-with-expired-cert-chain", signer_from("trusted_root_local_tsa.json")).unwrap();
}

/// The timestamp is genuine, but stamped after the 10-minute certificate
/// expired: what a stolen signing key would produce.
#[test]
fn timestamp_after_certificate_expiry() {
	assert_eq!(check("timestamp-with-incorrect-time_fail", signer()), Err(Error::OutsideValidity));
}

#[test]
fn timestamp_outside_tsa_certificate_validity() {
	let err = check(
		"timestamp-outside-tsa-cert-validity_fail",
		signer_from("trusted_root_local_tsa.json"),
	)
	.unwrap_err();
	assert!(matches!(err, Error::Timestamp(_)), "{err:?}");
}

#[test]
fn timestamp_failures() {
	for bundle in [
		"no-timestamp_fail",
		"timestamp-payload-mismatch_fail",
		"timestamp-untrusted-tsa-with-embedded-cert_fail",
		"timestamp-untrusted-tsa-without-embedded-cert_fail",
	] {
		let err = check(bundle, signer()).unwrap_err();
		assert!(matches!(err, Error::Timestamp(_)), "{bundle}: {err:?}");
	}
}

#[test]
fn checkpoint_failures() {
	for bundle in [
		"checkpoint-missing-log-signature_fail",
		"checkpoint-missing-origin_fail",
		"checkpoint-missing-root-hash_fail",
		"checkpoint-missing-size_fail",
		"checkpoint-no-matching-signature_fail",
		"no-inclusion-proof_fail",
	] {
		let err = check(bundle, signer()).unwrap_err();
		assert!(matches!(err, Error::Log(_) | Error::Malformed(_)), "{bundle}: {err:?}");
	}
}

#[test]
fn dsse_envelopes_are_refused() {
	let err = check("dsse-happy-path", signer()).unwrap_err();
	assert!(matches!(err, Error::Malformed(_)), "{err:?}");
}

#[test]
fn tampered_digest() {
	let mut digest = digest();
	digest[0] ^= 1;
	let err = verify(&digest, &fixture("happy-path.sigstore.json"), &Signer::Sigstore(signer()));
	assert_eq!(err, Err(Error::DigestMismatch));
}

/// Without the bundle's own statement of the digest, the signature itself
/// must refuse a different digest.
#[test]
fn tampered_digest_without_stated_digest() {
	let mut bundle: Value = serde_json::from_slice(&fixture("happy-path.sigstore.json")).unwrap();
	bundle["messageSignature"].as_object_mut().unwrap().remove("messageDigest");
	let bundle = serde_json::to_vec(&bundle).unwrap();
	verify(&digest(), &bundle, &Signer::Sigstore(signer())).unwrap();
	let mut digest = digest();
	digest[31] ^= 0x80;
	assert_eq!(verify(&digest, &bundle, &Signer::Sigstore(signer())), Err(Error::BadSignature));
}

#[test]
fn wrong_ids() {
	type Edit = fn(&mut SigstoreSigner);
	let cases: [(&str, Edit); 6] = [
		("repository_id", |s| s.repository_id = Some("632596898".into())),
		("repository_id", |s| s.repository_id = None),
		("owner_id", |s| s.owner_id = Some("1".into())),
		("owner_id", |s| s.owner_id = None),
		("issuer", |s| s.issuer = "https://gitlab.com".into()),
		("identity", |s| {
			s.identity = IDENTITY.replace("refs/heads/main", "refs/heads/evil");
		}),
	];
	for (field, change) in cases {
		let mut signer = signer();
		change(&mut signer);
		assert_eq!(
			check("happy-path", signer),
			Err(Error::Identity {
				field
			})
		);
	}
}

#[test]
fn wrong_log_key() {
	let root: Value = serde_json::from_slice(&fixture("trusted_root.json")).unwrap();
	for log in root["tlogs"].as_array().unwrap().iter().filter(|l| l["baseUrl"] != LOG_URL) {
		let mut signer = signer();
		signer.log = BASE64.decode(log["publicKey"]["rawBytes"].as_str().unwrap()).unwrap();
		let err = check("happy-path", signer).unwrap_err();
		assert!(matches!(err, Error::Log(_)), "{err:?}");
	}
	// The right key type with another value.
	let mut signer = signer();
	let last = signer.log.len() - 1;
	signer.log[last] ^= 1;
	let err = check("happy-path", signer).unwrap_err();
	assert!(matches!(err, Error::Log(_) | Error::InvalidSigner(_)), "{err:?}");
}

#[test]
fn wrong_ca() {
	// The timestamp authority's chain is a valid anchor list, but not the one
	// that issued the signing certificate.
	let mut signer = signer();
	signer.ca = signer.tsa.clone();
	let err = check("happy-path", signer).unwrap_err();
	assert!(matches!(err, Error::Chain(_)), "{err:?}");
}

#[test]
fn wrong_tsa() {
	let mut signer = signer();
	signer.tsa = signer.ca.clone();
	let err = check("happy-path", signer).unwrap_err();
	assert!(matches!(err, Error::Timestamp(_)), "{err:?}");
}

#[test]
fn key_signer_refuses_a_bundle() {
	let err = verify(
		&digest(),
		&fixture("happy-path.sigstore.json"),
		&Signer::Key("ssh-ed25519 AAAA".into()),
	);
	assert!(matches!(err, Err(Error::FormatMismatch { .. })), "{err:?}");
}

/// A client reads the claims, builds the signer from them and its anchors,
/// and `verify` proves them.
#[test]
fn claims_round_trip() {
	let bundle = fixture("happy-path.sigstore.json");
	let surrealism_verify::Claims::Sigstore {
		issuer,
		identity,
		repository_id,
		owner_id,
		anchors,
	} = surrealism_verify::claims(&bundle).unwrap()
	else {
		panic!("not Sigstore claims");
	};
	assert_eq!(anchors, None, "a bare bundle embeds no anchors");
	assert_eq!((issuer.as_str(), identity.as_str()), (ISSUER, IDENTITY));
	assert_eq!(repository_id.as_deref(), Some(REPOSITORY_ID));
	assert_eq!(owner_id.as_deref(), Some(OWNER_ID));
	let signer = SigstoreSigner {
		issuer,
		identity,
		repository_id,
		owner_id,
		..signer()
	};
	verify(&digest(), &bundle, &Signer::Sigstore(signer)).unwrap();
}
