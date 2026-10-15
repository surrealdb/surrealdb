//! Selecting a bundle's anchors from a Sigstore `trusted_root.json`, sealing
//! them into the signature envelope, and verifying offline from the envelope
//! alone, while `verify` keeps trusting only the signer's anchors.

#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde_json::Value;
use surrealism_verify::{
	Claims, ENVELOPE_MEDIA_TYPE, Error, ManifestDigest, Signer, SigstoreAnchors, TrustedRoot,
	verify, verify_offline,
};

/// `sha256` of the artifact every conformance bundle signs.
const DIGEST: &str = "a0cfc71271d6e278e57cd332ff957c3f7043fdda354c4cbb190a30d56efa01bf";
const IDENTITY: &str = "https://github.com/sigstore-conformance/extremely-dangerous-public-oidc-beacon/.github/workflows/extremely-dangerous-oidc-beacon.yml@refs/heads/main";

fn fixture(name: &str) -> Vec<u8> {
	fs::read(
		PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/conformance").join(name),
	)
	.unwrap()
}

fn digest() -> ManifestDigest {
	let mut out = [0u8; 32];
	for (i, byte) in out.iter_mut().enumerate() {
		*byte = u8::from_str_radix(&DIGEST[i * 2..i * 2 + 2], 16).unwrap();
	}
	out
}

fn json(bytes: &[u8]) -> Value {
	serde_json::from_slice(bytes).unwrap()
}

#[test]
fn the_anchors_that_verify_are_selected_and_verify_offline() {
	let full = TrustedRoot::parse(&fixture("trusted_root.json")).unwrap();
	let bundle = fixture("happy-path.sigstore.json");

	let (signer, reduced) = full.select(&digest(), &bundle).unwrap();
	assert_eq!(signer.identity, IDENTITY);
	assert_eq!(signer.repository_id.as_deref(), Some("632596897"));
	verify(&digest(), &bundle, &Signer::Sigstore(signer.clone())).unwrap();

	// Only the Rekor v2 log that logged this entry, one CA and one TSA.
	let written = reduced.to_json().unwrap();
	let reduced_json = json(&written);
	assert_eq!(reduced_json["tlogs"].as_array().unwrap().len(), 1);
	assert_eq!(reduced_json["tlogs"][0]["baseUrl"], "https://log2025-alpha1.rekor.sigstage.dev");
	assert_eq!(reduced_json["certificateAuthorities"].as_array().unwrap().len(), 1);
	assert_eq!(reduced_json["timestampAuthorities"].as_array().unwrap().len(), 1);
	assert_eq!(reduced_json["ctlogs"], serde_json::json!([]));

	// The reduced root alone verifies the signature, and yields the same
	// signer.
	let (again, _) = TrustedRoot::parse(&written).unwrap().select(&digest(), &bundle).unwrap();
	assert_eq!(again, signer);
}

#[test]
fn a_tampered_root_is_refused() {
	let full = TrustedRoot::parse(&fixture("trusted_root.json")).unwrap();
	let bundle = fixture("happy-path.sigstore.json");
	let (_, reduced) = full.select(&digest(), &bundle).unwrap();
	let original = json(&reduced.to_json().unwrap());

	for path in [
		&["certificateAuthorities", "certChain"][..],
		&["timestampAuthorities", "certChain"][..],
		&["tlogs", "publicKey"][..],
	] {
		let mut tampered = original.clone();
		let entry = &mut tampered[path[0]][0][path[1]];
		let flip = |raw: &mut Value| {
			let mut der = BASE64.decode(raw.as_str().unwrap()).unwrap();
			let at = der.len() / 2;
			der[at] ^= 0x01;
			*raw = Value::String(BASE64.encode(der));
		};
		if path[1] == "certChain" {
			// Every certificate in `ca` and `tsa` is an anchor, so each one is
			// changed inside its signed body.
			for cert in entry["certificates"].as_array_mut().unwrap() {
				flip(&mut cert["rawBytes"]);
			}
		} else {
			flip(&mut entry["rawBytes"]);
		}
		let bytes = serde_json::to_vec(&tampered).unwrap();
		let root = TrustedRoot::parse(&bytes).unwrap();
		assert!(
			matches!(root.select(&digest(), &bundle), Err(Error::TrustedRoot(_))),
			"a changed {path:?} still verified"
		);
	}
}

#[test]
fn another_digest_finds_no_anchors() {
	let full = TrustedRoot::parse(&fixture("trusted_root.json")).unwrap();
	let mut other = digest();
	other[0] ^= 1;
	assert!(full.select(&other, &fixture("happy-path.sigstore.json")).is_err());
}

#[test]
fn only_a_sigstore_trusted_root_is_read() {
	assert!(TrustedRoot::parse(b"{\"mediaType\":\"application/json\"}").is_err());
	assert!(TrustedRoot::parse(b"not json").is_err());
	let empty = TrustedRoot::parse(
		b"{\"mediaType\":\"application/vnd.dev.sigstore.trustedroot+json;version=0.1\"}",
	)
	.unwrap();
	let err = empty.select(&digest(), &fixture("happy-path.sigstore.json")).unwrap_err();
	assert!(err.to_string().contains("holds no certificate authority"), "{err}");
}

fn sealed() -> Vec<u8> {
	TrustedRoot::parse(&fixture("trusted_root.json"))
		.unwrap()
		.seal(&digest(), &fixture("happy-path.sigstore.json"))
		.unwrap()
}

/// Applies `edit` to the envelope's JSON.
fn edited(edit: impl FnOnce(&mut Value)) -> Vec<u8> {
	let mut json = json(&sealed());
	edit(&mut json);
	serde_json::to_vec(&json).unwrap()
}

fn flip(raw: &mut Value) {
	let mut der = BASE64.decode(raw.as_str().unwrap()).unwrap();
	let at = der.len() / 2;
	der[at] ^= 0x01;
	*raw = Value::String(BASE64.encode(der));
}

#[test]
fn an_envelope_verifies_offline_from_its_own_root() {
	let envelope = sealed();
	let parsed = json(&envelope);
	assert_eq!(parsed["mediaType"], ENVELOPE_MEDIA_TYPE);
	assert_eq!(parsed["bundle"], json(&fixture("happy-path.sigstore.json")));

	let verified = verify_offline(&digest(), &envelope).unwrap();
	assert!(
		matches!(verified, surrealism_verify::VerifiedSigner::Sigstore { ref identity, .. } if identity == IDENTITY)
	);

	// The claims carry the embedded anchors, so a client can build the
	// signer it shows and records.
	let Claims::Sigstore {
		anchors: Some(SigstoreAnchors {
			ca,
			log,
			tsa,
		}),
		identity,
		..
	} = surrealism_verify::claims(&envelope).unwrap()
	else {
		panic!("no anchors in the envelope's claims");
	};
	assert_eq!(identity, IDENTITY);
	let (signer, _) = TrustedRoot::parse(&fixture("trusted_root.json"))
		.unwrap()
		.select(&digest(), &fixture("happy-path.sigstore.json"))
		.unwrap();
	assert_eq!((ca, log, tsa), (signer.ca, signer.log, signer.tsa));
}

#[test]
fn a_tampered_envelope_does_not_verify_offline() {
	let cases: Vec<(&str, Vec<u8>)> = vec![
		(
			"ca",
			edited(|e| {
				for c in e["trustedRoot"]["certificateAuthorities"][0]["certChain"]["certificates"]
					.as_array_mut()
					.unwrap()
				{
					flip(&mut c["rawBytes"]);
				}
			}),
		),
		(
			"tsa",
			edited(|e| {
				for c in e["trustedRoot"]["timestampAuthorities"][0]["certChain"]["certificates"]
					.as_array_mut()
					.unwrap()
				{
					flip(&mut c["rawBytes"]);
				}
			}),
		),
		("log", edited(|e| flip(&mut e["trustedRoot"]["tlogs"][0]["publicKey"]["rawBytes"]))),
		("signature", edited(|e| flip(&mut e["bundle"]["messageSignature"]["signature"]))),
		(
			"certificate",
			edited(|e| flip(&mut e["bundle"]["verificationMaterial"]["certificate"]["rawBytes"])),
		),
	];
	for (what, envelope) in cases {
		assert!(verify_offline(&digest(), &envelope).is_err(), "a changed {what} verified");
	}
	let mut other = digest();
	other[0] ^= 1;
	assert!(verify_offline(&other, &sealed()).is_err());
}

#[test]
fn an_envelope_holds_exactly_one_anchor_of_each_kind_and_nothing_else() {
	let two_cas = edited(|e| {
		let ca = e["trustedRoot"]["certificateAuthorities"][0].clone();
		e["trustedRoot"]["certificateAuthorities"].as_array_mut().unwrap().push(ca);
	});
	assert!(matches!(verify_offline(&digest(), &two_cas), Err(Error::Malformed(_))));
	let unknown = edited(|e| e["note"] = Value::String("x".into()));
	assert!(matches!(verify_offline(&digest(), &unknown), Err(Error::Malformed(_))));
	let no_root = edited(|e| {
		e.as_object_mut().unwrap().remove("trustedRoot");
	});
	assert!(matches!(verify_offline(&digest(), &no_root), Err(Error::Malformed(_))));
}

/// The server path: `verify` checks only the signer's anchors. An envelope
/// whose embedded root names other anchors still verifies against the right
/// signer, and a signer built from the embedded root is not trusted because
/// it is embedded.
#[test]
fn verify_ignores_the_embedded_root() {
	let full = TrustedRoot::parse(&fixture("trusted_root.json")).unwrap();
	let (signer, _) = full.select(&digest(), &fixture("happy-path.sigstore.json")).unwrap();

	// Another deployment's CA in the embedded root: offline it fails, against
	// the recorded signer it passes.
	let foreign = edited(|e| {
		for c in e["trustedRoot"]["certificateAuthorities"][0]["certChain"]["certificates"]
			.as_array_mut()
			.unwrap()
		{
			flip(&mut c["rawBytes"]);
		}
	});
	assert!(verify_offline(&digest(), &foreign).is_err());
	verify(&digest(), &foreign, &Signer::Sigstore(signer.clone())).unwrap();

	// An embedded root of the wrong shape does not change the verdict:
	// `verify` does not read it.
	let malformed = edited(|e| {
		let ca = e["trustedRoot"]["certificateAuthorities"][0].clone();
		e["trustedRoot"]["certificateAuthorities"].as_array_mut().unwrap().push(ca);
		e["trustedRoot"]["tlogs"] = Value::String("not a list".into());
	});
	assert!(verify_offline(&digest(), &malformed).is_err());
	verify(&digest(), &malformed, &Signer::Sigstore(signer.clone())).unwrap();

	// The genuine envelope against a signer with other anchors fails, whatever
	// the envelope embeds.
	let mut wrong = signer;
	wrong.log = wrong.ca[0].clone();
	assert!(verify(&digest(), &sealed(), &Signer::Sigstore(wrong)).is_err());
}

#[test]
fn a_bare_bundle_verifies_only_against_a_signer() {
	let bundle = fixture("happy-path.sigstore.json");
	assert!(matches!(
		verify_offline(&digest(), &bundle),
		Err(Error::FormatMismatch {
			found: "a bare Sigstore bundle",
			..
		})
	));
	let (signer, _) = TrustedRoot::parse(&fixture("trusted_root.json"))
		.unwrap()
		.select(&digest(), &bundle)
		.unwrap();
	verify(&digest(), &bundle, &Signer::Sigstore(signer)).unwrap();
	assert!(
		TrustedRoot::parse(&fixture("trusted_root.json"))
			.unwrap()
			.seal(&digest(), &sealed())
			.is_err(),
		"an envelope is not sealed twice"
	);
}
