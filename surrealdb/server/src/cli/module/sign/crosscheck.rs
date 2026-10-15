//! What `sign` writes as `SIGNATURE`, checked by the verifier the server runs.
//!
//! - `--key`: [`ssh::sign`] with a fresh key, verified with that key's public line.
//! - envelope: [`super::trusted_root::seal`] reduces a root to the anchors that verify a bundle and
//!   wraps both, and the archive then verifies offline from its own MANIFEST and SIGNATURE; a
//!   changed embedded root verifies nothing offline, and `verify` still trusts only the recorded
//!   signer.
//! - `--bundle`: [`bundle::check_import`] passes each private-Sigstore fixture bundle
//!   (`surrealism/verify/tests/fixtures/private/`) and a real Rekor v2 bundle from the Sigstore
//!   conformance suite through unchanged in substance, and they still verify against their anchors.

use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use surrealism_verify::{Signer, SigstoreSigner, verify};

use super::{bundle, ssh};

fn fixtures() -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../surrealism/verify/tests/fixtures")
}

fn read(path: &Path) -> Vec<u8> {
	std::fs::read(path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn json(path: &Path) -> Value {
	serde_json::from_slice(&read(path)).unwrap()
}

fn b64(v: &Value) -> Vec<u8> {
	B64.decode(v.as_str().unwrap()).unwrap()
}

#[test]
fn an_ssh_signature_from_sign_verifies() {
	if !ssh::tests::have_ssh_keygen() {
		eprintln!("skipping: ssh-keygen not on PATH");
		return;
	}
	let dir = tempfile::tempdir().unwrap();
	let key = ssh::tests::generate_key(dir.path());
	let public = std::fs::read_to_string(key.with_extension("pub")).unwrap();
	let manifest = read(&fixtures().join("MANIFEST"));
	let digest: [u8; 32] = Sha256::digest(&manifest).into();

	let signature = ssh::sign(&key, &manifest).unwrap();
	let signer = Signer::Key(public.trim().to_owned());
	verify(&digest, &signature, &signer).unwrap();
	assert!(verify(&[0u8; 32], &signature, &signer).is_err());
}

/// The signer in a private case's `signer.json`.
fn private_signer(case: &str) -> SigstoreSigner {
	let s = json(&fixtures().join("private").join(case).join("signer.json"));
	let text = |v: &Value| v.as_str().map(str::to_owned);
	SigstoreSigner {
		ca: s["ca"].as_array().unwrap().iter().map(b64).collect(),
		log: b64(&s["log"]),
		tsa: s["tsa"].as_array().unwrap().iter().map(b64).collect(),
		issuer: text(&s["issuer"]).unwrap(),
		identity: text(&s["identity"]).unwrap(),
		repository_id: text(&s["repository_id"]),
		owner_id: text(&s["owner_id"]),
	}
}

/// Each private-Sigstore fixture bundle passes `--bundle`'s import checks
/// unchanged in substance and still verifies against its deployment.
#[test]
fn an_imported_private_bundle_verifies() {
	let digest: [u8; 32] = Sha256::digest(read(&fixtures().join("MANIFEST"))).into();
	for case in ["ecdsa-log", "ed25519-log", "email-identity"] {
		let made = read(&fixtures().join("private").join(case).join("bundle.sigstore.json"));
		let signer = Signer::Sigstore(private_signer(case));
		let imported = bundle::check_import(&made, &digest).unwrap();
		verify(&digest, &imported, &signer).unwrap_or_else(|e| panic!("{case}: {e}"));
		assert!(verify(&[0u8; 32], &imported, &signer).is_err(), "{case}: another digest");
	}
}

#[test]
fn an_imported_conformance_bundle_verifies() {
	let dir = fixtures().join("conformance");
	let made = read(&dir.join("happy-path.sigstore.json"));
	let digest: [u8; 32] =
		hex::decode("a0cfc71271d6e278e57cd332ff957c3f7043fdda354c4cbb190a30d56efa01bf")
			.unwrap()
			.try_into()
			.unwrap();
	let imported = bundle::check_import(&made, &digest).unwrap();

	let root = json(&dir.join("trusted_root.json"));
	let chain = |authority: &Value| -> Vec<Vec<u8>> {
		authority["certChain"]["certificates"]
			.as_array()
			.unwrap()
			.iter()
			.map(|c| b64(&c["rawBytes"]))
			.collect()
	};
	let log = root["tlogs"]
		.as_array()
		.unwrap()
		.iter()
		.find(|l| l["baseUrl"] == "https://log2025-alpha1.rekor.sigstage.dev")
		.unwrap();
	let signer = Signer::Sigstore(SigstoreSigner {
		ca: chain(root["certificateAuthorities"].as_array().unwrap().last().unwrap()),
		log: b64(&log["publicKey"]["rawBytes"]),
		tsa: chain(root["timestampAuthorities"].as_array().unwrap().last().unwrap()),
		issuer: "https://token.actions.githubusercontent.com".into(),
		identity: "https://github.com/sigstore-conformance/extremely-dangerous-public-oidc-beacon/.github/workflows/extremely-dangerous-oidc-beacon.yml@refs/heads/main".into(),
		repository_id: Some("632596897".into()),
		owner_id: Some("131804563".into()),
	});
	verify(&digest, &imported, &signer).unwrap();
}

/// A `trusted_root.json` holding the private Sigstore of `case` among the
/// staging root's entries, so that selection has decoys to pass over.
fn private_trusted_root(case: &str) -> Vec<u8> {
	let s = json(&fixtures().join("private").join(case).join("signer.json"));
	let chain = |certs: &Value| {
		serde_json::json!({ "certificates": certs
			.as_array()
			.unwrap()
			.iter()
			.map(|c| serde_json::json!({ "rawBytes": c }))
			.collect::<Vec<_>>() })
	};
	let mut root = json(&fixtures().join("conformance").join("trusted_root.json"));
	root["certificateAuthorities"].as_array_mut().unwrap().push(
		serde_json::json!({ "uri": "https://fulcio.surrealism.test", "certChain": chain(&s["ca"]) }),
	);
	root["timestampAuthorities"].as_array_mut().unwrap().push(
		serde_json::json!({ "uri": "https://tsa.surrealism.test", "certChain": chain(&s["tsa"]) }),
	);
	root["tlogs"].as_array_mut().unwrap().push(serde_json::json!({
		"baseUrl": "https://log.surrealism.test",
		"hashAlgorithm": "SHA2_256",
		"publicKey": { "rawBytes": s["log"] },
	}));
	serde_json::to_vec(&root).unwrap()
}

/// What `sign` writes for a Sigstore signature verifies offline from the
/// archive alone: its MANIFEST and its SIGNATURE envelope, whose root holds
/// only the anchors that verify it. The server path still verifies against the
/// recorded signer only, and a changed embedded root verifies nothing offline.
#[test]
fn a_signed_archive_verifies_offline_from_its_own_envelope() {
	let manifest = read(&fixtures().join("MANIFEST"));
	let digest: [u8; 32] = Sha256::digest(&manifest).into();
	let dir = tempfile::tempdir().unwrap();

	for case in ["ecdsa-log", "ed25519-log", "email-identity"] {
		let bundle = read(&fixtures().join("private").join(case).join("bundle.sigstore.json"));
		let envelope =
			super::trusted_root::seal(&private_trusted_root(case), &digest, &bundle).unwrap();
		let parsed: Value = serde_json::from_slice(&envelope).unwrap();
		assert_eq!(parsed["mediaType"], surrealism_verify::ENVELOPE_MEDIA_TYPE);
		let root = &parsed["trustedRoot"];
		assert_eq!(root["tlogs"][0]["baseUrl"], "https://log.surrealism.test", "{case}");
		assert_eq!(root["certificateAuthorities"].as_array().unwrap().len(), 1);
		assert_eq!(root["timestampAuthorities"].as_array().unwrap().len(), 1);

		let pkg = super::archive::tests::write_archive(
			&dir.path().join(format!("{case}.surli")),
			&[(super::archive::MANIFEST_PATH, manifest.as_slice())],
		);
		let signed = super::archive::read_manifest(&pkg).unwrap();
		super::archive::write_signature(&pkg, &pkg, &signed, &envelope).unwrap();

		// Offline: everything comes from the archive.
		let files = surrealism_runtime::package::trust_files_from_reader(
			std::fs::File::open(&pkg).unwrap(),
			surrealism_runtime::package::DEFAULT_MAX_FS_BYTES,
		)
		.unwrap();
		let digest = files.manifest.digest().0;
		let signature = files.signature.unwrap();
		surrealism_verify::verify_offline(&digest, &signature).unwrap();
		let surrealism_verify::Claims::Sigstore {
			anchors: Some(anchors),
			..
		} = surrealism_verify::claims(&signature).unwrap()
		else {
			panic!("{case}: the envelope's claims carry no anchors");
		};
		let expected = private_signer(case);
		assert_eq!(
			(&anchors.ca, &anchors.log, &anchors.tsa),
			(&expected.ca, &expected.log, &expected.tsa)
		);

		// The server path: the recorded signer, whatever the envelope embeds.
		verify(&digest, &signature, &Signer::Sigstore(expected)).unwrap();

		// A changed log key in the embedded root verifies nothing offline.
		let mut tampered = parsed.clone();
		let mut key = b64(&tampered["trustedRoot"]["tlogs"][0]["publicKey"]["rawBytes"]);
		let at = key.len() - 4;
		key[at] ^= 0x01;
		tampered["trustedRoot"]["tlogs"][0]["publicKey"]["rawBytes"] =
			Value::String(B64.encode(key));
		let tampered = serde_json::to_vec(&tampered).unwrap();
		assert!(
			surrealism_verify::verify_offline(&digest, &tampered).is_err(),
			"{case}: a changed root verified"
		);
	}
}
