//! SSH signatures made by `ssh-keygen -Y sign` (see `fixtures/generate.py`).

#![allow(clippy::unwrap_used)]

use std::fs;
use std::path::PathBuf;

use sha2::{Digest as _, Sha256};
use surrealism_verify::{Error, ManifestDigest, Signer, SigstoreSigner, VerifiedSigner, verify};

fn read(name: &str) -> Vec<u8> {
	fs::read(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)).unwrap()
}

fn key(name: &str) -> String {
	String::from_utf8(read(&format!("ssh/{name}.pub"))).unwrap().trim().to_string()
}

fn digest() -> ManifestDigest {
	Sha256::digest(read("MANIFEST")).into()
}

fn check(signature: &str, key: String) -> Result<VerifiedSigner, Error> {
	verify(&digest(), &read(&format!("ssh/{signature}")), &Signer::Key(key))
}

#[test]
fn ed25519() {
	assert_eq!(
		check("ed25519.sig", key("id_ed25519")),
		Ok(VerifiedSigner::Key {
			key_type: "ssh-ed25519".into(),
			// `ssh-keygen -lf id_ed25519.pub`
			fingerprint: "SHA256:isjlUeY6jTLmctZhC1sADPxYB78tEeeuee5nYGmt9V8".into(),
		})
	);
}

#[test]
fn ecdsa_p256() {
	assert_eq!(
		check("ecdsa.sig", key("id_ecdsa")),
		Ok(VerifiedSigner::Key {
			key_type: "ecdsa-sha2-nistp256".into(),
			fingerprint: "SHA256:noPc+mu94e13MbG+2v3ziVUlZASX2NAyQshc+MMSAsQ".into(),
		})
	);
}

#[test]
fn comment_is_ignored() {
	let full = key("id_ed25519");
	let without = full.rsplit_once(' ').unwrap().0.to_string();
	check("ed25519.sig", without.clone()).unwrap();
	check("ed25519.sig", format!("{without} someone-else@elsewhere")).unwrap();
}

#[test]
fn wrong_key() {
	assert_eq!(check("ed25519.sig", key("id_other")), Err(Error::KeyMismatch));
	assert_eq!(check("ed25519.sig", key("id_ecdsa")), Err(Error::KeyMismatch));
}

#[test]
fn other_namespace() {
	assert_eq!(
		check("ed25519-git-namespace.sig", key("id_ed25519")),
		Err(Error::Namespace("git".into()))
	);
}

/// `H(MANIFEST)` must be the digest, so only `sha256` can be checked.
#[test]
fn other_hash() {
	assert!(matches!(check("ed25519-sha512.sig", key("id_ed25519")), Err(Error::Malformed(_))));
}

#[test]
fn tampered_digest() {
	for (sig, k) in [("ed25519.sig", "id_ed25519"), ("ecdsa.sig", "id_ecdsa")] {
		let mut digest = digest();
		digest[0] ^= 1;
		assert_eq!(
			verify(&digest, &read(&format!("ssh/{sig}")), &Signer::Key(key(k))),
			Err(Error::BadSignature)
		);
	}
}

#[test]
fn malformed_recorded_key() {
	let blob = key("id_ed25519");
	let relabelled = blob.replacen("ssh-ed25519", "ssh-rsa", 1);
	assert!(matches!(check("ed25519.sig", relabelled), Err(Error::InvalidSigner(_))));
	assert!(matches!(check("ed25519.sig", "ssh-ed25519".into()), Err(Error::InvalidSigner(_))));
}

#[test]
fn format_mismatch() {
	let sigstore = Signer::Sigstore(SigstoreSigner {
		ca: vec![],
		log: vec![],
		tsa: vec![],
		issuer: String::new(),
		identity: String::new(),
		repository_id: None,
		owner_id: None,
	});
	assert!(matches!(
		verify(&digest(), &read("ssh/ed25519.sig"), &sigstore),
		Err(Error::FormatMismatch { .. })
	));
	assert!(matches!(
		verify(&digest(), b"garbage", &Signer::Key(key("id_ed25519"))),
		Err(Error::FormatMismatch { .. })
	));
}

#[test]
fn truncated_armor() {
	let sig = String::from_utf8(read("ssh/ed25519.sig")).unwrap();
	let cut = &sig[..sig.len() - 40];
	assert!(matches!(
		verify(&digest(), cut.as_bytes(), &Signer::Key(key("id_ed25519"))),
		Err(Error::Malformed(_))
	));
}

/// The claimed key is the signing key without its comment, and verifies.
#[test]
fn claims_round_trip() {
	for (sig, k) in [("ed25519.sig", "id_ed25519"), ("ecdsa.sig", "id_ecdsa")] {
		let surrealism_verify::Claims::Key {
			key: claimed,
		} = surrealism_verify::claims(&read(&format!("ssh/{sig}"))).unwrap()
		else {
			panic!("not key claims");
		};
		assert_eq!(claimed, key(k).rsplit_once(' ').unwrap().0);
		check(sig, claimed).unwrap();
	}
	assert!(surrealism_verify::claims(b"garbage").is_err());
}
