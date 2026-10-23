//! SSH signatures (OpenSSH `PROTOCOL.sshsig`), as made by
//! `ssh-keygen -Y sign -n surrealism -O hashalg=sha256 -f key MANIFEST`.
//!
//! An SSH signature signs `H(message)`, not the message. With `sha256` as the
//! hash, `H(MANIFEST)` is the archive's `DIGEST`, so the signature can be
//! checked from the digest alone. Other hashes are refused for that reason.
//!
//! Parsing the armored signature and the recorded key, and checking the
//! signature with the key, is `ssh-key`'s. The signed-data blob is built here
//! from the digest, because `ssh-key` would hash the message itself and the
//! verifier holds only its hash.
//!
//! Accepted key types: `ssh-ed25519` and `ecdsa-sha2-nistp256`.

use signature::Verifier as _;
use ssh_key::{Algorithm, EcdsaCurve, HashAlg, PublicKey, SshSig};

use crate::{Claims, Error, ManifestDigest, SSH_NAMESPACE, VerifiedSigner};

pub(crate) const ARMOR_BEGIN: &str = "-----BEGIN SSH SIGNATURE-----";
const MAGIC: &[u8] = b"SSHSIG";
const HASH_ALGORITHM: &str = "sha256";

fn malformed(message: impl Into<String>) -> Error {
	Error::Malformed(message.into())
}

/// The armored signature, parsed.
fn parse(armored: &[u8]) -> Result<SshSig, Error> {
	SshSig::from_pem(armored).map_err(|e| malformed(format!("not an SSH signature: {e}")))
}

/// The recorded key, `<type> <base64> [comment]`, of an accepted type.
fn recorded_key(key: &str) -> Result<PublicKey, Error> {
	let public = PublicKey::from_openssh(key)
		.map_err(|e| Error::InvalidSigner(format!("key is not an OpenSSH public key: {e}")))?;
	let alg = public.algorithm();
	if alg == Algorithm::Ed25519
		|| alg
			== (Algorithm::Ecdsa {
				curve: EcdsaCurve::NistP256,
			}) {
		Ok(public)
	} else {
		Err(Error::InvalidSigner(format!("unsupported SSH key type {alg}")))
	}
}

pub(crate) fn verify(
	digest: &ManifestDigest,
	armored: &[u8],
	key: &str,
) -> Result<VerifiedSigner, Error> {
	let recorded = recorded_key(key)?;
	let sig = parse(armored)?;

	if sig.namespace() != SSH_NAMESPACE {
		return Err(Error::Namespace(sig.namespace().to_owned()));
	}
	if sig.hash_alg() != HashAlg::Sha256 {
		return Err(malformed(format!(
			"the SSH signature hashes with {}, not {HASH_ALGORITHM}",
			sig.hash_alg()
		)));
	}
	// The key blob decides; the comment of the recorded key is not part of it.
	if sig.public_key() != recorded.key_data() {
		return Err(Error::KeyMismatch);
	}

	// PROTOCOL.sshsig: the signed data is the magic, then the namespace, the
	// reserved field, the hash algorithm and H(message), each an SSH string.
	let mut signed = Vec::with_capacity(64 + sig.namespace().len() + digest.len());
	signed.extend_from_slice(MAGIC);
	put_string(&mut signed, sig.namespace().as_bytes());
	put_string(&mut signed, sig.reserved());
	put_string(&mut signed, HASH_ALGORITHM.as_bytes());
	put_string(&mut signed, digest);
	recorded.key_data().verify(&signed, sig.signature()).map_err(|_| Error::BadSignature)?;

	Ok(VerifiedSigner::Key {
		key_type: recorded.algorithm().as_str().to_owned(),
		fingerprint: recorded.key_data().fingerprint(HashAlg::Sha256).to_string(),
	})
}

/// Reads the key an SSH signature carries, as an `authorized_keys` line.
pub(crate) fn claims(armored: &[u8]) -> Result<Claims, Error> {
	let sig = parse(armored)?;
	let key = PublicKey::from(sig.public_key().clone())
		.to_openssh()
		.map_err(|e| malformed(format!("the SSH signature's key cannot be written: {e}")))?;
	Ok(Claims::Key {
		key,
	})
}

fn put_string(out: &mut Vec<u8>, data: &[u8]) {
	// Every field here is far below 4 GiB: the signature is capped at 128 KiB.
	out.extend_from_slice(&(data.len() as u32).to_be_bytes());
	out.extend_from_slice(data);
}
