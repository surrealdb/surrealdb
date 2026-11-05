//! Public keys, hash algorithms and object identifiers.
//!
//! Three key types are enough for every signature in a Sigstore bundle and an
//! SSH signature: ECDSA P-256 (signing certificates, some logs and TSAs), ECDSA
//! P-384 (the Fulcio and TSA chains) and Ed25519 (Rekor v2 checkpoints and SSH
//! keys).

use ed25519_dalek::{Signature as Ed25519Signature, VerifyingKey as Ed25519Key};
use p256::ecdsa::signature::hazmat::PrehashVerifier;
use p256::ecdsa::{Signature as P256Signature, VerifyingKey as P256Key};
use p384::ecdsa::{Signature as P384Signature, VerifyingKey as P384Key};
use sha2::{Digest as _, Sha256, Sha384, Sha512};
use x509_parser::prelude::FromDer;
use x509_parser::x509::SubjectPublicKeyInfo;

/// DER contents of the object identifiers this crate compares against.
pub(crate) mod oid {
	/// 1.2.840.10045.2.1
	pub const EC_PUBLIC_KEY: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01];
	/// 1.2.840.10045.3.1.7
	pub const P256: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07];
	/// 1.3.132.0.34
	pub const P384: &[u8] = &[0x2b, 0x81, 0x04, 0x00, 0x22];
	/// 1.3.101.112
	pub const ED25519: &[u8] = &[0x2b, 0x65, 0x70];
	/// 1.2.840.10045.4.3.2
	pub const ECDSA_SHA256: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02];
	/// 1.2.840.10045.4.3.3
	pub const ECDSA_SHA384: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x03];
	/// 1.2.840.10045.4.3.4
	pub const ECDSA_SHA512: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x04];
	/// 2.16.840.1.101.3.4.2.1
	pub const SHA256: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01];
	/// 2.16.840.1.101.3.4.2.2
	pub const SHA384: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x02];
	/// 2.16.840.1.101.3.4.2.3
	pub const SHA512: &[u8] = &[0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x03];
	/// 1.2.840.113549.1.7.2
	pub const SIGNED_DATA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x07, 0x02];
	/// 1.2.840.113549.1.9.16.1.4
	pub const TST_INFO: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x09, 0x10, 0x01, 0x04];
	/// 1.2.840.113549.1.9.3
	pub const CONTENT_TYPE: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x09, 0x03];
	/// 1.2.840.113549.1.9.4
	pub const MESSAGE_DIGEST: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x09, 0x04];
	/// 1.3.6.1.4.1.57264.1.8: Fulcio OIDC issuer (V2, DER-encoded).
	pub const FULCIO_ISSUER: &[u8] = &[0x2b, 0x06, 0x01, 0x04, 0x01, 0x83, 0xbf, 0x30, 0x01, 0x08];
	/// 1.3.6.1.4.1.57264.1.15: Fulcio source repository identifier.
	pub const FULCIO_REPOSITORY_ID: &[u8] =
		&[0x2b, 0x06, 0x01, 0x04, 0x01, 0x83, 0xbf, 0x30, 0x01, 0x0f];
	/// 1.3.6.1.4.1.57264.1.17: Fulcio source repository owner identifier.
	pub const FULCIO_OWNER_ID: &[u8] =
		&[0x2b, 0x06, 0x01, 0x04, 0x01, 0x83, 0xbf, 0x30, 0x01, 0x11];
}

/// A hash algorithm named by an X.509 or CMS algorithm identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Hash {
	Sha256,
	Sha384,
	Sha512,
}

impl Hash {
	/// The hash of a digest algorithm OID.
	pub fn from_digest_oid(oid: &[u8]) -> Option<Self> {
		match oid {
			oid::SHA256 => Some(Self::Sha256),
			oid::SHA384 => Some(Self::Sha384),
			oid::SHA512 => Some(Self::Sha512),
			_ => None,
		}
	}

	/// The hash of an `ecdsa-with-SHA*` signature algorithm OID.
	pub fn from_ecdsa_oid(oid: &[u8]) -> Option<Self> {
		match oid {
			oid::ECDSA_SHA256 => Some(Self::Sha256),
			oid::ECDSA_SHA384 => Some(Self::Sha384),
			oid::ECDSA_SHA512 => Some(Self::Sha512),
			_ => None,
		}
	}

	pub fn digest(self, data: &[u8]) -> Vec<u8> {
		match self {
			Self::Sha256 => Sha256::digest(data).to_vec(),
			Self::Sha384 => Sha384::digest(data).to_vec(),
			Self::Sha512 => Sha512::digest(data).to_vec(),
		}
	}
}

/// A public key that can check a signature.
#[derive(Clone, Debug)]
pub(crate) enum PublicKey {
	P256(P256Key),
	P384(P384Key),
	Ed25519(Ed25519Key),
}

impl PublicKey {
	/// Parses a DER `SubjectPublicKeyInfo`, refusing trailing bytes.
	pub fn from_spki_der(der: &[u8]) -> Result<Self, String> {
		let (rest, spki) = SubjectPublicKeyInfo::from_der(der)
			.map_err(|e| format!("malformed SubjectPublicKeyInfo: {e}"))?;
		if !rest.is_empty() {
			return Err("trailing bytes after SubjectPublicKeyInfo".into());
		}
		Self::from_spki(&spki)
	}

	pub fn from_spki(spki: &SubjectPublicKeyInfo<'_>) -> Result<Self, String> {
		let point = spki.subject_public_key.data.as_ref();
		match spki.algorithm.algorithm.as_bytes() {
			oid::EC_PUBLIC_KEY => {
				let curve = spki
					.algorithm
					.parameters
					.as_ref()
					.and_then(|p| p.as_oid().ok())
					.ok_or("EC public key without a named curve")?;
				match curve.as_bytes() {
					oid::P256 => P256Key::from_sec1_bytes(point)
						.map(Self::P256)
						.map_err(|_| "invalid P-256 public key".into()),
					oid::P384 => P384Key::from_sec1_bytes(point)
						.map(Self::P384)
						.map_err(|_| "invalid P-384 public key".into()),
					_ => Err(format!("unsupported EC curve {curve}")),
				}
			}
			oid::ED25519 => {
				if spki.algorithm.parameters.is_some() {
					return Err("Ed25519 public key with parameters".into());
				}
				Self::ed25519(point)
			}
			_ => Err(format!("unsupported public key algorithm {}", spki.algorithm.algorithm)),
		}
	}

	pub fn ed25519(raw: &[u8]) -> Result<Self, String> {
		let raw: &[u8; 32] = raw.try_into().map_err(|_| "Ed25519 public key is not 32 bytes")?;
		Ed25519Key::from_bytes(raw)
			.map(Self::Ed25519)
			.map_err(|_| "invalid Ed25519 public key".into())
	}

	/// Checks a DER-encoded ECDSA signature over an already computed digest.
	/// Always false for an Ed25519 key.
	pub fn verify_ecdsa_prehash(&self, digest: &[u8], der_signature: &[u8]) -> bool {
		match self {
			Self::P256(key) => P256Signature::from_der(der_signature)
				.is_ok_and(|sig| key.verify_prehash(digest, &sig).is_ok()),
			Self::P384(key) => P384Signature::from_der(der_signature)
				.is_ok_and(|sig| key.verify_prehash(digest, &sig).is_ok()),
			Self::Ed25519(_) => false,
		}
	}

	/// Checks a DER-encoded ECDSA signature over `message`, hashed with the
	/// curve's own hash: SHA-256 for P-256, SHA-384 for P-384. Always false
	/// for an Ed25519 key.
	pub fn verify_ecdsa_message(&self, message: &[u8], der_signature: &[u8]) -> bool {
		match self {
			Self::P256(_) => {
				self.verify_ecdsa_prehash(&Hash::Sha256.digest(message), der_signature)
			}
			Self::P384(_) => {
				self.verify_ecdsa_prehash(&Hash::Sha384.digest(message), der_signature)
			}
			Self::Ed25519(_) => false,
		}
	}

	/// Checks an Ed25519 signature over `message` under the strict rules
	/// (canonical encodings, no small-order keys). Always false for an ECDSA
	/// key.
	pub fn verify_ed25519(&self, message: &[u8], signature: &[u8]) -> bool {
		match self {
			Self::Ed25519(key) => <&[u8; 64]>::try_from(signature).is_ok_and(|sig| {
				key.verify_strict(message, &Ed25519Signature::from_bytes(sig)).is_ok()
			}),
			Self::P256(_) | Self::P384(_) => false,
		}
	}

	/// Checks an X.509 or CMS signature made with `ecdsa-with-SHA*`.
	pub fn verify_ecdsa(&self, algorithm: &[u8], signed: &[u8], der_signature: &[u8]) -> bool {
		match Hash::from_ecdsa_oid(algorithm) {
			Some(hash) => self.verify_ecdsa_prehash(&hash.digest(signed), der_signature),
			None => false,
		}
	}
}
