//! Signature verification for Surrealism extension archives.
//!
//! An archive's `surrealism/MANIFEST` lists every other file with its hash, and
//! `DIGEST` is `sha256(MANIFEST)`. `surrealism/SIGNATURE` is evidence that a
//! signer signed that manifest. [`verify`] decides whether a `SIGNATURE` proves
//! that the [`Signer`] recorded in a definition's `SIGNER` clause signed the
//! digest.
//!
//! [`verify`] is a pure function. It reads no clock, does no I/O and needs no
//! network. Every anchor it trusts comes from the [`Signer`] argument; the
//! crate ships no keys, no roots and no trust settings. The only time it uses
//! is the time a trusted timestamp authority attests, so a verdict depends only
//! on the three arguments and is the same on every node and on every day.
//!
//! Two signature formats are accepted, told apart by their first bytes:
//!
//! - **Sigstore bundle v0.3** (`{`): a short-lived Fulcio certificate, a Rekor v2 `hashedrekord`
//!   0.0.2 entry with an inclusion proof and a signed checkpoint, and an RFC 3161 timestamp. See
//!   [`SigstoreSigner`].
//! - **SSH signature** (`-----BEGIN SSH SIGNATURE-----`), namespace [`SSH_NAMESPACE`], hash
//!   `sha256`. See [`Signer::Key`].
//!
//! The crate builds for `wasm32-unknown-unknown`: it uses no `std::time`, no
//! threads and no C code.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;

mod der;
mod envelope;
mod keys;
mod rekor;
mod sigstore;
mod ssh;
mod trusted_root;
mod tsa;
mod x509;

pub use envelope::ENVELOPE_MEDIA_TYPE;
pub use trusted_root::{MAX_TRUSTED_ROOT_LEN, TrustedRoot};

/// The SSH signature namespace for Surrealism archives.
///
/// A signature made for another purpose, such as a git commit, carries another
/// namespace and is refused.
pub const SSH_NAMESPACE: &str = "surrealism";

/// The largest `SIGNATURE` accepted, in bytes.
///
/// A Sigstore bundle is a few KB, its envelope adds a trust root of a few KB
/// more (three certificate chains and a log key), and an SSH signature is a
/// few hundred bytes. The limit leaves room for long chains and bounds the
/// work an archive can make the verifier do before any cryptographic check
/// has failed.
pub const MAX_SIGNATURE_LEN: usize = 128 * 1024;

/// `sha256(MANIFEST)`: the value a `DIGEST` clause pins and a signature covers.
pub type ManifestDigest = [u8; 32];

/// The signer a definition records, with the anchor its signature must reach.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Signer {
	/// A Sigstore identity: anchors (`ca`, `log`, `tsa`) plus an identity
	/// rule.
	Sigstore(SigstoreSigner),
	/// An SSH public key in `authorized_keys` form: `<type> <base64> [comment]`.
	///
	/// The key is the whole anchor. The comment is not part of the key and
	/// is ignored when comparing.
	Key(String),
}

/// A Sigstore anchor and the identity rule the signing certificate must meet.
///
/// The anchors are trusted as given. A statement that names someone else's
/// certificate authority trusts that authority for every identity it issues,
/// which is why a client shows the anchor and warns when it is not Sigstore's
/// public root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigstoreSigner {
	/// DER certificates of the certificate authority (Fulcio): the
	/// intermediate that issues signing certificates and the roots above it,
	/// in any order.
	pub ca: Vec<Vec<u8>>,
	/// DER `SubjectPublicKeyInfo` of the transparency log (Rekor v2). Ed25519,
	/// ECDSA P-256 and ECDSA P-384 keys are accepted.
	pub log: Vec<u8>,
	/// DER certificates of the RFC 3161 timestamp authority: the signing
	/// certificate and the certificates above it, in any order.
	pub tsa: Vec<Vec<u8>>,
	/// The OIDC issuer, matched against certificate extension
	/// `1.3.6.1.4.1.57264.1.8`.
	pub issuer: String,
	/// The signing identity, matched against the certificate's only subject
	/// alternative name (a URI or an email address).
	pub identity: String,
	/// The numeric source repository ID, matched against certificate
	/// extension `1.3.6.1.4.1.57264.1.15`.
	///
	/// `None` requires the certificate to carry no such extension, so a
	/// statement that omits the ID cannot accept a certificate whose
	/// repository could have been renamed and re-registered by someone else.
	pub repository_id: Option<String>,
	/// The numeric repository owner ID, matched against certificate extension
	/// `1.3.6.1.4.1.57264.1.17`. `None` behaves as for `repository_id`.
	pub owner_id: Option<String>,
}

/// What a successful verification proved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VerifiedSigner {
	/// A Sigstore bundle met every check against a [`SigstoreSigner`].
	Sigstore {
		/// The OIDC issuer from the certificate.
		issuer: String,
		/// The subject alternative name from the certificate.
		identity: String,
		/// The source repository ID from the certificate, if it has one.
		repository_id: Option<String>,
		/// The repository owner ID from the certificate, if it has one.
		owner_id: Option<String>,
		/// When the timestamp authority saw the signature, in seconds since
		/// the Unix epoch.
		signed_at: i64,
		/// The entry's index in the transparency log.
		log_index: u64,
	},
	/// An SSH signature verified with the recorded key.
	Key {
		/// The key type, such as `ssh-ed25519`.
		key_type: String,
		/// The OpenSSH fingerprint, `SHA256:` followed by unpadded base64.
		fingerprint: String,
	},
}

/// The anchors of a [`SigstoreSigner`]: certificate authority chain,
/// transparency log key and timestamp authority chain, all DER.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SigstoreAnchors {
	/// See [`SigstoreSigner::ca`].
	pub ca: Vec<Vec<u8>>,
	/// See [`SigstoreSigner::log`].
	pub log: Vec<u8>,
	/// See [`SigstoreSigner::tsa`].
	pub tsa: Vec<Vec<u8>>,
}

/// What a `SIGNATURE` says about its signer, read without checking anything.
///
/// Claims are untrusted. They exist so that a client, which does not yet know
/// the signer, can build a [`Signer`] from them and the anchors it trusts,
/// show it to the person installing, and record it. Only a [`verify`] that
/// passes with that [`Signer`] proves them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Claims {
	/// From the leaf certificate of a Sigstore bundle.
	Sigstore {
		/// Certificate extension `1.3.6.1.4.1.57264.1.8`.
		issuer: String,
		/// The only subject alternative name.
		identity: String,
		/// Certificate extension `1.3.6.1.4.1.57264.1.15`, if present.
		repository_id: Option<String>,
		/// Certificate extension `1.3.6.1.4.1.57264.1.17`, if present.
		owner_id: Option<String>,
		/// The anchors the signature envelope embeds; `None` for a bare
		/// bundle. They are the signer's choice, not a trust decision.
		anchors: Option<SigstoreAnchors>,
	},
	/// From an SSH signature.
	Key {
		/// The signing key in `authorized_keys` form, `<type> <base64>`,
		/// ready for [`Signer::Key`].
		key: String,
	},
}

/// Reads the signer a `SIGNATURE` claims to come from. It parses and proves
/// nothing: see [`Claims`].
pub fn claims(signature: &[u8]) -> Result<Claims, Error> {
	if signature.len() > MAX_SIGNATURE_LEN {
		return Err(Error::TooLarge);
	}
	match Format::detect(signature) {
		Format::Sigstore => {
			let split = envelope::split(signature)?;
			let mut claims = sigstore::claims(&split.bundle)?;
			if let (
				Claims::Sigstore {
					anchors,
					..
				},
				Some(root),
			) = (&mut claims, &split.trusted_root)
			{
				*anchors = Some(root.single_anchors()?);
			}
			Ok(claims)
		}
		Format::Ssh => ssh::claims(signature),
		Format::Unknown => Err(Error::Malformed("the signature is in an unknown format".into())),
	}
}

/// Checks a signature envelope against the trust root it embeds, with the
/// identity rule its signing certificate claims, and returns what that
/// proved.
///
/// This proves that the signature is internally consistent and was made
/// under the embedded anchors, so it can be checked offline. It does not say
/// whether those anchors, or that identity, should be trusted: that is the
/// caller's decision, recorded as a [`Signer`] and checked with [`verify`]. A
/// bare bundle and an SSH signature carry no root and are refused.
pub fn verify_offline(digest: &ManifestDigest, signature: &[u8]) -> Result<VerifiedSigner, Error> {
	if signature.len() > MAX_SIGNATURE_LEN {
		return Err(Error::TooLarge);
	}
	let Claims::Sigstore {
		issuer,
		identity,
		repository_id,
		owner_id,
		anchors,
	} = claims(signature)?
	else {
		return Err(Error::FormatMismatch {
			expected: "a signature envelope",
			found: Format::Ssh.name(),
		});
	};
	let Some(SigstoreAnchors {
		ca,
		log,
		tsa,
	}) = anchors
	else {
		return Err(Error::FormatMismatch {
			expected: "a signature envelope",
			found: "a bare Sigstore bundle",
		});
	};
	verify(
		digest,
		signature,
		&Signer::Sigstore(SigstoreSigner {
			ca,
			log,
			tsa,
			issuer,
			identity,
			repository_id,
			owner_id,
		}),
	)
}

/// Why a signature does not prove the signer.
///
/// Every variant means "refuse". The variants exist so that callers and
/// clients can say which check failed.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
	#[error("the signature is larger than {MAX_SIGNATURE_LEN} bytes")]
	TooLarge,
	#[error("the signature is {found}, but the signer expects {expected}")]
	FormatMismatch {
		expected: &'static str,
		found: &'static str,
	},
	#[error("the signature is malformed: {0}")]
	Malformed(String),
	#[error("the signer is invalid: {0}")]
	InvalidSigner(String),
	#[error("the signature covers a different manifest digest")]
	DigestMismatch,
	#[error("the signature does not verify with the signing key")]
	BadSignature,
	#[error("the signing certificate does not chain to the `ca` anchor: {0}")]
	Chain(String),
	#[error("no timestamp from the `tsa` anchor proves the signing time: {0}")]
	Timestamp(String),
	#[error("the signing time lies outside the signing certificate's validity")]
	OutsideValidity,
	#[error("the transparency log does not prove the entry: {0}")]
	Log(String),
	#[error("the certificate's {field} does not match the signer")]
	Identity {
		field: &'static str,
	},
	#[error("the signature uses namespace {0:?}, not {SSH_NAMESPACE:?}")]
	Namespace(String),
	#[error("the signature was made with a different key")]
	KeyMismatch,
	#[error("{0}")]
	TrustedRoot(String),
}

/// Checks that `signature`, the bytes of an archive's `SIGNATURE`, proves that
/// `signer` signed `digest`.
///
/// For a Sigstore signer every one of these holds, or the call fails:
///
/// - the bundle's digest, if it states one, equals `digest`;
/// - the leaf certificate's ECDSA P-256 key signed `digest`;
/// - an RFC 3161 timestamp over that signature verifies with a `tsa` certificate valid at the
///   stamped time, and no timestamp from `tsa` places the signature outside the leaf certificate's
///   validity;
/// - the leaf certificate chains to `ca`, with every certificate on the path valid at the stamped
///   time;
/// - a Rekor v2 `hashedrekord` 0.0.2 entry holds this digest, signature and certificate, its
///   inclusion proof leads to a checkpoint root, and `log` signed that checkpoint;
/// - the certificate's issuer, subject alternative name, repository ID and owner ID match the
///   signer.
///
/// For a key signer the SSH signature carries namespace [`SSH_NAMESPACE`] and
/// hash `sha256`, its key blob equals the recorded key's, and it verifies over
/// `digest`.
pub fn verify(
	digest: &ManifestDigest,
	signature: &[u8],
	signer: &Signer,
) -> Result<VerifiedSigner, Error> {
	if signature.len() > MAX_SIGNATURE_LEN {
		return Err(Error::TooLarge);
	}
	let format = Format::detect(signature);
	match (signer, format) {
		// The embedded root, if any, is never read: only the signer's anchors
		// are trusted.
		(Signer::Sigstore(signer), Format::Sigstore) => {
			sigstore::verify(digest, &envelope::bundle(signature)?, signer)
		}
		(Signer::Key(key), Format::Ssh) => ssh::verify(digest, signature, key),
		(Signer::Sigstore(_), found) => Err(Error::FormatMismatch {
			expected: Format::Sigstore.name(),
			found: found.name(),
		}),
		(Signer::Key(_), found) => Err(Error::FormatMismatch {
			expected: Format::Ssh.name(),
			found: found.name(),
		}),
	}
}

/// Decodes a base64 `bytes` field of a bundle. Like the protobuf JSON decoders
/// that write bundles, it ignores line breaks inside the value.
fn decode_base64(field: &str, value: &str) -> Result<Vec<u8>, Error> {
	let decoded = if value.contains(['\r', '\n']) {
		let joined: String = value.chars().filter(|c| !matches!(c, '\r' | '\n')).collect();
		BASE64.decode(joined)
	} else {
		BASE64.decode(value)
	};
	decoded.map_err(|_| Error::Malformed(format!("{field} is not valid base64")))
}

/// The format of a `SIGNATURE` file, decided by its first bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
	Sigstore,
	Ssh,
	Unknown,
}

impl Format {
	fn detect(signature: &[u8]) -> Self {
		if signature.first() == Some(&b'{') {
			Self::Sigstore
		} else if signature.starts_with(ssh::ARMOR_BEGIN.as_bytes()) {
			Self::Ssh
		} else {
			Self::Unknown
		}
	}

	fn name(self) -> &'static str {
		match self {
			Self::Sigstore => "a Sigstore bundle",
			Self::Ssh => "an SSH signature",
			Self::Unknown => "in an unknown format",
		}
	}
}
