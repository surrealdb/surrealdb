//! Sigstore bundle v0.3 with a Rekor v2 entry and an RFC 3161 timestamp.
//!
//! The chain of evidence, and the anchor each link ends at:
//!
//! ```text
//! signature over DIGEST
//!   └─ verifies with the leaf certificate's key        (bundle)
//!        └─ leaf certificate chains to                  anchor: ca
//! RFC 3161 timestamp over the signature
//!   └─ signed by                                        anchor: tsa
//!   └─ stamped time lies inside the leaf's validity
//! Rekor entry holds DIGEST, signature and certificate
//!   └─ inclusion proof leads to the checkpoint root
//!        └─ checkpoint signed by                        anchor: log
//! leaf issuer, SAN, repository ID, owner ID             rule: the signer
//! ```

use serde::Deserialize;
use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::GeneralName;

use crate::der::Stamp;
use crate::keys::{PublicKey, oid};
use crate::rekor::{self, TlogEntry};
use crate::{
	Claims, Error, ManifestDigest, SigstoreSigner, VerifiedSigner, decode_base64, der, tsa, x509,
};

/// The most timestamps and log entries a bundle may carry. A real bundle has
/// one of each; the bound limits how many signature checks a hostile bundle
/// can demand before it is refused.
const MAX_TIMESTAMPS: usize = 4;
const MAX_TLOG_ENTRIES: usize = 4;

/// The media types of a v0.3 bundle: the current spelling and the older one.
const MEDIA_TYPES: &[&str] = &[
	"application/vnd.dev.sigstore.bundle.v0.3+json",
	"application/vnd.dev.sigstore.bundle+json;version=0.3",
];

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Bundle {
	media_type: String,
	verification_material: VerificationMaterial,
	message_signature: Option<MessageSignature>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct VerificationMaterial {
	certificate: Option<RawBytes>,
	#[serde(default)]
	tlog_entries: Vec<TlogEntry>,
	timestamp_verification_data: Option<TimestampVerificationData>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawBytes {
	raw_bytes: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TimestampVerificationData {
	#[serde(default)]
	rfc3161_timestamps: Vec<Rfc3161Timestamp>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Rfc3161Timestamp {
	signed_timestamp: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageSignature {
	message_digest: Option<HashOutput>,
	signature: String,
}

#[derive(Deserialize)]
struct HashOutput {
	algorithm: String,
	digest: String,
}

fn malformed(message: impl Into<String>) -> Error {
	Error::Malformed(message.into())
}

pub(crate) fn verify(
	digest: &ManifestDigest,
	bundle: &[u8],
	signer: &SigstoreSigner,
) -> Result<VerifiedSigner, Error> {
	// The anchors come first: a bad statement is reported as such, whatever
	// the archive holds.
	let ca = x509::parse_all(&signer.ca).map_err(|e| Error::InvalidSigner(format!("ca: {e}")))?;
	let tsa_anchors =
		x509::parse_all(&signer.tsa).map_err(|e| Error::InvalidSigner(format!("tsa: {e}")))?;
	if ca.is_empty() || tsa_anchors.is_empty() {
		return Err(Error::InvalidSigner("ca and tsa must each hold a certificate".into()));
	}
	let log = PublicKey::from_spki_der(&signer.log)
		.map_err(|e| Error::InvalidSigner(format!("log: {e}")))?;

	let bundle: Bundle = serde_json::from_slice(bundle)
		.map_err(|e| malformed(format!("not a Sigstore bundle: {e}")))?;
	if !MEDIA_TYPES.contains(&bundle.media_type.as_str()) {
		return Err(malformed(format!("unsupported bundle media type {}", bundle.media_type)));
	}
	let message = bundle
		.message_signature
		.ok_or_else(|| malformed("the bundle holds no message signature"))?;
	let material = bundle.verification_material;
	let leaf_der = decode_base64(
		"certificate",
		&material
			.certificate
			.ok_or_else(|| malformed("the bundle holds no certificate"))?
			.raw_bytes,
	)?;
	let signature = decode_base64("signature", &message.signature)?;

	// 1. The signature is over this digest, by the leaf certificate's key.
	if let Some(stated) = &message.message_digest
		&& (stated.algorithm != "SHA2_256"
			|| decode_base64("messageDigest", &stated.digest)? != digest)
	{
		return Err(Error::DigestMismatch);
	}
	let leaf = x509::parse(&leaf_der).map_err(|e| malformed(format!("leaf certificate: {e}")))?;
	check_leaf_usage(&leaf)?;
	let leaf_key = x509::public_key(&leaf).map_err(|e| malformed(format!("leaf key: {e}")))?;
	if !matches!(leaf_key, PublicKey::P256(_)) || !leaf_key.verify_ecdsa_prehash(digest, &signature)
	{
		return Err(Error::BadSignature);
	}

	// 2. A trusted timestamp fixes when the signature existed.
	let signed_at =
		signing_time(&material.timestamp_verification_data, &signature, &leaf, &tsa_anchors)?;

	// 3. The leaf chains to `ca`, valid at that time.
	x509::validate_path(&leaf, false, &ca, signed_at).map_err(Error::Chain)?;

	// 4. The signing is in the log.
	if material.tlog_entries.len() > MAX_TLOG_ENTRIES {
		return Err(malformed("the bundle holds too many transparency log entries"));
	}
	let mut log_error = Error::Log("the bundle holds no transparency log entry".into());
	let mut log_index = None;
	for entry in &material.tlog_entries {
		match rekor::verify_entry(entry, &log, &signer.log, digest, &signature, &leaf_der) {
			Ok(index) => {
				log_index = Some(index);
				break;
			}
			Err(e) => log_error = e,
		}
	}
	let log_index = log_index.ok_or(log_error)?;

	// 5. The certificate names the recorded identity.
	let issuer = utf8_extension(&leaf, oid::FULCIO_ISSUER)?;
	if issuer.as_deref() != Some(signer.issuer.as_str()) {
		return Err(Error::Identity {
			field: "issuer",
		});
	}
	let identity = subject_alternative_name(&leaf)?;
	if identity != signer.identity {
		return Err(Error::Identity {
			field: "identity",
		});
	}
	let repository_id = utf8_extension(&leaf, oid::FULCIO_REPOSITORY_ID)?;
	if repository_id != signer.repository_id {
		return Err(Error::Identity {
			field: "repository_id",
		});
	}
	let owner_id = utf8_extension(&leaf, oid::FULCIO_OWNER_ID)?;
	if owner_id != signer.owner_id {
		return Err(Error::Identity {
			field: "owner_id",
		});
	}

	Ok(VerifiedSigner::Sigstore {
		issuer: signer.issuer.clone(),
		identity,
		repository_id,
		owner_id,
		signed_at: signed_at.earliest,
		log_index,
	})
}

/// Reads the identity the bundle's leaf certificate names.
pub(crate) fn claims(bundle: &[u8]) -> Result<Claims, Error> {
	let bundle: Bundle = serde_json::from_slice(bundle)
		.map_err(|e| malformed(format!("not a Sigstore bundle: {e}")))?;
	if !MEDIA_TYPES.contains(&bundle.media_type.as_str()) {
		return Err(malformed(format!("unsupported bundle media type {}", bundle.media_type)));
	}
	let leaf_der = decode_base64(
		"certificate",
		&bundle
			.verification_material
			.certificate
			.ok_or_else(|| malformed("the bundle holds no certificate"))?
			.raw_bytes,
	)?;
	let leaf = x509::parse(&leaf_der).map_err(|e| malformed(format!("leaf certificate: {e}")))?;
	Ok(Claims::Sigstore {
		issuer: utf8_extension(&leaf, oid::FULCIO_ISSUER)?
			.ok_or_else(|| malformed("the leaf certificate names no issuer"))?,
		identity: subject_alternative_name(&leaf)
			.map_err(|_| malformed("the leaf certificate has no single identity"))?,
		repository_id: utf8_extension(&leaf, oid::FULCIO_REPOSITORY_ID)?,
		owner_id: utf8_extension(&leaf, oid::FULCIO_OWNER_ID)?,
		anchors: None,
	})
}

/// A Fulcio signing certificate: an end entity whose key may sign code.
fn check_leaf_usage(leaf: &X509Certificate<'_>) -> Result<(), Error> {
	let digital_signature =
		matches!(leaf.key_usage(), Ok(Some(ku)) if ku.value.digital_signature());
	let code_signing = matches!(leaf.extended_key_usage(), Ok(Some(eku)) if eku.value.code_signing);
	let ca = matches!(leaf.basic_constraints(), Ok(Some(bc)) if bc.value.ca);
	if !digital_signature || !code_signing || ca {
		return Err(Error::Chain("the leaf certificate is not a code-signing certificate".into()));
	}
	Ok(())
}

/// The time attested by the bundle's timestamps.
///
/// Timestamps that do not verify against `tsa` are ignored: anyone can add
/// one. At least one must verify, and every one that verifies must place the
/// signature inside the leaf certificate's validity. A key copied out of a CI
/// job and used after its certificate expired cannot pass, because any
/// timestamp `tsa` signs for it carries a time after expiry.
fn signing_time(
	data: &Option<TimestampVerificationData>,
	signature: &[u8],
	leaf: &X509Certificate<'_>,
	anchors: &[X509Certificate<'_>],
) -> Result<Stamp, Error> {
	let timestamps = data.as_ref().map(|d| d.rfc3161_timestamps.as_slice()).unwrap_or_default();
	if timestamps.len() > MAX_TIMESTAMPS {
		return Err(malformed("the bundle holds too many timestamps"));
	}
	let mut last_error = "the bundle holds no RFC 3161 timestamp".to_string();
	let mut verified = None;
	for ts in timestamps {
		let token = decode_base64("signedTimestamp", &ts.signed_timestamp)?;
		match tsa::verify(&token, signature, anchors) {
			Ok(time) => {
				if !x509::valid_at(leaf, time) {
					return Err(Error::OutsideValidity);
				}
				verified.get_or_insert(time);
			}
			Err(e) => last_error = e,
		}
	}
	verified.ok_or(Error::Timestamp(last_error))
}

/// The value of a Fulcio extension that holds a DER `UTF8String`.
fn utf8_extension(cert: &X509Certificate<'_>, oid: &[u8]) -> Result<Option<String>, Error> {
	let Some(ext) = cert.extensions().iter().find(|e| e.oid.as_bytes() == oid) else {
		return Ok(None);
	};
	let value = der::single(ext.value, der::UTF8_STRING)
		.map_err(|e| malformed(format!("certificate extension {}: {e}", ext.oid)))?;
	String::from_utf8(value.to_vec())
		.map(Some)
		.map_err(|_| malformed(format!("certificate extension {} is not UTF-8", ext.oid)))
}

/// The certificate's only subject alternative name, a URI or an email address.
fn subject_alternative_name(cert: &X509Certificate<'_>) -> Result<String, Error> {
	let san = cert
		.subject_alternative_name()
		.map_err(|e| malformed(format!("subject alternative name: {e}")))?
		.ok_or(Error::Identity {
			field: "identity",
		})?;
	match san.value.general_names.as_slice() {
		[GeneralName::URI(name)] | [GeneralName::RFC822Name(name)] => Ok((*name).to_string()),
		_ => Err(Error::Identity {
			field: "identity",
		}),
	}
}
