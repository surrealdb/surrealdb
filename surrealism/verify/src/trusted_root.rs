//! Sigstore `trusted_root.json`: anchors in Sigstore's own format
//! (`application/vnd.dev.sigstore.trustedroot+json`).
//!
//! A signer reduces a full root to the entries that verify its bundle
//! ([`TrustedRoot::select`]) and embeds it in the signature envelope
//! ([`TrustedRoot::seal`], see [`crate::ENVELOPE_MEDIA_TYPE`]), so the signature
//! verifies offline.
//!
//! A root says only which anchors someone chose. Nothing here makes a root
//! trusted: the anchors a server verifies against come from a definition's
//! `SIGNER`, never from a signature.

use serde_json::{Value, json};

use crate::{
	Claims, Error, ManifestDigest, Signer, SigstoreAnchors, SigstoreSigner, claims, decode_base64,
	envelope, verify,
};

/// The largest `trusted_root.json` accepted, in bytes. Sigstore's public root
/// is a few KB; a root reduced to one signature's anchors is smaller still.
pub const MAX_TRUSTED_ROOT_LEN: usize = 1024 * 1024;

const MEDIA_TYPE_PREFIX: &str = "application/vnd.dev.sigstore.trustedroot+json";
const MEDIA_TYPE: &str = "application/vnd.dev.sigstore.trustedroot+json;version=0.1";

/// The most anchor combinations [`TrustedRoot::select`] tries. Sigstore's
/// public root holds a handful of each kind; this bounds the work a crafted
/// root can cause.
const MAX_COMBINATIONS: usize = 256;

/// A parsed `trusted_root.json`, kept as its JSON so that the entries it
/// writes back are the ones it read, field for field.
#[derive(Clone, Debug, PartialEq)]
pub struct TrustedRoot {
	json: Value,
}

fn malformed(why: impl std::fmt::Display) -> Error {
	Error::TrustedRoot(format!("the trusted root is malformed: {why}"))
}

/// The DER certificates of a certificate authority or timestamp authority
/// entry's `certChain`.
fn chain(entry: &Value, what: &str) -> Result<Vec<Vec<u8>>, Error> {
	let certs = entry["certChain"]["certificates"]
		.as_array()
		.ok_or_else(|| malformed(format!("a {what} has no certChain.certificates")))?;
	if certs.is_empty() {
		return Err(malformed(format!("a {what} has an empty certificate chain")));
	}
	certs
		.iter()
		.map(|c| {
			let raw = c["rawBytes"]
				.as_str()
				.ok_or_else(|| malformed(format!("a {what} certificate has no rawBytes")))?;
			decode_base64("certificate rawBytes", raw)
		})
		.collect()
}

/// The DER `SubjectPublicKeyInfo` of a transparency log entry.
fn log_key(entry: &Value) -> Result<Vec<u8>, Error> {
	let raw = entry["publicKey"]["rawBytes"]
		.as_str()
		.ok_or_else(|| malformed("a transparency log has no publicKey.rawBytes"))?;
	decode_base64("publicKey rawBytes", raw)
}

impl TrustedRoot {
	/// Parses a `trusted_root.json`. Its media type must be Sigstore's trusted
	/// root, and every certificate authority, transparency log and timestamp
	/// authority entry must carry its key material.
	pub fn parse(bytes: &[u8]) -> Result<Self, Error> {
		if bytes.len() > MAX_TRUSTED_ROOT_LEN {
			return Err(malformed(format!("it is larger than {MAX_TRUSTED_ROOT_LEN} bytes")));
		}
		Self::from_value(serde_json::from_slice(bytes).map_err(malformed)?)
	}

	/// As [`TrustedRoot::parse`], from parsed JSON.
	pub(crate) fn from_value(json: Value) -> Result<Self, Error> {
		let media = json["mediaType"].as_str().unwrap_or_default();
		if !media.starts_with(MEDIA_TYPE_PREFIX) {
			return Err(malformed(format!("mediaType {media:?} is not a Sigstore trusted root")));
		}
		let root = Self {
			json,
		};
		for ca in root.entries("certificateAuthorities") {
			chain(ca, "certificate authority")?;
		}
		for tsa in root.entries("timestampAuthorities") {
			chain(tsa, "timestamp authority")?;
		}
		for log in root.entries("tlogs") {
			log_key(log)?;
		}
		Ok(root)
	}

	pub(crate) fn as_value(&self) -> &Value {
		&self.json
	}

	/// The anchors of a root that holds exactly one certificate authority,
	/// one transparency log and one timestamp authority, as an envelope's does.
	pub(crate) fn single_anchors(&self) -> Result<SigstoreAnchors, Error> {
		let one = |field: &str| match self.entries(field) {
			[entry] => Ok(entry),
			entries => Err(Error::Malformed(format!(
				"the embedded trusted root holds {} {field} entries, not one",
				entries.len()
			))),
		};
		Ok(SigstoreAnchors {
			ca: chain(one("certificateAuthorities")?, "certificate authority")?,
			log: log_key(one("tlogs")?)?,
			tsa: chain(one("timestampAuthorities")?, "timestamp authority")?,
		})
	}

	/// The signature envelope for `bundle`, a Sigstore bundle over `digest`,
	/// holding this root reduced to the anchors that verify it
	/// ([`TrustedRoot::select`]). The envelope is checked with
	/// [`crate::verify_offline`] before it is returned, so it always verifies
	/// from its own contents.
	pub fn seal(&self, digest: &ManifestDigest, bundle: &[u8]) -> Result<Vec<u8>, Error> {
		if envelope::split(bundle)?.trusted_root.is_some() {
			return Err(Error::Malformed("the bundle is already a signature envelope".into()));
		}
		let (_, reduced) = self.select(digest, bundle)?;
		let sealed = envelope::envelope(bundle, &reduced)?;
		crate::verify_offline(digest, &sealed)?;
		Ok(sealed)
	}

	fn entries(&self, field: &str) -> &[Value] {
		self.json[field].as_array().map(Vec::as_slice).unwrap_or_default()
	}

	/// The anchors in this root that verify `signature`, a Sigstore bundle,
	/// over `digest`, and this root reduced to them.
	///
	/// The identity rule is the one the signing certificate claims (see
	/// [`claims`]), so the returned [`SigstoreSigner`] is what a client would
	/// record for this signer under these anchors. Every combination of one
	/// certificate authority, one transparency log and one timestamp authority
	/// is tried until one verifies. The reduced root holds that certificate
	/// authority, log and timestamp authority, entry for entry as this root
	/// has them, and nothing else.
	pub fn select(
		&self,
		digest: &ManifestDigest,
		signature: &[u8],
	) -> Result<(SigstoreSigner, TrustedRoot), Error> {
		let Claims::Sigstore {
			issuer,
			identity,
			repository_id,
			owner_id,
			..
		} = claims(signature)?
		else {
			return Err(Error::FormatMismatch {
				expected: "a Sigstore bundle",
				found: "an SSH signature",
			});
		};
		let cas = self.entries("certificateAuthorities");
		let logs = self.entries("tlogs");
		let tsas = self.entries("timestampAuthorities");
		if cas.len().saturating_mul(logs.len()).saturating_mul(tsas.len()) > MAX_COMBINATIONS {
			return Err(malformed(format!(
				"it holds more than {MAX_COMBINATIONS} combinations of anchors"
			)));
		}
		let mut last = None;
		for ca in cas {
			for log in logs {
				for tsa in tsas {
					let signer = SigstoreSigner {
						ca: chain(ca, "certificate authority")?,
						log: log_key(log)?,
						tsa: chain(tsa, "timestamp authority")?,
						issuer: issuer.clone(),
						identity: identity.clone(),
						repository_id: repository_id.clone(),
						owner_id: owner_id.clone(),
					};
					match verify(digest, signature, &Signer::Sigstore(signer.clone())) {
						Ok(_) => {
							let reduced = TrustedRoot {
								json: json!({
									"mediaType": self.json["mediaType"].as_str().unwrap_or(MEDIA_TYPE),
									"tlogs": [log],
									"certificateAuthorities": [ca],
									"ctlogs": [],
									"timestampAuthorities": [tsa],
								}),
							};
							return Ok((signer, reduced));
						}
						Err(e) => last = Some(e),
					}
				}
			}
		}
		Err(Error::TrustedRoot(match last {
			Some(e) => format!("no anchors in the trusted root verify the signature: {e}"),
			None => "the trusted root holds no certificate authority, transparency log or \
			         timestamp authority"
				.into(),
		}))
	}

	/// The root as `trusted_root.json` bytes.
	pub fn to_json(&self) -> Result<Vec<u8>, Error> {
		serde_json::to_vec_pretty(&self.json)
			.map_err(|e| Error::TrustedRoot(format!("the trusted root cannot be written: {e}")))
	}
}
