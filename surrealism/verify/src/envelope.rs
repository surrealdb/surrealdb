//! The SurrealDB extension signature envelope: a Sigstore bundle together
//! with the trust root that verifies it.
//!
//! ```json
//! {
//!   "mediaType": "application/vnd.surrealdb.extension-signature.v1+json",
//!   "bundle": { …a Sigstore bundle v0.3, unchanged… },
//!   "trustedRoot": { …a Sigstore trusted_root.json holding exactly the certificate
//!                     authority, transparency log and timestamp authority that verify it… }
//! }
//! ```
//!
//! The envelope carries everything needed to verify the signature offline, as
//! an SSH signature carries its public key. The embedded root says which
//! anchors the signer used; it does not make them trusted. [`crate::verify`]
//! never reads or validates it ([`bundle`]): it verifies the bundle against the
//! anchors of the [`crate::Signer`] it is given. [`crate::verify_offline`] and
//! [`crate::claims`] read it, for a client that has not yet chosen a signer.
//!
//! A bare Sigstore bundle, with no envelope, is still read as a signature. It
//! verifies only against a `Signer`'s anchors and has no embedded root, so
//! [`crate::verify_offline`] refuses it and its claims carry no anchors.

use std::borrow::Cow;

use serde_json::Value;

use crate::{Error, TrustedRoot};

/// The media type of the envelope.
pub const ENVELOPE_MEDIA_TYPE: &str = "application/vnd.surrealdb.extension-signature.v1+json";

/// A Sigstore `SIGNATURE`, split into its bundle and its embedded root.
pub(crate) struct Sigstore<'a> {
	/// The bundle's JSON bytes.
	pub(crate) bundle: Cow<'a, [u8]>,
	/// The embedded root, present only in an envelope.
	pub(crate) trusted_root: Option<TrustedRoot>,
}

fn malformed(why: impl std::fmt::Display) -> Error {
	Error::Malformed(format!("the signature envelope is malformed: {why}"))
}

/// Splits a Sigstore `SIGNATURE` (first byte `{`). An envelope must hold a
/// bundle and a trusted root with exactly one certificate authority, one
/// transparency log and one timestamp authority, and no other field.
/// Anything else is read as a bare bundle.
pub(crate) fn split(signature: &[u8]) -> Result<Sigstore<'_>, Error> {
	let json: Value = serde_json::from_slice(signature)
		.map_err(|e| Error::Malformed(format!("not a Sigstore bundle: {e}")))?;
	if json["mediaType"] != ENVELOPE_MEDIA_TYPE {
		return Ok(Sigstore {
			bundle: Cow::Borrowed(signature),
			trusted_root: None,
		});
	}
	let Value::Object(fields) = json else {
		return Err(malformed("it is not an object"));
	};
	if let Some(unknown) =
		fields.keys().find(|k| !matches!(k.as_str(), "mediaType" | "bundle" | "trustedRoot"))
	{
		return Err(malformed(format!("unknown field `{unknown}`")));
	}
	let bundle = fields.get("bundle").ok_or_else(|| malformed("it holds no bundle"))?;
	let root = fields.get("trustedRoot").ok_or_else(|| malformed("it holds no trustedRoot"))?;
	let trusted_root = TrustedRoot::from_value(root.clone())?;
	trusted_root.single_anchors()?;
	Ok(Sigstore {
		bundle: Cow::Owned(serde_json::to_vec(bundle).map_err(malformed)?),
		trusted_root: Some(trusted_root),
	})
}

/// The bundle of a Sigstore `SIGNATURE`: an envelope's `bundle` field, or a
/// bare bundle as it is. The embedded root is not read or validated, so a
/// verdict reached with this bundle cannot depend on it.
pub(crate) fn bundle(signature: &[u8]) -> Result<Cow<'_, [u8]>, Error> {
	let json: Value = serde_json::from_slice(signature)
		.map_err(|e| Error::Malformed(format!("not a Sigstore bundle: {e}")))?;
	if json["mediaType"] != ENVELOPE_MEDIA_TYPE {
		return Ok(Cow::Borrowed(signature));
	}
	let bundle = json.get("bundle").ok_or_else(|| malformed("it holds no bundle"))?;
	Ok(Cow::Owned(serde_json::to_vec(bundle).map_err(malformed)?))
}

/// The envelope for `bundle` and `trusted_root`, as `SIGNATURE` bytes.
pub(crate) fn envelope(bundle: &[u8], trusted_root: &TrustedRoot) -> Result<Vec<u8>, Error> {
	let bundle: Value = serde_json::from_slice(bundle)
		.map_err(|e| Error::Malformed(format!("not a Sigstore bundle: {e}")))?;
	serde_json::to_vec(&serde_json::json!({
		"mediaType": ENVELOPE_MEDIA_TYPE,
		"bundle": bundle,
		"trustedRoot": trusted_root.as_value(),
	}))
	.map_err(malformed)
}
