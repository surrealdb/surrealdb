//! Signed URLs for bucket objects.
//!
//! A browser cannot hold a SurrealDB session in an `<img src>`, and a
//! third-party uploader cannot hold one either. A presigned URL stands in for
//! the session: `file::presign_get` and `file::presign_put` evaluate the
//! bucket's `PERMISSIONS` clause once, then hand out a URL that the
//! bucket route (`[/ns/{ns}[/db/{db}]]/bucket/...`) accepts without evaluating
//! anything further.
//!
//! That is the bargain, and it is worth stating plainly. The URL is a **bearer
//! credential**. Until it expires it works for whoever holds it, it outlives
//! the session that minted it, it outlives the removal of the user who minted
//! it, and a `PERMISSIONS` clause tightened afterwards does not reach it. The
//! levers against an issued URL are its expiry, the bucket's
//! `presign_nonce`, and changing `SURREAL_BUCKET_PRESIGN_SECRET`.
//!
//! Two independent bindings keep a URL usable only where it was meant to be:
//!
//! - The **key** binds the tenant. The ids of the bucket's level, the bucket name and the nonce go
//!   into the derivation, never into the message, so a URL replayed against another level,
//!   namespace or database derives a different key and fails. There is no equality check for a
//!   refactor to drop.
//! - The **signature** binds the request. The method, the encoded path and every query parameter
//!   are covered, so the object, the expiry and the size limit cannot be edited.

mod canonical;
mod key;

use std::time::Duration;

use base64::alphabet::URL_SAFE;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use base64::engine::{DecodePaddingMode, Engine as _};
use chrono::{DateTime, TimeZone as _, Utc};
use hmac::Mac as _;
use subtle::ConstantTimeEq;
use surrealdb_types::{Datetime as PublicDatetime, SurrealValue};

use self::key::SigningKey;
pub(crate) use self::key::{BucketScope, ScopeLevel};
use crate::buc::PresignSecret;
use crate::buc::store::ObjectKey;
use crate::val::convert_public::convert_public_value_to_internal;
use crate::val::{CoerceError, Number, Object, Value, convert_value_to_public_value};

/// Signatures ride in a query string, so the alphabet has to be the URL-safe
/// one: the standard alphabet's `+` and `/` would need escaping, and a proxy
/// that re-encodes them would break an otherwise valid URL.
///
/// Unpadded, because `=` is equally awkward there.
const B64: GeneralPurpose = GeneralPurpose::new(
	&URL_SAFE,
	GeneralPurposeConfig::new()
		.with_encode_padding(false)
		.with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

/// What a presigned URL authorises.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PresignOp {
	Get,
	Put,
}

impl PresignOp {
	pub(crate) fn method(self) -> &'static str {
		match self {
			PresignOp::Get => "GET",
			PresignOp::Put => "PUT",
		}
	}
}

/// The signed query parameters of a presigned URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PresignParams {
	/// Unix seconds after which the URL stops working.
	pub exp: i64,
	/// Byte ceiling for an upload. `None` on a download.
	pub max: Option<u64>,
	/// Whether an upload may replace an existing object.
	pub overwrite: bool,
	/// The media type an upload must declare, and the one stored with it.
	/// `None` means it must declare none. Always `None` on a download.
	pub content_type: Option<String>,
}

/// What a verified URL permits.
///
/// Carries no operation: a caller passes the one it is checking into `verify`,
/// and a signature that verifies is one that named that operation.
#[derive(Clone, Debug)]
pub(crate) struct PresignGrant {
	pub max: Option<u64>,
	pub overwrite: bool,
	pub content_type: Option<String>,
}

/// Why a presigned URL was refused.
///
/// A caller reports every variant except `Expired` as `401`. The variants are
/// for the node's logs: telling a client *which* part of a signature failed
/// hands it an oracle.
#[derive(Debug, thiserror::Error)]
pub(crate) enum PresignError {
	/// Unreachable with HMAC, which accepts a key of any length.
	#[error("A key of an invalid length was supplied to a MAC")]
	InvalidKey,
	#[error("The presigned URL is malformed")]
	Malformed,
	#[error("The presigned URL has an invalid signature")]
	BadSignature,
	#[error("The presigned URL has expired")]
	Expired,
}

/// Sign a request, returning its path, its query string and the expiry it
/// encodes.
///
/// The URL is not assembled here: only the caller knows the configured public
/// origin, and whether there is one.
pub(crate) fn sign(
	secret: &PresignSecret,
	scope: BucketScope<'_>,
	op: PresignOp,
	key: &ObjectKey,
	params: &PresignParams,
) -> Result<(String, String, DateTime<Utc>), PresignError> {
	let signing_key = scope.signing_key(secret)?;

	let path = canonical::path(&scope.level, scope.bucket, key);
	let query = canonical::query(params);
	let signature = mac(&signing_key, &canonical::blob(op, &path, &query))?;

	let expires = Utc.timestamp_opt(params.exp, 0).single().ok_or(PresignError::Malformed)?;

	// `sig` is appended after the blob is built, so the signature covers every
	// other parameter and never itself.
	Ok((path, format!("?{query}&sig={signature}"), expires))
}

/// Verify a request against the signature it presents.
///
/// `key` must already be the normalised object key that this request resolves
/// to. Re-encoding the path from it is what makes the identity that was
/// verified and the identity that gets served the same object.
pub(crate) fn verify(
	secret: &PresignSecret,
	scope: BucketScope<'_>,
	op: PresignOp,
	key: &ObjectKey,
	params: PresignParams,
	presented: &str,
	now: i64,
) -> Result<PresignGrant, PresignError> {
	let signing_key = scope.signing_key(secret)?;

	let path = canonical::path(&scope.level, scope.bucket, key);
	let query = canonical::query(&params);
	let expected = mac(&signing_key, &canonical::blob(op, &path, &query))?;

	if !bool::from(expected.as_bytes().ct_eq(presented.as_bytes())) {
		return Err(PresignError::BadSignature);
	}

	// After the signature, never before. `exp` is inside the blob, so a URL that
	// verifies is one whose expiry has not been edited.
	if now > params.exp {
		return Err(PresignError::Expired);
	}

	Ok(PresignGrant {
		max: params.max,
		overwrite: params.overwrite,
		content_type: params.content_type,
	})
}

/// Base64url, unpadded, of the HMAC over `blob`.
fn mac(signing_key: &SigningKey, blob: &str) -> Result<String, PresignError> {
	let mut hmac = key::mac(signing_key)?;
	hmac.update(blob.as_bytes());
	Ok(B64.encode(hmac.finalize().into_bytes()))
}

/// How long a presigned URL lasts when the caller does not say.
///
/// Short on purpose. A caller that wants longer asks for it, and the node's
/// ceiling still applies on top.
const DEFAULT_EXPIRES: Duration = Duration::from_secs(15 * 60);

/// The options object accepted by `file::presign_get` and `file::presign_put`.
///
/// Deserialised through `SurrealValue`, so the accepted shape is the struct
/// definition and a caller gets the same field-name and type diagnostics every
/// other object-shaped argument produces.
#[derive(Clone, Debug, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
#[surreal(default)]
pub(crate) struct PresignOptions {
	pub expires: Duration,
	/// Signed, because SurrealQL has no unsigned integer. [`Self::max_size`]
	/// is the accessor that rejects a negative one.
	pub max_size: Option<i64>,
	pub overwrite: bool,
	/// The media type the upload must declare. [`Self::content_type`] is the
	/// accessor that validates it.
	pub content_type: Option<String>,
}

impl Default for PresignOptions {
	fn default() -> Self {
		Self {
			expires: DEFAULT_EXPIRES,
			max_size: None,
			overwrite: true,
			content_type: None,
		}
	}
}

impl PresignOptions {
	/// The options a download URL accepts.
	///
	/// `max_size` and `overwrite` describe an upload, and a download URL does
	/// nothing with either. Accepting them and dropping them would be the same
	/// silent-widening bug [`Self::from_object`] refuses a typo for.
	const GET_KEYS: [&'static str; 1] = ["expires"];

	/// The options an upload URL accepts.
	const PUT_KEYS: [&'static str; 4] = ["expires", "max_size", "overwrite", "content_type"];

	/// The media type the caller asked to bind into the URL, normalised.
	pub(crate) fn content_type(&self) -> anyhow::Result<Option<String>> {
		crate::buc::store::normalise_content_type(self.content_type.clone())
			.map_err(|e| anyhow::anyhow!("Invalid file::presign_put option `content_type`: {e}"))
	}

	/// The byte ceiling the caller asked for.
	///
	/// A negative value is refused rather than clamped: it is a mistake in the
	/// caller's arithmetic, and silently reading it as zero or as unlimited
	/// would each be wrong in a different direction.
	pub(crate) fn max_size(&self) -> Result<Option<u64>, CoerceError> {
		self.max_size
			.map(|n| {
				u64::try_from(n).map_err(|_| CoerceError::InvalidKind {
					from: Value::Number(Number::Int(n)),
					into: "non-negative int".into(),
				})
			})
			.transpose()
	}

	/// Read the options out of the object a caller passed, for `op`.
	///
	/// A key this operation does not act on is an error rather than something
	/// to ignore, and the accepted set is therefore the operation's own rather
	/// than the union of both. Every option here narrows what a bearer
	/// credential may do, so one that is quietly dropped hands out a URL more
	/// permissive than the one that was asked for -- whether it was dropped
	/// because it was misspelled (`overwrit: false`) or because it does not
	/// apply (`overwrite: false` on a download). Nothing in the result would
	/// show either.
	pub(crate) fn from_object(op: PresignOp, obj: Object) -> anyhow::Result<Self> {
		let accepted: &[&str] = match op {
			PresignOp::Get => &Self::GET_KEYS,
			PresignOp::Put => &Self::PUT_KEYS,
		};
		if let Some(unknown) = obj.keys().find(|k| !accepted.contains(&k.as_str())) {
			anyhow::bail!(
				"Unknown {} option `{unknown}`, expected one of: {}",
				match op {
					PresignOp::Get => "file::presign_get",
					PresignOp::Put => "file::presign_put",
				},
				accepted.join(", ")
			);
		}

		let public = convert_value_to_public_value(Value::Object(obj))?;
		let opts = Self::from_value(public).map_err(anyhow::Error::new)?;
		anyhow::ensure!(
			opts.expires.as_secs() > 0,
			"A presigned URL must last at least 1s, got {:?}",
			opts.expires
		);
		Ok(opts)
	}
}

/// What a caller asked `file::presign_get` / `file::presign_put` for.
#[derive(Clone, Debug)]
pub(crate) struct PresignRequest {
	pub op: PresignOp,
	pub expires: Duration,
	pub max: Option<u64>,
	pub overwrite: bool,
	pub content_type: Option<String>,
}

/// A minted URL, in the shape `file::presign_*` returns.
///
/// Serialised through `SurrealValue`, so the struct definition is the schema a
/// caller sees rather than a hand-built object that can drift from it.
#[derive(Clone, Debug, SurrealValue)]
#[surreal(crate = "surrealdb_types")]
pub(crate) struct PresignedUrl {
	/// The configured public origin, or `None` when the node has none.
	pub origin: Option<String>,
	/// Path and query. Always relative, whatever the node is configured with,
	/// so a caller behind a CDN has one field it can rely on.
	pub path: String,
	/// `origin` + `path`, falling back to `path` alone.
	pub href: String,
	pub method: String,
	pub expires: PublicDatetime,
	/// The `Content-Type` an upload must send, when the URL was minted with
	/// one. An upload URL minted without one accepts only an upload that
	/// declares none, or `application/octet-stream`.
	pub content_type: Option<String>,
}

impl From<PresignedUrl> for Value {
	fn from(url: PresignedUrl) -> Self {
		convert_public_value_to_internal(url.into_value())
	}
}

/// Sign a request and assemble the value a caller receives.
///
/// `now` is passed in rather than read here so the whole construction stays
/// testable, and so one statement cannot straddle two seconds.
pub(crate) fn issue(
	secret: &PresignSecret,
	origin: Option<&str>,
	scope: BucketScope<'_>,
	key: &ObjectKey,
	req: PresignRequest,
	now: i64,
) -> Result<PresignedUrl, PresignError> {
	let exp = now
		.checked_add(i64::try_from(req.expires.as_secs()).map_err(|_| PresignError::Malformed)?)
		.ok_or(PresignError::Malformed)?;

	let params = PresignParams {
		exp,
		max: req.max,
		overwrite: req.overwrite,
		content_type: req.content_type.clone(),
	};

	let (path, search, expires) = sign(secret, scope, req.op, key, &params)?;
	let path = format!("{path}{search}");
	let href = match origin {
		Some(origin) => format!("{}{path}", origin.trim_end_matches('/')),
		None => path.clone(),
	};

	Ok(PresignedUrl {
		origin: origin.map(str::to_owned),
		path,
		href,
		method: req.op.method().to_owned(),
		expires: expires.into(),
		content_type: req.content_type,
	})
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::catalog::{DatabaseId, NamespaceId};

	fn secret() -> PresignSecret {
		let mut bytes = [0u8; 32];
		bytes[31] = 1;
		PresignSecret::for_test(&bytes)
	}

	fn database(ns_id: u32, db_id: u32) -> ScopeLevel<'static> {
		ScopeLevel::Database {
			ns_id: NamespaceId(ns_id),
			db_id: DatabaseId(db_id),
			ns_name: "test",
			db_name: "app",
		}
	}

	fn scope() -> BucketScope<'static> {
		BucketScope {
			level: database(7, 12),
			bucket: "images",
			nonce: uuid::Uuid::from_u128(1),
		}
	}

	const NOW: i64 = 1756636800;

	fn params() -> PresignParams {
		PresignParams {
			exp: NOW + 900,
			max: Some(5242880),
			overwrite: true,
			content_type: None,
		}
	}

	fn key() -> ObjectKey {
		ObjectKey::new("/u/user:tobie/avatar.png")
	}

	/// Sign, then strip `sig` back out of the query string.
	fn signature(op: PresignOp, params: &PresignParams) -> String {
		let (_, search, _) = sign(&secret(), scope(), op, &key(), params).unwrap();
		search.rsplit_once("&sig=").unwrap().1.to_string()
	}

	fn check(
		op: PresignOp,
		s: BucketScope<'_>,
		k: &ObjectKey,
		params: PresignParams,
		sig: &str,
		now: i64,
	) -> Result<PresignGrant, PresignError> {
		verify(&secret(), s, op, k, params, sig, now)
	}

	#[test]
	fn a_signed_url_verifies() {
		let sig = signature(PresignOp::Put, &params());
		let grant = check(PresignOp::Put, scope(), &key(), params(), &sig, NOW).unwrap();
		assert_eq!(grant.max, Some(5242880));
	}

	#[test]
	fn a_url_verifies_at_every_level() {
		let levels = [
			("/bucket/images/", ScopeLevel::Root),
			(
				"/ns/test/bucket/images/",
				ScopeLevel::Namespace {
					id: NamespaceId(7),
					name: "test",
				},
			),
			("/ns/test/db/app/bucket/images/", database(7, 12)),
		];
		for (prefix, level) in levels {
			let s = BucketScope {
				level,
				..scope()
			};
			let (path, search, _) = sign(&secret(), s, PresignOp::Get, &key(), &params()).unwrap();
			assert!(path.starts_with(prefix), "{path}");
			let sig = search.rsplit_once("&sig=").unwrap().1;
			assert!(check(PresignOp::Get, s, &key(), params(), sig, NOW).is_ok(), "{prefix}");
		}
	}

	#[test]
	fn the_url_carries_what_it_signed() {
		let (path, search, expires) =
			sign(&secret(), scope(), PresignOp::Put, &key(), &params()).unwrap();
		assert_eq!(path, "/ns/test/db/app/bucket/images/u/user%3Atobie/avatar.png");
		assert!(search.starts_with("?exp=1756637700&max=5242880&sig="));
		assert_eq!(expires.timestamp(), 1756637700);
	}

	#[test]
	fn editing_the_object_key_is_refused() {
		let sig = signature(PresignOp::Get, &params());
		let other = ObjectKey::new("/u/user:jaime/avatar.png");
		assert!(matches!(
			check(PresignOp::Get, scope(), &other, params(), &sig, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn extending_the_expiry_is_refused() {
		let sig = signature(PresignOp::Get, &params());
		let mut edited = params();
		edited.exp = NOW + 86_400 * 365;
		assert!(matches!(
			check(PresignOp::Get, scope(), &key(), edited, &sig, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn raising_the_size_limit_is_refused() {
		let sig = signature(PresignOp::Put, &params());
		let mut edited = params();
		edited.max = Some(999_999_999);
		assert!(matches!(
			check(PresignOp::Put, scope(), &key(), edited, &sig, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn dropping_the_size_limit_is_refused() {
		// The interesting direction: an unlimited upload is what an attacker
		// wants, and removing a parameter must break the blob just as editing
		// one does.
		let sig = signature(PresignOp::Put, &params());
		let mut edited = params();
		edited.max = None;
		assert!(matches!(
			check(PresignOp::Put, scope(), &key(), edited, &sig, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn flipping_overwrite_is_refused() {
		let sig = signature(PresignOp::Put, &params());
		let mut edited = params();
		edited.overwrite = false;
		assert!(matches!(
			check(PresignOp::Put, scope(), &key(), edited, &sig, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn upgrading_the_method_is_refused() {
		// A download URL must not become an upload URL.
		let sig = signature(PresignOp::Get, &params());
		assert!(matches!(
			check(PresignOp::Put, scope(), &key(), params(), &sig, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn an_expired_url_is_refused_after_it_verifies() {
		let sig = signature(PresignOp::Get, &params());
		assert!(matches!(
			check(PresignOp::Get, scope(), &key(), params(), &sig, NOW + 901),
			Err(PresignError::Expired)
		));
		// One second before the boundary is still valid, so `exp` is inclusive.
		assert!(check(PresignOp::Get, scope(), &key(), params(), &sig, NOW + 900).is_ok());
	}

	#[test]
	fn a_changed_secret_refuses_every_outstanding_url() {
		// Changing `SURREAL_BUCKET_PRESIGN_SECRET` is the node-wide revocation.
		let sig = signature(PresignOp::Get, &params());
		let rotated = PresignSecret::for_test(&[2u8; 32]);
		assert!(matches!(
			verify(&rotated, scope(), PresignOp::Get, &key(), params(), &sig, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn a_url_does_not_verify_against_another_tenant() {
		// Each of these differs from the signing scope in exactly one component,
		// and every one of them really exists. Nothing in `verify` compares the
		// scope: a mismatch derives a different key, so the isolation cannot be
		// lost to a dropped equality check.
		let sig = signature(PresignOp::Get, &params());

		let mut other_ns = scope();
		other_ns.level = database(9, 12);
		let mut other_db = scope();
		other_db.level = database(7, 99);
		let mut other_bucket = scope();
		other_bucket.bucket = "other";
		// The same bucket name and nonce on the namespace, and on the root.
		let mut on_ns = scope();
		on_ns.level = ScopeLevel::Namespace {
			id: NamespaceId(7),
			name: "test",
		};
		let mut on_root = scope();
		on_root.level = ScopeLevel::Root;

		for (label, s) in [
			("namespace", other_ns),
			("database", other_db),
			("bucket", other_bucket),
			("level (namespace)", on_ns),
			("level (root)", on_root),
		] {
			assert!(
				matches!(
					check(PresignOp::Get, s, &key(), params(), &sig, NOW),
					Err(PresignError::BadSignature)
				),
				"a URL verified against a different {label}"
			);
		}
	}

	#[test]
	fn replacing_the_nonce_revokes_an_issued_url() {
		// `ALTER BUCKET ... REVOKE SIGNATURES` assigns a fresh nonce, and so
		// does redefining a bucket that was removed. Both must retire the key,
		// which is the whole mechanism: any value the bucket has not signed
		// under works, so no path that loses the old one can revive its URLs.
		let sig = signature(PresignOp::Get, &params());

		let mut revoked = scope();
		revoked.nonce = uuid::Uuid::from_u128(2);
		assert!(matches!(
			check(PresignOp::Get, revoked, &key(), params(), &sig, NOW),
			Err(PresignError::BadSignature)
		));

		// Including the nil nonce a pre-existing bucket reads back as: it is a
		// value like any other, not a wildcard.
		let mut nil = scope();
		nil.nonce = uuid::Uuid::nil();
		assert!(matches!(
			check(PresignOp::Get, nil, &key(), params(), &sig, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn the_signature_is_url_safe() {
		// A `+` or `/` in the query string would be re-encoded by some proxies
		// and break an otherwise valid URL.
		for n in 0..64i64 {
			let mut p = params();
			p.exp = NOW + n;
			let sig = signature(PresignOp::Get, &p);
			assert!(
				sig.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
				"signature is not url-safe: {sig}"
			);
		}
	}

	#[test]
	fn the_content_type_is_covered() {
		// Binding a type is what stops the holder of an upload URL storing
		// `text/html` on the database's origin, so swapping it, adding one to
		// a URL minted without one, and dropping one must all fail.
		let mut typed = params();
		typed.content_type = Some("image/png".to_owned());
		let sig = signature(PresignOp::Put, &typed);
		assert!(check(PresignOp::Put, scope(), &key(), typed.clone(), &sig, NOW).is_ok());

		let mut swapped = typed.clone();
		swapped.content_type = Some("text/html".to_owned());
		assert!(matches!(
			check(PresignOp::Put, scope(), &key(), swapped, &sig, NOW),
			Err(PresignError::BadSignature)
		));
		assert!(matches!(
			check(PresignOp::Put, scope(), &key(), params(), &sig, NOW),
			Err(PresignError::BadSignature)
		));

		let untyped = signature(PresignOp::Put, &params());
		assert!(matches!(
			check(PresignOp::Put, scope(), &key(), typed, &untyped, NOW),
			Err(PresignError::BadSignature)
		));
	}

	#[test]
	fn the_url_carries_its_content_type() {
		let mut typed = params();
		typed.content_type = Some("text/plain; charset=utf-8".to_owned());
		let (_, search, _) = sign(&secret(), scope(), PresignOp::Put, &key(), &typed).unwrap();
		assert!(
			search.starts_with("?ct=text%2Fplain%3B%20charset%3Dutf-8&exp="),
			"unexpected query: {search}"
		);
	}

	#[test]
	fn the_signature_matches_a_pinned_answer() {
		// Pins the whole construction: derivation chain, canonical form and
		// encoding. Changing it invalidates every URL in the wild, so it has to
		// be a deliberate edit of this constant.
		assert_eq!(
			signature(PresignOp::Put, &params()),
			"ymXEcBePMd7RORdQXL3fROv-pLhEFws8Bh0IkG3mHms"
		);
	}
}
