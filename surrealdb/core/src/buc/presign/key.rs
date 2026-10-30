//! Derivation of a bucket's URL-signing key.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use uuid::Uuid;

use super::PresignError;
use crate::buc::PresignSecret;
use crate::catalog::{DatabaseId, NamespaceId};

/// Separates URL signing from any other use the secret is ever put to.
const PURPOSE: &[u8] = b"surrealdb/bucket-presign/v1";

/// A derived key. HMAC-SHA256 output, so 32 bytes.
pub(super) type SigningKey = [u8; 32];

/// Everything a presigned URL is scoped to.
///
/// The ids and the nonce bind the key; the names appear in the URL path. Both
/// are needed because the path is what a browser presents and the ids are what
/// the node resolves it to.
#[derive(Clone, Copy, Debug)]
pub(crate) struct BucketScope<'a> {
	pub ns_id: NamespaceId,
	pub db_id: DatabaseId,
	pub ns_name: &'a str,
	pub db_name: &'a str,
	pub bucket: &'a str,
	pub nonce: Uuid,
}

impl BucketScope<'_> {
	/// The key that signs and verifies this bucket's URLs.
	///
	/// Chained rather than concatenated. Each link is its own HMAC evaluation
	/// keyed by the previous link's output, so the variable-length bucket name
	/// cannot be confused with a different name plus different ids — no
	/// escaping or length prefixing is needed to keep two buckets apart.
	///
	/// The bucket is identified by its name and its `presign_nonce`, not by
	/// `BucketId`: that field is never assigned, so it is `None` on every
	/// bucket a `DEFINE` creates. The nonce is what makes the identity durable
	/// anyway -- a bucket dropped and recreated under the same name gets a new
	/// one, so URLs issued for the old bucket cannot verify against the new
	/// one. It is carried across an `OVERWRITE`, which is the one redefinition
	/// that keeps the same bucket.
	pub(super) fn signing_key(&self, secret: &PresignSecret) -> Result<SigningKey, PresignError> {
		let mut ids = [0u8; 24];
		ids[0..4].copy_from_slice(&self.ns_id.0.to_be_bytes());
		ids[4..8].copy_from_slice(&self.db_id.0.to_be_bytes());
		ids[8..24].copy_from_slice(self.nonce.as_bytes());

		let purpose = derive(secret.as_bytes(), PURPOSE)?;
		let tenant = derive(&purpose, &ids)?;
		derive(&tenant, self.bucket.as_bytes())
	}
}

/// One link of the chain: HMAC-SHA256 keyed by `key`, over `context`.
fn derive(key: &[u8], context: &[u8]) -> Result<SigningKey, PresignError> {
	let mut mac = mac(key)?;
	mac.update(context);
	Ok(mac.finalize().into_bytes().into())
}

/// An HMAC-SHA256 keyed by `key`.
///
/// The error arm is unreachable, since HMAC accepts a key of any length. It
/// is a `Result` so that no caller spells `.expect()`.
pub(super) fn mac(key: &[u8]) -> Result<Hmac<Sha256>, PresignError> {
	<Hmac<Sha256> as Mac>::new_from_slice(key).map_err(|_| PresignError::InvalidKey)
}

#[cfg(test)]
mod tests {
	use base64::Engine as _;
	use base64::engine::general_purpose::STANDARD;

	use super::*;

	#[test]
	fn derive_chains_do_not_collide() {
		// The reason the derivation chains rather than hashing one concatenated
		// blob: a chain is a sequence of keyed evaluations, so no chain can be
		// confused with another chain or with a different split of the same
		// bytes. `derive(secret, a ++ b)` would let ("ab", "c") and ("a", "bc")
		// derive one key.
		let root = [9u8; 32];
		let chains: [&[&[u8]]; 6] = [
			&[b"ab", b"c"],
			&[b"a", b"bc"],
			&[b"abc"],
			&[b"abc", b""],
			&[b"", b"abc"],
			&[b"a", b"b", b"c"],
		];

		let derived: Vec<SigningKey> = chains
			.iter()
			.map(|chain| chain.iter().fold(root, |acc, part| derive(&acc, part).unwrap()))
			.collect();

		for (i, a) in derived.iter().enumerate() {
			for (j, b) in derived.iter().enumerate().skip(i + 1) {
				assert_ne!(a, b, "chain {:?} and chain {:?} derived one key", chains[i], chains[j]);
			}
		}
	}

	#[test]
	fn signing_key_matches_a_pinned_answer() {
		// Pins the construction. A change here invalidates every presigned URL
		// in the wild, so it must be a deliberate edit of this constant and not
		// a side effect of touching the derivation.
		let secret = PresignSecret::for_test(&[0u8; 32]);
		let scope = BucketScope {
			ns_id: NamespaceId(7),
			db_id: DatabaseId(12),
			ns_name: "test",
			db_name: "app",
			bucket: "images",
			nonce: Uuid::from_u128(1),
		};
		let key = scope.signing_key(&secret).unwrap();
		assert_eq!(STANDARD.encode(key), "FFCJyCqk8D5Iex+AVJdFVCz0jFUfP3s52XcxESV2/yk=");
	}
}
