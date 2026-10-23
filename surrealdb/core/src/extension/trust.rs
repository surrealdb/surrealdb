//! Whether an archive may run under a definition.
//!
//! A definition pins its code with `DIGEST` and names who must have signed it
//! with `SIGNER`, or waives that with `UNSIGNED`. An archive may run under the
//! definition only when both hold:
//!
//! 1. `sha256(surrealism/MANIFEST)` equals `DIGEST`;
//! 2. with a `SIGNER`, some `surrealism/SIGNATURE` proves it over that digest
//!    ([`surrealism_verify::verify`]). `UNSIGNED` skips this check only.
//!
//! The server checks this when `DEFINE EXTENSION` fetches the archive and
//! again before every load, so it holds however the definition reached the
//! catalog.
//!
//! `SIGNATURE` is outside the manifest, so copies of one `DIGEST` from
//! different sources can carry different signatures, or none. Only the
//! signature of the definition's own source counts: an archive whose source
//! carries no `SIGNATURE` that proves the `SIGNER` is refused, whatever other
//! copies of the same digest this process has seen. Every node, and the same
//! node after a restart, therefore reaches the same verdict from the same
//! source.
//!
//! A proof is cached per (`DIGEST`, `SIGNER`, signature), so a source whose
//! signature has verified is not verified again. When the signature in hand
//! does not prove the `SIGNER`, the source is fetched once more for its
//! current `SIGNATURE`. A refusal is repeated without a refetch for
//! [`REFETCH_AFTER_REFUSAL`], so a signature added later is found within that
//! time.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use web_time::Instant;

use crate::catalog::{DatabaseId, ModuleDefinition, NamespaceId};
use crate::ctx::Context;
use crate::expr::{ExtensionSigner, ManifestDigest};
use crate::extension::archive::{ArchiveSource, fetch_archive};

/// (`DIGEST`, `SIGNER`, signature) proofs and refusals cached.
const TRUST_CACHE_ENTRIES: usize = 1024;

/// How long a refusal is repeated before the definition's source is fetched
/// again: the most a newly signed archive waits to be found, and the least
/// time between two fetches a failing definition causes.
pub(crate) const REFETCH_AFTER_REFUSAL: Duration = Duration::from_secs(10);

/// What a definition says about the code it may run.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Trust<'a> {
	/// `DIGEST`. `None` only for a definition stored before revision 3, which
	/// does not run.
	pub digest: Option<&'a ManifestDigest>,
	/// `SIGNER` and its [`signer_fingerprint`]. The fingerprint is computed
	/// once, where the `Trust` is made, since every check of a call keys by it.
	pub signer: Option<(&'a ExtensionSigner, [u8; 32])>,
	/// `UNSIGNED`. Without a `SIGNER`, an archive runs only when this is set.
	pub unsigned: bool,
}

impl<'a> From<&'a ModuleDefinition> for Trust<'a> {
	fn from(definition: &'a ModuleDefinition) -> Self {
		Trust::new(definition.digest.as_ref(), definition.signer.as_ref(), definition.unsigned)
	}
}

impl<'a> Trust<'a> {
	pub(crate) fn new(
		digest: Option<&'a ManifestDigest>,
		signer: Option<&'a ExtensionSigner>,
		unsigned: bool,
	) -> Self {
		Trust {
			digest,
			signer: signer.map(|signer| (signer, signer_fingerprint(signer))),
			unsigned,
		}
	}

	/// The `SIGNER` a signature must prove, or `None` when the definition is
	/// `UNSIGNED`. A definition with neither is refused: no stored definition
	/// that pins a `DIGEST` lacks both, so one that does is not trusted to run.
	pub(crate) fn required_signer(&self) -> Result<Option<(&'a ExtensionSigner, [u8; 32])>> {
		match (self.signer, self.unsigned) {
			(Some(signer), _) => Ok(Some(signer)),
			(None, true) => Ok(None),
			(None, false) => {
				bail!("The extension names no SIGNER and is not UNSIGNED, so it cannot run")
			}
		}
	}

	/// The pinned digest, or the error a definition without one gets.
	pub(crate) fn pinned(&self) -> Result<[u8; 32]> {
		match self.digest {
			Some(digest) => digest_bytes(digest),
			None => bail!(
				"The extension records no DIGEST, so it cannot run: {}",
				crate::catalog::REINSTALL_WARNING
			),
		}
	}
}

/// The 32 bytes of a `sha256:` digest.
pub(crate) fn digest_bytes(digest: &ManifestDigest) -> Result<[u8; 32]> {
	let ManifestDigest::Sha256(bytes) = digest;
	bytes.as_slice().try_into().map_err(|_| anyhow!("DIGEST {digest} is not 32 bytes"))
}

/// Refuses an archive whose manifest digest is not the pinned one.
pub(crate) fn ensure_digest(archive: &[u8; 32], pinned: &[u8; 32]) -> Result<()> {
	if archive != pinned {
		bail!(
			"The archive's MANIFEST digest is {}, but the definition's DIGEST is {}",
			ManifestDigest::sha256(*archive),
			ManifestDigest::sha256(*pinned)
		);
	}
	Ok(())
}

/// A `SIGNER` reduced to SHA-256 over an unambiguous encoding of every field,
/// so a proof is keyed without holding or comparing the KBs of certificates
/// the signer carries. Two signers share a fingerprint only if every field is
/// equal.
pub(crate) fn signer_fingerprint(signer: &ExtensionSigner) -> [u8; 32] {
	use sha2::{Digest as _, Sha256};

	fn put(hasher: &mut Sha256, bytes: &[u8]) {
		hasher.update((bytes.len() as u64).to_le_bytes());
		hasher.update(bytes);
	}
	fn put_opt(hasher: &mut Sha256, value: Option<&String>) {
		match value {
			None => put(hasher, b"none"),
			Some(v) => {
				put(hasher, b"some");
				put(hasher, v.as_bytes());
			}
		}
	}
	fn put_list(hasher: &mut Sha256, list: &[String]) {
		hasher.update((list.len() as u64).to_le_bytes());
		for item in list {
			put(hasher, item.as_bytes());
		}
	}

	let mut hasher = Sha256::new();
	match signer {
		ExtensionSigner::Key(key) => {
			put(&mut hasher, b"key");
			put(&mut hasher, key.as_bytes());
		}
		ExtensionSigner::Sigstore(s) => {
			put(&mut hasher, b"sigstore");
			put_list(&mut hasher, &s.ca);
			put(&mut hasher, s.log.as_bytes());
			put_list(&mut hasher, &s.tsa);
			put(&mut hasher, s.issuer.as_bytes());
			put(&mut hasher, s.identity.as_bytes());
			put_opt(&mut hasher, s.repository_id.as_ref());
			put_opt(&mut hasher, s.owner_id.as_ref());
		}
	}
	hasher.finalize().into()
}

/// What a refusal is recorded against: the database whose definition was
/// refused, the source it names, the `DIGEST` it pins and its signer's
/// fingerprint (zero for `UNSIGNED`). A refusal is consulted only for the same
/// source of the same database, so one tenant's failing source never refuses
/// another tenant's definition, and its reason never reaches another tenant.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RefusalKey {
	ns: NamespaceId,
	db: DatabaseId,
	source: String,
	digest: [u8; 32],
	signer: [u8; 32],
}

impl RefusalKey {
	pub(crate) fn new(
		ns: NamespaceId,
		db: DatabaseId,
		source: ArchiveSource<'_>,
		digest: [u8; 32],
		trust: &Trust<'_>,
	) -> Self {
		Self {
			ns,
			db,
			source: source.key(),
			digest,
			signer: trust.signer.map(|(_, fingerprint)| fingerprint).unwrap_or_default(),
		}
	}
}

/// A recent refusal, so a definition that fails to load does not refetch,
/// unpack and verify its archive on every call.
#[derive(Clone)]
struct Refusal {
	at: Instant,
	reason: Arc<str>,
}

/// (`DIGEST`, signer fingerprint, SHA-256 of the signature).
type ProofKey = ([u8; 32], [u8; 32], [u8; 32]);

/// The (`DIGEST`, `SIGNER`, signature) triples proven, and recent refusals.
/// One per datastore, shared by every namespace and database: a proof depends
/// only on the digest, the signer and the signature bytes, so it holds for any
/// source that delivers that same signature, and for no other.
pub(crate) struct TrustStore {
	proven: quick_cache::sync::Cache<ProofKey, ()>,
	refused: quick_cache::sync::Cache<RefusalKey, Refusal>,
}

impl Default for TrustStore {
	fn default() -> Self {
		Self {
			proven: quick_cache::sync::Cache::new(TRUST_CACHE_ENTRIES),
			refused: quick_cache::sync::Cache::new(TRUST_CACHE_ENTRIES),
		}
	}
}

impl TrustStore {
	/// Fails with the reason `key` was refused, when that was less than
	/// `window` ago. The server's window is `surrealism_refetch_after_refusal`,
	/// [`REFETCH_AFTER_REFUSAL`] unless a debug build overrides it.
	pub(crate) fn check_refusal(&self, key: &RefusalKey, window: Duration) -> Result<()> {
		if let Some(refusal) = self.refused.get(key)
			&& refusal.at.elapsed() < window
		{
			bail!("{}", refusal.reason);
		}
		Ok(())
	}

	/// Records that `key` failed with `error`, or clears its refusal when it
	/// succeeded.
	pub(crate) fn record<T>(&self, key: RefusalKey, outcome: &Result<T>) {
		match outcome {
			Ok(_) => {
				self.refused.remove(&key);
			}
			Err(e) => {
				self.refused.insert(
					key,
					Refusal {
						at: Instant::now(),
						reason: format!("{e:#}").into(),
					},
				);
			}
		}
	}

	/// Whether `signature`, the `SIGNATURE` of an archive with manifest digest
	/// `digest`, proves `signer`. A proof is cached per signature.
	#[cfg(test)]
	pub(crate) fn prove(
		&self,
		digest: &[u8; 32],
		signer: &ExtensionSigner,
		signature: Option<&[u8]>,
	) -> Result<()> {
		self.prove_keyed(digest, (signer, signer_fingerprint(signer)), signature)
	}

	/// The key a proof of `signature` for the signer of `fingerprint` over
	/// `digest` is cached under.
	fn proof_key(digest: &[u8; 32], fingerprint: [u8; 32], signature: &[u8]) -> ProofKey {
		use sha2::{Digest as _, Sha256};
		(*digest, fingerprint, Sha256::digest(signature).into())
	}

	/// Whether `signature` is proven for the signer of `fingerprint` over
	/// `digest`, without verifying anything.
	fn is_proven(
		&self,
		digest: &[u8; 32],
		fingerprint: [u8; 32],
		signature: Option<&[u8]>,
	) -> bool {
		signature
			.is_some_and(|s| self.proven.get(&Self::proof_key(digest, fingerprint, s)).is_some())
	}

	fn prove_keyed(
		&self,
		digest: &[u8; 32],
		(signer, fingerprint): (&ExtensionSigner, [u8; 32]),
		signature: Option<&[u8]>,
	) -> Result<()> {
		let Some(signature) = signature else {
			bail!("The extension has a SIGNER, but its archive carries no SIGNATURE");
		};
		let key = Self::proof_key(digest, fingerprint, signature);
		if self.proven.get(&key).is_some() {
			return Ok(());
		}
		surrealism_verify::verify(digest, signature, &verify_signer(signer)?)
			.map_err(|e| anyhow!("The extension's SIGNATURE does not prove its SIGNER: {e}"))?;
		self.proven.insert(key, ());
		Ok(())
	}

	/// Check that the archive with manifest digest `archive`, already read
	/// from the definition's own source, may run under `trust`; see the
	/// [module documentation](self).
	///
	/// `signature` is that archive's `SIGNATURE`. `source` is where the
	/// definition fetches its archive from, in the database `ns`/`db`. A cached
	/// proof answers at once. Otherwise a refusal of this definition's source
	/// in the last [`REFETCH_AFTER_REFUSAL`] is repeated before any signature
	/// is verified. Otherwise `signature` is verified, and when it does not
	/// prove the `SIGNER`, the archive is fetched from `source` once more and
	/// its current `SIGNATURE` verified; that outcome is recorded. `source` is
	/// `None` when the archive was read from the source just now, so nothing
	/// is fetched again and the caller records the outcome.
	///
	/// Returns whether the source's current `SIGNATURE` proved the `SIGNER` in
	/// place of `signature`, so a caller holding the archive can drop it.
	#[allow(clippy::too_many_arguments)]
	pub(crate) async fn ensure(
		&self,
		ctx: &Context,
		ns: NamespaceId,
		db: DatabaseId,
		source: Option<ArchiveSource<'_>>,
		archive: &[u8; 32],
		signature: Option<&[u8]>,
		trust: Trust<'_>,
	) -> Result<bool> {
		let pinned = trust.pinned()?;
		ensure_digest(archive, &pinned)?;
		let Some(signer) = trust.required_signer()? else {
			return Ok(false);
		};
		if self.is_proven(&pinned, signer.1, signature) {
			return Ok(false);
		}
		let refusal = source.map(|source| RefusalKey::new(ns, db, source, pinned, &trust));
		if let Some(key) = &refusal {
			self.check_refusal(key, ctx.config.surrealism.surrealism_refetch_after_refusal)?;
		}
		let held = self.prove_keyed(&pinned, signer, signature);
		let (Err(_), Some(source), Some(key)) = (&held, source, refusal) else {
			return held.map(|()| false);
		};
		let refetched = async {
			let bytes = fetch_archive(ctx, ns, db, source).await?;
			let max_fs_bytes = ctx.config.surrealism.surrealism_max_fs_bytes;
			let files = tokio::task::spawn_blocking(move || {
				surrealism_runtime::package::trust_files_from_reader(bytes.as_ref(), max_fs_bytes)
			})
			.await
			.map_err(|e| anyhow!("archive read task aborted: {e}"))??;
			ensure_digest(&files.manifest.digest().0, &pinned)?;
			self.prove_keyed(&pinned, signer, files.signature.as_deref())
		}
		.await;
		self.record(key, &refetched);
		refetched.map(|()| true)
	}
}

/// The verifier's form of a `SIGNER`: every certificate and key decoded from
/// the base64 the statement holds.
fn verify_signer(signer: &ExtensionSigner) -> Result<surrealism_verify::Signer> {
	fn der(field: &str, value: &str) -> Result<Vec<u8>> {
		BASE64.decode(value).map_err(|_| anyhow!("SIGNER field `{field}` is not valid base64"))
	}
	Ok(match signer {
		ExtensionSigner::Key(key) => surrealism_verify::Signer::Key(key.clone()),
		ExtensionSigner::Sigstore(s) => {
			surrealism_verify::Signer::Sigstore(surrealism_verify::SigstoreSigner {
				ca: s.ca.iter().map(|c| der("ca", c)).collect::<Result<_>>()?,
				log: der("log", &s.log)?,
				tsa: s.tsa.iter().map(|c| der("tsa", c)).collect::<Result<_>>()?,
				issuer: s.issuer.clone(),
				identity: s.identity.clone(),
				repository_id: s.repository_id.clone(),
				owner_id: s.owner_id.clone(),
			})
		}
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	/// An Ed25519 key, and its SSH signature (namespace `surrealism`, hash
	/// `sha256`) over a manifest whose digest is [`digest`]. Copied from the
	/// verifier's fixtures in `surrealism/verify/tests/fixtures/ssh/`.
	const KEY: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIIvrfoxNFOr3w71HTbdQgYnA7sJ3ykfXuMzw6TbMnFQt release@acme";
	const OTHER_KEY: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIGK5vRUrfIJQgxwH3NS1lwPIwQ+rEG+87m/oqB1f/O82 release@acme";
	const SIGNATURE: &[u8] = b"-----BEGIN SSH SIGNATURE-----
U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAgi+t+jE0U6vfDvUdNt1CBicDuwn
fKR9e4zPDpNsycVC0AAAAKc3VycmVhbGlzbQAAAAAAAAAGc2hhMjU2AAAAUwAAAAtzc2gt
ZWQyNTUxOQAAAEBK5iEKGhVHbnIyCQGovtOcqdYw5j88HyhlNyMAHJpiO2hVw9A756wIum
b51y/HTpFN02c1FcBADVKJ5DHqZwEH
-----END SSH SIGNATURE-----
";

	fn digest() -> [u8; 32] {
		hex::decode("496d4dec3dbab962062cef307ffda41f038a6a38ea685da64b19067215845592")
			.unwrap()
			.try_into()
			.unwrap()
	}

	fn key(line: &str) -> ExtensionSigner {
		ExtensionSigner::Key(line.trim().to_owned())
	}

	#[test]
	fn a_signature_proves_its_signer_and_the_proof_is_cached() {
		let store = TrustStore::default();
		let d = digest();
		let err = store.prove(&d, &key(KEY), None).unwrap_err().to_string();
		assert!(err.contains("carries no SIGNATURE"), "{err}");

		store.prove(&d, &key(KEY), Some(SIGNATURE)).unwrap();
		let fingerprint = signer_fingerprint(&key(KEY));
		assert!(store.is_proven(&d, fingerprint, Some(SIGNATURE)));

		let err = store.prove(&d, &key(OTHER_KEY), Some(SIGNATURE)).unwrap_err().to_string();
		assert!(err.contains("does not prove its SIGNER"), "{err}");
		assert!(!store.is_proven(&d, signer_fingerprint(&key(OTHER_KEY)), Some(SIGNATURE)));
	}

	/// Only the signature in hand counts. Once a signed copy of a digest has
	/// verified, a copy of the same digest without a signature, or with one
	/// that proves nothing, is still refused: the verdict depends on the
	/// source, not on what this process has seen before.
	#[test]
	fn a_copy_without_its_own_signature_is_refused_after_a_signed_one() {
		let store = TrustStore::default();
		let d = digest();
		store.prove(&d, &key(KEY), Some(SIGNATURE)).unwrap();

		let err = store.prove(&d, &key(KEY), None).unwrap_err().to_string();
		assert!(err.contains("carries no SIGNATURE"), "{err}");
		let junk = b"-----BEGIN SSH SIGNATURE-----\nnot a signature\n".as_slice();
		assert!(store.prove(&d, &key(KEY), Some(junk)).is_err());
		assert!(!store.is_proven(&d, signer_fingerprint(&key(KEY)), None));
	}

	#[test]
	fn a_proof_is_kept_per_digest() {
		let store = TrustStore::default();
		store.prove(&digest(), &key(KEY), Some(SIGNATURE)).unwrap();
		let mut other = digest();
		other[0] ^= 1;
		assert!(store.prove(&other, &key(KEY), Some(SIGNATURE)).is_err());
		assert!(!store.is_proven(&other, signer_fingerprint(&key(KEY)), Some(SIGNATURE)));
	}

	#[test]
	fn the_fingerprint_separates_every_field() {
		let sigstore = |tsa: Vec<&str>, owner: Option<&str>| {
			ExtensionSigner::Sigstore(crate::expr::SigstoreSigner {
				ca: vec!["YQ==".into()],
				log: "Yg==".into(),
				tsa: tsa.into_iter().map(Into::into).collect(),
				issuer: "i".into(),
				identity: "id".into(),
				repository_id: None,
				owner_id: owner.map(Into::into),
			})
		};
		let a = signer_fingerprint(&sigstore(vec!["Yw=="], None));
		assert_eq!(a, signer_fingerprint(&sigstore(vec!["Yw=="], None)));
		assert_ne!(a, signer_fingerprint(&sigstore(vec!["Yw==", "Yw=="], None)));
		assert_ne!(a, signer_fingerprint(&sigstore(vec!["Yw=="], Some(""))));
		assert_ne!(signer_fingerprint(&key(KEY)), signer_fingerprint(&key(OTHER_KEY)));
	}

	/// A refusal answers only for the database and source it was recorded
	/// for: another tenant's definition of the same DIGEST and signer, or the
	/// same tenant's other source, is not refused by it.
	#[test]
	fn a_refusal_is_kept_per_database_and_source() {
		let store = TrustStore::default();
		let signer = key(KEY);
		let pinned = ManifestDigest::sha256(digest());
		let trust = Trust::new(Some(&pinned), Some(&signer), false);
		let source = |key| ArchiveSource::File {
			bucket: "mods",
			key,
		};
		let refused = RefusalKey::new(NamespaceId(1), DatabaseId(1), source("a"), digest(), &trust);
		store.record::<()>(refused.clone(), &Err(anyhow!("bucket `mods` is gone")));

		let err = store.check_refusal(&refused, REFETCH_AFTER_REFUSAL).unwrap_err().to_string();
		assert!(err.contains("bucket `mods` is gone"), "{err}");
		for other in [
			RefusalKey::new(NamespaceId(2), DatabaseId(1), source("a"), digest(), &trust),
			RefusalKey::new(NamespaceId(1), DatabaseId(2), source("a"), digest(), &trust),
			RefusalKey::new(NamespaceId(1), DatabaseId(1), source("b"), digest(), &trust),
			RefusalKey::new(NamespaceId(1), DatabaseId(1), source("a"), [0; 32], &trust),
		] {
			store.check_refusal(&other, REFETCH_AFTER_REFUSAL).unwrap();
		}

		// A zero window repeats no refusal.
		store.check_refusal(&refused, Duration::ZERO).unwrap();

		store.record(refused.clone(), &Ok(()));
		store.check_refusal(&refused, REFETCH_AFTER_REFUSAL).unwrap();
	}

	/// Without a `SIGNER`, only `UNSIGNED` lets an archive run.
	#[test]
	fn no_signer_runs_only_when_unsigned() {
		let pinned = ManifestDigest::sha256(digest());
		let signer = key(KEY);
		assert!(Trust::new(Some(&pinned), None, true).required_signer().unwrap().is_none());
		let err = Trust::new(Some(&pinned), None, false).required_signer().unwrap_err();
		assert!(err.to_string().contains("names no SIGNER and is not UNSIGNED"), "{err}");
		// A SIGNER is required to verify whatever the flag says.
		for unsigned in [false, true] {
			let trust = Trust::new(Some(&pinned), Some(&signer), unsigned);
			assert!(trust.required_signer().unwrap().is_some());
		}
	}

	#[test]
	fn the_digest_must_be_the_pinned_one() {
		let d = digest();
		let mut other = d;
		other[31] ^= 1;
		ensure_digest(&d, &d).unwrap();
		let err = ensure_digest(&other, &d).unwrap_err().to_string();
		assert!(err.contains("MANIFEST digest is sha256:"), "{err}");
		assert!(
			Trust {
				digest: None,
				signer: None,
				unsigned: true,
			}
			.pinned()
			.is_err()
		);
	}
}
