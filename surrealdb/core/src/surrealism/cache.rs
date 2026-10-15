use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use anyhow::{Result, bail};
use quick_cache::{Equivalent, Weighter};

use crate::catalog::{DatabaseId, NamespaceId};
use crate::dbs::Capabilities;
use crate::expr::{ExtensionCapabilities, ManifestDigest};
use crate::extension::trust::TrustStore;
use crate::surrealism::runner::{
	ExtensionRunner, NO_RUNNER, PreparedExtension, RunnerCapabilities, VerifiedSource,
};

/// The datastore's extensions: what each definition resolves to, and the
/// prepared code behind it.
///
/// Three maps, none of which holds tenant data:
///
/// - **Prepared**, keyed by [`ManifestDigest`]: the runner's compiled extension. It holds no tenant
///   state — every invocation runs in fresh memory — so every definition whose archive has the same
///   digest shares one entry, in any namespace or database.
/// - **Sources**, keyed by namespace, database, source and the `DIGEST` the definition pins: that
///   the source delivered that archive. Keyed by `DIGEST` too, so definitions that pin different
///   archives at one source keep separate entries instead of evicting each other.
/// - **Grants**, keyed by a definition's `CAPABILITIES` and the datastore's capabilities: what an
///   invocation of a definition with that clause may do. See [`SurrealismCache::grant`].
///
/// Neither level vouches for an archive: the caller checks the definition's
/// `DIGEST` and `SIGNER` against the [`TrustStore`] before an archive is
/// prepared and again on every call, since one prepared entry serves
/// definitions that name different signers.
pub struct SurrealismCache {
	runner: Option<Arc<dyn ExtensionRunner>>,
	sources: quick_cache::sync::Cache<SourceKey, Arc<SourceEntry>, Weight>,
	prepared: quick_cache::sync::Cache<ManifestDigest, Arc<dyn PreparedExtension>, Weight>,
	grants: quick_cache::sync::Cache<GrantKey, Arc<RunnerCapabilities>, Weight>,
	trust: TrustStore,
}

/// What one definition's source resolved to.
pub(crate) struct SourceEntry {
	pub digest: ManifestDigest,
	/// The `SIGNATURE` the source delivered with that digest.
	pub signature: Option<Arc<[u8]>>,
	/// `organisation::name`, for logging.
	pub display_name: Arc<str>,
}

/// A definition ready to invoke.
#[derive(Clone)]
pub(crate) struct SurrealismCachedModule {
	pub prepared: Arc<dyn PreparedExtension>,
	/// `sha256(surrealism/MANIFEST)` of the archive `prepared` was built from.
	pub digest: ManifestDigest,
	/// The `SIGNATURE` the definition's source delivered with that archive.
	/// Every load checks it against the loading definition's `SIGNER`.
	pub signature: Option<Arc<[u8]>>,
	pub module_display_name: Arc<str>,
}

impl SurrealismCache {
	pub fn new(size: usize, runner: Option<Arc<dyn ExtensionRunner>>) -> Self {
		Self {
			runner,
			sources: quick_cache::sync::Cache::with_weighter(size, size as u64, Weight),
			prepared: quick_cache::sync::Cache::with_weighter(size, size as u64, Weight),
			grants: quick_cache::sync::Cache::with_weighter(size, size as u64, Weight),
			trust: TrustStore::default(),
		}
	}

	/// The signers each source's `SIGNATURE` has proved, and the refusals.
	pub(crate) fn trust(&self) -> &TrustStore {
		&self.trust
	}

	/// The installed runner, shared with a datastore derived from this one.
	pub fn runner(&self) -> Option<Arc<dyn ExtensionRunner>> {
		self.runner.clone()
	}

	fn require_runner(&self) -> Result<&Arc<dyn ExtensionRunner>> {
		match &self.runner {
			Some(runner) => Ok(runner),
			None => bail!(NO_RUNNER),
		}
	}

	/// Forget that the source at `lookup` delivered the archive with `digest`.
	/// The prepared code stays, for any other definition with the same digest.
	pub fn remove(&self, lookup: &SurrealismCacheLookup, digest: [u8; 32]) {
		self.sources.remove(&SourceLookup(lookup, &ManifestDigest::sha256(digest)));
	}

	/// The definition behind `lookup`, from the cache when its source resolved
	/// to `pinned`, the definition's `DIGEST`, and the prepared code for it is
	/// there.
	///
	/// Otherwise `fetch` produces the archive, verified against the
	/// definition, and its digest selects or creates the prepared entry.
	/// A source entry for another digest was resolved from what the source
	/// held earlier, and a prepared entry can be evicted while its source entry
	/// survives; both are fetched again the same way, so a definition always
	/// runs the archive it pins.
	pub(crate) async fn get_or_load<F, Fut>(
		&self,
		lookup: &SurrealismCacheLookup<'_>,
		pinned: &ManifestDigest,
		fetch: F,
	) -> Result<SurrealismCachedModule>
	where
		F: FnOnce() -> Fut,
		Fut: Future<Output = Result<VerifiedSource>>,
	{
		if let Some(source) = self.sources.get(&SourceLookup(lookup, pinned))
			&& let Some(prepared) = self.prepared.get(&source.digest)
		{
			return Ok(SurrealismCachedModule {
				prepared,
				digest: source.digest.clone(),
				signature: source.signature.clone(),
				module_display_name: Arc::clone(&source.display_name),
			});
		}

		let runner = self.require_runner()?;
		let source = fetch().await?;
		let digest = source.digest.clone();
		let prepared = self
			.prepared
			.get_or_insert_async(&digest, async { runner.prepare(&source).await })
			.await?;

		let package = prepared.package();
		let display_name: Arc<str> =
			format!("{}::{}", package.meta.organisation, package.meta.name).into();
		self.sources.insert(
			(lookup.to_key(), digest.clone()),
			Arc::new(SourceEntry {
				digest: digest.clone(),
				signature: source.signature.clone(),
				display_name: Arc::clone(&display_name),
			}),
		);
		Ok(SurrealismCachedModule {
			prepared,
			digest,
			signature: source.signature.clone(),
			module_display_name: display_name,
		})
	}

	/// What an invocation of a definition whose `CAPABILITIES` are
	/// `capabilities` may do on a datastore running with `server`: built by
	/// `build` the first time, and refused when the runner cannot honour it.
	///
	/// `server` must be the datastore's own capabilities
	/// ([`Context::server_capabilities`](crate::ctx::Context::server_capabilities)),
	/// never the narrowed ones of the context a call is made from. An
	/// extension that another extension calls through `tx.run` runs with its
	/// own definition's grant, which can be more than its caller's; whether
	/// the caller may call it at all is checked before this. That makes the
	/// grant a function of the two keys alone, whichever call builds it first.
	///
	/// A grant is shared by every definition that writes the same clause. One
	/// built from capabilities the datastore has since replaced is never
	/// served again: the key holds the `Arc` it was built from and compares
	/// it by identity, so the allocation cannot be reused for the new ones,
	/// and the stale entry ages out.
	///
	/// A caller that finds another one building the same grant waits for it
	/// without blocking its thread.
	pub(crate) async fn grant<B>(
		&self,
		capabilities: &ExtensionCapabilities,
		server: &Arc<Capabilities>,
		build: B,
	) -> Result<Arc<RunnerCapabilities>>
	where
		B: FnOnce() -> Result<RunnerCapabilities>,
	{
		let runner = self.require_runner()?;
		let lookup = GrantLookup {
			capabilities,
			server,
		};
		if let Some(grant) = self.grants.get(&lookup) {
			return Ok(grant);
		}
		self.grants
			.get_or_insert_async(&GrantKey::from(&lookup), async {
				let built = build()?;
				runner.check(&built)?;
				Ok(Arc::new(built))
			})
			.await
	}

	/// How many prepared extensions are held.
	#[cfg(test)]
	pub(crate) fn prepared_len(&self) -> usize {
		self.prepared.len()
	}
}

/// A source entry's key: where the archive comes from, and the `DIGEST` the
/// definition pins.
type SourceKey = (SurrealismCacheKey, ManifestDigest);

/// The borrowed form of a [`SourceKey`], so a hit allocates nothing. It
/// hashes as the tuple does: the source, then the digest.
#[derive(Hash)]
struct SourceLookup<'a, 'b>(&'a SurrealismCacheLookup<'b>, &'a ManifestDigest);

impl Equivalent<SourceKey> for SourceLookup<'_, '_> {
	fn equivalent(&self, key: &SourceKey) -> bool {
		*self.1 == key.1 && self.0.equivalent(&key.0)
	}
}

/// The key of a grant: a definition's `CAPABILITIES` and the datastore
/// capabilities it was intersected with, the latter compared by identity.
#[derive(Clone)]
pub(crate) struct GrantKey {
	capabilities: ExtensionCapabilities,
	server: Arc<Capabilities>,
}

impl Hash for GrantKey {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.capabilities.hash(state);
	}
}

impl PartialEq for GrantKey {
	fn eq(&self, other: &Self) -> bool {
		self.capabilities == other.capabilities && Arc::ptr_eq(&self.server, &other.server)
	}
}

impl Eq for GrantKey {}

/// A [`GrantKey`] borrowed from the call, so a hit allocates nothing.
struct GrantLookup<'a> {
	capabilities: &'a ExtensionCapabilities,
	server: &'a Arc<Capabilities>,
}

impl Hash for GrantLookup<'_> {
	fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
		self.capabilities.hash(state);
	}
}

impl Equivalent<GrantKey> for GrantLookup<'_> {
	fn equivalent(&self, key: &GrantKey) -> bool {
		*self.capabilities == key.capabilities && Arc::ptr_eq(self.server, &key.server)
	}
}

impl From<&GrantLookup<'_>> for GrantKey {
	fn from(lookup: &GrantLookup<'_>) -> Self {
		GrantKey {
			capabilities: lookup.capabilities.clone(),
			server: Arc::clone(lookup.server),
		}
	}
}

/// Identity of a definition's source.
///
/// Scoped by namespace and database, because two databases can hold different
/// archives under the same bucket and key. The prepared code it points at is
/// not scoped: it is keyed by digest alone.
#[derive(Clone, Hash, Eq, PartialEq)]
pub enum SurrealismCacheKey {
	// NS - DB - BUCKET - KEY
	File(NamespaceId, DatabaseId, String, String),
	// NS - DB - Organisation - Package - MAJOR - MINOR - PATCH
	Silo(NamespaceId, DatabaseId, String, String, u32, u32, u32),
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum SurrealismCacheLookup<'a> {
	// NS - DB - BUCKET - KEY
	File(&'a NamespaceId, &'a DatabaseId, &'a str, &'a str),
	// NS - DB - Organisation - Package - MAJOR - MINOR - PATCH
	Silo(&'a NamespaceId, &'a DatabaseId, &'a str, &'a str, u32, u32, u32),
}

impl SurrealismCacheLookup<'_> {
	pub fn to_key(&self) -> SurrealismCacheKey {
		match self {
			SurrealismCacheLookup::File(ns, db, bucket, key) => {
				SurrealismCacheKey::File(**ns, **db, (*bucket).to_string(), (*key).to_string())
			}
			SurrealismCacheLookup::Silo(ns, db, org, pkg, maj, min, pat) => {
				SurrealismCacheKey::Silo(
					**ns,
					**db,
					(*org).to_string(),
					(*pkg).to_string(),
					*maj,
					*min,
					*pat,
				)
			}
		}
	}
}

impl<'a> From<SurrealismCacheLookup<'a>> for SurrealismCacheKey {
	fn from(lookup: SurrealismCacheLookup<'a>) -> Self {
		lookup.to_key()
	}
}

impl Equivalent<SurrealismCacheKey> for SurrealismCacheLookup<'_> {
	fn equivalent(&self, key: &SurrealismCacheKey) -> bool {
		match (self, key) {
			(Self::File(a1, b1, c1, d1), SurrealismCacheKey::File(a2, b2, c2, d2)) => {
				a1.0 == a2.0 && b1.0 == b2.0 && c1 == c2 && d1 == d2
			}
			(
				Self::Silo(a1, b1, c1, d1, e1, f1, g1),
				SurrealismCacheKey::Silo(a2, b2, c2, d2, e2, f2, g2),
			) => {
				a1.0 == a2.0
					&& b1.0 == b2.0 && c1 == c2
					&& d1 == d2 && e1 == e2
					&& f1 == f2 && g1 == g2
			}
			_ => false,
		}
	}
}

/// Uniform weight: each entry counts as 1 toward the budget, giving a hard cap
/// of `SURREALISM_CACHE_SIZE` entries per level. Compiled size is a poor proxy
/// for what an entry costs once its memory image is shared copy-on-write.
#[derive(Clone)]
pub(crate) struct Weight;

impl<K, V> Weighter<K, V> for Weight {
	fn weight(&self, _key: &K, _val: &V) -> u64 {
		1
	}
}

#[cfg(test)]
mod tests {
	use std::collections::hash_map::DefaultHasher;
	use std::hash::Hasher;
	use std::sync::atomic::{AtomicUsize, Ordering};

	use surrealism_runtime::config::SurrealismConfig as PackageConfig;
	use surrealism_runtime::exports::ExportsManifest;

	use super::*;
	use crate::surrealism::runner::{BoxFut, Invocation};
	use crate::types::PublicValue;

	fn hash_of<T: Hash>(value: &T) -> u64 {
		let mut hasher = DefaultHasher::new();
		value.hash(&mut hasher);
		hasher.finish()
	}

	/// The same source in two databases is two source entries: each carries
	/// its own definition's capabilities.
	#[test]
	fn source_entries_are_scoped_per_database() {
		let ns = NamespaceId(1);
		let db = DatabaseId(1);
		let other_ns = NamespaceId(2);
		let other_db = DatabaseId(2);

		let base = SurrealismCacheLookup::Silo(&ns, &db, "org", "pkg", 1, 2, 3);
		let key = base.to_key();

		assert!(base.equivalent(&key));
		assert!(
			!SurrealismCacheLookup::Silo(&other_ns, &db, "org", "pkg", 1, 2, 3).equivalent(&key)
		);
		assert!(
			!SurrealismCacheLookup::Silo(&ns, &other_db, "org", "pkg", 1, 2, 3).equivalent(&key)
		);
		assert!(!SurrealismCacheLookup::Silo(&ns, &db, "org", "pkg", 1, 2, 4).equivalent(&key));
	}

	/// `quick_cache` looks a key up by the lookup's hash before comparing with
	/// `Equivalent`, so the borrowed and owned forms must hash identically.
	#[test]
	fn lookup_and_key_hash_identically() {
		let ns = NamespaceId(7);
		let db = DatabaseId(9);

		let silo = SurrealismCacheLookup::Silo(&ns, &db, "org", "pkg", 1, 2, 3);
		assert_eq!(hash_of(&silo), hash_of(&silo.to_key()));

		let file = SurrealismCacheLookup::File(&ns, &db, "bucket", "key");
		assert_eq!(hash_of(&file), hash_of(&file.to_key()));
	}

	/// A runner that counts what it prepares and runs nothing.
	#[derive(Default)]
	struct CountingRunner {
		prepared: AtomicUsize,
	}

	struct Inert {
		package: PackageConfig,
		exports: ExportsManifest,
	}

	impl PreparedExtension for Inert {
		fn package(&self) -> &PackageConfig {
			&self.package
		}
		fn exports(&self) -> &ExportsManifest {
			&self.exports
		}
		fn invoke<'a>(&'a self, _: Invocation<'a>) -> BoxFut<'a, Result<Vec<PublicValue>>> {
			Box::pin(async { Ok(Vec::new()) })
		}
	}

	impl ExtensionRunner for CountingRunner {
		fn name(&self) -> &'static str {
			"counting"
		}
		fn check(&self, _: &RunnerCapabilities) -> Result<()> {
			Ok(())
		}
		fn prepare<'a>(
			&'a self,
			_: &'a VerifiedSource,
		) -> BoxFut<'a, Result<Arc<dyn PreparedExtension>>> {
			self.prepared.fetch_add(1, Ordering::SeqCst);
			Box::pin(async {
				Ok(Arc::new(Inert {
					package: surrealism_runtime::package_config::PackageConfig::parse(
						"[package]\norganisation = \"test\"\nname = \"inert\"\nversion = \"1.0.0\"\n",
					)
					.expect("package manifest")
					.into_config(),
					exports: ExportsManifest::empty(),
				}) as Arc<dyn PreparedExtension>)
			})
		}
	}

	fn digest_of(archive: &[u8]) -> ManifestDigest {
		use sha2::Digest as _;
		ManifestDigest::sha256(sha2::Sha256::digest(archive).into())
	}

	fn source(archive: &[u8]) -> VerifiedSource {
		VerifiedSource {
			digest: digest_of(archive),
			archive: bytes::Bytes::copy_from_slice(archive),
			temp_base: None,
			max_fs_bytes: 0,
			signature: None,
		}
	}

	/// Two databases whose definitions resolve to the same archive share one
	/// prepared entry: the runner prepares it once, and both get the same
	/// object back.
	#[tokio::test]
	async fn two_databases_share_one_prepared_entry() {
		let runner = Arc::new(CountingRunner::default());
		let cache = SurrealismCache::new(16, Some(Arc::clone(&runner) as Arc<dyn ExtensionRunner>));
		let ns = NamespaceId(1);
		let (db_a, db_b) = (DatabaseId(1), DatabaseId(2));

		let a = cache
			.get_or_load(
				&SurrealismCacheLookup::File(&ns, &db_a, "bucket", "a.surli"),
				&digest_of(b"archive"),
				async || Ok(source(b"archive")),
			)
			.await
			.unwrap();
		let b = cache
			.get_or_load(
				&SurrealismCacheLookup::Silo(&ns, &db_b, "org", "pkg", 1, 0, 0),
				&digest_of(b"archive"),
				async || Ok(source(b"archive")),
			)
			.await
			.unwrap();

		assert_eq!(runner.prepared.load(Ordering::SeqCst), 1);
		assert_eq!(cache.prepared_len(), 1);
		assert!(Arc::ptr_eq(&a.prepared, &b.prepared));

		// Different bytes are different code.
		cache
			.get_or_load(
				&SurrealismCacheLookup::File(&ns, &db_a, "bucket", "b.surli"),
				&digest_of(b"other archive"),
				async || Ok(source(b"other archive")),
			)
			.await
			.unwrap();
		assert_eq!(runner.prepared.load(Ordering::SeqCst), 2);
	}

	/// A source entry resolved to another digest than the definition pins is
	/// not served: the source is fetched again, and the pinned archive is
	/// prepared.
	#[tokio::test]
	async fn a_source_entry_for_another_digest_is_fetched_again() {
		let runner = Arc::new(CountingRunner::default());
		let cache = SurrealismCache::new(16, Some(Arc::clone(&runner) as Arc<dyn ExtensionRunner>));
		let (ns, db) = (NamespaceId(1), DatabaseId(1));
		let lookup = SurrealismCacheLookup::File(&ns, &db, "bucket", "a.surli");

		let old = cache
			.get_or_load(&lookup, &digest_of(b"old"), async || Ok(source(b"old")))
			.await
			.unwrap();
		let fetched = AtomicUsize::new(0);
		let new = cache
			.get_or_load(&lookup, &digest_of(b"new"), async || {
				fetched.fetch_add(1, Ordering::SeqCst);
				Ok(source(b"new"))
			})
			.await
			.unwrap();

		assert_eq!(fetched.load(Ordering::SeqCst), 1);
		assert_eq!(old.digest, digest_of(b"old"));
		assert_eq!(new.digest, digest_of(b"new"));
		assert!(!Arc::ptr_eq(&old.prepared, &new.prepared));
	}

	/// A grant is built once per distinct `CAPABILITIES` value and shared by
	/// every definition that writes the same clause.
	#[tokio::test]
	async fn equal_capabilities_share_one_grant() {
		let cache = SurrealismCache::new(16, Some(Arc::new(CountingRunner::default()) as _));
		let server = Arc::new(Capabilities::default());
		let built = AtomicUsize::new(0);
		let build = || {
			built.fetch_add(1, Ordering::SeqCst);
			Ok(crate::surrealism::host::test_capabilities())
		};
		let none = ExtensionCapabilities::default();
		let queries = ExtensionCapabilities {
			queries: true,
			..Default::default()
		};
		let a = cache.grant(&none, &server, build).await.unwrap();
		let b = cache.grant(&none, &server, build).await.unwrap();
		cache.grant(&queries, &server, build).await.unwrap();
		assert!(Arc::ptr_eq(&a, &b));
		assert_eq!(built.load(Ordering::SeqCst), 2);
	}

	/// Capabilities the datastore swapped in are never served a grant built
	/// from the ones they replaced, even when the two are equal in value.
	#[tokio::test]
	async fn replaced_server_capabilities_build_a_new_grant() {
		let cache = SurrealismCache::new(16, Some(Arc::new(CountingRunner::default()) as _));
		let caps = ExtensionCapabilities::default();
		let built = AtomicUsize::new(0);
		let build = || {
			built.fetch_add(1, Ordering::SeqCst);
			Ok(crate::surrealism::host::test_capabilities())
		};
		let before = Arc::new(Capabilities::default());
		let first = cache.grant(&caps, &before, build).await.unwrap();
		let after = Arc::new(Capabilities::default());
		let second = cache.grant(&caps, &after, build).await.unwrap();
		assert!(!Arc::ptr_eq(&first, &second));
		assert_eq!(built.load(Ordering::SeqCst), 2);
		// The old capabilities still resolve to their own grant while held.
		assert!(Arc::ptr_eq(&first, &cache.grant(&caps, &before, build).await.unwrap()));
	}

	/// Without a runner, loading fails with the error that names the cause.
	#[tokio::test]
	async fn no_runner_fails_clearly() {
		let cache = SurrealismCache::new(16, None);
		let (ns, db) = (NamespaceId(1), DatabaseId(1));
		let err = cache
			.get_or_load(
				&SurrealismCacheLookup::File(&ns, &db, "bucket", "a.surli"),
				&digest_of(b"archive"),
				async || Ok(source(b"archive")),
			)
			.await
			.err()
			.unwrap();
		assert_eq!(err.to_string(), NO_RUNNER);
	}
}
