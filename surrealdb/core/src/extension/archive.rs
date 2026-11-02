//! Fetching an extension's archive, and the `DIGEST` that pins it.
//!
//! `DEFINE EXTENSION` fetches the archive and checks it through
//! [`verify_definition`], recording its digest in the definition when the
//! statement gave none. Nothing is recorded later, when an extension loads, so
//! a read path never writes the catalog.

use anyhow::{Result, bail};
use bytes::Bytes;

use crate::catalog::{DatabaseId, ModuleExecutable, NamespaceId};
use crate::ctx::Context;
use crate::expr::{ExtensionSigner, ManifestDigest};

/// Where an archive comes from, borrowed from a definition or a cache lookup.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(
	not(feature = "surrealism"),
	expect(dead_code, reason = "only a build that reads archives fetches them")
)]
pub(crate) enum ArchiveSource<'a> {
	/// An object in one of the database's buckets.
	File {
		bucket: &'a str,
		key: &'a str,
	},
	/// A package release on the Silo endpoint.
	Silo {
		organisation: &'a str,
		package: &'a str,
		major: u32,
		minor: u32,
		patch: u32,
	},
}

#[cfg(feature = "surrealism")]
impl ArchiveSource<'_> {
	/// The source as text, for keying what is recorded about it.
	pub(crate) fn key(&self) -> String {
		match self {
			ArchiveSource::File {
				bucket,
				key,
			} => format!("file\0{bucket}\0{key}"),
			ArchiveSource::Silo {
				organisation,
				package,
				major,
				minor,
				patch,
			} => format!("silo\0{organisation}\0{package}\0{major}.{minor}.{patch}"),
		}
	}
}

impl<'a> From<&'a ModuleExecutable> for ArchiveSource<'a> {
	fn from(executable: &'a ModuleExecutable) -> Self {
		match executable {
			ModuleExecutable::Surrealism(s) => ArchiveSource::File {
				bucket: &s.bucket,
				key: &s.key,
			},
			ModuleExecutable::Silo(s) => ArchiveSource::Silo {
				organisation: &s.organisation,
				package: &s.package,
				major: s.major,
				minor: s.minor,
				patch: s.patch,
			},
		}
	}
}

/// Check, as `DEFINE EXTENSION` runs, that the archive `executable` names may
/// run under the definition, and pin its `DIGEST` when the statement gave none.
///
/// The archive is fetched and its `surrealism/MANIFEST` read. `DIGEST` is
/// `sha256(MANIFEST)`: recorded in `digest` when absent, and otherwise required
/// to equal it. With a `SIGNER`, the archive's `SIGNATURE` must prove it; see
/// [`crate::extension::trust`].
///
/// An archive that cannot be fetched fails the definition when it has no
/// `DIGEST`, since there is nothing to pin it to. With a `DIGEST` it is
/// accepted unchecked, so an export restores before its archives are in place;
/// the check before every load still applies. A build without the Surrealism
/// runtime cannot read archives, so there a definition must give `DIGEST`, and
/// is accepted without a fetch.
///
/// Returns the archive's bytes when it was fetched and passed, so the module
/// can be built from them without a second fetch, and clears any refusal
/// recorded for this definition's source.
#[cfg(feature = "surrealism")]
pub(crate) async fn verify_definition(
	ctx: &Context,
	ns: NamespaceId,
	db: DatabaseId,
	executable: &ModuleExecutable,
	digest: &mut Option<ManifestDigest>,
	signer: Option<&ExtensionSigner>,
	unsigned: bool,
) -> Result<Option<Bytes>> {
	use crate::extension::trust::{RefusalKey, Trust, TrustStore, digest_bytes, ensure_digest};

	let source = ArchiveSource::from(executable);
	let archive = match fetch_archive(ctx, ns, db, source).await {
		Ok(archive) => archive,
		Err(e) if digest.is_none() => {
			bail!("Cannot compute the extension's DIGEST: {e:#}")
		}
		Err(_) => return Ok(None),
	};
	let files = surrealism_runtime::package::trust_files_from_reader(
		archive.as_ref(),
		ctx.config.surrealism.surrealism_max_fs_bytes,
	)?;
	let computed = files.manifest.digest().0;
	match digest {
		Some(pinned) => ensure_digest(&computed, &digest_bytes(pinned)?)?,
		None => *digest = Some(ManifestDigest::sha256(computed)),
	}

	let cache = ctx.get_surrealism_cache();
	let local;
	let store = match &cache {
		Some(cache) => cache.trust(),
		None => {
			local = TrustStore::default();
			&local
		}
	};
	let trust = Trust::new(digest.as_ref(), signer, unsigned);
	store.ensure(ctx, ns, db, None, &computed, files.signature.as_deref(), trust).await?;
	store.record(RefusalKey::new(ns, db, source, computed, &trust), &Ok(()));
	Ok(Some(archive))
}

#[cfg(not(feature = "surrealism"))]
pub(crate) async fn verify_definition(
	_ctx: &Context,
	_ns: NamespaceId,
	_db: DatabaseId,
	_executable: &ModuleExecutable,
	digest: &mut Option<ManifestDigest>,
	_signer: Option<&ExtensionSigner>,
	_unsigned: bool,
) -> Result<Option<Bytes>> {
	if digest.is_none() {
		bail!(
			"Cannot compute the extension's DIGEST: this build cannot read extension archives, so the definition must give `DIGEST`"
		);
	}
	Ok(None)
}

/// Fetch the archive `source` names, for the database `ns`/`db`.
///
/// A bucket object is read through the database's bucket. A Silo package is
/// downloaded from `surrealism_silo_endpoint`, which needs a build with both
/// Surrealism and HTTP support.
#[cfg(feature = "surrealism")]
pub(crate) async fn fetch_archive(
	ctx: &Context,
	ns: NamespaceId,
	db: DatabaseId,
	source: ArchiveSource<'_>,
) -> Result<Bytes> {
	match source {
		ArchiveSource::File {
			bucket,
			key,
		} => {
			let store =
				ctx.get_bucket_store(crate::catalog::BucketLevel::Db(ns, db), bucket).await?;
			let key = crate::buc::store::ObjectKey::new(key);
			match store.get(&key).await.map_err(|e| anyhow::anyhow!("failed to get file: {e}"))? {
				Some(archive) => Ok(archive),
				None => bail!("file not found"),
			}
		}
		ArchiveSource::Silo {
			organisation,
			package,
			major,
			minor,
			patch,
		} => fetch_silo(ctx, organisation, package, major, minor, patch).await,
	}
}

#[cfg(all(feature = "surrealism", feature = "http"))]
async fn fetch_silo(
	ctx: &Context,
	org: &str,
	pkg: &str,
	major: u32,
	minor: u32,
	patch: u32,
) -> Result<Bytes> {
	use anyhow::Context as _;

	use crate::dbs::capabilities::{NetTarget, Targets};
	use crate::http::HttpClient;

	let endpoint = &ctx.config.surrealism.surrealism_silo_endpoint;
	let url = crate::surrealism::silo::package_url(endpoint, org, pkg, major, minor, patch)?;
	// A silo fetch is first-party package resolution rather than
	// user-directed traffic, so it needs no `allow_net` grant. An operator's
	// explicit deny is still honoured.
	let allow = match url.host() {
		Some(host) => Targets::Some(
			[NetTarget::Host(host.to_owned(), url.port_or_known_default())].into_iter().collect(),
		),
		None => bail!("The silo module endpoint `{endpoint}` has no host"),
	};
	let client = HttpClient::new(
		allow,
		ctx.get_capabilities().denied_network_targets_ref().clone(),
		&ctx.config.http,
	)
	.context("Failed to create http client for silo module packages")?;
	crate::surrealism::silo::fetch_package(&client, &url, org, pkg, major, minor, patch).await
}

#[cfg(all(feature = "surrealism", not(feature = "http")))]
async fn fetch_silo(
	_ctx: &Context,
	org: &str,
	pkg: &str,
	major: u32,
	minor: u32,
	patch: u32,
) -> Result<Bytes> {
	bail!(
		"The silo module `silo::{org}::{pkg}<{major}.{minor}.{patch}>` cannot be fetched: this build does not support silo module packages, so its definition must give `DIGEST`"
	)
}
