//! Resolving and authorising one request against a bucket object.
//!
//! The entry point a transport reaches for when it holds a bucket, a key and
//! either a signature or a session, and needs to know whether the request is
//! allowed before it moves any bytes. Nothing here is HTTP: no header is read,
//! no status is chosen, and the caller decides how an outcome is reported. A
//! signature is verified here rather than at the edge so that it sits beside
//! the catalog lookup that resolves the ids it is bound to, and so that no
//! transport has to hold the signing secret.
//!
//! # Order of operations
//!
//! Each step guards the next, and the order is load-bearing:
//!
//! 1. Resolve the bucket from the location and name given. This yields the **ids** of the level it
//!    is defined at and the nonce, which is what a signature is checked against — never anything
//!    the caller asserts.
//! 2. Authenticate. A signature wins over a session when both are present. A session must
//!    additionally be in scope for the location, because it came from the caller and has already
//!    replaced whatever the session had selected. An anonymous session is a guest, admitted only
//!    where the node allows guest access, exactly as on every other route.
//! 3. Check expiry, *after* the signature. `exp` is inside the signed blob, so a URL that verifies
//!    is one whose expiry has not been edited.
//! 4. Normalise the key and reject traversal. A valid signature proves that SurrealDB authorised
//!    this exact request; it does not prove the request is safe, so validation still runs, on the
//!    same value the signature covered.
//! 5. Move the bytes.

use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use bytes::Bytes;
use futures::stream::BoxStream;

use super::presign::{self, BucketScope, PresignGrant, PresignOp, PresignParams, ScopeLevel};
use super::store::{
	ByteRange, ListEntry, ListOptions, ObjectKey, ObjectMeta, ObjectStore, ObjectStream,
	PutCondition, PutError, PutOptions, PutResult,
};
use super::{BucketController, BucketOperation, Error};
use crate::catalog::BucketDefinition;
use crate::catalog::providers::BucketProvider;
use crate::dbs::Session;
use crate::iam::{Auth, Level};
use crate::kvs::Datastore;

/// Where a request addresses a bucket: the level it is defined at, by name.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BucketLocation<'a> {
	Root,
	Namespace(&'a str),
	Database(&'a str, &'a str),
}

impl<'a> BucketLocation<'a> {
	fn ns(self) -> Option<&'a str> {
		match self {
			BucketLocation::Root => None,
			BucketLocation::Namespace(ns) | BucketLocation::Database(ns, _) => Some(ns),
		}
	}

	fn db(self) -> Option<&'a str> {
		match self {
			BucketLocation::Database(_, db) => Some(db),
			BucketLocation::Root | BucketLocation::Namespace(_) => None,
		}
	}

	/// Whether a principal may address a bucket here at all: a root principal
	/// anywhere, a namespace principal in its namespace, a database or record
	/// principal in its database. An anonymous principal is a guest, judged by
	/// the guest rule instead.
	fn admits(self, au: &Auth) -> bool {
		match self {
			BucketLocation::Root => matches!(au.level(), Level::Root | Level::No),
			BucketLocation::Namespace(ns) => match au.level() {
				Level::Root | Level::No => true,
				Level::Namespace(n) => n == ns,
				Level::Database(..) | Level::Record(..) => false,
			},
			BucketLocation::Database(ns, db) => au.can_access_ns_db(ns, db),
		}
	}
}

/// The signed parameters a request presented.
///
/// Parsed by the transport, which owns the wire format, and interpreted only
/// here.
#[derive(Clone, Debug)]
pub struct SignedQuery {
	pub exp: i64,
	pub max: Option<u64>,
	pub overwrite: bool,
	/// The media type an upload URL was minted for, as it appears in the URL:
	/// still percent-encoded. Decoded here rather than at the transport, beside
	/// the check of the signature that covers it.
	pub content_type: Option<String>,
	pub sig: String,
}

/// How a bucket request proved it was allowed.
pub enum BucketAuth {
	/// A presigned URL. The bucket's `PERMISSIONS` clause was evaluated when it
	/// was minted and is not consulted again.
	Signature(SignedQuery),
	/// An ordinary session. The clause is evaluated for this request.
	Session,
}

/// What a request may do, once it has been authorised.
///
/// A session-authorised request carries no limits, because the clause that
/// allowed it ran against this request rather than a future one.
pub struct BucketGrant {
	/// Byte ceiling for an upload.
	pub max: Option<u64>,
	/// Whether an upload may replace an existing object.
	pub overwrite: bool,
	/// The media types an upload may store.
	pub content_type: ContentTypeGrant,
}

/// The media types an upload may store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContentTypeGrant {
	/// Whatever the caller names. A session's upload, whose permission clause
	/// ran against this request.
	Any,
	/// Exactly this, and `None` means no type at all. A presigned upload, whose
	/// type was fixed when it was minted: without that, whoever holds the URL
	/// could store `text/html` and have it served from the database's origin.
	Exactly(Option<String>),
}

impl From<PresignGrant> for BucketGrant {
	fn from(grant: PresignGrant) -> Self {
		Self {
			max: grant.max,
			overwrite: grant.overwrite,
			content_type: ContentTypeGrant::Exactly(grant.content_type),
		}
	}
}

/// How a copy or rename ended, when it did not fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferOutcome {
	/// The object was copied or moved.
	Done,
	/// Nothing was at the source key.
	SourceMissing,
	/// The target was occupied and the request asked not to replace it.
	TargetExists,
}

/// Why a bucket request was refused.
///
/// Three outcomes rather than one because a caller answers them differently —
/// an HTTP transport reports them as `410`, `403` and a mapped error — and
/// `buc::Error` is not public. Deliberately coarse beyond that: a caller learns
/// that its credential did not authorise the request, never which part of it
/// failed, because naming the part is an oracle.
#[derive(Debug, thiserror::Error)]
pub enum BucketAccessError {
	/// A genuine presigned URL, presented past its expiry.
	///
	/// Separate from `Denied` because the remedy differs: the URL was valid, so
	/// a caller should mint a new one rather than retry or re-authenticate.
	#[error("The presigned URL has expired")]
	Expired,

	/// No usable credential, or one that does not authorise this request.
	#[error("{0}")]
	Denied(anyhow::Error),

	/// The request names a media type the bucket's backend cannot store.
	///
	/// Refused rather than dropped, so a caller never believes a type was kept
	/// when it was not.
	#[error("{0}")]
	UnsupportedMediaType(anyhow::Error),

	/// Anything else — an unknown bucket, an unreachable store.
	#[error("{0}")]
	Other(anyhow::Error),
}

impl BucketAccessError {
	fn classify(error: anyhow::Error) -> Self {
		match error.downcast_ref::<Error>() {
			Some(Error::PresignExpired) => Self::Expired,
			Some(
				Error::PresignBadSignature
				| Error::BucketPermissions {
					..
				}
				| Error::ReadonlyBucket(_),
			) => Self::Denied(error),
			// A credential that cannot authorise this request, whatever the
			// reason. `Presign` covers a method no signature is minted for and a
			// key this build cannot normalise; `PresignNoSecret` is reached
			// before any signature is checked, so reporting it would tell a
			// caller who has proven nothing how the node is configured. Both
			// are refusals, and the variants stay distinct for the node's logs.
			Some(Error::Presign(_) | Error::PresignNoSecret) => Self::Denied(error),
			Some(Error::ContentTypeUnsupported(_)) => Self::UnsupportedMediaType(error),
			_ => Self::Other(error),
		}
	}
}

/// A resolved, authorised bucket request.
pub struct BucketAccess {
	store: Arc<dyn ObjectStore>,
	bucket: Arc<BucketDefinition>,
	key: ObjectKey,
	/// The destination of a copy or rename, authorised with the request.
	target: Option<ObjectKey>,
	pub grant: BucketGrant,
}

impl Datastore {
	/// Resolve and authorise one bucket request.
	///
	/// The argument list is wide because a bucket request is addressed by its
	/// location, its name and its key, and grouping them into a struct would
	/// hide which of them a signature is bound to.
	///
	/// `key` is the percent-decoded trailing path. It is normalised here, and
	/// the normalised form is what both the signature check and the store see.
	/// `target` is the destination of a copy or rename, which the permission
	/// clause sees as `$target`; it is ignored for every other operation.
	#[allow(clippy::too_many_arguments)]
	#[cfg_attr(
		feature = "observability",
		tracing::instrument(
			err,
			level = "trace",
			target = "surrealdb::core::kvs::ds",
			skip(self, session, auth)
		)
	)]
	pub async fn bucket_access(
		&self,
		session: &Session,
		location: BucketLocation<'_>,
		bucket: &str,
		key: &str,
		op: BucketOperation,
		target: Option<&str>,
		auth: BucketAuth,
	) -> Result<BucketAccess, BucketAccessError> {
		self.bucket_access_inner(session, location, bucket, key, op, target, auth)
			.await
			.map_err(BucketAccessError::classify)
	}

	#[allow(clippy::too_many_arguments)]
	async fn bucket_access_inner(
		&self,
		session: &Session,
		location: BucketLocation<'_>,
		bucket: &str,
		key: &str,
		op: BucketOperation,
		target: Option<&str>,
		auth: BucketAuth,
	) -> Result<BucketAccess> {
		// The same rule as every other route: an anonymous caller is a guest,
		// and is admitted only where the node allows guests.
		let guests_allowed = self.check_anon(session).is_ok();
		let opt = self
			.setup_options(session)
			.with_ns(location.ns().map(Into::into))
			.with_db(location.db().map(Into::into));
		let mut ctx = self.setup_ctx()?;
		let txn = Arc::new(self.transaction(crate::kvs::TransactionType::Read).await?);
		ctx.set_transaction(Arc::clone(&txn));
		ctx.attach_session(session)?;
		let ctx = ctx.freeze();

		// The transaction is cancelled on every exit path below, not dropped:
		// `Transactor::drop` reports a transaction that ended without either,
		// and leaves it to be reaped rather than released now. It is a read, so
		// there is nothing to commit.
		// Read once, here, rather than at the HTTP boundary: expiry is a
		// property of the request, and one request must not straddle two
		// seconds between its parse and its check.
		let resolved = Self::resolve_bucket_access(
			&ctx,
			&opt,
			&txn,
			session,
			location,
			bucket,
			key,
			op,
			target,
			auth,
			guests_allowed,
			chrono::Utc::now().timestamp(),
		)
		.await;
		let _ = txn.cancel().await;
		resolved
	}

	#[allow(clippy::too_many_arguments)]
	async fn resolve_bucket_access(
		ctx: &crate::ctx::FrozenContext,
		opt: &crate::dbs::Options,
		txn: &Arc<crate::kvs::Transaction>,
		session: &Session,
		location: BucketLocation<'_>,
		bucket: &str,
		key: &str,
		op: BucketOperation,
		target: Option<&str>,
		auth: BucketAuth,
		guests_allowed: bool,
		now: i64,
	) -> Result<BucketAccess> {
		// The options select the location's namespace and database, so the
		// level they resolve to is the location's.
		let level = ctx.expect_bucket_level(opt).await?;
		let definition = txn.expect_bucket(level, bucket).await?;
		let store = ctx.get_bucket_store(level, bucket).await?;

		// Normalise before anything reads the key, so the signature check and
		// the store agree on which object this is.
		let key = ObjectKey::new(key);
		key.check_no_traversal().map_err(Error::Presign)?;
		let target = match (op, target) {
			(BucketOperation::Copy | BucketOperation::Rename, Some(target)) => {
				let target = ObjectKey::new(target);
				target.check_no_traversal().map_err(Error::Presign)?;
				Some(target)
			}
			(BucketOperation::Copy | BucketOperation::Rename, None) => {
				bail!(Error::Presign("A copy or rename needs a target".to_string()))
			}
			_ => None,
		};

		if matches!(
			op,
			BucketOperation::Put
				| BucketOperation::Delete
				| BucketOperation::Copy
				| BucketOperation::Rename
		) {
			ensure!(!definition.readonly, Error::ReadonlyBucket(definition.name.to_string()));
		}

		let grant = match auth {
			BucketAuth::Signature(query) => {
				let secret = ctx
					.get_buckets()
					.and_then(|b| b.presign_secret())
					.cloned()
					.ok_or(Error::PresignNoSecret)?;
				let presign_op = match op {
					BucketOperation::Get | BucketOperation::Head => PresignOp::Get,
					BucketOperation::Put => PresignOp::Put,
					// A signed URL names one method. Nothing mints a signature
					// for anything else, so no signature can authorise it.
					BucketOperation::Delete
					| BucketOperation::Copy
					| BucketOperation::Rename
					| BucketOperation::Exists
					| BucketOperation::List
					| BucketOperation::PresignGet
					| BucketOperation::PresignPut => {
						bail!(Error::Presign("This method cannot be presigned".to_string()))
					}
				};

				// A `ct` that does not decode to UTF-8 is no type this node ever
				// signed, so it is refused as any other unverifiable signature is.
				let content_type = query
					.content_type
					.map(|ct| {
						percent_encoding::percent_decode_str(&ct)
							.decode_utf8()
							.map(|ct| ct.into_owned())
							.map_err(|_| Error::PresignBadSignature)
					})
					.transpose()?;
				let params = PresignParams {
					exp: query.exp,
					max: query.max,
					overwrite: query.overwrite,
					content_type,
				};

				presign::verify(
					&secret,
					BucketScope {
						level: ScopeLevel::of(level, opt)?,
						bucket: definition.name.as_str(),
						nonce: definition.presign_nonce,
					},
					presign_op,
					&key,
					params,
					&query.sig,
					now,
				)
				.map_err(|e| match e {
					presign::PresignError::Expired => Error::PresignExpired,
					// Every other cause is reported alike. Telling a client
					// which part of a signature failed hands it an oracle.
					presign::PresignError::InvalidKey
					| presign::PresignError::Malformed
					| presign::PresignError::BadSignature => Error::PresignBadSignature,
				})?
				.into()
			}
			BucketAuth::Session => {
				// An anonymous request is a guest, and is judged as one: refused
				// where the node does not allow guests, and otherwise held to
				// the bucket's clause, as `file::get` from a guest's query is.
				// A caller with no session on a node without guest access uses
				// a presigned URL, which is what they are for.
				ensure!(!session.au.is_anon() || guests_allowed, Error::PresignBadSignature);

				// The location was taken from the request path and has already
				// replaced whatever the session had selected, so the
				// authenticated level is the only trustworthy scope. A
				// principal authenticated for one tenant must not reach
				// another's bucket, nor one above its own level, and
				// `PERMISSIONS FULL` would allow exactly that -- the clause says
				// "any authenticated caller" and cannot see which tenant was
				// asked for. The same gate guards `/api/{ns}/{db}` for the same
				// reason (GHSA-848m-r628-vrxw).
				//
				// A signature needs no equivalent check: the nonce and the
				// level's ids are inside its key derivation, so one minted for
				// another tenant or level derives a different key and fails to
				// verify.
				ensure!(
					location.admits(&session.au),
					Error::BucketPermissions {
						name: definition.name.to_string(),
						op,
					}
				);

				// The clause runs for this request, so nothing is carried
				// forward: a session-authorised upload is bounded by the
				// transport limit alone.
				let mut controller = BucketController::from_parts(
					ctx,
					opt,
					Arc::clone(&definition),
					Arc::clone(&store),
					level,
				);
				controller.check_permission(op, Some(&key), target.as_ref()).await?;

				BucketGrant {
					max: None,
					overwrite: true,
					content_type: ContentTypeGrant::Any,
				}
			}
		};

		Ok(BucketAccess {
			store,
			bucket: definition,
			key,
			target,
			grant,
		})
	}
}

impl BucketAccess {
	/// Open the object for streamed reading.
	pub async fn get(&self, range: Option<ByteRange>) -> Result<Option<ObjectStream>> {
		self.store
			.get_streamed(&self.key, range)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}

	/// Read the object's metadata.
	pub async fn head(&self) -> Result<Option<ObjectMeta>> {
		self.store
			.head(&self.key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}

	/// Stream a body into the object.
	///
	/// The caller is responsible for erroring `data` past [`BucketGrant::max`]:
	/// the limit has to be enforced as the bytes arrive, and `put_streamed`
	/// treats a stream error as an abort, which is exactly the behaviour a
	/// breached limit needs.
	///
	/// `overwrite: false` on the grant, or `if_absent`, becomes a condition on
	/// the commit rather than a check the caller makes first: two redemptions
	/// of one write-once URL would both find the key free, and both would take
	/// it. `Ok(None)` means the key was already occupied and nothing was
	/// written.
	///
	/// `content_type` is checked against the grant and against the backend
	/// before `data` is polled, so a refused upload reads none of its body.
	pub async fn put(
		&self,
		data: BoxStream<'static, Result<Bytes, String>>,
		content_type: Option<String>,
		if_absent: bool,
	) -> Result<Option<PutResult>, BucketAccessError> {
		if let ContentTypeGrant::Exactly(allowed) = &self.grant.content_type
			&& *allowed != content_type
		{
			return Err(BucketAccessError::Denied(anyhow::Error::new(Error::PresignBadSignature)));
		}
		if content_type.is_some() && !self.store.supports_content_type() {
			return Err(BucketAccessError::UnsupportedMediaType(anyhow::Error::new(
				Error::ContentTypeUnsupported(self.bucket.name.to_string()),
			)));
		}
		let condition = if self.grant.overwrite && !if_absent {
			PutCondition::Overwrite
		} else {
			PutCondition::IfNotExists
		};
		let opts = PutOptions {
			condition,
			content_type,
		};
		match self.store.put_streamed(&self.key, data, opts).await {
			Ok(res) => Ok(Some(res)),
			Err(PutError::AlreadyExists) => Ok(None),
			Err(PutError::Failed(e)) => Err(BucketAccessError::Other(anyhow::Error::new(
				Error::ObjectStoreFailure(self.bucket.name.to_string(), e),
			))),
		}
	}

	/// Copy the object to the target this request was authorised for.
	pub async fn copy(&self, overwrite: bool) -> Result<TransferOutcome> {
		self.transfer(overwrite, false).await
	}

	/// Move the object to the target this request was authorised for.
	pub async fn rename(&self, overwrite: bool) -> Result<TransferOutcome> {
		self.transfer(overwrite, true).await
	}

	/// The source and target are probed first so a caller learns which of them
	/// stopped the request. The store's own `_if_not_exists` call still
	/// decides the commit, so a target taken between the probe and the commit
	/// is not replaced.
	async fn transfer(&self, overwrite: bool, remove_source: bool) -> Result<TransferOutcome> {
		let Some(target) = self.target.as_ref() else {
			bail!(Error::Presign("A copy or rename needs a target".to_string()));
		};
		let fail =
			|e| anyhow::Error::new(Error::ObjectStoreFailure(self.bucket.name.to_string(), e));
		if !self.store.exists(&self.key).await.map_err(fail)? {
			return Ok(TransferOutcome::SourceMissing);
		}
		if !overwrite && self.store.exists(target).await.map_err(fail)? {
			return Ok(TransferOutcome::TargetExists);
		}
		match (remove_source, overwrite) {
			(false, true) => self.store.copy(&self.key, target).await,
			(false, false) => self.store.copy_if_not_exists(&self.key, target).await,
			(true, true) => self.store.rename(&self.key, target).await,
			(true, false) => self.store.rename_if_not_exists(&self.key, target).await,
		}
		.map_err(fail)?;
		Ok(TransferOutcome::Done)
	}

	/// List the bucket's entries. The key this request was resolved with is
	/// not read: a listing is addressed by its options.
	pub async fn list(&self, opts: &ListOptions) -> Result<Vec<ListEntry>> {
		self.store
			.list(opts)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}

	/// Whether an object already exists at this key.
	pub async fn exists(&self) -> Result<bool> {
		self.store
			.exists(&self.key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}

	/// Remove the object.
	pub async fn delete(&self) -> Result<()> {
		self.store
			.delete(&self.key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}
}
