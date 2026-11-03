use core::fmt;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};

use super::Error;
use super::presign::{BucketScope, PresignOp, PresignRequest, PresignedUrl, ScopeLevel};
use super::store::{
	ListEntry, ListOptions, ObjectKey, ObjectMeta, ObjectStore, PutCondition, put_bytes,
};
use crate::catalog::providers::BucketProvider;
use crate::catalog::{BucketDefinition, BucketLevel, Permission};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Error as ExprError;
use crate::iam::Action;
use crate::val::{Bytes, Duration, File, Value};

/// Converts a Value into raw bytes for storage.
///
/// Accepts `Bytes` or `String` values and converts them into `bytes::Bytes`.
fn accept_payload(value: Value) -> Result<bytes::Bytes> {
	value.cast_to::<Bytes>().map(|x| x.0).map_err(ExprError::from).map_err(anyhow::Error::new)
}

/// Allows you to control a specific bucket in the context of the current user
pub(crate) struct BucketController<'a> {
	ctx: &'a FrozenContext,
	opt: &'a Options,
	doc: Option<&'a CursorDoc>,

	bucket: Arc<BucketDefinition>,
	store: Arc<dyn ObjectStore>,
	level: BucketLevel,
}

impl<'a> BucketController<'a> {
	/// Creates a new `BucketController` for the specified bucket.
	///
	/// Obtains a bucket connection from the context and returns a controller
	/// that can be used to perform operations on the bucket.
	///
	/// # Arguments
	/// * `ctx` - The execution context
	/// * `opt` - The database options
	/// * `doc` - Optional cursor document for permission evaluation
	/// * `buc` - The name of the bucket to control
	///
	/// # Errors
	/// Returns an error if the bucket doesn't exist or connection fails.
	pub(crate) async fn new(
		ctx: &'a FrozenContext,
		opt: &'a Options,
		doc: Option<&'a CursorDoc>,
		buc: &str,
	) -> Result<Self> {
		let level = ctx.expect_bucket_level(opt).await?;
		let bucket = ctx.tx().expect_bucket(level, buc).await?;
		let store = ctx.get_bucket_store(level, buc).await?;

		Ok(Self {
			ctx,
			opt,
			doc,

			bucket,
			store,
			level,
		})
	}

	/// Build a controller from pieces a caller has already resolved.
	///
	/// The HTTP route resolves the bucket definition and store itself, because
	/// it needs the ids of the bucket's level to check a signature against
	/// before it knows whether a permission check is even required.
	pub(crate) fn from_parts(
		ctx: &'a FrozenContext,
		opt: &'a Options,
		bucket: Arc<BucketDefinition>,
		store: Arc<dyn ObjectStore>,
		level: BucketLevel,
	) -> Self {
		Self {
			ctx,
			opt,
			doc: None,
			bucket,
			store,
			level,
		}
	}

	/// Checks if the bucket and execution options allow writes.
	fn require_writeable(&self) -> Result<()> {
		if let Some(frame) = self.opt.no_write {
			bail!(frame.side_effect_error("file write"));
		}
		ensure!(!self.bucket.readonly, Error::ReadonlyBucket(self.bucket.name.to_string()));
		Ok(())
	}

	/// Attempt to put a file
	/// `Bytes` and `String` values are supported, and will be converted into
	/// `Bytes` Create or update permissions will be used, based on if the
	/// remote file already exists
	pub(crate) async fn put(
		&mut self,
		key: &ObjectKey,
		value: Value,
		content_type: Option<String>,
	) -> Result<()> {
		self.write(key, value, PutCondition::Overwrite, content_type).await
	}

	/// Stores data at the specified key only if the key does not already exist.
	///
	/// Unlike [`put`](Self::put), this method will not overwrite existing data.
	/// If the key already exists, the operation is a no-op.
	///
	/// `Bytes` and `String` values are supported and will be converted into bytes.
	pub(crate) async fn put_if_not_exists(
		&mut self,
		key: &ObjectKey,
		value: Value,
		content_type: Option<String>,
	) -> Result<()> {
		self.write(key, value, PutCondition::IfNotExists, content_type).await
	}

	async fn write(
		&mut self,
		key: &ObjectKey,
		value: Value,
		condition: PutCondition,
		content_type: Option<String>,
	) -> Result<()> {
		let payload = accept_payload(value)?;
		self.require_writeable()?;
		ensure!(
			content_type.is_none() || self.store.supports_content_type(),
			Error::ContentTypeUnsupported(self.bucket.name.to_string())
		);
		self.check_permission(BucketOperation::Put, Some(key), None).await?;

		put_bytes(self.store.as_ref(), key, payload, condition, content_type)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Retrieves metadata for an object without fetching its contents.
	///
	/// Returns `None` if the object does not exist.
	pub(crate) async fn head(&mut self, key: &ObjectKey) -> Result<Option<ObjectMeta>> {
		self.check_permission(BucketOperation::Head, Some(key), None).await?;

		self.store
			.head(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}

	/// Retrieves the contents of an object.
	///
	/// Returns `None` if the object does not exist.
	pub(crate) async fn get(&mut self, key: &ObjectKey) -> Result<Option<Bytes>> {
		self.check_permission(BucketOperation::Get, Some(key), None).await?;

		let bytes = match self
			.store
			.get(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))?
		{
			Some(v) => v,
			None => return Ok(None),
		};

		Ok(Some(Bytes(bytes)))
	}

	/// Deletes an object from the bucket.
	///
	/// This operation is idempotent - deleting a non-existent object is not an error.
	pub(crate) async fn delete(&mut self, key: &ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Delete, Some(key), None).await?;

		self.store
			.delete(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Copies an object to a new location within the bucket.
	///
	/// The source object remains unchanged. If the target already exists, it will be overwritten.
	pub(crate) async fn copy(&mut self, key: &ObjectKey, target: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy, Some(key), Some(&target)).await?;

		self.store
			.copy(key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Copies an object to a new location only if the target does not already exist.
	///
	/// The source object remains unchanged. If the target exists, this is a no-op.
	pub(crate) async fn copy_if_not_exists(
		&mut self,
		key: &ObjectKey,
		target: ObjectKey,
	) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy, Some(key), Some(&target)).await?;

		self.store
			.copy_if_not_exists(key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Moves an object to a new location within the bucket.
	///
	/// The source object is deleted after a successful copy. If the target already exists,
	/// it will be overwritten.
	pub(crate) async fn rename(&mut self, key: &ObjectKey, target: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename, Some(key), Some(&target)).await?;

		self.store
			.rename(key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Moves an object to a new location only if the target does not already exist.
	///
	/// The source object is deleted after a successful copy. If the target exists, this is a no-op.
	pub(crate) async fn rename_if_not_exists(
		&mut self,
		key: &ObjectKey,
		target: ObjectKey,
	) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename, Some(key), Some(&target)).await?;

		self.store
			.rename_if_not_exists(key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Checks whether an object exists in the bucket.
	pub(crate) async fn exists(&mut self, key: &ObjectKey) -> Result<bool> {
		self.check_permission(BucketOperation::Exists, Some(key), None).await?;
		self.store
			.exists(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}

	/// Lists objects in the bucket according to the provided options.
	///
	/// The listing can be filtered by prefix and paginated using start key and limit.
	/// Note: Guest and Record users are not allowed to list files in buckets.
	pub(crate) async fn list(&mut self, opts: &ListOptions) -> Result<Vec<ListEntry>> {
		self.check_permission(BucketOperation::List, None, None).await?;
		self.store
			.list(opts)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}

	/// Mint a URL that downloads this object without a session.
	pub(crate) async fn presign_get(
		&mut self,
		key: &ObjectKey,
		expires: std::time::Duration,
	) -> Result<PresignedUrl> {
		self.presign(
			key,
			PresignRequest {
				op: PresignOp::Get,
				expires,
				max: None,
				overwrite: true,
				content_type: None,
			},
		)
		.await
	}

	/// Mint a URL that uploads to this object without a session.
	pub(crate) async fn presign_put(
		&mut self,
		key: &ObjectKey,
		expires: std::time::Duration,
		max: Option<u64>,
		overwrite: bool,
		content_type: Option<String>,
	) -> Result<PresignedUrl> {
		self.require_writeable()?;
		// Refused at mint time: a URL whose upload can never succeed is not
		// worth handing out.
		ensure!(
			content_type.is_none() || self.store.supports_content_type(),
			Error::ContentTypeUnsupported(self.bucket.name.to_string())
		);
		let max = self.resolve_max_size(max)?;
		self.presign(
			key,
			PresignRequest {
				op: PresignOp::Put,
				expires,
				max: Some(max),
				overwrite,
				content_type,
			},
		)
		.await
	}

	/// The byte ceiling this upload URL carries.
	///
	/// A caller that names none gets the node's, rather than no limit at all:
	/// `$max_size` has to be a number for a `PERMISSIONS` clause to bound it,
	/// and the route has to have something to enforce while the body streams
	/// in. A caller that names more than the node allows is refused, so the
	/// value in the URL is never larger than what the node will accept.
	fn resolve_max_size(&self, requested: Option<u64>) -> Result<u64> {
		let allowed = self.ctx.get_buckets().map(|b| b.presign_max_size()).unwrap_or_default();
		match requested {
			Some(max) => {
				ensure!(
					max <= allowed,
					Error::PresignMaxSizeTooLarge {
						requested: max,
						allowed,
					}
				);
				Ok(max)
			}
			None => Ok(allowed),
		}
	}

	async fn presign(&mut self, key: &ObjectKey, req: PresignRequest) -> Result<PresignedUrl> {
		let op = match req.op {
			PresignOp::Get => BucketOperation::PresignGet,
			PresignOp::Put => BucketOperation::PresignPut,
		};

		// The node's ceiling is checked before the bucket's clause, so a caller
		// asking for more than the deployment allows is told so plainly rather
		// than getting a permission error that names the wrong cause.
		let allowed = self
			.ctx
			.get_buckets()
			.map(|b| b.presign_max_ttl())
			.unwrap_or(std::time::Duration::ZERO);
		ensure!(
			req.expires <= allowed,
			Error::PresignTtlTooLong {
				requested: req.expires,
				allowed,
			}
		);

		self.check_permission_with(op, Some(key), None, Some(&req)).await?;

		let secret = self
			.ctx
			.get_buckets()
			.and_then(|b| b.presign_secret())
			.cloned()
			.ok_or(Error::PresignNoSecret)?;
		let level = ScopeLevel::of(self.level, self.opt)?;
		let origin = self.ctx.get_buckets().and_then(|b| b.public_origin()).map(str::to_owned);

		super::presign::issue(
			&secret,
			origin.as_deref(),
			BucketScope {
				level,
				bucket: self.bucket.name.as_str(),
				nonce: self.bucket.presign_nonce,
			},
			key,
			req,
			chrono::Utc::now().timestamp(),
		)
		.map_err(|e| Error::Presign(e.to_string()))
		.map_err(anyhow::Error::new)
	}

	/// Checks if the current user has permission to perform the specified operation.
	///
	/// This method evaluates the bucket's permission policy against the current user
	/// and operation. For operations involving files, the `$action`, `$file`, and
	/// optionally `$target` variables are made available to the permission expression.
	///
	/// # Arguments
	/// * `op` - The bucket operation being performed
	/// * `key` - The source object key (if applicable)
	/// * `target` - The target object key for copy/rename operations (if applicable)
	pub(crate) async fn check_permission(
		&mut self,
		op: BucketOperation,
		key: Option<&ObjectKey>,
		target: Option<&ObjectKey>,
	) -> Result<()> {
		self.check_permission_with(op, key, target, None).await
	}

	/// As [`check_permission`](Self::check_permission), additionally binding
	/// `$expires` (for `presign_get` and `presign_put`) and `$max_size` and
	/// `$overwrite` (for `presign_put` only) when `presign` is set.
	///
	/// Each variable is bound only for the actions it applies to, in the same
	/// way `$target` exists only for `copy` and `rename`: the action names which
	/// variables are in scope, so a clause branched on `$action` can rely on
	/// them being there rather than testing for them.
	pub(crate) async fn check_permission_with(
		&mut self,
		op: BucketOperation,
		key: Option<&ObjectKey>,
		target: Option<&ObjectKey>,
		presign: Option<&PresignRequest>,
	) -> Result<()> {
		if self.ctx.check_bucket_perms(self.opt, op.into(), self.level)? {
			// Guest and Record users are not allowed to list files in buckets,
			// and reach no bucket above a database: its clause admits only the
			// principals of the level it is defined at.
			ensure!(
				!op.is_list() && matches!(self.level, BucketLevel::Db(..)),
				Error::BucketPermissions {
					name: self.bucket.name.to_string(),
					op,
				}
			);

			match &self.bucket.permissions {
				Permission::None => {
					bail!(Error::BucketPermissions {
						name: self.bucket.name.to_string(),
						op,
					})
				}
				Permission::Full => (),
				Permission::Specific(e) => {
					// Disable permission recursion and block side effects
					let opt = &self.opt.new_for_permission_predicate();

					// Add $action, $file and $target to context
					let mut ctx = Context::new_child(self.ctx);
					ctx.add_value("action", Value::from(op.to_string()).into());
					if let Some(key) = key {
						ctx.add_value(
							"file",
							Value::File(File {
								bucket: self.bucket.name.to_string(),
								key: key.to_string(),
							})
							.into(),
						)
					}
					if let Some(target) = target {
						ctx.add_value(
							"target",
							Value::File(File {
								bucket: self.bucket.name.to_string(),
								key: target.to_string(),
							})
							.into(),
						)
					}
					if let Some(presign) = presign {
						ctx.add_value("expires", Value::Duration(Duration(presign.expires)).into());
						if matches!(op, BucketOperation::PresignPut) {
							// Always a number, never NONE. A clause reading
							// `$max_size <= n` must refuse an upload with no
							// limit rather than pass it, and `NONE <= n` is
							// truthy.
							ctx.add_value(
								"max_size",
								Value::from(presign.max.unwrap_or_default()).into(),
							);
							ctx.add_value("overwrite", Value::Bool(presign.overwrite).into());
							ctx.add_value(
								"content_type",
								presign
									.content_type
									.clone()
									.map(Value::from)
									.unwrap_or(Value::None)
									.into(),
							);
						}
					}
					let ctx = ctx.freeze();

					// Process the PERMISSION clause
					let exec = crate::exec::entry::frame(&ctx, opt, self.doc).await;
					let res = crate::exec::entry::scalar(&exec, e).await?;
					ensure!(
						res.is_truthy(),
						Error::BucketPermissions {
							name: self.bucket.name.to_string(),
							op,
						}
					);
				}
			}
		}

		Ok(())
	}
}

/// Represents the different operations that can be performed on a bucket.
///
/// These operations are used for permission checking and action logging.
#[derive(Clone, Copy, Debug)]
pub enum BucketOperation {
	/// Store data at a key
	Put,
	/// Retrieve data from a key
	Get,
	/// Retrieve metadata for a key
	Head,
	/// Remove data at a key
	Delete,
	/// Copy data from one key to another
	Copy,
	/// Move data from one key to another
	Rename,
	/// Check if a key exists
	Exists,
	/// List objects in the bucket
	List,
	/// Mint a URL that downloads a key without a session
	PresignGet,
	/// Mint a URL that uploads to a key without a session
	PresignPut,
}

impl BucketOperation {
	/// Returns `true` if this is a list operation.
	///
	/// List operations have special permission restrictions - guest and record
	/// users are not allowed to list files in buckets.
	pub fn is_list(self) -> bool {
		matches!(self, Self::List)
	}
}

impl From<BucketOperation> for Action {
	fn from(val: BucketOperation) -> Self {
		match val {
			// Action::View
			BucketOperation::Get
			| BucketOperation::Head
			| BucketOperation::Exists
			| BucketOperation::List
			| BucketOperation::PresignGet => Action::View,

			// Action::Edit
			BucketOperation::Put
			| BucketOperation::Delete
			| BucketOperation::Copy
			| BucketOperation::Rename
			| BucketOperation::PresignPut => Action::Edit,
		}
	}
}

impl fmt::Display for BucketOperation {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Put => write!(f, "put"),
			Self::Get => write!(f, "get"),
			Self::Head => write!(f, "head"),
			Self::Delete => write!(f, "delete"),
			Self::Copy => write!(f, "copy"),
			Self::Rename => write!(f, "rename"),
			Self::Exists => write!(f, "exists"),
			Self::List => write!(f, "list"),
			Self::PresignGet => write!(f, "presign_get"),
			Self::PresignPut => write!(f, "presign_put"),
		}
	}
}
