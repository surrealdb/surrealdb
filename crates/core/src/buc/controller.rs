use crate::{
	ctx::{Context, MutableContext},
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	iam::Action,
	sql::{statements::define::BucketDefinition, Bytes, File, FlowResultExt, Permission, Value},
};
use core::fmt;
use reblessive::tree::Stk;
use std::sync::Arc;

use super::store::{ListOptions, ObjectKey, ObjectMeta, ObjectStore};

fn accept_payload(value: Value) -> Result<bytes::Bytes, Error> {
	value.cast_to::<Bytes>().map(|x| bytes::Bytes::from(x.0)).map_err(Error::from)
}

/// Allows you to control a specific bucket in the context of the current user
pub(crate) struct BucketController<'a> {
	stk: &'a mut Stk,
	ctx: &'a Context,
	opt: &'a Options,
	doc: Option<&'a CursorDoc>,

	bucket: Arc<BucketDefinition>,
	store: Arc<dyn ObjectStore>,
}

impl<'a> BucketController<'a> {
	/// Create a `FileController` for a specified file
	/// Will obtain a bucket connection and return back a `FileController` or `Error`
	pub(crate) async fn new(
		stk: &'a mut Stk,
		ctx: &'a Context,
		opt: &'a Options,
		doc: Option<&'a CursorDoc>,
		buc: &str,
	) -> Result<Self, Error> {
		let (ns, db) = opt.ns_db()?;
		let bucket = ctx.tx().get_db_bucket(ns, db, buc).await?;
		let store = ctx.get_bucket_store(ns, db, buc).await?;

		Ok(Self {
			stk,
			ctx,
			opt,
			doc,

			bucket,
			store,
		})
	}

	/// Checks if the bucket allows writes, and if not, return an `Error::ReadonlyBucket`
	fn require_writeable(&self) -> Result<(), Error> {
		if self.bucket.readonly {
			Err(Error::ReadonlyBucket(self.bucket.name.to_raw()))
		} else {
			Ok(())
		}
	}

	/// Attempt to put a file
	/// `Bytes` and `Strand` values are supported, and will be converted into `Bytes`
	/// Create or update permissions will be used, based on if the remote file already exists
	pub(crate) async fn put(&mut self, key: &ObjectKey, value: Value) -> Result<(), Error> {
		let payload = accept_payload(value)?;
		self.require_writeable()?;
		self.check_permission(BucketOperation::Put, Some(key), None).await?;

		self.store
			.put(key, payload)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	/// Attempt to put a file
	/// `Bytes` and `Strand` values are supported, and will be converted into `Bytes`
	/// Create or update permissions will be used, based on if the remote file already exists
	pub(crate) async fn put_if_not_exists(
		&mut self,
		key: &ObjectKey,
		value: Value,
	) -> Result<(), Error> {
		let payload = accept_payload(value)?;
		self.require_writeable()?;
		self.check_permission(BucketOperation::Put, Some(key), None).await?;

		self.store
			.put_if_not_exists(key, payload)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn head(&mut self, key: &ObjectKey) -> Result<Option<ObjectMeta>, Error> {
		self.check_permission(BucketOperation::Head, Some(key), None).await?;

		self.store
			.head(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))
	}

	pub(crate) async fn get(&mut self, key: &ObjectKey) -> Result<Option<Bytes>, Error> {
		self.check_permission(BucketOperation::Get, Some(key), None).await?;

		let bytes = match self
			.store
			.get(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?
		{
			Some(v) => v,
			None => return Ok(None),
		};

		Ok(Some(bytes.to_vec().into()))
	}

	pub(crate) async fn delete(&mut self, key: &ObjectKey) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Delete, Some(key), None).await?;

		self.store
			.delete(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn copy(&mut self, key: &ObjectKey, target: ObjectKey) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy, Some(key), Some(&target)).await?;

		self.store
			.copy(key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn copy_if_not_exists(
		&mut self,
		key: &ObjectKey,
		target: ObjectKey,
	) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy, Some(key), Some(&target)).await?;

		self.store
			.copy_if_not_exists(key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn rename(&mut self, key: &ObjectKey, target: ObjectKey) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename, Some(key), Some(&target)).await?;

		self.store
			.rename(key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn rename_if_not_exists(
		&mut self,
		key: &ObjectKey,
		target: ObjectKey,
	) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename, Some(key), Some(&target)).await?;

		self.store
			.rename_if_not_exists(key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn exists(&mut self, key: &ObjectKey) -> Result<bool, Error> {
		self.check_permission(BucketOperation::Exists, Some(key), None).await?;
		self.store
			.exists(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))
	}

	pub(crate) async fn list(&mut self, opts: &ListOptions) -> Result<Vec<ObjectMeta>, Error> {
		self.check_permission(BucketOperation::Exists, None, None).await?;
		self.store
			.list(opts)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))
	}

	pub(crate) async fn check_permission(
		&mut self,
		op: BucketOperation,
		key: Option<&ObjectKey>,
		target: Option<&ObjectKey>,
	) -> Result<(), Error> {
		if self.opt.check_perms(op.into())? {
			// Guest and Record users are not allowed to list files in buckets
			if op.is_list() {
				return Err(Error::BucketPermissions {
					name: self.bucket.name.to_raw(),
					op,
				});
			}

			match &self.bucket.permissions {
				Permission::None => {
					return Err(Error::BucketPermissions {
						name: self.bucket.name.to_raw(),
						op,
					})
				}
				Permission::Full => (),
				Permission::Specific(ref e) => {
					// Disable permissions
					let opt = &self.opt.new_with_perms(false);

					// Add $action, $file and $target to context
					let mut ctx = MutableContext::new(self.ctx);
					ctx.add_value("action", Value::from(op.to_string()).into());
					if let Some(key) = key {
						ctx.add_value(
							"file",
							Value::File(File {
								bucket: self.bucket.name.to_raw(),
								key: key.to_string(),
							})
							.into(),
						)
					}
					if let Some(target) = target {
						ctx.add_value(
							"target",
							Value::File(File {
								bucket: self.bucket.name.to_raw(),
								key: target.to_string(),
							})
							.into(),
						)
					}
					let ctx = ctx.freeze();

					// Process the PERMISSION clause
					if !e.compute(self.stk, &ctx, opt, self.doc).await.catch_return()?.is_truthy() {
						return Err(Error::BucketPermissions {
							name: self.bucket.name.to_raw(),
							op,
						});
					}
				}
			}
		}

		Ok(())
	}
}

#[derive(Clone, Copy, Debug)]
pub enum BucketOperation {
	Put,
	Get,
	Head,
	Delete,
	Copy,
	Rename,
	Exists,
	List,
}

impl BucketOperation {
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
			| BucketOperation::List => Action::View,

			// Action::Edit
			BucketOperation::Put
			| BucketOperation::Delete
			| BucketOperation::Copy
			| BucketOperation::Rename => Action::Edit,
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
		}
	}
}
