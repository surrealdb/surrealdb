use core::fmt;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;

use super::store::{ListOptions, ObjectKey, ObjectMeta, ObjectStore};
use crate::catalog::{BucketDefinition, Permission};
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err;
use crate::expr::FlowResultExt;
use crate::iam::Action;
use crate::val::{Bytes, File, Value};

fn accept_payload(value: Value) -> Result<bytes::Bytes> {
	value
		.cast_to::<Bytes>()
		.map(|x| bytes::Bytes::from(x.0))
		.map_err(err::Error::from)
		.map_err(anyhow::Error::new)
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
	/// Will obtain a bucket connection and return back a `FileController` or
	/// `Error`
	pub(crate) async fn new(
		stk: &'a mut Stk,
		ctx: &'a Context,
		opt: &'a Options,
		doc: Option<&'a CursorDoc>,
		buc: &str,
	) -> Result<Self> {
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let bucket = ctx.tx().expect_db_bucket(ns, db, buc).await?;
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

	/// Checks if the bucket allows writes, and if not, return an
	/// `Error::ReadonlyBucket`
	fn require_writeable(&self) -> Result<()> {
		ensure!(!self.bucket.readonly, err::Error::ReadonlyBucket(self.bucket.name.clone()));
		Ok(())
	}

	/// Attempt to put a file
	/// `Bytes` and `Strand` values are supported, and will be converted into
	/// `Bytes` Create or update permissions will be used, based on if the
	/// remote file already exists
	pub(crate) async fn put(&mut self, key: &ObjectKey, value: Value) -> Result<()> {
		let payload = accept_payload(value)?;
		self.require_writeable()?;
		self.check_permission(BucketOperation::Put, Some(key), None).await?;

		self.store
			.put(key, payload)
			.await
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))?;

		Ok(())
	}

	/// Attempt to put a file
	/// `Bytes` and `Strand` values are supported, and will be converted into
	/// `Bytes` Create or update permissions will be used, based on if the
	/// remote file already exists
	pub(crate) async fn put_if_not_exists(&mut self, key: &ObjectKey, value: Value) -> Result<()> {
		let payload = accept_payload(value)?;
		self.require_writeable()?;
		self.check_permission(BucketOperation::Put, Some(key), None).await?;

		self.store
			.put_if_not_exists(key, payload)
			.await
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn head(&mut self, key: &ObjectKey) -> Result<Option<ObjectMeta>> {
		self.check_permission(BucketOperation::Head, Some(key), None).await?;

		self.store
			.head(key)
			.await
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))
			.map_err(anyhow::Error::new)
	}

	pub(crate) async fn get(&mut self, key: &ObjectKey) -> Result<Option<Bytes>> {
		self.check_permission(BucketOperation::Get, Some(key), None).await?;

		let bytes =
			match self.store.get(key).await.map_err(|e| {
				err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string())
			})? {
				Some(v) => v,
				None => return Ok(None),
			};

		Ok(Some(bytes.to_vec().into()))
	}

	pub(crate) async fn delete(&mut self, key: &ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Delete, Some(key), None).await?;

		self.store
			.delete(key)
			.await
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn copy(&mut self, key: &ObjectKey, target: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy, Some(key), Some(&target)).await?;

		self.store
			.copy(key, &target)
			.await
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))?;

		Ok(())
	}

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
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn rename(&mut self, key: &ObjectKey, target: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename, Some(key), Some(&target)).await?;

		self.store
			.rename(key, &target)
			.await
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))?;

		Ok(())
	}

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
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn exists(&mut self, key: &ObjectKey) -> Result<bool> {
		self.check_permission(BucketOperation::Exists, Some(key), None).await?;
		self.store
			.exists(key)
			.await
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))
			.map_err(anyhow::Error::new)
	}

	pub(crate) async fn list(&mut self, opts: &ListOptions) -> Result<Vec<ObjectMeta>> {
		self.check_permission(BucketOperation::Exists, None, None).await?;
		self.store
			.list(opts)
			.await
			.map_err(|e| err::Error::ObjectStoreFailure(self.bucket.name.clone(), e.to_string()))
			.map_err(anyhow::Error::new)
	}

	pub(crate) async fn check_permission(
		&mut self,
		op: BucketOperation,
		key: Option<&ObjectKey>,
		target: Option<&ObjectKey>,
	) -> Result<()> {
		if self.opt.check_perms(op.into())? {
			// Guest and Record users are not allowed to list files in buckets
			ensure!(
				!op.is_list(),
				err::Error::BucketPermissions {
					name: self.bucket.name.clone(),
					op,
				}
			);

			match &self.bucket.permissions {
				Permission::None => {
					bail!(err::Error::BucketPermissions {
						name: self.bucket.name.clone(),
						op,
					})
				}
				Permission::Full => (),
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &self.opt.new_with_perms(false);

					// Add $action, $file and $target to context
					let mut ctx = MutableContext::new(self.ctx);
					ctx.add_value("action", Value::from(op.to_string()).into());
					if let Some(key) = key {
						ctx.add_value(
							"file",
							Value::File(File {
								bucket: self.bucket.name.clone(),
								key: key.to_string(),
							})
							.into(),
						)
					}
					if let Some(target) = target {
						ctx.add_value(
							"target",
							Value::File(File {
								bucket: self.bucket.name.clone(),
								key: target.to_string(),
							})
							.into(),
						)
					}
					let ctx = ctx.freeze();

					// Process the PERMISSION clause
					let res = self
						.stk
						.run(|stk| e.compute(stk, &ctx, opt, self.doc))
						.await
						.catch_return()?;
					ensure!(
						res.is_truthy(),
						err::Error::BucketPermissions {
							name: self.bucket.name.clone(),
							op,
						}
					);
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
