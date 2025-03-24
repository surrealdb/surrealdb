use crate::{
	ctx::{Context, MutableContext},
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::{
		permission::PermissionKind, statements::define::BucketDefinition, Bytes, File, Permission,
		Value,
	},
};
use object_store::{path::Path, ObjectMeta, ObjectStore, PutPayload};
use reblessive::tree::Stk;
use std::sync::Arc;

/// Allows you to control a specific in the context of the current user
pub struct FileController<'a> {
	stk: &'a mut Stk,
	ctx: &'a Context,
	opt: &'a Options,
	doc: Option<&'a CursorDoc>,

	bucket: Arc<BucketDefinition>,
	store: Arc<dyn ObjectStore>,
	key: Path,
}

impl<'a> FileController<'a> {
	/// Create a `FileController` for a specified file
	/// Will obtain a bucket connection and return back a `FileController` or `Error`
	pub(crate) async fn from_file(
		stk: &'a mut Stk,
		ctx: &'a Context,
		opt: &'a Options,
		doc: Option<&'a CursorDoc>,
		file: &'a File,
	) -> Result<Self, Error> {
		let (ns, db) = opt.ns_db()?;
		let bucket = ctx.tx().get_db_bucket(ns, db, &file.bucket).await?;
		let store = ctx.get_bucket_store(ns, db, &file.bucket).await?;
		let key = file.get_path()?;

		Ok(Self {
			stk,
			ctx,
			opt,
			doc,

			bucket,
			store,
			key,
		})
	}

	/// Generate a `File` based on the current `FileController`
	pub(crate) fn to_file(&self) -> File {
		File::new(self.bucket.name.to_string(), self.key.to_string())
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
	pub(crate) async fn put(&mut self, value: Value) -> Result<(), Error> {
		let payload = match value {
			Value::Bytes(v) => PutPayload::from_bytes(v.0.into()),
			Value::Strand(v) => PutPayload::from_bytes(v.0.into_bytes().into()),
			from => {
				return Err(Error::ConvertTo {
					from,
					into: "bytes".into(),
				})
			}
		};

		self.require_writeable()?;

		let permission_kind = if self.exists_inner(None).await? {
			PermissionKind::Update
		} else {
			PermissionKind::Create
		};

		self.check_permission(permission_kind).await?;

		self.store
			.put(&self.key, payload)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn head(&mut self) -> Result<Option<ObjectMeta>, Error> {
		self.check_permission(PermissionKind::Select).await?;

		match self.store.head(&self.key).await {
			Ok(v) => Ok(Some(v)),
			Err(object_store::Error::NotFound {
				..
			}) => Ok(None),
			Err(e) => Err(Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string())),
		}
	}

	pub(crate) async fn get(&mut self) -> Result<Option<Bytes>, Error> {
		self.check_permission(PermissionKind::Select).await?;

		let payload = match self.store.get(&self.key).await {
			Ok(v) => v,
			Err(object_store::Error::NotFound {
				..
			}) => return Ok(None),
			Err(e) => {
				return Err(Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))
			}
		};

		let bytes = payload
			.bytes()
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(Some(bytes.to_vec().into()))
	}

	pub(crate) async fn delete(&mut self) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(PermissionKind::Delete).await?;

		self.store
			.delete(&self.key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn copy(&mut self, target: Path) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(PermissionKind::Select).await?;

		if self.exists_inner(Some(&target)).await? {
			self.check_permission(PermissionKind::Update).await?;
		} else {
			self.check_permission(PermissionKind::Create).await?;
		};

		self.store
			.copy(&self.key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn copy_if_not_exists(&mut self, target: Path) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(PermissionKind::Select).await?;
		self.check_permission(PermissionKind::Create).await?;

		self.store
			.copy_if_not_exists(&self.key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn rename(&mut self, target: Path) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(PermissionKind::Select).await?;

		if self.exists_inner(Some(&target)).await? {
			self.check_permission(PermissionKind::Update).await?;
		} else {
			self.check_permission(PermissionKind::Create).await?;
		};

		self.store
			.rename(&self.key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn rename_if_not_exists(&mut self, target: Path) -> Result<(), Error> {
		self.require_writeable()?;
		self.check_permission(PermissionKind::Select).await?;
		self.check_permission(PermissionKind::Create).await?;

		self.store
			.rename_if_not_exists(&self.key, &target)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string()))?;

		Ok(())
	}

	pub(crate) async fn exists(&mut self) -> Result<bool, Error> {
		self.check_permission(PermissionKind::Select).await?;
		self.exists_inner(None).await
	}

	pub(crate) async fn exists_inner(&self, key: Option<&Path>) -> Result<bool, Error> {
		match self.store.head(key.unwrap_or(&self.key)).await {
			Ok(_) => Ok(true),
			Err(object_store::Error::NotFound {
				..
			}) => Ok(false),
			Err(e) => Err(Error::ObjectStoreFailure(self.bucket.name.to_raw(), e.to_string())),
		}
	}

	pub(crate) async fn check_permission(&mut self, kind: PermissionKind) -> Result<(), Error> {
		if self.opt.check_perms(kind.into())? {
			match self.bucket.permissions.get_by_kind(kind) {
				Permission::None => {
					return Err(Error::BucketPermissions {
						name: self.bucket.name.to_raw(),
						kind,
					})
				}
				Permission::Full => (),
				Permission::Specific(e) => {
					// Disable permissions
					let opt = &self.opt.new_with_perms(false);
					// Add $file to context
					let mut ctx = MutableContext::new(self.ctx);
					ctx.add_value("file", Value::File(self.to_file()).into());
					let ctx = ctx.freeze();

					// Process the PERMISSION clause
					if !e.compute(self.stk, &ctx, opt, self.doc).await?.is_truthy() {
						return Err(Error::BucketPermissions {
							name: self.bucket.name.to_raw(),
							kind,
						});
					}
				}
			}
		}

		Ok(())
	}
}
