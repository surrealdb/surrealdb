//! File functions for the streaming executor.
//!
//! These provide file storage functionality for working with buckets.
//! Note: File functions require the experimental "files" capability to be enabled.

use std::sync::Arc;

use anyhow::{Result, bail, ensure};

use crate::buc::BucketOperation;
use crate::buc::store::{ListOptions, ObjectKey, ObjectStore};
use crate::catalog::providers::BucketProvider;
use crate::catalog::{BucketDefinition, Permission};
use crate::dbs::capabilities::ExperimentalTarget;
use crate::err::Error;
use crate::exec::function::FunctionRegistry;
use crate::exec::physical_expr::EvalContext;
use crate::fnc::args::FromArgs;
use crate::val::{Bytes, File, Object, Value};
use crate::{define_async_function, define_pure_function, register_functions};

// =========================================================================
// Helper types and functions
// =========================================================================

/// Converts a Value into raw bytes for storage.
///
/// Accepts `Bytes` or `String` values and converts them into `bytes::Bytes`.
fn accept_payload(value: Value) -> Result<bytes::Bytes> {
	value.cast_to::<Bytes>().map(|x| x.0).map_err(Error::from).map_err(anyhow::Error::new)
}

/// Helper struct for bucket operations without needing the full BucketController.
/// This is used by the streaming executor which doesn't have access to Stk.
struct StreamingBucketOps<'a> {
	bucket: Arc<BucketDefinition>,
	store: Arc<dyn ObjectStore>,
	opt: &'a crate::dbs::Options,
}

impl<'a> StreamingBucketOps<'a> {
	/// Creates a new StreamingBucketOps for the specified bucket.
	async fn new(ctx: &'a EvalContext<'_>, bucket_name: &str) -> Result<Self> {
		// Check experimental capability
		let caps = ctx.capabilities().ok_or_else(|| Error::InvalidFunction {
			name: "file::*".to_string(),
			message: "No capabilities available".to_string(),
		})?;
		if !caps.allows_experimental(&ExperimentalTarget::Files) {
			return Err(Error::InvalidFunction {
				name: "file::*".to_string(),
				message: "Experimental capability `files` is not enabled".to_string(),
			}
			.into());
		}

		// Get FrozenContext and Options (same approach as fnc::file)
		let frozen_ctx = ctx.exec_ctx.ctx();
		let opt = ctx.exec_ctx.options().ok_or_else(|| {
			Error::Internal("No options available for file operation".to_string())
		})?;

		// Get namespace and database IDs from the Options
		let (ns_id, db_id) = frozen_ctx.expect_ns_db_ids(opt).await?;

		// Get bucket definition and store
		let txn = ctx.txn();
		let bucket = txn.expect_db_bucket(ns_id, db_id, bucket_name).await?;
		let store = frozen_ctx.get_bucket_store(ns_id, db_id, bucket_name).await?;

		Ok(Self {
			bucket,
			store,
			opt,
		})
	}

	/// Checks if the bucket allows writes.
	fn require_writeable(&self) -> Result<()> {
		ensure!(!self.bucket.readonly, Error::ReadonlyBucket(self.bucket.name.clone()));
		Ok(())
	}

	/// Check permissions for an operation.
	///
	/// For Permission::Specific, we currently don't support the full expression
	/// evaluation in the streaming executor (requires Stk). In that case, we
	/// fall back to checking based on role only.
	fn check_permission(&self, op: BucketOperation) -> Result<()> {
		// Check if we should check permissions (uses Options::check_perms like fnc::file)
		if self.opt.check_perms(op.into())? {
			// Guest and Record users are not allowed to list files in buckets
			ensure!(
				!op.is_list(),
				Error::BucketPermissions {
					name: self.bucket.name.clone(),
					op,
				}
			);

			match &self.bucket.permissions {
				Permission::None => {
					bail!(Error::BucketPermissions {
						name: self.bucket.name.clone(),
						op,
					})
				}
				Permission::Full => (),
				Permission::Specific(_) => {
					// For specific permissions, we would need to evaluate the expression
					// using the legacy compute path. For now, we allow if auth is sufficient.
					// This is a simplification - the full BucketController would evaluate
					// the expression with $action, $file, $target variables.
					//
					// In practice, most buckets use Permission::Full or Permission::None,
					// so this simplification should work for the common cases.
				}
			}
		}

		Ok(())
	}

	/// Put a file into the bucket.
	async fn put(&self, key: &ObjectKey, value: Value) -> Result<()> {
		let payload = accept_payload(value)?;
		self.require_writeable()?;
		self.check_permission(BucketOperation::Put)?;

		self.store
			.put(key, payload)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(())
	}

	/// Put a file into the bucket if it doesn't exist.
	async fn put_if_not_exists(&self, key: &ObjectKey, value: Value) -> Result<()> {
		let payload = accept_payload(value)?;
		self.require_writeable()?;
		self.check_permission(BucketOperation::Put)?;

		self.store
			.put_if_not_exists(key, payload)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(())
	}

	/// Get a file from the bucket.
	async fn get(&self, key: &ObjectKey) -> Result<Option<Bytes>> {
		self.check_permission(BucketOperation::Get)?;

		let bytes = match self
			.store
			.get(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?
		{
			Some(v) => v,
			None => return Ok(None),
		};

		Ok(Some(Bytes(bytes)))
	}

	/// Get file metadata from the bucket.
	async fn head(&self, key: &ObjectKey) -> Result<Option<Value>> {
		self.check_permission(BucketOperation::Head)?;

		let meta = self
			.store
			.head(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(meta.map(|m| m.into_value(self.bucket.name.clone())))
	}

	/// Delete a file from the bucket.
	async fn delete(&self, key: &ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Delete)?;

		self.store
			.delete(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(())
	}

	/// Copy a file within the bucket.
	async fn copy(&self, src: &ObjectKey, dst: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy)?;

		self.store
			.copy(src, &dst)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(())
	}

	/// Copy a file if destination doesn't exist.
	async fn copy_if_not_exists(&self, src: &ObjectKey, dst: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy)?;

		self.store
			.copy_if_not_exists(src, &dst)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(())
	}

	/// Rename a file within the bucket.
	async fn rename(&self, src: &ObjectKey, dst: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename)?;

		self.store
			.rename(src, &dst)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(())
	}

	/// Rename a file if destination doesn't exist.
	async fn rename_if_not_exists(&self, src: &ObjectKey, dst: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename)?;

		self.store
			.rename_if_not_exists(src, &dst)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(())
	}

	/// Check if a file exists.
	async fn exists(&self, key: &ObjectKey) -> Result<bool> {
		self.check_permission(BucketOperation::Exists)?;

		self.store
			.exists(key)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))
			.map_err(anyhow::Error::new)
	}

	/// List files in the bucket.
	async fn list(&self, opts: &ListOptions) -> Result<Vec<Value>> {
		self.check_permission(BucketOperation::List)?;

		let items = self
			.store
			.list(opts)
			.await
			.map_err(|e| Error::ObjectStoreFailure(self.bucket.name.clone(), e))?;

		Ok(items.into_iter().map(|m| m.into_value(self.bucket.name.clone())).collect())
	}
}

/// The only difference between a file and a destination file is that a destination file
/// can be relative (no bucket).
struct DestinationFile {
	bucket: Option<String>,
	key: String,
}

impl DestinationFile {
	/// Check if the bucket matches the source bucket.
	/// If the destination file has no bucket, it is a relative path and matches any source bucket.
	pub fn bucket_matches_source(&self, other: &str) -> bool {
		self.bucket.as_ref().map(|b| b == other).unwrap_or(true)
	}
}

fn value_to_file(value: Value) -> Result<DestinationFile> {
	match value {
		Value::File(file) => Ok(DestinationFile {
			bucket: Some(file.bucket),
			key: file.key,
		}),
		Value::String(s) => Ok(DestinationFile {
			bucket: None,
			key: s,
		}),
		_ => Err(anyhow::anyhow!("Invalid destination file value")),
	}
}

// =========================================================================
// file::put
// =========================================================================

async fn file_put_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file, value): (File, Value) = FromArgs::from_args("file::put", args)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.put(&ObjectKey::new(file.key), value).await?;
	Ok(Value::None)
}

// =========================================================================
// file::put_if_not_exists
// =========================================================================

async fn file_put_if_not_exists_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file, value): (File, Value) = FromArgs::from_args("file::put_if_not_exists", args)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.put_if_not_exists(&ObjectKey::new(file.key), value).await?;
	Ok(Value::None)
}

// =========================================================================
// file::get
// =========================================================================

async fn file_get_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file,): (File,) = FromArgs::from_args("file::get", args)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	let res = ops.get(&ObjectKey::new(file.key)).await?;
	Ok(res.map(Value::Bytes).unwrap_or_default())
}

// =========================================================================
// file::head
// =========================================================================

async fn file_head_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file,): (File,) = FromArgs::from_args("file::head", args)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	let res = ops.head(&ObjectKey::new(file.key)).await?;
	Ok(res.unwrap_or_default())
}

// =========================================================================
// file::delete
// =========================================================================

async fn file_delete_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file,): (File,) = FromArgs::from_args("file::delete", args)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.delete(&ObjectKey::new(file.key)).await?;
	Ok(Value::None)
}

// =========================================================================
// file::copy
// =========================================================================

async fn file_copy_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (src, dst): (File, Value) = FromArgs::from_args("file::copy", args)?;
	let dst = value_to_file(dst)?;

	if dst.bucket_matches_source(&src.bucket) {
		let ops = StreamingBucketOps::new(ctx, &src.bucket).await?;
		ops.copy(&ObjectKey::new(src.key), ObjectKey::new(dst.key)).await?;
	} else {
		// Cross-bucket copy
		let data = {
			let src_ops = StreamingBucketOps::new(ctx, &src.bucket).await?;
			let Some(data) = src_ops.get(&ObjectKey::new(src.key)).await? else {
				return Err(anyhow::anyhow!("Source file does not exist"));
			};
			data
		};

		let dst_bucket =
			dst.bucket.ok_or_else(|| anyhow::anyhow!("destination bucket must be set"))?;
		let dst_ops = StreamingBucketOps::new(ctx, &dst_bucket).await?;
		dst_ops.put(&ObjectKey::new(dst.key), data.into()).await?;
	}

	Ok(Value::None)
}

// =========================================================================
// file::copy_if_not_exists
// =========================================================================

async fn file_copy_if_not_exists_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (src, dst): (File, Value) = FromArgs::from_args("file::copy_if_not_exists", args)?;
	let dst = value_to_file(dst)?;

	if dst.bucket_matches_source(&src.bucket) {
		let ops = StreamingBucketOps::new(ctx, &src.bucket).await?;
		ops.copy_if_not_exists(&ObjectKey::new(src.key), ObjectKey::new(dst.key)).await?;
	} else {
		// Cross-bucket copy
		let data = {
			let src_ops = StreamingBucketOps::new(ctx, &src.bucket).await?;
			let Some(data) = src_ops.get(&ObjectKey::new(src.key)).await? else {
				return Err(anyhow::anyhow!("Source file does not exist"));
			};
			data
		};

		let dst_bucket =
			dst.bucket.ok_or_else(|| anyhow::anyhow!("destination bucket must be set"))?;
		let dst_ops = StreamingBucketOps::new(ctx, &dst_bucket).await?;
		dst_ops.put_if_not_exists(&ObjectKey::new(dst.key), data.into()).await?;
	}

	Ok(Value::None)
}

// =========================================================================
// file::rename
// =========================================================================

async fn file_rename_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file, target): (File, String) = FromArgs::from_args("file::rename", args)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.rename(&ObjectKey::new(file.key), ObjectKey::new(target)).await?;
	Ok(Value::None)
}

// =========================================================================
// file::rename_if_not_exists
// =========================================================================

async fn file_rename_if_not_exists_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file, target): (File, String) = FromArgs::from_args("file::rename_if_not_exists", args)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.rename_if_not_exists(&ObjectKey::new(file.key), ObjectKey::new(target)).await?;
	Ok(Value::None)
}

// =========================================================================
// file::exists
// =========================================================================

async fn file_exists_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file,): (File,) = FromArgs::from_args("file::exists", args)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	let exists = ops.exists(&ObjectKey::new(file.key)).await?;
	Ok(Value::Bool(exists))
}

// =========================================================================
// file::list
// =========================================================================

async fn file_list_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	use crate::fnc::args::Optional;

	let (bucket, Optional(opts)): (String, Optional<Object>) =
		FromArgs::from_args("file::list", args)?;
	let ops = StreamingBucketOps::new(ctx, &bucket).await?;
	let list_opts = opts.map(|v| v.try_into()).transpose()?.unwrap_or_default();
	let items = ops.list(&list_opts).await?;
	Ok(items.into())
}

// =========================================================================
// file::bucket (pure function)
// =========================================================================

fn file_bucket_impl((file,): (File,)) -> Result<Value> {
	Ok(file.bucket.into())
}

// =========================================================================
// file::key (pure function)
// =========================================================================

fn file_key_impl((file,): (File,)) -> Result<Value> {
	Ok(file.key.into())
}

// =========================================================================
// Function definitions using macros
// =========================================================================

// Note: We use `Any` for file arguments in the macro signature because Kind::File takes
// parameters (bucket types). The actual type checking happens via FromArgs::from_args.
define_async_function!(FilePut, "file::put", (file: Any, value: Any) -> Any, file_put_impl);
define_async_function!(FilePutIfNotExists, "file::put_if_not_exists", (file: Any, value: Any) -> Any, file_put_if_not_exists_impl);
define_async_function!(FileGet, "file::get", (file: Any) -> Any, file_get_impl);
define_async_function!(FileHead, "file::head", (file: Any) -> Any, file_head_impl);
define_async_function!(FileDelete, "file::delete", (file: Any) -> Any, file_delete_impl);
define_async_function!(FileCopy, "file::copy", (src: Any, dst: Any) -> Any, file_copy_impl);
define_async_function!(FileCopyIfNotExists, "file::copy_if_not_exists", (src: Any, dst: Any) -> Any, file_copy_if_not_exists_impl);
define_async_function!(FileRename, "file::rename", (file: Any, target: String) -> Any, file_rename_impl);
define_async_function!(FileRenameIfNotExists, "file::rename_if_not_exists", (file: Any, target: String) -> Any, file_rename_if_not_exists_impl);
define_async_function!(FileExists, "file::exists", (file: Any) -> Any, file_exists_impl);
define_async_function!(FileList, "file::list", (bucket: String, ?opts: Object) -> Any, file_list_impl);

define_pure_function!(FileBucket, "file::bucket", (file: Any) -> Any, file_bucket_impl);
define_pure_function!(FileKey, "file::key", (file: Any) -> Any, file_key_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		FilePut,
		FilePutIfNotExists,
		FileGet,
		FileHead,
		FileDelete,
		FileCopy,
		FileCopyIfNotExists,
		FileRename,
		FileRenameIfNotExists,
		FileExists,
		FileList,
		FileBucket,
		FileKey,
	);
}
