//! File functions for the engine.
//!
//! These provide file storage functionality for working with buckets.
//!
//! File writes are not transactional. `file::put`, `file::delete`, `file::copy`,
//! `file::rename` and their `_if_not_exists` variants act on the bucket's object
//! store when they run, and a `CANCEL`, a failed statement or a failed commit
//! does not undo them, while the records written beside them roll back. A
//! transaction that must keep records and bucket objects consistent writes the
//! object first and commits the records that name it after, so a failure leaves
//! at worst an unreferenced object.

use std::sync::Arc;

use anyhow::{Result, bail, ensure};

use crate::buc::presign::{
	self, BucketScope, PresignOp, PresignOptions, PresignRequest, ScopeLevel,
};
use crate::buc::store::{
	ListOptions, ObjectKey, ObjectStore, PutCondition, put_bytes, put_content_type,
};
use crate::buc::{BucketOperation, Error as BucError};
use crate::catalog::providers::BucketProvider;
use crate::catalog::{BucketDefinition, LogicalPermissionBody};
use crate::err::EngineError;
use crate::exec::function::ScalarFunction;
use crate::exec::{EvalContext, ExecutionContext};
use crate::expr::Error as ExprError;
use crate::expr::logical::LogicalPermission;
use crate::fnc::args::{FromArgs, Optional};
use crate::val::{Bytes, File, Object, Value};
use crate::{define_async_function, define_pure_function};

// =========================================================================
// Helper types and functions
// =========================================================================

/// Converts a Value into raw bytes for storage.
///
/// Accepts `Bytes` or `String` values and converts them into `bytes::Bytes`.
fn accept_payload(value: Value) -> Result<bytes::Bytes> {
	value.cast_to::<Bytes>().map(|x| x.0).map_err(ExprError::from).map_err(anyhow::Error::new)
}

/// Helper struct for bucket operations without needing the full BucketController.
struct StreamingBucketOps<'a> {
	bucket: Arc<BucketDefinition>,
	level: crate::catalog::BucketLevel,
	store: Arc<dyn ObjectStore>,
	frozen_ctx: &'a crate::ctx::FrozenContext,
	exec_ctx: &'a ExecutionContext,
	opt: &'a crate::dbs::Options,
	skip_fetch_perms: bool,
}

impl<'a> StreamingBucketOps<'a> {
	/// Creates a new StreamingBucketOps for the specified bucket.
	async fn new(ctx: &'a EvalContext<'_>, bucket_name: &str) -> Result<Self> {
		// Get the FrozenContext and the Options for this call.
		let frozen_ctx = ctx.exec_ctx.ctx();
		let opt = ctx.exec_ctx.options().ok_or_else(|| {
			EngineError::Internal("No options available for file operation".to_string())
		})?;

		// The level of the session's buckets: its database, its namespace or the root
		let level = frozen_ctx.expect_bucket_level(opt).await?;

		// Get bucket definition and store
		let txn = ctx.txn();
		let (bucket, store) = crate::exec::vm::descriptor::object(async {
			let bucket = txn.expect_bucket(level, bucket_name).await?;
			let store = frozen_ctx.get_bucket_store(level, bucket_name).await?;
			anyhow::Ok((bucket, store))
		})
		.await?;

		Ok(Self {
			bucket,
			level,
			store,
			frozen_ctx,
			exec_ctx: ctx.exec_ctx,
			opt,
			skip_fetch_perms: ctx.skip_fetch_perms,
		})
	}

	/// Checks if the bucket and execution context allow writes.
	fn require_writeable(&self) -> Result<()> {
		if let Some(frame) = self.exec_ctx.root().no_write.or(self.opt.no_write) {
			bail!(frame.side_effect_error("file write"));
		}
		ensure!(!self.bucket.readonly, BucError::ReadonlyBucket(self.bucket.name.to_string()));
		Ok(())
	}

	/// Mint a URL that downloads this object without a session.
	async fn presign_get(&self, key: &ObjectKey, opts: PresignOptions) -> Result<Value> {
		self.presign(
			key,
			PresignRequest {
				op: PresignOp::Get,
				expires: opts.expires,
				max: None,
				overwrite: true,
				content_type: None,
			},
		)
		.await
	}

	/// Mint a URL that uploads to this object without a session.
	async fn presign_put(&self, key: &ObjectKey, opts: PresignOptions) -> Result<Value> {
		self.require_writeable()?;
		let content_type = opts.content_type()?;
		// Refused at mint time: a URL whose upload can never succeed is not
		// worth handing out.
		ensure!(
			content_type.is_none() || self.store.supports_content_type(),
			BucError::ContentTypeUnsupported(self.bucket.name.to_string())
		);
		let max = self.resolve_max_size(opts.max_size()?)?;
		self.presign(
			key,
			PresignRequest {
				op: PresignOp::Put,
				expires: opts.expires,
				max: Some(max),
				overwrite: opts.overwrite,
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
		let allowed =
			self.frozen_ctx.get_buckets().map(|b| b.presign_max_size()).unwrap_or_default();
		match requested {
			Some(max) => {
				ensure!(
					max <= allowed,
					BucError::PresignMaxSizeTooLarge {
						requested: max,
						allowed,
					}
				);
				Ok(max)
			}
			None => Ok(allowed),
		}
	}

	async fn presign(&self, key: &ObjectKey, req: PresignRequest) -> Result<Value> {
		let op = match req.op {
			PresignOp::Get => BucketOperation::PresignGet,
			PresignOp::Put => BucketOperation::PresignPut,
		};

		// The node's ceiling is checked before the bucket's clause, so a caller
		// asking for more than the deployment allows is told so plainly rather
		// than getting a permission error that names the wrong cause.
		let allowed = self
			.frozen_ctx
			.get_buckets()
			.map(|b| b.presign_max_ttl())
			.unwrap_or(std::time::Duration::ZERO);
		ensure!(
			req.expires <= allowed,
			BucError::PresignTtlTooLong {
				requested: req.expires,
				allowed,
			}
		);

		self.check_permission_with(op, Some(key), None, Some(&req)).await?;

		let secret = self
			.frozen_ctx
			.get_buckets()
			.and_then(|b| b.presign_secret())
			.cloned()
			.ok_or(BucError::PresignNoSecret)?;
		let level = ScopeLevel::of(self.level, self.opt)?;
		let origin =
			self.frozen_ctx.get_buckets().and_then(|b| b.public_origin()).map(str::to_owned);

		let url = presign::issue(
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
		.map_err(|e| BucError::Presign(e.to_string()))?;

		Ok(url.into())
	}

	/// Check permissions for an operation.
	async fn check_permission(
		&self,
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
	/// variables are in scope.
	async fn check_permission_with(
		&self,
		op: BucketOperation,
		key: Option<&ObjectKey>,
		target: Option<&ObjectKey>,
		presign: Option<&PresignRequest>,
	) -> Result<()> {
		// Permissions are checked only when the execution context and options require it.
		if !self.skip_fetch_perms
			&& self.exec_ctx.should_check_perms(op.into())?
			&& self.frozen_ctx.check_bucket_perms(self.opt, op.into(), self.level)?
		{
			// Guest and Record users are not allowed to list files in buckets,
			// and reach no bucket above a database: its clause admits only the
			// principals of the level it is defined at.
			ensure!(
				!op.is_list() && matches!(self.level, crate::catalog::BucketLevel::Db(..)),
				BucError::BucketPermissions {
					name: self.bucket.name.to_string(),
					op,
				}
			);

			// The bucket permission is compiled from its stored text,
			// the same way as every other permission clause.
			let permission = LogicalPermissionBody::of(&self.bucket.permissions)
				.logical()
				.map_err(|e| anyhow::anyhow!("bucket permission has no logical form: {e:#}"))?;
			match permission {
				LogicalPermission::None => {
					bail!(BucError::BucketPermissions {
						name: self.bucket.name.to_string(),
						op,
					})
				}
				LogicalPermission::Full => (),
				LogicalPermission::Specific(e) => {
					// The clause is served from the compile cache, keyed by the clause as written.
					// The names bound around it (`action`, `file`, `target`, `expires`, `max_size`,
					// `overwrite`, `content_type`) are read when the check runs, so one compilation
					// serves every operation and file ([`crate::exec::vm::serve`]).
					// It is compiled without a principal, since the clause is judged with
					// permissions off. A principal would bind guards over reads that the frame
					// is meant to waive.
					let expr = crate::exec::vm::serve::expr_binding(
						self.exec_ctx,
						e,
						None,
						0,
						[
							"action",
							"file",
							"target",
							"expires",
							"max_size",
							"overwrite",
							"content_type",
						]
						.map(str::to_owned)
						.to_vec(),
					)
					.await?;
					let mut exec_ctx =
						self.exec_ctx.with_params([("action", Value::from(op.to_string()))]);
					if let Some(key) = key {
						exec_ctx = exec_ctx.with_params([(
							"file",
							Value::File(File {
								bucket: self.bucket.name.to_string(),
								key: key.to_string(),
							}),
						)]);
					}
					if let Some(target) = target {
						exec_ctx = exec_ctx.with_params([(
							"target",
							Value::File(File {
								bucket: self.bucket.name.to_string(),
								key: target.to_string(),
							}),
						)]);
					}

					if let Some(presign) = presign {
						exec_ctx = exec_ctx.with_params([(
							"expires",
							Value::Duration(crate::val::Duration(presign.expires)),
						)]);
						if matches!(op, BucketOperation::PresignPut) {
							// Always a number, never NONE. A clause reading
							// `$max_size <= n` must refuse an upload with no
							// limit rather than pass it, and `NONE <= n` is
							// truthy.
							exec_ctx = exec_ctx.with_params([
								("max_size", Value::from(presign.max.unwrap_or_default())),
								("overwrite", Value::Bool(presign.overwrite)),
								// NONE when the URL binds no type: such a URL
								// stores none, which is the type a clause
								// restricting types should let through least.
								(
									"content_type",
									presign
										.content_type
										.clone()
										.map(Value::from)
										.unwrap_or(Value::None),
								),
							]);
						}
					}

					// SECURITY: the bucket predicate runs with the caller's auth. Nested permission
					// checks are disabled, as `Options::new_with_perms(false)` does, so the
					// predicate does not recurse into permission evaluation. The predicate is
					// judged with enforcement off, so a write issued from it is refused, as it
					// is for a table's `select` guard.
					let exec_ctx = exec_ctx
						.dive()?
						.with_skip_fetch_perms(true)
						.refusing_writes(crate::dbs::NoWriteFrame::PermissionPredicate);
					let eval_ctx = EvalContext::from_exec_ctx(&exec_ctx);
					let res = expr
						.evaluate(eval_ctx)
						.await
						.map_err(|e| EngineError::Internal(e.to_string()))?;

					ensure!(
						res.is_truthy(),
						BucError::BucketPermissions {
							name: self.bucket.name.to_string(),
							op,
						}
					);
				}
			}
		}

		Ok(())
	}

	/// Put a file into the bucket.
	async fn put(&self, key: &ObjectKey, value: Value, content_type: Option<String>) -> Result<()> {
		self.write(key, value, PutCondition::Overwrite, content_type).await
	}

	/// Put a file into the bucket if it doesn't exist.
	async fn put_if_not_exists(
		&self,
		key: &ObjectKey,
		value: Value,
		content_type: Option<String>,
	) -> Result<()> {
		self.write(key, value, PutCondition::IfNotExists, content_type).await
	}

	async fn write(
		&self,
		key: &ObjectKey,
		value: Value,
		condition: PutCondition,
		content_type: Option<String>,
	) -> Result<()> {
		let payload = accept_payload(value)?;
		self.require_writeable()?;
		ensure!(
			content_type.is_none() || self.store.supports_content_type(),
			BucError::ContentTypeUnsupported(self.bucket.name.to_string())
		);
		self.check_permission(BucketOperation::Put, Some(key), None).await?;

		put_bytes(self.store.as_ref(), key, payload, condition, content_type)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// The content type stored with an object, for a caller that has already
	/// been allowed to read it.
	async fn content_type(&self, key: &ObjectKey) -> Result<Option<String>> {
		let meta = self
			.store
			.head(key)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;
		Ok(meta.and_then(|m| m.content_type))
	}

	/// Get a file from the bucket.
	async fn get(&self, key: &ObjectKey) -> Result<Option<Bytes>> {
		self.check_permission(BucketOperation::Get, Some(key), None).await?;

		let bytes = match self
			.store
			.get(key)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?
		{
			Some(v) => v,
			None => return Ok(None),
		};

		Ok(Some(Bytes(bytes)))
	}

	/// Get file metadata from the bucket.
	async fn head(&self, key: &ObjectKey) -> Result<Option<Value>> {
		self.check_permission(BucketOperation::Head, Some(key), None).await?;

		let meta = self
			.store
			.head(key)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(meta.map(|m| m.into_value(self.bucket.name.to_string())))
	}

	/// Delete a file from the bucket.
	async fn delete(&self, key: &ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Delete, Some(key), None).await?;

		self.store
			.delete(key)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Copy a file within the bucket.
	async fn copy(&self, src: &ObjectKey, dst: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy, Some(src), Some(&dst)).await?;

		self.store
			.copy(src, &dst)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Copy a file if destination doesn't exist.
	async fn copy_if_not_exists(&self, src: &ObjectKey, dst: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Copy, Some(src), Some(&dst)).await?;

		self.store
			.copy_if_not_exists(src, &dst)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Rename a file within the bucket.
	async fn rename(&self, src: &ObjectKey, dst: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename, Some(src), Some(&dst)).await?;

		self.store
			.rename(src, &dst)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Rename a file if destination doesn't exist.
	async fn rename_if_not_exists(&self, src: &ObjectKey, dst: ObjectKey) -> Result<()> {
		self.require_writeable()?;
		self.check_permission(BucketOperation::Rename, Some(src), Some(&dst)).await?;

		self.store
			.rename_if_not_exists(src, &dst)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(())
	}

	/// Check if a file exists.
	async fn exists(&self, key: &ObjectKey) -> Result<bool> {
		self.check_permission(BucketOperation::Exists, Some(key), None).await?;

		self.store
			.exists(key)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))
			.map_err(anyhow::Error::new)
	}

	/// List files in the bucket.
	async fn list(&self, opts: &ListOptions) -> Result<Vec<Value>> {
		self.check_permission(BucketOperation::List, None, None).await?;

		let items = self
			.store
			.list(opts)
			.await
			.map_err(|e| BucError::ObjectStoreFailure(self.bucket.name.to_string(), e))?;

		Ok(items.into_iter().map(|m| m.into_value(self.bucket.name.to_string())).collect())
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
	if let Value::File(file) = value {
		Ok(DestinationFile {
			bucket: Some(file.bucket),
			key: file.key,
		})
	} else if let Value::String(s) = value {
		Ok(DestinationFile {
			bucket: None,
			key: s.into_string(),
		})
	} else {
		Err(anyhow::anyhow!("Invalid destination file value"))
	}
}

// =========================================================================
// file::put
// =========================================================================

async fn file_put_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file, value, Optional(opts)): (File, Value, Optional<Object>) =
		FromArgs::from_args("file::put", args)?;
	let content_type = put_content_type("file::put", opts)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.put(&ObjectKey::new(file.key), value, content_type).await?;
	Ok(Value::None)
}

// =========================================================================
// file::put_if_not_exists
// =========================================================================

async fn file_put_if_not_exists_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file, value, Optional(opts)): (File, Value, Optional<Object>) =
		FromArgs::from_args("file::put_if_not_exists", args)?;
	let content_type = put_content_type("file::put_if_not_exists", opts)?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.put_if_not_exists(&ObjectKey::new(file.key), value, content_type).await?;
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
// file::presign_get / file::presign_put
// =========================================================================

async fn file_presign_get_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file, Optional(opts)): (File, Optional<Object>) =
		FromArgs::from_args("file::presign_get", args)?;
	let opts = PresignOptions::from_object(PresignOp::Get, opts.unwrap_or_default())?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.presign_get(&ObjectKey::new(file.key), opts).await
}

async fn file_presign_put_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	let (file, Optional(opts)): (File, Optional<Object>) =
		FromArgs::from_args("file::presign_put", args)?;
	let opts = PresignOptions::from_object(PresignOp::Put, opts.unwrap_or_default())?;
	let ops = StreamingBucketOps::new(ctx, &file.bucket).await?;
	ops.presign_put(&ObjectKey::new(file.key), opts).await
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
		// The source's content type travels with it. A destination that
		// cannot hold one refuses the copy rather than dropping the type.
		let (data, content_type) = {
			let src_ops = StreamingBucketOps::new(ctx, &src.bucket).await?;
			let src_key = ObjectKey::new(src.key);
			let Some(data) = src_ops.get(&src_key).await? else {
				return Err(anyhow::anyhow!("Source file does not exist"));
			};
			(data, src_ops.content_type(&src_key).await?)
		};

		let dst_bucket =
			dst.bucket.ok_or_else(|| anyhow::anyhow!("destination bucket must be set"))?;
		let dst_ops = StreamingBucketOps::new(ctx, &dst_bucket).await?;
		dst_ops.put(&ObjectKey::new(dst.key), data.into(), content_type).await?;
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
		// The source's content type travels with it. A destination that
		// cannot hold one refuses the copy rather than dropping the type.
		let (data, content_type) = {
			let src_ops = StreamingBucketOps::new(ctx, &src.bucket).await?;
			let src_key = ObjectKey::new(src.key);
			let Some(data) = src_ops.get(&src_key).await? else {
				return Err(anyhow::anyhow!("Source file does not exist"));
			};
			(data, src_ops.content_type(&src_key).await?)
		};

		let dst_bucket =
			dst.bucket.ok_or_else(|| anyhow::anyhow!("destination bucket must be set"))?;
		let dst_ops = StreamingBucketOps::new(ctx, &dst_bucket).await?;
		dst_ops.put_if_not_exists(&ObjectKey::new(dst.key), data.into(), content_type).await?;
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
define_async_function!(FilePut, "file::put", effects: EffectSet::WRITES_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any, value: Any, ?opts: Object) -> Any, file_put_impl);
define_async_function!(FilePutIfNotExists, "file::put_if_not_exists", effects: EffectSet::WRITES_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any, value: Any, ?opts: Object) -> Any, file_put_if_not_exists_impl);
define_async_function!(FileGet, "file::get", effects: EffectSet::READS_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any) -> Any, file_get_impl);
define_async_function!(FileHead, "file::head", effects: EffectSet::READS_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any) -> Any, file_head_impl);
define_async_function!(FileDelete, "file::delete", effects: EffectSet::WRITES_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any) -> Any, file_delete_impl);
define_async_function!(FileCopy, "file::copy", effects: EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL), fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (src: Any, dst: Any) -> Any, file_copy_impl);
define_async_function!(FileCopyIfNotExists, "file::copy_if_not_exists", effects: EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL), fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (src: Any, dst: Any) -> Any, file_copy_if_not_exists_impl);
define_async_function!(FileRename, "file::rename", effects: EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL), fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any, target: String) -> Any, file_rename_impl);
define_async_function!(FileRenameIfNotExists, "file::rename_if_not_exists", effects: EffectSet::READS_EXTERNAL.union(EffectSet::WRITES_EXTERNAL), fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any, target: String) -> Any, file_rename_if_not_exists_impl);
define_async_function!(FileExists, "file::exists", effects: EffectSet::READS_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any) -> Any, file_exists_impl);
define_async_function!(FileList, "file::list", effects: EffectSet::READS_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (bucket: String, ?opts: Object) -> Any, file_list_impl);
define_async_function!(FilePresignGet, "file::presign_get", effects: EffectSet::READS_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any, ?opts: Object) -> Any, file_presign_get_impl);
define_async_function!(FilePresignPut, "file::presign_put", effects: EffectSet::READS_EXTERNAL, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (file: Any, ?opts: Object) -> Any, file_presign_put_impl);

define_pure_function!(FileBucket, "file::bucket", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (file: Any) -> Any, file_bucket_impl);
define_pure_function!(FileKey, "file::key", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (file: Any) -> Any, file_key_impl);

// =========================================================================
// Registration
// =========================================================================

// A bucket lives outside the transaction's snapshot, so every function that reaches into
// one declares `READS_EXTERNAL`, `WRITES_EXTERNAL` or both. A write has an effect that a
// rule may not repeat or drop, and a grouped projection may not run it once per group.
// The bucket keeps what a write leaves behind, so the number of effects must not depend
// on how a query grouped its rows. A read can answer differently on either side of a
// write. `file::presign_get` and `file::presign_put` read the bucket's signing identity
// and do not write an object. `file::bucket` and `file::key` only read two fields of the
// file value they are given, so they are pure.
/// The `file::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&FilePut,
	&FilePutIfNotExists,
	&FileGet,
	&FileHead,
	&FileDelete,
	&FileCopy,
	&FileCopyIfNotExists,
	&FileRename,
	&FileRenameIfNotExists,
	&FileExists,
	&FileList,
	&FilePresignGet,
	&FilePresignPut,
	&FileBucket,
	&FileKey,
];
