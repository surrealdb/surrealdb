use anyhow::{Context, Result};

use super::CursorDoc;
use super::args::Optional;
use crate::buc::BucketController;
use crate::buc::presign::{PresignOp, PresignOptions};
use crate::buc::store::{ObjectKey, put_content_type};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::val::{File, Object, Value};

/// Put a file into a bucket.
pub async fn put(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file, value, Optional(opts)): (File, Value, Optional<Object>),
) -> Result<Value> {
	let content_type = put_content_type("file::put", opts)?;
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	controller.put(&ObjectKey::new(file.key), value, content_type).await?;

	Ok(Value::None)
}

/// Put a file into a bucket if it does not exist.
pub async fn put_if_not_exists(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file, value, Optional(opts)): (File, Value, Optional<Object>),
) -> Result<Value> {
	let content_type = put_content_type("file::put_if_not_exists", opts)?;
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	controller.put_if_not_exists(&ObjectKey::new(file.key), value, content_type).await?;

	Ok(Value::None)
}

/// Get a file from a bucket.
pub async fn get(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	let res = controller.get(&ObjectKey::new(file.key)).await?;
	Ok(res.map(Value::Bytes).unwrap_or_default())
}

/// Get the metadata of a file from a bucket.
///
/// Returns None if the file doesn't exist.
pub async fn head(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	let res = controller.head(&ObjectKey::new(file.key)).await?;
	Ok(res.map(|v| v.into_value(file.bucket)).unwrap_or_default())
}

/// Delete a file.
pub async fn delete(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	controller.delete(&ObjectKey::new(file.key)).await?;

	Ok(Value::None)
}

/// Copy a file.
///
/// Destination can be a string (relative path within the source bucket) or a file pointer which
/// allows you to copy to another bucket.
pub async fn copy(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(src, dst): (File, Value),
) -> Result<Value> {
	let dst = value_to_file(dst)?;
	if dst.bucket_matches_source(&src.bucket) {
		let mut controller = BucketController::new(ctx, opt, doc, &src.bucket).await?;
		let src_key = ObjectKey::new(src.key);
		let dst_key = ObjectKey::new(dst.key);
		controller.copy(&src_key, dst_key).await?;
	} else {
		let (data, content_type) = {
			let mut src_controller = BucketController::new(ctx, opt, doc, &src.bucket).await?;
			let src_key = ObjectKey::new(src.key);
			let Some(data) = src_controller.get(&src_key).await? else {
				return Err(anyhow::anyhow!("Source file does not exist"));
			};
			// The source's content type travels with it. A destination that
			// cannot hold one refuses the copy rather than dropping the type.
			let content_type = src_controller.head(&src_key).await?.and_then(|m| m.content_type);
			(data, content_type)
		};

		let DestinationFile {
			bucket,
			key,
		} = dst;
		let dst_bucket = bucket.context("destination bucket must be set")?;

		let dst_key = ObjectKey::new(key);
		let mut dst_controller = BucketController::new(ctx, opt, doc, &dst_bucket).await?;
		dst_controller.put(&dst_key, data.into(), content_type).await?;
	}

	Ok(Value::None)
}

/// Copy a file to a destination path only if the destination path does not already exist.
///
/// Destination can be a string (relative path within the source bucket) or a file pointer which
/// allows you to copy to another bucket.
pub async fn copy_if_not_exists(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(src, dst): (File, Value),
) -> Result<Value> {
	let dst = value_to_file(dst)?;

	if dst.bucket_matches_source(&src.bucket) {
		let mut controller = BucketController::new(ctx, opt, doc, &src.bucket).await?;
		let src_key = ObjectKey::new(src.key);
		let dst_key = ObjectKey::new(dst.key);
		controller.copy_if_not_exists(&src_key, dst_key).await?;
	} else {
		let (data, content_type) = {
			let mut src_controller = BucketController::new(ctx, opt, doc, &src.bucket).await?;
			let src_key = ObjectKey::new(src.key);
			let Some(data) = src_controller.get(&src_key).await? else {
				return Err(anyhow::anyhow!("Source file does not exist"));
			};
			// The source's content type travels with it. A destination that
			// cannot hold one refuses the copy rather than dropping the type.
			let content_type = src_controller.head(&src_key).await?.and_then(|m| m.content_type);
			(data, content_type)
		};

		let DestinationFile {
			bucket,
			key,
		} = dst;
		let dst_bucket = bucket.context("destination bucket must be set")?;

		let dst_key = ObjectKey::new(key);
		let mut dst_controller = BucketController::new(ctx, opt, doc, &dst_bucket).await?;
		dst_controller.put_if_not_exists(&dst_key, data.into(), content_type).await?;
	}

	Ok(Value::None)
}

/// Rename a file within the same bucket.
pub async fn rename(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value> {
	let target = ObjectKey::new(target);
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	controller.rename(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

/// Rename a file within the same bucket if the destination path does not already exist.
pub async fn rename_if_not_exists(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value> {
	let target = ObjectKey::new(target);
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	controller.rename_if_not_exists(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

/// Check if a file exists.
pub async fn exists(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	let exists = controller.exists(&ObjectKey::new(file.key)).await?;

	Ok(Value::Bool(exists))
}

/// Mint a URL that downloads a file without a SurrealDB session.
///
/// The URL is a bearer credential: until it expires it works for whoever holds
/// it, it outlives this session, and a `PERMISSIONS` clause tightened afterwards
/// does not reach it.
pub async fn presign_get(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file, Optional(opts)): (File, Optional<Object>),
) -> Result<Value> {
	let opts = PresignOptions::from_object(PresignOp::Get, opts.unwrap_or_default())?;
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	let url = controller.presign_get(&ObjectKey::new(file.key), opts.expires).await?;

	Ok(url.into())
}

/// Mint a URL that uploads to a file without a SurrealDB session.
///
/// `max_size` is enforced by the node as the body streams in, not by the
/// signature — the signature only stops the value being edited. `overwrite:
/// false` makes the URL refuse to replace an object that already exists.
pub async fn presign_put(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(file, Optional(opts)): (File, Optional<Object>),
) -> Result<Value> {
	let opts = PresignOptions::from_object(PresignOp::Put, opts.unwrap_or_default())?;
	let mut controller = BucketController::new(ctx, opt, doc, &file.bucket).await?;
	let url = controller
		.presign_put(
			&ObjectKey::new(file.key),
			opts.expires,
			opts.max_size()?,
			opts.overwrite,
			opts.content_type()?,
		)
		.await?;

	Ok(url.into())
}

/// List files in a bucket.
pub async fn list(
	(ctx, opt, doc): (&FrozenContext, &Options, Option<&CursorDoc>),
	(bucket, Optional(opts)): (String, Optional<Object>),
) -> Result<Value> {
	let mut controller = BucketController::new(ctx, opt, doc, &bucket).await?;
	let opts = opts.map(|v| v.try_into()).transpose()?.unwrap_or_default();
	let res = controller
		.list(&opts)
		.await?
		.into_iter()
		.map(|v| v.into_value(bucket.clone()))
		.collect::<Vec<Value>>()
		.into();

	Ok(res)
}

pub fn bucket((file,): (File,)) -> Result<Value> {
	Ok(file.bucket.into())
}

pub fn key((file,): (File,)) -> Result<Value> {
	Ok(file.key.into())
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
			key: s.into_string(),
		}),
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Array(_)
		| Value::Set(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::Regex(_)
		| Value::Range(_)
		| Value::Closure(_) => Err(anyhow::anyhow!("Invalid destination file value")),
	}
}
