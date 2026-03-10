use anyhow::{Context, Result};
use reblessive::tree::Stk;

use super::CursorDoc;
use super::args::Optional;
use crate::buc::BucketController;
use crate::buc::store::ObjectKey;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::val::{File, Object, Value};

/// Put a file into a bucket.
pub async fn put(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.put(&ObjectKey::new(file.key), value).await?;

	Ok(Value::None)
}

/// Put a file into a bucket if it does not exist.
pub async fn put_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.put_if_not_exists(&ObjectKey::new(file.key), value).await?;

	Ok(Value::None)
}

/// Get a file from a bucket.
pub async fn get(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let res = controller.get(&ObjectKey::new(file.key)).await?;
	Ok(res.map(Value::Bytes).unwrap_or_default())
}

/// Get the metadata of a file from a bucket.
///
/// Returns None if the file doesn't exist.
pub async fn head(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let res = controller.head(&ObjectKey::new(file.key)).await?;
	Ok(res.map(|v| v.into_value(file.bucket)).unwrap_or_default())
}

/// Delete a file.
pub async fn delete(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.delete(&ObjectKey::new(file.key)).await?;

	Ok(Value::None)
}

/// Copy a file.
///
/// Destination can be a string (relative path within the source bucket) or a file pointer which
/// allows you to copy to another bucket.
pub async fn copy(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(src, dst): (File, Value),
) -> Result<Value> {
	let dst = value_to_file(dst)?;
	if dst.bucket_matches_source(&src.bucket) {
		let mut controller = BucketController::new(stk, ctx, opt, doc, &src.bucket).await?;
		let src_key = ObjectKey::new(src.key);
		let dst_key = ObjectKey::new(dst.key);
		controller.copy(&src_key, dst_key).await?;
	} else {
		let data = {
			let mut src_controller = BucketController::new(stk, ctx, opt, doc, &src.bucket).await?;
			let src_key = ObjectKey::new(src.key);
			let Some(data) = src_controller.get(&src_key).await? else {
				return Err(anyhow::anyhow!("Source file does not exist"));
			};
			data
		};

		let DestinationFile {
			bucket,
			key,
		} = dst;
		let dst_bucket = bucket.context("destination bucket must be set")?;

		let dst_key = ObjectKey::new(key);
		let mut dst_controller = BucketController::new(stk, ctx, opt, doc, &dst_bucket).await?;
		dst_controller.put(&dst_key, data.into()).await?;
	}

	Ok(Value::None)
}

/// Copy a file to a destination path only if the destination path does not already exist.
///
/// Destination can be a string (relative path within the source bucket) or a file pointer which
/// allows you to copy to another bucket.
pub async fn copy_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(src, dst): (File, Value),
) -> Result<Value> {
	let dst = value_to_file(dst)?;

	if dst.bucket_matches_source(&src.bucket) {
		let mut controller = BucketController::new(stk, ctx, opt, doc, &src.bucket).await?;
		let src_key = ObjectKey::new(src.key);
		let dst_key = ObjectKey::new(dst.key);
		controller.copy_if_not_exists(&src_key, dst_key).await?;
	} else {
		let data = {
			let mut src_controller = BucketController::new(stk, ctx, opt, doc, &src.bucket).await?;
			let src_key = ObjectKey::new(src.key);
			let Some(data) = src_controller.get(&src_key).await? else {
				return Err(anyhow::anyhow!("Source file does not exist"));
			};
			data
		};

		let DestinationFile {
			bucket,
			key,
		} = dst;
		let dst_bucket = bucket.context("destination bucket must be set")?;

		let dst_key = ObjectKey::new(key);
		let mut dst_controller = BucketController::new(stk, ctx, opt, doc, &dst_bucket).await?;
		dst_controller.put_if_not_exists(&dst_key, data.into()).await?;
	}

	Ok(Value::None)
}

/// Rename a file within the same bucket.
pub async fn rename(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value> {
	let target = ObjectKey::new(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.rename(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

/// Rename a file within the same bucket if the destination path does not already exist.
pub async fn rename_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value> {
	let target = ObjectKey::new(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.rename_if_not_exists(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

/// Check if a file exists.
pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let exists = controller.exists(&ObjectKey::new(file.key)).await?;

	Ok(Value::Bool(exists))
}

/// List files in a bucket.
pub async fn list(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(bucket, Optional(opts)): (String, Optional<Object>),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &bucket).await?;
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
			key: s,
		}),
		_ => Err(anyhow::anyhow!("Invalid destination file value")),
	}
}
