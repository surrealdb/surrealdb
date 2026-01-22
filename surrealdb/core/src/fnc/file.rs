use anyhow::Result;
use reblessive::tree::Stk;

use super::CursorDoc;
use super::args::Optional;
use crate::buc::BucketController;
use crate::buc::store::ObjectKey;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::val::{File, Object, Value};

pub async fn put(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.put(&ObjectKey::new(file.key), value).await?;

	Ok(Value::None)
}

pub async fn put_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.put_if_not_exists(&ObjectKey::new(file.key), value).await?;

	Ok(Value::None)
}

pub async fn get(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let res = controller.get(&ObjectKey::new(file.key)).await?;
	Ok(res.map(Value::Bytes).unwrap_or_default())
}

pub async fn head(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let res = controller.head(&ObjectKey::new(file.key)).await?;
	Ok(res.map(|v| v.into_value(file.bucket)).unwrap_or_default())
}

pub async fn delete(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.delete(&ObjectKey::new(file.key)).await?;

	Ok(Value::None)
}

pub async fn copy(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(src, dst): (File, File),
) -> Result<Value> {
	if src.bucket == dst.bucket || dst.bucket.is_empty() {
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

		let dst_key = ObjectKey::new(dst.key);
		let mut dst_controller = BucketController::new(stk, ctx, opt, doc, &dst.bucket).await?;
		dst_controller.put(&dst_key, data.into()).await?;
	}
	Ok(Value::None)
}

pub async fn copy_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value> {
	let target = ObjectKey::new(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.copy_if_not_exists(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

pub async fn rename(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value> {
	let target = ObjectKey::new(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.rename(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

pub async fn rename_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value> {
	let target = ObjectKey::new(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.rename_if_not_exists(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &FrozenContext, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let exists = controller.exists(&ObjectKey::new(file.key)).await?;

	Ok(Value::Bool(exists))
}

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
