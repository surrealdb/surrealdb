use anyhow::Result;
use reblessive::tree::Stk;

use super::CursorDoc;
use super::args::Optional;
use crate::buc::BucketController;
use crate::buc::store::ObjectKey;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::val::{File, Object, Strand, Value};

pub async fn put(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.put(&ObjectKey::new(file.key), value).await?;

	Ok(Value::None)
}

pub async fn put_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.put_if_not_exists(&ObjectKey::new(file.key), value).await?;

	Ok(Value::None)
}

pub async fn get(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let res = controller.get(&ObjectKey::new(file.key)).await?;
	Ok(res.map(Value::Bytes).unwrap_or_default())
}

pub async fn head(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let res = controller.head(&ObjectKey::new(file.key)).await?;
	Ok(res.map(|v| v.into_value(file.bucket)).unwrap_or_default())
}

pub async fn delete(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.delete(&ObjectKey::new(file.key)).await?;

	Ok(Value::None)
}

pub async fn copy(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, Strand),
) -> Result<Value> {
	let target = ObjectKey::new(target.into_string());
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.copy(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

pub async fn copy_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, Strand),
) -> Result<Value> {
	let target = ObjectKey::new(target.into_string());
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.copy_if_not_exists(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

pub async fn rename(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, Strand),
) -> Result<Value> {
	let target = ObjectKey::new(target.into_string());
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.rename(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

pub async fn rename_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, Strand),
) -> Result<Value> {
	let target = ObjectKey::new(target.into_string());
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.rename_if_not_exists(&ObjectKey::new(file.key), target).await?;

	Ok(Value::None)
}

pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let exists = controller.exists(&ObjectKey::new(file.key)).await?;

	Ok(Value::Bool(exists))
}

pub async fn list(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
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
