use reblessive::tree::Stk;

use crate::{
	buc::{store::ObjectKey, BucketController},
	ctx::Context,
	dbs::Options,
	err::Error,
	sql::{File, Object, Value},
};

use super::CursorDoc;

pub async fn put(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value, Error> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.put(&file.key.into(), value).await?;

	Ok(Value::None)
}

pub async fn put_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value, Error> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.put_if_not_exists(&file.key.into(), value).await?;

	Ok(Value::None)
}

pub async fn get(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let res = controller.get(&file.key.into()).await?;
	Ok(res.map(Value::Bytes).unwrap_or_default())
}

pub async fn head(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let res = controller.head(&file.key.into()).await?;
	Ok(res.map(|v| v.into_value(file.bucket)).unwrap_or_default())
}

pub async fn delete(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.delete(&file.key.into()).await?;

	Ok(Value::None)
}

pub async fn copy(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value, Error> {
	let target = ObjectKey::from(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.copy(&file.key.into(), target).await?;

	Ok(Value::None)
}

pub async fn copy_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value, Error> {
	let target = ObjectKey::from(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.copy_if_not_exists(&file.key.into(), target).await?;

	Ok(Value::None)
}

pub async fn rename(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value, Error> {
	let target = ObjectKey::from(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.rename(&file.key.into(), target).await?;

	Ok(Value::None)
}

pub async fn rename_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value, Error> {
	let target = ObjectKey::from(target);
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	controller.rename_if_not_exists(&file.key.into(), target).await?;

	Ok(Value::None)
}

pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let mut controller = BucketController::new(stk, ctx, opt, doc, &file.bucket).await?;
	let exists = controller.exists(&file.key.into()).await?;

	Ok(Value::Bool(exists))
}

pub async fn list(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(bucket, opts): (String, Option<Object>),
) -> Result<Value, Error> {
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

pub fn bucket((file,): (File,)) -> Result<Value, Error> {
	Ok(file.bucket.into())
}

pub fn key((file,): (File,)) -> Result<Value, Error> {
	Ok(file.key.into())
}
