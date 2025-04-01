use reblessive::tree::Stk;

use crate::{
	buc::{store::Path, FileController},
	ctx::Context,
	dbs::Options,
	err::Error,
	sql::{File, Value},
};

use super::CursorDoc;

pub async fn put(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value, Error> {
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	controller.put(value).await?;

	Ok(Value::None)
}

pub async fn get(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	let res = controller.get().await?;
	Ok(res.map(Value::Bytes).unwrap_or_default())
}

pub async fn head(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	let res = controller.head().await?;
	Ok(res.map(Into::into).unwrap_or_default())
}

pub async fn delete(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	controller.delete().await?;

	Ok(Value::None)
}

pub async fn copy(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value, Error> {
	let target = Path::from(target);
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	controller.copy(target).await?;

	Ok(Value::None)
}

pub async fn copy_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value, Error> {
	let target = Path::from(target);
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	controller.copy_if_not_exists(target).await?;

	Ok(Value::None)
}

pub async fn rename(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value, Error> {
	let target = Path::from(target);
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	controller.rename(target).await?;

	Ok(Value::None)
}

pub async fn rename_if_not_exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, target): (File, String),
) -> Result<Value, Error> {
	let target = Path::from(target);
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	controller.rename_if_not_exists(target).await?;

	Ok(Value::None)
}

pub async fn exists(
	(stk, ctx, opt, doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let mut controller = FileController::from_file(stk, ctx, opt, doc, &file).await?;
	let exists = controller.exists().await?;

	Ok(Value::Bool(exists))
}
