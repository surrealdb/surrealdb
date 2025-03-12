use object_store::{path::Path, PutPayload};
use reblessive::tree::Stk;

use crate::{
	ctx::Context,
	dbs::Options,
	err::Error,
	sql::{File, Value},
};

use super::CursorDoc;

pub async fn put(
	(_stk, ctx, opt, _doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file, value): (File, Value),
) -> Result<Value, Error> {
	let (ns, db) = opt.ns_db()?;
	let store = ctx.get_bucket_store(ns, db, &file.bucket).await?;

	let key = Path::parse(file.key).map_err(|_| Error::Unreachable("Invalid path".into()))?;
	let payload = match value {
		Value::Bytes(v) => PutPayload::from_bytes(v.0.into()),
		// Value::Strand(v) => v.0.as_bytes(),
		_ => return Err(Error::Unreachable("Invalid value passed".into())),
	};

	store.put(&key, payload).await.map_err(|_| Error::Unreachable("Failed to put file".into()))?;

	Ok(Value::None)
}

pub async fn get(
	(_stk, ctx, opt, _doc): (&mut Stk, &Context, &Options, Option<&CursorDoc>),
	(file,): (File,),
) -> Result<Value, Error> {
	let (ns, db) = opt.ns_db()?;
	let store = ctx.get_bucket_store(ns, db, &file.bucket).await?;

	let key = Path::parse(file.key).map_err(|_| Error::Unreachable("Invalid path".into()))?;
	let payload =
		store.get(&key).await.map_err(|_| Error::Unreachable("Failed to get file".into()))?;

	let bytes =
		payload.bytes().await.map_err(|_| Error::Unreachable("Failed to get bytes".into()))?;

	Ok(Value::Bytes(bytes.to_vec().into()))
}
