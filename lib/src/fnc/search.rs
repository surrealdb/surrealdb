use crate::ctx::Context;
use crate::err::Error;
use crate::sql::Value;

pub async fn score(ctx: &Context<'_>, (match_ref,): (Value,)) -> Result<Value, Error> {
	if let Some(thg) = ctx.thing() {
		if let Some(exe) = ctx.get_query_executor(&thg.tb) {
			let txn = ctx.try_clone_transaction()?;
			return exe.score(txn, &match_ref, thg, ctx.doc_id()).await;
		}
	}
	Ok(Value::None)
}

pub async fn highlight(
	ctx: &Context<'_>,
	(prefix, suffix, match_ref): (Value, Value, Value),
) -> Result<Value, Error> {
	if let Some(doc) = ctx.doc() {
		if let Some(thg) = ctx.thing() {
			if let Some(exe) = ctx.get_query_executor(&thg.tb) {
				let txn = ctx.try_clone_transaction()?;
				return exe.highlight(txn, thg, prefix, suffix, &match_ref, doc).await;
			}
		}
	}
	Ok(Value::None)
}

pub async fn offsets(ctx: &Context<'_>, (match_ref,): (Value,)) -> Result<Value, Error> {
	if let Some(thg) = ctx.thing() {
		if let Some(exe) = ctx.get_query_executor(&thg.tb) {
			let txn = ctx.try_clone_transaction()?;
			return exe.offsets(txn, thg, &match_ref).await;
		}
	}
	Ok(Value::None)
}
