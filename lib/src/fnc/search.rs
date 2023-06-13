use crate::ctx::Context;
use crate::err::Error;
use crate::sql::Value;

pub async fn highlight(
	ctx: &Context<'_>,
	(prefix, suffix, match_ref): (Value, Value, Value),
) -> Result<Value, Error> {
	if let Some(doc) = ctx.doc() {
		if let Some(thg) = ctx.thing() {
			if let Some(exe) = ctx.query_executor() {
				let txn = ctx.clone_transaction()?;
				return exe.highlight(&txn, thg, prefix, suffix, match_ref.clone(), doc).await;
			}
		}
	}
	Ok(Value::None)
}
