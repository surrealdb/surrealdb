use crate::ctx::Context;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::sql::{Thing, Value};

pub async fn highlight(
	(_ctx, txn, exe, thg, doc): (
		&Context<'_>,
		&Transaction,
		Option<&'_ QueryExecutor>,
		Option<&'_ Thing>,
		Option<&'_ Value>,
	),
	(prefix, suffix, match_ref): (Value, Value, Value),
) -> Result<Value, Error> {
	if let Some(doc) = doc {
		if let Some(exe) = exe {
			return exe.highlight(txn, thg, prefix, suffix, match_ref.clone(), doc).await;
		}
	}
	Ok(Value::None)
}
