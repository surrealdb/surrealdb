use crate::ctx::Context;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::idx::planner::executor::QueryExecutor;
use crate::sql::{Thing, Value};

pub async fn highlight(
	(_ctx, txn, exe, thg): (
		&Context<'_>,
		&Transaction,
		Option<&'_ QueryExecutor>,
		Option<&'_ Thing>,
	),
	(prefix, suffix, field): (Value, Value, Value),
) -> Result<Value, Error> {
	if let Some(exe) = exe {
		exe.highlight(txn, thg, prefix, suffix, field).await
	} else {
		Ok(field)
	}
}
