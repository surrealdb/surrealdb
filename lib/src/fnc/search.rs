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
	(prefix, suffix, match_ref): (Value, Value, Value),
) -> Result<Value, Error> {
	if let Some(exe) = exe {
		exe.highlight(txn, thg, prefix, suffix, match_ref.clone(), match_ref).await
	} else {
		Ok(match_ref)
	}
}
