use crate::ctx::Context;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::Value;

pub async fn score(
	(ctx, txn, doc): (&Context<'_>, Option<&Transaction>, Option<&CursorDoc<'_>>),
	(match_ref,): (Value,),
) -> Result<Value, Error> {
	if let Some(txn) = txn {
		if let Some(doc) = doc {
			if let Some(thg) = doc.rid {
				if let Some(exe) = ctx.get_query_executor(&thg.tb) {
					return exe.score(txn, &match_ref, thg, doc.doc_id).await;
				}
			}
		}
	}
	Ok(Value::None)
}

pub async fn highlight(
	(ctx, txn, doc): (&Context<'_>, Option<&Transaction>, Option<&CursorDoc<'_>>),
	(prefix, suffix, match_ref): (Value, Value, Value),
) -> Result<Value, Error> {
	if let Some(txn) = txn {
		if let Some(doc) = doc {
			if let Some(thg) = doc.rid {
				if let Some(exe) = ctx.get_query_executor(&thg.tb) {
					return exe
						.highlight(txn, thg, prefix, suffix, &match_ref, doc.doc.as_ref())
						.await;
				}
			}
		}
	}
	Ok(Value::None)
}

pub async fn offsets(
	(ctx, txn, doc): (&Context<'_>, Option<&Transaction>, Option<&CursorDoc<'_>>),
	(match_ref,): (Value,),
) -> Result<Value, Error> {
	if let Some(txn) = txn {
		if let Some(doc) = doc {
			if let Some(thg) = doc.rid {
				if let Some(exe) = ctx.get_query_executor(&thg.tb) {
					return exe.offsets(txn, thg, &match_ref).await;
				}
			}
		}
	}
	Ok(Value::None)
}
