use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc::get_execution_context;
use crate::idx::ft::analyzer::Analyzer;
use crate::sql::Value;
use reblessive::tree::Stk;

pub async fn analyze(
	(stk, ctx, txn, opt): (&mut Stk, &Context<'_>, Option<&Transaction>, Option<&Options>),
	(az, val): (Value, Value),
) -> Result<Value, Error> {
	if let (Some(txn), Some(opt), Value::Strand(az), Value::Strand(val)) = (txn, opt, az, val) {
		let az: Analyzer =
			txn.lock().await.get_db_analyzer(opt.ns(), opt.db(), az.as_str()).await?.into();
		az.analyze(stk, ctx, opt, txn, val.0).await
	} else {
		Ok(Value::None)
	}
}

pub async fn score(
	(ctx, txn, doc): (&Context<'_>, Option<&Transaction>, Option<&CursorDoc<'_>>),
	(match_ref,): (Value,),
) -> Result<Value, Error> {
	if let Some((txn, exe, doc, thg)) = get_execution_context(ctx, txn, doc) {
		exe.score(txn, &match_ref, thg, doc.doc_id).await
	} else {
		Ok(Value::None)
	}
}

pub async fn highlight(
	(ctx, txn, doc): (&Context<'_>, Option<&Transaction>, Option<&CursorDoc<'_>>),
	(prefix, suffix, match_ref, partial): (Value, Value, Value, Option<Value>),
) -> Result<Value, Error> {
	if let Some((txn, exe, doc, thg)) = get_execution_context(ctx, txn, doc) {
		let partial = partial.map(|p| p.convert_to_bool()).unwrap_or(Ok(false))?;
		exe.highlight(txn, thg, prefix, suffix, match_ref, partial, doc.doc.as_ref()).await
	} else {
		Ok(Value::None)
	}
}

pub async fn offsets(
	(ctx, txn, doc): (&Context<'_>, Option<&Transaction>, Option<&CursorDoc<'_>>),
	(match_ref, partial): (Value, Option<Value>),
) -> Result<Value, Error> {
	if let Some((txn, exe, _, thg)) = get_execution_context(ctx, txn, doc) {
		let partial = partial.map(|p| p.convert_to_bool()).unwrap_or(Ok(false))?;
		exe.offsets(txn, thg, match_ref, partial).await
	} else {
		Ok(Value::None)
	}
}
