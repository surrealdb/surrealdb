use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::fnc::get_execution_context;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::highlighter::HighlightParams;
use crate::sql::Value;
use reblessive::tree::Stk;

pub async fn analyze(
	(stk, ctx, opt): (&mut Stk, &Context<'_>, Option<&Options>),
	(az, val): (Value, Value),
) -> Result<Value, Error> {
	if let (Some(opt), Value::Strand(az), Value::Strand(val)) = (opt, az, val) {
		let az: Analyzer =
			ctx.tx_lock().await.get_db_analyzer(opt.ns()?, opt.db()?, az.as_str()).await?.into();
		az.analyze(stk, ctx, opt, val.0).await
	} else {
		Ok(Value::None)
	}
}

pub async fn score(
	(ctx, doc): (&Context<'_>, Option<&CursorDoc<'_>>),
	(match_ref,): (Value,),
) -> Result<Value, Error> {
	if let Some((exe, doc, thg)) = get_execution_context(ctx, doc) {
		return exe.score(ctx, &match_ref, thg, doc.ir).await;
	}
	Ok(Value::None)
}

pub async fn highlight(
	(ctx, doc): (&Context<'_>, Option<&CursorDoc<'_>>),
	args: (Value, Value, Value, Option<Value>),
) -> Result<Value, Error> {
	if let Some((exe, doc, thg)) = get_execution_context(ctx, doc) {
		let hlp: HighlightParams = args.try_into()?;
		return exe.highlight(ctx, thg, hlp, doc.doc.as_ref()).await;
	}
	Ok(Value::None)
}

pub async fn offsets(
	(ctx, doc): (&Context<'_>, Option<&CursorDoc<'_>>),
	(match_ref, partial): (Value, Option<Value>),
) -> Result<Value, Error> {
	if let Some((exe, _, thg)) = get_execution_context(ctx, doc) {
		let partial = partial.map(|p| p.convert_to_bool()).unwrap_or(Ok(false))?;
		return exe.offsets(ctx, thg, match_ref, partial).await;
	}
	Ok(Value::None)
}
