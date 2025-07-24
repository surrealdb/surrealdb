use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Array, Value};
use crate::fnc::get_execution_context;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::highlighter::HighlightParams;
use anyhow::Result;
use reblessive::tree::Stk;

use super::args::Optional;

pub async fn analyze(
	(stk, ctx, opt): (&mut Stk, &Context, Option<&Options>),
	(az, val): (Value, Value),
) -> Result<Value> {
	if let (Some(opt), Value::Strand(az), Value::Strand(val)) = (opt, az, val) {
		let (ns, db) = opt.ns_db()?;
		let az = ctx.tx().get_db_analyzer(ns, db, &az).await?;
		let az = Analyzer::new(ctx.get_index_stores(), az)?;
		az.analyze(stk, ctx, opt, val.0).await
	} else {
		Ok(Value::None)
	}
}

pub async fn score(
	(ctx, doc): (&Context, Option<&CursorDoc>),
	(match_ref,): (Value,),
) -> Result<Value> {
	if let Some((exe, doc, thg)) = get_execution_context(ctx, doc) {
		return exe.score(ctx, &match_ref, thg, doc.ir.as_ref()).await;
	}
	Ok(Value::None)
}

pub async fn highlight(
	(ctx, doc): (&Context, Option<&CursorDoc>),
	(prefix, suffix, match_ref, Optional(partial)): (Value, Value, Value, Optional<bool>),
) -> Result<Value> {
	if let Some((exe, doc, thg)) = get_execution_context(ctx, doc) {
		let hlp = HighlightParams {
			prefix,
			suffix,
			match_ref,
			partial: partial.unwrap_or(false),
		};

		return exe.highlight(ctx, thg, hlp, doc.doc.as_ref()).await;
	}
	Ok(Value::None)
}

pub async fn offsets(
	(ctx, doc): (&Context, Option<&CursorDoc>),
	(match_ref, Optional(partial)): (Value, Optional<bool>),
) -> Result<Value> {
	if let Some((exe, _, thg)) = get_execution_context(ctx, doc) {
		let partial = partial.unwrap_or(false);
		return exe.offsets(ctx, thg, match_ref, partial).await;
	}
	Ok(Value::None)
}

pub async fn rrf(
	_ctx: &Context,
	(results, limit, rrf_constant): (Array, i64, Optional<i64>),
) -> Result<Value> {
	let limit = if limit < 1 {
		anyhow::bail!(Error::InvalidArguments {
			name: "search::rrf".to_string(),
			message: "limit must be at least 1".to_string(),
		});
	} else {
		limit as usize
	};
	let rrf_constant = if let Some(rrf_constant) = rrf_constant.0 {
		if rrf_constant < 0 {
			anyhow::bail!(Error::InvalidArguments {
				name: "search::rrf".to_string(),
				message: "RRF constant must be at least 0".to_string(),
			});
		}
		rrf_constant as f64
	} else {
		60.0
	};
	if results.is_empty() {
		return Ok(Value::Array(Array::new()));
	}
	// Eg of array: [[{ id: test:1 }, { id: test:3 }], [{ id: test:1, score: 0.5366538763046265f }]]
	todo!("Implement the actual RRF (Reciprocal Rank Fusion) algorithm")
}
