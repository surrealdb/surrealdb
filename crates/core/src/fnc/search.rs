use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Array, Value};
use crate::expr::{Number, Object};
use crate::fnc::get_execution_context;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::highlighter::HighlightParams;
use anyhow::Result;
use reblessive::tree::Stk;
use std::collections::HashMap;

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

	// Map to store the original documents objects and scores
	let mut documents: HashMap<Value, (f64, Vec<Object>)> = HashMap::new();

	// Process each result list
	for result_list in results.into_iter() {
		if let Value::Array(array) = result_list {
			// Process each document in this result list
			for (rank, doc) in array.into_iter().enumerate() {
				if let Value::Object(mut obj) = doc {
					// Extract the ID from the document
					if let Some(id_value) = obj.remove("id") {
						// Calculate RRF contribution: 1 / (k + rank + 1)
						// rank is 0-based, but RRF uses 1-based ranking
						let rrf_contribution = 1.0 / (rrf_constant + (rank + 1) as f64);

						// Store the document (use the first occurrence or merge if needed)
						match documents.get_mut(&id_value) {
							// Insert the first occurrence
							None => {
								documents.insert(id_value, (rrf_contribution, vec![obj]));
							}
							// Or merge
							Some((score, objects)) => {
								// Add to RRF score
								*score += rrf_contribution;
								objects.push(obj);
							}
						}
					}
				}
			}
		}
	}

	// Convert to vector and sort by RRF score (descending)
	let mut scored_docs: Vec<_> = documents.into_iter().collect();
	scored_docs.sort_by(|a, b| b.1.0.partial_cmp(&a.1.0).unwrap_or(std::cmp::Ordering::Equal));

	// Take top `limit` results and create the final array
	let mut result_array = Array::new();
	for (id, (rrf_score, objects)) in scored_docs.into_iter().take(limit) {
		// Merge the documents
		let mut obj = Object::default();
		for mut o in objects {
			obj.append(&mut o.0);
		}
		// Add the ID and the RRF score
		obj.insert("id".to_string(), id);
		obj.insert("rrf_score".to_string(), Value::Number(Number::Float(rrf_score)));
		result_array.push(Value::Object(obj));
	}
	// Return the result
	Ok(Value::Array(result_array))
}
