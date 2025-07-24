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
use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};

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

/// Internal structure for storing documents during RRF (Reciprocal Rank Fusion) processing.
///
/// This tuple struct contains:
/// - `f64`: The accumulated RRF score for the document
/// - `Value`: The document ID used to identify the same document across different result lists
/// - `Vec<Object>`: Collection of original objects from different search results that will be merged
///
/// The struct implements comparison traits (`Eq`, `Ord`, `PartialEq`, `PartialOrd`) based solely
/// on the RRF score (first field) to enable efficient sorting and heap operations during the
/// top-k selection process.
struct RrfDoc(f64, Value, Vec<Object>);

impl PartialEq for RrfDoc {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}

impl Eq for RrfDoc {}

impl PartialOrd for RrfDoc {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for RrfDoc {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
	}
}

/// Implements Reciprocal Rank Fusion (RRF) to combine multiple ranked result lists.
///
/// RRF is a method for combining results from different search algorithms (e.g., vector search
/// and full-text search) by computing a unified score based on the reciprocal of each document's
/// rank in each result list. The algorithm uses the formula: `1 / (k + rank)` where `k` is the
/// RRF constant and `rank` is the 1-based position in the result list.
///
/// # Parameters
///
/// * `ctx` - The execution context for cancellation checking and transaction management
/// * `results` - An array of result lists, where each list contains documents with an "id" field
/// * `limit` - Maximum number of documents to return (must be ≥ 1)
/// * `rrf_constant` - Optional RRF constant (k) for score calculation (defaults to 60.0, must be ≥ 0)
///
/// # Returns
///
/// Returns a `Value::Array` containing the top `limit` documents sorted by RRF score in descending
/// order. Each document includes:
/// - All original fields from the input documents (merged if the same document appears in multiple lists)
/// - `id`: The document identifier
/// - `rrf_score`: The computed RRF score as a float
///
/// # Errors
///
/// * `Error::InvalidArguments` - If `limit` < 1 or `rrf_constant` < 0
/// * Context cancellation errors if the operation is cancelled during processing
///
/// # Example
///
/// ```surql
/// -- Combine vector search and full-text search results
/// LET $vector_results = SELECT id, distance FROM docs WHERE embedding <|5|> $query_vector;
/// LET $text_results = SELECT id, ft_score FROM docs WHERE text @@ 'search terms';
/// RETURN search::rrf([$vector_results, $text_results], 10, 60);
/// ```
pub async fn rrf(
	ctx: &Context,
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

	// Map to store document IDs with their accumulated RRF scores and original objects
	// Key: document ID, Value: (accumulated_rrf_score, vector_of_original_objects)
	#[expect(clippy::mutable_key_type)]
	let mut documents: HashMap<Value, (f64, Vec<Object>)> = HashMap::new();

	// Process each result list from the input array (e.g., vector search results, full-text search results)
	let mut count = 0;
	for result_list in results.into_iter() {
		if let Value::Array(array) = result_list {
			// Process each document in this result list, using enumerate to get 0-based rank
			for (rank, doc) in array.into_iter().enumerate() {
				if let Value::Object(mut obj) = doc {
					// Extract the document ID (required for RRF to identify same documents across lists)
					if let Some(id_value) = obj.remove("id") {
						// Calculate RRF contribution using the standard formula: 1 / (k + rank + 1)
						// where k is the RRF constant and rank is converted from 0-based to 1-based
						let rrf_contribution = 1.0 / (rrf_constant + (rank + 1) as f64);

						// Store or merge the document based on whether we've seen this ID before
						match documents.entry(id_value) {
							// First time seeing this document ID - store it with its RRF contribution
							Entry::Vacant(entry) => {
								entry.insert((rrf_contribution, vec![obj]));
							}
							// Document ID already exists - accumulate RRF scores and merge objects
							Entry::Occupied(e) => {
								let (score, objects) = e.into_mut();
								// Accumulate RRF scores (this is the core of RRF fusion)
								*score += rrf_contribution;
								// Keep all original objects for later merging
								objects.push(obj);
							}
						}
					}
				}
				if ctx.is_done(count % 100 == 0).await? {
					break;
				}
				count += 1;
			}
		}
	}

	// Use a min-heap (BinaryHeap) to efficiently maintain only the top `limit` documents
	// This avoids sorting all documents when we only need the top-k results
	let mut scored_docs = BinaryHeap::with_capacity(limit);
	for (id, (score, objects)) in documents {
		if scored_docs.len() < limit {
			// Heap not full yet - add document directly
			scored_docs.push(RrfDoc(score, id, objects));
		} else if let Some(RrfDoc(min_score, _, _)) = scored_docs.peek() {
			// Heap is full - only add if this document has a higher score than the minimum
			if score > *min_score {
				scored_docs.pop(); // Remove the lowest scoring document
				scored_docs.push(RrfDoc(score, id, objects)); // Add the new higher scoring document
			}
		}
		if ctx.is_done(count % 100 == 0).await? {
			break;
		}
		count += 1;
	}

	// Extract the top `limit` results from the heap and build the final result array
	// Note: BinaryHeap.pop() returns documents in descending order by RRF score (highest first)
	let mut result_array = Array::new();
	while let Some(doc) = scored_docs.pop() {
		// Merge all objects from the same document ID across different result lists
		// This combines fields like 'distance' from vector search and 'ft_score' from full-text search
		let mut obj = Object::default();
		for mut o in doc.2 {
			obj.append(&mut o.0);
		}
		// Add the document ID back (was removed during processing) and the computed RRF score
		obj.insert("id".to_string(), doc.1);
		obj.insert("rrf_score".to_string(), Value::Number(Number::Float(doc.0)));
		result_array.push(Value::Object(obj));
		if ctx.is_done(count % 100 == 0).await? {
			break;
		}
		count += 1;
	}
	// Return the fused results sorted by RRF score in descending order
	Ok(Value::Array(result_array))
}
