//! Search functions for the engine.
//!
//! These functions provide full-text search capabilities including
//! analyzer operations, result highlighting, scoring, offsets, and fusion.
//!
//! Functions are divided into:
//! - ScalarFunction: search::analyze, search::rrf, search::linear
//! - IndexFunction: search::highlight, search::score, search::offsets

use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};

use anyhow::Result;

use crate::catalog::providers::DatabaseProvider;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::function::index::{IndexContext, IndexContextKind, IndexFunction};
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::ir::iface::Fallibility;
use crate::exec::{ContextLevel, EvalContext};
use crate::expr::logical::EffectSet;
use crate::expr::{Error, Kind};
use crate::fnc::args::{FromArgs, Optional};
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::highlighter::HighlightParams;
use crate::val::{Array, Number, Object, Value};

// =========================================================================
// search::analyze - ScalarFunction
// =========================================================================

/// Analyzes text using a specified analyzer.
///
/// Usage: `search::analyze('analyzer_name', 'text to analyze')`
///
/// Returns an array of tokens produced by the analyzer.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchAnalyze;

impl ScalarFunction for SearchAnalyze {
	fn name(&self) -> &'static str {
		"search::analyze"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Scalar,
			"search::analyze",
			Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA)
		)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::READS_CATALOG
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("analyzer", Kind::String)
			.arg("value", Kind::String)
			.returns(Kind::Array(Box::new(Kind::Any), None))
	}

	fn required_context(&self) -> crate::exec::ContextLevel {
		crate::exec::ContextLevel::Database
	}

	fn is_pure(&self) -> bool {
		false // Depends on database state (analyzer definition)
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			// Expects exactly two arguments. Returns NONE when either is not a
			// string.
			let (az, val): (Value, Value) = FromArgs::from_args("search::analyze", args)?;
			let (Value::String(az), Value::String(val)) = (az, val) else {
				return Ok(Value::None);
			};

			// Get database context - if not available, return NONE
			let db_ctx = match ctx.exec_ctx.database() {
				Ok(db_ctx) => db_ctx,
				Err(_) => return Ok(Value::None),
			};

			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;

			// Get the analyzer definition from the database
			let txn = ctx.txn();
			let az_def = crate::exec::vm::descriptor::object(txn.get_db_analyzer(
				ns_id,
				db_id,
				&az,
				ctx.exec_ctx.version_stamp(),
			))
			.await
			.map_err(|e| anyhow::anyhow!("Analyzer '{}' not found: {}", az, e))?;

			// Create the analyzer
			let analyzer = Analyzer::new(ctx.exec_ctx.ctx().get_index_stores(), az_def).await?;

			let az_fn = StreamingAnalyzerFunction::new(ctx.exec_ctx);
			analyzer.analyze(&az_fn, val).await
		})
	}
}

// =========================================================================
// search::highlight - IndexFunction
// =========================================================================

/// Highlights matching keywords in full-text search results.
///
/// Usage: `search::highlight('<b>', '</b>', 1)` or
///        `search::highlight('<b>', '</b>', 1, true)`
///
/// The match_ref (3rd argument, index 2) is extracted at plan time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchHighlight;

impl IndexFunction for SearchHighlight {
	fn name(&self) -> &'static str {
		"search::highlight"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(
			Index,
			"search::highlight",
			Fallibility::VALUE.union(Fallibility::DATA)
		)
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("prefix", Kind::String)
			.arg("suffix", Kind::String)
			.arg("match_ref", Kind::Number)
			.optional("partial", Kind::Bool)
			.returns(Kind::Any)
	}

	fn index_context_kind(&self) -> IndexContextKind {
		IndexContextKind::FullText
	}

	fn index_ref_arg_index(&self) -> Option<usize> {
		Some(2)
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		index_ctx: &'a IndexContext,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let match_ctx = match index_ctx {
				IndexContext::FullText(ctx) => ctx,
				_ => {
					return Err(anyhow::anyhow!(
						"search::highlight requires a FullText index context"
					));
				}
			};

			let mut args = args.into_iter();

			let prefix = args.next().unwrap_or(Value::None);
			let suffix = args.next().unwrap_or(Value::None);
			let partial = args.next().map(|v| v.is_truthy()).unwrap_or(false);

			// Extract RecordId from the current row
			let rid = extract_record_id(ctx)?;

			// Get the current document value
			let doc = ctx.value_in_scope().read()?.unwrap_or(&Value::None);

			// Get the full-text index resources (lazy init); no index on
			// the scan table covering the idiom → NONE.
			let Some(ft) = match_ctx.ft_resources(ctx).await? else {
				return Ok(Value::None);
			};

			let tx = ctx.txn();

			let hlp = HighlightParams {
				prefix,
				suffix,
				match_ref: Value::None, // Not needed - already resolved via MatchContext
				partial,
			};

			let Some(doc_id) = match_ctx.doc_id(ctx, ft, &rid).await? else {
				return Ok(Value::None);
			};
			let terms: Vec<&_> = std::iter::once(&ft.terms).chain(ft.extra_terms.iter()).collect();
			ft.index.highlight_doc(&tx, doc_id, &terms, hlp, &ft.column, doc).await
		})
	}
}

// =========================================================================
// search::score - IndexFunction
// =========================================================================

/// Returns the relevance score for a full-text search match.
///
/// Usage: `search::score(1)`
///
/// The match_ref (1st argument, index 0) is extracted at plan time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchScore;

impl IndexFunction for SearchScore {
	fn name(&self) -> &'static str {
		"search::score"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Index, "search::score", Fallibility::DATA)
	}

	fn signature(&self) -> Signature {
		Signature::new().arg("match_ref", Kind::Number).returns(Kind::Number)
	}

	fn index_context_kind(&self) -> IndexContextKind {
		IndexContextKind::FullText
	}

	fn index_ref_arg_index(&self) -> Option<usize> {
		Some(0)
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		index_ctx: &'a IndexContext,
		_args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let match_ctx = match index_ctx {
				IndexContext::FullText(ctx) => ctx,
				_ => {
					return Err(anyhow::anyhow!("search::score requires a FullText index context"));
				}
			};

			// Extract RecordId from the current row
			let rid = extract_record_id(ctx)?;

			// Get the full-text index resources (lazy init); no index on
			// the scan table covering the idiom → NONE.
			let Some(ft) = match_ctx.ft_resources(ctx).await? else {
				return Ok(Value::None);
			};

			// The scan that listed the row scored it with its batch.
			if let Some(score) = match_ctx.remembered_score(&rid) {
				return Ok(Value::Number(Number::Float(score as f64)));
			}
			#[cfg(test)]
			crate::exec::function::ROW_SCORES.with(|scores| scores.set(scores.get() + 1));

			let Some(scorer) = match_ctx.scorer(ctx, ft).await? else {
				return Ok(Value::None);
			};

			let tx = ctx.txn();

			// Get the document ID from the record ID
			let Some(doc_id) = match_ctx.doc_id(ctx, ft, &rid).await? else {
				return Ok(Value::None);
			};

			// Compute the BM25 score
			let score = scorer.score(&ft.index, &tx, &ft.terms, doc_id).await?;
			match_ctx.remember_score(&rid, doc_id, score);
			Ok(Value::Number(Number::Float(score as f64)))
		})
	}
}

// =========================================================================
// search::offsets - IndexFunction
// =========================================================================

/// Returns the positions of matching keywords in full-text search results.
///
/// Usage: `search::offsets(1)` or `search::offsets(1, true)`
///
/// The match_ref (1st argument, index 0) is extracted at plan time.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchOffsets;

impl IndexFunction for SearchOffsets {
	fn name(&self) -> &'static str {
		"search::offsets"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Index, "search::offsets", Fallibility::DATA)
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("match_ref", Kind::Number)
			.optional("partial", Kind::Bool)
			.returns(Kind::Any)
	}

	fn index_context_kind(&self) -> IndexContextKind {
		IndexContextKind::FullText
	}

	fn index_ref_arg_index(&self) -> Option<usize> {
		Some(0)
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		index_ctx: &'a IndexContext,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let match_ctx = match index_ctx {
				IndexContext::FullText(ctx) => ctx,
				_ => {
					return Err(anyhow::anyhow!(
						"search::offsets requires a FullText index context"
					));
				}
			};

			let mut args = args.into_iter();
			let partial = args.next().map(|v| v.is_truthy()).unwrap_or(false);

			// Extract RecordId from the current row
			let rid = extract_record_id(ctx)?;

			// Get the full-text index resources (lazy init); no index on
			// the scan table covering the idiom → NONE.
			let Some(ft) = match_ctx.ft_resources(ctx).await? else {
				return Ok(Value::None);
			};

			let tx = ctx.txn();

			let Some(doc_id) = match_ctx.doc_id(ctx, ft, &rid).await? else {
				return Ok(Value::None);
			};
			let terms: Vec<&_> = std::iter::once(&ft.terms).chain(ft.extra_terms.iter()).collect();
			ft.index.read_offsets_doc(&tx, doc_id, &terms, partial).await
		})
	}
}

// =========================================================================
// search::rrf - ScalarFunction
// =========================================================================

/// Reciprocal Rank Fusion for combining multiple ranked result lists.
///
/// Usage: `search::rrf([$vs, $ft], 10, 60)`
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchRrf;

/// Internal structure for storing documents during RRF/linear processing.
struct FusionDoc(f64, Value, Vec<Object>);

impl PartialEq for FusionDoc {
	fn eq(&self, other: &Self) -> bool {
		self.0 == other.0
	}
}

impl Eq for FusionDoc {}

impl PartialOrd for FusionDoc {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for FusionDoc {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		// Reversed comparison for min-heap behavior with BinaryHeap.
		// peek() returns the smallest element, enabling correct top-k selection:
		// evict the smallest when a higher-scoring element arrives.
		other.0.partial_cmp(&self.0).unwrap_or(std::cmp::Ordering::Equal)
	}
}

impl ScalarFunction for SearchRrf {
	fn name(&self) -> &'static str {
		"search::rrf"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Scalar, "search::rrf", Fallibility::VALUE)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::PURE
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("results", Kind::Array(Box::new(Kind::Any), None))
			.arg("limit", Kind::Number)
			.optional("rrf_constant", Kind::Number)
			.returns(Kind::Array(Box::new(Kind::Any), None))
	}

	fn is_pure(&self) -> bool {
		false // Needs context for cancellation checks
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let frozen = ctx.exec_ctx.ctx();

			// results and limit are required. rrf_constant is optional.
			let (results, limit, Optional(rrf_constant)): (Array, i64, Optional<i64>) =
				FromArgs::from_args("search::rrf", args)?;
			let limit = if limit < 1 {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "search::rrf".to_string(),
					message: "limit must be at least 1".to_string(),
				});
			} else {
				limit as usize
			};
			let rrf_constant = if let Some(rrf_constant) = rrf_constant {
				if rrf_constant < 0 {
					anyhow::bail!(Error::InvalidFunctionArguments {
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

			#[expect(clippy::mutable_key_type)]
			let mut documents: HashMap<Value, (f64, Vec<Object>)> = HashMap::new();
			let mut count = 0;

			for result_list in results {
				if let Value::Array(array) = result_list {
					for (rank, doc) in array.into_iter().enumerate() {
						if let Value::Object(mut obj) = doc
							&& let Some(id_value) = obj.remove("id")
						{
							let rrf_contribution = 1.0 / (rrf_constant + (rank + 1) as f64);
							match documents.entry(id_value) {
								Entry::Vacant(entry) => {
									entry.insert((rrf_contribution, vec![obj]));
								}
								Entry::Occupied(e) => {
									let (score, objects) = e.into_mut();
									*score += rrf_contribution;
									objects.push(obj);
								}
							}
						}
						if frozen.is_done(Some(count)).await? {
							break;
						}
						count += 1;
					}
				}
			}

			let mut scored_docs = BinaryHeap::with_capacity(limit.min(documents.len()));
			for (id, (score, objects)) in documents {
				if scored_docs.len() < limit {
					scored_docs.push(FusionDoc(score, id, objects));
				} else if let Some(FusionDoc(worst_score, _, _)) = scored_docs.peek()
					&& score > *worst_score
				{
					scored_docs.pop();
					scored_docs.push(FusionDoc(score, id, objects));
				}
				if frozen.is_done(Some(count)).await? {
					break;
				}
				count += 1;
			}

			let mut result_array = Array::new();
			while let Some(doc) = scored_docs.pop() {
				let mut obj = Object::default();
				for mut o in doc.2 {
					obj.append(&mut o.0);
				}
				obj.insert("id", doc.1);
				obj.insert("rrf_score", Value::Number(Number::Float(doc.0)));
				result_array.push(Value::Object(obj));
			}
			// Min-heap pop yields ascending order; reverse for descending score order
			result_array.0.reverse();

			Ok(Value::Array(result_array))
		})
	}
}

// =========================================================================
// search::linear - ScalarFunction
// =========================================================================

/// Weighted linear combination to fuse multiple ranked result lists.
///
/// Usage: `search::linear([$vs, $ft], [2, 1], 10, 'minmax')`
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchLinear;

enum LinearNorm {
	MinMax,
	ZScore,
}

impl ScalarFunction for SearchLinear {
	fn name(&self) -> &'static str {
		"search::linear"
	}

	fn fallibility(&self) -> Fallibility {
		crate::declare_fallibility!(Scalar, "search::linear", Fallibility::VALUE)
	}

	fn effects(&self) -> EffectSet {
		EffectSet::PURE
	}

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("results", Kind::Array(Box::new(Kind::Any), None))
			.arg("weights", Kind::Array(Box::new(Kind::Number), None))
			.arg("limit", Kind::Number)
			.arg("norm", Kind::String)
			.returns(Kind::Array(Box::new(Kind::Any), None))
	}

	fn is_pure(&self) -> bool {
		false // Needs context for cancellation checks
	}

	fn is_async(&self) -> bool {
		true
	}

	fn invoke(&self, _args: Vec<Value>) -> Result<Value> {
		Err(anyhow::anyhow!("Function '{}' requires async execution", self.name()))
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		args: Vec<Value>,
	) -> crate::exec::BoxFut<'a, Result<Value>> {
		Box::pin(async move {
			let frozen = ctx.exec_ctx.ctx();

			// results, weights, limit and norm are all required.
			let (results, weights, limit, norm): (Array, Array, i64, String) =
				FromArgs::from_args("search::linear", args)?;
			let limit = if limit < 1 {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "search::linear".to_string(),
					message: "Limit must be at least 1".to_string(),
				});
			} else {
				limit as usize
			};
			if weights.len() != results.len() {
				anyhow::bail!(Error::InvalidFunctionArguments {
					name: "search::linear".to_string(),
					message: "The results and the weights array should have the same length"
						.to_string(),
				});
			}
			for (i, weight) in weights.iter().enumerate() {
				if !matches!(weight, Value::Number(_)) {
					anyhow::bail!(Error::InvalidFunctionArguments {
						name: "search::linear".to_string(),
						message: format!("Weight at index {} must be a number", i),
					});
				}
			}
			let norm = match norm.as_str() {
				"minmax" => LinearNorm::MinMax,
				"zscore" => LinearNorm::ZScore,
				_ => anyhow::bail!(Error::InvalidFunctionArguments {
					name: "search::linear".to_string(),
					message: "Norm must be 'minmax' or 'zscore'".to_string(),
				}),
			};

			if results.is_empty() {
				return Ok(Value::Array(Array::new()));
			}

			let results_len = results.len();
			#[expect(clippy::mutable_key_type)]
			let mut documents: HashMap<Value, (Vec<f64>, Vec<Object>)> = HashMap::new();
			let mut count = 0;

			for (list_idx, result_list) in results.into_iter().enumerate() {
				if let Value::Array(array) = result_list {
					for (rank, doc) in array.into_iter().enumerate() {
						if let Value::Object(mut obj) = doc
							&& let Some(id_value) = obj.remove("id")
						{
							let score = if let Some(Value::Number(n)) = obj.get("distance") {
								1.0 / (1.0 + n.as_float())
							} else if let Some(Value::Number(n)) = obj.get("ft_score") {
								n.as_float()
							} else if let Some(Value::Number(n)) = obj.get("score") {
								n.as_float()
							} else {
								1.0 / (1.0 + rank as f64)
							};

							match documents.entry(id_value) {
								Entry::Vacant(entry) => {
									let mut scores = vec![0.0; results_len];
									scores[list_idx] = score;
									entry.insert((scores, vec![obj]));
								}
								Entry::Occupied(e) => {
									let (scores, objects) = e.into_mut();
									scores[list_idx] = score;
									objects.push(obj);
								}
							}
						}
						if frozen.is_done(Some(count)).await? {
							break;
						}
						count += 1;
					}
				}
			}

			// Compute normalization parameters
			let mut all_scores_by_list: Vec<Vec<f64>> = vec![Vec::new(); results_len];
			for (scores, _) in documents.values() {
				for (list_idx, &score) in scores.iter().enumerate() {
					if score > 0.0 {
						all_scores_by_list[list_idx].push(score);
					}
				}
			}

			let mut normalized_params: Vec<(f64, f64)> = Vec::new();
			for list_scores in &all_scores_by_list {
				if list_scores.is_empty() {
					normalized_params.push((0.0, 1.0));
					continue;
				}
				match norm {
					LinearNorm::MinMax => {
						let min_score = list_scores.iter().fold(f64::INFINITY, |a, &b| a.min(b));
						let max_score =
							list_scores.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
						let range = max_score - min_score;
						if range > 0.0 {
							normalized_params.push((min_score, range));
						} else {
							normalized_params.push((min_score, 1.0));
						}
					}
					LinearNorm::ZScore => {
						let mean = list_scores.iter().sum::<f64>() / list_scores.len() as f64;
						let variance = list_scores.iter().map(|&x| (x - mean).powi(2)).sum::<f64>()
							/ list_scores.len() as f64;
						let std_dev = variance.sqrt();
						if std_dev > 0.0 {
							normalized_params.push((mean, std_dev));
						} else {
							normalized_params.push((mean, 1.0));
						}
					}
				}
			}

			let mut scored_docs = BinaryHeap::with_capacity(limit.min(documents.len()));
			for (id, (scores, objects)) in documents {
				let mut combined_score = 0.0;
				for (list_idx, &score) in scores.iter().enumerate() {
					if score > 0.0 {
						let weight = if let Some(Value::Number(w)) = weights.get(list_idx) {
							w.as_float()
						} else {
							1.0
						};
						let normalized_score = match norm {
							LinearNorm::MinMax => {
								let (min_val, range) = normalized_params[list_idx];
								(score - min_val) / range
							}
							LinearNorm::ZScore => {
								let (mean, std_dev) = normalized_params[list_idx];
								(score - mean) / std_dev
							}
						};
						combined_score += weight * normalized_score;
					}
				}

				if scored_docs.len() < limit {
					scored_docs.push(FusionDoc(combined_score, id, objects));
				} else if let Some(FusionDoc(worst_score, _, _)) = scored_docs.peek()
					&& combined_score > *worst_score
				{
					scored_docs.pop();
					scored_docs.push(FusionDoc(combined_score, id, objects));
				}
				if frozen.is_done(Some(count)).await? {
					break;
				}
				count += 1;
			}

			let mut result_array = Array::new();
			while let Some(doc) = scored_docs.pop() {
				let mut obj = Object::default();
				for mut o in doc.2 {
					obj.append(&mut o.0);
				}
				obj.insert("id", doc.1);
				obj.insert("linear_score", Value::Number(Number::Float(doc.0)));
				result_array.push(Value::Object(obj));
			}
			// Min-heap pop yields ascending order; reverse for descending score order
			result_array.0.reverse();

			Ok(Value::Array(result_array))
		})
	}
}

// =========================================================================
// Helpers
// =========================================================================

/// Extract the RecordId from the current row value.
///
/// The current row is a Value::Object with an "id" field containing the RecordId.
fn extract_record_id(ctx: &EvalContext<'_>) -> Result<crate::val::RecordId> {
	let current = ctx.value_in_scope().read()?.ok_or_else(|| {
		anyhow::anyhow!("Index function requires a current document (must be used in SELECT)")
	})?;

	if let Value::Object(obj) = current {
		match obj.get("id") {
			Some(Value::RecordId(rid)) => Ok(rid.clone()),
			Some(_) => Err(anyhow::anyhow!("Current document 'id' field is not a RecordId")),
			None => Err(anyhow::anyhow!("Current document has no 'id' field")),
		}
	} else if let Value::RecordId(rid) = current {
		Ok(rid.clone())
	} else {
		Err(anyhow::anyhow!(
			"Expected current document to be an Object, got: {}",
			current.kind_of()
		))
	}
}

// =========================================================================
// Registration
// =========================================================================

/// The `search::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&SearchAnalyze, &SearchRrf, &SearchLinear];

pub fn register(registry: &mut FunctionRegistry) {
	// Index functions (bound to WHERE clause MATCHES predicates)
	registry.register_index_function(SearchHighlight);
	registry.register_index_function(SearchScore);
	registry.register_index_function(SearchOffsets);
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::TestDb;

	fn doc(id: &str) -> Value {
		let mut obj = Object::default();
		obj.insert("id", Value::from(id));
		Value::Object(obj)
	}

	#[tokio::test]
	async fn rrf_and_linear_cap_heap_capacity_on_huge_limit() {
		let db = TestDb::new("").await;
		let exec = db.exec_ctx().await;
		let eval = EvalContext::from_exec_ctx(&exec);

		let empty_lists = Value::Array(Array::from(vec![Value::Array(Array::new())]));
		let rrf_res = SearchRrf
			.invoke_async(&eval, vec![empty_lists.clone(), Value::from(i64::MAX)])
			.await
			.expect("search::rrf succeeds with huge limit");
		assert_eq!(rrf_res, Value::Array(Array::new()));

		let weights = Value::Array(Array::from(vec![Value::from(1)]));
		let linear_res = SearchLinear
			.invoke_async(
				&eval,
				vec![empty_lists, weights, Value::from(i64::MAX), Value::from("minmax")],
			)
			.await
			.expect("search::linear succeeds with huge limit");
		assert_eq!(linear_res, Value::Array(Array::new()));

		exec.txn().cancel().await.expect("cancel");
	}

	#[tokio::test]
	async fn linear_rank_fallback_is_per_list() {
		let db = TestDb::new("").await;
		let exec = db.exec_ctx().await;
		let eval = EvalContext::from_exec_ctx(&exec);

		let list0 = Value::Array(Array::from(vec![doc("d1"), doc("d2")]));
		let list1 = Value::Array(Array::from(vec![doc("d3"), doc("d1"), doc("d4")]));
		let results = Value::Array(Array::from(vec![list0, list1]));
		let weights = Value::Array(Array::from(vec![Value::from(1.0), Value::from(1.0)]));

		let Value::Array(out) = SearchLinear
			.invoke_async(&eval, vec![results, weights, Value::from(10), Value::from("minmax")])
			.await
			.expect("search::linear succeeds")
		else {
			panic!("expected array output");
		};

		let d1_score = out
			.iter()
			.find_map(|v| {
				if let Value::Object(obj) = v
					&& obj.get("id") == Some(&Value::from("d1"))
					&& let Some(Value::Number(n)) = obj.get("linear_score")
				{
					Some(n.as_float())
				} else {
					None
				}
			})
			.expect("d1 in output");
		assert!((d1_score - 1.25).abs() < 1e-12, "expected 1.25, got {d1_score}");

		exec.txn().cancel().await.expect("cancel");
	}
}
