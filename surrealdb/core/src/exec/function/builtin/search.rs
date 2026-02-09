//! Search functions for the streaming executor.
//!
//! These functions provide full-text search capabilities including
//! analyzer operations, result highlighting, scoring, offsets, and fusion.
//!
//! Functions are divided into:
//! - ScalarFunction: search::analyze, search::rrf, search::linear
//! - IndexFunction: search::highlight, search::score, search::offsets

use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};
use std::pin::Pin;

use anyhow::Result;
use reblessive::tree::TreeStack;

use crate::catalog::providers::DatabaseProvider;
use crate::err::Error;
use crate::exec::ContextLevel;
use crate::exec::function::index::{IndexFunction, MatchContext};
use crate::exec::function::{FunctionRegistry, ScalarFunction, Signature};
use crate::exec::physical_expr::EvalContext;
use crate::expr::Kind;
use crate::idx::ft::analyzer::Analyzer;
use crate::idx::ft::highlighter::HighlightParams;
use crate::val::{Array, Number, Object, Value};

// =========================================================================
// search::analyze - ScalarFunction (already existed)
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
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			let mut args = args.into_iter();

			// Get analyzer name
			let az = match args.next() {
				Some(Value::String(s)) => s,
				Some(v) => {
					return Err(anyhow::anyhow!(
						"Function 'search::analyze' expects a string analyzer name, got: {}",
						v.kind_of()
					));
				}
				None => {
					return Err(anyhow::anyhow!(
						"Function 'search::analyze' expects two arguments: analyzer name and value"
					));
				}
			};

			// Get value to analyze
			let val = match args.next() {
				Some(Value::String(s)) => s,
				Some(v) => {
					return Err(anyhow::anyhow!(
						"Function 'search::analyze' expects a string value, got: {}",
						v.kind_of()
					));
				}
				None => {
					return Err(anyhow::anyhow!(
						"Function 'search::analyze' expects two arguments: analyzer name and value"
					));
				}
			};

			// Get the options - if not available, return NONE (matching original behavior)
			let opt = match ctx.exec_ctx.options() {
				Some(opt) => opt,
				None => return Ok(Value::None),
			};

			// Get database context - if not available, return NONE
			let db_ctx = match ctx.exec_ctx.database() {
				Ok(db_ctx) => db_ctx,
				Err(_) => return Ok(Value::None),
			};

			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;

			// Get the analyzer definition from the database
			let az_def = ctx
				.txn()
				.get_db_analyzer(ns_id, db_id, &az)
				.await
				.map_err(|e| anyhow::anyhow!("Analyzer '{}' not found: {}", az, e))?;

			// Create the analyzer
			let analyzer = Analyzer::new(ctx.exec_ctx.ctx().get_index_stores(), az_def)?;

			// Analyze the value using a TreeStack
			let frozen = ctx.exec_ctx.ctx();
			let mut stack = TreeStack::new();
			stack
				.enter(|stk| async move { analyzer.analyze(stk, frozen, opt, val).await })
				.finish()
				.await
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

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("prefix", Kind::String)
			.arg("suffix", Kind::String)
			.arg("match_ref", Kind::Number)
			.optional("partial", Kind::Bool)
			.returns(Kind::Any)
	}

	fn match_ref_arg_index(&self) -> usize {
		2
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		match_ctx: &'a MatchContext,
		args: Vec<Value>,
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			let mut args = args.into_iter();

			let prefix = args.next().unwrap_or(Value::None);
			let suffix = args.next().unwrap_or(Value::None);
			let partial = args.next().map(|v| v.is_truthy()).unwrap_or(false);

			// Extract RecordId from the current row
			let rid = extract_record_id(ctx)?;

			// Get the current document value
			let doc = ctx.current_value.unwrap_or(&Value::None);

			// Get the full-text index resources (lazy init)
			let (fti, qt, _scorer) = match_ctx.ft_resources(ctx).await?;

			let tx = ctx.txn();

			let hlp = HighlightParams {
				prefix,
				suffix,
				match_ref: Value::None, // Not needed - already resolved via MatchContext
				partial,
			};

			fti.highlight(&tx, &rid, qt, hlp, &match_ctx.idiom, doc).await
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

	fn signature(&self) -> Signature {
		Signature::new().arg("match_ref", Kind::Number).returns(Kind::Number)
	}

	fn match_ref_arg_index(&self) -> usize {
		0
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		match_ctx: &'a MatchContext,
		_args: Vec<Value>,
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			// Extract RecordId from the current row
			let rid = extract_record_id(ctx)?;

			// Get the full-text index resources (lazy init)
			let (fti, qt, scorer) = match_ctx.ft_resources(ctx).await?;

			let scorer = match scorer {
				Some(s) => s,
				None => return Ok(Value::None),
			};

			let tx = ctx.txn();

			// Get the document ID from the record ID
			let doc_id = match fti.get_doc_id(&tx, &rid).await? {
				Some(id) => id,
				None => return Ok(Value::None),
			};

			// Compute the BM25 score
			let score = scorer.score(fti, &tx, qt, doc_id).await?;
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

	fn signature(&self) -> Signature {
		Signature::new()
			.arg("match_ref", Kind::Number)
			.optional("partial", Kind::Bool)
			.returns(Kind::Any)
	}

	fn match_ref_arg_index(&self) -> usize {
		0
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn invoke_async<'a>(
		&'a self,
		ctx: &'a EvalContext<'_>,
		match_ctx: &'a MatchContext,
		args: Vec<Value>,
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			let mut args = args.into_iter();
			let partial = args.next().map(|v| v.is_truthy()).unwrap_or(false);

			// Extract RecordId from the current row
			let rid = extract_record_id(ctx)?;

			// Get the full-text index resources (lazy init)
			let (fti, qt, _scorer) = match_ctx.ft_resources(ctx).await?;

			let tx = ctx.txn();

			fti.read_offsets(&tx, &rid, qt, partial).await
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
		self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
	}
}

impl ScalarFunction for SearchRrf {
	fn name(&self) -> &'static str {
		"search::rrf"
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
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			let frozen = ctx.exec_ctx.ctx();
			let mut args = args.into_iter();

			let results = match args.next() {
				Some(Value::Array(a)) => a,
				_ => return Ok(Value::Array(Array::new())),
			};
			let limit = match args.next() {
				Some(Value::Number(n)) => {
					let l = n.as_int();
					if l < 1 {
						anyhow::bail!(Error::InvalidArguments {
							name: "search::rrf".to_string(),
							message: "limit must be at least 1".to_string(),
						});
					}
					l as usize
				}
				_ => anyhow::bail!(Error::InvalidArguments {
					name: "search::rrf".to_string(),
					message: "limit must be a number".to_string(),
				}),
			};
			let rrf_constant = match args.next() {
				Some(Value::Number(n)) => {
					let k = n.as_int();
					if k < 0 {
						anyhow::bail!(Error::InvalidArguments {
							name: "search::rrf".to_string(),
							message: "RRF constant must be at least 0".to_string(),
						});
					}
					k as f64
				}
				_ => 60.0,
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

			let mut scored_docs = BinaryHeap::with_capacity(limit);
			for (id, (score, objects)) in documents {
				if scored_docs.len() < limit {
					scored_docs.push(FusionDoc(score, id, objects));
				} else if let Some(FusionDoc(min_score, _, _)) = scored_docs.peek()
					&& score > *min_score
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
				obj.insert("id".to_string(), doc.1);
				obj.insert("rrf_score".to_string(), Value::Number(Number::Float(doc.0)));
				result_array.push(Value::Object(obj));
			}

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
	) -> Pin<Box<dyn std::future::Future<Output = Result<Value>> + Send + 'a>> {
		Box::pin(async move {
			let frozen = ctx.exec_ctx.ctx();
			let mut args = args.into_iter();

			let results = match args.next() {
				Some(Value::Array(a)) => a,
				_ => return Ok(Value::Array(Array::new())),
			};
			let weights = match args.next() {
				Some(Value::Array(a)) => a,
				_ => anyhow::bail!(Error::InvalidArguments {
					name: "search::linear".to_string(),
					message: "weights must be an array".to_string(),
				}),
			};
			let limit = match args.next() {
				Some(Value::Number(n)) => {
					let l = n.as_int();
					if l < 1 {
						anyhow::bail!(Error::InvalidArguments {
							name: "search::linear".to_string(),
							message: "Limit must be at least 1".to_string(),
						});
					}
					l as usize
				}
				_ => anyhow::bail!(Error::InvalidArguments {
					name: "search::linear".to_string(),
					message: "limit must be a number".to_string(),
				}),
			};
			let norm = match args.next() {
				Some(Value::String(s)) => match s.as_str() {
					"minmax" => LinearNorm::MinMax,
					"zscore" => LinearNorm::ZScore,
					_ => anyhow::bail!(Error::InvalidArguments {
						name: "search::linear".to_string(),
						message: "Norm must be 'minmax' or 'zscore'".to_string(),
					}),
				},
				_ => anyhow::bail!(Error::InvalidArguments {
					name: "search::linear".to_string(),
					message: "norm must be a string".to_string(),
				}),
			};

			if weights.len() != results.len() {
				anyhow::bail!(Error::InvalidArguments {
					name: "search::linear".to_string(),
					message: "The results and the weights array should have the same length"
						.to_string(),
				});
			}
			for (i, weight) in weights.iter().enumerate() {
				if !matches!(weight, Value::Number(_)) {
					anyhow::bail!(Error::InvalidArguments {
						name: "search::linear".to_string(),
						message: format!("Weight at index {} must be a number", i),
					});
				}
			}

			if results.is_empty() {
				return Ok(Value::Array(Array::new()));
			}

			let results_len = results.len();
			#[expect(clippy::mutable_key_type)]
			let mut documents: HashMap<Value, (Vec<f64>, Vec<Object>)> = HashMap::new();
			let mut count = 0;

			for (list_idx, result_list) in results.into_iter().enumerate() {
				if let Value::Array(array) = result_list {
					for doc in array {
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
								1.0 / (1.0 + count as f64)
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

			let mut scored_docs = BinaryHeap::with_capacity(limit);
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
				} else if let Some(FusionDoc(min_score, _, _)) = scored_docs.peek()
					&& combined_score > *min_score
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
				obj.insert("id".to_string(), doc.1);
				obj.insert("linear_score".to_string(), Value::Number(Number::Float(doc.0)));
				result_array.push(Value::Object(obj));
			}

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
	let current = ctx.current_value.ok_or_else(|| {
		anyhow::anyhow!("Index function requires a current document (must be used in SELECT)")
	})?;

	match current {
		Value::Object(obj) => match obj.get("id") {
			Some(Value::RecordId(rid)) => Ok(rid.clone()),
			Some(_) => Err(anyhow::anyhow!("Current document 'id' field is not a RecordId")),
			None => Err(anyhow::anyhow!("Current document has no 'id' field")),
		},
		Value::RecordId(rid) => Ok(rid.clone()),
		_ => Err(anyhow::anyhow!(
			"Expected current document to be an Object, got: {}",
			current.kind_of()
		)),
	}
}

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	// Scalar functions
	registry.register(SearchAnalyze);
	registry.register(SearchRrf);
	registry.register(SearchLinear);

	// Index functions (bound to WHERE clause MATCHES predicates)
	registry.register_index_function(SearchHighlight);
	registry.register_index_function(SearchScore);
	registry.register_index_function(SearchOffsets);
}
