//! Full-text search scan operator.
//!
//! This operator retrieves records using full-text search indexes,
//! supporting the MATCHES operator with BM25 or VS scoring.

use std::sync::Arc;

use async_trait::async_trait;
use reblessive::TreeStack;

use super::common::fetch_and_filter_records_batch;
use super::resolved::ResolvedTableContext;
use crate::catalog::Index;
use crate::err::Error;
use crate::exec::index::access_path::IndexRef;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::operator::MatchesOperator;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::planner::iterators::MatchesHitsIterator;
use crate::kvs::CachePolicy;

/// Batch size for full-text result batching.
///
/// Smaller than the default [`super::common::BATCH_SIZE`] because full-text
/// results are ordered by relevance score, so smaller batches let downstream
/// operators begin processing sooner.
const BATCH_SIZE: usize = 100;

/// Full-text search scan operator.
///
/// Executes a full-text search query against a FullText index and returns
/// matching records ordered by relevance score.
#[derive(Debug)]
pub struct FullTextScan {
	/// Reference to the index definition
	pub index_ref: IndexRef,
	/// The search query string
	pub query: String,
	/// The MATCHES operator configuration (reference number, scoring)
	pub operator: MatchesOperator,
	/// Table name for record fetching
	pub table_name: crate::val::TableName,
	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// Plan-time resolved table context. When present, `execute()` skips
	/// runtime table def + permission lookup.
	pub(crate) resolved: Option<ResolvedTableContext>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl FullTextScan {
	pub(crate) fn new(
		index_ref: IndexRef,
		query: String,
		operator: MatchesOperator,
		table_name: crate::val::TableName,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			index_ref,
			query,
			operator,
			table_name,
			version,
			resolved: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for FullTextScan {
	fn name(&self) -> &'static str {
		"FullTextScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("index".to_string(), self.index_ref.name.clone()),
			("query".to_string(), self.query.clone()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let index_ref = self.index_ref.clone();
		let query = self.query.clone();
		let operator = self.operator.clone();
		let table_name = self.table_name.clone();
		let version_expr = self.version.clone();
		let resolved = self.resolved.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			// Get namespace and database IDs
			let db_ctx = ctx.database().context("FullTextScan requires database context")?;
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let txn = ctx.txn();

			// Evaluate VERSION expression
			let version: Option<u64> = match &version_expr {
				Some(expr) => {
					let eval_ctx = crate::exec::EvalContext::from_exec_ctx(&ctx);
					let v = expr.evaluate(eval_ctx).await?;
					Some(
						v.cast_to::<crate::val::Datetime>()
							.map_err(|e| anyhow::anyhow!("{e}"))?
							.to_version_stamp()?,
					)
				}
				None => None,
			};

			// Get the FrozenContext and Options from the root context
			let root = ctx.root();
			let frozen_ctx = &root.ctx;
			let opt = root.options.as_ref().context("FullTextScan requires Options context")?;

			// Resolve table permissions: plan-time fast path or runtime fallback
			let select_permission = if let Some(ref res) = resolved {
				res.select_permission(check_perms)
			} else if check_perms {
				let table_def = db_ctx
					.get_table_def(&table_name)
					.await
					.context("Failed to get table")?;

				if let Some(def) = &table_def {
					convert_permission_to_physical(&def.permissions.select, ctx.ctx()).await
						.context("Failed to convert permission")?
				} else {
					Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
						name: table_name.clone(),
					})))?
				}
			} else {
				PhysicalPermission::Allow
			};

			// Early exit if denied
			if matches!(select_permission, PhysicalPermission::Deny) {
				return;
			}

			// Get the FullText index parameters from the index definition
			let index_def = index_ref.definition();
			let ft_params = match &index_def.index {
				Index::FullText(params) => params,
				_ => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"Index '{}' is not a full-text index",
						index_def.name
					)))?
				}
			};

			// Create the index key base
			let ikb = IndexKeyBase::new(ns.namespace_id, db.database_id, table_name.clone(), index_def.index_id);

			// Open the full-text index
			let fti = FullTextIndex::new(
				frozen_ctx.get_index_stores(),
				txn.as_ref(),
				ikb,
				ft_params,
			)
			.await
			.context("Failed to open full-text index")?;

			// Extract query terms using TreeStack for stack management
			let query_terms = {
				let mut stack = TreeStack::new();
				stack
					.enter(|stk| fti.extract_querying_terms(stk, frozen_ctx, opt, query.clone()))
					.finish()
					.await
					.context("Failed to extract query terms")?
			};

			// If query terms are empty, no results
			if query_terms.is_empty() {
				return;
			}

			// Get the boolean operator from the MATCHES operator
			let bool_op = operator.operator;

			// Create hits iterator
			let hits_iter = match fti.new_hits_iterator(&query_terms, bool_op) {
				Some(iter) => iter,
				None => {
					// No matching documents
					return;
				}
			};

			// Iterate over hits, collecting record IDs into batches for
			// batch-fetching via getm_records.
			let mut hits_iter = hits_iter;
			let mut rid_batch = Vec::with_capacity(BATCH_SIZE);

			loop {
				// Collect up to BATCH_SIZE record IDs from the hits iterator
				let hit = hits_iter.next(txn.as_ref()).await
					.context("Failed to get next hit")?;

				match hit {
					Some((rid, _doc_id)) => {
						rid_batch.push(rid);

						if rid_batch.len() >= BATCH_SIZE {
							let values = fetch_and_filter_records_batch(
								&ctx,
								&txn,
								ns.namespace_id,
								db.database_id,
								&rid_batch,
								&select_permission,
								check_perms,
								version,
								CachePolicy::ReadOnly,
							).await?;
							if !values.is_empty() {
								yield ValueBatch { values };
							}
							rid_batch.clear();
						}
					}
					None => {
						// No more hits
						break;
					}
				}
			}

			// Yield any remaining records
			if !rid_batch.is_empty() {
				let values = fetch_and_filter_records_batch(
					&ctx,
					&txn,
					ns.namespace_id,
					db.database_id,
					&rid_batch,
					&select_permission,
					check_perms,
					version,
					CachePolicy::ReadOnly,
				).await?;
				if !values.is_empty() {
					yield ValueBatch { values };
				}
			}
		};

		Ok(monitor_stream(Box::pin(stream), "FullTextScan", &self.metrics))
	}
}
