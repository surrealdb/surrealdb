//! KNN scan operator for HNSW index-backed vector search.
//!
//! This operator performs approximate nearest-neighbor search using an HNSW
//! index. It retrieves the top-K records closest to a query vector, ordered
//! by distance (nearest first).

use std::sync::Arc;

use async_trait::async_trait;
use reblessive::TreeStack;

use super::common::fetch_and_filter_records_batch;
use crate::catalog::Index;
use crate::err::Error;
use crate::exec::index::access_path::IndexRef;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{Cond, ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::kvs::CachePolicy;
use crate::val::Number;

/// KNN scan operator using an HNSW index.
///
/// Executes an approximate nearest-neighbor search against an HNSW index
/// and returns the top-K matching records ordered by distance.
#[derive(Debug)]
pub struct KnnScan {
	/// Reference to the HNSW index definition
	pub index_ref: IndexRef,
	/// The query vector to search for nearest neighbors of
	pub vector: Vec<Number>,
	/// Number of nearest neighbors to return
	pub k: u32,
	/// HNSW search expansion factor
	pub ef: u32,
	/// Table name for record fetching
	pub table_name: crate::val::TableName,
	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
	/// KNN distance context, shared with IndexFunctionExec for vector::distance::knn().
	pub(crate) knn_context: Option<Arc<crate::exec::function::KnnContext>>,
	/// Residual WHERE condition (non-KNN predicates) to push down into HNSW
	/// search. When present, the HNSW search will only consider candidates
	/// that satisfy this condition, preventing non-matching rows from
	/// consuming top-K slots.
	pub(crate) residual_cond: Option<Cond>,
}

impl KnnScan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		index_ref: IndexRef,
		vector: Vec<Number>,
		k: u32,
		ef: u32,
		table_name: crate::val::TableName,
		version: Option<Arc<dyn PhysicalExpr>>,
		knn_context: Option<Arc<crate::exec::function::KnnContext>>,
		residual_cond: Option<Cond>,
	) -> Self {
		Self {
			index_ref,
			vector,
			k,
			ef,
			table_name,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
			knn_context,
			residual_cond,
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for KnnScan {
	fn name(&self) -> &'static str {
		"KnnScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("index".to_string(), self.index_ref.name.clone()),
			("k".to_string(), self.k.to_string()),
			("ef".to_string(), self.ef.to_string()),
			("dimension".to_string(), self.vector.len().to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::Bounded(self.k as usize)
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
		let vector = self.vector.clone();
		let k = self.k;
		let ef = self.ef;
		let table_name = self.table_name.clone();
		let version_expr = self.version.clone();
		let knn_context = self.knn_context.clone();
		let residual_cond = self.residual_cond.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			// Get namespace and database context
			let db_ctx = ctx.database().context("KnnScan requires database context")?;
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

			// Get the FrozenContext from the root context
			let root = ctx.root();
			let frozen_ctx = &root.ctx;

			// Look up the table definition (needed for both permissions and table_id)
			let table_def = db_ctx
				.get_table_def(&table_name)
				.await
				.context("Failed to get table")?;

			let table_def = match table_def {
				Some(def) => def,
				None => {
					Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
						name: table_name.clone(),
					})))?;
					unreachable!()
				}
			};

			// Resolve table permissions
			let select_permission = if check_perms {
				convert_permission_to_physical(&table_def.permissions.select, ctx.ctx()).await
					.context("Failed to convert permission")?
			} else {
				PhysicalPermission::Allow
			};

			// Early exit if denied
			if matches!(select_permission, PhysicalPermission::Deny) {
				return;
			}

			// Get the HNSW parameters from the index definition
			let index_def = index_ref.definition();
			let hnsw_params = match &index_def.index {
				Index::Hnsw(params) => params,
				_ => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"Index '{}' is not an HNSW index",
						index_def.name
					)))?;
					unreachable!()
				}
			};

			// Obtain the shared HNSW index
			let hnsw_index = frozen_ctx
				.get_index_stores()
				.get_index_hnsw(
					ns.namespace_id,
					db.database_id,
					frozen_ctx,
					table_def.table_id,
					index_def,
					hnsw_params,
				)
				.await
				.context("Failed to get HNSW index")?;

			// Ensure the HNSW index state is current
			hnsw_index
				.write()
				.await
				.check_state(frozen_ctx)
				.await
				.context("Failed to check HNSW index state")?;

			// Build condition checker. When there are residual (non-KNN) predicates
			// in the WHERE clause, push them into the HNSW search so that rows
			// not satisfying the condition do not consume top-K slots.
			let opt = ctx.options();
			let cond_checker = match (&residual_cond, opt) {
				(Some(cond), Some(opt)) => {
					let frozen = &root.ctx;
					HnswConditionChecker::new_cond(frozen, opt, Arc::new(cond.clone()))
				}
				_ => HnswConditionChecker::new(),
			};

			// Execute the KNN search using a TreeStack for recursion safety
			let knn_results = {
				let txn_for_search = txn.clone();
				let mut stack = TreeStack::new();
				stack
					.enter(|stk| {
						let hnsw_index = &hnsw_index;
						let db_def = &db;
						let vector = &vector;
						async move {
							hnsw_index
								.read()
								.await
								.knn_search(
									db_def,
									&txn_for_search,
									stk,
									vector,
									k as usize,
									ef as usize,
									cond_checker,
								)
								.await
						}
					})
					.finish()
					.await
					.context("HNSW KNN search failed")?
			};

			let mut rids = Vec::with_capacity(knn_results.len());
			// Populate KNN distance context (if present) before yielding records.
			// This makes distances available to vector::distance::knn() during
			// downstream projection evaluation.
			if let Some(ref knn_ctx) = knn_context {
				for (rid, distance, _) in &knn_results {
					knn_ctx.insert(rid.as_ref().clone(), Number::Float(*distance));
					rids.push(rid.as_ref().clone());
				}
			} else {
				for (rid, _, _) in &knn_results {
					rids.push(rid.as_ref().clone());
				}
			}


			// Batch-fetch all records and apply permission filtering
			let values = fetch_and_filter_records_batch(
				&ctx,
				&txn,
				ns.namespace_id,
				db.database_id,
				&rids,
				&select_permission,
				check_perms,
				version,
				CachePolicy::ReadWrite,
			).await?;

			if !values.is_empty() {
				yield ValueBatch { values };
			}
		};

		Ok(monitor_stream(Box::pin(stream), "KnnScan", &self.metrics))
	}
}
