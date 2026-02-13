//! KNN scan operator for HNSW index-backed vector search.
//!
//! This operator performs approximate nearest-neighbor search using an HNSW
//! index. It retrieves the top-K records closest to a query vector, ordered
//! by distance (nearest first).

use std::sync::Arc;

use async_trait::async_trait;
use reblessive::TreeStack;

use super::common::fetch_and_filter_record;
use crate::catalog::Index;
use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::index::access_path::IndexRef;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, OperatorMetrics,
	ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::idx::planner::checker::HnswConditionChecker;
use crate::val::Number;

/// Batch size for KNN result batching.
///
/// KNN results are bounded by `k` (typically small, e.g. 10-100), so this
/// is mainly a safety bound. Most KNN queries will emit a single batch.
const BATCH_SIZE: usize = 100;

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
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl KnnScan {
	pub(crate) fn new(
		index_ref: IndexRef,
		vector: Vec<Number>,
		k: u32,
		ef: u32,
		table_name: crate::val::TableName,
	) -> Self {
		Self {
			index_ref,
			vector,
			k,
			ef,
			table_name,
			metrics: Arc::new(OperatorMetrics::new()),
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
		ContextLevel::Root
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
		let vector = self.vector.clone();
		let k = self.k;
		let ef = self.ef;
		let table_name = self.table_name.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			// Get namespace and database context
			let db_ctx = ctx.database().context("KnnScan requires database context")?;
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let txn = ctx.txn();

			// Get the FrozenContext from the root context
			let root = ctx.root();
			let frozen_ctx = &root.ctx;

			// Resolve table permissions
			let select_permission = if check_perms {
				let table_def = txn
					.get_tb_by_name(&ns.name, &db.name, &table_name)
					.await
					.context("Failed to get table")?;

				if let Some(def) = &table_def {
					convert_permission_to_physical(&def.permissions.select, ctx.ctx())
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

			// Get the HNSW parameters from the index definition
			let index_def = index_ref.definition();
			let hnsw_params = match &index_def.index {
				Index::Hnsw(params) => params.clone(),
				_ => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"Index '{}' is not an HNSW index",
						index_def.name
					)))?;
					unreachable!()
				}
			};

			// Look up the table definition to get the table_id
			let table_def = txn
				.get_tb_by_name(&ns.name, &db.name, &table_name)
				.await
				.context("Failed to get table definition")?;

			let table_def = table_def.ok_or_else(|| {
				ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
					name: table_name.clone(),
				}))
			})?;

			// Obtain the shared HNSW index
			let hnsw_index = frozen_ctx
				.get_index_stores()
				.get_index_hnsw(
					ns.namespace_id,
					db.database_id,
					frozen_ctx,
					table_def.table_id,
					index_def,
					&hnsw_params,
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

			// Build condition checker (no WHERE pushdown -- downstream Filter handles it)
			let cond_checker = HnswConditionChecker::new();

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

			// Convert results to records and yield as batches
			let mut batch = Vec::with_capacity(BATCH_SIZE.min(knn_results.len()));

			for (rid, _distance, _cached_record) in knn_results {
				// Fetch the full record (ignoring any cached record from the checker
				// since we need to apply permissions consistently)
				if let Some(value) = fetch_and_filter_record(
					&ctx,
					&txn,
					ns.namespace_id,
					db.database_id,
					&rid,
					&select_permission,
					check_perms,
				).await? {
					batch.push(value);

					if batch.len() >= BATCH_SIZE {
						yield ValueBatch { values: std::mem::take(&mut batch) };
						batch.reserve(BATCH_SIZE);
					}
				}
			}

			// Yield any remaining records
			if !batch.is_empty() {
				yield ValueBatch { values: batch };
			}
		};

		Ok(monitor_stream(Box::pin(stream), "KnnScan", &self.metrics))
	}
}
