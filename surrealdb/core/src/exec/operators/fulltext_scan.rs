//! Full-text search scan operator.
//!
//! This operator retrieves records using full-text search indexes,
//! supporting the MATCHES operator with BM25 or VS scoring.

use std::sync::Arc;

use async_trait::async_trait;
use reblessive::TreeStack;

use crate::catalog::Index;
use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::index::access_path::IndexRef;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms, validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, ValueBatch,
	ValueBatchStream,
};
use crate::expr::ControlFlow;
use crate::expr::operator::MatchesOperator;
use crate::iam::Action;
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::planner::iterators::MatchesHitsIterator;
use crate::val::{RecordId, Value};

/// Batch size for result batching.
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
}

#[async_trait]
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
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			// Get namespace and database IDs
			let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let txn = ctx.txn();

			// Get the FrozenContext and Options from the root context
			let root = ctx.root();
			let frozen_ctx = &root.ctx;
			let opt = root.options.as_ref().ok_or_else(|| {
				ControlFlow::Err(anyhow::anyhow!("FullTextScan requires Options context"))
			})?;

			// Resolve table permissions
			let select_permission = if check_perms {
				let table_def = txn
					.get_tb_by_name(&ns.name, &db.name, &table_name)
					.await
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get table: {e}")))?;

				if let Some(def) = &table_def {
					convert_permission_to_physical(&def.permissions.select, ctx.ctx())
						.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to convert permission: {e}")))?
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
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to open full-text index: {}", e)))?;

			// Extract query terms using TreeStack for stack management
			let query_terms = {
				let mut stack = TreeStack::new();
				stack
					.enter(|stk| fti.extract_querying_terms(stk, frozen_ctx, opt, query.clone()))
					.finish()
					.await
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to extract query terms: {}", e)))?
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

			// Iterate over hits and fetch records
			let mut hits_iter = hits_iter;
			let mut batch = Vec::with_capacity(BATCH_SIZE);

			loop {
				// Get the next hit
				let hit = hits_iter.next(txn.as_ref()).await
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get next hit: {}", e)))?;

				match hit {
					Some((rid, _doc_id)) => {
						// Fetch the record and check permission
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

							// Yield batch when full
							if batch.len() >= BATCH_SIZE {
								yield ValueBatch { values: std::mem::take(&mut batch) };
								batch.reserve(BATCH_SIZE);
							}
						}
					}
					None => {
						// No more hits
						break;
					}
				}
			}

			// Yield any remaining records
			if !batch.is_empty() {
				yield ValueBatch { values: batch };
			}
		};

		Ok(Box::pin(stream))
	}
}

/// Fetch a record by ID and apply permission filtering.
async fn fetch_and_filter_record(
	ctx: &ExecutionContext,
	txn: &crate::kvs::Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	rid: &RecordId,
	select_permission: &PhysicalPermission,
	check_perms: bool,
) -> Result<Option<Value>, ControlFlow> {
	// Fetch the record
	let record = txn
		.get_record(ns, db, &rid.table, &rid.key, None)
		.await
		.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get record: {e}")))?;

	// Check if record exists
	if record.data.as_ref().is_none() {
		return Ok(None);
	}

	// Inject the id field into the document
	let mut value = record.data.as_ref().clone();
	value.def(rid);

	// Check permission
	if check_perms {
		let allowed = check_permission_for_value(select_permission, &value, ctx)
			.await
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}")))?;

		if !allowed {
			return Ok(None);
		}
	}

	Ok(Some(value))
}
