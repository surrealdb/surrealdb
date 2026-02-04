//! Index scan operator for B-tree index access.
//!
//! This operator retrieves records using B-tree index structures (Idx and Uniq),
//! supporting equality lookups, range scans, and union operations.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::index::access_path::{BTreeAccess, IndexRef, RangeBound};
use crate::exec::index::iterator::{
	IndexEqualIterator, IndexRangeIterator, UniqueEqualIterator, UniqueRangeIterator,
};
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms, validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, ExecutionContext, FlowResult, ValueBatch,
	ValueBatchStream,
};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, Value};

/// Batch size for index scans.
const BATCH_SIZE: usize = 1000;

/// Index scan operator for B-tree indexes (Idx and Uniq).
///
/// Retrieves records using an index access path, then fetches the full
/// record data and applies permission filtering.
#[derive(Debug)]
pub struct IndexScan {
	/// Reference to the index definition
	pub index_ref: IndexRef,
	/// How to access the index
	pub access: BTreeAccess,
	/// Scan direction (forward or backward)
	pub direction: ScanDirection,
	/// Table name for record fetching
	pub table_name: crate::val::TableName,
}

#[async_trait]
impl ExecOperator for IndexScan {
	fn name(&self) -> &'static str {
		"IndexScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let access_str = match &self.access {
			BTreeAccess::Equality(v) => format!("= {}", v.to_sql()),
			BTreeAccess::Range {
				from,
				to,
			} => {
				let from_str = from
					.as_ref()
					.map(|r| {
						format!(
							"{}{}",
							if r.inclusive {
								">="
							} else {
								">"
							},
							r.value.to_sql()
						)
					})
					.unwrap_or_default();
				let to_str = to
					.as_ref()
					.map(|r| {
						format!(
							"{}{}",
							if r.inclusive {
								"<="
							} else {
								"<"
							},
							r.value.to_sql()
						)
					})
					.unwrap_or_default();
				format!("{} {}", from_str, to_str).trim().to_string()
			}
			BTreeAccess::Compound {
				prefix,
				range,
			} => {
				let prefix_str = prefix.iter().map(|v| v.to_sql()).collect::<Vec<_>>().join(", ");
				if let Some((op, val)) = range {
					format!("[{}] {:?} {}", prefix_str, op, val.to_sql())
				} else {
					format!("[{}]", prefix_str)
				}
			}
			// FullText and KNN should use dedicated operators
			BTreeAccess::FullText {
				..
			}
			| BTreeAccess::Knn {
				..
			} => {
				unreachable!("IndexScan does not support FullText or KNN access")
			}
		};
		vec![
			("index".to_string(), self.index_ref.name.clone()),
			("access".to_string(), access_str),
			("direction".to_string(), format!("{:?}", self.direction)),
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
		let access = self.access.clone();
		let direction = self.direction;
		let table_name = self.table_name.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
			let txn = Arc::clone(ctx.txn());
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

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

			// Create the appropriate iterator based on access type and index uniqueness
			let is_unique = index_ref.is_unique();
			let ix = index_ref.definition();

			// Collect record IDs from index and fetch full records
			let mut batch = Vec::with_capacity(BATCH_SIZE);

			match (&access, is_unique) {
				// Unique equality - at most one record
				(BTreeAccess::Equality(value), true) => {
					let mut iter = UniqueEqualIterator::new(ns.namespace_id, db.database_id, ix, value)
						.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create iterator: {e}")))?;

					let rids = iter.next_batch(&txn).await
						.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to iterate index: {e}")))?;

					for rid in rids {
						if let Some(value) = fetch_and_filter_record(&ctx, &txn, ns.namespace_id, db.database_id, &rid, &select_permission, check_perms).await? {
							batch.push(value);
						}
					}

					if !batch.is_empty() {
						yield ValueBatch { values: batch };
					}
				}

				// Non-unique equality - multiple records possible
				(BTreeAccess::Equality(value), false) => {
					let mut iter = IndexEqualIterator::new(ns.namespace_id, db.database_id, ix, value)
						.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create iterator: {e}")))?;

					loop {
						let rids = iter.next_batch(&txn).await
							.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to iterate index: {e}")))?;

						if rids.is_empty() {
							break;
						}

						for rid in rids {
							if let Some(value) = fetch_and_filter_record(&ctx, &txn, ns.namespace_id, db.database_id, &rid, &select_permission, check_perms).await? {
								batch.push(value);
								if batch.len() >= BATCH_SIZE {
									yield ValueBatch { values: std::mem::take(&mut batch) };
									batch.reserve(BATCH_SIZE);
								}
							}
						}
					}

					if !batch.is_empty() {
						yield ValueBatch { values: batch };
					}
				}

				// Range scan on unique index
				(BTreeAccess::Range { from, to }, true) => {
					let mut iter = UniqueRangeIterator::new(
						ns.namespace_id,
						db.database_id,
						ix,
						from.as_ref(),
						to.as_ref(),
					)
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create iterator: {e}")))?;

					loop {
						let rids = iter.next_batch(&txn).await
							.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to iterate index: {e}")))?;

						if rids.is_empty() {
							break;
						}

						for rid in rids {
							if let Some(value) = fetch_and_filter_record(&ctx, &txn, ns.namespace_id, db.database_id, &rid, &select_permission, check_perms).await? {
								batch.push(value);
								if batch.len() >= BATCH_SIZE {
									yield ValueBatch { values: std::mem::take(&mut batch) };
									batch.reserve(BATCH_SIZE);
								}
							}
						}
					}

					if !batch.is_empty() {
						yield ValueBatch { values: batch };
					}
				}

				// Range scan on non-unique index
				(BTreeAccess::Range { from, to }, false) => {
					let mut iter = IndexRangeIterator::new(
						ns.namespace_id,
						db.database_id,
						ix,
						from.as_ref(),
						to.as_ref(),
					)
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create iterator: {e}")))?;

					loop {
						let rids = iter.next_batch(&txn).await
							.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to iterate index: {e}")))?;

						if rids.is_empty() {
							break;
						}

						for rid in rids {
							if let Some(value) = fetch_and_filter_record(&ctx, &txn, ns.namespace_id, db.database_id, &rid, &select_permission, check_perms).await? {
								batch.push(value);
								if batch.len() >= BATCH_SIZE {
									yield ValueBatch { values: std::mem::take(&mut batch) };
									batch.reserve(BATCH_SIZE);
								}
							}
						}
					}

					if !batch.is_empty() {
						yield ValueBatch { values: batch };
					}
				}

				// Compound index access - for now, treat as range
				(BTreeAccess::Compound { prefix, range }, _) => {
					// TODO: Implement proper compound index iteration
					// For now, fall back to a simple range scan
					let (from, to) = if let Some((op, val)) = range {
						match op {
							crate::expr::BinaryOperator::MoreThan => (Some(RangeBound::exclusive(val.clone())), None),
							crate::expr::BinaryOperator::MoreThanEqual => (Some(RangeBound::inclusive(val.clone())), None),
							crate::expr::BinaryOperator::LessThan => (None, Some(RangeBound::exclusive(val.clone()))),
							crate::expr::BinaryOperator::LessThanEqual => (None, Some(RangeBound::inclusive(val.clone()))),
							_ => (None, None),
						}
					} else {
						(None, None)
					};

					let mut iter = IndexRangeIterator::new(
						ns.namespace_id,
						db.database_id,
						ix,
						from.as_ref(),
						to.as_ref(),
					)
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create iterator: {e}")))?;

					loop {
						let rids = iter.next_batch(&txn).await
							.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to iterate index: {e}")))?;

						if rids.is_empty() {
							break;
						}

						for rid in rids {
							if let Some(value) = fetch_and_filter_record(&ctx, &txn, ns.namespace_id, db.database_id, &rid, &select_permission, check_perms).await? {
								batch.push(value);
								if batch.len() >= BATCH_SIZE {
									yield ValueBatch { values: std::mem::take(&mut batch) };
									batch.reserve(BATCH_SIZE);
								}
							}
						}
					}

					if !batch.is_empty() {
						yield ValueBatch { values: batch };
					}
				}

				// FullText and KNN should use dedicated operators
				(BTreeAccess::FullText { .. }, _) | (BTreeAccess::Knn { .. }, _) => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"IndexScan does not support FullText or KNN access - use dedicated operators"
					)))?
				}
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
