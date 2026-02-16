//! Index scan operator for B-tree index access.
//!
//! This operator retrieves records using B-tree index structures (Idx and Uniq),
//! supporting equality lookups, range scans, and union operations.

use std::sync::Arc;

use async_trait::async_trait;
use surrealdb_types::ToSql;

use super::common::fetch_and_filter_records_batch;
use super::pipeline::eval_limit_expr;
use crate::err::Error;
use crate::exec::index::access_path::{BTreeAccess, IndexRef};
use crate::exec::index::iterator::{
	CompoundEqualIterator, CompoundRangeIterator, IndexEqualIterator, IndexRangeIterator,
	UniqueEqualIterator, UniqueRangeIterator,
};
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ControlFlowExt, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::idx::planner::ScanDirection;
use crate::kvs::CachePolicy;

/// Index scan operator for B-tree indexes (Idx and Uniq).
///
/// Retrieves records using an index access path, then fetches the full
/// record data and applies permission filtering.
///
/// When `limit` and/or `start` are provided (pushed down from the planner),
/// the operator stops iteration early once the limit is reached, avoiding
/// unnecessary index and record reads.
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
	/// Pushed-down LIMIT expression (evaluated at execution time).
	pub(crate) limit: Option<Arc<dyn PhysicalExpr>>,
	/// Pushed-down START expression (evaluated at execution time).
	pub(crate) start: Option<Arc<dyn PhysicalExpr>>,
	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl IndexScan {
	pub(crate) fn new(
		index_ref: IndexRef,
		access: BTreeAccess,
		direction: ScanDirection,
		table_name: crate::val::TableName,
		limit: Option<Arc<dyn PhysicalExpr>>,
		start: Option<Arc<dyn PhysicalExpr>>,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			index_ref,
			access,
			direction,
			table_name,
			limit,
			start,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
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
		let mut attrs = vec![
			("index".to_string(), self.index_ref.name.clone()),
			("access".to_string(), access_str),
			("direction".to_string(), format!("{:?}", self.direction)),
		];
		if let Some(ref limit) = self.limit {
			attrs.push(("limit".to_string(), limit.to_sql()));
		}
		if let Some(ref start) = self.start {
			attrs.push(("offset".to_string(), start.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = AccessMode::ReadOnly;
		if let Some(ref limit) = self.limit {
			mode = mode.combine(limit.access_mode());
		}
		if let Some(ref start) = self.start {
			mode = mode.combine(start.access_mode());
		}
		mode
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		use crate::exec::operators::SortDirection;
		use crate::exec::ordering::SortProperty;

		let dir = match self.direction {
			ScanDirection::Forward => SortDirection::Asc,
			ScanDirection::Backward => SortDirection::Desc,
		};
		let cols: Vec<SortProperty> = self
			.index_ref
			.definition()
			.cols
			.iter()
			.filter_map(|idiom| {
				crate::exec::field_path::FieldPath::try_from(idiom).ok().map(|path| SortProperty {
					path,
					direction: dir,
					collate: false,
					numeric: false,
				})
			})
			.collect();
		if cols.is_empty() {
			crate::exec::OutputOrdering::Unordered
		} else {
			crate::exec::OutputOrdering::Sorted(cols)
		}
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
		let table_name = self.table_name.clone();
		let limit_expr = self.limit.clone();
		let start_expr = self.start.clone();
		let version_expr = self.version.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database()?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let ns_id = ns.namespace_id;
			let db_id = db.database_id;

			// Evaluate pushed-down LIMIT and START expressions
			let limit_val: Option<usize> = match &limit_expr {
				Some(expr) => Some(eval_limit_expr(&**expr, &ctx).await?),
				None => None,
			};
			let start_val: usize = match &start_expr {
				Some(expr) => eval_limit_expr(&**expr, &ctx).await?,
				None => 0,
			};

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

			// Early exit if limit is 0
			if limit_val == Some(0) {
				return;
			}

			// Resolve table permissions
			let select_permission = if check_perms {
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

			// Create a ScanPipeline for limit/start tracking.
			// Permissions are already handled by fetch_and_filter_records_batch,
			// so we use Allow and an empty FieldState here — the pipeline is
			// only used for limit/start counting.
			let mut pipeline = super::pipeline::ScanPipeline::new(
				PhysicalPermission::Allow,
				None, // no predicate
				super::pipeline::FieldState::empty(),
				false, // permissions handled by fetch_and_filter_records_batch
				limit_val,
				start_val,
			);

			// Create the appropriate iterator based on access type and index uniqueness
			let is_unique = index_ref.is_unique();
			let ix = index_ref.definition();

			// Collect record IDs from index and batch-fetch full records
			match (&access, is_unique) {
				// Unique equality - at most one record
				(BTreeAccess::Equality(value), true) => {
					let mut iter = UniqueEqualIterator::new(ns_id, db_id, ix, value)
						.context("Failed to create iterator")?;

					let rids = iter.next_batch(&txn).await
						.context("Failed to iterate index")?;

					let mut values = fetch_and_filter_records_batch(
						&ctx, &txn, ns_id, db_id, &rids, &select_permission, check_perms, version,
						CachePolicy::ReadWrite,
					).await?;

					pipeline.process_batch(&mut values, &ctx).await?;

					if !values.is_empty() {
						yield ValueBatch { values };
					}
				}

				// Non-unique equality - multiple records possible
				(BTreeAccess::Equality(value), false) => {
					let mut iter = IndexEqualIterator::new(ns_id, db_id, ix, value)
						.context("Failed to create iterator")?;

					loop {
						let rids = iter.next_batch(&txn).await
							.context("Failed to iterate index")?;
						if rids.is_empty() {
							break;
						}

						let mut values = fetch_and_filter_records_batch(
							&ctx, &txn, ns_id, db_id, &rids, &select_permission, check_perms, version,
							CachePolicy::ReadOnly,
						).await?;

						let cont = pipeline.process_batch(&mut values, &ctx).await?;

						if !values.is_empty() {
							yield ValueBatch { values };
						}
						if !cont {
							break;
						}
					}
				}

				// Range scan (unique or non-unique).
				//
				// Both branches share the same batch-fetch-yield loop; they
				// differ only in iterator construction.  We keep them as two
				// explicit `loop` blocks rather than abstracting over the
				// iterator type because `async_stream` closures cannot
				// easily hold trait objects or generics.
				(BTreeAccess::Range { from, to }, true) => {
					let mut iter = UniqueRangeIterator::new(ns_id, db_id, ix, from.as_ref(), to.as_ref())
						.context("Failed to create iterator")?;
					loop {
						let rids = iter.next_batch(&txn).await
							.context("Failed to iterate index")?;
						if rids.is_empty() { break; }

						let mut values = fetch_and_filter_records_batch(
							&ctx, &txn, ns_id, db_id, &rids, &select_permission, check_perms, version,
							CachePolicy::ReadOnly,
						).await?;

						let cont = pipeline.process_batch(&mut values, &ctx).await?;

						if !values.is_empty() {
							yield ValueBatch { values };
						}
						if !cont {
							break;
						}
					}
				}

				(BTreeAccess::Range { from, to }, false) => {
					let mut iter = IndexRangeIterator::new(ns_id, db_id, ix, from.as_ref(), to.as_ref())
						.context("Failed to create iterator")?;
					loop {
						let rids = iter.next_batch(&txn).await
							.context("Failed to iterate index")?;
						if rids.is_empty() { break; }

						let mut values = fetch_and_filter_records_batch(
							&ctx, &txn, ns_id, db_id, &rids, &select_permission, check_perms, version,
							CachePolicy::ReadOnly,
						).await?;

						let cont = pipeline.process_batch(&mut values, &ctx).await?;

						if !values.is_empty() {
							yield ValueBatch { values };
						}
						if !cont {
							break;
						}
					}
				}

				// Compound index access — equality prefix only (no range)
				(BTreeAccess::Compound { prefix, range: None }, _) => {
					let mut iter = CompoundEqualIterator::new(ns_id, db_id, ix, prefix, None)
						.context("Failed to create compound iterator")?;

					// Compute the maximum number of index entries we need.
					// When a LIMIT + START is pushed down, we cap the scan.
					let mut remaining: u32 = match limit_val {
						Some(l) => (l + start_val) as u32,
						None => u32::MAX,
					};

					// Fetch the first batch of record IDs sequentially.
					let mut rids = iter.next_batch(&txn, remaining).await
						.context("Failed to iterate compound index")?;

					while !rids.is_empty() {
						remaining = remaining.saturating_sub(rids.len() as u32);

						// Overlap: fetch records for the current batch while
						// scanning the next batch of index entries concurrently.
						// This halves serial latency on TiKV.
						let (values_result, next_rids_result) = if remaining > 0 {
							let fetch_fut = fetch_and_filter_records_batch(
								&ctx, &txn, ns_id, db_id, &rids, &select_permission, check_perms, version,
								CachePolicy::ReadOnly,
							);
							let scan_fut = iter.next_batch(&txn, remaining);
							let (v, n) = futures::join!(fetch_fut, scan_fut);
							(v, Some(n))
						} else {
							// No more entries needed; skip the prefetch.
							let v = fetch_and_filter_records_batch(
								&ctx, &txn, ns_id, db_id, &rids, &select_permission, check_perms, version,
								CachePolicy::ReadOnly,
							).await;
							(v, None)
						};

						let mut values = values_result?;
						let cont = pipeline.process_batch(&mut values, &ctx).await?;

						if !values.is_empty() {
							yield ValueBatch { values };
						}
						if !cont || remaining == 0 {
							break;
						}

						rids = next_rids_result
							.expect("next_rids_result should be Some when remaining > 0")
							.context("Failed to iterate compound index")?;
					}
				}

				// Compound index access — equality prefix with range on next column
				(BTreeAccess::Compound { prefix, range: Some(range) }, _) => {
					let mut iter = CompoundRangeIterator::new(ns_id, db_id, ix, prefix, range)
						.context("Failed to create compound range iterator")?;

					let mut remaining: u32 = match limit_val {
						Some(l) => (l + start_val) as u32,
						None => u32::MAX,
					};

					// Fetch the first batch of record IDs sequentially.
					let mut rids = iter.next_batch(&txn, remaining).await
						.context("Failed to iterate compound index")?;

					while !rids.is_empty() {
						remaining = remaining.saturating_sub(rids.len() as u32);

						// Overlap: fetch records for the current batch while
						// scanning the next batch of index entries concurrently.
						let (values_result, next_rids_result) = if remaining > 0 {
							let fetch_fut = fetch_and_filter_records_batch(
								&ctx, &txn, ns_id, db_id, &rids, &select_permission, check_perms, version,
								CachePolicy::ReadOnly,
							);
							let scan_fut = iter.next_batch(&txn, remaining);
							let (v, n) = futures::join!(fetch_fut, scan_fut);
							(v, Some(n))
						} else {
							let v = fetch_and_filter_records_batch(
								&ctx, &txn, ns_id, db_id, &rids, &select_permission, check_perms, version,
								CachePolicy::ReadOnly,
							).await;
							(v, None)
						};

						let mut values = values_result?;
						let cont = pipeline.process_batch(&mut values, &ctx).await?;

						if !values.is_empty() {
							yield ValueBatch { values };
						}
						if !cont || remaining == 0 {
							break;
						}

						rids = next_rids_result
							.expect("next_rids_result should be Some when remaining > 0")
							.context("Failed to iterate compound index")?;
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

		Ok(monitor_stream(Box::pin(stream), "IndexScan", &self.metrics))
	}
}
