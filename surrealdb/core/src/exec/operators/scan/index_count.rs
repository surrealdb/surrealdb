//! IndexCountScan operator - optimized COUNT() using index count metadata.
//!
//! When a query is `SELECT count() FROM <table> WHERE <cond> GROUP ALL` and a
//! COUNT index exists whose stored condition matches the WHERE clause exactly,
//! this operator replaces the full Scan -> Filter -> Aggregate pipeline.
//!
//! Instead of deserializing and filtering every record, it sums the delta
//! entries stored in `IndexCountKey` for the matching COUNT index.  This is
//! O(index entries) with no record I/O.
//!
//! The planner emits this operator when:
//! - Fields are count-all-only
//! - GROUP ALL is present
//! - A WHERE clause is present
//! - No SPLIT, ORDER BY, FETCH, or OMIT clauses
//! - A single table source
//!
//! At execution time the operator:
//! 1. Resolves the table and looks up its indexes.
//! 2. Finds a `Index::Count(cond)` whose condition equals the query's WHERE.
//! 3. Scans `IndexCountKey` deltas to compute the total count.
//! 4. Falls back to a full scan + filter + count if no matching COUNT index is found or permissions
//!    are conditional.

use std::sync::Arc;

use async_trait::async_trait;
use tracing::instrument;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, Index, NamespaceId, Permission};
use crate::err::Error;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::cond::Cond;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::key::index::iu::IndexCountKey;
use crate::key::record;
use crate::kvs::KVValue;
use crate::val::{Number, Object, TableName, Value};

/// Optimized operator for `SELECT count() FROM <table> WHERE <cond> GROUP ALL`
/// when a matching COUNT index exists.
///
/// Falls back to full scan + filter + count if no matching COUNT index is found
/// at execution time.
///
/// NOTE: This operator is fully implemented but not yet auto-detected by the
/// planner because the planner cannot verify COUNT index existence at plan time.
/// It will be wired in once plan-time catalog access is available.
#[allow(dead_code)] // Ready for use when plan-time index detection is added.
#[derive(Debug, Clone)]
pub struct IndexCountScan {
	/// Expression that evaluates to the table name.
	pub(crate) source: Arc<dyn PhysicalExpr>,
	/// The physical expression for the WHERE predicate (used for fallback).
	pub(crate) predicate: Arc<dyn PhysicalExpr>,
	/// The AST-level WHERE condition for exact matching against COUNT index
	/// conditions.
	pub(crate) condition: Cond,
	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<u64>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl IndexCountScan {
	/// Create a new IndexCountScan operator.
	#[allow(dead_code)]
	pub(crate) fn new(
		source: Arc<dyn PhysicalExpr>,
		predicate: Arc<dyn PhysicalExpr>,
		condition: Cond,
		version: Option<u64>,
	) -> Self {
		Self {
			source,
			predicate,
			condition,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for IndexCountScan {
	fn name(&self) -> &'static str {
		"IndexCountScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("source".to_string(), self.source.to_sql()),
			("condition".to_string(), self.predicate.to_sql()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		// IndexCountScan needs database context, combined with expression contexts
		self.source
			.required_context()
			.max(self.predicate.required_context())
			.max(ContextLevel::Database)
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("source", &self.source), ("predicate", &self.predicate)]
	}

	fn access_mode(&self) -> AccessMode {
		self.source.access_mode().combine(self.predicate.access_mode())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	#[instrument(name = "IndexCountScan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		validate_record_user_access(&db_ctx)?;
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		let source_expr = Arc::clone(&self.source);
		let predicate_expr = Arc::clone(&self.predicate);
		let condition = self.condition.clone();
		let version = self.version;
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database().context("IndexCountScan requires database context")?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate source expression to get the table name.
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let table_value = source_expr.evaluate(eval_ctx).await?;

			let table_name = match table_value {
				Value::Table(t) => t,
				_ => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"IndexCountScan received a non-table source"
					)))?;
					unreachable!()
				}
			};

			// Verify table exists.
			let table_def = txn
				.get_tb_by_name(&ns.name, &db.name, &table_name)
				.await
				.context("Failed to get table")?;

			if table_def.is_none() {
				Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
					name: table_name.clone(),
				})))?;
			}

			// Resolve SELECT permission.
			let select_permission = if check_perms {
				let catalog_perm = match &table_def {
					Some(def) => def.permissions.select.clone(),
					None => Permission::None,
				};
				convert_permission_to_physical(&catalog_perm, ctx.ctx()).await
					.context("Failed to convert permission")?
			} else {
				PhysicalPermission::Allow
			};

			match select_permission {
				PhysicalPermission::Deny => {
					// Table is invisible.
					return;
				}
				PhysicalPermission::Conditional(_) => {
					// Per-record permissions: fall back to full scan + filter + count.
					let count = count_with_filter_fallback(
						&ctx,
						ns.namespace_id,
						db.database_id,
						&table_name,
						version,
						&select_permission,
						&predicate_expr,
					)
					.await?;
					yield make_count_batch(count);
					return;
				}
				PhysicalPermission::Allow => {
					// Proceed to look for a matching COUNT index.
				}
			}

			// Look up all indexes for the table and find a matching COUNT index.
			let indexes = txn
				.all_tb_indexes(ns.namespace_id, db.database_id, &table_name)
				.await
				.context("Failed to fetch table indexes")?;

			let matching_index = indexes.iter().find(|ix| {
				if let Index::Count(ref idx_cond) = ix.index {
					// The COUNT index condition must exactly match the WHERE clause.
					idx_cond.as_ref() == Some(&condition)
				} else {
					false
				}
			});

			if let Some(ix_def) = matching_index {
				// Fast path: sum delta counts from the COUNT index.
				let count = sum_index_count_deltas(
					&ctx,
					&txn,
					ns.namespace_id,
					db.database_id,
					&table_name,
					ix_def.index_id,
				)
				.await?;
				yield make_count_batch(count);
			} else {
				// No matching COUNT index found: fall back to full scan + filter + count.
				let perm = PhysicalPermission::Allow;
				let count = count_with_filter_fallback(
					&ctx,
					ns.namespace_id,
					db.database_id,
					&table_name,
					version,
					&perm,
					&predicate_expr,
				)
				.await?;
				yield make_count_batch(count);
			}
		};

		Ok(monitor_stream(Box::pin(stream), "IndexCountScan", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the single-row `{ "count": N }` batch.
fn make_count_batch(count: usize) -> ValueBatch {
	let mut obj = Object::default();
	obj.insert("count".to_string(), Value::Number(Number::Int(count as i64)));
	ValueBatch {
		values: vec![Value::Object(obj)],
	}
}

/// Sum the delta entries in `IndexCountKey` for a given COUNT index.
async fn sum_index_count_deltas(
	ctx: &ExecutionContext,
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	ix: crate::catalog::IndexId,
) -> Result<usize, ControlFlow> {
	use futures::StreamExt;

	let range =
		IndexCountKey::range(ns, db, tb, ix).context("Failed to compute index count key range")?;
	let key_stream = txn.stream_keys(
		range,
		None, // no version
		None, // no limit
		0,    // no skip
		crate::idx::planner::ScanDirection::Forward,
	);
	futures::pin_mut!(key_stream);

	let mut count: i64 = 0;
	while let Some(batch_result) = key_stream.next().await {
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(Error::QueryCancelled)));
		}
		let keys = batch_result.context("Failed to scan index count keys")?;
		for key in &keys {
			let iu = IndexCountKey::decode_key(key).context("Failed to decode index count key")?;
			if iu.pos {
				count += iu.count as i64;
			} else {
				count -= iu.count as i64;
			}
		}
	}
	Ok(count.max(0) as usize)
}

/// Fallback: scan all records, apply the predicate, and count matches.
///
/// Used when no matching COUNT index exists or when per-record permissions
/// require row-level evaluation.
async fn count_with_filter_fallback(
	ctx: &ExecutionContext,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table_name: &TableName,
	version: Option<u64>,
	permission: &PhysicalPermission,
	predicate: &Arc<dyn PhysicalExpr>,
) -> Result<usize, ControlFlow> {
	use futures::StreamExt;

	use crate::exec::permission::PhysicalPermission;

	let txn = ctx.txn();
	let beg = record::prefix(ns_id, db_id, table_name)?;
	let end = record::suffix(ns_id, db_id, table_name)?;

	let kv_stream = txn.stream_keys_vals(
		beg..end,
		version,
		None, // no limit
		0,    // no skip
		crate::idx::planner::ScanDirection::Forward,
		false, // no prefetch for count scans
	);
	futures::pin_mut!(kv_stream);

	let mut count = 0usize;
	while let Some(result) = kv_stream.next().await {
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(Error::QueryCancelled)));
		}
		let entries = result.context("Failed to scan record")?;
		for (key, val) in entries {
			let decoded_key = crate::key::record::RecordKey::decode_key(&key)
				.context("Failed to decode record key")?;
			let rid_val = crate::val::RecordId {
				table: decoded_key.tb.into_owned(),
				key: decoded_key.id,
			};
			let mut record = crate::catalog::Record::kv_decode_value(val)
				.context("Failed to deserialize record")?;
			record.data.def(&rid_val);
			let value = record.data;

			// Check per-record permission first.
			let perm_allowed = match permission {
				PhysicalPermission::Allow => true,
				PhysicalPermission::Deny => false,
				PhysicalPermission::Conditional(expr) => {
					let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(&value);
					expr.evaluate(eval_ctx).await.map(|v| v.is_truthy()).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}"))
					})?
				}
			};
			if !perm_allowed {
				continue;
			}

			// Apply the WHERE predicate.
			let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(&value);
			let matches =
				predicate.evaluate(eval_ctx).await.map(|v| v.is_truthy()).map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("Failed to evaluate predicate: {e}"))
				})?;
			if matches {
				count += 1;
			}
		}
	}

	Ok(count)
}
