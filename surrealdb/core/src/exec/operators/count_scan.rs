//! CountScan operator - optimized COUNT() without materializing records.
//!
//! When a query is `SELECT count() FROM table GROUP ALL` (with no WHERE, SPLIT,
//! or meaningful ORDER BY), this operator replaces the full Scan -> Aggregate
//! pipeline.  Instead of streaming, decoding, and aggregating every record it
//! calls `txn.count(beg..end)` on the KV key range and emits a single
//! `{ "count": N }` row.
//!
//! The planner emits this operator only when it can statically determine that
//! the query is eligible.  Permissions are resolved at execution time:
//!
//! - **Allow** – proceed with the key-range count.
//! - **Deny**  – yield an empty stream (the table is invisible).
//! - **Conditional** – per-record evaluation is required, so the operator falls back to a full scan
//!   + count at runtime.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::instrument;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Permission};
use crate::err::Error;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::key::record;
use crate::kvs::{KVKey, KVValue};
use crate::val::{Number, Object, RecordIdKey, TableName, Value};

/// Optimized operator for `SELECT count() FROM <table> GROUP ALL`.
///
/// Counts records by iterating KV keys (`txn.count()`) instead of
/// deserializing every record through the Scan -> Aggregate pipeline.
/// Emits a single `ValueBatch` containing `{ "count": N }`.
#[derive(Debug, Clone)]
pub struct CountScan {
	/// Expression that evaluates to the table name (or a record range).
	pub(crate) source: Arc<dyn PhysicalExpr>,
	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<u64>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl CountScan {
	/// Create a new CountScan operator.
	pub(crate) fn new(source: Arc<dyn PhysicalExpr>, version: Option<u64>) -> Self {
		Self {
			source,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for CountScan {
	fn name(&self) -> &'static str {
		"CountScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("source".to_string(), self.source.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("source", &self.source)]
	}

	fn access_mode(&self) -> AccessMode {
		// CountScan is read-only, but delegate to the source expression
		// in case it contains a subquery with mutations.
		self.source.access_mode()
	}

	#[instrument(name = "CountScan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		validate_record_user_access(&db_ctx)?;
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		let source_expr = Arc::clone(&self.source);
		let version = self.version;
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database().context("CountScan requires database context")?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate the source expression to get the table name (or range).
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let table_value = source_expr.evaluate(eval_ctx).await?;

			let (table_name, rid) = match table_value {
				Value::Table(t) => (t, None),
				Value::RecordId(rid) => (rid.table.clone(), Some(rid)),
				// Non-table sources are not eligible for CountScan.
				_ => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"CountScan received a non-table source"
					)))?;
					unreachable!()
				}
			};

			// Verify that the table exists.
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
				convert_permission_to_physical(&catalog_perm, ctx.ctx())
					.context("Failed to convert permission")?
			} else {
				PhysicalPermission::Allow
			};

			match select_permission {
				PhysicalPermission::Deny => {
					// Table is invisible – yield nothing (empty result → no GROUP ALL row).
					return;
				}
				PhysicalPermission::Conditional(_) => {
					// Per-record permissions – fall back to a full scan + count.
					// This should not normally happen because the planner avoids
					// emitting CountScan for conditional permissions, but we handle
					// it defensively.
					let count = count_with_perm_fallback(
						&ctx, ns.namespace_id, db.database_id,
						&table_name, rid.as_ref(), version, &select_permission,
					).await?;
					yield make_count_batch(count);
					return;
				}
				PhysicalPermission::Allow => {
					// Proceed with the fast KV count path.
				}
			}

			// ── Fast path: count KV keys without deserializing ──────────
			let count = if let Some(ref rid) = rid {
				// Range source
				count_range(
					ns.namespace_id, db.database_id, &rid.table,
					&rid.key, &txn, version,
				).await?
			} else {
				// Full table
				let beg = record::prefix(ns.namespace_id, db.database_id, &table_name)?;
				let end = record::suffix(ns.namespace_id, db.database_id, &table_name)?;
				txn.count(beg..end, version).await
					.context("Failed to count table records")?
			};

			yield make_count_batch(count);
		};

		Ok(monitor_stream(Box::pin(stream), "CountScan", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the single-row `{ "count": N }` batch that the Aggregate operator
/// would normally produce for `SELECT count() … GROUP ALL`.
fn make_count_batch(count: usize) -> ValueBatch {
	let mut obj = Object::default();
	obj.insert("count".to_string(), Value::Number(Number::Int(count as i64)));
	ValueBatch {
		values: vec![Value::Object(obj)],
	}
}

/// Count records in a record-id range using `txn.count()`.
async fn count_range(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	key: &RecordIdKey,
	txn: &crate::kvs::Transaction,
	version: Option<u64>,
) -> Result<usize, ControlFlow> {
	match key {
		RecordIdKey::Range(range) => {
			let beg = range_start_key(ns_id, db_id, table, &range.start)?;
			let end = range_end_key(ns_id, db_id, table, &range.end)?;
			txn.count(beg..end, version).await.context("Failed to count range records")
		}
		_ => {
			// Single record ID: count is 0 or 1. Use a point lookup.
			let record_key = record::new(ns_id, db_id, table, key);
			let exists = txn
				.exists(&record_key, version)
				.await
				.context("Failed to check record existence")?;
			Ok(usize::from(exists))
		}
	}
}

/// Compute the start key for a range count (mirrors scan.rs helpers).
fn range_start_key(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	bound: &Bound<RecordIdKey>,
) -> Result<crate::kvs::Key, ControlFlow> {
	match bound {
		Bound::Unbounded => {
			record::prefix(ns_id, db_id, table).context("Failed to create prefix key")
		}
		Bound::Included(v) => {
			record::new(ns_id, db_id, table, v).encode_key().context("Failed to create begin key")
		}
		Bound::Excluded(v) => {
			let mut key = record::new(ns_id, db_id, table, v)
				.encode_key()
				.context("Failed to create begin key")?;
			key.push(0x00);
			Ok(key)
		}
	}
}

/// Compute the end key for a range count (mirrors scan.rs helpers).
fn range_end_key(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	bound: &Bound<RecordIdKey>,
) -> Result<crate::kvs::Key, ControlFlow> {
	match bound {
		Bound::Unbounded => {
			record::suffix(ns_id, db_id, table).context("Failed to create suffix key")
		}
		Bound::Excluded(v) => {
			record::new(ns_id, db_id, table, v).encode_key().context("Failed to create end key")
		}
		Bound::Included(v) => {
			let mut key = record::new(ns_id, db_id, table, v)
				.encode_key()
				.context("Failed to create end key")?;
			key.push(0x00);
			Ok(key)
		}
	}
}

/// Fallback: scan all records, checking per-record permissions, and count
/// those that pass.  Used when the table has conditional SELECT permissions.
async fn count_with_perm_fallback(
	ctx: &ExecutionContext,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table_name: &TableName,
	rid: Option<&crate::val::RecordId>,
	version: Option<u64>,
	permission: &PhysicalPermission,
) -> Result<usize, ControlFlow> {
	use futures::StreamExt;

	let txn = ctx.txn();

	// Determine key range
	let (beg, end) = if let Some(rid) = rid {
		match &rid.key {
			RecordIdKey::Range(range) => {
				let beg = range_start_key(ns_id, db_id, &rid.table, &range.start)?;
				let end = range_end_key(ns_id, db_id, &rid.table, &range.end)?;
				(beg, end)
			}
			_ => {
				// Single record – do a point check with permission evaluation
				let record = txn
					.get_record(ns_id, db_id, table_name, &rid.key, version)
					.await
					.context("Failed to get record")?;

				if record.data.as_ref().is_none() {
					return Ok(0);
				}

				let mut value = record.data.as_ref().clone();
				value.def(rid);
				let allowed = check_perm_value(ctx, &value, permission).await?;
				return Ok(usize::from(allowed));
			}
		}
	} else {
		let beg = record::prefix(ns_id, db_id, table_name)?;
		let end = record::suffix(ns_id, db_id, table_name)?;
		(beg, end)
	};

	// Stream keys+values to check permissions
	let kv_stream = txn.stream_keys_vals(
		beg..end,
		version,
		None, // no limit
		crate::idx::planner::ScanDirection::Forward,
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
			record.data.to_mut().def(&rid_val);
			let value = record.data.into_value();

			// Check per-record permission
			let allowed = match permission {
				PhysicalPermission::Allow => true,
				PhysicalPermission::Deny => false,
				PhysicalPermission::Conditional(expr) => {
					let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(&value);
					expr.evaluate(eval_ctx).await.map(|v| v.is_truthy()).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}"))
					})?
				}
			};
			if allowed {
				count += 1;
			}
		}
	}

	Ok(count)
}

/// Check if a single value passes the permission check.
async fn check_perm_value(
	ctx: &ExecutionContext,
	value: &Value,
	permission: &PhysicalPermission,
) -> Result<bool, ControlFlow> {
	match permission {
		PhysicalPermission::Allow => Ok(true),
		PhysicalPermission::Deny => Ok(false),
		PhysicalPermission::Conditional(expr) => {
			let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(value);
			expr.evaluate(eval_ctx)
				.await
				.map(|v| v.is_truthy())
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}")))
		}
	}
}
