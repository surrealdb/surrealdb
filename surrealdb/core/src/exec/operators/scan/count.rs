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

use std::borrow::Cow;
use std::ops::Bound;
use std::sync::Arc;

use common::future::stream::{Yielder, try_async_stream};
use tracing::instrument;

use crate::catalog::{DatabaseId, Error, Index, NamespaceId, table_select_permission};
use crate::err::EngineError;
use crate::exec::operators::scan::index_count::sum_index_count_deltas;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical_runtime, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::iam::Action;
use crate::key::schema::{RecordKey, RecordPrefix};
use crate::key::{KVKeyDecode, KVValue, RawRange};
use crate::val::{Number, Object, RecordIdKey, RecordIdKeyRange, TableName, Value};

/// Optimized operator for `SELECT count() FROM <table> GROUP ALL`.
///
/// Counts records by iterating KV keys (`txn.count()`) instead of
/// deserializing every record through the Scan -> Aggregate pipeline.
/// Emits a single `ValueBatch` containing one field per count expression,
/// e.g. `{ "count": N }` or `{ "c": N }` when an alias is used.
#[derive(Debug, Clone)]
pub struct CountScan {
	/// Expression that evaluates to the table name (or a record range).
	pub(crate) source: Arc<dyn PhysicalExpr>,
	/// Optional VERSION expression for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// Output field names for the count result (one per SELECT field).
	/// For `SELECT count() as c FROM t GROUP ALL` this would be `["c"]`.
	/// For `SELECT count() FROM t GROUP ALL` this would be `["count"]`.
	pub(crate) field_names: Vec<String>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl CountScan {
	/// Create a new CountScan operator.
	pub(crate) fn new(
		source: Arc<dyn PhysicalExpr>,
		version: Option<Arc<dyn PhysicalExpr>>,
		field_names: Vec<String>,
	) -> Self {
		debug_assert!(!field_names.is_empty(), "CountScan requires at least one field name");
		Self {
			source,
			version,
			field_names,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
impl ExecOperator for CountScan {
	fn name(&self) -> &'static str {
		"CountScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("source".to_string(), self.source.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		// CountScan needs database context, combined with expression contexts
		let exprs_ctx = [Some(&self.source), self.version.as_ref()]
			.into_iter()
			.flatten()
			.map(|e| e.required_context())
			.max()
			.unwrap_or(ContextLevel::Root);
		exprs_ctx.max(ContextLevel::Database)
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		let mut exprs = vec![("source", &self.source)];
		if let Some(ref version) = self.version {
			exprs.push(("version", version));
		}
		exprs
	}

	fn access_mode(&self) -> AccessMode {
		// CountScan is read-only, but delegate to expressions
		// in case they contain subqueries with mutations.
		let version_mode =
			self.version.as_ref().map(|e| e.access_mode()).unwrap_or(AccessMode::ReadOnly);
		self.source.access_mode().combine(version_mode)
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	#[instrument(name = "CountScan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		validate_record_user_access(&db_ctx)?;
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		let source_expr = Arc::clone(&self.source);
		let version = self.version.clone();
		let field_names = self.field_names.clone();
		let ctx = ctx.clone();

		let stream = try_async_stream(async move |mut yielder: Yielder<_>| {
			let db_ctx = ctx.database().context("CountScan requires database context")?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate VERSION expression to a timestamp
			let version: Option<u64> = match &version {
				Some(expr) => {
					let eval_ctx = EvalContext::from_exec_ctx(&ctx);
					let v = expr.evaluate(eval_ctx).await?;
					Some(
						v.cast_to::<crate::val::Datetime>()
							.map_err(|e| anyhow::anyhow!("{e}"))?
							.to_version_stamp(txn.timestamp_impl().as_ref())?,
					)
				}
				None => ctx.version_stamp(),
			};

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
			let table_def =
				db_ctx.get_table_def(&table_name, version).await.context("Failed to get table")?;

			if table_def.is_none() {
				Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
					name: table_name.clone(),
				})))?;
			}

			// Resolve SELECT permission.
			let select_permission = if check_perms {
				let catalog_perm = table_select_permission(table_def.as_deref());
				convert_permission_to_physical_runtime(catalog_perm, ctx.ctx())
					.await
					.context("Failed to convert permission")?
			} else {
				PhysicalPermission::Allow
			};

			match select_permission {
				PhysicalPermission::Deny => {
					// Table is invisible – yield nothing (empty result → no GROUP ALL row).
					return Ok(());
				}
				PhysicalPermission::Conditional(_) => {
					// Per-record permissions – fall back to a full scan + count.
					// This should not normally happen because the planner avoids
					// emitting CountScan for conditional permissions, but we handle
					// it defensively.
					let count = count_with_perm_fallback(
						&ctx,
						ns.namespace_id,
						db.database_id,
						&table_name,
						rid.as_ref(),
						version,
						&select_permission,
					)
					.await?;
					yielder.emit(make_count_batch(count, &field_names)).await;
					return Ok(());
				}
				PhysicalPermission::Allow => {
					// Proceed with the fast KV count path.
				}
			}

			// ── Fast path: count KV keys without deserializing ──────────
			let count = if let Some(ref rid) = rid {
				// Range source
				count_range(ns.namespace_id, db.database_id, &rid.table, &rid.key, &txn, version)
					.await?
			} else {
				// Check for an unconditional COUNT index first (O(deltas) vs O(records))
				if let None = version
					&& let Some(indexes) = db_ctx.get_table_indexes(&table_name, version).await.ok()
					&& let Some(ix_def) =
						indexes.iter().find(|ix| matches!(&ix.index, Index::Count(None)))
				{
					sum_index_count_deltas(
						&ctx,
						&txn,
						ns.namespace_id,
						db.database_id,
						&table_name,
						ix_def.index_id,
					)
					.await?
				} else {
					// Fallback: iterate all KV keys
					let range = RecordPrefix {
						ns: ns.namespace_id,
						db: db.database_id,
						tb: Cow::Borrowed(&table_name),
					}
					.range()?;
					txn.count(range, version).await.context("Failed to count table records")?
				}
			};

			yielder.emit(make_count_batch(count, &field_names)).await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "CountScan", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the single-row batch that the Aggregate operator would normally
/// produce for `SELECT count() … GROUP ALL`.
///
/// Each entry in `field_names` becomes a key in the output object, all
/// mapping to the same count value. For example:
/// - `SELECT count() FROM t GROUP ALL`      → `{ "count": N }`
/// - `SELECT count() AS c FROM t GROUP ALL`  → `{ "c": N }`
/// - `SELECT count() AS a, count() AS b …`  → `{ "a": N, "b": N }`
fn make_count_batch(count: usize, field_names: &[String]) -> ValueBatch {
	let mut obj = Object::default();
	let count_val = Value::Number(Number::Int(count as i64));
	for name in field_names {
		obj.insert(name.clone(), count_val.clone());
	}
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
			let range = record_key_range(ns_id, db_id, table, range)?;
			txn.count(range, version).await.context("Failed to count range records")
		}
		_ => {
			// Single record ID: count is 0 or 1. Use a point lookup.
			let record_key = RecordKey {
				ns: ns_id,
				db: db_id,
				tb: Cow::Borrowed(table),
				id: Cow::Borrowed(key),
			};
			let exists = txn
				.exists_key(&record_key, version)
				.await
				.context("Failed to check record existence")?;
			Ok(usize::from(exists))
		}
	}
}

/// The record keys a record-id range covers.
pub(crate) fn record_key_range(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	range: &RecordIdKeyRange,
) -> Result<RawRange, ControlFlow> {
	let start = match &range.start {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(v) => Bound::Included(Cow::Borrowed(v)),
		Bound::Excluded(v) => Bound::Excluded(Cow::Borrowed(v)),
	};
	let end = match &range.end {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(v) => Bound::Included(Cow::Borrowed(v)),
		Bound::Excluded(v) => Bound::Excluded(Cow::Borrowed(v)),
	};
	Ok(RecordPrefix {
		ns: ns_id,
		db: db_id,
		tb: Cow::Borrowed(table),
	}
	.range_where((start, end))?)
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
	let txn = ctx.txn();

	// Determine key range
	let range = if let Some(rid) = rid {
		match &rid.key {
			RecordIdKey::Range(range) => record_key_range(ns_id, db_id, &rid.table, range)?,
			_ => {
				// Single record – do a point check with permission evaluation
				let Some(value) =
					crate::exec::operators::fetch::fetch_raw_record(ctx, rid, version).await?
				else {
					return Ok(0);
				};
				let allowed = check_perm_value(ctx, &value, permission).await?;
				return Ok(usize::from(allowed));
			}
		}
	} else {
		RecordPrefix {
			ns: ns_id,
			db: db_id,
			tb: Cow::Borrowed(table_name),
		}
		.range()?
	};

	// Walk the cursor batch-by-batch, decoding records inline from
	// borrowed bytes — no per-row `Vec<u8>` allocation.
	let mut cursor = txn
		.open_vals_cursor_raw(range, crate::kvs::Direction::Forward, 0, version)
		.await
		.context("Failed to open scan cursor")?;
	let mut count = 0usize;
	loop {
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}
		let batch = cursor
			.next_batch(crate::kvs::NORMAL_BATCH_SIZE)
			.await
			.context("Failed to scan record")?;
		if batch.is_empty() {
			break;
		}
		for (key, val) in &batch {
			let decoded_key = RecordKey::decode_key(key).context("Failed to decode record key")?;
			let rid_val = crate::val::RecordId {
				table: decoded_key.tb.into_owned(),
				key: decoded_key.id.into_owned(),
			};
			let record = crate::catalog::Record::kv_decode_value(val, rid_val)
				.context("Failed to deserialize record")?;
			let value = record.data;

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
