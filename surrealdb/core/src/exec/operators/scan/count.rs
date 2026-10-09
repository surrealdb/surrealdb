//! CountScan operator - optimized COUNT() without materializing records.
//!
//! When a query is `SELECT count() FROM table GROUP ALL` (with no WHERE, SPLIT,
//! or meaningful ORDER BY), this operator replaces the full Scan -> Aggregate
//! pipeline.  Instead of streaming, decoding, and aggregating every record it
//! calls `txn.count(beg..end)` on the KV key range and emits the single row
//! the aggregation would have — see [`CountOutput`].
//!
//! The planner emits this operator only when it can statically determine that
//! the query is eligible.  Permissions are resolved at execution time:
//!
//! - **Allow** – proceed with the key-range count.
//! - **Deny**  – count nothing (the table is invisible), and report that count.
//! - **Conditional** – per-record evaluation is required, so the operator falls back to a full scan
//!   + count at runtime.

use std::borrow::Cow;
use std::ops::Bound;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use common::future::stream::{Yielder, try_async_stream};

use crate::catalog::{DatabaseId, NamespaceId};
#[cfg(test)]
use crate::catalog::{Error, Index};
#[cfg(test)]
use crate::err::EngineError;
use crate::exec::ValueBatch;
#[cfg(test)]
use crate::exec::operators::scan::index_count::sum_index_count_deltas;
#[cfg(test)]
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, should_check_perms,
	table_select_permission_physical_runtime, validate_record_user_access,
};
#[cfg(test)]
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatchStream, monitor_stream,
};
use crate::expr::ControlFlow;
#[cfg(test)]
use crate::expr::ControlFlowExt;
#[cfg(test)]
use crate::iam::Action;
use crate::key::RawRange;
#[cfg(test)]
use crate::key::schema::RecordKey;
use crate::key::schema::RecordPrefix;
#[cfg(test)]
use crate::key::{KVKeyDecode, KVValue};
#[cfg(test)]
use crate::val::RecordIdKey;
use crate::val::{Number, Object, RecordIdKeyRange, TableName, Value};

/// What a count scan answers with: the row the aggregation *and the projection
/// above it* would have produced, since a count scan stands in for both.
///
/// A shortcut decides which plan runs, never what the plan answers, so the
/// shape is settled once by the matcher that chose the shortcut and carried to
/// every place a count is emitted — including the one where the count is zero
/// because the table is invisible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CountOutput {
	/// `SELECT VALUE count() … GROUP ALL` — the count itself. `VALUE` asks for
	/// the value rather than for a row holding it, so no key is named and the
	/// alias a statement may write over it names nothing.
	Value,
	/// `SELECT count() [AS c] … GROUP ALL` — one key per counted field, in the
	/// order the statement wrote them, each holding the same count:
	/// `{ "count": N }`, `{ "c": N }`, `{ "a": N, "b": N }`.
	Fields(Vec<String>),
}

impl CountOutput {
	/// The single-row batch this shape answers `count` with.
	pub(crate) fn batch(&self, count: usize) -> ValueBatch {
		let count = Value::Number(Number::Int(count as i64));
		let value = match self {
			Self::Value => count,
			Self::Fields(names) => {
				let mut obj = Object::default();
				for name in names {
					obj.insert(name.clone(), count.clone());
				}
				Value::Object(obj)
			}
		};
		ValueBatch::new(vec![value])
	}

	#[cfg(test)]
	/// Whether the shape has somewhere to put the count. `VALUE` is that place
	/// itself; a field list a matcher accepted always names at least one field.
	pub(crate) fn is_well_formed(&self) -> bool {
		match self {
			Self::Value => true,
			Self::Fields(names) => !names.is_empty(),
		}
	}
}

#[cfg(test)]
/// Optimized operator for `SELECT count() FROM <table> GROUP ALL`.
///
/// Counts records by iterating KV keys (`txn.count()`) instead of
/// deserializing every record through the Scan -> Aggregate pipeline.
/// Emits a single `ValueBatch` holding the row the pipeline it replaces would
/// have answered with, as [`CountOutput`] describes it.
#[derive(Debug, Clone)]
pub struct CountScan {
	/// Expression that evaluates to the table name (or a record range).
	pub(crate) source: Arc<dyn PhysicalExpr>,
	/// Optional VERSION expression for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// The shape to answer in: a row of named counts, or the count itself.
	pub(crate) output: CountOutput,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl CountScan {
	#[cfg(test)]
	/// Create a new CountScan operator.
	pub(crate) fn new(
		source: Arc<dyn PhysicalExpr>,
		version: Option<Arc<dyn PhysicalExpr>>,
		output: CountOutput,
	) -> Self {
		debug_assert!(output.is_well_formed(), "CountScan requires at least one output field");
		Self {
			source,
			version,
			output,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
#[cfg(test)]
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

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(name = "CountScan::execute", level = "trace", skip_all)
	)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		validate_record_user_access(&db_ctx)?;
		// Refused here rather than in the stream, so the refusal surfaces when
		// the plan is executed.
		should_check_perms(&db_ctx, Action::View)?;

		let source_expr = Arc::clone(&self.source);
		let version = self.version.clone();
		let output = self.output.clone();
		let ctx = ctx.clone();

		let stream = try_async_stream(async move |mut yielder: Yielder<_>| {
			let txn = ctx.txn();

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

			let count = count_source(&ctx, table_value, version).await?;
			yielder.emit(output.batch(count)).await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "CountScan", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
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

#[cfg(test)]
/// The number of records `source` names that the caller may select, as
/// [`CountScan`] counts them: a table's or a record range's keys, a table's
/// unconditional `COUNT` index where there is one, and under a conditional
/// permission the records it admits. A table the caller may not see counts
/// none. Read as of `version`.
pub(crate) async fn count_source(
	ctx: &ExecutionContext,
	source: Value,
	version: Option<u64>,
) -> FlowResult<usize> {
	let db_ctx = ctx.database().context("CountScan requires database context")?;
	let check_perms = should_check_perms(db_ctx, Action::View)?;
	let txn = ctx.txn();
	let ns = Arc::clone(&db_ctx.ns_ctx.ns);
	let db = Arc::clone(&db_ctx.db);
	let table_value = source;
	let (table_name, rid) = match table_value {
		Value::Table(t) => (t, None),
		Value::RecordId(rid) => (rid.table.clone(), Some(rid)),
		// Non-table sources are not eligible for CountScan.
		_ => {
			Err(ControlFlow::Err(anyhow::anyhow!("CountScan received a non-table source")))?;
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

	// Fail closed: a lightweight relation stores no records, so every
	// key-space count below — point, range, table, or the conditional
	// permission fallback — would be wrong-zero. Statically-named
	// sources were declined at plan time; only runtime-resolved
	// sources reach this.
	if table_def
		.as_deref()
		.is_some_and(|def| crate::kvs::lightweight::lightweight_relation(&def.table_type).is_some())
	{
		Err(ControlFlow::Err(anyhow::anyhow!(
			"a LIGHTWEIGHT relation cannot be scanned by this execution path; \
				 reference the table statically so the record-less scan can serve it"
		)))?;
	}

	// Resolve SELECT permission.
	let select_permission = if check_perms {
		table_select_permission_physical_runtime(table_def.as_deref(), ctx)
			.await
			.context("Failed to convert permission")?
	} else {
		PhysicalPermission::Allow
	};

	match select_permission {
		PhysicalPermission::Deny => {
			// The table is invisible, so no key of it is counted. The
			// `GROUP ALL` this stands in for reports one row whatever
			// reaches it, and a caller who may read nothing counts
			// nothing — the answer an empty table gives, which is what
			// the caller is entitled to see.
			return Ok(0);
		}
		PhysicalPermission::Conditional(_) => {
			// Per-record permissions – fall back to a full scan + count.
			// This should not normally happen because the planner avoids
			// emitting CountScan for conditional permissions, but we handle
			// it defensively.
			let count = count_with_perm_fallback(
				ctx,
				ns.namespace_id,
				db.database_id,
				&table_name,
				rid.as_ref(),
				version,
				&select_permission,
			)
			.await?;
			return Ok(count);
		}
		PhysicalPermission::Allow => {
			// Proceed with the fast KV count path.
		}
	}

	// ── Fast path: count KV keys without deserializing ──────────
	let count = if let Some(ref rid) = rid {
		// Range source
		count_range(ns.namespace_id, db.database_id, &rid.table, &rid.key, &txn, version).await?
	} else {
		// Check for an unconditional COUNT index first (O(deltas) vs O(records))
		if let None = version
			&& let Some(indexes) = db_ctx.get_table_indexes(&table_name, version).await.ok()
			&& let Some(ix_def) = indexes.iter().find(|ix| matches!(&ix.index, Index::Count(None)))
		{
			sum_index_count_deltas(
				ctx,
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
	Ok(count)
}

#[cfg(test)]
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

			// Check per-record permission through the same helper the scan path
			// uses, so a count and the SELECT it counts agree on visibility.
			let allowed = check_permission_for_value(permission, &value, None, ctx)
				.await
				.map_err(ControlFlow::Err)?;
			if allowed {
				count += 1;
			}
		}
	}

	Ok(count)
}

#[cfg(test)]
/// Check if a single value passes the permission check.
async fn check_perm_value(
	ctx: &ExecutionContext,
	value: &Value,
	permission: &PhysicalPermission,
) -> Result<bool, ControlFlow> {
	check_permission_for_value(permission, value, None, ctx).await.map_err(ControlFlow::Err)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// A count scan stands in for the projection above the aggregation, so the
	/// row it emits is the projected one: a named row, or — under `VALUE` —
	/// the count with nothing wrapped around it.
	#[test]
	fn the_output_shape_decides_what_a_count_is_wrapped_in() {
		let batch = |output: CountOutput| output.batch(7).into_values();

		assert_eq!(batch(CountOutput::Value), vec![Value::from(7)]);
		assert_eq!(
			batch(CountOutput::Fields(vec!["count".to_string()])),
			vec![Value::Object(Object::from(map! { "count".to_string() => Value::from(7) }))]
		);
		assert_eq!(
			batch(CountOutput::Fields(vec!["a".to_string(), "b".to_string()])),
			vec![Value::Object(Object::from(map! {
				"a".to_string() => Value::from(7),
				"b".to_string() => Value::from(7),
			}))]
		);
	}

	/// Only a field list can be empty, and a matcher never builds one: the
	/// debug assertion in each scan's constructor rests on that.
	#[test]
	fn a_shape_with_nowhere_to_put_the_count_is_not_well_formed() {
		assert!(CountOutput::Value.is_well_formed());
		assert!(CountOutput::Fields(vec!["count".to_string()]).is_well_formed());
		assert!(!CountOutput::Fields(Vec::new()).is_well_formed());
	}
}
