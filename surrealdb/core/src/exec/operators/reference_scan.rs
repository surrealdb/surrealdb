//! Reference scanning operator for the streaming execution engine.
//!
//! This operator scans record references (the `<~` operator) to find records
//! that reference a given source record. Unlike graph edges which are explicit
//! relationships, references are field-level links tracked by the database.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use super::common::{BATCH_SIZE, evaluate_bound_key, extract_record_ids, resolve_record_batch};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::exec::{
	AccessMode, ContextLevel, ControlFlowExt, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, TableName};

/// What kind of output the ReferenceScan should produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReferenceScanOutput {
	/// Return only the referencing record IDs
	#[default]
	RecordId,
	/// Return full referencing records (fetched from the datastore).
	/// Required when downstream operators need field access (e.g. Sort, Split).
	FullRecord,
}

/// Scans record references for a given target record.
///
/// This operator finds all records that reference the target record through
/// a specific field. It implements the `<~` (reference lookup) operator.
///
/// Example: For `person:alice<~post`, this finds all `post` records that
/// have a field referencing `person:alice`.
#[derive(Debug, Clone)]
pub struct ReferenceScan {
	/// Source expression that evaluates to the target RecordId(s) being referenced
	pub(crate) source: Arc<dyn PhysicalExpr>,

	/// The table that contains the referencing records (e.g., `post`).
	/// If None, scans ALL tables that reference the target (wildcard `<~?`).
	pub(crate) referencing_table: Option<TableName>,

	/// Optional: The specific field in the referencing table that holds the reference
	/// If None, scans all fields that reference the target
	pub(crate) referencing_field: Option<String>,

	/// What to output: RecordId or FullRecord
	pub(crate) output_mode: ReferenceScanOutput,

	/// Range start bound for the referencing record IDs.
	/// When `Unbounded`, starts from the field/table prefix.
	pub(crate) range_start: Bound<Arc<dyn PhysicalExpr>>,

	/// Range end bound for the referencing record IDs.
	/// When `Unbounded`, ends at the field/table suffix.
	pub(crate) range_end: Bound<Arc<dyn PhysicalExpr>>,

	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl ReferenceScan {
	pub(crate) fn new(
		source: Arc<dyn PhysicalExpr>,
		referencing_table: Option<TableName>,
		referencing_field: Option<String>,
		output_mode: ReferenceScanOutput,
		range_start: Bound<Arc<dyn PhysicalExpr>>,
		range_end: Bound<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			source,
			referencing_table,
			referencing_field,
			output_mode,
			range_start,
			range_end,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for ReferenceScan {
	fn name(&self) -> &'static str {
		"ReferenceScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![
			("source".to_string(), self.source.to_sql()),
			(
				"table".to_string(),
				self.referencing_table
					.as_ref()
					.map(|t| t.as_str().to_string())
					.unwrap_or_else(|| "?".to_string()),
			),
		];
		if let Some(field) = &self.referencing_field {
			attrs.push(("field".to_string(), field.clone()));
		}
		if self.output_mode == ReferenceScanOutput::FullRecord {
			attrs.push(("output".to_string(), "full_record".to_string()));
		}
		if !matches!(self.range_start, Bound::Unbounded)
			|| !matches!(self.range_end, Bound::Unbounded)
		{
			attrs.push(("range".to_string(), "bounded".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// ReferenceScan needs database context, combined with expression contexts
		self.source.required_context().max(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
		self.source.access_mode()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		let source_expr = Arc::clone(&self.source);
		let referencing_table = self.referencing_table.clone();
		let referencing_field = self.referencing_field.clone();
		let output_mode = self.output_mode;
		let range_start = self.range_start.clone();
		let range_end = self.range_end.clone();
		let ctx = ctx.clone();
		let fetch_full = output_mode == ReferenceScanOutput::FullRecord;

		let stream = async_stream::try_stream! {
			let txn = ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;

			// Evaluate the source expression to get the target RecordId(s)
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let source_value = source_expr.evaluate(eval_ctx).await?;
			let target_rids = extract_record_ids(source_value);

			if target_rids.is_empty() {
				return;
			}

			let mut rid_batch: Vec<RecordId> = Vec::with_capacity(BATCH_SIZE);

			// Scan references for each target record
			for rid in &target_rids {
				let (beg, end) = compute_ref_key_range(
					ns_id, db_id, rid,
					referencing_table.as_ref(),
					referencing_field.as_deref(),
					&range_start, &range_end,
					&ctx,
				).await?;

				let kv_stream = txn.stream_keys(beg..end, None, None, 0, ScanDirection::Forward);
				futures::pin_mut!(kv_stream);

				while let Some(result) = kv_stream.next().await {
					let keys = result.context("Failed to scan reference")?;

					for key in keys {
						let decoded = crate::key::r#ref::Ref::decode_key(&key)
							.context("Failed to decode ref key")?;

						rid_batch.push(RecordId {
							table: decoded.ft.into_owned(),
							key: decoded.fk.into_owned(),
						});

						if rid_batch.len() >= BATCH_SIZE {
							let values = resolve_record_batch(
								&txn, ns_id, db_id, &rid_batch, fetch_full,
							).await?;
							yield ValueBatch { values };
							rid_batch.clear();
						}
					}
				}
			}

			// Yield remaining batch
			if !rid_batch.is_empty() {
				let values = resolve_record_batch(
					&txn, ns_id, db_id, &rid_batch, fetch_full,
				).await?;
				yield ValueBatch { values };
			}
		};

		Ok(monitor_stream(Box::pin(stream), "ReferenceScan", &self.metrics))
	}
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Compute the KV key range `(beg, end)` for a reference scan.
///
/// Dispatches to the correct key prefix/suffix functions based on what
/// combination of table, field, and range bounds was supplied.
#[allow(clippy::too_many_arguments)]
async fn compute_ref_key_range(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	referencing_table: Option<&TableName>,
	referencing_field: Option<&str>,
	range_start: &Bound<Arc<dyn PhysicalExpr>>,
	range_end: &Bound<Arc<dyn PhysicalExpr>>,
	ctx: &ExecutionContext,
) -> Result<(Vec<u8>, Vec<u8>), ControlFlow> {
	let has_range =
		!matches!(range_start, Bound::Unbounded) || !matches!(range_end, Bound::Unbounded);

	if has_range {
		// Range-bounded scan requires both table and field
		let table = referencing_table
			.context("Range-bounded reference scans require a referencing table")?;
		let field = referencing_field
			.context("Range-bounded reference scans require a referencing field")?;

		let beg = eval_ref_bound(ns_id, db_id, rid, table, field, range_start, true, ctx).await?;
		let end = eval_ref_bound(ns_id, db_id, rid, table, field, range_end, false, ctx).await?;
		Ok((beg, end))
	} else if let Some(table) = referencing_table {
		if let Some(field) = referencing_field {
			// Field-specific scan
			let beg = crate::key::r#ref::ffprefix(ns_id, db_id, &rid.table, &rid.key, table, field)
				.context("Failed to create field prefix")?;
			let end = crate::key::r#ref::ffsuffix(ns_id, db_id, &rid.table, &rid.key, table, field)
				.context("Failed to create field suffix")?;
			Ok((beg, end))
		} else {
			// Table-only scan (all fields)
			let beg = crate::key::r#ref::ftprefix(ns_id, db_id, &rid.table, &rid.key, table)
				.context("Failed to create table prefix")?;
			let end = crate::key::r#ref::ftsuffix(ns_id, db_id, &rid.table, &rid.key, table)
				.context("Failed to create table suffix")?;
			Ok((beg, end))
		}
	} else {
		// Wildcard scan: all references for this record (any table, any field)
		let beg = crate::key::r#ref::prefix(ns_id, db_id, &rid.table, &rid.key)
			.context("Failed to create wildcard prefix")?;
		let end = crate::key::r#ref::suffix(ns_id, db_id, &rid.table, &rid.key)
			.context("Failed to create wildcard suffix")?;
		Ok((beg, end))
	}
}

/// Evaluate a single start or end bound of a reference key range.
///
/// `is_start` determines the fallback for `Unbounded` (prefix vs suffix) and
/// the semantics of `Included` / `Excluded` bounds:
///
/// | Bound     | start (`is_start=true`) | end (`is_start=false`)    |
/// |-----------|-------------------------|---------------------------|
/// | Unbounded | `ffprefix`              | `ffsuffix`                |
/// | Included  | `refprefix` (from key)  | `refsuffix` (through key) |
/// | Excluded  | `refsuffix` (past key)  | `refprefix` (before key)  |
#[allow(clippy::too_many_arguments)]
async fn eval_ref_bound(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	rid: &RecordId,
	table: &TableName,
	field: &str,
	bound: &Bound<Arc<dyn PhysicalExpr>>,
	is_start: bool,
	ctx: &ExecutionContext,
) -> Result<Vec<u8>, ControlFlow> {
	match bound {
		Bound::Unbounded => {
			if is_start {
				crate::key::r#ref::ffprefix(ns_id, db_id, &rid.table, &rid.key, table, field)
					.context("Failed to create field prefix")
			} else {
				crate::key::r#ref::ffsuffix(ns_id, db_id, &rid.table, &rid.key, table, field)
					.context("Failed to create field suffix")
			}
		}
		Bound::Included(expr) => {
			let fk = evaluate_bound_key(expr, ctx).await?;
			// Included start → refprefix (begin at key)
			// Included end   → refsuffix (include key)
			if is_start {
				crate::key::r#ref::refprefix(ns_id, db_id, &rid.table, &rid.key, table, field, &fk)
					.context("Failed to create range key")
			} else {
				crate::key::r#ref::refsuffix(ns_id, db_id, &rid.table, &rid.key, table, field, &fk)
					.context("Failed to create range key")
			}
		}
		Bound::Excluded(expr) => {
			let fk = evaluate_bound_key(expr, ctx).await?;
			// Excluded start → refsuffix (skip past key)
			// Excluded end   → refprefix (stop before key)
			if is_start {
				crate::key::r#ref::refsuffix(ns_id, db_id, &rid.table, &rid.key, table, field, &fk)
					.context("Failed to create range key")
			} else {
				crate::key::r#ref::refprefix(ns_id, db_id, &rid.table, &rid.key, table, field, &fk)
					.context("Failed to create range key")
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::{RecordIdKey, Value};

	#[test]
	fn test_reference_scan_attrs() {
		use crate::exec::physical_expr::Literal;

		let scan = ReferenceScan::new(
			Arc::new(Literal(Value::RecordId(RecordId {
				table: "person".into(),
				key: RecordIdKey::String("alice".to_string()),
			}))),
			Some("post".into()),
			Some("author".to_string()),
			ReferenceScanOutput::RecordId,
			Bound::Unbounded,
			Bound::Unbounded,
		);

		assert_eq!(scan.name(), "ReferenceScan");
		let attrs = scan.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "table" && v == "post"));
		assert!(attrs.iter().any(|(k, v)| k == "field" && v == "author"));
	}
}
