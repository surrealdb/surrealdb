//! Reference scanning operator for the streaming execution engine.
//!
//! This operator scans record references (the `<~` operator) to find records
//! that reference a given source record. Unlike graph edges which are explicit
//! relationships, references are field-level links tracked by the database.

use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::catalog::providers::TableProvider;
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Batch size for collecting references before yielding.
const BATCH_SIZE: usize = 1000;

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
		// Reference scanning requires database context
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		// Reference scan is read-only, but propagate source expression's access mode
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

		let stream = async_stream::try_stream! {
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate the source expression to get the target RecordId(s)
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let source_value = source_expr.evaluate(eval_ctx).await?;

			// Convert source value to a list of RecordIds
			let target_rids = match source_value {
				Value::RecordId(rid) => vec![rid],
				Value::Array(arr) => {
					let mut rids = Vec::with_capacity(arr.len());
					for v in arr.iter() {
						if let Value::RecordId(rid) = v {
							rids.push(rid.clone());
						}
					}
					rids
				}
				_ => vec![],
			};

			if target_rids.is_empty() {
				return;
			}

			// Check if we have range bounds -- ranges require a referencing_field
			let has_range = !matches!(range_start, Bound::Unbounded)
				|| !matches!(range_end, Bound::Unbounded);

			if has_range && referencing_field.is_none() {
				Err(ControlFlow::Err(anyhow::anyhow!(
					"Cannot scan a specific range of record references without a referencing field"
				)))?;
			}

			let mut batch = Vec::with_capacity(BATCH_SIZE);

			// Scan references for each target record
			for rid in &target_rids {
				let (beg, end) = if has_range {
					// Range-bounded scan: requires referencing_field and referencing_table
					let table = referencing_table.as_ref()
						.expect("Range-bounded reference scans require a referencing table");
					let field = referencing_field.as_deref()
						.expect("Range-bounded reference scans require a referencing field (validated above)");

					// Compute scan start key based on range start bound
					let beg = match &range_start {
						Bound::Unbounded => {
							crate::key::r#ref::ffprefix(
								ns.namespace_id,
								db.database_id,
								&rid.table,
								&rid.key,
								table,
								field,
							).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix: {}", e)))?
						}
						Bound::Included(expr) => {
							let bound_ctx = EvalContext::from_exec_ctx(&ctx);
							let val = expr.evaluate(bound_ctx).await?;
							let fk = value_to_record_id_key(val);
							crate::key::r#ref::refprefix(
								ns.namespace_id,
								db.database_id,
								&rid.table,
								&rid.key,
								table,
								field,
								&fk,
							).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create range start key: {}", e)))?
						}
						Bound::Excluded(expr) => {
							let bound_ctx = EvalContext::from_exec_ctx(&ctx);
							let val = expr.evaluate(bound_ctx).await?;
							let fk = value_to_record_id_key(val);
							crate::key::r#ref::refsuffix(
								ns.namespace_id,
								db.database_id,
								&rid.table,
								&rid.key,
								table,
								field,
								&fk,
							).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create range start key: {}", e)))?
						}
					};

					// Compute scan end key based on range end bound
					let end = match &range_end {
						Bound::Unbounded => {
							crate::key::r#ref::ffsuffix(
								ns.namespace_id,
								db.database_id,
								&rid.table,
								&rid.key,
								table,
								field,
							).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix: {}", e)))?
						}
						Bound::Excluded(expr) => {
							let bound_ctx = EvalContext::from_exec_ctx(&ctx);
							let val = expr.evaluate(bound_ctx).await?;
							let fk = value_to_record_id_key(val);
							crate::key::r#ref::refprefix(
								ns.namespace_id,
								db.database_id,
								&rid.table,
								&rid.key,
								table,
								field,
								&fk,
							).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create range end key: {}", e)))?
						}
						Bound::Included(expr) => {
							let bound_ctx = EvalContext::from_exec_ctx(&ctx);
							let val = expr.evaluate(bound_ctx).await?;
							let fk = value_to_record_id_key(val);
							crate::key::r#ref::refsuffix(
								ns.namespace_id,
								db.database_id,
								&rid.table,
								&rid.key,
								table,
								field,
								&fk,
							).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create range end key: {}", e)))?
						}
					};

					(beg, end)
				} else if referencing_table.is_none() {
					// Wildcard scan: scan ALL references for this record (any table, any field)
					// This implements the `<~?` syntax.
					let beg = crate::key::r#ref::prefix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix: {}", e)))?;

					let end = crate::key::r#ref::suffix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix: {}", e)))?;

					(beg, end)
				} else if let Some(ref field) = referencing_field {
					// Field-specific scan (no range bounds)
					let table = referencing_table.as_ref().expect("referencing_table required");
					let beg = crate::key::r#ref::ffprefix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
						table,
						field,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix: {}", e)))?;

					let end = crate::key::r#ref::ffsuffix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
						table,
						field,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix: {}", e)))?;

					(beg, end)
				} else {
					// Scan all references from a specific referencing table (all fields)
					let table = referencing_table.as_ref().expect("referencing_table required");
					let beg = crate::key::r#ref::ftprefix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
						table,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix: {}", e)))?;

					let end = crate::key::r#ref::ftsuffix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
						table,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix: {}", e)))?;

					(beg, end)
				};

				// Stream the keys
				let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
				futures::pin_mut!(kv_stream);

				while let Some(result) = kv_stream.next().await {
					let keys = result.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to scan reference: {}", e))
					})?;

					for key in keys {
						// Decode the reference key to get the referencing record ID
						let decoded = crate::key::r#ref::Ref::decode_key(&key)
							.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to decode ref key: {}", e)))?;

						// The referencing record ID (fk = foreign key, ft = foreign table)
						let referencing_rid = RecordId {
							table: decoded.ft.into_owned(),
							key: decoded.fk.into_owned(),
						};

						let value = match output_mode {
							ReferenceScanOutput::RecordId => Value::RecordId(referencing_rid),
							ReferenceScanOutput::FullRecord => {
								// Fetch the full record from the datastore
								let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
								let record = txn
									.get_record(
										db_ctx.ns_ctx.ns.namespace_id,
										db_ctx.db.database_id,
										&referencing_rid.table,
										&referencing_rid.key,
										None,
									)
									.await
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to fetch record: {}", e)))?;

								if record.data.as_ref().is_none() {
									Value::None
								} else {
									let mut v = record.data.as_ref().clone();
									v.def(&referencing_rid);
									v
								}
							}
						};

						batch.push(value);

						if batch.len() >= BATCH_SIZE {
							yield ValueBatch { values: std::mem::take(&mut batch) };
							batch.reserve(BATCH_SIZE);
						}
					}
				}
			}

			// Yield remaining batch
			if !batch.is_empty() {
				yield ValueBatch { values: batch };
			}
		};

		Ok(monitor_stream(Box::pin(stream), "ReferenceScan", &self.metrics))
	}
}

/// Convert a `Value` to a `RecordIdKey` for use in reference key range construction.
fn value_to_record_id_key(val: Value) -> RecordIdKey {
	match val {
		Value::Number(n) => RecordIdKey::Number(n.as_int()),
		Value::String(s) => RecordIdKey::String(s),
		Value::Uuid(u) => RecordIdKey::Uuid(u),
		Value::Array(a) => RecordIdKey::Array(a),
		Value::Object(o) => RecordIdKey::Object(o),
		// For other types, convert to string representation
		other => RecordIdKey::String(other.to_raw_string()),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::val::RecordIdKey;

	#[test]
	fn test_reference_scan_attrs() {
		use crate::exec::physical_expr::Literal;

		let scan = ReferenceScan {
			source: Arc::new(Literal(Value::RecordId(RecordId {
				table: "person".into(),
				key: RecordIdKey::String("alice".to_string()),
			}))),
			referencing_table: Some("post".into()),
			referencing_field: Some("author".to_string()),
			output_mode: ReferenceScanOutput::RecordId,
			range_start: Bound::Unbounded,
			range_end: Bound::Unbounded,
			metrics: Arc::new(crate::exec::OperatorMetrics::new()),
		};

		assert_eq!(scan.name(), "ReferenceScan");
		let attrs = scan.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "table" && v == "post"));
		assert!(attrs.iter().any(|(k, v)| k == "field" && v == "author"));
	}
}
