//! Reference scanning operator for the streaming execution engine.
//!
//! This operator scans record references (the `<~` operator) to find records
//! that reference a given source record. Unlike graph edges which are explicit
//! relationships, references are field-level links tracked by the database.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecutionContext, FlowResult, OperatorPlan,
	PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::ControlFlow;
use crate::idx::planner::ScanDirection;
use crate::val::{RecordId, TableName, Value};

/// Batch size for collecting references before yielding.
const BATCH_SIZE: usize = 1000;

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

	/// The table that contains the referencing records (e.g., `post`)
	pub(crate) referencing_table: TableName,

	/// Optional: The specific field in the referencing table that holds the reference
	/// If None, scans all fields that reference the target
	pub(crate) referencing_field: Option<String>,
}

#[async_trait]
impl OperatorPlan for ReferenceScan {
	fn name(&self) -> &'static str {
		"ReferenceScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![
			("source".to_string(), self.source.to_sql()),
			("table".to_string(), self.referencing_table.as_str().to_string()),
		];
		if let Some(field) = &self.referencing_field {
			attrs.push(("field".to_string(), field.clone()));
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

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		let source_expr = Arc::clone(&self.source);
		let referencing_table = self.referencing_table.clone();
		let referencing_field = self.referencing_field.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let txn = Arc::clone(ctx.txn());
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate the source expression to get the target RecordId(s)
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let source_value = source_expr.evaluate(eval_ctx).await.map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!("Failed to evaluate source: {}", e))
			})?;

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

			let mut batch = Vec::with_capacity(BATCH_SIZE);

			// Scan references for each target record
			for rid in &target_rids {
				// Create the key range based on whether a specific field is specified
				let (beg, end) = if let Some(ref field) = referencing_field {
					// Scan references from a specific field
					let beg = crate::key::r#ref::ffprefix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
						&referencing_table,
						field,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix: {}", e)))?;

					let end = crate::key::r#ref::ffsuffix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
						&referencing_table,
						field,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix: {}", e)))?;

					(beg, end)
				} else {
					// Scan all references from the referencing table
					let beg = crate::key::r#ref::ftprefix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
						&referencing_table,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix: {}", e)))?;

					let end = crate::key::r#ref::ftsuffix(
						ns.namespace_id,
						db.database_id,
						&rid.table,
						&rid.key,
						&referencing_table,
					).map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix: {}", e)))?;

					(beg, end)
				};

				// Stream the keys
				let kv_stream = txn.stream_keys(beg..end, None, None, ScanDirection::Forward);
				futures::pin_mut!(kv_stream);

				while let Some(result) = kv_stream.next().await {
					let key = result.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to scan reference: {}", e))
					})?;

					// Decode the reference key to get the referencing record ID
					let decoded = crate::key::r#ref::Ref::decode_key(&key)
						.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to decode ref key: {}", e)))?;

					// The referencing record ID (fk = foreign key, ft = foreign table)
					let referencing_rid = RecordId {
						table: decoded.ft.into_owned(),
						key: decoded.fk.into_owned(),
					};

					batch.push(Value::RecordId(referencing_rid));

					if batch.len() >= BATCH_SIZE {
						yield ValueBatch { values: std::mem::take(&mut batch) };
						batch.reserve(BATCH_SIZE);
					}
				}
			}

			// Yield remaining batch
			if !batch.is_empty() {
				yield ValueBatch { values: batch };
			}
		};

		Ok(Box::pin(stream))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_reference_scan_attrs() {
		use crate::exec::physical_expr::Literal;

		let scan = ReferenceScan {
			source: Arc::new(Literal(Value::RecordId(RecordId {
				table: "person".into(),
				key: RecordIdKey::String("alice".to_string()),
			}))),
			referencing_table: "post".into(),
			referencing_field: Some("author".to_string()),
		};

		assert_eq!(scan.name(), "ReferenceScan");
		let attrs = scan.attrs();
		assert!(attrs.iter().any(|(k, v)| k == "table" && v == "post"));
		assert!(attrs.iter().any(|(k, v)| k == "field" && v == "author"));
	}
}
