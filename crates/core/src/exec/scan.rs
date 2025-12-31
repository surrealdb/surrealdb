use std::sync::Arc;

use futures::stream;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::err::Error;
use crate::exec::{
	ContextLevel, EvalContext, ExecutionContext, ExecutionPlan, PhysicalExpr, ValueBatchStream,
};
use crate::val::{TableName, Value};

/// Full table scan - iterates over all records in a table.
///
/// Requires database-level context since it reads from a specific table
/// in the selected namespace and database.
#[derive(Debug, Clone)]
pub struct Scan {
	pub(crate) table: Arc<dyn PhysicalExpr>,
	// fields: Vec<Field>,
}

impl ExecutionPlan for Scan {
	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		use crate::exec::ValueBatch;

		// Get database context - we declared Database level, so this should succeed
		let db_ctx = ctx.database()?;

		// Clone the context for the async block (all fields are Arc/String so cheap to clone)
		let table_expr = self.table.clone();
		let ns_name = db_ctx.ns_ctx.ns.name.clone();
		let db_name = db_ctx.db.name.clone();
		let txn = db_ctx.ns_ctx.txn.clone();
		let params = db_ctx.ns_ctx.root.params.clone();

		// Create state for the scan
		#[derive(Clone)]
		struct ScanState {
			next_key: Option<Vec<u8>>,
			end: Vec<u8>,
			table_name: TableName,
			ns_id: crate::catalog::NamespaceId,
			db_id: crate::catalog::DatabaseId,
		}

		// Create an async stream using try_unfold
		let stream = stream::try_unfold(None::<ScanState>, move |state| {
			let table_expr = table_expr.clone();
			let ns_name = ns_name.clone();
			let db_name = db_name.clone();
			let txn = txn.clone();
			let params = params.clone();

			async move {
				use crate::expr::ControlFlow;

				// Initialize state on first call
				let state = if let Some(s) = state {
					s
				} else {
					// Evaluate the table expression to get the table name
					let eval_ctx = EvalContext::scalar(
						&params,
						Some(&ns_name),
						Some(&db_name),
						Some(txn.as_ref()),
					);
					let table_value = table_expr.evaluate(eval_ctx).await.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!(
							"Failed to evaluate table expression: {}",
							e
						))
					})?;

					// Convert to table name
					let table_name = match table_value {
						Value::String(s) => TableName::from(s),
						Value::Table(t) => t,
						_ => {
							return Err(ControlFlow::Err(anyhow::anyhow!(
								"Table expression must evaluate to a string or table, got: {:?}",
								table_value
							)));
						}
					};

					// Get namespace and database IDs
					let ns_id = txn.expect_ns_by_name(&ns_name).await.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to get namespace: {}", e))
					})?;
					let db_id = txn.expect_db_by_name(&ns_name, &db_name).await.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to get database: {}", e))
					})?;

					// Create key range for all records in the table
					let beg = crate::key::record::prefix(
						ns_id.namespace_id,
						db_id.database_id,
						&table_name,
					)
					.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to create prefix key: {}", e))
					})?;
					let end = crate::key::record::suffix(
						ns_id.namespace_id,
						db_id.database_id,
						&table_name,
					)
					.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to create suffix key: {}", e))
					})?;

					ScanState {
						next_key: Some(beg),
						end,
						table_name,
						ns_id: ns_id.namespace_id,
						db_id: db_id.database_id,
					}
				};

				// Check if we're done
				let Some(next_key) = state.next_key else {
					return Ok(None);
				};

				// Scan a batch
				const BATCH_SIZE: u32 = 1000;
				let records =
					txn.scan(next_key.clone()..state.end.clone(), BATCH_SIZE, None).await.map_err(
						|e| ControlFlow::Err(anyhow::anyhow!("Failed to scan records: {}", e)),
					)?;

				if records.is_empty() {
					return Ok(None);
				}

				// Save length and last key before consuming
				let records_len = records.len();
				let last_key = records.last().map(|(k, _)| k.clone());

				// Deserialize and collect values
				let mut values = Vec::with_capacity(records_len);
				for (_key, val) in records {
					use crate::kvs::KVValue;
					let record = crate::catalog::Record::kv_decode_value(val).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to deserialize record: {}", e))
					})?;
					values.push(record.data.as_ref().clone());
				}

				// Determine next state
				let next_state = if records_len < BATCH_SIZE as usize {
					// Done scanning
					ScanState {
						next_key: None,
						..state
					}
				} else if let Some(last_key) = last_key {
					// More to scan - start after the last key
					let mut new_key = last_key;
					new_key.push(0);
					ScanState {
						next_key: Some(new_key),
						..state
					}
				} else {
					// No more records
					ScanState {
						next_key: None,
						..state
					}
				};

				Ok(Some((
					ValueBatch {
						values,
					},
					Some(next_state),
				)))
			}
		});

		Ok(Box::pin(stream))
	}
}
