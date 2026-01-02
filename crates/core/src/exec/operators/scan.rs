use std::sync::Arc;

use futures::stream;

use crate::catalog::Permission;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
use crate::err::Error;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms,
};
use crate::exec::{
	ContextLevel, EvalContext, ExecutionContext, ExecutionPlan, PhysicalExpr, ValueBatch,
	ValueBatchStream,
};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::val::{TableName, Value};

/// Full table scan - iterates over all records in a table.
///
/// Requires database-level context since it reads from a specific table
/// in the selected namespace and database.
///
/// Permission checking is performed at execution time by resolving the table
/// definition from the current transaction's schema view and filtering records
/// based on the SELECT permission.
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
		// Get database context - we declared Database level, so this should succeed
		let db_ctx = ctx.database()?.clone();

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone the context for the async block (all fields are Arc/String so cheap to clone)
		let table_expr = self.table.clone();
		let ctx = ctx.clone();

		// Create an async stream using try_unfold
		let stream = stream::try_unfold(None::<ScanState>, move |state| {
			let table_expr = table_expr.clone();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let ctx = ctx.clone();

			async move {
				let txn = ctx.txn().clone();

				// Initialize state on first call
				let state = if let Some(s) = state {
					s
				} else {
					// Build execution context for table expression evaluation
					let exec_ctx = ctx.clone();

					// Evaluate the table expression to get the table name
					let eval_ctx = EvalContext::from_exec_ctx(&exec_ctx);
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

					// Resolve table definition and SELECT permission at execution time
					let select_permission = if check_perms {
						let table_def =
							txn.get_tb_by_name(&ns.name, &db.name, &table_name).await.map_err(
								|e| ControlFlow::Err(anyhow::anyhow!("Failed to get table: {}", e)),
							)?;

						let catalog_perm = match table_def {
							Some(def) => def.permissions.select.clone(),
							// Schemaless table: deny access for record users
							None => Permission::None,
						};
						// Convert to physical permission
						convert_permission_to_physical(&catalog_perm).map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!("Failed to convert permission: {}", e))
						})?
					} else {
						// Permissions bypassed - allow all
						PhysicalPermission::Allow
					};

					// Create key range for all records in the table
					let beg =
						crate::key::record::prefix(ns.namespace_id, db.database_id, &table_name)
							.map_err(|e| {
								ControlFlow::Err(anyhow::anyhow!(
									"Failed to create prefix key: {}",
									e
								))
							})?;
					let end =
						crate::key::record::suffix(ns.namespace_id, db.database_id, &table_name)
							.map_err(|e| {
								ControlFlow::Err(anyhow::anyhow!(
									"Failed to create suffix key: {}",
									e
								))
							})?;

					ScanState {
						next_key: Some(beg),
						end,
						table_name,
						ns_id: ns.namespace_id,
						db_id: db.database_id,
						select_permission,
					}
				};

				// Check if permission is Deny - return empty result immediately
				if matches!(&state.select_permission, PhysicalPermission::Deny) {
					return Ok(None);
				}

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

				// Deserialize and collect values, filtering by permission
				let mut values = Vec::with_capacity(records_len);
				for (key, val) in records {
					use crate::kvs::KVValue;

					// Decode the record key to get the RecordId
					let decoded_key =
						crate::key::record::RecordKey::decode_key(&key).map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!("Failed to decode record key: {}", e))
						})?;

					let rid = crate::val::RecordId {
						table: decoded_key.tb.into_owned(),
						key: decoded_key.id,
					};

					let mut record = crate::catalog::Record::kv_decode_value(val).map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to deserialize record: {}", e))
					})?;

					// Inject the id field into the document
					record.data.to_mut().def(&rid);

					let value = record.data.as_ref().clone();

					// Check permission for this record
					let allowed =
						check_permission_for_value(&state.select_permission, &value, &ctx)
							.await
							.map_err(|e| {
								ControlFlow::Err(anyhow::anyhow!(
									"Failed to check permission: {}",
									e
								))
							})?;

					if allowed {
						values.push(value);
					}
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

		// Filter out empty batches from the stream
		let filtered_stream = futures::StreamExt::filter_map(stream, |result| async move {
			match result {
				Ok(batch) if batch.values.is_empty() => None,
				other => Some(other),
			}
		});

		Ok(Box::pin(filtered_stream))
	}
}

// Create state for the scan
#[derive(Clone)]
struct ScanState {
	next_key: Option<Vec<u8>>,
	end: Vec<u8>,
	table_name: TableName,
	ns_id: crate::catalog::NamespaceId,
	db_id: crate::catalog::DatabaseId,
	select_permission: PhysicalPermission,
}
