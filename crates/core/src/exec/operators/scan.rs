use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::catalog::Permission;
use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms, validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecutionContext, OperatorPlan, PhysicalExpr,
	ValueBatch, ValueBatchStream,
};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::idx::planner::ScanDirection;
use crate::key::record;
use crate::kvs::{KVKey, KVValue};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Batch size for collecting records before yielding.
const BATCH_SIZE: usize = 1000;

/// Represents the target of a scan operation.
enum ScanTarget {
	/// Scan all records in a table
	Table(TableName),
	/// Scan a specific record or range by RecordId
	RecordId(RecordId),
}

impl ScanTarget {
	/// Get the table name for permission lookup
	fn table_name(&self) -> TableName {
		match self {
			ScanTarget::Table(t) => t.clone(),
			ScanTarget::RecordId(rid) => rid.table.clone(),
		}
	}
}

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
	/// Optional version timestamp for time-travel queries (VERSION clause)
	pub(crate) version: Option<u64>,
}

#[async_trait]
impl OperatorPlan for Scan {
	fn name(&self) -> &'static str {
		"Scan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("table".to_string(), self.table.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		// Scan is read-only, but the table expression could theoretically contain a subquery
		self.table.access_mode()
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// Get database context - we declared Database level, so this should succeed
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let table_expr = Arc::clone(&self.table);
		let version = self.version;
		let ctx = ctx.clone();

		// Use try_stream! for clean async generator syntax
		let stream = async_stream::try_stream! {
			let txn = Arc::clone(ctx.txn());
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate table expression
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let table_value = table_expr.evaluate(eval_ctx).await.map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!("Failed to evaluate table expression: {e}"))
			})?;

			// Determine scan target: either a table name or a record ID
			let scan_target = match table_value {
				Value::String(s) => ScanTarget::Table(TableName::from(s)),
				Value::Table(t) => ScanTarget::Table(t),
				Value::RecordId(rid) => ScanTarget::RecordId(rid),
				_ => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"Table expression must evaluate to a string, table, or record ID, got: {:?}",
						table_value
					)))?
				}
			};

			// Get table name for permission lookup
			let table_name = scan_target.table_name();

			// Resolve SELECT permission
			let select_permission = if check_perms {
				let table_def = txn
					.get_tb_by_name(&ns.name, &db.name, &table_name)
					.await
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get table: {e}")))?;

				let catalog_perm = match table_def {
					Some(def) => def.permissions.select.clone(),
					None => Permission::None, // Schemaless: deny for record users
				};

				convert_permission_to_physical(&catalog_perm)
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to convert permission: {e}")))?
			} else {
				PhysicalPermission::Allow
			};

			// Early exit if denied - yield nothing
			if matches!(select_permission, PhysicalPermission::Deny) {
				return;
			}

			// Execute based on scan target type
			match scan_target {
				ScanTarget::Table(table_name) => {
					// Full table scan
					let beg = crate::key::record::prefix(ns.namespace_id, db.database_id, &table_name)
						.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix key: {e}")))?;
					let end = crate::key::record::suffix(ns.namespace_id, db.database_id, &table_name)
						.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix key: {e}")))?;

					let kv_stream = txn.stream_keys_vals(beg..end, version, None, ScanDirection::Forward);
					futures::pin_mut!(kv_stream);

					let mut batch = Vec::with_capacity(BATCH_SIZE);

					while let Some(result) = kv_stream.next().await {
						let (key, val) = result
							.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to scan record: {e}")))?;

						let value = decode_record(&key, val)?;

						let allowed = check_permission_for_value(&select_permission, &value, &ctx)
							.await
							.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}")))?;

						if allowed {
							batch.push(value);
							if batch.len() >= BATCH_SIZE {
								yield ValueBatch { values: std::mem::take(&mut batch) };
								batch.reserve(BATCH_SIZE);
							}
						}
					}

					if !batch.is_empty() {
						yield ValueBatch { values: batch };
					}
				}
				ScanTarget::RecordId(rid) => {
					// Check if this is a range query or a point lookup
					match &rid.key {
						RecordIdKey::Range(range) => {
							// Range scan within the table - prepare key range like processor.rs does
							let beg = match &range.start {
								Bound::Unbounded => record::prefix(ns.namespace_id, db.database_id, &rid.table)
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix key: {e}")))?,
								Bound::Included(v) => record::new(ns.namespace_id, db.database_id, &rid.table, v)
									.encode_key()
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create begin key: {e}")))?,
								Bound::Excluded(v) => {
									let mut key = record::new(ns.namespace_id, db.database_id, &rid.table, v)
										.encode_key()
										.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create begin key: {e}")))?;
									key.push(0x00);
									key
								}
							};
							let end = match &range.end {
								Bound::Unbounded => record::suffix(ns.namespace_id, db.database_id, &rid.table)
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix key: {e}")))?,
								Bound::Excluded(v) => record::new(ns.namespace_id, db.database_id, &rid.table, v)
									.encode_key()
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create end key: {e}")))?,
								Bound::Included(v) => {
									let mut key = record::new(ns.namespace_id, db.database_id, &rid.table, v)
										.encode_key()
										.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create end key: {e}")))?;
									key.push(0x00);
									key
								}
							};

							let kv_stream = txn.stream_keys_vals(beg..end, version, None, ScanDirection::Forward);
							futures::pin_mut!(kv_stream);

							let mut batch = Vec::with_capacity(BATCH_SIZE);

							while let Some(result) = kv_stream.next().await {
								let (key, val) = result
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to scan record: {e}")))?;

								let value = decode_record(&key, val)?;

								let allowed = check_permission_for_value(&select_permission, &value, &ctx)
									.await
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}")))?;

								if allowed {
									batch.push(value);
									if batch.len() >= BATCH_SIZE {
										yield ValueBatch { values: std::mem::take(&mut batch) };
										batch.reserve(BATCH_SIZE);
									}
								}
							}

							if !batch.is_empty() {
								yield ValueBatch { values: batch };
							}
						}
						_ => {
							// Point lookup for a single record
							let record = txn
								.get_record(ns.namespace_id, db.database_id, &rid.table, &rid.key, version)
								.await
								.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get record: {e}")))?;

							// Check if record exists
							if record.data.as_ref().is_none() {
								return;
							}

							// Inject the id field into the document
							let mut value = record.data.as_ref().clone();
							value.def(&rid);

							// Check permission for this record
							let allowed = check_permission_for_value(&select_permission, &value, &ctx)
								.await
								.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}")))?;

							if allowed {
								yield ValueBatch { values: vec![value] };
							}
						}
					}
				}
			}
		};

		Ok(Box::pin(stream))
	}
}

/// Decode a record from its key and value bytes.
fn decode_record(key: &[u8], val: Vec<u8>) -> Result<Value, ControlFlow> {
	let decoded_key = crate::key::record::RecordKey::decode_key(key)
		.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to decode record key: {e}")))?;

	let rid = crate::val::RecordId {
		table: decoded_key.tb.into_owned(),
		key: decoded_key.id,
	};

	let mut record = crate::catalog::Record::kv_decode_value(val)
		.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to deserialize record: {e}")))?;

	// Inject the id field into the document
	record.data.to_mut().def(&rid);

	Ok(record.data.as_ref().clone())
}
