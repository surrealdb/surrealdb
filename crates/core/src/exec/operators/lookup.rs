use std::sync::Arc;

use futures::stream;

use crate::catalog::Permission;
use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms, validate_record_user_access,
};
use crate::exec::{ContextLevel, ExecutionContext, ExecutionPlan, ValueBatch, ValueBatchStream};
use crate::iam::Action;
use crate::val::RecordId;

/// Direct lookup of a record by its ID.
///
/// Requires database-level context since it looks up a record
/// in a specific table within the selected namespace and database.
///
/// Permission checking is performed at execution time by resolving the table
/// definition from the current transaction's schema view and checking the
/// SELECT permission for the returned record.
#[derive(Debug, Clone)]
pub struct RecordIdLookup {
	pub(crate) record_id: RecordId,
	/// Optional version timestamp for time-travel queries (VERSION clause)
	pub(crate) version: Option<u64>,
}

impl ExecutionPlan for RecordIdLookup {
	fn name(&self) -> &'static str {
		"RecordIdLookup"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// Get database context - we declared Database level, so this should succeed
		let db_ctx = ctx.database()?;

		// Validate record user has access to this namespace/database
		validate_record_user_access(db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(db_ctx, Action::View)?;

		// Clone what we need for the async block
		let record_id = self.record_id.clone();
		let version = self.version;
		let ns = Arc::clone(&db_ctx.ns_ctx.ns);
		let db = Arc::clone(&db_ctx.db);
		let ns_name = db_ctx.ns_ctx.ns.name.clone();
		let db_name = db_ctx.db.name.clone();
		let txn = db_ctx.ns_ctx.root.txn.clone();
		let params = db_ctx.ns_ctx.root.params.clone();
		let auth = db_ctx.ns_ctx.root.auth.clone();
		let auth_enabled = db_ctx.ns_ctx.root.auth_enabled;

		// Create an async stream that looks up the record
		let stream = stream::once(async move {
			use crate::expr::ControlFlow;

			// Resolve table definition and SELECT permission at execution time
			let select_permission = if check_perms {
				let table_def = txn
					.get_tb_by_name(&ns_name, &db_name, &record_id.table)
					.await
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get table: {}", e)))?;

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

			// Check if permission is Deny - return empty result immediately
			if matches!(&select_permission, PhysicalPermission::Deny) {
				return Ok(ValueBatch {
					values: vec![],
				});
			}

			// Look up the record (with optional version for time-travel)
			let record = txn
				.get_record(
					ns.namespace_id,
					db.database_id,
					&record_id.table,
					&record_id.key,
					version,
				)
				.await
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get record: {}", e)))?;

			// Extract the value - if None, the record doesn't exist
			let value = record.data.as_ref();

			// Check if the record exists - return empty batch if not
			if value.is_none() {
				return Ok(ValueBatch {
					values: vec![],
				});
			}

			// Inject the id field into the document
			let mut value = value.clone();
			value.def(&record_id);

			// Check permission for this record
			match &select_permission {
				PhysicalPermission::Allow => {
					// No filtering needed
					Ok(ValueBatch {
						values: vec![value],
					})
				}
				PhysicalPermission::Deny => {
					// Should have been handled above, but return empty just in case
					Ok(ValueBatch {
						values: vec![],
					})
				}
				PhysicalPermission::Conditional(_) => {
					// Build execution context for permission evaluation
					let exec_ctx = ExecutionContext::Database(crate::exec::DatabaseContext {
						ns_ctx: crate::exec::NamespaceContext {
							root: crate::exec::RootContext {
								datastore: None,
								params: params.clone(),
								cancellation: tokio_util::sync::CancellationToken::new(),
								auth: auth.clone(),
								auth_enabled,
								txn: txn.clone(),
							},
							ns: ns.clone(),
						},
						db: db.clone(),
					});

					// Check permission for this specific value
					let allowed = check_permission_for_value(&select_permission, &value, &exec_ctx)
						.await
						.map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {}", e))
						})?;

					if allowed {
						Ok(ValueBatch {
							values: vec![value],
						})
					} else {
						Ok(ValueBatch {
							values: vec![],
						})
					}
				}
			}
		});

		Ok(Box::pin(stream))
	}
}
