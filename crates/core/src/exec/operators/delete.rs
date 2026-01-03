//! Delete operator for removing records with permission checking.
//!
//! The Delete operator removes existing records, checking DELETE permissions
//! at execution time.

use std::sync::Arc;

use futures::StreamExt;

use crate::catalog::Permission;
use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms,
};
use crate::exec::{ContextLevel, ExecutionContext, OperatorPlan, ValueBatch, ValueBatchStream};
use crate::iam::Action;
use crate::val::TableName;

/// Delete operator - removes existing records from a table.
///
/// This operator checks DELETE permissions at execution time by resolving
/// the table definition from the current transaction's schema view.
#[derive(Debug, Clone)]
pub struct Delete {
	/// The table to delete records from
	pub table: TableName,
	/// The input plan providing records to delete
	pub input: Arc<dyn OperatorPlan>,
}

impl OperatorPlan for Delete {
	fn name(&self) -> &'static str {
		"Delete"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database.max(self.input.required_context())
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// Get database context
		let db_ctx = ctx.database()?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(db_ctx, Action::Edit)?;

		// Clone what we need for the async block
		let input_stream = self.input.execute(ctx)?;
		let table = self.table.clone();
		let ns = Arc::clone(&db_ctx.ns_ctx.ns);
		let db = Arc::clone(&db_ctx.db);
		let ns_name = db_ctx.ns_ctx.ns.name.clone();
		let db_name = db_ctx.db.name.clone();
		let txn = db_ctx.ns_ctx.root.txn.clone();
		let params = db_ctx.ns_ctx.root.params.clone();
		let auth = db_ctx.ns_ctx.root.auth.clone();
		let auth_enabled = db_ctx.ns_ctx.root.auth_enabled;

		// Cache for permission (resolved on first batch)
		let delete_permission: Arc<tokio::sync::Mutex<Option<PhysicalPermission>>> =
			Arc::new(tokio::sync::Mutex::new(None));

		// Create a stream that performs the delete operation
		let deleted = input_stream.filter_map(move |batch_result| {
			let table = table.clone();
			let ns = ns.clone();
			let db = db.clone();
			let ns_name = ns_name.clone();
			let db_name = db_name.clone();
			let txn = txn.clone();
			let params = params.clone();
			let auth = auth.clone();
			let delete_permission = delete_permission.clone();

			async move {
				use crate::expr::ControlFlow;

				// Handle errors in the input batch
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Some(Err(e)),
				};

				// Resolve DELETE permission on first batch
				let perm = {
					let mut perm_guard = delete_permission.lock().await;
					if perm_guard.is_none() {
						let resolved = if check_perms {
							let table_def =
								match txn.get_tb_by_name(&ns_name, &db_name, &table).await {
									Ok(def) => def,
									Err(e) => {
										return Some(Err(ControlFlow::Err(anyhow::anyhow!(
											"Failed to get table: {}",
											e
										))));
									}
								};

							let catalog_perm = match table_def {
								Some(def) => def.permissions.delete.clone(),
								None => Permission::None,
							};
							match convert_permission_to_physical(&catalog_perm) {
								Ok(perm) => perm,
								Err(e) => {
									return Some(Err(ControlFlow::Err(anyhow::anyhow!(
										"Failed to convert permission: {}",
										e
									))));
								}
							}
						} else {
							PhysicalPermission::Allow
						};
						*perm_guard = Some(resolved);
					}
					perm_guard.clone().unwrap()
				};

				// Check if permission is Deny - deny all
				if matches!(&perm, PhysicalPermission::Deny) {
					return Some(Err(ControlFlow::Err(anyhow::anyhow!(
						"Permission denied: DELETE on table '{}'",
						table
					))));
				}

				let mut deleted_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					// Build execution context for permission checks
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

					// Check permission for this value if it's a Conditional permission
					let allowed = match &perm {
						PhysicalPermission::Allow => true,
						PhysicalPermission::Deny => false,
						PhysicalPermission::Conditional(_) => {
							match check_permission_for_value(&perm, &value, &exec_ctx).await {
								Ok(allowed) => allowed,
								Err(e) => {
									return Some(Err(ControlFlow::Err(anyhow::anyhow!(
										"Failed to check permission: {}",
										e
									))));
								}
							}
						}
					};

					if !allowed {
						// Skip this record (permission denied)
						continue;
					}

					// TODO: Actually delete the record from the database
					// This requires write transaction support which is beyond the current scope
					// For now, return the value that would be deleted
					deleted_values.push(value);
				}

				if deleted_values.is_empty() {
					None
				} else {
					Some(Ok(ValueBatch {
						values: deleted_values,
					}))
				}
			}
		});

		Ok(Box::pin(deleted))
	}
}
