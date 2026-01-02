//! Create operator for inserting new records with permission checking.
//!
//! The Create operator inserts new records into a table, checking CREATE
//! permissions at execution time.

use std::sync::Arc;

use futures::stream;

use crate::catalog::Permission;
use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms,
};
use crate::exec::{ContextLevel, ExecutionContext, ExecutionPlan, ValueBatch, ValueBatchStream};
use crate::iam::Action;
use crate::val::{TableName, Value};

/// Source of content for a CREATE operation.
#[derive(Debug, Clone)]
pub enum ContentSource {
	/// A single value to create
	Value(Value),
	/// Multiple values to create
	Values(Vec<Value>),
}

/// Create operator - inserts new records into a table.
///
/// This operator checks CREATE permissions at execution time by resolving
/// the table definition from the current transaction's schema view.
#[derive(Debug, Clone)]
pub struct Create {
	/// The table to create records in
	pub table: TableName,
	/// The content to create
	pub content: ContentSource,
}

impl ExecutionPlan for Create {
	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// Get database context
		let db_ctx = ctx.database()?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(db_ctx, Action::Edit)?;

		// Clone what we need for the async block
		let table = self.table.clone();
		let content = self.content.clone();
		let ns_name = db_ctx.ns_ctx.ns.name.clone();
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_name = db_ctx.db.name.clone();
		let db_id = db_ctx.db.database_id;
		let txn = db_ctx.ns_ctx.txn.clone();
		let params = db_ctx.ns_ctx.root.params.clone();
		let auth = db_ctx.ns_ctx.root.auth.clone();
		let auth_enabled = db_ctx.ns_ctx.root.auth_enabled;

		// Create a stream that performs the create operation
		let stream = stream::once(async move {
			use crate::expr::ControlFlow;

			// Resolve table definition and CREATE permission at execution time
			let create_permission = if check_perms {
				let table_def = txn
					.get_tb_by_name(&ns_name, &db_name, &table)
					.await
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get table: {}", e)))?;

				let catalog_perm = match table_def {
					Some(def) => def.permissions.create.clone(),
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

			// Check if permission is Deny - deny immediately
			if matches!(&create_permission, PhysicalPermission::Deny) {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"Permission denied: CREATE on table '{}'",
					table
				)));
			}

			// Get the values to create
			let values = match content {
				ContentSource::Value(v) => vec![v],
				ContentSource::Values(vs) => vs,
			};

			let mut created_values = Vec::with_capacity(values.len());

			for value in values {
				// Check permission for this value if it's a Conditional permission
				let allowed = match &create_permission {
					PhysicalPermission::Allow => true,
					PhysicalPermission::Deny => false, // Already handled above
					PhysicalPermission::Conditional(_) => {
						// Create a temporary database context for permission evaluation
						let db_ctx = crate::exec::DatabaseContext {
							ns_ctx: crate::exec::NamespaceContext {
								root: crate::exec::RootContext {
									datastore: None,
									params: params.clone(),
									cancellation: tokio_util::sync::CancellationToken::new(),
									auth: auth.clone(),
									auth_enabled,
								},
								ns: Arc::new(crate::catalog::NamespaceDefinition {
									namespace_id: ns_id,
									name: ns_name.clone(),
									comment: None,
								}),
								txn: txn.clone(),
							},
							db: Arc::new(crate::catalog::DatabaseDefinition {
								namespace_id: ns_id,
								database_id: db_id,
								name: db_name.clone(),
								comment: None,
								changefeed: None,
								strict: false,
							}),
						};

						check_permission_for_value(&create_permission, &value, &db_ctx)
							.await
							.map_err(|e| {
								ControlFlow::Err(anyhow::anyhow!(
									"Failed to check permission: {}",
									e
								))
							})?
					}
				};

				if !allowed {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						"Permission denied: CREATE on table '{}'",
						table
					)));
				}

				// TODO: Actually create the record in the database
				// For now, just return the value that would be created
				// This requires write transaction support which is beyond the current scope
				created_values.push(value);
			}

			Ok(ValueBatch {
				values: created_values,
			})
		});

		Ok(Box::pin(stream))
	}
}
