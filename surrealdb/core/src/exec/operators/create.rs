//! Create operator for inserting new records with permission checking.
//!
//! The Create operator inserts new records into a table, checking CREATE
//! permissions at execution time.
//!
//! Note: This module is work-in-progress for CREATE statement support.
#![allow(dead_code)]

use async_trait::async_trait;
use futures::stream;

use crate::catalog::Permission;
use crate::catalog::providers::TableProvider;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms,
};
use crate::exec::{
	AccessMode, ContextLevel, ExecutionContext, FlowResult, OperatorPlan, ValueBatch,
	ValueBatchStream,
};
use crate::expr::ControlFlow;
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

#[async_trait]
impl OperatorPlan for Create {
	fn name(&self) -> &'static str {
		"Create"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		// CREATE always mutates data
		AccessMode::ReadWrite
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Get database context
		let db_ctx = ctx.database()?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(db_ctx, Action::Edit)?;

		// Clone what we need for the async block
		let table = self.table.clone();
		let content = self.content.clone();
		let exec_ctx = ctx.clone();
		let ns_name = db_ctx.ns_ctx.ns.name.clone();
		let db_name = db_ctx.db.name.clone();
		let txn = db_ctx.ns_ctx.root.txn.clone();

		// Create a stream that performs the create operation
		let stream = stream::once(async move {
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
				let allowed = check_permission_for_value(&create_permission, &value, &exec_ctx)
					.await
					.map_err(|e| {
						ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {}", e))
					})?;

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
