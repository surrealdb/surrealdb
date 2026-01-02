//! Update operator for modifying records with permission checking.
//!
//! The Update operator modifies existing records, checking UPDATE permissions
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
use crate::exec::{
	ContextLevel, EvalContext, ExecutionContext, ExecutionPlan, PhysicalExpr, ValueBatch,
	ValueBatchStream,
};
use crate::iam::Action;
use crate::val::{TableName, Value};

/// A field to set during an update operation.
#[derive(Debug, Clone)]
pub struct SetField {
	/// The field name to set
	pub field: String,
	/// The expression to evaluate for the new value
	pub value: Arc<dyn PhysicalExpr>,
}

/// Update operator - modifies existing records in a table.
///
/// This operator checks UPDATE permissions at execution time by resolving
/// the table definition from the current transaction's schema view.
#[derive(Debug, Clone)]
pub struct Update {
	/// The table to update records in
	pub table: TableName,
	/// The input plan providing records to update
	pub input: Arc<dyn ExecutionPlan>,
	/// The fields to set
	pub changes: Vec<SetField>,
}

impl ExecutionPlan for Update {
	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database.max(self.input.required_context())
	}

	fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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
		let changes = self.changes.clone();
		let ns_name = db_ctx.ns_ctx.ns.name.clone();
		let ns_id = db_ctx.ns_ctx.ns.namespace_id;
		let db_name = db_ctx.db.name.clone();
		let db_id = db_ctx.db.database_id;
		let txn = db_ctx.ns_ctx.txn.clone();
		let params = db_ctx.ns_ctx.root.params.clone();
		let auth = db_ctx.ns_ctx.root.auth.clone();
		let auth_enabled = db_ctx.ns_ctx.root.auth_enabled;

		// Cache for permission (resolved on first batch)
		let update_permission: Arc<tokio::sync::Mutex<Option<PhysicalPermission>>> =
			Arc::new(tokio::sync::Mutex::new(None));

		// Create a stream that performs the update operation
		let updated = input_stream.filter_map(move |batch_result| {
			let table = table.clone();
			let changes = changes.clone();
			let ns_name = ns_name.clone();
			let db_name = db_name.clone();
			let txn = txn.clone();
			let params = params.clone();
			let auth = auth.clone();
			let update_permission = update_permission.clone();

			async move {
				use crate::expr::ControlFlow;

				// Handle errors in the input batch
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Some(Err(e)),
				};

				// Resolve UPDATE permission on first batch
				let perm = {
					let mut perm_guard = update_permission.lock().await;
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
								Some(def) => def.permissions.update.clone(),
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
						"Permission denied: UPDATE on table '{}'",
						table
					))));
				}

				let mut updated_values = Vec::with_capacity(batch.values.len());

				for mut value in batch.values {
					// Check permission for this value if it's a Conditional permission
					let allowed = match &perm {
						PhysicalPermission::Allow => true,
						PhysicalPermission::Deny => false,
						PhysicalPermission::Conditional(_) => {
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

							match check_permission_for_value(&perm, &value, &db_ctx).await {
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

					// First, evaluate all change expressions
					let mut evaluated_changes = Vec::with_capacity(changes.len());
					{
						let eval_ctx = EvalContext::scalar(
							&params,
							Some(&ns_name),
							Some(&db_name),
							Some(txn.as_ref()),
						)
						.with_value(&value);

						for change in &changes {
							let new_value = match change.value.evaluate(eval_ctx.clone()).await {
								Ok(v) => v,
								Err(e) => {
									return Some(Err(ControlFlow::Err(anyhow::anyhow!(
										"Failed to evaluate change expression: {}",
										e
									))));
								}
							};
							evaluated_changes.push((change.field.clone(), new_value));
						}
					}

					// Now apply all the evaluated changes
					for (field, new_value) in evaluated_changes {
						if let Value::Object(ref mut obj) = value {
							obj.insert(field, new_value);
						}
					}

					// TODO: Actually persist the update to the database
					// This requires write transaction support which is beyond the current scope
					updated_values.push(value);
				}

				if updated_values.is_empty() {
					None
				} else {
					Some(Ok(ValueBatch {
						values: updated_values,
					}))
				}
			}
		});

		Ok(Box::pin(updated))
	}
}
