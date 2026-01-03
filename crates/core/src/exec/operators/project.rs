//! Project operator for field selection with permissions.
//!
//! The Project operator selects and transforms fields from input records,
//! applying field-level permissions at execution time.

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::Mutex;

use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms,
};
use crate::exec::{
	ContextLevel, EvalContext, ExecutionContext, OperatorPlan, PhysicalExpr, ValueBatch,
	ValueBatchStream,
};
use crate::iam::Action;
use crate::val::{Object, TableName, Value};

/// Field selection specification.
#[derive(Debug, Clone)]
pub struct FieldSelection {
	/// The output name for this field
	pub output_name: String,
	/// The expression to evaluate for this field's value
	pub expr: Arc<dyn PhysicalExpr>,
	/// If this selects a table field (for permission lookup), the field name
	pub field_name: Option<String>,
}

/// Project operator - selects and transforms fields from input records.
///
/// This operator applies field-level permissions at execution time by resolving
/// field definitions from the current transaction's schema view.
#[derive(Debug, Clone)]
pub struct Project {
	/// The input plan to project from
	pub input: Arc<dyn OperatorPlan>,
	/// The table name (for field permission lookup)
	pub table: TableName,
	/// The fields to select/project
	pub fields: Vec<FieldSelection>,
}

impl OperatorPlan for Project {
	fn name(&self) -> &'static str {
		"Project"
	}

	fn required_context(&self) -> ContextLevel {
		// Project needs Database for field permission lookup, but also
		// inherits child requirements (take the maximum)
		ContextLevel::Database.max(self.input.required_context())
	}

	fn children(&self) -> Vec<&Arc<dyn OperatorPlan>> {
		vec![&self.input]
	}

	fn execute(&self, ctx: &ExecutionContext) -> Result<ValueBatchStream, Error> {
		// Get database context
		let db_ctx = ctx.database()?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(db_ctx, Action::View)?;

		// Clone what we need for the async block
		let input_stream = self.input.execute(ctx)?;
		let table = self.table.clone();
		let fields = self.fields.clone();
		let ns = Arc::clone(&db_ctx.ns_ctx.ns);
		let db = Arc::clone(&db_ctx.db);
		let txn = db_ctx.ns_ctx.root.txn.clone();
		let params = db_ctx.ns_ctx.root.params.clone();
		let auth = db_ctx.ns_ctx.root.auth.clone();
		let auth_enabled = db_ctx.ns_ctx.root.auth_enabled;

		// Use Arc<Mutex> for shared state across async iterations
		let field_permissions: Arc<Mutex<Option<HashMap<String, PhysicalPermission>>>> =
			Arc::new(Mutex::new(None));

		// Create a stream that projects fields with permission checking
		let projected = input_stream.filter_map(move |batch_result| {
			let table = table.clone();
			let fields = fields.clone();
			let ns = ns.clone();
			let db = db.clone();
			let txn = txn.clone();
			let params = params.clone();
			let auth = auth.clone();
			let field_permissions = field_permissions.clone();

			async move {
				use crate::expr::ControlFlow;

				// Handle errors in the input batch
				let batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Some(Err(e)),
				};

				// Initialize field permissions on first batch
				{
					let mut perms_guard = field_permissions.lock().await;
					if perms_guard.is_none() && check_perms {
						// Get all field definitions for this table
						let field_defs = match txn
							.all_tb_fields(ns.namespace_id, db.database_id, &table, None)
							.await
						{
							Ok(defs) => defs,
							Err(e) => {
								return Some(Err(ControlFlow::Err(anyhow::anyhow!(
									"Failed to get field definitions: {}",
									e
								))));
							}
						};

						// Build permission map
						let mut perm_map = HashMap::new();
						for field_def in field_defs.iter() {
							let field_name = field_def.name.to_raw_string();
							let physical_perm = match convert_permission_to_physical(
								&field_def.select_permission,
							) {
								Ok(perm) => perm,
								Err(e) => {
									return Some(Err(ControlFlow::Err(anyhow::anyhow!(
										"Failed to convert field permission: {}",
										e
									))));
								}
							};
							perm_map.insert(field_name, physical_perm);
						}
						*perms_guard = Some(perm_map);
					}
				}

				let perms_guard = field_permissions.lock().await;
				let perms_map = perms_guard.as_ref();

				let mut projected_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					// Build execution context for expression evaluation and permission checks
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
					let eval_ctx = EvalContext::from_exec_ctx(&exec_ctx).with_value(&value);

					// Build the projected object
					let mut obj = Object::default();

					for field in &fields {
						// Check field permission if we're checking perms and this is a table field
						let allowed = if check_perms {
							if let Some(field_name) = &field.field_name {
								match perms_map.and_then(|m| m.get(field_name)) {
									Some(PhysicalPermission::Deny) => false,
									Some(PhysicalPermission::Allow) => true,
									Some(PhysicalPermission::Conditional(_perm)) => {
										// Use the execution context we already built for permission
										// evaluation
										match check_permission_for_value(
											perms_map.and_then(|m| m.get(field_name)).unwrap(),
											&value,
											&exec_ctx,
										)
										.await
										{
											Ok(allowed) => allowed,
											Err(e) => {
												return Some(Err(ControlFlow::Err(
													anyhow::anyhow!(
														"Failed to check field permission: {}",
														e
													),
												)));
											}
										}
									}
									None => {
										// No explicit field definition - allow by default
										true
									}
								}
							} else {
								// Not a table field (e.g., computed expression) - allow
								true
							}
						} else {
							// Not checking permissions
							true
						};

						if allowed {
							// Evaluate the field expression
							match field.expr.evaluate(eval_ctx.clone()).await {
								Ok(field_value) => {
									obj.insert(field.output_name.clone(), field_value);
								}
								Err(e) => {
									return Some(Err(ControlFlow::Err(anyhow::anyhow!(
										"Failed to evaluate field expression: {}",
										e
									))));
								}
							}
						}
						// If not allowed, we simply don't include the field in the output
					}

					projected_values.push(Value::Object(obj));
				}

				if projected_values.is_empty() {
					// Skip empty batches
					None
				} else {
					Some(Ok(ValueBatch {
						values: projected_values,
					}))
				}
			}
		});

		Ok(Box::pin(projected))
	}
}
