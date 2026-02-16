//! Physical part expressions for streaming execution.
//!
//! Each part is a `PhysicalExpr` that reads its input from `ctx.current_value`
//! and produces a new value. `IdiomExpr` chains parts together via `ctx.with_value()`.
//!
//! This module replaces the old `PhysicalPart` enum with individual structs
//! that each implement `PhysicalExpr`, following the same pattern as the
//! `physical_expr` and `operators` modules.

use std::sync::Arc;

use crate::catalog::providers::TableProvider;
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::expr::FlowResult;
// Re-export recursion utilities from the canonical definitions in `expr::idiom::recursion`.
// These are shared between the legacy compute path and the streaming execution engine.
pub(crate) use crate::expr::idiom::recursion::{clean_iteration, get_final, is_final};
use crate::val::{RecordId, Value};

pub(crate) mod array_ops;
pub(crate) mod destructure;
pub(crate) mod field;
pub(crate) mod filter;
pub(crate) mod index;
pub(crate) mod lookup;
pub(crate) mod method;
pub(crate) mod optional;
pub(crate) mod recurse;

// Re-export part types
pub(crate) use array_ops::{AllPart, FirstPart, FlattenPart, LastPart};
pub(crate) use destructure::{DestructureField, DestructurePart};
pub(crate) use field::FieldPart;
pub(crate) use filter::WherePart;
pub(crate) use index::IndexPart;
pub(crate) use lookup::{LookupDirection, LookupPart};
pub(crate) use method::{ClosureFieldCallPart, MethodPart};
pub(crate) use optional::OptionalChainPart;
pub(crate) use recurse::{PhysicalRecurseInstruction, RecursePart, RepeatRecursePart};

// ============================================================================
// Shared utilities
// ============================================================================

/// Fetch a record and evaluate any computed fields on it.
///
/// This is necessary for computed fields that reference other computed fields
/// to work correctly (e.g., `DEFINE FIELD subproducts ON product COMPUTED ->contains->product.*`).
pub(crate) async fn fetch_record_with_computed_fields(
	rid: &RecordId,
	ctx: EvalContext<'_>,
) -> anyhow::Result<Value> {
	use reblessive::TreeStack;

	let db_ctx = ctx.exec_ctx.database().map_err(|e| anyhow::anyhow!("{}", e))?;
	let txn = ctx.exec_ctx.txn();

	// Fetch the raw record from storage
	let record = txn
		.get_record(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			&rid.table,
			&rid.key,
			None,
		)
		.await
		.map_err(|e| anyhow::anyhow!("Failed to fetch record: {}", e))?;

	let mut result = record.data.clone();

	// If the record doesn't exist (e.g. was deleted), return None early.
	// Don't proceed to evaluate computed fields on a non-existent record.
	if result.is_none() {
		return Ok(Value::None);
	}

	// Get the table's field definitions to check for computed fields
	let fields = txn
		.all_tb_fields(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, &rid.table, None)
		.await
		.map_err(|e| anyhow::anyhow!("Failed to get field definitions: {}", e))?;

	// Check if any fields have computed values
	let has_computed = fields.iter().any(|fd| fd.computed.is_some());

	if has_computed {
		// We need to evaluate computed fields using the legacy compute path
		// Get the Options from the context (if available)
		let root = ctx.exec_ctx.root();
		if let Some(ref opt) = root.options {
			let frozen = &root.ctx;
			let rid_arc = std::sync::Arc::new(rid.clone());
			let fields_clone = fields.clone();

			// Collect computed fields with their deps for topological sorting
			let computed_with_deps: Vec<(String, Vec<String>)> = fields_clone
				.iter()
				.filter(|fd| fd.computed.is_some())
				.map(|fd| {
					let name = fd.name.to_raw_string();
					let deps = if let Some(ref cd) = fd.computed_deps {
						cd.fields.clone()
					} else if let Some(ref expr) = fd.computed {
						crate::expr::computed_deps::extract_computed_deps(expr).fields
					} else {
						Vec::new()
					};
					(name, deps)
				})
				.collect();

			let sorted_indices =
				crate::expr::computed_deps::topological_sort_computed_fields(&computed_with_deps);

			// Build a map from field name to index in the original fields_clone
			let computed_fields_ordered: Vec<_> = {
				let name_to_fd: std::collections::HashMap<
					String,
					&crate::catalog::FieldDefinition,
				> = fields_clone
					.iter()
					.filter(|fd| fd.computed.is_some())
					.map(|fd| (fd.name.to_raw_string(), fd))
					.collect();
				sorted_indices
					.iter()
					.filter_map(|&idx| {
						let (ref name, _) = computed_with_deps[idx];
						name_to_fd.get(name.as_str()).copied()
					})
					.collect()
			};

			// Use TreeStack for stack management during recursive computation
			let mut stack = TreeStack::new();
			result = stack
				.enter(|stk| async move {
					let mut doc_value = result;
					for fd in computed_fields_ordered {
						if let Some(computed) = &fd.computed {
							// Evaluate the computed expression using the legacy compute method
							// The document context is the current result value
							let doc = crate::doc::CursorDoc::new(
								Some(rid_arc.clone()),
								None,
								doc_value.clone(),
							);
							match computed.compute(stk, frozen, opt, Some(&doc)).await {
								Ok(val) => {
									// Coerce to the field's type if specified
									let coerced_val = if let Some(kind) = fd.field_kind.as_ref() {
										val.clone().coerce_to_kind(kind).unwrap_or(val)
									} else {
										val
									};
									doc_value.put(&fd.name, coerced_val);
								}
								Err(crate::expr::ControlFlow::Return(val)) => {
									doc_value.put(&fd.name, val);
								}
								Err(_) => {
									// If computation fails, leave the field as-is or set to None
									doc_value.put(&fd.name, Value::None);
								}
							}
						}
					}
					doc_value
				})
				.finish()
				.await;
		}
	}

	// Ensure the record has its ID
	result.def(rid.clone());

	Ok(result)
}

/// Evaluate a path of PhysicalExpr parts against a value.
///
/// This helper function traverses a sequence of parts, applying each one
/// in order to the current value. Used by recursion and destructure aliased paths.
///
/// Note: This function uses `Box::pin` to support recursive evaluation through
/// `RepeatRecursePart` -> `RecursePart` -> `evaluate_physical_path` chains.
/// The recursion depth is bounded by the system recursion limit.
pub(crate) fn evaluate_physical_path<'a>(
	value: &'a Value,
	path: &'a [Arc<dyn PhysicalExpr>],
	ctx: EvalContext<'a>,
) -> crate::exec::BoxFut<'a, FlowResult<Value>> {
	Box::pin(async move {
		let mut current = value.clone();
		for part in path {
			current = part.evaluate(ctx.with_value(&current)).await?;
		}
		Ok(current)
	})
}
