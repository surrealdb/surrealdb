use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::catalog::providers::TableProvider;
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	resolve_select_permission, should_check_perms,
};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlowExt;
use crate::expr::idiom::Idiom;
use crate::expr::part::Part;
use crate::iam::Action;
use crate::val::{RecordId, Value};

/// Fetches related records for specified fields.
///
/// The FETCH clause replaces record IDs with their full record data.
/// For example, if a field contains `author:tobie`, FETCH will replace
/// it with the full author record `{ id: author:tobie, name: 'Tobie', ... }`.
#[derive(Debug, Clone)]
pub struct Fetch {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// The fields to fetch. Each idiom points to a field that may contain
	/// record IDs to be resolved.
	pub(crate) fields: Vec<Idiom>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Fetch {
	pub(crate) fn new(input: Arc<dyn ExecOperator>, fields: Vec<Idiom>) -> Self {
		Self {
			input,
			fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Fetch {
	fn name(&self) -> &'static str {
		"Fetch"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		use surrealdb_types::ToSql;
		vec![(
			"fields".to_string(),
			self.fields.iter().map(|f| f.to_sql()).collect::<Vec<_>>().join(", "),
		)]
	}

	fn required_context(&self) -> ContextLevel {
		// Fetch needs database context for reading related records
		ContextLevel::Database.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// Fetch reads related records
		AccessMode::ReadOnly.combine(self.input.access_mode())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.input.cardinality_hint()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn is_scalar(&self) -> bool {
		self.input.is_scalar()
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let fields = self.fields.clone();
		let ctx = ctx.clone();

		let fetch_stream = input_stream.then(move |batch_result| {
			let fields = fields.clone();
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut fetched_values = Vec::with_capacity(batch.values.len());

				for value in batch.values {
					let fetched = fetch_fields(&ctx, value, &fields).await?;
					fetched_values.push(fetched);
				}

				Ok(ValueBatch {
					values: fetched_values,
				})
			}
		});

		Ok(monitor_stream(Box::pin(fetch_stream), "Fetch", &self.metrics))
	}
}

/// Fetch all specified fields for a value.
async fn fetch_fields(
	ctx: &ExecutionContext,
	mut value: Value,
	fields: &[Idiom],
) -> crate::expr::FlowResult<Value> {
	for field in fields {
		fetch_field_path(ctx, &mut value, &field.0).await?;
	}
	Ok(value)
}

/// Traverse a field path through a value, fetching record IDs along the way.
///
/// Uses an iterative loop for linear path descent (Field on Object, First, Last)
/// and only recurses for fan-out cases (iterating over array/object elements).
/// This avoids per-step heap allocation and reduces stack depth compared to
/// fully recursive traversal.
fn fetch_field_path<'a>(
	ctx: &'a ExecutionContext,
	value: &'a mut Value,
	path: &'a [Part],
) -> crate::exec::BoxFut<'a, crate::expr::FlowResult<()>> {
	Box::pin(async move {
		let mut current = value;
		let mut depth = 0usize;

		loop {
			// End of path: fetch the current value if it's a record
			if depth >= path.len() {
				return fetch_value_if_record(ctx, current).await;
			}

			// Mid-path RecordId: fetch in place and retry at the same depth
			if matches!(&*current, Value::RecordId(_)) {
				fetch_value_if_record(ctx, current).await?;
				continue;
			}

			match &path[depth] {
				Part::Field(name) => match current {
					Value::Object(obj) => {
						current = match obj.get_mut(name.as_str()) {
							Some(child) => child,
							None => return Ok(()),
						};
						depth += 1;
					}
					Value::Array(arr) => {
						// Fan-out: apply the same field path to each array element
						return fetch_each(ctx, arr.iter_mut(), &path[depth..]).await;
					}
					_ => return Ok(()),
				},
				Part::All => match current {
					Value::Array(arr) => {
						return fetch_each(ctx, arr.iter_mut(), &path[depth + 1..]).await;
					}
					Value::Object(obj) => {
						return fetch_each(ctx, obj.values_mut(), &path[depth + 1..]).await;
					}
					_ => return Ok(()),
				},
				Part::First => {
					current = match current {
						Value::Array(arr) => match arr.first_mut() {
							Some(v) => v,
							None => return Ok(()),
						},
						_ => return Ok(()),
					};
					depth += 1;
				}
				Part::Last => {
					current = match current {
						Value::Array(arr) => match arr.last_mut() {
							Some(v) => v,
							None => return Ok(()),
						},
						_ => return Ok(()),
					};
					depth += 1;
				}
				// For other path parts, we don't support fetching through them
				_ => return Ok(()),
			}
		}
	})
}

/// Fan-out helper: process each value with the remaining path.
///
/// Used when traversal encounters a collection (array or object) that
/// requires applying the remaining field path to each element.
async fn fetch_each<'a>(
	ctx: &'a ExecutionContext,
	values: impl Iterator<Item = &'a mut Value>,
	remaining_path: &'a [Part],
) -> crate::expr::FlowResult<()> {
	for item in values {
		fetch_field_path(ctx, item, remaining_path).await?;
	}
	Ok(())
}

/// If the value is a RecordId, fetch it and replace the value.
/// If it's an array of RecordIds, fetch each one using batch fetching.
async fn fetch_value_if_record(
	ctx: &ExecutionContext,
	value: &mut Value,
) -> crate::expr::FlowResult<()> {
	match value {
		Value::RecordId(rid) => {
			let fetched = fetch_record(ctx, rid).await?;
			*value = fetched;
		}
		Value::Array(arr) => {
			// Use batch fetch for arrays - more efficient for larger arrays
			batch_fetch_in_place(ctx, arr.as_mut_slice()).await?;
		}
		_ => {}
	}
	Ok(())
}

/// Fetch a single raw record value from the datastore.
///
/// Returns `Ok(Some(val))` with the record's ID injected, or `Ok(None)` if
/// the record does not exist.  This is the single point of record retrieval
/// that all higher-level helpers compose on top of.
pub(crate) async fn fetch_raw_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
	version: Option<u64>,
) -> crate::expr::FlowResult<Option<Value>> {
	let db_ctx = ctx.database().context("fetch_raw_record requires database context")?;
	let txn = db_ctx.txn();
	let record = txn
		.get_record(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			&rid.table,
			&rid.key,
			version,
		)
		.await
		.context("Failed to fetch record")?;
	if record.data.is_none() {
		return Ok(None);
	}

	Ok(Some(record.data.clone()))
}

/// Process a fetched record: check table-level permissions, evaluate computed
/// fields, and apply field-level permissions.
///
/// Returns `Ok(true)` if the record passes all permission checks, or
/// `Ok(false)` if the record should be hidden (Deny or failed Conditional).
/// Computed fields are always evaluated (even without permissions) using the
/// same `FieldState` + `compute_fields_for_value` path as the scan pipeline,
/// ensuring a single code path for computed field evaluation throughout the
/// executor.
pub(crate) async fn process_fetched_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
	val: &mut Value,
) -> crate::expr::FlowResult<bool> {
	let db_ctx = ctx.database().context("process_fetched_record requires database context")?;
	let check_perms =
		should_check_perms(db_ctx, Action::View).context("Failed to check permissions")?;

	// 1. Table-level permission check (on raw value, before computing fields)
	if check_perms {
		let table_def =
			db_ctx.get_table_def(&rid.table).await.context("Failed to get table definition")?;
		let catalog_perm = resolve_select_permission(table_def.as_deref());
		let select_perm = convert_permission_to_physical(catalog_perm, ctx.ctx())
			.await
			.context("Failed to convert permission")?;

		match &select_perm {
			PhysicalPermission::Deny => return Ok(false),
			PhysicalPermission::Allow => {}
			PhysicalPermission::Conditional(_) => {
				let allowed = check_permission_for_value(&select_perm, val, ctx)
					.await
					.context("Permission check failed")?;
				if !allowed {
					return Ok(false);
				}
			}
		}
	}

	// 2. Build FieldState (computed fields + field permissions)
	let field_state =
		super::scan::pipeline::build_field_state(ctx, &rid.table, check_perms, None).await?;

	// 3. Evaluate computed fields via the modern PhysicalExpr path
	super::scan::pipeline::compute_fields_for_value(ctx, &field_state, val, false).await?;

	// 4. Apply field-level permissions
	if check_perms {
		super::scan::pipeline::filter_fields_by_permission(ctx, &field_state, val).await?;
	}

	Ok(true)
}

/// Fetch a single record by its ID without permission checks, but with
/// computed fields evaluated.
///
/// Used during permission predicate evaluation to prevent reentrant
/// permission checks that would recurse infinitely on cyclic links.
/// Computed fields are still evaluated so that permission expressions
/// referencing computed fields on linked records work correctly.
pub(crate) async fn fetch_record_no_perms(
	ctx: &ExecutionContext,
	rid: &RecordId,
) -> crate::expr::FlowResult<Value> {
	let Some(mut val) = fetch_raw_record(ctx, rid, None).await? else {
		return Ok(Value::None);
	};
	let field_state =
		super::scan::pipeline::build_field_state(ctx, &rid.table, false, None).await?;
	super::scan::pipeline::compute_fields_for_value(ctx, &field_state, &mut val, true).await?;
	Ok(val)
}

/// Fetch a single record by its ID, evaluating computed fields and applying
/// table-level and field-level permission checks.
///
/// Uses the transaction record cache so repeated fetches of the same record
/// within a transaction only hit the datastore once.
pub(crate) async fn fetch_record(
	ctx: &ExecutionContext,
	rid: &RecordId,
) -> crate::expr::FlowResult<Value> {
	let Some(mut val) = fetch_raw_record(ctx, rid, None).await? else {
		return Ok(Value::None);
	};
	if !process_fetched_record(ctx, rid, &mut val).await? {
		return Ok(Value::None);
	}
	Ok(val)
}

/// Batch fetch multiple records by their IDs concurrently.
///
/// This function fetches multiple records in parallel using `try_join_all`,
/// which is more efficient than sequential fetching for larger batches.
/// The transaction cache will deduplicate repeated IDs automatically.
///
/// # Arguments
///
/// * `ctx` - The execution context containing the transaction
/// * `rids` - A slice of RecordIds to fetch
///
/// # Returns
///
/// A vector of Values in the same order as the input RecordIds.
/// Missing records are returned as `Value::None`.
pub(crate) async fn batch_fetch_records(
	ctx: &ExecutionContext,
	rids: &[RecordId],
) -> crate::expr::FlowResult<Vec<Value>> {
	if rids.is_empty() {
		return Ok(Vec::new());
	}

	// For small batches, sequential fetch may be more efficient
	// due to lower overhead. Threshold chosen empirically.
	const PARALLEL_THRESHOLD: usize = 4;

	if rids.len() < PARALLEL_THRESHOLD {
		// Sequential fetch for small batches
		let mut results = Vec::with_capacity(rids.len());
		for rid in rids {
			results.push(fetch_record(ctx, rid).await?);
		}
		return Ok(results);
	}

	// Parallel fetch for larger batches
	let futures: Vec<_> = rids.iter().map(|rid| fetch_record(ctx, rid)).collect();

	futures::future::try_join_all(futures).await
}

/// Batch fetch records and replace RecordIds in an array in place.
///
/// This is a convenience function that modifies an array of values,
/// replacing any RecordIds with their fetched record data.
///
/// # Arguments
///
/// * `ctx` - The execution context containing the transaction
/// * `values` - A mutable slice of Values to process
///
/// # Note
///
/// This function collects all RecordIds first, fetches them in batch,
/// then replaces them in the original array. Non-RecordId values are
/// left unchanged.
pub(crate) async fn batch_fetch_in_place(
	ctx: &ExecutionContext,
	values: &mut [Value],
) -> crate::expr::FlowResult<()> {
	// Collect indices and RecordIds to fetch
	let to_fetch: Vec<(usize, RecordId)> = values
		.iter()
		.enumerate()
		.filter_map(|(i, v)| {
			if let Value::RecordId(rid) = v {
				Some((i, rid.clone()))
			} else {
				None
			}
		})
		.collect();

	if to_fetch.is_empty() {
		return Ok(());
	}

	// Extract just the RecordIds for batch fetching
	let rids: Vec<RecordId> = to_fetch.iter().map(|(_, rid)| rid.clone()).collect();

	// Batch fetch all records
	let fetched = batch_fetch_records(ctx, &rids).await?;

	// Replace RecordIds with fetched values
	for ((idx, _), fetched_value) in to_fetch.into_iter().zip(fetched) {
		values[idx] = fetched_value;
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::physical_expr::Literal;

	#[test]
	fn test_fetch_name() {
		use crate::exec::operators::scan::DynamicScan;
		use crate::expr::part::Part;

		let fields = vec![Idiom(vec![Part::Field("author".into())])];

		// Create a minimal scan for testing
		let scan = Arc::new(DynamicScan::new(
			Arc::new(Literal(Value::from("test"))) as Arc<dyn crate::exec::PhysicalExpr>,
			None,
			None,
			None,
			None,
			None,
			None,
			None,
			None,
		));

		let fetch = Fetch::new(scan, fields);

		assert_eq!(fetch.name(), "Fetch");
	}
}
