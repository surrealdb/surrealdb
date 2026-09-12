use std::sync::Arc;

use futures::StreamExt;
use surrealdb_strand::Strand;

use crate::catalog::providers::TableProvider;
use crate::exec::fan_out::{RECORD_DEREFERENCE, evaluate_each};
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, should_check_perms,
	table_select_permission_physical_runtime, validate_record_user_access,
};
use crate::exec::physical_expr::{EvalContext, PhysicalExpr};
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, OutputShape, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlowExt;
use crate::expr::logical::LogicalExpr;
use crate::iam::Action;
use crate::val::{RecordId, TableName, Value};

/// A single step of a FETCH field path, walked structurally through the
/// row's value.
///
/// A computed index or key (`refs[0]`, `refs[$i]`) is compiled into a
/// `PhysicalExpr` at plan time instead of being compiled on every row. A step
/// the fetch cannot walk through ends its path: the value reached so far is
/// left as it is, unfetched.
#[derive(Debug, Clone)]
pub(crate) enum FetchStep {
	/// `.name`: the field of an object, or of every element of an array.
	Field(Strand),
	/// `.*`: every element of an array, every value of an object.
	All,
	/// The first element of an array.
	First,
	/// `[$]`: the last element of an array.
	Last,
	/// `[e]`: a computed index into an array or key into an object.
	Index(Arc<dyn PhysicalExpr>),
	/// A step the fetch does not walk through (a filter, a flatten, a method,
	/// a lookup, a destructure, a recursion, an optional chain, a path not
	/// rooted at a field of the row).
	Unsupported,
}

/// A [`FetchStep`] before its computed index is compiled: what
/// [`fetch_parts`] reads off a FETCH path for the planner to compile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FetchPart<'a> {
	Field(&'a Strand),
	All,
	First,
	Last,
	Index(&'a LogicalExpr),
	Unsupported,
}

/// The steps of a FETCH path, root first, up to and including the first one
/// the fetch cannot walk.
///
/// A fetch path names a stored position, so it starts from a field of the row
/// and continues through field accesses, `.*`, first/last and computed
/// indices. A `Broadcast` is its base's steps followed by its body's, since
/// the body continues the same written path over each element. Any other
/// form — another root, or a step the fetch cannot walk — is an
/// [`FetchPart::Unsupported`] step, and nothing after it is listed because
/// the walk ends there.
pub(crate) fn fetch_parts(expr: &LogicalExpr) -> Vec<FetchPart<'_>> {
	fn collect<'a>(expr: &'a LogicalExpr, out: &mut Vec<FetchPart<'a>>) {
		match expr {
			LogicalExpr::Field(name) => out.push(FetchPart::Field(name)),
			LogicalExpr::Current => {}
			LogicalExpr::Access {
				base,
				field,
			} => {
				collect(base, out);
				out.push(FetchPart::Field(field));
			}
			LogicalExpr::Each(base) => {
				collect(base, out);
				out.push(FetchPart::All);
			}
			LogicalExpr::First(base) => {
				collect(base, out);
				out.push(FetchPart::First);
			}
			LogicalExpr::Last(base) => {
				collect(base, out);
				out.push(FetchPart::Last);
			}
			LogicalExpr::Index {
				base,
				index,
			} => {
				collect(base, out);
				out.push(FetchPart::Index(index));
			}
			LogicalExpr::Broadcast {
				base,
				body,
				..
			} => {
				collect(base, out);
				collect(body, out);
			}
			other => {
				if let Some(base) = other.step_base() {
					collect(base, out);
				}
				out.push(FetchPart::Unsupported);
			}
		}
	}
	let mut out = Vec::new();
	collect(expr, &mut out);
	if let Some(end) = out.iter().position(|part| *part == FetchPart::Unsupported) {
		out.truncate(end + 1);
	}
	out
}

/// Fetches related records for specified fields.
///
/// The FETCH clause replaces record IDs with their full record data.
/// For example, if a field contains `author:tobie`, FETCH will replace
/// it with the full author record `{ id: author:tobie, name: 'Tobie', ... }`.
#[derive(Debug, Clone)]
pub struct Fetch {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// SQL rendering of the fetch paths, captured at plan time. Kept only for
	/// EXPLAIN's `attrs()`; execution walks `physical_fields`.
	pub(crate) fields_sql: String,
	/// The fetch paths to walk, each a sequence of steps root first (see
	/// [`fetch_parts`]).
	pub(crate) physical_fields: Vec<Vec<FetchStep>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Fetch {
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		fields_sql: String,
		physical_fields: Vec<Vec<FetchStep>>,
	) -> Self {
		Self {
			input,
			fields_sql,
			physical_fields,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}
impl ExecOperator for Fetch {
	fn name(&self) -> &'static str {
		"Fetch"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("fields".to_string(), self.fields_sql.clone())]
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

	fn output_shape(&self) -> OutputShape {
		self.input.output_shape()
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);
		let fields = self.physical_fields.clone();
		let ctx = ctx.clone();

		let fetch_stream = input_stream.then(move |batch_result| {
			let fields = fields.clone();
			let ctx = ctx.clone();

			async move {
				let batch = batch_result?;
				let mut fetched_values = Vec::with_capacity(batch.len());

				for value in batch.into_values() {
					let fetched = fetch_fields(&ctx, value, &fields).await?;
					fetched_values.push(fetched);
				}

				Ok(ValueBatch::new(fetched_values))
			}
		});

		Ok(monitor_stream(Box::pin(fetch_stream), "Fetch", &self.metrics))
	}
}

/// Fetch all specified fields for a value.
async fn fetch_fields(
	ctx: &ExecutionContext,
	mut value: Value,
	fields: &[Vec<FetchStep>],
) -> crate::expr::FlowResult<Value> {
	for field in fields {
		fetch_field_path(ctx, &mut value, field).await?;
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
	path: &'a [FetchStep],
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
				FetchStep::Field(name) => match current {
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
				FetchStep::All => match current {
					Value::Array(arr) => {
						return fetch_each(ctx, arr.iter_mut(), &path[depth + 1..]).await;
					}
					Value::Object(obj) => {
						return fetch_each(ctx, obj.values_mut(), &path[depth + 1..]).await;
					}
					_ => return Ok(()),
				},
				FetchStep::First => {
					current = match current {
						Value::Array(arr) => match arr.first_mut() {
							Some(v) => v,
							None => return Ok(()),
						},
						_ => return Ok(()),
					};
					depth += 1;
				}
				FetchStep::Last => {
					current = match current {
						Value::Array(arr) => match arr.last_mut() {
							Some(v) => v,
							None => return Ok(()),
						},
						_ => return Ok(()),
					};
					depth += 1;
				}
				// A computed index/key part, e.g. `refs[0]`, `refs[-1]`, `refs[$i]`.
				// Evaluate it, then mutably index into an array (numeric,
				// negative/out-of-range skips silently) or key into an object
				// (string or numeric-as-string key); anything else is a no-op,
				// matching the other unsupported-part fallthrough below.
				FetchStep::Index(phys_expr) => {
					let index_value = phys_expr.evaluate(EvalContext::from_exec_ctx(ctx)).await?;
					let next = match (&mut *current, &index_value) {
						(Value::Array(arr), Value::Number(n)) => {
							n.as_array_index().and_then(|idx| arr.get_mut(idx))
						}
						(Value::Object(obj), Value::String(key)) => obj.get_mut(key.as_str()),
						(Value::Object(obj), Value::Number(n)) => obj.get_mut(&n.to_string()),
						_ => None,
					};
					current = match next {
						Some(v) => v,
						None => return Ok(()),
					};
					depth += 1;
				}
				// The fetch does not walk through any other step.
				FetchStep::Unsupported => return Ok(()),
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
	remaining_path: &'a [FetchStep],
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

/// Fetch a single raw record value as a locked read, registering the
/// record key for commit-time conflict detection.
///
/// Reads exactly what [`fetch_raw_record`] reads at the current version,
/// except that the read is always issued to the underlying datastore —
/// never served from the transaction read cache — so the conflict
/// registration reaches the storage engine even for a record that does
/// not exist. There is no versioned form: a locked read of a historical
/// snapshot could never be validated at commit.
pub(crate) async fn fetch_raw_record_for_update(
	ctx: &ExecutionContext,
	rid: &RecordId,
) -> crate::expr::FlowResult<Option<Value>> {
	let db_ctx = ctx.database().context("fetch_raw_record_for_update requires database context")?;
	let txn = db_ctx.txn();
	let record = txn
		.get_record_for_update(
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			&rid.table,
			&rid.key,
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
	// SECURITY (GHSA-2v9j): confine record materialization to the caller's
	// tenant. This path serves record dereference and the FETCH clause, which
	// resolve records by id with no preceding scan, so it is the fetch-side
	// equivalent of the scan-operator tenant guard.
	validate_record_user_access(db_ctx)?;
	let check_perms =
		should_check_perms(db_ctx, Action::View).context("Failed to check permissions")?;

	// 1. Table-level permission check (on raw value, before computing fields)
	if check_perms {
		let select_perm = table_select_permission(ctx, &rid.table).await?;

		match &select_perm {
			PhysicalPermission::Deny => return Ok(false),
			PhysicalPermission::Allow => {}
			PhysicalPermission::Conditional(_) => {
				let allowed = check_permission_for_value(&select_perm, val, None, ctx)
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

	// 3. Evaluate computed fields and apply field-level permissions
	super::scan::pipeline::materialise_fields_with_permissions(
		ctx,
		&field_state,
		val,
		false,
		check_perms,
	)
	.await?;

	Ok(true)
}

/// The table `SELECT` permission a record of `table` read by id is checked
/// against, compiled once per table per execution and kept on the database
/// context; every record of the table fetched by this execution — a `FETCH`
/// target, a record dereference — is checked against the same compiled guard.
/// A versioned read compiles the permission of the definition at that stamp
/// and keeps nothing, as `get_table_def` does for the definition.
pub(crate) async fn table_select_permission(
	ctx: &ExecutionContext,
	table: &TableName,
) -> crate::expr::FlowResult<PhysicalPermission> {
	let db_ctx = ctx.database().context("table_select_permission requires database context")?;
	let version = ctx.version_stamp();
	if version.is_none()
		&& let Some(cached) = db_ctx.select_permission_cache.read().await.get(table)
	{
		return Ok(cached.clone());
	}
	let table_def =
		db_ctx.get_table_def(table, version).await.context("Failed to get table definition")?;
	let permission = table_select_permission_physical_runtime(table_def.as_deref(), ctx)
		.await
		.context("Failed to convert permission")?;
	if version.is_none() {
		db_ctx.select_permission_cache.write().await.insert(table.clone(), permission.clone());
	}
	Ok(permission)
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
	let Some(mut val) = fetch_raw_record(ctx, rid, ctx.version_stamp()).await? else {
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
	let Some(mut val) = fetch_raw_record(ctx, rid, ctx.version_stamp()).await? else {
		return Ok(Value::None);
	};
	if !process_fetched_record(ctx, rid, &mut val).await? {
		return Ok(Value::None);
	}
	Ok(val)
}

/// Fetch several records by id, overlapping the fetches.
///
/// The transaction cache deduplicates repeated ids automatically. The only
/// expressions that run are each target's own, which is what
/// [`RECORD_DEREFERENCE`] names.
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
	evaluate_each(ctx, RECORD_DEREFERENCE, rids, |rid| fetch_record(ctx, rid)).await
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
	use crate::exec::operators::test_util::{parse_expr, parse_path};
	use crate::exec::physical_expr::Literal;

	#[test]
	fn a_fetch_path_is_read_root_first_through_its_broadcasts() {
		let path = parse_path("a.b[0].c.*[$i]");
		let parts = fetch_parts(&path);
		let index = match parts.as_slice() {
			[
				FetchPart::Field(a),
				FetchPart::Field(b),
				FetchPart::Index(index),
				FetchPart::Field(c),
				FetchPart::All,
				FetchPart::Index(i),
			] => {
				assert_eq!(a.as_str(), "a");
				assert_eq!(b.as_str(), "b");
				assert_eq!(c.as_str(), "c");
				assert_eq!(**i, parse_expr("$i"));
				index
			}
			other => panic!("unexpected steps {other:?}"),
		};
		assert_eq!(**index, LogicalExpr::Literal(crate::expr::logical::LogicalLiteral::Integer(0)));
	}

	#[test]
	fn a_step_the_fetch_cannot_walk_ends_the_path() {
		assert_eq!(
			fetch_parts(&parse_path("a[WHERE x].b")),
			vec![FetchPart::Field(&Strand::new_static("a")), FetchPart::Unsupported]
		);
		assert_eq!(
			fetch_parts(&parse_path("a[$].b")),
			vec![
				FetchPart::Field(&Strand::new_static("a")),
				FetchPart::Last,
				FetchPart::Field(&Strand::new_static("b"))
			]
		);
		// A path not rooted at a field of the row fetches nothing.
		assert_eq!(fetch_parts(&parse_path("$p.a")), vec![FetchPart::Unsupported]);
	}

	/// The guard of a table is compiled once per execution: two records of
	/// the same table are checked against the same compiled predicate.
	#[tokio::test]
	async fn a_tables_guard_is_compiled_once_per_execution() {
		use crate::exec::operators::test_util::TestDb;
		let db = TestDb::new_with_auth(
			"DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE public = true;
			 DEFINE TABLE other SCHEMALESS PERMISSIONS FOR select WHERE public = true;",
		)
		.await;
		let anonymous = crate::dbs::Session::default().with_ns("test").with_db("test");
		let ctx = db.exec_ctx_as(&anonymous, crate::kvs::TransactionType::Read).await;
		let doc = TableName::from("doc");
		let first = table_select_permission(&ctx, &doc).await.expect("compiles");
		let second = table_select_permission(&ctx, &doc).await.expect("cached");
		let (PhysicalPermission::Conditional(first), PhysicalPermission::Conditional(second)) =
			(&first, &second)
		else {
			panic!("a conditional guard: {first:?} / {second:?}");
		};
		assert!(Arc::ptr_eq(first, second), "the second fetch reuses the compiled guard");
		// Another table compiles its own.
		let other =
			table_select_permission(&ctx, &TableName::from("other")).await.expect("compiles");
		let PhysicalPermission::Conditional(other) = &other else {
			panic!("a conditional guard: {other:?}");
		};
		assert!(!Arc::ptr_eq(first, other));
		// A context sharing the execution shares the cache.
		let shared = ctx.with_param("x", Value::from(1));
		let again = table_select_permission(&shared, &doc).await.expect("cached");
		let PhysicalPermission::Conditional(again) = &again else {
			panic!("a conditional guard: {again:?}");
		};
		assert!(Arc::ptr_eq(first, again));
	}

	#[test]
	fn test_fetch_name() {
		use crate::exec::operators::scan::DynamicScan;

		let physical_fields = vec![vec![FetchStep::Field(Strand::new_static("author"))]];

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

		let fetch = Fetch::new(scan, "author".to_string(), physical_fields);

		assert_eq!(fetch.name(), "Fetch");
	}
}
