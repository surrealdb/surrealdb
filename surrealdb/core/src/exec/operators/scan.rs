use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::instrument;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Permission};
use crate::err::Error;
use crate::exec::index::access_path::{AccessPath, select_access_path};
use crate::exec::index::analysis::IndexAnalyzer;
use crate::exec::operators::{FullTextScan, IndexScan};
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms, validate_record_user_access,
};
use crate::exec::planner::expr_to_physical_expr;
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	PhysicalExpr, ValueBatch, ValueBatchStream, instrument_stream,
};
use crate::expr::order::Ordering;
use crate::expr::with::With;
use crate::expr::{Cond, ControlFlow};
use crate::iam::Action;
use crate::idx::planner::ScanDirection;
use crate::key::record;
use crate::kvs::{KVKey, KVValue};
use crate::val::{RecordIdKey, TableName, Value};

/// Batch size for collecting records before yielding.
const BATCH_SIZE: usize = 1000;

/// Check if a value passes the permission check.
///
/// Inlined at each call site so the `Allow`/`Deny` branches are pure synchronous
/// code with zero async state-machine overhead. The `.await` only exists in the
/// `Conditional` arm.
macro_rules! check_perm {
	($permission:expr, $value:expr, $ctx:expr) => {
		match $permission {
			PhysicalPermission::Allow => Ok::<bool, ControlFlow>(true),
			PhysicalPermission::Deny => Ok(false),
			PhysicalPermission::Conditional(expr) => {
				let eval_ctx = EvalContext::from_exec_ctx($ctx).with_value($value);
				expr.evaluate(eval_ctx).await.map(|v| v.is_truthy()).map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}"))
				})
			}
		}
	};
}

/// Full table scan - iterates over all records in a table.
///
/// Requires database-level context since it reads from a specific table
/// in the selected namespace and database.
///
/// Permission checking is performed at execution time by resolving the table
/// definition from the current transaction's schema view and filtering records
/// based on the SELECT permission.
///
/// When scanning a table, this operator can perform index selection based on
/// the provided WHERE condition, ORDER BY clause, and WITH hints.
#[derive(Debug, Clone)]
pub struct Scan {
	pub(crate) source: Arc<dyn PhysicalExpr>,
	/// Optional version timestamp for time-travel queries (VERSION clause)
	pub(crate) version: Option<u64>,
	/// Optional WHERE condition for index selection
	pub(crate) cond: Option<Cond>,
	/// Optional ORDER BY for index selection
	pub(crate) order: Option<Ordering>,
	/// Optional WITH INDEX/NOINDEX hints
	pub(crate) with: Option<With>,
	/// Fields needed by the query (projection + WHERE + ORDER + GROUP).
	/// `None` means all fields are needed (SELECT *).
	pub(crate) needed_fields: Option<std::collections::HashSet<String>>,
}

#[async_trait]
impl ExecOperator for Scan {
	fn name(&self) -> &'static str {
		"Scan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("source".to_string(), self.source.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		// Scan is read-only, but the source expression could contain a subquery containing a
		// mutation.
		self.source.access_mode()
	}

	#[instrument(name = "Scan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		// Get database context - we declared Database level, so this should succeed
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let source_expr = Arc::clone(&self.source);
		let version = self.version;
		let cond = self.cond.clone();
		let order = self.order.clone();
		let with = self.with.clone();
		let needed_fields = self.needed_fields.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);

			// Evaluate table expression
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let table_value = source_expr.evaluate(eval_ctx).await?;

			// Determine scan target: either a table name or a record ID
			let (table_name, rid) = match table_value {
				Value::Table(t) => (t, None),
				Value::RecordId(rid) => (rid.table.clone(), Some(rid)),
				Value::Array(arr) => {
					yield ValueBatch { values: arr.0 };
					return;
				}
				// For any other value type, yield as a single row.
				// This matches legacy FROM behavior for non-table values.
				other => {
					yield ValueBatch { values: vec![other] };
					return;
				}
			};

			// Check table existence and resolve SELECT permission
			let table_def = txn
				.get_tb_by_name(&ns.name, &db.name, &table_name)
				.await
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get table: {e}")))?;

			if table_def.is_none() {
				Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
					name: table_name.clone(),
				})))?;
			}

			let select_permission = if check_perms {
				let catalog_perm = match &table_def {
					Some(def) => def.permissions.select.clone(),
					None => Permission::None,
				};
				convert_permission_to_physical(&catalog_perm, ctx.ctx()).map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("Failed to convert permission: {e}"))
				})?
			} else {
				PhysicalPermission::Allow
			};

			// Early exit if denied
			if matches!(select_permission, PhysicalPermission::Deny) {
				return;
			}

			// Eagerly initialize field state (computed fields + field permissions)
			let field_state = build_field_state(&ctx, &table_name, check_perms, needed_fields.as_ref()).await?;

			// Pre-compute whether any post-decode processing is needed.
			// When false, the scan loop can skip filter/process calls entirely
			// (zero async overhead beyond the KV stream poll).
			let needs_processing = !matches!(select_permission, PhysicalPermission::Allow)
				|| !field_state.computed_fields.is_empty()
				|| (check_perms && !field_state.field_permissions.is_empty());

			match rid {
				// === POINT LOOKUP (single record by ID) ===
				Some(rid) if !matches!(rid.key, RecordIdKey::Range(_)) => {
					let record = txn
						.get_record(ns.namespace_id, db.database_id, &rid.table, &rid.key, version)
						.await
						.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get record: {e}")))?;

					if record.data.as_ref().is_none() {
						return;
					}

					let mut value = record.data.as_ref().clone();
					value.def(&rid);

					if check_perm!(&select_permission, &value, &ctx)? {
						let mut batch = vec![value];
						process_batch(&ctx, &field_state, check_perms, &mut batch).await?;
						yield ValueBatch { values: batch };
					}
				}

				// === RANGE SCAN (record ID with range key) ===
				Some(rid) => {
					let RecordIdKey::Range(range) = &rid.key else { unreachable!() };

					let beg = range_start_key(ns.namespace_id, db.database_id, &rid.table, &range.start)?;
					let end = range_end_key(ns.namespace_id, db.database_id, &rid.table, &range.end)?;

					let kv_stream = txn.stream_keys_vals(beg..end, version, None, ScanDirection::Forward);
					let chunks = kv_stream.ready_chunks(BATCH_SIZE);
					futures::pin_mut!(chunks);

					if needs_processing {
						while let Some(chunk) = chunks.next().await {
							let mut batch = Vec::with_capacity(chunk.len());
							for result in chunk {
								let (key, val) = result
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to scan record: {e}")))?;
								batch.push(decode_record(&key, val)?);
							}
							filter_and_process_batch(&mut batch, &select_permission, &ctx, &field_state, check_perms).await?;
							if !batch.is_empty() {
								yield ValueBatch { values: batch };
							}
						}
					} else {
						// Fast path: no permission filtering, no computed fields
						while let Some(chunk) = chunks.next().await {
							let mut batch = Vec::with_capacity(chunk.len());
							for result in chunk {
								let (key, val) = result
									.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to scan record: {e}")))?;
								batch.push(decode_record(&key, val)?);
							}
							yield ValueBatch { values: batch };
						}
					}
				}

				// === TABLE SCAN (with optional index selection) ===
				None => {
					// Determine if we should use an index
					let access_path = if matches!(&with, Some(With::NoIndex)) {
						None
					} else {
						let indexes = txn
							.all_tb_indexes(ns.namespace_id, db.database_id, &table_name)
							.await
							.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to fetch indexes: {e}")))?;

						let analyzer = IndexAnalyzer::new(&table_name, indexes, with.as_ref());
						let candidates = analyzer.analyze(cond.as_ref(), order.as_ref());
						let direction = determine_scan_direction(&order);
						Some(select_access_path(table_name.clone(), candidates, with.as_ref(), direction))
					};

					match access_path {
						// B-tree index scan (single-column only)
						Some(AccessPath::BTreeScan { index_ref, access, direction }) if index_ref.cols.len() == 1 => {
							let operator = IndexScan { index_ref, access, direction, table_name: table_name.clone() };
							let mut stream = operator.execute(&ctx)?;
							while let Some(batch_result) = stream.next().await {
								let mut batch = batch_result?;
								process_batch(&ctx, &field_state, check_perms, &mut batch.values).await?;
								yield batch;
							}
						}

						// Full-text search
						Some(AccessPath::FullTextSearch { index_ref, query, operator }) => {
							let ft_op = FullTextScan { index_ref, query, operator, table_name: table_name.clone() };
							let mut stream = ft_op.execute(&ctx)?;
							while let Some(batch_result) = stream.next().await {
								let mut batch = batch_result?;
								process_batch(&ctx, &field_state, check_perms, &mut batch.values).await?;
								yield batch;
							}
						}

						// Fall back to table scan (NOINDEX, compound indexes, KNN, etc.)
						_ => {
							let beg = record::prefix(ns.namespace_id, db.database_id, &table_name)
								.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix key: {e}")))?;
							let end = record::suffix(ns.namespace_id, db.database_id, &table_name)
								.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix key: {e}")))?;

							let kv_stream = txn.stream_keys_vals(beg..end, version, None, ScanDirection::Forward);
							let chunks = kv_stream.ready_chunks(BATCH_SIZE);
							futures::pin_mut!(chunks);

							if needs_processing {
								while let Some(chunk) = chunks.next().await {
									let mut batch = Vec::with_capacity(chunk.len());
									for result in chunk {
										let (key, val) = result
											.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to scan record: {e}")))?;
										batch.push(decode_record(&key, val)?);
									}
									filter_and_process_batch(&mut batch, &select_permission, &ctx, &field_state, check_perms).await?;
									if !batch.is_empty() {
										yield ValueBatch { values: batch };
									}
								}
							} else {
								// Fast path: no permission filtering, no computed fields
								while let Some(chunk) = chunks.next().await {
									let mut batch = Vec::with_capacity(chunk.len());
									for result in chunk {
										let (key, val) = result
											.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to scan record: {e}")))?;
										batch.push(decode_record(&key, val)?);
									}
									yield ValueBatch { values: batch };
								}
							}
						}
					}
				}
			}
		};

		Ok(instrument_stream(Box::pin(stream), "Scan"))
	}
}

/// Determine scan direction from ORDER BY clause.
/// Returns Backward if the first ORDER BY is `id DESC`, otherwise Forward.
fn determine_scan_direction(order: &Option<Ordering>) -> ScanDirection {
	use crate::expr::order::Ordering as OrderingType;
	if let Some(OrderingType::Order(order_list)) = order
		&& let Some(first) = order_list.0.first()
		&& !first.direction
		&& first.value.is_id()
	{
		ScanDirection::Backward
	} else {
		ScanDirection::Forward
	}
}

/// Combined single-pass filter and process for a batch of decoded values.
///
/// Performs table-level permission filtering, computed field evaluation,
/// and field-level permission filtering in a single iteration over the batch.
/// Records that fail the table-level permission check are skipped entirely,
/// avoiding wasted compute on records that will be filtered out.
async fn filter_and_process_batch(
	batch: &mut Vec<Value>,
	permission: &PhysicalPermission,
	ctx: &ExecutionContext,
	state: &FieldState,
	check_perms: bool,
) -> Result<(), ControlFlow> {
	let needs_perm_filter = !matches!(permission, PhysicalPermission::Allow);
	let mut write_idx = 0;
	for read_idx in 0..batch.len() {
		// Check table-level permission (skip if Allow)
		if needs_perm_filter && !check_perm!(permission, &batch[read_idx], ctx)? {
			continue;
		}
		// Move to write position
		if write_idx != read_idx {
			batch.swap(write_idx, read_idx);
		}
		// Process in-place: computed fields + field permissions
		compute_fields_for_value(ctx, state, &mut batch[write_idx]).await?;
		if check_perms {
			filter_fields_by_permission(ctx, state, &mut batch[write_idx]).await?;
		}
		write_idx += 1;
	}
	batch.truncate(write_idx);
	Ok(())
}

/// Compute the start key for a range scan.
fn range_start_key(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	bound: &Bound<RecordIdKey>,
) -> Result<crate::kvs::Key, ControlFlow> {
	match bound {
		Bound::Unbounded => record::prefix(ns_id, db_id, table)
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create prefix key: {e}"))),
		Bound::Included(v) => record::new(ns_id, db_id, table, v)
			.encode_key()
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create begin key: {e}"))),
		Bound::Excluded(v) => {
			let mut key = record::new(ns_id, db_id, table, v).encode_key().map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!("Failed to create begin key: {e}"))
			})?;
			key.push(0x00);
			Ok(key)
		}
	}
}

/// Compute the end key for a range scan.
fn range_end_key(
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table: &TableName,
	bound: &Bound<RecordIdKey>,
) -> Result<crate::kvs::Key, ControlFlow> {
	match bound {
		Bound::Unbounded => record::suffix(ns_id, db_id, table)
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create suffix key: {e}"))),
		Bound::Excluded(v) => record::new(ns_id, db_id, table, v)
			.encode_key()
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create end key: {e}"))),
		Bound::Included(v) => {
			let mut key = record::new(ns_id, db_id, table, v)
				.encode_key()
				.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to create end key: {e}")))?;
			key.push(0x00);
			Ok(key)
		}
	}
}

/// Decode a record from its key and value bytes.
#[inline]
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

	// Take ownership of the value (zero-cost move for freshly deserialized Mutable data)
	Ok(record.data.into_value())
}

/// Cached state for field processing (computed fields and permissions).
/// Initialized on first batch and reused for subsequent batches.
#[derive(Debug)]
struct FieldState {
	/// Computed field definitions converted to physical expressions
	computed_fields: Vec<ComputedFieldDef>,
	/// Field-level permissions (field name -> permission)
	field_permissions: HashMap<String, PhysicalPermission>,
}

/// A computed field definition ready for evaluation.
#[derive(Debug)]
struct ComputedFieldDef {
	/// The field name where to store the result
	field_name: String,
	/// The physical expression to evaluate
	expr: Arc<dyn PhysicalExpr>,
	/// Optional type coercion
	kind: Option<crate::expr::Kind>,
}

/// Fetch field definitions and build the cached field state.
#[allow(clippy::type_complexity)]
async fn build_field_state(
	ctx: &ExecutionContext,
	table_name: &TableName,
	check_perms: bool,
	needed_fields: Option<&std::collections::HashSet<String>>,
) -> Result<FieldState, ControlFlow> {
	let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let txn = ctx.txn();

	let field_defs = txn
		.all_tb_fields(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, table_name, None)
		.await
		.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get field definitions: {}", e)))?;

	// Collect computed fields and their dependency metadata
	let mut raw_computed: Vec<(
		String,
		Arc<dyn PhysicalExpr>,
		Option<crate::expr::Kind>,
		Vec<String>,
	)> = Vec::new();
	let mut dep_map: HashMap<String, crate::expr::computed_deps::ComputedDeps> = HashMap::new();

	for fd in field_defs.iter() {
		if let Some(ref expr) = fd.computed {
			let field_name = fd.name.to_raw_string();

			// Get deps: use stored deps, or extract on the fly for legacy fields
			let deps = if let Some(ref cd) = fd.computed_deps {
				crate::expr::computed_deps::ComputedDeps {
					fields: cd.fields.clone(),
					is_complete: cd.is_complete,
				}
			} else {
				crate::expr::computed_deps::extract_computed_deps(expr)
			};

			dep_map.insert(field_name.clone(), deps.clone());

			let physical_expr = expr_to_physical_expr(expr.clone(), ctx.ctx()).map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!(
					"Computed field '{}' has unsupported expression: {}",
					field_name,
					e
				))
			})?;

			raw_computed.push((field_name, physical_expr, fd.field_kind.clone(), deps.fields));
		}
	}

	// Determine which computed fields are actually needed
	let needed_computed = if let Some(needed) = needed_fields {
		crate::expr::computed_deps::resolve_required_computed_fields(needed, &dep_map)
	} else {
		// SELECT * -- compute all
		None
	};

	// Filter to only needed computed fields
	let filtered: Vec<_> = if let Some(ref required) = needed_computed {
		raw_computed.into_iter().filter(|(name, _, _, _)| required.contains(name)).collect()
	} else {
		raw_computed
	};

	// Topologically sort for correct evaluation order
	let topo_input: Vec<(String, Vec<String>)> =
		filtered.iter().map(|(name, _, _, deps)| (name.clone(), deps.clone())).collect();
	let sorted_indices = crate::expr::computed_deps::topological_sort_computed_fields(&topo_input);

	let mut computed_fields = Vec::with_capacity(sorted_indices.len());
	for idx in sorted_indices {
		let (field_name, expr, kind, _) = &filtered[idx];
		computed_fields.push(ComputedFieldDef {
			field_name: field_name.clone(),
			expr: Arc::clone(expr),
			kind: kind.clone(),
		});
	}

	// Build field permissions
	let mut field_permissions = HashMap::new();
	if check_perms {
		for fd in field_defs.iter() {
			let field_name = fd.name.to_raw_string();
			let physical_perm = convert_permission_to_physical(&fd.select_permission, ctx.ctx())
				.map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("Failed to convert field permission: {}", e))
				})?;
			field_permissions.insert(field_name, physical_perm);
		}
	}

	Ok(FieldState {
		computed_fields,
		field_permissions,
	})
}

/// Process a batch of values: evaluate computed fields and apply field-level permissions.
async fn process_batch(
	ctx: &ExecutionContext,
	state: &FieldState,
	check_perms: bool,
	values: &mut [Value],
) -> Result<(), ControlFlow> {
	for value in values.iter_mut() {
		// Evaluate computed fields
		compute_fields_for_value(ctx, state, value).await?;

		// Apply field-level permissions
		if check_perms {
			filter_fields_by_permission(ctx, state, value).await?;
		}
	}

	Ok(())
}

/// Compute all computed fields for a single value.
async fn compute_fields_for_value(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
) -> Result<(), ControlFlow> {
	if state.computed_fields.is_empty() {
		return Ok(());
	}

	let eval_ctx = EvalContext::from_exec_ctx(ctx);

	for cf in &state.computed_fields {
		// Evaluate with the current value as context
		let row_ctx = eval_ctx.with_value(value);
		let computed_value = match cf.expr.evaluate(row_ctx).await {
			Ok(v) => v,
			Err(ControlFlow::Return(v)) => v,
			Err(e) => return Err(e),
		};

		// Apply type coercion if specified
		let final_value = if let Some(kind) = &cf.kind {
			computed_value.coerce_to_kind(kind).map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!(
					"Failed to coerce computed field '{}': {}",
					cf.field_name,
					e
				))
			})?
		} else {
			computed_value
		};

		// Inject the computed value into the document
		if let Value::Object(obj) = value {
			obj.insert(cf.field_name.clone(), final_value);
		} else {
			return Err(ControlFlow::Err(anyhow::anyhow!("Value is not an object: {:?}", value)));
		}
	}

	Ok(())
}

/// Filter fields from a value based on field-level permissions.
async fn filter_fields_by_permission(
	ctx: &ExecutionContext,
	state: &FieldState,
	value: &mut Value,
) -> Result<(), ControlFlow> {
	if state.field_permissions.is_empty() {
		return Ok(());
	}

	// Collect fields to check
	let field_names: Vec<String> = {
		let Value::Object(obj) = &*value else {
			return Ok(());
		};
		obj.keys().cloned().collect()
	};

	for field_name in field_names {
		// Check if there's a permission for this field
		if let Some(perm) = state.field_permissions.get(&field_name) {
			let allowed = check_permission_for_value(perm, &*value, ctx).await.map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!("Failed to check field permission: {}", e))
			})?;

			if !allowed && let Value::Object(obj) = value {
				obj.remove(&field_name);
			}
		}
	}

	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::ctx::Context;
	use crate::exec::planner::expr_to_physical_expr;

	/// Helper to create a Scan with all fields for testing
	fn create_test_scan(table_name: &str, with_index_hints: bool) -> Scan {
		let ctx = std::sync::Arc::new(Context::background());
		let source = expr_to_physical_expr(
			crate::expr::Expr::Literal(crate::expr::literal::Literal::String(
				table_name.to_string(),
			)),
			&ctx,
		)
		.expect("Failed to create physical expression");

		Scan {
			source,
			version: None,
			cond: None,
			order: None,
			with: if with_index_hints {
				Some(With::NoIndex)
			} else {
				None
			},
			needed_fields: None,
		}
	}

	#[test]
	fn test_scan_struct_with_index_fields() {
		// Test that Scan can be created with all fields
		let scan = create_test_scan("test_table", false);
		assert!(scan.cond.is_none());
		assert!(scan.order.is_none());
		assert!(scan.with.is_none());
	}

	#[test]
	fn test_scan_struct_with_noindex_hint() {
		// Test that Scan can be created with WITH NOINDEX
		let scan = create_test_scan("test_table", true);
		assert!(scan.with.is_some());
		assert!(matches!(scan.with, Some(With::NoIndex)));
	}

	#[test]
	fn test_scan_operator_name() {
		let scan = create_test_scan("test_table", false);
		assert_eq!(scan.name(), "Scan");
	}

	#[test]
	fn test_scan_required_context() {
		let scan = create_test_scan("test_table", false);
		assert!(matches!(scan.required_context(), ContextLevel::Database));
	}

	#[test]
	fn test_determine_scan_direction_no_order() {
		// No order -> Forward
		let direction = determine_scan_direction(&None);
		assert!(matches!(direction, ScanDirection::Forward));
	}

	#[test]
	fn test_determine_scan_direction_random_order() {
		use crate::expr::order::Ordering;

		// Random order -> Forward
		let order = Ordering::Random;
		let direction = determine_scan_direction(&Some(order));
		assert!(matches!(direction, ScanDirection::Forward));
	}
}
