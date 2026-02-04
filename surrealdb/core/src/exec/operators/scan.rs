use std::collections::HashMap;
use std::ops::Bound;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::catalog::Permission;
use crate::catalog::providers::TableProvider;
use crate::err::Error;
use crate::exec::index::access_path::{AccessPath, BTreeAccess, IndexRef, select_access_path};
use crate::exec::index::analysis::IndexAnalyzer;
use crate::exec::operators::{FullTextScan, IndexScan};
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, convert_permission_to_physical,
	should_check_perms, validate_record_user_access,
};
use crate::exec::physical_expr::ReturnValue;
use crate::exec::planner::expr_to_physical_expr;
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	PhysicalExpr, ValueBatch, ValueBatchStream,
};
use crate::expr::order::Ordering;
use crate::expr::with::With;
use crate::expr::{Cond, ControlFlow};
use crate::iam::Action;
use crate::idx::planner::ScanDirection;
use crate::key::record;
use crate::kvs::{KVKey, KVValue, Key};
use crate::val::{RecordId, RecordIdKey, TableName, Value};

/// Batch size for collecting records before yielding.
const BATCH_SIZE: usize = 1000;

/// Represents the target of a scan operation.
enum ScanTarget {
	/// Scan all records in a table
	Table(TableName),
	/// Scan a specific record or range by RecordId
	RecordId(RecordId),
}

impl ScanTarget {
	/// Get the table name for permission lookup
	fn table_name(&self) -> TableName {
		match self {
			ScanTarget::Table(t) => t.clone(),
			ScanTarget::RecordId(rid) => rid.table.clone(),
		}
	}
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
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let mut executor = ScanExecutor::init(&ctx, source_expr, version, check_perms, cond, order, with).await?;

			while let Some(batch) = executor.next_batch().await? {
				yield batch;
			}
		};

		Ok(Box::pin(stream))
	}
}

/// Decode a record from its key and value bytes.
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

	Ok(record.data.as_ref().clone())
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

/// The active scan mode with its associated state
enum ScanMode {
	/// KV stream iteration (used for both table and range scans)
	/// Stores the key range (begin, end)
	KvStream {
		beg: Key,
		end: Key,
	},
	/// Single record point lookup
	PointLookup {
		rid: RecordId,
		done: bool,
	},
	/// Delegate to an index operator (IndexScan, FullTextScan, KnnScan)
	IndexDelegate(IndexDelegateMode),
	/// Scan complete
	Done,
}

/// Mode for delegating to specialized index operators.
#[derive(Clone)]
enum IndexDelegateMode {
	/// B-tree index scan (Idx or Uniq)
	BTree {
		index_ref: IndexRef,
		access: BTreeAccess,
		direction: ScanDirection,
		table_name: TableName,
	},
	/// Full-text search scan
	FullText {
		index_ref: IndexRef,
		query: String,
		operator: crate::expr::operator::MatchesOperator,
		table_name: TableName,
	},
	// NOTE: Future variants when operators are implemented:
	// /// KNN vector search scan
	// Knn { index_ref, vector, k, ef, table_name },
}

/// Executes a scan operation, managing state across batch yields.
struct ScanExecutor {
	ctx: ExecutionContext,
	table_name: TableName,
	select_permission: PhysicalPermission,
	check_perms: bool,
	version: Option<u64>,
	field_state: Option<FieldState>,
	batch: Vec<Value>,
	mode: ScanMode,
}

impl ScanExecutor {
	/// Initialize a new ScanExecutor by evaluating the source expression,
	/// resolving permissions, and creating the appropriate scan mode.
	///
	/// When the source is a table, index selection is performed based on
	/// the provided WHERE condition, ORDER BY clause, and WITH hints.
	async fn init(
		ctx: &ExecutionContext,
		source_expr: Arc<dyn PhysicalExpr>,
		version: Option<u64>,
		check_perms: bool,
		cond: Option<Cond>,
		order: Option<Ordering>,
		with: Option<With>,
	) -> Result<Self, ControlFlow> {
		let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
		let txn = Arc::clone(ctx.txn());
		let ns = Arc::clone(&db_ctx.ns_ctx.ns);
		let db = Arc::clone(&db_ctx.db);

		// Evaluate table expression
		let eval_ctx = EvalContext::from_exec_ctx(ctx);
		let table_value = source_expr.evaluate(eval_ctx).await.map_err(|e| {
			ControlFlow::Err(anyhow::anyhow!("Failed to evaluate table expression: {e}"))
		})?;

		// Determine scan target: either a table name or a record ID
		let scan_target = match table_value {
			Value::String(s) => ScanTarget::Table(TableName::from(s)),
			Value::Table(t) => ScanTarget::Table(t),
			Value::RecordId(rid) => ScanTarget::RecordId(rid),
			_ => {
				return Err(ControlFlow::Err(anyhow::anyhow!(
					"Table expression must evaluate to a string, table, or record ID, got: {:?}",
					table_value
				)));
			}
		};

		// Get table name for permission lookup
		let table_name = scan_target.table_name();

		// Check table existence and resolve SELECT permission
		let table_def = txn
			.get_tb_by_name(&ns.name, &db.name, &table_name)
			.await
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get table: {e}")))?;

		// For SELECT queries, the table must exist (unless it's schemaless and we allow creation)
		// Check if the table is defined; if not, return TbNotFound error
		if table_def.is_none() {
			return Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
				name: table_name.clone(),
			})));
		}

		let select_permission = if check_perms {
			let catalog_perm = match &table_def {
				Some(def) => def.permissions.select.clone(),
				None => Permission::None, // Should not reach here after above check
			};

			convert_permission_to_physical(&catalog_perm, ctx.ctx()).map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!("Failed to convert permission: {e}"))
			})?
		} else {
			PhysicalPermission::Allow
		};

		// Early exit if denied - mode is Done
		if matches!(select_permission, PhysicalPermission::Deny) {
			return Ok(Self {
				ctx: ctx.clone(),
				table_name,
				select_permission,
				check_perms,
				version,
				field_state: None,
				batch: Vec::new(),
				mode: ScanMode::Done,
			});
		}

		// Create the appropriate scan mode
		let mode = match scan_target {
			ScanTarget::Table(table_name_ref) => {
				// Check for WITH NOINDEX - skip index selection and do full table scan
				if matches!(&with, Some(With::NoIndex)) {
					let beg = record::prefix(ns.namespace_id, db.database_id, &table_name_ref)
						.map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!("Failed to create prefix key: {e}"))
						})?;
					let end = record::suffix(ns.namespace_id, db.database_id, &table_name_ref)
						.map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!("Failed to create suffix key: {e}"))
						})?;
					ScanMode::KvStream {
						beg,
						end,
					}
				} else {
					// Fetch indexes for this table
					let indexes = txn
						.all_tb_indexes(ns.namespace_id, db.database_id, &table_name_ref)
						.await
						.map_err(|e| {
							ControlFlow::Err(anyhow::anyhow!("Failed to fetch indexes: {e}"))
						})?;

					// Analyze and select access path
					let analyzer = IndexAnalyzer::new(&table_name_ref, indexes, with.as_ref());
					let candidates = analyzer.analyze(cond.as_ref(), order.as_ref());

					// Determine scan direction from ORDER BY
					let direction = Self::determine_scan_direction(&order);

					// Select the best access path
					let access_path = select_access_path(
						table_name_ref.clone(),
						candidates,
						with.as_ref(),
						direction,
					);

					// Create appropriate scan mode based on access path
					match access_path {
						AccessPath::BTreeScan {
							index_ref,
							access,
							direction,
						} => {
							// Check if this is a compound index (multiple columns).
							// The IndexScan operator currently only supports single-column
							// indexes properly. For compound indexes, fall back to table scan.
							let is_compound_index = index_ref.cols.len() > 1;
							if is_compound_index {
								// Fall back to table scan for compound indexes
								let beg = record::prefix(
									ns.namespace_id,
									db.database_id,
									&table_name_ref,
								)
								.map_err(|e| {
									ControlFlow::Err(anyhow::anyhow!(
										"Failed to create prefix key: {e}"
									))
								})?;
								let end = record::suffix(
									ns.namespace_id,
									db.database_id,
									&table_name_ref,
								)
								.map_err(|e| {
									ControlFlow::Err(anyhow::anyhow!(
										"Failed to create suffix key: {e}"
									))
								})?;
								ScanMode::KvStream {
									beg,
									end,
								}
							} else {
								// IndexScan operator is implemented for single-column indexes
								ScanMode::IndexDelegate(IndexDelegateMode::BTree {
									index_ref,
									access,
									direction,
									table_name: table_name_ref,
								})
							}
						}
						AccessPath::FullTextSearch {
							index_ref,
							query,
							operator,
						} => ScanMode::IndexDelegate(IndexDelegateMode::FullText {
							index_ref,
							query,
							operator,
							table_name: table_name_ref,
						}),
						// NOTE: KnnScan operator is not yet implemented.
						// For now, fall back to table scan.
						// AccessPath::KnnSearch { .. } => ScanMode::IndexDelegate(...)
						_ => {
							// Fall back to KV stream for:
							// - TableScan
							// - KnnSearch (operator not implemented)
							// - PointLookup, IndexUnion, CountIndex
							let beg =
								record::prefix(ns.namespace_id, db.database_id, &table_name_ref)
									.map_err(|e| {
										ControlFlow::Err(anyhow::anyhow!(
											"Failed to create prefix key: {e}"
										))
									})?;
							let end =
								record::suffix(ns.namespace_id, db.database_id, &table_name_ref)
									.map_err(|e| {
										ControlFlow::Err(anyhow::anyhow!(
											"Failed to create suffix key: {e}"
										))
									})?;
							ScanMode::KvStream {
								beg,
								end,
							}
						}
					}
				}
			}
			ScanTarget::RecordId(rid) => {
				match &rid.key {
					RecordIdKey::Range(range) => {
						// Range scan within the table - store the key range
						let beg = match &range.start {
							Bound::Unbounded => {
								record::prefix(ns.namespace_id, db.database_id, &rid.table)
									.map_err(|e| {
										ControlFlow::Err(anyhow::anyhow!(
											"Failed to create prefix key: {e}"
										))
									})?
							}
							Bound::Included(v) => {
								record::new(ns.namespace_id, db.database_id, &rid.table, v)
									.encode_key()
									.map_err(|e| {
										ControlFlow::Err(anyhow::anyhow!(
											"Failed to create begin key: {e}"
										))
									})?
							}
							Bound::Excluded(v) => {
								let mut key =
									record::new(ns.namespace_id, db.database_id, &rid.table, v)
										.encode_key()
										.map_err(|e| {
											ControlFlow::Err(anyhow::anyhow!(
												"Failed to create begin key: {e}"
											))
										})?;
								key.push(0x00);
								key
							}
						};
						let end = match &range.end {
							Bound::Unbounded => {
								record::suffix(ns.namespace_id, db.database_id, &rid.table)
									.map_err(|e| {
										ControlFlow::Err(anyhow::anyhow!(
											"Failed to create suffix key: {e}"
										))
									})?
							}
							Bound::Excluded(v) => {
								record::new(ns.namespace_id, db.database_id, &rid.table, v)
									.encode_key()
									.map_err(|e| {
										ControlFlow::Err(anyhow::anyhow!(
											"Failed to create end key: {e}"
										))
									})?
							}
							Bound::Included(v) => {
								let mut key =
									record::new(ns.namespace_id, db.database_id, &rid.table, v)
										.encode_key()
										.map_err(|e| {
											ControlFlow::Err(anyhow::anyhow!(
												"Failed to create end key: {e}"
											))
										})?;
								key.push(0x00);
								key
							}
						};

						ScanMode::KvStream {
							beg,
							end,
						}
					}
					_ => {
						// Point lookup for a single record
						ScanMode::PointLookup {
							rid,
							done: false,
						}
					}
				}
			}
		};

		Ok(Self {
			ctx: ctx.clone(),
			table_name,
			select_permission,
			check_perms,
			version,
			field_state: None,
			batch: Vec::with_capacity(BATCH_SIZE),
			mode,
		})
	}

	/// Determine scan direction from ORDER BY clause.
	///
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

	/// Get the next batch of values, or None if the scan is complete.
	async fn next_batch(&mut self) -> Result<Option<ValueBatch>, ControlFlow> {
		// Handle IndexDelegate mode first - it manages its own stream
		if let ScanMode::IndexDelegate(delegate_mode) = &self.mode {
			return self.execute_index_delegate(delegate_mode.clone()).await;
		}

		// Extract data from mode before processing to avoid borrow conflicts
		let mode_info = match &self.mode {
			ScanMode::KvStream {
				beg,
				end,
			} => Some((beg.clone(), end.clone())),
			ScanMode::PointLookup {
				rid: _,
				done,
			} => {
				if *done {
					return Ok(None);
				}
				// Will process point lookup below
				None
			}
			ScanMode::IndexDelegate(_) => unreachable!("Handled above"),
			ScanMode::Done => return Ok(None),
		};

		if let Some((beg, end)) = mode_info {
			// KV stream mode
			let txn = Arc::clone(self.ctx.txn());
			let kv_stream =
				txn.stream_keys_vals(beg..end.clone(), self.version, None, ScanDirection::Forward);
			futures::pin_mut!(kv_stream);

			while let Some(result) = kv_stream.next().await {
				let (key, val) = result
					.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to scan record: {e}")))?;

				let value = decode_record(&key, val)?;

				// Check permission and add to batch
				if self.check_and_add_to_batch(value).await? {
					// Batch is full - update beg to continue from after this key
					// Append 0x00 to get a key strictly after the current one
					let mut next_beg = key.clone();
					next_beg.push(0x00);
					self.mode = ScanMode::KvStream {
						beg: next_beg,
						end,
					};
					return self.flush_batch().await.map(Some);
				}
			}

			// Stream exhausted - flush remaining batch and mark as done
			self.mode = ScanMode::Done;
			if self.batch.is_empty() {
				Ok(None)
			} else {
				self.flush_batch().await.map(Some)
			}
		} else {
			// Point lookup mode
			let rid = if let ScanMode::PointLookup {
				rid,
				..
			} = &self.mode
			{
				rid.clone()
			} else {
				unreachable!()
			};
			self.point_lookup(rid).await
		}
	}

	/// Perform a point lookup for a single record.
	async fn point_lookup(&mut self, rid: RecordId) -> Result<Option<ValueBatch>, ControlFlow> {
		// Mark as done immediately
		if let ScanMode::PointLookup {
			done,
			..
		} = &mut self.mode
		{
			*done = true;
		}

		let db_ctx = self.ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
		let txn = self.ctx.txn();

		let record = txn
			.get_record(
				db_ctx.ns_ctx.ns.namespace_id,
				db_ctx.db.database_id,
				&rid.table,
				&rid.key,
				self.version,
			)
			.await
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get record: {e}")))?;

		// Check if record exists
		if record.data.as_ref().is_none() {
			return Ok(None);
		}

		// Inject the id field into the document
		let mut value = record.data.as_ref().clone();
		value.def(&rid);

		// Check permission and add to batch
		self.check_and_add_to_batch(value).await?;

		// Flush the single-item batch
		if self.batch.is_empty() {
			Ok(None)
		} else {
			self.flush_batch().await.map(Some)
		}
	}

	/// Execute an index delegate mode by creating and running the appropriate index operator.
	///
	/// This method delegates execution to the IndexScan, FullTextScan, or KnnScan operators
	/// and forwards their results.
	async fn execute_index_delegate(
		&mut self,
		delegate_mode: IndexDelegateMode,
	) -> Result<Option<ValueBatch>, ControlFlow> {
		// Mark as done to prevent re-execution
		self.mode = ScanMode::Done;

		// Create the appropriate index operator
		let operator: Box<dyn ExecOperator> = match delegate_mode {
			IndexDelegateMode::BTree {
				index_ref,
				access,
				direction,
				table_name,
			} => Box::new(IndexScan {
				index_ref,
				access,
				direction,
				table_name,
			}),
			IndexDelegateMode::FullText {
				index_ref,
				query,
				operator,
				table_name,
			} => Box::new(FullTextScan {
				index_ref,
				query,
				operator,
				table_name,
			}),
		};

		// Execute the index operator and collect all results
		let mut stream = operator.execute(&self.ctx)?;
		let mut all_values = Vec::new();

		while let Some(batch_result) = stream.next().await {
			let batch = batch_result?;
			all_values.extend(batch.values);
		}

		if all_values.is_empty() {
			Ok(None)
		} else {
			Ok(Some(ValueBatch {
				values: all_values,
			}))
		}
	}

	/// Check permission for a value and add it to the batch if allowed.
	/// Returns true if the batch is now full and should be flushed.
	async fn check_and_add_to_batch(&mut self, value: Value) -> Result<bool, ControlFlow> {
		let allowed = check_permission_for_value(&self.select_permission, &value, &self.ctx)
			.await
			.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to check permission: {e}")))?;

		if allowed {
			self.batch.push(value);
			Ok(self.batch.len() >= BATCH_SIZE)
		} else {
			Ok(false)
		}
	}

	/// Flush the current batch: process computed fields and field permissions,
	/// then return the batch as a ValueBatch.
	async fn flush_batch(&mut self) -> Result<ValueBatch, ControlFlow> {
		// Lazy-initialize field state on first batch
		if self.field_state.is_none() {
			let state = build_field_state(&self.ctx, &self.table_name, self.check_perms).await?;
			self.field_state = Some(state);
		}

		// Process batch: computed fields and field permissions
		if let Some(ref state) = self.field_state {
			process_batch(&self.ctx, state, self.check_perms, &mut self.batch).await?;
		}

		// Take the batch and reserve capacity for the next one
		let batch = std::mem::take(&mut self.batch);
		self.batch.reserve(BATCH_SIZE);

		Ok(ValueBatch {
			values: batch,
		})
	}
}

/// Fetch field definitions and build the cached field state.
async fn build_field_state(
	ctx: &ExecutionContext,
	table_name: &TableName,
	check_perms: bool,
) -> Result<FieldState, ControlFlow> {
	let db_ctx = ctx.database().map_err(|e| ControlFlow::Err(e.into()))?;
	let txn = ctx.txn();

	let field_defs = txn
		.all_tb_fields(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, table_name, None)
		.await
		.map_err(|e| ControlFlow::Err(anyhow::anyhow!("Failed to get field definitions: {}", e)))?;

	// Build computed fields
	let mut computed_fields = Vec::new();
	for fd in field_defs.iter() {
		if let Some(ref expr) = fd.computed {
			let physical_expr = expr_to_physical_expr(expr.clone(), ctx.ctx()).map_err(|e| {
				ControlFlow::Err(anyhow::anyhow!(
					"Computed field '{}' has unsupported expression: {}",
					fd.name.to_raw_string(),
					e
				))
			})?;

			computed_fields.push(ComputedFieldDef {
				field_name: fd.name.to_raw_string(),
				expr: physical_expr,
				kind: fd.field_kind.clone(),
			});
		}
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
	values: &mut Vec<Value>,
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
			Err(e) => {
				// Check if this is a RETURN control flow - extract the value
				if let Some(return_value) = e.downcast_ref::<ReturnValue>() {
					return_value.0.clone()
				} else {
					return Err(ControlFlow::Err(anyhow::anyhow!(
						"Failed to compute field '{}': {}",
						cf.field_name,
						e
					)));
				}
			}
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
		let direction = ScanExecutor::determine_scan_direction(&None);
		assert!(matches!(direction, ScanDirection::Forward));
	}

	#[test]
	fn test_determine_scan_direction_random_order() {
		use crate::expr::order::Ordering;

		// Random order -> Forward
		let order = Ordering::Random;
		let direction = ScanExecutor::determine_scan_direction(&Some(order));
		assert!(matches!(direction, ScanDirection::Forward));
	}
}
