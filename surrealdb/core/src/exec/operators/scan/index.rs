//! Index scan operator for B-tree index access.
//!
//! This operator retrieves records using B-tree index structures (Idx and Uniq),
//! supporting equality lookups, range scans, and union operations.

use std::collections::HashSet;
use std::sync::Arc;

use common::future::stream::{self, Yielder};
use surrealdb_types::ToSql;

use super::common::{
	DecodeFields, RECORD_PROBE_ROWS, ReadAhead, decode_fields_for_execution, drain_read_ahead,
	next_batch_len, probe_batch_len, resolve_version_stamp,
};
use super::pipeline::{build_field_state, eval_limit_expr};
use super::resolved::ResolvedTableContext;
use crate::catalog::Error;
use crate::err::EngineError;
use crate::exec::index::access_path::{BTreeAccess, IndexRef};
use crate::exec::index::iterator::btree::{BTreeIterator, INDEX_BATCH_SIZE};
use crate::exec::operators::limit::PagingClause;
use crate::exec::permission::{
	PhysicalPermission, should_check_perms, table_select_permission_physical_runtime,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, ControlFlowExt, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatchStream, monitor_stream,
};
use crate::expr::ControlFlow;
use crate::iam::Action;
use crate::kvs::Direction;

/// Index scan operator for B-tree indexes (Idx and Uniq).
///
/// Retrieves records using an index access path, then fetches the full
/// record data and applies permission filtering.
///
/// When `limit` and/or `start` are provided (pushed down from the planner),
/// the operator stops iteration early once the limit is reached, avoiding
/// unnecessary index and record reads.
#[derive(Debug)]
pub struct IndexScan {
	/// Reference to the index definition
	pub index_ref: IndexRef,
	/// How to access the index
	pub access: BTreeAccess,
	/// Scan direction (forward or backward)
	pub direction: Direction,
	/// Table name for record fetching
	pub table_name: surrealdb_strand::TableName,
	/// Pushed-down LIMIT expression (evaluated at execution time).
	pub(crate) limit: Option<Arc<dyn PhysicalExpr>>,
	/// Pushed-down START expression (evaluated at execution time).
	pub(crate) start: Option<Arc<dyn PhysicalExpr>>,
	/// Optional VERSION timestamp for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// Plan-time resolved table context. When present, `execute()` skips
	/// runtime table def + permission lookup, and its enforcement mode says
	/// whether the scan applies the table permission, computed fields and
	/// field permissions itself or yields records as stored for the plan
	/// above it to apply them.
	pub(crate) resolved: Option<ResolvedTableContext>,
	/// Projection-aware field set for computed-field materialization.
	/// Outer `None` = sub-operator mode (parent handles fields).
	/// `Some(None)` = all fields, `Some(Some(set))` = specific fields.
	pub(crate) needed_fields: Option<Option<HashSet<String>>>,
	/// Full WHERE predicate as a streaming physical expression. Unlike
	/// [`TableScan`](super::TableScan), index seeks only narrow candidates;
	/// this predicate is evaluated on fetched rows to enforce any conditions
	/// beyond what the indexer proved (matching `DynamicScan`'s unified
	/// outer pipeline behaviour).
	pub(crate) where_predicate: Option<Arc<dyn PhysicalExpr>>,
	/// Per-batch size ceiling when LIMIT wasn't pushed (due to a residual
	/// filter preventing direct pushdown).  The scan reads batches of at
	/// most this many entries, enabling faster early termination from the
	/// downstream Limit operator.  Does NOT cap total entries — the loop
	/// continues until either the range is exhausted or the consumer
	/// drops the stream.
	pub(crate) batch_ceiling: Option<usize>,
	/// The fields to decode from each fetched record, when the plan reads no
	/// other field of it; `None` decodes whole records.
	pub(crate) decode_fields: Option<DecodeFields>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl IndexScan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		index_ref: IndexRef,
		access: BTreeAccess,
		direction: Direction,
		table_name: surrealdb_strand::TableName,
		limit: Option<Arc<dyn PhysicalExpr>>,
		start: Option<Arc<dyn PhysicalExpr>>,
		version: Option<Arc<dyn PhysicalExpr>>,
		needed_fields: Option<Option<HashSet<String>>>,
		where_predicate: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			index_ref,
			access,
			direction,
			table_name,
			limit,
			start,
			version,
			resolved: None,
			needed_fields,
			where_predicate,
			batch_ceiling: None,
			decode_fields: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}

	/// Set a per-batch ceiling for downstream LIMIT awareness.
	///
	/// When the planner knows there is a downstream LIMIT but cannot push
	/// it to the scan (residual filter), it passes the user's LIMIT here.
	/// This makes each batch small so the downstream Limit operator can
	/// terminate the stream quickly instead of waiting for a full 1000-entry
	/// batch.
	pub(crate) fn with_batch_ceiling(mut self, ceiling: Option<usize>) -> Self {
		self.batch_ceiling = ceiling;
		self
	}

	/// Decode only `fields` of each fetched record; see [`DecodeFields`].
	pub(crate) fn with_decode_fields(mut self, fields: DecodeFields) -> Self {
		self.decode_fields = Some(fields);
		self
	}
}

/// The most index entries a read needs: the rows `pipeline` can still use
/// under its LIMIT and START, or every entry without a limit; see
/// [`super::pipeline::ScanPipeline::rows_wanted`].
fn entries_wanted(pipeline: &super::pipeline::ScanPipeline) -> u32 {
	pipeline.rows_wanted().map_or(u32::MAX, |n| u32::try_from(n).unwrap_or(u32::MAX))
}

/// The number of index entries the next read requests, so that a read like
/// the one that found `read_rows` records in `read_bytes`, requested with
/// `current` entries, stays near `budget` bytes; see [`next_batch_len`].
fn next_index_batch_len(read_bytes: usize, read_rows: usize, current: u32, budget: usize) -> u32 {
	next_batch_len(read_bytes, read_rows, current as usize, budget, INDEX_BATCH_SIZE as usize)
		as u32
}

impl ExecOperator for IndexScan {
	fn name(&self) -> &'static str {
		"IndexScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let access_str = self.access.describe();
		let mut attrs = vec![
			("index".to_string(), self.index_ref.name.to_string()),
			("access".to_string(), access_str),
			("direction".to_string(), format!("{:?}", self.direction)),
		];
		if let Some(ref limit) = self.limit {
			attrs.push(("limit".to_string(), limit.to_sql()));
		}
		if let Some(ref start) = self.start {
			attrs.push(("offset".to_string(), start.to_sql()));
		}
		if let Some(ref predicate) = self.where_predicate {
			attrs.push(("predicate".to_string(), predicate.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = AccessMode::ReadOnly;
		if let Some(ref limit) = self.limit {
			mode = mode.combine(limit.access_mode());
		}
		if let Some(ref start) = self.start {
			mode = mode.combine(start.access_mode());
		}
		if let Some(ref pred) = self.where_predicate {
			mode = mode.combine(pred.access_mode());
		}
		mode
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		// The pinned prefix orders nothing, so the claim begins at the first
		// free column (`ORDER BY modified DESC` is met by
		// `idx(IsVisible, modified)` under `WHERE IsVisible = true`), and a
		// non-unique index's record-id tail lets a fully pinned scan answer
		// `ORDER BY id`. See `scan_ordering` for where the claim stops.
		let cols = crate::exec::index::access_path::scan_ordering(
			&self.index_ref,
			&self.access,
			self.direction,
		);
		if cols.is_empty() {
			crate::exec::OutputOrdering::Unordered
		} else {
			crate::exec::OutputOrdering::Sorted(cols)
		}
	}

	fn constant_output_fields(&self) -> Vec<crate::exec::field_path::FieldPath> {
		use crate::exec::index::access_path::BTreeAccess;

		let ix_def = self.index_ref.definition();
		match &self.access {
			// All index columns have the same value for Equality scans
			BTreeAccess::Equality(_) => ix_def
				.cols
				.iter()
				.filter_map(|s| crate::exec::field_path_convert::field_path_from_idiom(s).ok())
				.collect(),
			// Compound prefix columns are all equality-pinned
			BTreeAccess::Compound {
				prefix,
				..
			} => ix_def
				.cols
				.iter()
				.take(prefix.len())
				.filter_map(|s| crate::exec::field_path_convert::field_path_from_idiom(s).ok())
				.collect(),
			_ => vec![],
		}
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let index_ref = self.index_ref.clone();
		let access = self.access.clone();
		let direction = self.direction;
		let table_name = self.table_name.clone();
		let limit_expr = self.limit.clone();
		let start_expr = self.start.clone();
		let ceiling = self.batch_ceiling;
		let version_expr = self.version.clone();
		let resolved = self.resolved.clone();
		let needed_fields = self.needed_fields.clone();
		let decode_fields = self.decode_fields.clone();
		let where_predicate = self.where_predicate.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let db_ctx = ctx.database()?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let ns_id = ns.namespace_id;
			let db_id = db.database_id;

			// Evaluate pushed-down LIMIT and START expressions
			let limit_val: Option<usize> = match &limit_expr {
				Some(expr) => Some(eval_limit_expr(&**expr, &ctx, PagingClause::Limit).await?),
				None => None,
			};
			let start_val: usize = match &start_expr {
				Some(expr) => eval_limit_expr(&**expr, &ctx, PagingClause::Start).await?,
				None => 0,
			};

			// The batch ceiling is a downstream LIMIT hint for residual-filter
			// queries: each batch reads at most this many index entries so the
			// downstream Limit operator can stop the stream quickly instead of
			// waiting for a full 1000-entry batch. A 4x multiplier accounts
			// for rows rejected by the residual filter — large enough to avoid
			// excessive small-batch round-trips, still far smaller than the
			// default 1000-entry INDEX_BATCH_SIZE.
			//
			// It is a plan-time number rather than an expression because the
			// `Limit` operator above this scan evaluates the statement's LIMIT
			// itself; evaluating it here as well would run a clause the
			// statement wrote once as many times as there are scans beneath
			// that operator.
			let batch_max: u32 = match ceiling {
				Some(c) => c
					.saturating_add(start_val)
					.saturating_mul(4)
					.clamp(1, INDEX_BATCH_SIZE as usize) as u32,
				None => u32::MAX, // next_batch caps at INDEX_BATCH_SIZE internally
			};

			// Resolve VERSION timestamp; see [`resolve_version_stamp`] for
			// why we prefer the stamp already set by the enclosing
			// `VersionScope` over re-evaluating `version_expr` here.
			let version: Option<u64> = resolve_version_stamp(&ctx, version_expr.as_ref()).await?;

			// Resolve table permissions: plan-time fast path or runtime fallback
			let select_permission = if let Some(ref res) = resolved {
				res.select_permission(check_perms)
			} else if check_perms {
				let table_def = db_ctx
					.get_table_def(&table_name, version)
					.await
					.context("Failed to get table")?;

				if let Some(def) = &table_def {
					table_select_permission_physical_runtime(Some(&**def), &ctx)
						.await
						.context("Failed to convert permission")?
				} else {
					Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
						name: table_name.clone(),
					})))?
				}
			} else {
				PhysicalPermission::Allow
			};

			// Early exit if denied
			if matches!(select_permission, PhysicalPermission::Deny) {
				return Ok(());
			}

			if limit_val == Some(0) {
				return Ok(());
			}

			// Resolve field state for computed fields and field-level
			// permissions. When needed_fields is None (sub-operator mode),
			// the parent operator handles field processing.
			let field_state = match &needed_fields {
				Some(nf) => {
					if let Some(ref res) = resolved {
						res.field_state_for_projection(nf.as_ref())
					} else {
						build_field_state(&ctx, &table_name, check_perms, nf.as_ref()).await?
					}
				}
				None => super::pipeline::FieldState::empty(),
			};

			// Table-level permissions are already handled by
			// ReadAhead::decode_batch, so the pipeline uses Allow
			// to avoid double-checking.
			let mut pipeline = super::pipeline::ScanPipeline::new(
				PhysicalPermission::Allow,
				where_predicate,
				field_state,
				check_perms,
				limit_val,
				start_val,
			);

			let decode_fields = decode_fields_for_execution(
				&ctx,
				&table_name,
				resolved.as_ref(),
				&select_permission,
				check_perms,
				decode_fields.as_ref(),
			)
			.await?;
			// Records are read ahead as stored bytes and decoded in batches of
			// at most the byte budget. The first read is a small probe; each
			// later one requests as many entries as keep it near the byte
			// budget, judged from the bytes the read before it held.
			let batch_bytes = ctx.root().ctx.config.exec.scan_batch_bytes;
			let mut chunk_len = probe_batch_len(batch_bytes, RECORD_PROBE_ROWS) as u32;
			let mut read_ahead = ReadAhead::new(
				&select_permission,
				check_perms,
				decode_fields.as_ref(),
				batch_bytes,
			);

			// Create the appropriate iterator based on access type and index uniqueness
			let is_unique = index_ref.is_unique();
			let ix = index_ref.definition();

			// Collect record IDs from index and batch-fetch full records
			let Some(mut iter) =
				BTreeIterator::new(ns_id, db_id, ix, is_unique, &access, direction)
					.context("Failed to create iterator")?
			else {
				Err(ControlFlow::Err(anyhow::anyhow!(
					"IndexScan does not support FullText or KNN access - use dedicated operators"
				)))?
			};
			// Every lookup overlaps reading each chunk's records with reading
			// the next chunk of index entries.
			// Fetch the first batch of record IDs sequentially.
			// Use batch_max to keep batches small when a downstream
			// LIMIT exists but wasn't pushed (residual filter).
			let mut rids = iter
				.next_batch(&txn, entries_wanted(&pipeline).min(batch_max).min(chunk_len))
				.await
				.context("Failed to iterate index")?;

			while !rids.is_empty() {
				if ctx.cancellation().is_cancelled() {
					Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)))?;
				}
				// What the pipeline still wants once this read reaches it,
				// if every entry in it becomes a row.
				let wanted_after = entries_wanted(&pipeline).saturating_sub(rids.len() as u32);

				// Overlap: fetch records for the current batch while
				// scanning the next batch of index entries concurrently.
				// This halves serial latency on TiKV.
				let (read_result, next_rids_result) = if wanted_after > 0 {
					let read_fut = read_ahead.read(&txn, ns_id, db_id, rids, version);
					let scan_fut =
						iter.next_batch(&txn, wanted_after.min(batch_max).min(chunk_len));
					let (r, n) = futures::join!(read_fut, scan_fut);
					(r, Some(n))
				} else {
					// The read covers what the pipeline wants; skip the prefetch.
					(read_ahead.read(&txn, ns_id, db_id, rids, version).await, None)
				};

				let (read_bytes, read_rows) = read_result?;
				chunk_len = next_index_batch_len(read_bytes, read_rows, chunk_len, batch_bytes);
				let cont =
					drain_read_ahead(&mut read_ahead, &mut pipeline, &mut yielder, &ctx).await?;
				if !cont {
					break;
				}

				rids = match next_rids_result {
					Some(r) => r.context("Failed to iterate index")?,
					// The read was to cover what the pipeline wanted, but some of
					// its entries did not become rows (a stale entry, a denied or
					// filtered row): read what it still wants.
					None => iter
						.next_batch(&txn, entries_wanted(&pipeline).min(batch_max).min(chunk_len))
						.await
						.context("Failed to iterate index")?,
				};
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "IndexScan", &self.metrics))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::scan::resolved::test_support::{
		RESTRICTED_TABLE, anonymous, assert_as_stored, assert_reduced, contexts, index_ref,
	};
	use crate::exec::operators::test_util::{TestDb, collect};
	use crate::kvs::TransactionType;
	use crate::val::Value;

	/// Under the plan's enforcement the scan yields the fetched record as
	/// stored, for the nodes above it to reduce; under its own it reduces the
	/// record for a permission-checked caller.
	#[tokio::test]
	async fn a_planned_scan_yields_the_stored_record_and_a_runtime_scan_reduces_it() {
		let db = TestDb::new_with_auth(RESTRICTED_TABLE).await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;
		let (planned, runtime) = contexts(&ctx, "t").await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let scan = |tc: ResolvedTableContext| -> Arc<dyn ExecOperator> {
			Arc::new(
				IndexScan::new(
					index_ref.clone(),
					BTreeAccess::Equality(Value::from(1)),
					Direction::Forward,
					"t".into(),
					None,
					None,
					None,
					Some(None),
					None,
				)
				.with_resolved(tc),
			)
		};
		assert_as_stored(&collect(&scan(planned), &ctx).await);
		assert_reduced(&collect(&scan(runtime), &ctx).await);
	}
	/// More than two full batches, so a later batch is short only if sized.
	const WIDE_ROWS: usize = 2500;

	/// `WIDE_ROWS` records with `n = 1` carrying a 1024-float `emb`.
	fn wide_table() -> String {
		format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{WIDE_ROWS} {{ CREATE t SET n = 1, label = 'x', emb = array::repeat(0.5f, 1024) }};"
		)
	}

	/// The batches an equality scan of `n_idx` on `n = 1` yields, decoding
	/// only `decode` of each record when given.
	async fn scan_batches(db: &TestDb, decode: Option<&[&str]>) -> Vec<Vec<Value>> {
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let mut scan = IndexScan::new(
			index_ref,
			BTreeAccess::Equality(Value::from(1)),
			Direction::Forward,
			"t".into(),
			None,
			None,
			None,
			Some(None),
			None,
		);
		if let Some(fields) = decode {
			scan = scan.with_decode_fields(DecodeFields::new(fields.iter().map(|f| f.to_string())));
		}
		let scan: Arc<dyn ExecOperator> = Arc::new(scan);
		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut batches = Vec::new();
		while let Some(batch) = futures::StreamExt::next(&mut stream).await {
			batches.push(batch.expect("batch should be Ok").into_values());
		}
		batches
	}

	fn field(row: &Value, name: &str) -> Value {
		match row {
			Value::Object(obj) => obj.get(name).cloned().unwrap_or(Value::None),
			_ => Value::None,
		}
	}

	/// A scan given a decode set yields each record's id and the listed
	/// fields, and none of the fields it does not list.
	#[tokio::test]
	async fn a_projected_scan_decodes_only_the_listed_fields() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			CREATE t:1 SET n = 1, label = 'a', emb = [0.5, 0.5];",
		)
		.await;
		let rows: Vec<Value> =
			scan_batches(&db, Some(&["n", "label"])).await.into_iter().flatten().collect();
		assert_eq!(rows.len(), 1, "{rows:?}");
		assert!(matches!(field(&rows[0], "id"), Value::RecordId(_)), "id kept: {rows:?}");
		assert_eq!(field(&rows[0], "label"), Value::from("a"), "listed field decoded: {rows:?}");
		assert_eq!(field(&rows[0], "emb"), Value::None, "unlisted field not decoded: {rows:?}");
	}

	/// A scan given an exclusion set yields every stored field but the
	/// excluded ones.
	#[tokio::test]
	async fn an_excluding_scan_decodes_every_field_but_the_listed_ones() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			CREATE t:1 SET n = 1, label = 'a', emb = [0.5, 0.5];",
		)
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let scan: Arc<dyn ExecOperator> = Arc::new(
			IndexScan::new(
				index_ref,
				BTreeAccess::Equality(Value::from(1)),
				Direction::Forward,
				"t".into(),
				None,
				None,
				None,
				Some(None),
				None,
			)
			.with_decode_fields(
				DecodeFields::excluding(["emb".to_string()]).expect("excludes emb"),
			),
		);
		let rows = collect(&scan, &ctx).await;
		assert_eq!(rows.len(), 1, "{rows:?}");
		assert!(matches!(field(&rows[0], "id"), Value::RecordId(_)), "id kept: {rows:?}");
		assert_eq!(field(&rows[0], "n"), Value::from(1), "kept field decoded: {rows:?}");
		assert_eq!(field(&rows[0], "label"), Value::from("a"), "kept field decoded: {rows:?}");
		assert_eq!(field(&rows[0], "emb"), Value::None, "excluded field not decoded: {rows:?}");
	}

	/// Wide records arrive in batches sized to the byte budget after the
	/// first, whether whole records are decoded (sized by their decoded size)
	/// or only some fields (sized by the stored bytes read), and every record
	/// arrives once.
	#[tokio::test]
	async fn wide_records_scan_in_batches_sized_to_the_byte_budget() {
		let db = TestDb::new(&wide_table()).await;
		for decode in [None, Some(&["label"][..])] {
			let batches = scan_batches(&db, decode).await;
			let lens: Vec<usize> = batches.iter().map(Vec::len).collect();
			assert_eq!(lens.iter().sum::<usize>(), WIDE_ROWS, "{decode:?}: {lens:?}");
			assert_eq!(lens[0], RECORD_PROBE_ROWS, "{decode:?}: the first batch is the probe");
			assert!(
				lens[1] < INDEX_BATCH_SIZE as usize,
				"{decode:?}: later batches sized: {lens:?}"
			);
		}
	}

	/// The scan takes its byte budget from the execution config, so a smaller
	/// `scan_batch_bytes` yields a smaller probe and smaller later batches.
	///
	/// Gated like [`TestDb::new_with_config`], which only the in-memory
	/// backend provides.
	#[cfg(all(feature = "kv-mem", not(target_family = "wasm")))]
	#[tokio::test]
	async fn the_configured_batch_bytes_size_the_batches() {
		let config = surrealdb_cnf::ConfigMap::empty().with_key_value("scan_batch_bytes", "256KiB");
		let db = TestDb::new_with_config(&wide_table(), config).await;
		let lens: Vec<usize> = scan_batches(&db, None).await.iter().map(Vec::len).collect();
		assert_eq!(lens.iter().sum::<usize>(), WIDE_ROWS, "every record once: {lens:?}");
		// ~66 KB decoded per record: 256 KiB holds about 4, where the default
		// 2 MiB holds about 31.
		assert!(lens[0] <= 4, "the probe follows the configured budget: {lens:?}");
		assert!(lens[1] <= 8, "later batches follow the configured budget: {lens:?}");
	}

	/// However many narrow records precede them, wide records decode in
	/// batches of at most the byte budget plus one record: the read that
	/// reaches them is sized from the narrow records, but only its stored
	/// bytes are held until a batch decodes them.
	#[tokio::test]
	async fn wide_records_after_a_long_narrow_run_decode_within_the_budget() {
		// Enough narrow records for the reads to double up to the row cap;
		// the index lists equal values in record-id order, so the numeric ids
		// put every narrow record first.
		const NARROW: usize = 1100;
		const TOTAL: usize = 1500;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{NARROW} {{ CREATE type::record('t', $i) SET n = 1, label = 'x' }};
			FOR $i IN {NARROW}..{TOTAL} {{
				CREATE type::record('t', $i) SET n = 1, label = 'x', emb = array::repeat(0.5f, 1024)
			}};"
		))
		.await;
		let batches = scan_batches(&db, None).await;

		assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), TOTAL, "every record once");
		let size = super::super::common::approx_value_size;
		let widest = batches.iter().flatten().map(size).max().unwrap_or(0);
		for batch in &batches {
			let bytes: usize = batch.iter().map(size).sum();
			assert!(
				bytes < crate::exec::config::DEFAULT_SCAN_BATCH_BYTES + widest,
				"a batch of {} rows holds {bytes} bytes",
				batch.len()
			);
		}
	}

	/// A computed field can make a row far wider than its stored record, so
	/// the batches are sized from the materialised rows.
	#[tokio::test]
	async fn wide_computed_fields_size_the_batches() {
		const ROWS: usize = 2500;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE FIELD wide ON t COMPUTED array::repeat(0.5f, 1024);
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{ROWS} {{ CREATE t SET n = 1 }};"
		))
		.await;
		let batches = scan_batches(&db, None).await;
		let lens: Vec<usize> = batches.iter().map(Vec::len).collect();
		assert_eq!(lens.iter().sum::<usize>(), ROWS, "every record once: {lens:?}");
		assert!(
			batches.iter().flatten().all(|r| field(r, "wide") != Value::None),
			"wide materialised"
		);
		// ~66 KB per materialised row: the default 2 MiB holds about 31.
		let largest = lens.iter().copied().max().unwrap_or(0);
		assert!(largest <= 2 * RECORD_PROBE_ROWS, "largest batch {largest}: {lens:?}");
	}

	/// A NONE lookup on a unique index can match many records, so its reads
	/// start from the probe and grow by bytes like every other lookup's,
	/// rather than taking a full row-capped batch at once.
	#[tokio::test]
	async fn a_unique_none_lookup_reads_from_the_probe() {
		const ROWS: usize = 1500;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX slug_idx ON t FIELDS slug UNIQUE;
			FOR $i IN 0..{ROWS} {{ CREATE t SET n = 1 }};"
		))
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "slug_idx").await;
		let scan: Arc<dyn ExecOperator> = Arc::new(IndexScan::new(
			index_ref,
			BTreeAccess::Equality(Value::None),
			Direction::Forward,
			"t".into(),
			None,
			None,
			None,
			Some(None),
			None,
		));
		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut lens = Vec::new();
		while let Some(batch) = futures::StreamExt::next(&mut stream).await {
			lens.push(batch.expect("batch should be Ok").into_values().len());
		}
		assert_eq!(lens.iter().sum::<usize>(), ROWS, "every record once: {lens:?}");
		// Narrow rows decode within the budget, so each batch is one read.
		assert_eq!(lens[0], RECORD_PROBE_ROWS, "the first read is the probe: {lens:?}");
	}

	/// A batch ceiling caps every read of a single-column lookup, as it caps
	/// the compound ones, so a residual-filtered LIMIT or a union sub-scan
	/// reads a few entries at a time.
	#[tokio::test]
	async fn a_batch_ceiling_caps_single_column_reads() {
		const ROWS: usize = 100;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX n_idx ON t FIELDS n;
			FOR $i IN 0..{ROWS} {{ CREATE t SET n = 1 }};"
		))
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "n_idx").await;
		let scan: Arc<dyn ExecOperator> = Arc::new(
			IndexScan::new(
				index_ref,
				BTreeAccess::Equality(Value::from(1)),
				Direction::Forward,
				"t".into(),
				None,
				None,
				None,
				Some(None),
				None,
			)
			.with_batch_ceiling(Some(2)),
		);
		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut lens = Vec::new();
		while let Some(batch) = futures::StreamExt::next(&mut stream).await {
			lens.push(batch.expect("batch should be Ok").into_values().len());
		}
		assert_eq!(lens.iter().sum::<usize>(), ROWS, "every record once: {lens:?}");
		// A ceiling of 2 caps reads at 4 × 2 entries; narrow rows decode within
		// the budget, so each batch is one read.
		assert!(lens.iter().all(|&n| n <= 8), "reads past the ceiling: {lens:?}");
	}

	/// A compound lookup under a pushed LIMIT reads until the pipeline has its
	/// rows, so an index entry whose record is gone does not end the scan
	/// short.
	#[tokio::test]
	async fn a_compound_lookup_under_a_limit_reads_past_a_stale_entry() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX ab ON t FIELDS a, b;
			FOR $i IN 0..5 { CREATE type::record('t', $i) SET a = 1, b = $i };",
		)
		.await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let index_ref = index_ref(&ctx, "t", "ab").await;
		let db_ctx = ctx.database().expect("a database context");
		// Leave t:1's index entry behind with no record.
		ctx.txn()
			.del_key(&crate::key::schema::RecordKey {
				ns: db_ctx.ns_ctx.ns.namespace_id,
				db: db_ctx.db.database_id,
				tb: std::borrow::Cow::Owned("t".into()),
				id: std::borrow::Cow::Owned(crate::val::RecordIdKey::Number(1)),
			})
			.await
			.expect("delete the record");
		let limit = crate::exec::operators::test_util::physical_expr("3", &ctx).await;
		let scan: Arc<dyn ExecOperator> = Arc::new(IndexScan::new(
			index_ref,
			BTreeAccess::Compound {
				prefix: vec![Value::from(1)],
				range: None,
			},
			Direction::Forward,
			"t".into(),
			Some(limit),
			None,
			None,
			Some(None),
			None,
		));
		let rows = collect(&scan, &ctx).await;
		assert_eq!(rows.len(), 3, "LIMIT 3 over four live records: {rows:?}");
	}

	/// A compound range lookup under a pushed LIMIT reads until the pipeline
	/// has its rows, in either direction, so an index entry whose record is
	/// gone does not end the scan short.
	#[tokio::test]
	async fn a_compound_range_lookup_under_a_limit_reads_past_a_stale_entry() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX ab ON t FIELDS a, b;
			FOR $i IN 0..5 { CREATE type::record('t', $i) SET a = 1, b = $i };",
		)
		.await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let index_ref = index_ref(&ctx, "t", "ab").await;
		let db_ctx = ctx.database().expect("a database context");
		// Leave the index entries of t:1 and t:3 behind with no record, so the
		// first read meant to cover LIMIT 3 meets a stale entry going forward
		// and going backward.
		for id in [1, 3] {
			ctx.txn()
				.del_key(&crate::key::schema::RecordKey {
					ns: db_ctx.ns_ctx.ns.namespace_id,
					db: db_ctx.db.database_id,
					tb: std::borrow::Cow::Owned("t".into()),
					id: std::borrow::Cow::Owned(crate::val::RecordIdKey::Number(id)),
				})
				.await
				.expect("delete the record");
		}
		for direction in [Direction::Forward, Direction::Backward] {
			let limit = crate::exec::operators::test_util::physical_expr("3", &ctx).await;
			let scan: Arc<dyn ExecOperator> = Arc::new(IndexScan::new(
				index_ref.clone(),
				BTreeAccess::Compound {
					prefix: vec![Value::from(1)],
					range: Some((
						crate::expr::operator::BinaryOperator::MoreThanEqual,
						Value::from(0),
					)),
				},
				direction,
				"t".into(),
				Some(limit),
				None,
				None,
				Some(None),
				None,
			));
			let rows = collect(&scan, &ctx).await;
			assert_eq!(rows.len(), 3, "{direction:?}: LIMIT 3 over three live records: {rows:?}");
		}
	}

	/// Delete the records `t:<id>` for `ids`, leaving their index entries
	/// behind with no record.
	async fn orphan_index_entries(ctx: &ExecutionContext, ids: &[i64]) {
		let db_ctx = ctx.database().expect("a database context");
		for &id in ids {
			ctx.txn()
				.del_key(&crate::key::schema::RecordKey {
					ns: db_ctx.ns_ctx.ns.namespace_id,
					db: db_ctx.db.database_id,
					tb: std::borrow::Cow::Owned("t".into()),
					id: std::borrow::Cow::Owned(crate::val::RecordIdKey::Number(id)),
				})
				.await
				.expect("delete the record");
		}
	}

	/// A single-column equality lookup under a pushed LIMIT reads until the
	/// pipeline has its rows, in either direction, so an index entry whose
	/// record is gone does not end the scan short.
	#[tokio::test]
	async fn an_equality_lookup_under_a_limit_reads_past_a_stale_entry() {
		let db = TestDb::new(
			"DEFINE TABLE t SCHEMALESS;
			DEFINE INDEX a ON t FIELDS a;
			FOR $i IN 0..5 { CREATE type::record('t', $i) SET a = 1 };",
		)
		.await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let index_ref = index_ref(&ctx, "t", "a").await;
		// The first read meant to cover LIMIT 3 meets a stale entry going
		// forward and going backward.
		orphan_index_entries(&ctx, &[1, 3]).await;
		for direction in [Direction::Forward, Direction::Backward] {
			let limit = crate::exec::operators::test_util::physical_expr("3", &ctx).await;
			let scan: Arc<dyn ExecOperator> = Arc::new(IndexScan::new(
				index_ref.clone(),
				BTreeAccess::Equality(Value::from(1)),
				direction,
				"t".into(),
				Some(limit),
				None,
				None,
				Some(None),
				None,
			));
			let rows = collect(&scan, &ctx).await;
			assert_eq!(rows.len(), 3, "{direction:?}: LIMIT 3 over three live records: {rows:?}");
		}
	}

	/// A single-column range lookup under a pushed LIMIT reads until the
	/// pipeline has its rows, on a unique and a non-unique index and in either
	/// direction, so an index entry whose record is gone does not end the scan
	/// short.
	#[tokio::test]
	async fn a_range_lookup_under_a_limit_reads_past_a_stale_entry() {
		for unique in ["", "UNIQUE"] {
			let db = TestDb::new(&format!(
				"DEFINE TABLE t SCHEMALESS;
				DEFINE INDEX a ON t FIELDS a {unique};
				FOR $i IN 0..5 {{ CREATE type::record('t', $i) SET a = $i }};"
			))
			.await;
			let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
			let index_ref = index_ref(&ctx, "t", "a").await;
			orphan_index_entries(&ctx, &[1, 3]).await;
			for direction in [Direction::Forward, Direction::Backward] {
				let limit = crate::exec::operators::test_util::physical_expr("3", &ctx).await;
				let scan: Arc<dyn ExecOperator> = Arc::new(IndexScan::new(
					index_ref.clone(),
					BTreeAccess::Range {
						range: crate::val::Range {
							start: std::ops::Bound::Included(Value::from(0)),
							end: std::ops::Bound::Unbounded,
						},
					},
					direction,
					"t".into(),
					Some(limit),
					None,
					None,
					Some(None),
					None,
				));
				let rows = collect(&scan, &ctx).await;
				assert_eq!(
					rows.len(),
					3,
					"{unique:?} {direction:?}: LIMIT 3 over three live records: {rows:?}"
				);
			}
		}
	}
}
