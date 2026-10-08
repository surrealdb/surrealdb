//! Full-text search scan operator.
//!
//! This operator retrieves records using full-text search indexes,
//! supporting the MATCHES operator with BM25 or VS scoring.

#[cfg(test)]
use std::collections::HashSet;
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use common::future::stream::{self, Yielder};
use reblessive::TreeStack;

#[cfg(test)]
use super::common::{
	DecodeFields, RECORD_PROBE_ROWS, ReadAhead, decode_fields_for_execution, drain_read_ahead,
	next_batch_len, probe_batch_len, resolve_version_stamp,
};
#[cfg(test)]
use super::pipeline::{ScanPipeline, build_field_state};
#[cfg(test)]
use super::resolved::ResolvedTableContext;
#[cfg(test)]
use crate::catalog::Error;
use crate::catalog::{Index, IndexDefinition};
#[cfg(test)]
#[cfg(test)]
use crate::err::EngineError;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
#[cfg(test)]
use crate::exec::index::access_path::IndexRef;
#[cfg(test)]
use crate::exec::permission::{
	PhysicalPermission, should_check_perms, validate_record_user_access,
};
#[cfg(test)]
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, OperatorMetrics, PhysicalExpr, ValueBatchStream,
	monitor_stream,
};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::operator::MatchesOperator;
use crate::expr::{ControlFlow, ControlFlowExt};
#[cfg(test)]
use crate::iam::Action;
use crate::idx::IndexKeyBase;
use crate::idx::ft::MatchesHitsIterator;
use crate::idx::ft::fulltext::{FullTextHitsIterator, FullTextIndex};
use crate::kvs::Transaction;
use crate::val::{RecordId, TableName};

/// Batch size for full-text result batching.
///
/// The most record ids a full-text scan reads at once.
///
/// Smaller than the default
/// [`ExecConfig::scan_batch_size`](crate::exec::config::ExecConfig::scan_batch_size) because
/// full-text results are ordered by relevance score, so smaller batches let
/// downstream operators begin processing sooner. Each read below it is sized
/// from the bytes the read before it held, as the other record scans do.
pub(crate) const FULL_TEXT_BATCH_SIZE: usize = 100;

#[cfg(test)]
/// Full-text search scan operator.
///
/// Executes a full-text search query against a FullText index and returns
/// matching records ordered by relevance score.
#[derive(Debug)]
pub struct FullTextScan {
	/// Reference to the index definition
	pub index_ref: IndexRef,
	/// The search query string
	pub query: String,
	/// The MATCHES operator configuration (reference number, scoring)
	pub operator: MatchesOperator,
	/// Table name for record fetching
	pub table_name: surrealdb_strand::TableName,
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
	/// The fields to decode from each fetched record, when the plan reads no
	/// other field of it; `None` decodes whole records.
	pub(crate) decode_fields: Option<DecodeFields>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl FullTextScan {
	#[cfg(test)]
	pub(crate) fn new(
		index_ref: IndexRef,
		query: String,
		operator: MatchesOperator,
		table_name: surrealdb_strand::TableName,
		version: Option<Arc<dyn PhysicalExpr>>,
		needed_fields: Option<Option<HashSet<String>>>,
	) -> Self {
		Self {
			index_ref,
			query,
			operator,
			table_name,
			version,
			resolved: None,
			needed_fields,
			decode_fields: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	#[cfg(test)]
	/// Set the plan-time resolved table context.
	pub(crate) fn with_resolved(mut self, resolved: ResolvedTableContext) -> Self {
		self.resolved = Some(resolved);
		self
	}

	/// Decode only `fields` of each fetched record; see [`DecodeFields`].
	pub(crate) fn with_decode_fields(mut self, fields: DecodeFields) -> Self {
		self.decode_fields = Some(fields);
		self
	}
}
#[cfg(test)]
impl ExecOperator for FullTextScan {
	fn name(&self) -> &'static str {
		"FullTextScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("index".to_string(), self.index_ref.name.to_string()),
			("query".to_string(), self.query.clone()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();

		// Validate record user has access to this namespace/database
		validate_record_user_access(&db_ctx)?;

		// Check if we need to enforce permissions
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		// Clone for the async block
		let index_ref = self.index_ref.clone();
		let query = self.query.clone();
		let operator = self.operator.clone();
		let table_name = self.table_name.clone();
		let version_expr = self.version.clone();
		let resolved = self.resolved.clone();
		let needed_fields = self.needed_fields.clone();
		let decode_fields = self.decode_fields.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// Get namespace and database IDs
			let db_ctx = ctx.database().context("FullTextScan requires database context")?;
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let txn = ctx.txn();

			// The stamp the enclosing `VersionScope` set, rather than the
			// clause evaluated again; see [`resolve_version_stamp`].
			let version: Option<u64> = resolve_version_stamp(&ctx, version_expr.as_ref()).await?;

			// Resolve table permissions: plan-time fast path or runtime fallback
			let select_permission = if let Some(ref res) = resolved {
				res.select_permission(check_perms)
			} else if check_perms {
				let table_def = db_ctx
					.get_table_def(&table_name, version)
					.await
					.context("Failed to get table")?;

				if table_def.is_some() {
					crate::exec::vm::descriptor::select_permission(&ctx, &table_name).await?
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
			let decode_fields = decode_fields_for_execution(
				&ctx,
				&table_name,
				resolved.as_ref(),
				&select_permission,
				check_perms,
				decode_fields.as_ref(),
			)
			.await?;

			// Table-level permissions are handled by ReadAhead::decode_batch.
			// The pipeline handles computed fields and field-level permissions.
			let mut pipeline = ScanPipeline::new(
				PhysicalPermission::Allow,
				None,
				field_state,
				check_perms,
				None,
				0,
			);

			let Some(mut hits) =
				open_full_text_hits(&ctx, &table_name, index_ref.definition(), &query, &operator)
					.await?
			else {
				return Ok(());
			};

			// Read each chunk of hits ahead as stored bytes, and decode it in
			// batches of at most the byte budget. The first chunk is a small
			// probe and each later one is sized from the bytes the one before
			// it read, up to `FULL_TEXT_BATCH_SIZE`.
			let batch_bytes = ctx.root().ctx.config.exec.scan_batch_bytes;
			let mut chunk_len =
				probe_batch_len(batch_bytes, RECORD_PROBE_ROWS.min(FULL_TEXT_BATCH_SIZE));
			let mut read_ahead = ReadAhead::new(
				select_permission.clone(),
				check_perms,
				decode_fields.clone(),
				batch_bytes,
			);
			loop {
				if ctx.cancellation().is_cancelled() {
					Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)))?;
				}
				let rids = hits.next_batch(txn.as_ref(), chunk_len).await?;
				if rids.is_empty() {
					break;
				}
				let (read_bytes, read_rows) =
					read_ahead.read(&txn, ns.namespace_id, db.database_id, rids, version).await?;
				chunk_len = next_batch_len(
					read_bytes,
					read_rows,
					chunk_len,
					batch_bytes,
					FULL_TEXT_BATCH_SIZE,
				);
				if !drain_read_ahead(&mut read_ahead, &mut pipeline, &mut yielder, &ctx).await? {
					break;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "FullTextScan", &self.metrics))
	}
}

/// The hits of a full-text query, in the order the index lists them, read a
/// batch of record IDs at a time.
pub(crate) struct FullTextHits {
	iter: FullTextHitsIterator,
}

impl FullTextHits {
	/// The next `limit` record IDs the query matches, fewer only when the
	/// hits run out; empty once they have. A hit whose record ID no longer
	/// resolves is passed over.
	pub(crate) async fn next_batch(
		&mut self,
		txn: &Transaction,
		limit: usize,
	) -> FlowResult<Vec<RecordId>> {
		let mut batch = Vec::with_capacity(limit);
		while batch.len() < limit {
			match self.iter.next(txn).await.context("Failed to get next hit")? {
				Some((rid, _doc_id)) => batch.push(rid),
				None => break,
			}
		}
		Ok(batch)
	}
}

/// Open the hits of `query` against full-text index `index_def` of `table`,
/// or `None` when no record can match: the query yields no terms, or a term
/// has no document.
///
/// The hits are the index's own: no table permission or version applies to
/// them, which the caller applies to the records the hits name.
pub(crate) async fn open_full_text_hits(
	ctx: &ExecutionContext,
	table: &TableName,
	index_def: &IndexDefinition,
	query: &str,
	operator: &MatchesOperator,
) -> FlowResult<Option<FullTextHits>> {
	let db_ctx = ctx.database().context("a full-text scan requires database context")?;
	let txn = ctx.txn();
	let root = ctx.root();
	let frozen_ctx = &root.ctx;
	// Legacy (pre-shared-doc-ID) indexes resolve postings through their
	// historical private doc-ID layout until explicitly rebuilt; the format
	// version selects the resolver used below.
	let ft_params = match &index_def.index {
		Index::FullText(params) => params,
		_ => Err(ControlFlow::Err(anyhow::anyhow!(
			"Index '{}' is not a full-text index",
			index_def.name
		)))?,
	};
	let ikb = IndexKeyBase::new(
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		table.clone(),
		index_def.index_id,
	);
	let fti = crate::exec::vm::descriptor::object(FullTextIndex::new(
		frozen_ctx.get_index_stores(),
		txn.as_ref(),
		ikb,
		ft_params,
		&frozen_ctx.config.idx.file_allowlist,
		index_def.format_version,
	))
	.await
	.context("Failed to open full-text index")?;
	let query_terms = {
		let az_fn = StreamingAnalyzerFunction::new(ctx);
		let mut stack = TreeStack::new();
		stack
			.enter(|stk| fti.extract_querying_terms(stk, frozen_ctx, &az_fn, query.to_owned()))
			.finish()
			.await
			.context("Failed to extract query terms")?
	};
	if query_terms.is_empty() {
		return Ok(None);
	}
	Ok(fti.new_hits_iterator(&query_terms, operator.operator).map(|iter| FullTextHits {
		iter,
	}))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::scan::resolved::test_support::{
		anonymous, assert_as_stored, assert_reduced, contexts, index_ref,
	};
	use crate::exec::operators::test_util::{TestDb, collect};
	use crate::expr::operator::BooleanOperator;
	use crate::kvs::TransactionType;

	/// Under the plan's enforcement the scan yields the matched record as
	/// stored, for the nodes above it to reduce; under its own it reduces the
	/// record for a permission-checked caller.
	#[tokio::test]
	async fn a_planned_scan_yields_the_stored_record_and_a_runtime_scan_reduces_it() {
		let db = TestDb::new_with_auth(
			"DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
			 DEFINE FIELD secret ON t TYPE string PERMISSIONS FOR select NONE;
			 DEFINE FIELD label ON t COMPUTED 'computed';
			 DEFINE ANALYZER blank TOKENIZERS blank;
			 DEFINE INDEX body_ft ON t FIELDS body FULLTEXT ANALYZER blank BM25;
			 CREATE t:1 SET body = 'hello world', secret = 's';",
		)
		.await;
		let ctx = db.exec_ctx_as(&anonymous(), TransactionType::Read).await;
		let (planned, runtime) = contexts(&ctx, "t").await;
		let index_ref = index_ref(&ctx, "t", "body_ft").await;
		let scan = |tc: ResolvedTableContext| -> Arc<dyn ExecOperator> {
			Arc::new(
				FullTextScan::new(
					index_ref.clone(),
					"hello".to_owned(),
					MatchesOperator {
						rf: None,
						operator: BooleanOperator::And,
					},
					"t".into(),
					None,
					Some(None),
				)
				.with_resolved(tc),
			)
		};
		assert_as_stored(&collect(&scan(planned), &ctx).await);
		assert_reduced(&collect(&scan(runtime), &ctx).await);
	}

	/// Wide matching records are decoded in batches of at most the byte budget
	/// plus one record, whole or projected, and every match is returned once.
	#[tokio::test]
	async fn wide_matches_decode_in_batches_within_the_byte_budget() {
		const ROWS: usize = 300;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE ANALYZER blank TOKENIZERS blank;
			 DEFINE INDEX body_ft ON t FIELDS body FULLTEXT ANALYZER blank BM25;
			 FOR $i IN 0..{ROWS} {{ CREATE t SET body = 'hello', emb = array::repeat(0.5f, 1024) }};"
		))
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "body_ft").await;
		let size = super::super::common::approx_value_size;
		for decode in [None, Some(DecodeFields::new(["emb".to_string()]))] {
			let mut scan = FullTextScan::new(
				index_ref.clone(),
				"hello".to_owned(),
				MatchesOperator {
					rf: None,
					operator: BooleanOperator::And,
				},
				"t".into(),
				None,
				Some(None),
			);
			if let Some(fields) = decode.clone() {
				scan = scan.with_decode_fields(fields);
			}
			let scan: Arc<dyn ExecOperator> = Arc::new(scan);
			let mut stream = scan.execute(&ctx).expect("execute should succeed");
			let mut batches = Vec::new();
			while let Some(batch) = futures::StreamExt::next(&mut stream).await {
				batches.push(batch.expect("batch should be Ok").into_values());
			}
			assert_eq!(batches.iter().map(Vec::len).sum::<usize>(), ROWS, "{decode:?}");
			let widest = batches.iter().flatten().map(size).max().unwrap_or(0);
			for batch in &batches {
				let bytes: usize = batch.iter().map(size).sum();
				assert!(
					bytes < crate::exec::config::DEFAULT_SCAN_BATCH_BYTES + widest,
					"{decode:?}: a batch of {} rows holds {bytes} bytes",
					batch.len()
				);
			}
		}
	}

	/// Reads of matching records start from the probe and grow by bytes, up
	/// to `FULL_TEXT_BATCH_SIZE`.
	#[tokio::test]
	async fn matches_are_read_from_the_probe_up_to_the_batch_size() {
		const ROWS: usize = 300;
		let db = TestDb::new(&format!(
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE ANALYZER blank TOKENIZERS blank;
			 DEFINE INDEX body_ft ON t FIELDS body FULLTEXT ANALYZER blank BM25;
			 FOR $i IN 0..{ROWS} {{ CREATE t SET body = 'hello' }};"
		))
		.await;
		let ctx = db.exec_ctx().await;
		let index_ref = index_ref(&ctx, "t", "body_ft").await;
		let scan: Arc<dyn ExecOperator> = Arc::new(FullTextScan::new(
			index_ref,
			"hello".to_owned(),
			MatchesOperator {
				rf: None,
				operator: BooleanOperator::And,
			},
			"t".into(),
			None,
			Some(None),
		));
		let mut stream = scan.execute(&ctx).expect("execute should succeed");
		let mut lens = Vec::new();
		while let Some(batch) = futures::StreamExt::next(&mut stream).await {
			lens.push(batch.expect("batch should be Ok").into_values().len());
		}
		assert_eq!(lens.iter().sum::<usize>(), ROWS, "every match once: {lens:?}");
		// Narrow rows decode within the budget, so each batch is one read.
		assert_eq!(lens[0], RECORD_PROBE_ROWS, "the first read is the probe: {lens:?}");
		assert!(
			lens.iter().all(|&n| n <= FULL_TEXT_BATCH_SIZE),
			"no read past FULL_TEXT_BATCH_SIZE: {lens:?}"
		);
	}
}
