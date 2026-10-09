//! Full-text search scan operator.
//!
//! This operator retrieves records using full-text search indexes,
//! supporting the MATCHES operator with BM25 or VS scoring.

use crate::catalog::{Index, IndexDefinition};
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::operator::MatchesOperator;
use crate::expr::{ControlFlow, ControlFlowExt};
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
		fti.extract_querying_terms(frozen_ctx, &az_fn, query.to_owned())
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
