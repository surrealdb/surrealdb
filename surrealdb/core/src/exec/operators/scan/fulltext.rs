//! Full-text search scan operator.
//!
//! This operator retrieves records using full-text search indexes,
//! supporting the MATCHES operator with BM25 or VS scoring.

use crate::catalog::{Index, IndexDefinition};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::IndexKeyBase;
use crate::idx::docids::DocId;
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
	/// How many of the clauses of a sum an index union's branch ranks by the
	/// hits are no longer listed for alone.
	pub(crate) passed_clauses: usize,
}

impl FullTextHits {
	pub(crate) fn new(iter: FullTextHitsIterator) -> Self {
		Self {
			iter,
			passed_clauses: 0,
		}
	}

	/// The doc ids of up to `limit` hits not yet taken, in doc-id order; empty
	/// once the hits have run out.
	pub(crate) fn take_doc_ids(&mut self, limit: usize) -> Vec<DocId> {
		self.iter.take_doc_ids(limit)
	}

	/// The index's iterator over the hits, for a take that passes hits over by
	/// their bounds.
	pub(crate) fn hits_mut(&mut self) -> &mut FullTextHitsIterator {
		&mut self.iter
	}

	/// The record each of `doc_ids` maps to, in order, in one batched read;
	/// `None` for a doc id whose mapping is gone.
	pub(crate) async fn records(
		&self,
		txn: &Transaction,
		doc_ids: &[DocId],
	) -> FlowResult<Vec<Option<RecordId>>> {
		self.iter.records(txn, doc_ids).await.context("Failed to resolve hits")
	}
}

/// The full-text index `index_def` of `table`, opened for a query in `ctx`.
pub(crate) async fn open_full_text_index(
	ctx: &ExecutionContext,
	table: &TableName,
	index_def: &IndexDefinition,
) -> FlowResult<FullTextIndex> {
	let db_ctx = ctx.database().context("a full-text scan requires database context")?;
	let txn = ctx.txn();
	let root = ctx.root();
	let frozen_ctx = &root.ctx;
	// Legacy (pre-shared-doc-ID) indexes resolve postings through their
	// historical private doc-ID layout until explicitly rebuilt; the format
	// version selects the resolver.
	let Index::FullText(ft_params) = &index_def.index else {
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"Index '{}' is not a full-text index",
			index_def.name
		)));
	};
	let ikb = IndexKeyBase::new(
		db_ctx.ns_ctx.ns.namespace_id,
		db_ctx.db.database_id,
		table.clone(),
		index_def.index_id,
	);
	crate::exec::vm::descriptor::object(FullTextIndex::new(
		frozen_ctx.get_index_stores(),
		txn.as_ref(),
		ikb,
		ft_params,
		&frozen_ctx.config.idx.file_allowlist,
		index_def.format_version,
	))
	.await
	.context("Failed to open full-text index")
}
