//! The candidates of a full-text query, as a program reads them.
//!
//! The index is opened through [`Services::open_full_text`] for its analyzer
//! and its doc-id layout, and the query is analyzed into its terms
//! (`FullTextIndex::plan_query`), which names what each term reads. Every one
//! of those reads is a plain read of the keyspace — the change keys and change
//! batches through [`Services::read_range`], the residuals and compacted
//! bitmaps through [`Services::read_values`] — and the changes the transaction
//! buffered come from [`Services::pending_term_changes`]. The full-text kernels
//! (`idx::kernels::fulltext`) fold what was read into each term's document set
//! and combine the sets under the query's operator; the hits are the records
//! the combined set names, resolved a batch at a time.

use roaring::RoaringTreemap;

use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::index::access_path::IndexRef;
use crate::exec::index::iterator::btree::INDEX_BATCH_SIZE;
use crate::exec::operators::scan::FullTextHits;
use crate::exec::vm::services::Services;
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::operator::MatchesOperator;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::idx::ft::fulltext::{TermDocsResidual, TermState};
use crate::key::{KVValue, RawRange};
use crate::kvs::Direction;
use crate::val::TableName;

/// The hits of `query` against full-text index `index` of `table` under
/// `operator`, or `None` when no record can match.
pub(super) async fn candidates(
	ctx: &ExecutionContext,
	services: &impl Services,
	table: &TableName,
	index: &IndexRef,
	query: &str,
	operator: &MatchesOperator,
) -> FlowResult<Option<FullTextHits>> {
	let fti = services.open_full_text(table, index).await?;
	let az_fn = StreamingAnalyzerFunction::new(ctx);
	let plan =
		fti.plan_query(&az_fn, query.to_owned()).await.context("Failed to extract query terms")?;
	if plan.terms().is_empty() {
		return Ok(None);
	}
	// Every term's change ranges, and the residual and bitmap point keys every
	// term names, are read at once: an engine whose transaction serves reads
	// concurrently overlaps their round trips, and one that does not serves
	// them in turn.
	let stored = plan.reads().iter().flat_map(|r| [r.residual.clone(), r.bitmap.clone()]).collect();
	let ranges = futures::future::try_join_all(plan.reads().iter().map(|reads| async move {
		let change_keys = read_all_keys(ctx, services, reads.changes.clone().into()).await?;
		let batches = read_all(ctx, services, reads.batches.clone().into())
			.await?
			.into_iter()
			.map(|(key, value)| Ok((key, decode::<RoaringTreemap>(&value)?)))
			.collect::<FlowResult<Vec<_>>>()?;
		FlowResult::Ok((change_keys, batches))
	}));
	let (ranges, values) = futures::future::try_join(ranges, services.read_values(stored)).await?;
	let mut states = Vec::with_capacity(plan.terms().len());
	for (term, (change_keys, batches)) in plan.terms().iter().zip(ranges) {
		let (pending_added, pending_removed) = services.pending_term_changes(&fti, term);
		states.push(TermState {
			change_keys,
			batches,
			pending_added,
			pending_removed,
			residual: None,
			bitmap: None,
		});
	}
	for (state, pair) in states.iter_mut().zip(values.chunks_exact(2)) {
		state.residual = pair[0].as_deref().map(decode::<TermDocsResidual>).transpose()?;
		state.bitmap = pair[1].as_deref().map(decode::<RoaringTreemap>).transpose()?;
	}
	let terms = plan.resolve(states).context("Failed to extract query terms")?;
	Ok(fti.new_hits_iterator(&terms, operator.operator).map(FullTextHits::new))
}

/// Every entry of `range`, read a batch at a time.
///
/// Each read asks for no more entries than the fuel left allows
/// ([`super::fuel::batch`]) and charges the entries it returns. A read that
/// answers fewer entries than it asked for has drained the range, so the
/// loop ends on it rather than on a further read that answers none.
pub(super) async fn read_all(
	ctx: &ExecutionContext,
	services: &impl Services,
	mut range: RawRange,
) -> FlowResult<Vec<(Vec<u8>, Vec<u8>)>> {
	let mut out = Vec::new();
	loop {
		let most = super::fuel::batch(INDEX_BATCH_SIZE);
		let batch = services.read_range(&mut range, Direction::Forward, most).await?;
		let read = batch.len();
		out.extend(batch);
		super::fuel::tick(ctx, super::fuel::units(read)).await?;
		if read < most as usize {
			return Ok(out);
		}
	}
}

/// Every key of `range`, read a batch at a time with no value read, as
/// [`read_all`] reads its entries.
pub(super) async fn read_all_keys(
	ctx: &ExecutionContext,
	services: &impl Services,
	mut range: RawRange,
) -> FlowResult<Vec<Vec<u8>>> {
	let mut out = Vec::new();
	loop {
		let most = super::fuel::batch(INDEX_BATCH_SIZE);
		let batch = services.read_range_keys(&mut range, Direction::Forward, most).await?;
		let read = batch.len();
		out.extend(batch);
		super::fuel::tick(ctx, super::fuel::units(read)).await?;
		if read < most as usize {
			return Ok(out);
		}
	}
}

fn decode<V: KVValue<KeyContext = ()>>(bytes: &[u8]) -> FlowResult<V> {
	V::kv_decode_value(bytes, ()).map_err(ControlFlow::Err)
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use super::*;
	use crate::catalog::providers::TableProvider;
	use crate::exec::operators::test_util::TestDb;
	use crate::exec::vm::services::EngineServices;

	/// More documents carrying the term than one read takes, each written by a
	/// statement of its own, so each leaves the term a change batch of its own.
	const DOCS: usize = INDEX_BATCH_SIZE as usize + 100;

	/// A term with more uncompacted change batches than one read takes is
	/// read whole: every batch once, in key order, by both the entry and the
	/// key read, and the match answers every document.
	#[tokio::test]
	async fn a_term_read_crosses_batches_and_misses_none() {
		let mut setup = String::from(
			"DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; \
			 DEFINE TABLE d SCHEMALESS; \
			 DEFINE INDEX d_body ON d FIELDS body FULLTEXT ANALYZER simple BM25;",
		);
		for i in 0..DOCS {
			setup.push_str(&format!(" CREATE d:{i} SET body = 'hello world';"));
		}
		// No maintenance task runs, so no compaction folds the batches.
		let db = TestDb::quiescent(&setup, false).await;
		let ctx = db.exec_ctx().await;
		let db_ctx = ctx.database().expect("a database context");
		let table = TableName::from("d");
		let def = ctx
			.txn()
			.expect_tb_index(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, &table, "d_body")
			.await
			.expect("the index");
		let index = IndexRef::new(std::slice::from_ref(def.as_ref()), 0);
		let services = EngineServices::new(&ctx);
		let fti = services.open_full_text(&table, &index).await.expect("the index opens");
		let plan = fti
			.plan_query(&StreamingAnalyzerFunction::new(&ctx), "hello".to_owned())
			.await
			.expect("the query plans");
		let reads = &plan.reads()[0];

		let entries = read_all(&ctx, &services, reads.batches.clone().into()).await.expect("read");
		let keys =
			read_all_keys(&ctx, &services, reads.batches.clone().into()).await.expect("read");
		assert_eq!(entries.len(), DOCS, "one change batch per committed statement");
		assert!(entries.windows(2).all(|pair| pair[0].0 < pair[1].0), "in key order, once each");
		let entry_keys: Vec<_> = entries.into_iter().map(|(key, _)| key).collect();
		assert_eq!(keys, entry_keys, "the key read answers the entry read's keys");
		let none =
			read_all_keys(&ctx, &services, reads.changes.clone().into()).await.expect("read");
		assert!(none.is_empty(), "no per-document change key is written");

		let ds = db.datastore();
		let session = TestDb::owner();
		let count = |sql: &'static str| {
			let session = &session;
			async move {
				let mut responses = ds.execute(sql, session, None).await.expect("the query runs");
				responses.remove(0).result.expect("a count")
			}
		};
		assert_eq!(
			count("SELECT count() FROM d WHERE body @@ 'hello' GROUP ALL").await,
			count("SELECT count() FROM d GROUP ALL").await,
			"the match answers every document"
		);
	}
}
