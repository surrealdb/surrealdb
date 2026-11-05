//! The table's indexes.
//!
//! Index maintenance, and the doc-id release an in-flight index build may
//! defer.
//!
//! The two stages are deliberately separate and adjacent: the maintenance
//! reports the deferral on [`WriteRow::doc_id_removal_deferred`](super::super::row::WriteRow),
//! and the release acts on it before the purge. Folding them together, or
//! moving the release past the purge, breaks a concurrent build's replay.
//!
//! # What the index stage is handed
//!
//! Both images are read as they stand when the stage runs, which is *before*
//! the computed fields are materialised on every order that has both stages:
//! an index therefore holds the record's stored values. On `DELETE` the
//! post-image has already been emptied, so every entry the record held is
//! removed and nothing is written back.

use crate::catalog::IndexDefinition;
use crate::exec::analyzer_function::StreamingAnalyzerFunction;
use crate::exec::operators::write::ctx::WriteCtx;
use crate::expr::ControlFlow;
use crate::idx::index::IndexOperation;
use crate::kvs::index::{ConsumeResult, IndexMutation};
use crate::val::{RecordId, Value};

/// Apply the record's change to one index.
///
/// Answers `true` when the index took the mutation into an in-flight build's
/// queue *and* consumes the table's shared doc-id space: the record's doc-id
/// must then outlive this transaction, because the build's replay still
/// resolves the record through it.
pub(crate) async fn one_index(
	ctx: &WriteCtx,
	ix: &IndexDefinition,
	o: Option<Vec<Value>>,
	n: Option<Vec<Value>>,
	rid: &RecordId,
	count_cond_match: Option<(bool, bool)>,
) -> Result<bool, ControlFlow> {
	let frozen = ctx.exec().ctx();
	// Does this index consume the table's shared doc-ID space?
	let doc_id_index = ix.uses_shared_doc_ids();
	let (o, n) = if let Some(ib) = frozen.get_index_builder() {
		let mutation = IndexMutation {
			old_values: o,
			new_values: n,
			rid,
			count_cond_match,
		};
		match ib.consume(&ctx.db, frozen, ix, mutation).await.map_err(ControlFlow::Err)? {
			// The build owns this mutation now and applies it when it replays,
			// so nothing is written here. For a doc-ID index that carries the
			// record's use of the shared `!dj`/`!dd` mapping into the replay.
			ConsumeResult::Enqueued => return Ok(doc_id_index),
			// The build is finished; index the record normally.
			ConsumeResult::Ignored(o, n) => (o, n),
			// The definition was retired after it was read from a cache.
			ConsumeResult::Retired => return Ok(false),
		}
	} else {
		(o, n)
	};
	let az_fn = StreamingAnalyzerFunction::new(ctx.exec());
	let mut ic = IndexOperation::new(
		frozen,
		ctx.db.namespace_id,
		ctx.db.database_id,
		ctx.table.table_id,
		ix,
		o,
		n,
		rid,
	);
	if let Some((old_matches, new_matches)) = count_cond_match {
		ic = ic.with_count_cond_match(old_matches, new_matches);
	}
	// A compaction request raised while the entries are written is triggered
	// once the write is complete, never in the middle of it.
	let mut require_compaction = false;
	// The maintainer of a full-text index loads its analyzer as it computes
	// the entries.
	crate::exec::vm::descriptor::object(ic.compute(&az_fn, &mut require_compaction))
		.await
		.map_err(ControlFlow::Err)?;
	if require_compaction {
		ic.trigger_compaction().await.map_err(ControlFlow::Err)?;
	}
	Ok(false)
}
