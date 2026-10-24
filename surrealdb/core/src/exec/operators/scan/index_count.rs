//! IndexCountScan operator - optimized COUNT() using index count metadata.
//!
//! When a query is `SELECT count() FROM <table> WHERE <cond> GROUP ALL` and a
//! COUNT index exists whose stored condition matches the WHERE clause exactly,
//! this operator replaces the full Scan -> Filter -> Aggregate pipeline.
//!
//! Instead of deserializing and filtering every record, it sums the delta
//! entries stored in `IndexCountKey` for the matching COUNT index.  This is
//! O(index entries) with no record I/O.
//!
//! The planner emits this operator (via `is_indexed_count_eligible`) when:
//! - Fields are count-all-only
//! - GROUP ALL is present
//! - A WHERE clause is present
//! - No SPLIT, ORDER BY, FETCH, or OMIT clauses
//! - A single table source
//!
//! At execution time the operator:
//! 1. Resolves the table and looks up its indexes.
//! 2. Finds a `Index::Count(cond)` whose condition equals the query's WHERE.
//! 3. Scans `IndexCountKey` deltas to compute the total count.
//! 4. Falls back to a full scan + filter + count if no matching COUNT index is found or permissions
//!    are conditional.

use std::borrow::Cow;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::err::EngineError;
use crate::exec::ExecutionContext;
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::key::KVKeyDecode;
use crate::key::schema::{IndexCountKey, IndexCountPrefix};
use crate::val::TableName;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Sum the delta entries in `IndexCountKey` for a given COUNT index.
pub(crate) async fn sum_index_count_deltas(
	ctx: &ExecutionContext,
	txn: &crate::kvs::Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	ix: crate::catalog::IndexId,
) -> Result<usize, ControlFlow> {
	let range = IndexCountPrefix {
		ns,
		db,
		tb: Cow::Borrowed(tb),
		ix,
	}
	.range()?;

	let mut cursor = txn
		.open_keys_cursor(range, crate::kvs::Direction::Forward, 0, None)
		.await
		.context("Failed to open index-count cursor")?;
	let mut count: i64 = 0;
	loop {
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}
		let batch = cursor
			.next_batch(crate::exec::vm::fuel::batch(crate::kvs::NORMAL_BATCH_SIZE))
			.await
			.context("Failed to scan index count keys")?;
		if batch.is_empty() {
			break;
		}
		for key in &batch {
			let iu = IndexCountKey::decode_key(key).context("Failed to decode index count key")?;
			if iu.pos {
				count += iu.count as i64;
			} else {
				count -= iu.count as i64;
			}
		}
		crate::exec::vm::fuel::tick(ctx, crate::exec::vm::fuel::units(batch.len())).await?;
	}
	// Add the deltas this transaction has buffered but not yet flushed. Count
	// deltas are aggregated per transaction and written at commit, so they are
	// not in the scanned range yet; without this a read would not observe writes
	// made earlier in its own transaction.
	count += txn.pending_count_delta(ns, db, tb, ix);
	Ok(count.max(0) as usize)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use crate::catalog::{CondText, Index, IndexDefinition, IndexId, SurqlText};

	fn stored_index_def(index: Index) -> crate::catalog::StoredIndexDefinition {
		crate::catalog::StoredIndexDefinition {
			index_id: IndexId(1),
			name: Strand::from("cnt"),
			table_name: Strand::from("t"),
			cols: Vec::new(),
			index,
			prepare_remove: false,
			format_version: 0,
			comment: None,
			nulls_not_distinct: false,
		}
	}

	/// A guard whose stored text does not re-parse never reaches the operator:
	/// `from_stored` fails first, so the whole query fails at catalog load. This
	/// pins that boundary, so the operator is not expected to defend against a
	/// state it cannot be handed.
	#[test]
	fn an_unparseable_guard_fails_before_the_operator_sees_it() {
		use crate::catalog::FromStored;

		let stored = stored_index_def(Index::Count(Some(CondText(SurqlText::from_raw(
			"*** not surql ***",
		)))));
		assert!(IndexDefinition::from_stored(&stored).is_err());
	}
}
