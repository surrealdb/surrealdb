//! Scanning a lightweight relation.
//!
//! A lightweight relation stores no records: each edge lives only as the two
//! vertex-side adjacency keys of its endpoints, so there is no record range to
//! open a cursor over. Enumerating the relation means walking the graph
//! subspace of every table its `IN` clause names and keeping the edges that
//! belong to it, which is what [`LightweightEdgeScanner`] does.
//!
//! What it yields are the edges' canonical `[in, out]` ids. The record for one
//! is synthesized on demand by the record provider, so every row this operator
//! produces is read exactly as a stored record is, and the rest of the scan —
//! field permissions, computed fields, the pushed-down predicate — is the
//! shared pipeline every other source uses.

use common::future::stream::{Yielder, try_async_stream};

use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, ControlFlowExt};
use crate::kvs::lightweight::{LightweightEdgeScanner, lightweight_relation};
use crate::val::{RecordId, RecordIdKey, TableName};

/// Whether an enumerated edge id is one the statement asked for: any of them
/// when it named no id, the one it named, or one its range holds.
///
/// The walk is over adjacency rather than over record keys, so a range bounds
/// what the scan keeps rather than where it starts.
pub(crate) fn wanted_keeps(key: &RecordIdKey, wanted: Option<&RecordIdKey>) -> bool {
	let Some(wanted) = wanted else {
		return true;
	};
	let RecordIdKey::Range(range) = wanted else {
		return key == wanted;
	};
	use std::ops::Bound;
	let after_start = match &range.start {
		Bound::Included(b) => key >= b,
		Bound::Excluded(b) => key > b,
		Bound::Unbounded => true,
	};
	let before_end = match &range.end {
		Bound::Included(b) => key <= b,
		Bound::Excluded(b) => key < b,
		Bound::Unbounded => true,
	};
	after_start && before_end
}

/// The edge ids of a lightweight relation, in batches.
pub(crate) type EdgeIdStream =
	std::pin::Pin<Box<dyn futures::Stream<Item = FlowResult<Vec<RecordId>>> + Send>>;

/// Walk the adjacency of lightweight relation `table` as of `version`,
/// yielding the ids of the edges `wanted` keeps ([`wanted_keeps`]) a scanner
/// batch at a time, in canonical-id order along `direction`.
///
/// The stream fails when `table` is not a lightweight relation in the
/// catalog the walk reads. It applies no permission, computed field or
/// predicate: the ids are the relation's own, and the caller reads each
/// record and judges it.
pub(crate) fn lightweight_edge_ids(
	ctx: &ExecutionContext,
	table: TableName,
	wanted: Option<RecordIdKey>,
	direction: surrealdb_kvs::Direction,
	version: Option<u64>,
) -> EdgeIdStream {
	let ctx = ctx.clone();
	Box::pin(try_async_stream(async move |mut yielder: Yielder<_>| {
		let db_ctx = ctx.database().context("a lightweight edge scan requires database context")?;
		let txn = ctx.txn();
		let table_def = db_ctx
			.get_table_def(&table, version)
			.await
			.context("Failed to read the table definition")?
			.context("the table is not defined")?;
		let Some(rel) = lightweight_relation(&table_def.table_type) else {
			return Err(ControlFlow::Err(anyhow::anyhow!(
				"`{table}` is not a lightweight relation"
			)));
		};
		let mut scanner = LightweightEdgeScanner::new(
			&txn,
			db_ctx.ns_ctx.ns.namespace_id,
			db_ctx.db.database_id,
			&table,
			rel,
			direction,
			version,
			Some(ctx.ctx().get_index_stores().adjacency_resolve()),
		);
		loop {
			let ids = scanner
				.next_batch(crate::kvs::NORMAL_BATCH_SIZE)
				.await
				.map_err(ControlFlow::Err)?;
			if ids.is_empty() {
				break;
			}
			let kept: Vec<RecordId> =
				ids.into_iter().filter(|id| wanted_keeps(&id.key, wanted.as_ref())).collect();
			if !kept.is_empty() {
				yielder.emit(kept).await;
			}
		}
		Ok(())
	}))
}
