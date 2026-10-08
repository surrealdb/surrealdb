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

#[cfg(test)]
use std::sync::Arc;

use common::future::stream::{Yielder, try_async_stream};
#[cfg(test)]
use tracing::instrument;

#[cfg(test)]
use super::pipeline::{FieldState, build_field_state, filter_and_process_batch};
#[cfg(test)]
use super::resolved::{EnforcementMode, ResolvedTableContext};
#[cfg(test)]
use crate::exec::operators::fetch::fetch_raw_record;
#[cfg(test)]
use crate::exec::permission::{
	PhysicalPermission, should_check_perms, validate_record_user_access,
};
#[cfg(test)]
use crate::exec::{
	AccessMode, ContextLevel, ExecOperator, OperatorMetrics, PhysicalExpr, ValueBatch,
	ValueBatchStream, monitor_stream,
};
use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::{ControlFlow, ControlFlowExt};
#[cfg(test)]
use crate::iam::Action;
use crate::kvs::lightweight::{LightweightEdgeScanner, lightweight_relation};
#[cfg(test)]
use crate::val::Value;
use crate::val::{RecordId, RecordIdKey, TableName};

#[cfg(test)]
/// The edges of one lightweight relation, as rows.
#[derive(Debug, Clone)]
pub struct LightweightEdgeScan {
	/// The relation's table.
	pub(crate) table: TableName,
	/// `FROM edge:[a, b]` or `FROM edge:[a, a]..=[b, z]`: the record id the
	/// statement named, compiled. The walk is over adjacency rather than over
	/// record keys, so a range cannot bound where it starts — it bounds what
	/// it keeps. `None` for a whole-table source.
	pub(crate) record_id: Option<Arc<dyn PhysicalExpr>>,
	/// Fields the query needs; `None` for all of them.
	pub(crate) needed_fields: Option<std::collections::HashSet<String>>,
	/// The `WHERE` predicate pushed down from the filter above.
	pub(crate) predicate: Option<Arc<dyn PhysicalExpr>>,
	/// Plan-time resolved table context, when the planner resolved one.
	pub(crate) table_ctx: Option<ResolvedTableContext>,
	/// Who enforces the schema-derived behaviours on the rows.
	pub(crate) mode: EnforcementMode,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl ExecOperator for LightweightEdgeScan {
	fn name(&self) -> &'static str {
		"LightweightEdgeScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("table".to_string(), self.table.to_string())];
		if let Some(id) = &self.record_id {
			attrs.push(("id".to_string(), id.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		self.record_id
			.iter()
			.map(|id| ("id", id))
			.chain(self.predicate.iter().map(|p| ("predicate", p)))
			.collect()
	}

	fn access_mode(&self) -> AccessMode {
		self.predicate.as_ref().map(|p| p.access_mode()).unwrap_or(AccessMode::ReadOnly)
	}

	#[instrument(name = "LightweightEdgeScan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		validate_record_user_access(&db_ctx)?;
		let check_perms = should_check_perms(&db_ctx, Action::View)?;

		let table = self.table.clone();
		let record_id = self.record_id.clone();
		let needed_fields = self.needed_fields.clone();
		let predicate = self.predicate.clone();
		let table_ctx = self.table_ctx.clone();
		let mode = self.mode;
		let ctx = ctx.clone();

		let stream = try_async_stream(async move |mut yielder: Yielder<_>| {
			let db_ctx = ctx.database().context("LightweightEdgeScan requires database context")?;
			let txn = ctx.txn();
			let ns = db_ctx.ns_ctx.ns.namespace_id;
			let db = db_ctx.db.database_id;
			let version = ctx.version_stamp();

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

			// The permission and the field state are the ones any scan of this
			// table applies, so a synthesized edge is gated exactly as a
			// stored record is.
			let permission: PhysicalPermission = match &table_ctx {
				Some(tc) => tc.select_permission(check_perms),
				None if !check_perms => PhysicalPermission::Allow,
				None => crate::exec::vm::descriptor::select_permission(&ctx, &table).await?,
			};
			let state: FieldState =
				build_field_state(&ctx, &table, check_perms, needed_fields.as_ref()).await?;
			let _ = mode;

			// What the statement named, if it named an id: one edge, or the
			// bounds a range keeps. Evaluated once for the statement, as the
			// record-id scan evaluates it.
			let wanted: Option<RecordIdKey> = match &record_id {
				Some(expr) => {
					let eval_ctx = crate::exec::EvalContext::from_exec_ctx(&ctx);
					match expr.evaluate(eval_ctx).await? {
						Value::RecordId(rid) => Some(rid.key),
						// Anything else names no edge of this relation.
						_ => return Ok(()),
					}
				}
				None => None,
			};

			let mut scanner = LightweightEdgeScanner::new(
				&txn,
				ns,
				db,
				&table,
				rel,
				surrealdb_kvs::Direction::Forward,
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
				let mut batch = Vec::with_capacity(ids.len());
				for id in ids {
					if !wanted_keeps(&id.key, wanted.as_ref()) {
						continue;
					}
					// The record does not exist as stored bytes; the provider
					// builds it from the edge's canonical id.
					if let Some(record) = fetch_raw_record(&ctx, &id, version).await? {
						batch.push(record);
					}
				}
				if batch.is_empty() {
					continue;
				}
				filter_and_process_batch(
					&mut batch,
					&permission,
					predicate.as_ref(),
					&ctx,
					&state,
					check_perms,
				)
				.await?;
				if !batch.is_empty() {
					yielder.emit(ValueBatch::new(batch)).await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "LightweightEdgeScan", &self.metrics))
	}
}

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
