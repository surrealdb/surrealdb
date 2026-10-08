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
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use common::future::stream::{self, Yielder};
#[cfg(test)]
use surrealdb_types::ToSql;
#[cfg(test)]
use tracing::instrument;

#[cfg(test)]
use super::count::CountOutput;
use crate::catalog::{DatabaseId, NamespaceId};
#[cfg(test)]
use crate::catalog::{Error, Index};
use crate::err::EngineError;
use crate::exec::ExecutionContext;
use crate::exec::index::access_path::{BTreeAccess, IndexRef};
use crate::exec::index::iterator::btree::INDEX_BATCH_SIZE;
#[cfg(test)]
use crate::exec::permission::{
	PhysicalPermission, check_permission_for_value, should_check_perms, validate_record_user_access,
};
#[cfg(test)]
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatchStream, monitor_stream,
};
#[cfg(test)]
use crate::expr::logical::LogicalExpr;
use crate::expr::{ControlFlow, ControlFlowExt};
#[cfg(test)]
use crate::iam::Action;
use crate::key::KVKeyDecode;
#[cfg(test)]
use crate::key::KVValue;
use crate::key::schema::{IndexCountKey, IndexCountPrefix};
#[cfg(test)]
use crate::key::schema::{RecordKey, RecordPrefix};
use crate::val::TableName;
#[cfg(test)]
use crate::val::Value;

#[cfg(test)]
/// Optimized operator for `SELECT count() FROM <table> WHERE <cond> GROUP ALL`
/// when a matching COUNT index exists.
///
/// Falls back to B-tree index key counting (when a covering B-tree index is
/// available) or full scan + filter + count if no index can service the query.
#[derive(Debug, Clone)]
pub struct IndexCountScan {
	/// Expression that evaluates to the table name.
	pub(crate) source: Arc<dyn PhysicalExpr>,
	/// The physical expression for the WHERE predicate (used for fallback).
	pub(crate) predicate: Arc<dyn PhysicalExpr>,
	/// The logical WHERE condition, matched against COUNT index guards.
	pub(crate) condition: LogicalExpr,
	/// Optional VERSION expression for time-travel queries.
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	/// The shape to answer in: a row of named counts, or the count itself.
	pub(crate) output: CountOutput,
	/// Optional B-tree index access path for key-only counting when no
	/// matching COUNT index exists.  The planner resolves this from the
	/// same index analysis it performs for regular queries.
	pub(crate) btree_access: Option<(IndexRef, BTreeAccess)>,
	/// Optional exact bitmap fusion plan for multi-index AND
	/// conditions no single index covers: the count is the fused bitmap's
	/// cardinality, with zero record fetches. Only set when every conjunct
	/// is exactly represented (see `IndexAnalyzer::try_bitmap_count_fusion`).
	pub(crate) bitmap_plan: Option<Arc<super::bitmap::BitmapNode>>,
	/// The `bitmap_plan` root coerced for `children()` / EXPLAIN.
	pub(crate) bitmap_plan_dyn: Option<Arc<dyn ExecOperator>>,
	/// Per-operator runtime metrics for EXPLAIN ANALYZE.
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl IndexCountScan {
	#[cfg(test)]
	pub(crate) fn new(
		source: Arc<dyn PhysicalExpr>,
		predicate: Arc<dyn PhysicalExpr>,
		condition: LogicalExpr,
		version: Option<Arc<dyn PhysicalExpr>>,
		output: CountOutput,
	) -> Self {
		debug_assert!(output.is_well_formed(), "IndexCountScan requires at least one output field");
		Self {
			source,
			predicate,
			condition,
			version,
			output,
			btree_access: None,
			bitmap_plan: None,
			bitmap_plan_dyn: None,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	#[cfg(test)]
	/// Set the B-tree index access path for key-only counting.
	pub(crate) fn with_btree_access(mut self, access: Option<(IndexRef, BTreeAccess)>) -> Self {
		self.btree_access = access;
		self
	}

	#[cfg(test)]
	/// Set the exact bitmap fusion plan for multi-index counting.
	pub(crate) fn with_bitmap_plan(mut self, plan: Option<Arc<super::bitmap::BitmapNode>>) -> Self {
		self.bitmap_plan_dyn = plan.clone().map(|p| p as Arc<dyn ExecOperator>);
		self.bitmap_plan = plan;
		self
	}
}
#[cfg(test)]
impl ExecOperator for IndexCountScan {
	fn name(&self) -> &'static str {
		"IndexCountScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("source".to_string(), self.source.to_sql()),
			("condition".to_string(), self.predicate.to_sql()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		// IndexCountScan needs database context, combined with expression contexts
		self.source
			.required_context()
			.max(self.predicate.required_context())
			.max(ContextLevel::Database)
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		// The bitmap plan (when present) is evaluated by this operator, but
		// surfaces as a child so EXPLAIN shows how the count is computed.
		self.bitmap_plan_dyn.iter().collect()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("source", &self.source), ("predicate", &self.predicate)]
	}

	fn access_mode(&self) -> AccessMode {
		self.source.access_mode().combine(self.predicate.access_mode())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	#[instrument(name = "IndexCountScan::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		validate_record_user_access(&db_ctx)?;
		// Refused here rather than in the stream, so the refusal surfaces when
		// the plan is executed.
		should_check_perms(&db_ctx, Action::View)?;

		let source_expr = Arc::clone(&self.source);
		let predicate_expr = Arc::clone(&self.predicate);
		let condition = self.condition.clone();
		let version = self.version.clone();
		let output = self.output.clone();
		let btree_access = self.btree_access.clone();
		let bitmap_plan = self.bitmap_plan.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			// The stamp the enclosing `VersionScope` set, rather than the
			// clause evaluated again; see [`super::common::resolve_version_stamp`].
			let version: Option<u64> =
				super::common::resolve_version_stamp(&ctx, version.as_ref()).await?;

			// Evaluate source expression to get the table name.
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let table_value = source_expr.evaluate(eval_ctx).await?;

			let table_name = match table_value {
				Value::Table(t) => t,
				_ => {
					Err(ControlFlow::Err(anyhow::anyhow!(
						"IndexCountScan received a non-table source"
					)))?;
					unreachable!()
				}
			};

			let count = count_indexed(
				&ctx,
				table_name,
				&condition,
				&predicate_expr,
				btree_access.as_ref(),
				bitmap_plan.as_ref(),
				version,
			)
			.await?;
			yielder.emit(output.batch(count)).await;
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "IndexCountScan", &self.metrics))
	}
}

#[cfg(test)]
/// The number of records of `table` that `condition` admits and the caller
/// may select, as [`IndexCountScan`] counts them: from a `COUNT` index whose
/// guard is `condition`, else by walking `btree_access`'s keys, else as the
/// cardinality of `bitmap_plan`'s exact fused bitmap, else by reading and
/// judging every record with `predicate`. Under a conditional permission every
/// record is read and judged; a table the caller may not see counts none.
pub(crate) async fn count_indexed(
	ctx: &ExecutionContext,
	table_name: TableName,
	condition: &LogicalExpr,
	predicate_expr: &Arc<dyn PhysicalExpr>,
	btree_access: Option<&(IndexRef, BTreeAccess)>,
	bitmap_plan: Option<&Arc<super::bitmap::BitmapNode>>,
	version: Option<u64>,
) -> FlowResult<usize> {
	let db_ctx = ctx.database().context("IndexCountScan requires database context")?;
	let check_perms = should_check_perms(db_ctx, Action::View)?;
	let txn = ctx.txn();
	let ns = Arc::clone(&db_ctx.ns_ctx.ns);
	let db = Arc::clone(&db_ctx.db);
	// Verify table exists.
	let table_def =
		db_ctx.get_table_def(&table_name, version).await.context("Failed to get table")?;

	if table_def.is_none() {
		Err(ControlFlow::Err(anyhow::Error::new(Error::TbNotFound {
			name: table_name.clone(),
		})))?;
	}

	// Resolve SELECT permission.
	let select_permission = if check_perms {
		crate::exec::vm::descriptor::select_permission(ctx, &table_name).await?
	} else {
		PhysicalPermission::Allow
	};

	match select_permission {
		PhysicalPermission::Deny => {
			// The table is invisible, so no index entry of it is
			// counted. The `GROUP ALL` this stands in for reports one
			// row whatever reaches it, and a caller who may read
			// nothing counts nothing.
			return Ok(0);
		}
		PhysicalPermission::Conditional(_) => {
			// Per-record permissions: fall back to full scan + filter + count.
			let count = count_with_filter_fallback(
				ctx,
				ns.namespace_id,
				db.database_id,
				&table_name,
				version,
				&select_permission,
				predicate_expr,
			)
			.await?;
			return Ok(count);
		}
		PhysicalPermission::Allow => {
			// Proceed to look for a matching COUNT index.
		}
	}

	// Look up all indexes for the table (using the execution-level cache).
	let indexes = db_ctx
		.get_table_indexes(&table_name, version)
		.await
		.context("Failed to fetch table indexes")?;

	let matching_index = indexes.iter().find(|ix| matches_count_guard(ix, condition));

	if let Some(ix_def) = matching_index {
		// Fast path: sum delta counts from the COUNT index.
		let count = sum_index_count_deltas(
			ctx,
			&txn,
			ns.namespace_id,
			db.database_id,
			&table_name,
			ix_def.index_id,
		)
		.await?;
		Ok(count)
	} else if let Some((ix_ref, access)) = btree_access {
		// Medium path: count entries by iterating B-tree index
		// keys only — no record value deserialization.
		let count =
			count_btree_index_keys(ctx, &txn, ns.namespace_id, db.database_id, ix_ref, access)
				.await?;
		Ok(count)
	} else if let Some(plan) = bitmap_plan {
		// Bitmap path: the count is the cardinality of
		// the exact fused bitmap over the table's shared doc-ID
		// space — index entries only, zero record fetches.
		Ok(plan.build_exact_cardinality(ctx, &table_name).await? as usize)
	} else {
		// No matching COUNT index found: fall back to full scan + filter + count.
		let perm = PhysicalPermission::Allow;
		let count = count_with_filter_fallback(
			ctx,
			ns.namespace_id,
			db.database_id,
			&table_name,
			version,
			&perm,
			predicate_expr,
		)
		.await?;
		Ok(count)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
/// True when `ix` is a guarded COUNT index whose guard is the query's WHERE
/// clause.
///
/// A guard is stored as SurrealQL text (`CondText`), so its identity is its
/// rendering: the catalog's parsed guard and the query's logical condition are
/// compared as SurrealQL, which is the same comparison the planner makes when
/// it chooses the `IndexCount` plan
/// (`exec::planner::select::has_matching_count_index`), so both agree on which
/// index services a given condition.
fn matches_count_guard(ix: &crate::catalog::IndexDefinition, condition: &LogicalExpr) -> bool {
	matches!(&ix.index, Index::Count(Some(_)))
		&& ix.count_cond.as_ref().map(|guard| guard.0.to_sql()) == Some(condition.to_sql())
}

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
			.next_batch(crate::kvs::NORMAL_BATCH_SIZE)
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
	}
	// Add the deltas this transaction has buffered but not yet flushed. Count
	// deltas are aggregated per transaction and written at commit, so they are
	// not in the scanned range yet; without this a read would not observe writes
	// made earlier in its own transaction.
	count += txn.pending_count_delta(ns, db, tb, ix);
	Ok(count.max(0) as usize)
}

#[cfg(test)]
/// Fallback: scan all records, apply the predicate, and count matches.
///
/// Used when no matching COUNT index exists or when per-record permissions
/// require row-level evaluation.
async fn count_with_filter_fallback(
	ctx: &ExecutionContext,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	table_name: &TableName,
	version: Option<u64>,
	permission: &PhysicalPermission,
	predicate: &Arc<dyn PhysicalExpr>,
) -> Result<usize, ControlFlow> {
	let txn = ctx.txn();
	let range = RecordPrefix {
		ns: ns_id,
		db: db_id,
		tb: Cow::Borrowed(table_name),
	}
	.range()?;

	let mut cursor = txn
		.open_vals_cursor_raw(range, crate::kvs::Direction::Forward, 0, version)
		.await
		.context("Failed to open scan cursor")?;
	let mut count = 0usize;
	loop {
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}
		let batch = cursor
			.next_batch(crate::kvs::NORMAL_BATCH_SIZE)
			.await
			.context("Failed to scan record")?;
		if batch.is_empty() {
			break;
		}
		for (key, val) in &batch {
			let decoded_key = RecordKey::decode_key(key).context("Failed to decode record key")?;
			let rid_val = crate::val::RecordId {
				table: decoded_key.tb.into_owned(),
				key: decoded_key.id.into_owned(),
			};
			let record = crate::catalog::Record::kv_decode_value(val, rid_val)
				.context("Failed to deserialize record")?;
			let value = record.data;

			// Check per-record permission first, through the same helper the scan
			// path uses. The WHERE predicate below is NOT a permission and keeps
			// its own evaluation, without the permission bypass.
			let perm_allowed = check_permission_for_value(permission, &value, None, ctx)
				.await
				.map_err(ControlFlow::Err)?;
			if !perm_allowed {
				continue;
			}

			// Apply the WHERE predicate.
			let eval_ctx = EvalContext::from_exec_ctx(ctx).with_value(&value);
			let matches =
				predicate.evaluate(eval_ctx).await.map(|v| v.is_truthy()).map_err(|e| {
					ControlFlow::Err(anyhow::anyhow!("Failed to evaluate predicate: {e}"))
				})?;
			if matches {
				count += 1;
			}
		}
	}

	Ok(count)
}

/// Count matching records by iterating B-tree index keys only.
///
/// This is much faster than the full-scan fallback because it avoids
/// reading and deserializing record values.  Each index entry corresponds
/// to exactly one matching record, so we simply count entries in the
/// appropriate key range.
pub(crate) async fn count_btree_index_keys(
	ctx: &ExecutionContext,
	txn: &crate::kvs::Transaction,
	ns_id: NamespaceId,
	db_id: DatabaseId,
	index_ref: &IndexRef,
	access: &BTreeAccess,
) -> Result<usize, ControlFlow> {
	use crate::exec::index::iterator::btree::BTreeIterator;
	use crate::kvs::Direction;

	let ix = index_ref.definition();
	let Some(mut iter) =
		BTreeIterator::new(ns_id, db_id, ix, index_ref.is_unique(), access, Direction::Forward)
			.context("Failed to create index iterator")?
	else {
		// FullText and Knn are not supported for counting.
		return Err(ControlFlow::Err(anyhow::anyhow!(
			"Unsupported BTreeAccess type for index key counting"
		)));
	};
	let mut count = 0usize;
	loop {
		if ctx.cancellation().is_cancelled() {
			return Err(ControlFlow::Err(anyhow::anyhow!(EngineError::QueryCancelled)));
		}
		let rids =
			iter.next_batch(txn, INDEX_BATCH_SIZE).await.context("Failed to iterate index")?;
		if rids.is_empty() {
			break;
		}
		count += rids.len();
	}
	Ok(count)
}

#[cfg(test)]
mod tests {
	use surrealdb_strand::Strand;

	use super::*;
	use crate::catalog::{CondText, IndexDefinition, IndexId, SurqlText};
	use crate::expr::logical::LogicalLiteral;
	use crate::expr::{Cond, Expr, Literal};

	/// The guard as the catalog holds it.
	fn cond(b: bool) -> Cond {
		Cond(Expr::Literal(Literal::Bool(b)))
	}

	/// The same condition as a query's WHERE clause reaches the operator.
	fn logical(b: bool) -> LogicalExpr {
		LogicalExpr::Literal(LogicalLiteral::Bool(b))
	}

	fn index_def(index: Index, count_cond: Option<Cond>) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(1),
			name: Strand::from("cnt"),
			table_name: crate::val::TableName::from("t"),
			cols: Vec::new(),
			index,
			count_cond,
			prepare_remove: false,
			format_version: 0,
			comment: None,
		}
	}

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
		}
	}

	/// The operator and the planner must reach the same verdict for a guard
	/// built the way production builds one, which is through `from_stored`.
	/// Constructing the runtime definition directly would let this pass even if
	/// the operator went back to re-parsing the stored text.
	#[test]
	fn guard_match_agrees_with_the_planner_predicate() {
		use crate::catalog::FromStored;

		let guard = cond(true);
		let stored = stored_index_def(Index::Count(Some(CondText(SurqlText::new(&guard)))));
		let ix = IndexDefinition::from_stored(&stored).expect("a rendered guard re-parses");

		// The planner's predicate, verbatim from exec/planner/select/mod.rs.
		let planner_says = matches!(&ix.index, Index::Count(Some(_)))
			&& ix.count_cond.as_ref().map(|g| g.0.to_sql()) == Some(logical(true).to_sql());

		assert!(planner_says, "planner must select this index");
		assert!(matches_count_guard(&ix, &logical(true)), "operator must agree with the planner");
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

	#[test]
	fn guard_match_rejects_a_different_condition() {
		let ix =
			index_def(Index::Count(Some(CondText(SurqlText::new(&cond(true))))), Some(cond(true)));
		assert!(!matches_count_guard(&ix, &logical(false)));
	}

	#[test]
	fn guard_match_rejects_an_unguarded_count_index() {
		let ix = index_def(Index::Count(None), None);
		assert!(!matches_count_guard(&ix, &logical(true)));
	}
}
