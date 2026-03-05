use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use super::{merge_left_null, merge_records};
use crate::exec::index::access_path::IndexRef;
use crate::exec::index::iterator::{IndexEqualIterator, UniqueEqualIterator};
use crate::exec::operators::scan::common::fetch_and_filter_records_batch;
use crate::exec::operators::scan::resolved::ResolvedTableContext;
use crate::exec::permission::{
	PhysicalPermission, convert_permission_to_physical, should_check_perms,
	validate_record_user_access,
};
use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::expr::ControlFlowExt;
use crate::expr::join::JoinKind;
use crate::iam::Action;
use crate::kvs::CachePolicy;
use crate::val::TableName;

/// Index Nested Loop Join: for each left row, performs an index equality
/// lookup on the right table's B-tree index instead of buffering the
/// entire right side in memory.
///
/// Supports INNER and LEFT join kinds. RIGHT and CROSS joins are not
/// suitable for this operator since they require scanning the entire
/// right side (the planner routes those to HashJoin or NLJ instead).
///
/// O(n * log(m)) for non-unique indexes, O(n) for unique indexes.
#[derive(Debug)]
pub struct IndexNestedLoopJoin {
	pub(crate) left: Arc<dyn ExecOperator>,
	pub(crate) kind: JoinKind,
	pub(crate) left_key: Arc<dyn PhysicalExpr>,
	pub(crate) right_index_ref: IndexRef,
	pub(crate) right_table_name: TableName,
	pub(crate) right_resolved: Option<ResolvedTableContext>,
	pub(crate) left_alias: String,
	pub(crate) right_alias: String,
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl IndexNestedLoopJoin {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		left: Arc<dyn ExecOperator>,
		kind: JoinKind,
		left_key: Arc<dyn PhysicalExpr>,
		right_index_ref: IndexRef,
		right_table_name: TableName,
		right_resolved: Option<ResolvedTableContext>,
		left_alias: String,
		right_alias: String,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			left,
			kind,
			left_key,
			right_index_ref,
			right_table_name,
			right_resolved,
			left_alias,
			right_alias,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for IndexNestedLoopJoin {
	fn name(&self) -> &'static str {
		"IndexNestedLoopJoin"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("join_type".to_string(), format!("{:?}", self.kind)),
			("left_key".to_string(), self.left_key.to_sql()),
			("index".to_string(), self.right_index_ref.definition().name.clone()),
			("unique".to_string(), self.right_index_ref.is_unique().to_string()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.left]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("left_key", &self.left_key)]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let left_stream = buffer_stream(
			self.left.execute(ctx)?,
			self.left.access_mode(),
			self.left.cardinality_hint(),
		);

		let left_key = Arc::clone(&self.left_key);
		let kind = self.kind.clone();
		let left_alias = self.left_alias.clone();
		let right_alias = self.right_alias.clone();
		let index_ref = self.right_index_ref.clone();
		let table_name = self.right_table_name.clone();
		let resolved = self.right_resolved.clone();
		let version_expr = self.version.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let db_ctx = ctx.database()?.clone();
			validate_record_user_access(&db_ctx)?;
			let check_perms = should_check_perms(&db_ctx, Action::View)?;
			let txn = ctx.txn();
			let ns = Arc::clone(&db_ctx.ns_ctx.ns);
			let db = Arc::clone(&db_ctx.db);
			let ns_id = ns.namespace_id;
			let db_id = db.database_id;
			let is_unique = index_ref.is_unique();
			let ix_def = index_ref.definition().clone();

			let select_permission = if let Some(ref res) = resolved {
				res.select_permission(check_perms)
			} else if check_perms {
				let table_def = db_ctx
					.get_table_def(&table_name)
					.await
					.context("Failed to get table")?;
				if let Some(def) = &table_def {
					convert_permission_to_physical(&def.permissions.select, ctx.ctx()).await
						.context("Failed to convert permission")?
				} else {
					PhysicalPermission::Allow
				}
			} else {
				PhysicalPermission::Allow
			};

			if matches!(select_permission, PhysicalPermission::Deny) {
				return;
			}

			let version: Option<u64> = match &version_expr {
				Some(expr) => {
					let eval_ctx = EvalContext::from_exec_ctx(&ctx);
					let v = expr.evaluate(eval_ctx).await?;
					Some(
						v.cast_to::<crate::val::Datetime>()
							.map_err(|e| anyhow::anyhow!("{e}"))?
							.to_version_stamp()?,
					)
				}
				None => None,
			};

			futures::pin_mut!(left_stream);
			while let Some(left_batch) = left_stream.next().await {
				let left_batch = left_batch?;
				let mut output = Vec::new();

				for left_val in &left_batch.values {
					let key_val = left_key.evaluate(
						EvalContext::from_exec_ctx(&ctx).with_value_and_doc(left_val)
					).await?;

					let rids = if is_unique {
						let mut iter = UniqueEqualIterator::new(ns_id, db_id, &ix_def, &key_val)
							.context("Failed to create unique index iterator")?;
						iter.next_batch(&txn).await
							.context("Failed to iterate unique index")?
					} else {
						let mut iter = IndexEqualIterator::new(ns_id, db_id, &ix_def, &key_val)
							.context("Failed to create index iterator")?;
						iter.next_batch(&txn).await
							.context("Failed to iterate index")?
					};

					if rids.is_empty() {
						match kind {
							JoinKind::Left => {
								output.push(merge_left_null(left_val, &left_alias, &right_alias));
							}
							JoinKind::Anti => {
								output.push(left_val.clone());
							}
							_ => {}
						}
						continue;
					}

					match kind {
						JoinKind::Semi => {
							output.push(left_val.clone());
						}
						JoinKind::Anti => {
							// Has matches -- skip this left row
						}
						_ => {
							let right_vals = fetch_and_filter_records_batch(
								&ctx, &txn, ns_id, db_id, &rids, &select_permission,
								check_perms, version, CachePolicy::ReadOnly,
							).await?;

							if right_vals.is_empty() && matches!(kind, JoinKind::Left) {
								output.push(merge_left_null(left_val, &left_alias, &right_alias));
							} else {
								for right_val in &right_vals {
									output.push(merge_records(left_val, right_val, &left_alias, &right_alias));
								}
							}
						}
					}
				}

				if !output.is_empty() {
					yield ValueBatch { values: output };
				}
			}
		};

		Ok(monitor_stream(Box::pin(stream), "IndexNestedLoopJoin", &self.metrics))
	}
}
