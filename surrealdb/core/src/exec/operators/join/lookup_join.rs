//! `LookupJoin` — a join whose right side is read by record id, one batch of
//! reads per batch of left rows.
//!
//! The physical form of a `LogicalOp::Join` whose single key pair is
//! `(left key, alias.id)` over a right side that is a table, optionally under
//! its permission filter, layers, filters and semi or anti joins of its own:
//! an `IN (SELECT VALUE id FROM t WHERE …)` and its `NOT IN` / `CONTAINS`
//! forms. Nothing of the right table is scanned; for each left batch the
//! operator evaluates the left key, collects the distinct record ids of the
//! right table among the values, reads those records in one batched get, runs
//! the right plan over them, and pairs each left row with the right rows whose
//! `id` equals its key.
//!
//! The right plan is a chain of operators that keep or drop the rows they are
//! given, over a [`RecordBatchFetch`] leaf, which yields the records whose ids
//! the operator hands it through the context's current value. The chain
//! enforces what the plan enforces — the table's `PERMISSIONS FOR select` as a
//! `PermissionFilter` or the join lifted from it, its computed fields and
//! field permissions as layers, the user's own filter — exactly as the same
//! chain over a table scan would, so a record the caller may not see is a
//! miss, and a record the caller may see arrives with the fields the caller
//! may read.
//!
//! Join types, the residual, the frame and `EXPLAIN` redaction follow
//! [`HashJoin`](super::HashJoin); see its module docs. A left key that is not a
//! record id of the right table — `NONE`, `NULL`, an array of ids, an id of
//! another table — matches nothing: a `Left` join null-fills the alias, a
//! `Semi` join drops the row, an `Anti` join keeps it.

#[cfg(test)]
use std::collections::{BTreeMap, BTreeSet, HashMap};
#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use common::future::stream::{self, Yielder};
#[cfg(test)]
use futures::StreamExt;
#[cfg(test)]
use tracing::instrument;

#[cfg(test)]
use super::JoinFrame;
#[cfg(test)]
use super::hash_join::JoinType;
#[cfg(test)]
use crate::catalog::providers::TableProvider;
#[cfg(test)]
use crate::exec::operators::scan::common::{resolve_record_batch, resolve_version_stamp};
#[cfg(test)]
use crate::exec::operators::scan::resolved::EnforcementMode;
#[cfg(test)]
use crate::exec::permission::{should_check_perms, validate_record_user_access};
#[cfg(test)]
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ControlFlowExt, EvalContext, ExecOperator,
	ExecutionContext, FlowResult, OperatorMetrics, OutputOrdering, PhysicalExpr, ValueBatch,
	ValueBatchStream, buffer_stream, monitor_stream,
};
#[cfg(test)]
use crate::kvs::CachePolicy;
#[cfg(test)]
use crate::val::{RecordId, TableName, Value};

#[cfg(test)]
/// A join that reads its right side by record id; see the module docs.
#[derive(Debug, Clone)]
pub struct LookupJoin {
	/// The left input, streamed.
	pub(crate) left: Arc<dyn ExecOperator>,
	/// The right plan: row-preserving operators over a [`RecordBatchFetch`]
	/// of `table`, run once per left batch.
	pub(crate) right: Arc<dyn ExecOperator>,
	/// Evaluated against each left row; a record id of `table` among its
	/// values names the right row it pairs with.
	pub(crate) key: Arc<dyn PhysicalExpr>,
	/// The right table. Ids of any other table match nothing.
	pub(crate) table: TableName,
	pub(crate) join_type: JoinType,
	/// The key the matched right row lands under on the joined row.
	pub(crate) alias: String,
	/// Evaluated against the joined row; a pair failing it is not a match.
	pub(crate) residual: Option<Arc<dyn PhysicalExpr>>,
	/// The permission frame a join lifted from a permission predicate
	/// evaluates in; `None` evaluates as the caller.
	pub(crate) frame: Option<JoinFrame>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl LookupJoin {
	#[cfg(test)]
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		left: Arc<dyn ExecOperator>,
		right: Arc<dyn ExecOperator>,
		key: Arc<dyn PhysicalExpr>,
		table: TableName,
		join_type: JoinType,
		alias: String,
		residual: Option<Arc<dyn PhysicalExpr>>,
		frame: Option<JoinFrame>,
	) -> Self {
		Self {
			left,
			right,
			key,
			table,
			join_type,
			alias,
			residual,
			frame,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	#[cfg(test)]
	/// Whether the join's expressions and right plan are the table
	/// definer's and the plan is compiled for a caller they are enforced on.
	fn redacted(&self) -> bool {
		self.frame.as_ref().is_some_and(|frame| frame.checked_caller)
	}

	#[cfg(test)]
	fn own_expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		let mut out = vec![("key", &self.key)];
		if let Some(residual) = self.residual.as_ref() {
			out.push(("residual", residual));
		}
		out
	}
}

#[cfg(test)]
impl ExecOperator for LookupJoin {
	fn name(&self) -> &'static str {
		"LookupJoin"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		// The alias and the table read are the definer's along with the keys:
		// a checked caller is shown the join's kind alone.
		let mut attrs = vec![("type".to_string(), self.join_type.label().to_string())];
		if self.redacted() {
			attrs.push(("alias".to_string(), "<permission>".to_string()));
			attrs.push(("table".to_string(), "<permission>".to_string()));
			attrs.push(("on".to_string(), "<permission>".to_string()));
		} else {
			attrs.push(("alias".to_string(), self.alias.clone()));
			attrs.push(("table".to_string(), self.table.to_string()));
			attrs.push(("on".to_string(), format!("{} = {}.id", self.key.to_sql(), self.alias)));
		}
		if let Some(residual) = self.residual.as_ref() {
			let residual = if self.redacted() {
				"<permission>".to_string()
			} else {
				residual.to_sql()
			};
			attrs.push(("residual".to_string(), residual));
		}
		if let Some(frame) = &self.frame {
			attrs.push(("frame".to_string(), "permission".to_string()));
			if frame.auth.is_some() {
				attrs.push(("auth".to_string(), "limited".to_string()));
			}
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		let mut level = ContextLevel::Database
			.max(self.left.required_context())
			.max(self.right.required_context());
		for (_, expr) in self.own_expressions() {
			level = level.max(expr.required_context());
		}
		level
	}

	fn access_mode(&self) -> AccessMode {
		let mut mode = self.left.access_mode().combine(self.right.access_mode());
		for (_, expr) in self.own_expressions() {
			mode = mode.combine(expr.access_mode());
		}
		mode
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		match self.join_type {
			// Each id names at most one record, so a left or semi join emits
			// at most one row per left row and an anti join a subset.
			JoinType::Left | JoinType::Semi | JoinType::Anti => self.left.cardinality_hint(),
			JoinType::Inner | JoinType::Cross => CardinalityHint::Unbounded,
		}
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.left, &self.right]
	}

	fn explain_children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		if self.redacted() {
			vec![&self.left]
		} else {
			self.children()
		}
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn hides_metrics_beneath(&self) -> bool {
		// The left side's rows are the records before the permission judged
		// them; a checked caller is shown the counts of what reaches it.
		self.redacted()
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		if self.redacted() {
			return Vec::new();
		}
		self.own_expressions()
	}

	fn output_ordering(&self) -> OutputOrdering {
		// Left rows are emitted in left order, each at most once per matched
		// right row, so the left side's ordering holds on the output.
		self.left.output_ordering()
	}

	#[instrument(name = "LookupJoin::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let buffer_size = ctx.root().ctx.config.exec.operator_buffer_size;
		let left_stream = buffer_stream(
			self.left.execute(ctx)?,
			self.access_mode().combine(self.left.access_mode()),
			self.left.cardinality_hint(),
			buffer_size,
		);

		// A framed semi or anti join inside an enclosing permission frame
		// enforces nothing, as `PermissionFilter` does there.
		if self.frame.is_some()
			&& ctx.root().skip_fetch_perms
			&& matches!(self.join_type, JoinType::Semi | JoinType::Anti)
		{
			return Ok(monitor_stream(left_stream, "LookupJoin", &self.metrics));
		}

		let frame_ctx = match &self.frame {
			Some(frame) => frame.context(ctx),
			None => ctx.clone(),
		};
		let right = Arc::clone(&self.right);
		let key = Arc::clone(&self.key);
		let table = self.table.clone();
		let join_type = self.join_type;
		let alias = self.alias.clone();
		let residual = self.residual.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			let mut left_stream = left_stream;
			let frame_eval = EvalContext::from_exec_ctx(&frame_ctx);
			while let Some(batch_result) = left_stream.next().await {
				crate::exec::operators::check_cancelled(&ctx)?;
				let batch = batch_result?;
				let keys = key.evaluate_batch(frame_eval.clone(), batch.values()).await?;

				// One read for the distinct ids of the right table this batch
				// names, then the right plan over what the read yields.
				let ids: BTreeSet<RecordId> = keys
					.iter()
					.filter_map(|key| match key {
						Value::RecordId(rid) if rid.table == table => Some(rid.clone()),
						_ => None,
					})
					.collect();
				let mut matched: BTreeMap<RecordId, Vec<Value>> = BTreeMap::new();
				if !ids.is_empty() {
					let wanted = Value::Array(
						ids.into_iter().map(Value::RecordId).collect::<Vec<_>>().into(),
					);
					let right_ctx = frame_ctx.with_current_value(wanted);
					let mut right_stream = right.execute(&right_ctx)?;
					while let Some(right_batch) = right_stream.next().await {
						for row in right_batch?.into_values() {
							if let Value::Object(object) = &row
								&& let Some(Value::RecordId(rid)) = object.get("id")
							{
								matched.entry(rid.clone()).or_default().push(row);
							}
						}
					}
				}

				let mut out: Vec<Value> = Vec::with_capacity(batch.len());
				for (row, key) in batch.into_values().into_iter().zip(keys) {
					let candidates: &[Value] = match &key {
						Value::RecordId(rid) => matched.get(rid).map_or(&[], Vec::as_slice),
						_ => &[],
					};
					match join_type {
						JoinType::Inner | JoinType::Cross => {
							for right_row in candidates {
								let merged = with_alias(&row, &alias, right_row.clone());
								if passes(residual.as_deref(), &frame_eval, &merged).await? {
									out.push(merged);
								}
							}
						}
						JoinType::Left => {
							let mut any = false;
							for right_row in candidates {
								let merged = with_alias(&row, &alias, right_row.clone());
								if passes(residual.as_deref(), &frame_eval, &merged).await? {
									out.push(merged);
									any = true;
								}
							}
							if !any {
								out.push(with_alias(&row, &alias, Value::Null));
							}
						}
						JoinType::Semi | JoinType::Anti => {
							let mut any = false;
							for right_row in candidates {
								let merged = with_alias(&row, &alias, right_row.clone());
								if passes(residual.as_deref(), &frame_eval, &merged).await? {
									any = true;
									break;
								}
							}
							if any == (join_type == JoinType::Semi) {
								out.push(row);
							}
						}
					}
				}
				if !out.is_empty() {
					yielder.emit(ValueBatch::new(out)).await;
				}
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "LookupJoin", &self.metrics))
	}
}

#[cfg(test)]
/// The left row with `value` set under `alias`. A left row that is not an
/// object has no field to hold the alias and is emitted as it is.
fn with_alias(left_row: &Value, alias: &str, value: Value) -> Value {
	match left_row {
		Value::Object(row) => {
			let mut row = row.clone();
			row.insert(alias.to_string(), value);
			Value::Object(row)
		}
		other => other.clone(),
	}
}

#[cfg(test)]
async fn passes(
	residual: Option<&dyn PhysicalExpr>,
	eval: &EvalContext<'_>,
	merged: &Value,
) -> FlowResult<bool> {
	match residual {
		None => Ok(true),
		Some(predicate) => {
			Ok(predicate.evaluate(eval.with_value_and_doc(merged)).await?.is_truthy())
		}
	}
}

#[cfg(test)]
/// The leaf of a [`LookupJoin`]'s right plan: yields the records of `table`
/// whose ids the context's current value holds, in one batched read.
///
/// The current value is the array of record ids the join collected from a
/// left batch; ids of another table are ignored. A record that does not
/// exist yields nothing. What the leaf enforces follows its
/// [`EnforcementMode`]: under `Planned` the plan above it carries the
/// table's permission filter and layers, so the leaf yields each record as
/// stored; under `Runtime` the leaf applies the table's `SELECT` permission,
/// computed fields and field permissions itself, as a record read by id
/// does. Either way the read is confined to the caller's namespace and
/// database, and honours the `VERSION` the plan runs under.
#[derive(Debug, Clone)]
pub struct RecordBatchFetch {
	pub(crate) table: TableName,
	pub(crate) enforcement: EnforcementMode,
	pub(crate) version: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl RecordBatchFetch {
	#[cfg(test)]
	pub(crate) fn new(
		table: TableName,
		enforcement: EnforcementMode,
		version: Option<Arc<dyn PhysicalExpr>>,
	) -> Self {
		Self {
			table,
			enforcement,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg(test)]
impl ExecOperator for RecordBatchFetch {
	fn name(&self) -> &'static str {
		"RecordBatchFetch"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("table".to_string(), self.table.to_string())];
		if let Some(version) = &self.version {
			attrs.push(("version".to_string(), version.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Database
	}

	fn access_mode(&self) -> AccessMode {
		match &self.version {
			Some(version) => AccessMode::ReadOnly.combine(version.access_mode()),
			None => AccessMode::ReadOnly,
		}
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		match &self.version {
			Some(version) => vec![("version", version)],
			None => vec![],
		}
	}

	#[instrument(name = "RecordBatchFetch::execute", level = "trace", skip_all)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let db_ctx = ctx.database()?.clone();
		validate_record_user_access(&db_ctx)?;
		let check_perms = should_check_perms(&db_ctx, crate::iam::Action::View)?;

		let rids: Vec<RecordId> = match ctx.current_value() {
			Some(Value::Array(ids)) => ids
				.iter()
				.filter_map(|id| match id {
					Value::RecordId(rid) if rid.table == self.table => Some(rid.clone()),
					_ => None,
				})
				.collect(),
			_ => Vec::new(),
		};
		let enforcement = self.enforcement;
		let version_expr = self.version.clone();
		let ctx = ctx.clone();

		let stream = stream::try_async_stream(async move |mut yielder: Yielder<_>| {
			if rids.is_empty() {
				return Ok(());
			}
			let version = resolve_version_stamp(&ctx, version_expr.as_ref()).await?;
			let txn = ctx.txn();
			let ns_id = db_ctx.ns_ctx.ns.namespace_id;
			let db_id = db_ctx.db.database_id;
			let rows = match enforcement {
				EnforcementMode::Planned => {
					let records = txn
						.get_records(ns_id, db_id, &rids, version, CachePolicy::ReadWrite)
						.await
						.context("Failed to fetch records")?;
					records
						.into_iter()
						.filter(|record| !record.data.is_none())
						.map(|record| match Arc::try_unwrap(record) {
							Ok(record) => record.data,
							Err(record) => record.data.clone(),
						})
						.collect::<Vec<Value>>()
				}
				EnforcementMode::Runtime => {
					let mut perm_cache = HashMap::new();
					resolve_record_batch(
						&ctx,
						&txn,
						ns_id,
						db_id,
						&rids,
						true,
						check_perms,
						version,
						CachePolicy::ReadWrite,
						&mut perm_cache,
					)
					.await?
				}
			};
			if !rows.is_empty() {
				yielder.emit(ValueBatch::new(rows)).await;
			}
			Ok(())
		});

		Ok(monitor_stream(Box::pin(stream), "RecordBatchFetch", &self.metrics))
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::Filter;
	use crate::exec::operators::test_util::{TestDb, ValuesOperator, collect, physical_expr, val};

	async fn docs(srcs: &[&str]) -> Vec<Value> {
		let mut out = Vec::with_capacity(srcs.len());
		for src in srcs {
			out.push(val(src).await);
		}
		out
	}

	fn ids(rows: &[Value]) -> Vec<Value> {
		rows.iter()
			.map(|row| match row {
				Value::Object(o) => o.get("id").cloned().unwrap_or(Value::None),
				_ => Value::None,
			})
			.collect()
	}

	/// People linking to teams, one link dangling and one unset.
	async fn people_and_teams() -> TestDb {
		TestDb::new(
			"DEFINE TABLE team SCHEMALESS; DEFINE TABLE person SCHEMALESS;
			 CREATE team:a SET name = 'A', active = true;
			 CREATE team:b SET name = 'B', active = false;
			 CREATE person:1 SET team = team:a;
			 CREATE person:2 SET team = team:b;
			 CREATE person:3 SET team = team:zzz;
			 CREATE person:4;",
		)
		.await
	}

	async fn lookup(
		ctx: &crate::exec::ExecutionContext,
		left: Vec<Value>,
		right_filter: Option<&str>,
		join_type: JoinType,
		frame: Option<JoinFrame>,
	) -> Arc<dyn ExecOperator> {
		let leaf: Arc<dyn ExecOperator> = Arc::new(RecordBatchFetch::new(
			TableName::from("team"),
			EnforcementMode::Runtime,
			None,
		));
		let right: Arc<dyn ExecOperator> = match right_filter {
			Some(src) => Arc::new(Filter::new(leaf, physical_expr(src, ctx).await)),
			None => leaf,
		};
		Arc::new(LookupJoin::new(
			ValuesOperator::new(left),
			right,
			physical_expr("team", ctx).await,
			TableName::from("team"),
			join_type,
			"_join0".to_string(),
			None,
			frame,
		))
	}

	#[tokio::test]
	async fn a_semi_lookup_keeps_the_rows_whose_link_names_a_record() {
		let db = people_and_teams().await;
		let ctx = db.exec_ctx().await;
		let left = docs(&[
			"{ id: person:1, team: team:a }",
			"{ id: person:2, team: team:b }",
			"{ id: person:3, team: team:zzz }",
			"{ id: person:4 }",
			"{ id: person:5, team: [team:a] }",
		])
		.await;
		let op = lookup(&ctx, left, None, JoinType::Semi, None).await;
		// A dangling link, an unset link and an array of links match nothing.
		assert_eq!(
			ids(&collect(&op, &ctx).await),
			vec![val("person:1").await, val("person:2").await]
		);
	}

	#[tokio::test]
	async fn the_right_chain_filters_what_the_read_yields() {
		let db = people_and_teams().await;
		let ctx = db.exec_ctx().await;
		let left =
			docs(&["{ id: person:1, team: team:a }", "{ id: person:2, team: team:b }"]).await;
		let semi = lookup(&ctx, left.clone(), Some("active = true"), JoinType::Semi, None).await;
		assert_eq!(ids(&collect(&semi, &ctx).await), vec![val("person:1").await]);
		let anti = lookup(&ctx, left, Some("active = true"), JoinType::Anti, None).await;
		assert_eq!(ids(&collect(&anti, &ctx).await), vec![val("person:2").await]);
	}

	#[tokio::test]
	async fn a_left_lookup_lands_the_record_or_null_under_the_alias() {
		let db = people_and_teams().await;
		let ctx = db.exec_ctx().await;
		let left =
			docs(&["{ id: person:1, team: team:a }", "{ id: person:3, team: team:zzz }"]).await;
		let op = lookup(&ctx, left, None, JoinType::Left, None).await;
		let out = collect(&op, &ctx).await;
		assert_eq!(
			out[0],
			val("{ id: person:1, team: team:a, _join0: { id: team:a, name: 'A', active: true } }")
				.await
		);
		assert_eq!(out[1], val("{ id: person:3, team: team:zzz, _join0: NULL }").await);
	}

	#[tokio::test]
	async fn a_framed_semi_join_inside_a_permission_frame_passes_every_row() {
		let db = people_and_teams().await;
		let ctx = db.exec_ctx().await;
		let left = docs(&["{ id: person:3, team: team:zzz }", "{ id: person:4 }"]).await;
		let op =
			lookup(&ctx, left.clone(), None, JoinType::Semi, Some(JoinFrame::new(None, false)))
				.await;
		// Outside a permission frame the join enforces.
		assert!(collect(&op, &ctx).await.is_empty());
		// Inside one it does not, as `PermissionFilter` does not.
		let inside = ctx.clone().with_skip_fetch_perms(true);
		assert_eq!(collect(&op, &inside).await, left);
	}

	#[tokio::test]
	async fn the_leaf_reads_only_the_ids_of_its_table_that_exist() {
		let db = people_and_teams().await;
		let ctx = db.exec_ctx().await;
		let leaf: Arc<dyn ExecOperator> = Arc::new(RecordBatchFetch::new(
			TableName::from("team"),
			EnforcementMode::Planned,
			None,
		));
		let wanted = val("[team:b, team:zzz, person:1, team:a]").await;
		let out = collect(&leaf, &ctx.with_current_value(wanted)).await;
		assert_eq!(ids(&out), vec![val("team:b").await, val("team:a").await]);
		// Nothing to read yields nothing.
		assert!(collect(&leaf, &ctx).await.is_empty());
	}
}
