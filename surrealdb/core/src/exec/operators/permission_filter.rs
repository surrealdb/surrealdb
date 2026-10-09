//! PermissionFilter operator: a table's `PERMISSIONS FOR select` predicate,
//! applied per row in the permission frame.
//!
//! The predicate is the definer's, so it is evaluated the way every stored
//! permission predicate is: reads inside it are not themselves
//! permission-checked (a predicate following a record link would otherwise
//! recurse into itself), a write inside it is rejected (GHSA-66r2-5gwj-gxm2),
//! and it runs under the authority the definition recorded, not the caller's.
//! The same frame is what [`crate::exec::permission::check_permission_for_value`]
//! establishes for a permission resolved at scan time.

#[cfg(test)]
use std::sync::Arc;

#[cfg(test)]
use futures::StreamExt;
use surrealdb_iam::AuthLimit;

#[cfg(test)]
use crate::err::EngineError;
use crate::exec::ExecutionContext;
#[cfg(test)]
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, EvalContext, ExecOperator, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
#[cfg(test)]
use crate::expr::ControlFlow;
use crate::expr::logical::GuardKind;

#[cfg(test)]
/// Keeps the rows of `input` for which `predicate` is truthy, evaluating the
/// predicate in the permission frame.
///
/// Each row is bound as both the current value and the document root, so
/// `$this` and `$parent` inside the predicate — including inside an
/// idiom-level filter such as `acl[WHERE $parent.owner = $auth.id]` — denote
/// the row under test. The rows arrive as stored; a predicate reading one of
/// the table's computed fields is compiled to derive it on a copy of the row
/// in this frame (`exec::permission::ComputedFieldGuard`), and the row passes
/// on unchanged.
///
/// A context already inside a permission frame (`skip_fetch_perms` set on its
/// root) passes every row through: enforcement is bypassed there so that a
/// definer-authored predicate can read freely, which is also what stops a
/// cyclic permission from recursing.
#[derive(Debug, Clone)]
pub struct PermissionFilterExec {
	pub(crate) input: Arc<dyn ExecOperator>,
	pub(crate) predicate: Arc<dyn PhysicalExpr>,
	/// The authority the predicate runs under; `None` runs as the caller.
	pub(crate) auth: Option<AuthLimit>,
	/// Which stored clause the predicate is, which decides the frame it is
	/// evaluated in; see [`Self::frame_context`].
	pub(crate) guard: GuardKind,
	/// Whether the plan is compiled for a caller the predicate is enforced
	/// on; see [`Self::for_checked_caller`].
	pub(crate) checked_caller: bool,
	/// Whether the predicate was offered to the access path of the scan
	/// beneath this node; see [`Self::with_access_path_from_predicate`].
	pub(crate) access_path_from_predicate: bool,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

#[cfg(test)]
impl PermissionFilterExec {
	#[cfg(test)]
	pub(crate) fn new(
		input: Arc<dyn ExecOperator>,
		predicate: Arc<dyn PhysicalExpr>,
		auth: Option<AuthLimit>,
		guard: GuardKind,
	) -> Self {
		Self {
			input,
			predicate,
			auth,
			guard,
			checked_caller: false,
			access_path_from_predicate: false,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}

	#[cfg(test)]
	/// Records whether the plan is compiled for a caller the predicate is
	/// enforced on. The predicate is the table definer's, and a caller it is
	/// enforced on must not learn it from the plan: for a checked caller the
	/// `EXPLAIN` attributes render it as `<permission>` and the operator
	/// reports no expressions, so neither form of `EXPLAIN` carries its text,
	/// and `EXPLAIN ANALYZE` withholds the metrics of every operator beneath
	/// it, which count records the caller may not see. An unchecked caller
	/// (a root or database owner) sees the predicate and the metrics.
	pub(crate) fn for_checked_caller(mut self, checked: bool) -> Self {
		self.checked_caller = checked;
		self
	}

	#[cfg(test)]
	/// Records whether the predicate was offered to the access path of the
	/// scan beneath this node. When it was, the bound an index scan or a
	/// record-id lookup beneath prints may be the definer's, so for a checked
	/// caller `EXPLAIN` renders every access path beneath as `<permission>`.
	pub(crate) fn with_access_path_from_predicate(mut self, offered: bool) -> Self {
		self.access_path_from_predicate = offered;
		self
	}

	#[cfg(test)]
	/// The context the predicate is evaluated in: the permission frame, under
	/// the recorded authority.
	///
	/// Every predicate here is evaluated with enforcement disabled, so it does
	/// not recurse into the gates of the table it is guarding. A `SELECT`
	/// predicate must on top of that be free of observable side effects, so
	/// its frame refuses a write (GHSA-66r2-5gwj-gxm2); a create/update/delete
	/// predicate filters the rows a mutation is about to write and is reached
	/// from a statement that is already writing, so a write of its own runs.
	fn frame_context(&self, ctx: &ExecutionContext) -> ExecutionContext {
		permission_frame(ctx, self.guard, self.auth.as_ref())
	}
}

/// The context a table's permission predicate is evaluated in: its reads are
/// not themselves permission-checked, a read guard refuses a write, and it
/// runs under the authority the definition recorded when there is one. The
/// predicate is definer code, so the frame is privileged
/// ([`ExecutionContext::privileged_frame`]): it holds none of the caller's
/// bindings. Every engine that applies a `PermissionFilter` evaluates its
/// predicate here.
pub(crate) fn permission_frame(
	ctx: &ExecutionContext,
	guard: GuardKind,
	auth: Option<&AuthLimit>,
) -> ExecutionContext {
	let frame = ctx.clone().privileged_frame().with_skip_fetch_perms(true);
	let frame = match guard {
		GuardKind::Read => frame.refusing_writes(crate::dbs::NoWriteFrame::PermissionPredicate),
		GuardKind::Write => frame,
	};
	match auth {
		Some(limit) => frame.with_limited_auth(limit),
		None => frame,
	}
}

#[cfg(test)]
impl ExecOperator for PermissionFilterExec {
	fn name(&self) -> &'static str {
		"PermissionFilter"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let predicate = if self.checked_caller {
			"<permission>".to_string()
		} else {
			self.predicate.to_sql()
		};
		let mut attrs = vec![("predicate".to_string(), predicate)];
		if self.auth.is_some() {
			attrs.push(("auth".to_string(), "limited".to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		// A permission predicate belongs to a table definition, so evaluating
		// it needs the database it is defined in.
		ContextLevel::Database
			.max(self.predicate.required_context())
			.max(self.input.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		// The permission frame rejects writes, so the predicate cannot add
		// write access in practice; combining stays conservative.
		self.input.access_mode().combine(self.predicate.access_mode())
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.input.cardinality_hint()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn hides_metrics_beneath(&self) -> bool {
		self.checked_caller
	}

	fn redacts_access_paths_beneath(&self) -> bool {
		self.checked_caller && self.access_path_from_predicate
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		if self.checked_caller {
			Vec::new()
		} else {
			vec![("predicate", &self.predicate)]
		}
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.input.output_ordering()
	}

	fn constant_output_fields(&self) -> Vec<crate::exec::field_path::FieldPath> {
		self.input.constant_output_fields()
	}

	#[cfg_attr(
		feature = "observability",
		tracing::instrument(name = "PermissionFilter::execute", level = "trace", skip_all)
	)]
	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.access_mode().combine(self.input.access_mode()),
			self.input.cardinality_hint(),
			ctx.root().ctx.config.exec.operator_buffer_size,
		);

		// Inside a permission predicate every row passes; see the type docs.
		if ctx.root().skip_fetch_perms {
			return Ok(monitor_stream(input_stream, "PermissionFilter", &self.metrics));
		}

		let predicate = Arc::clone(&self.predicate);
		let frame_ctx = self.frame_context(ctx);

		let filtered = input_stream.filter_map(move |batch_result| {
			let predicate = Arc::clone(&predicate);
			let frame_ctx = frame_ctx.clone();

			async move {
				let mut batch = match batch_result {
					Ok(b) => b,
					Err(e) => return Some(Err(e)),
				};
				if let Err(err) = filter_batch_in_place(&mut batch, &*predicate, &frame_ctx).await {
					return Some(Err(err));
				}
				if batch.is_empty() {
					None
				} else {
					Some(Ok(batch))
				}
			}
		});

		Ok(monitor_stream(Box::pin(filtered), "PermissionFilter", &self.metrics))
	}
}

#[cfg(test)]
/// Evaluate the predicate for every row of `batch` in the permission frame,
/// keeping the truthy rows in place.
///
/// A predicate that signals control flow has nowhere to send it — there is no
/// surrounding block — so those become an error, as they do for a permission
/// resolved at scan time. Errors keep their type so a write conflict raised
/// inside the predicate stays retryable and a cancelled query stays reported
/// as cancelled.
async fn filter_batch_in_place(
	batch: &mut ValueBatch,
	predicate: &dyn PhysicalExpr,
	frame_ctx: &ExecutionContext,
) -> FlowResult<()> {
	let eval_ctx = EvalContext::from_exec_ctx(frame_ctx);
	let results =
		predicate.evaluate_batch(eval_ctx, batch.values()).await.map_err(|ctrl| match ctrl {
			ControlFlow::Err(e) => ControlFlow::Err(e),
			other => ControlFlow::Err(anyhow::Error::new(EngineError::Internal(format!(
				"unexpected control flow in a permission predicate: {other}"
			)))),
		})?;

	let mut write_idx = 0;
	for (read_idx, result) in results.into_iter().enumerate() {
		if result.is_truthy() {
			if write_idx != read_idx {
				batch.values_mut().swap(write_idx, read_idx);
			}
			write_idx += 1;
		}
	}
	batch.values_mut().truncate(write_idx);
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::{
		TestDb, ValuesOperator, collect, field_idiom, physical_expr, val,
	};
	use crate::val::Value;

	async fn rows(srcs: &[&str]) -> Vec<Value> {
		let mut out = Vec::with_capacity(srcs.len());
		for src in srcs {
			out.push(val(src).await);
		}
		out
	}

	#[tokio::test]
	async fn keeps_only_the_rows_the_predicate_admits() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let input = ValuesOperator::new(
			rows(&["{ n: 1, public: true }", "{ n: 2, public: false }", "{ n: 3, public: true }"])
				.await,
		);
		let op: Arc<dyn ExecOperator> = Arc::new(PermissionFilterExec::new(
			input,
			physical_expr("public = true", &ctx).await,
			None,
			GuardKind::Read,
		));
		let out = collect(&op, &ctx).await;
		assert_eq!(out.len(), 2);
		assert_eq!(out[0].pick(&field_idiom("n").0), Value::from(1));
		assert_eq!(out[1].pick(&field_idiom("n").0), Value::from(3));
	}

	#[tokio::test]
	async fn the_predicate_sees_the_row_as_parent_and_this() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let input = ValuesOperator::new(rows(&["{ n: 1 }", "{ n: 2 }"]).await);
		let op: Arc<dyn ExecOperator> = Arc::new(PermissionFilterExec::new(
			input,
			physical_expr("$parent.n = 2 AND $this.n = 2", &ctx).await,
			None,
			GuardKind::Read,
		));
		let out = collect(&op, &ctx).await;
		assert_eq!(out, rows(&["{ n: 2 }"]).await);
	}

	#[tokio::test]
	async fn inside_a_permission_frame_every_row_passes() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await.with_skip_fetch_perms(true);
		let input = ValuesOperator::new(rows(&["{ n: 1 }", "{ n: 2 }"]).await);
		let op: Arc<dyn ExecOperator> = Arc::new(PermissionFilterExec::new(
			input,
			physical_expr("false", &ctx).await,
			None,
			GuardKind::Read,
		));
		assert_eq!(collect(&op, &ctx).await.len(), 2);
	}

	#[tokio::test]
	async fn requires_a_database_context() {
		let db = TestDb::new("").await;
		let ctx = db.exec_ctx().await;
		let op = PermissionFilterExec::new(
			ValuesOperator::new(Vec::new()),
			physical_expr("true", &ctx).await,
			None,
			GuardKind::Read,
		);
		assert_eq!(op.required_context(), ContextLevel::Database);
		assert_eq!(op.name(), "PermissionFilter");
	}
}
