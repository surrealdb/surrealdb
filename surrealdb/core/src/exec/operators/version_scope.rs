//! VersionScope operator — sets the evaluated VERSION timestamp on the
//! `ExecutionContext` so that downstream operators (Fetch, FieldPart) read
//! records at the correct point in time.

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, ContextLevel, EvalContext, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, PhysicalExpr, ValueBatchStream, monitor_stream,
};

/// Evaluates the VERSION expression once and propagates the resulting
/// timestamp onto the `ExecutionContext` before delegating to the inner
/// operator tree.
///
/// Inserted by the planner at the top of the SELECT pipeline when a
/// VERSION clause is present.  Scan operators still carry their own
/// version expression for storage-level reads; this operator ensures
/// that non-scan paths (Fetch, FieldPart record dereference) also
/// honour the same timestamp.
#[derive(Debug, Clone)]
pub struct VersionScope {
	pub(crate) inner: Arc<dyn ExecOperator>,
	pub(crate) version: Arc<dyn PhysicalExpr>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl VersionScope {
	pub(crate) fn new(inner: Arc<dyn ExecOperator>, version: Arc<dyn PhysicalExpr>) -> Self {
		Self {
			inner,
			version,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for VersionScope {
	fn name(&self) -> &'static str {
		"VersionScope"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("version".to_string(), self.version.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		self.inner.required_context().max(self.version.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		self.inner.access_mode().combine(self.version.access_mode())
	}

	fn cardinality_hint(&self) -> crate::exec::CardinalityHint {
		self.inner.cardinality_hint()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.inner]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn is_scalar(&self) -> bool {
		self.inner.is_scalar()
	}

	fn output_ordering(&self) -> crate::exec::OutputOrdering {
		self.inner.output_ordering()
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("version", &self.version)]
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let version_expr = self.version.clone();
		let inner = self.inner.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let eval_ctx = EvalContext::from_exec_ctx(&ctx);
			let v = version_expr.evaluate(eval_ctx).await?;
			let stamp = v
				.cast_to::<crate::val::Datetime>()
				.map_err(|e| anyhow::anyhow!("{e}"))?
				.to_version_stamp(ctx.txn().timestamp_impl().as_ref())?;

			let versioned_ctx = ctx.with_version_stamp(Some(stamp));
			let inner_stream = inner.execute(&versioned_ctx)?;
			futures::pin_mut!(inner_stream);
			while let Some(batch) = inner_stream.next().await {
				yield batch?;
			}
		};

		Ok(monitor_stream(Box::pin(stream), "VersionScope", &self.metrics))
	}
}
