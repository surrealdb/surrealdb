//! Test-only helpers for exercising MATCH operators in isolation.
//!
//! The exec module has no general-purpose stub source operator, so the
//! binding-table operators (which all wrap a single input) need one to be
//! driven end-to-end through their `execute()` streams without a datastore.
//! [`ValuesOperator`] replays a fixed list of [`Value`] rows as one batch, and
//! [`root_ctx`] builds a minimal root-level [`ExecutionContext`] that satisfies
//! `buffer_stream`/`monitor_stream` (neither touches the transaction).

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::ctx::Context;
use crate::exec::context::RootContext;
use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OutputOrdering, ValueBatch, ValueBatchStream,
};
use crate::iam::Auth;
use crate::val::Value;

/// A source operator that yields a fixed list of rows as a single batch.
///
/// `cardinality` lets a test pick the buffering strategy `buffer_stream`
/// applies to the consuming operator; the default is `Unbounded`, matching the
/// status-quo path most operators see.
#[derive(Debug)]
pub(crate) struct ValuesOperator {
	values: Vec<Value>,
	cardinality: CardinalityHint,
	ordering: OutputOrdering,
}

impl ValuesOperator {
	/// Build a source over the given rows with conservative `Unbounded`
	/// cardinality and `Unordered` output. Returns a trait object (the operator
	/// builders all hand back `Arc<dyn ExecOperator>`), not `Self`.
	#[allow(clippy::new_ret_no_self)]
	pub(crate) fn new(values: Vec<Value>) -> Arc<dyn ExecOperator> {
		Arc::new(Self {
			values,
			cardinality: CardinalityHint::Unbounded,
			ordering: OutputOrdering::Unordered,
		})
	}
}

impl ExecOperator for ValuesOperator {
	fn name(&self) -> &'static str {
		"Values"
	}

	fn required_context(&self) -> ContextLevel {
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		AccessMode::ReadOnly
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		self.cardinality
	}

	fn output_ordering(&self) -> OutputOrdering {
		self.ordering.clone()
	}

	fn execute(&self, _ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let values = self.values.clone();
		Ok(Box::pin(futures::stream::once(std::future::ready(Ok(ValueBatch {
			values,
		})))))
	}
}

/// Build a minimal root-level [`ExecutionContext`] for operator tests.
///
/// It carries no transaction or datastore — only the config (for
/// `operator_buffer_size`) and the bits `buffer_stream`/`monitor_stream` read.
/// Operators that fetch records or evaluate context-bound expressions cannot
/// run under it; Bind/Distinct (this slice) do not.
pub(crate) fn root_ctx() -> ExecutionContext {
	ExecutionContext::Root(RootContext {
		ctx: Context::new_test().freeze(),
		options: None,
		datastore: None,
		cancellation: CancellationToken::new(),
		auth: Arc::new(Auth::default()),
		session: None,
		current_value: None,
		skip_fetch_perms: false,
		version_stamp: None,
	})
}

/// Drain an operator's output stream into a flat list of rows.
pub(crate) async fn collect(op: &Arc<dyn ExecOperator>, ctx: &ExecutionContext) -> Vec<Value> {
	use futures::StreamExt;
	let mut stream = op.execute(ctx).expect("execute should succeed");
	let mut out = Vec::new();
	while let Some(batch) = stream.next().await {
		out.extend(batch.expect("batch should be Ok").values);
	}
	out
}
