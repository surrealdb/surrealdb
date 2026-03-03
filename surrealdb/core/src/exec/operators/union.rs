//! Union operator for multi-source queries.
//!
//! Combines results from multiple execution plans sequentially,
//! used for `SELECT * FROM a, b, c` which fetches from each source
//! in order. Sequential execution preserves atomicity guarantees
//! when branches contain mutations.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{self, StreamExt};

use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatchStream, buffer_stream, monitor_stream,
};

/// Union operator - combines results from multiple execution plans.
///
/// Executes inputs strictly sequentially: all results from input 0, then all
/// from input 1, etc. Each input's stream is only constructed once the previous
/// input has been fully consumed.
///
/// Sequential execution is required because branches may contain mutations
/// (e.g., UPDATE) and executing them in parallel would break atomicity
/// guarantees.
///
/// This is used for `SELECT * FROM a, b, c` which fetches from a, b, and c
/// in sequence, returning results in order a → b → c.
#[derive(Debug, Clone)]
pub struct Union {
	pub(crate) inputs: Vec<Arc<dyn ExecOperator>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl Union {
	pub(crate) fn new(inputs: Vec<Arc<dyn ExecOperator>>) -> Self {
		Self {
			inputs,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for Union {
	fn name(&self) -> &'static str {
		"Union"
	}

	fn required_context(&self) -> ContextLevel {
		// Union requires the maximum context level of all its inputs
		self.inputs.iter().map(|input| input.required_context()).max().unwrap_or(ContextLevel::Root)
	}

	fn access_mode(&self) -> AccessMode {
		// Combine all inputs' access modes
		self.inputs.iter().map(|input| input.access_mode()).combine_all()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		self.inputs.iter().collect()
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		if self.inputs.is_empty() {
			return Ok(monitor_stream(Box::pin(stream::empty()), "Union", &self.metrics));
		}

		if self.inputs.len() == 1 {
			let stream = buffer_stream(
				self.inputs[0].execute(ctx)?,
				self.inputs[0].access_mode(),
				self.inputs[0].cardinality_hint(),
				ctx.ctx().config().limits.operator_buffer_size,
			);
			return Ok(monitor_stream(stream, "Union", &self.metrics));
		}

		// Execute inputs lazily and sequentially. Each input's stream is only
		// constructed after the previous input has been fully consumed. This
		// ensures that mutations in one branch complete before the next branch
		// begins, preserving atomicity guarantees.
		let inputs = self.inputs.clone();
		let ctx = ctx.clone();

		let combined = stream::unfold(
			(inputs, ctx, 0usize, Option::<ValueBatchStream>::None),
			|(inputs, ctx, mut idx, mut current)| async move {
				loop {
					// Poll the current stream if we have one
					let item = match &mut current {
						Some(stream) => stream.next().await,
						None => None,
					};

					if let Some(item) = item {
						return Some((item, (inputs, ctx, idx, current)));
					}

					// Current stream is exhausted (or there was none) — start
					// the next input. Only now do we call execute(), ensuring
					// the previous branch has been fully drained first.
					if idx >= inputs.len() {
						return None;
					}

					let i = idx;
					idx += 1;

					match inputs[i].execute(&ctx) {
						Ok(stream) => {
							current = Some(buffer_stream(
								stream,
								inputs[i].access_mode(),
								inputs[i].cardinality_hint(),
								ctx.ctx().config().limits.operator_buffer_size,
							))
						}
						Err(e) => return Some((Err(e), (inputs, ctx, idx, None))),
					}
				}
			},
		);

		Ok(monitor_stream(Box::pin(combined), "Union", &self.metrics))
	}
}
