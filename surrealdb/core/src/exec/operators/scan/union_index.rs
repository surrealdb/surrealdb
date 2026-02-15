//! Union index scan operator for multi-index OR conditions.
//!
//! Created by the planner when the access path is `AccessPath::Union`,
//! meaning the WHERE clause has top-level OR branches that can each be
//! served by a different index. Each sub-operator handles one branch;
//! results are deduplicated by record ID at execution time.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, monitor_stream,
};
use crate::val::{RecordId, Value};

/// Union index scan operator for OR conditions.
///
/// Wraps multiple pre-planned index scan operators (one per OR branch)
/// and executes them sequentially, deduplicating results by record ID
/// so that a record matching multiple branches is only returned once.
///
/// Unlike [`super::super::Union`] (which handles `SELECT FROM a, b, c`),
/// this operator targets a single table with multiple index access paths
/// and performs record-level deduplication.
#[derive(Debug)]
pub struct UnionIndexScan {
	pub(crate) inputs: Vec<Arc<dyn ExecOperator>>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl UnionIndexScan {
	pub(crate) fn new(inputs: Vec<Arc<dyn ExecOperator>>) -> Self {
		Self {
			inputs,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for UnionIndexScan {
	fn name(&self) -> &'static str {
		"UnionIndexScan"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("branches".to_string(), self.inputs.len().to_string())]
	}

	fn required_context(&self) -> ContextLevel {
		self.inputs
			.iter()
			.map(|input| input.required_context())
			.max()
			.unwrap_or(ContextLevel::Database)
	}

	fn access_mode(&self) -> AccessMode {
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
			return Ok(monitor_stream(
				Box::pin(futures::stream::empty()),
				"UnionIndexScan",
				&self.metrics,
			));
		}

		// Execute each sub-operator and collect their streams.
		// Streams are created eagerly (matching the runtime DynamicScan
		// implementation) so that any setup errors surface immediately.
		let mut sub_streams: Vec<ValueBatchStream> = Vec::with_capacity(self.inputs.len());
		for input in &self.inputs {
			let sub_stream = input.execute(ctx)?;
			sub_streams.push(sub_stream);
		}

		let stream: ValueBatchStream = Box::pin(async_stream::try_stream! {
			let mut seen: HashSet<RecordId> = HashSet::new();
			for mut sub_stream in sub_streams {
				while let Some(batch_result) = sub_stream.next().await {
					let batch: ValueBatch = batch_result?;
					let deduped: Vec<Value> = batch.values.into_iter()
						.filter(|v| {
							if let Value::Object(obj) = v
								&& let Some(Value::RecordId(rid)) = obj.get("id")
							{
								return seen.insert(rid.clone());
							}
							true // non-object values pass through
						})
						.collect();
					if !deduped.is_empty() {
						yield ValueBatch { values: deduped };
					}
				}
			}
		});

		Ok(monitor_stream(stream, "UnionIndexScan", &self.metrics))
	}
}
