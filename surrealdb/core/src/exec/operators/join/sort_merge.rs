use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use super::{merge_left_null, merge_records, merge_right_null};
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, OutputOrdering, PhysicalExpr, ValueBatch, ValueBatchStream,
	buffer_stream, monitor_stream,
};
use crate::expr::join::JoinKind;
use crate::val::Value;

/// Sort-Merge Join: merges two pre-sorted streams on the join key.
///
/// Both inputs MUST be sorted in ascending order on their respective join
/// key columns. The planner verifies this via `output_ordering()` before
/// selecting this operator.
///
/// O(n + m) time, O(g) memory where g is the size of the largest group of
/// equal-key right rows. Preserves sort order on the join key.
#[derive(Debug)]
pub struct SortMergeJoin {
	pub(crate) left: Arc<dyn ExecOperator>,
	pub(crate) right: Arc<dyn ExecOperator>,
	pub(crate) kind: JoinKind,
	pub(crate) left_key: Arc<dyn PhysicalExpr>,
	pub(crate) right_key: Arc<dyn PhysicalExpr>,
	pub(crate) left_alias: String,
	pub(crate) right_alias: String,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SortMergeJoin {
	pub(crate) fn new(
		left: Arc<dyn ExecOperator>,
		right: Arc<dyn ExecOperator>,
		kind: JoinKind,
		left_key: Arc<dyn PhysicalExpr>,
		right_key: Arc<dyn PhysicalExpr>,
		left_alias: String,
		right_alias: String,
	) -> Self {
		Self {
			left,
			right,
			kind,
			left_key,
			right_key,
			left_alias,
			right_alias,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for SortMergeJoin {
	fn name(&self) -> &'static str {
		"SortMergeJoin"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![
			("join_type".to_string(), format!("{:?}", self.kind)),
			("left_key".to_string(), self.left_key.to_sql()),
			("right_key".to_string(), self.right_key.to_sql()),
		]
	}

	fn required_context(&self) -> ContextLevel {
		self.left
			.required_context()
			.max(self.right.required_context())
			.max(self.left_key.required_context())
			.max(self.right_key.required_context())
	}

	fn access_mode(&self) -> AccessMode {
		[
			self.left.access_mode(),
			self.right.access_mode(),
			self.left_key.access_mode(),
			self.right_key.access_mode(),
		]
		.into_iter()
		.combine_all()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.left, &self.right]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		vec![("left_key", &self.left_key), ("right_key", &self.right_key)]
	}

	fn output_ordering(&self) -> OutputOrdering {
		self.left.output_ordering()
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let left_stream = buffer_stream(
			self.left.execute(ctx)?,
			self.left.access_mode(),
			self.left.cardinality_hint(),
		);
		let right_stream = buffer_stream(
			self.right.execute(ctx)?,
			self.right.access_mode(),
			self.right.cardinality_hint(),
		);

		let left_key = Arc::clone(&self.left_key);
		let right_key = Arc::clone(&self.right_key);
		let kind = self.kind.clone();
		let left_alias = self.left_alias.clone();
		let right_alias = self.right_alias.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			// Materialize both sides into sorted vectors.
			// The inputs are guaranteed pre-sorted by the planner.
			let mut left_rows: Vec<(String, Value)> = Vec::new();
			{
				futures::pin_mut!(left_stream);
				while let Some(batch) = left_stream.next().await {
					let batch = batch?;
					for val in batch.values {
						let k = left_key.evaluate(
							EvalContext::from_exec_ctx(&ctx).with_value_and_doc(&val)
						).await?;
						left_rows.push((k.to_raw_string(), val));
					}
				}
			}

			let mut right_rows: Vec<(String, Value)> = Vec::new();
			{
				futures::pin_mut!(right_stream);
				while let Some(batch) = right_stream.next().await {
					let batch = batch?;
					for val in batch.values {
						let k = right_key.evaluate(
							EvalContext::from_exec_ctx(&ctx).with_value_and_doc(&val)
						).await?;
						right_rows.push((k.to_raw_string(), val));
					}
				}
			}

			let mut li = 0usize;
			let mut ri = 0usize;
			let mut right_matched = vec![false; right_rows.len()];
			let mut output = Vec::new();

			while li < left_rows.len() && ri < right_rows.len() {
				let lk = &left_rows[li].0;
				let rk = &right_rows[ri].0;

				if lk < rk {
					// Left key is smaller -- no right match for this left row
					match kind {
						JoinKind::Left => {
							output.push(merge_left_null(&left_rows[li].1, &left_alias, &right_alias));
						}
						JoinKind::Anti => {
							output.push(left_rows[li].1.clone());
						}
						_ => {}
					}
					li += 1;
				} else if lk > rk {
					ri += 1;
				} else {
					// Keys match. Collect the group of right rows with equal key.
					let group_start = ri;
					let match_key = lk.clone();
					while ri < right_rows.len() && right_rows[ri].0 == match_key {
						ri += 1;
					}
					let right_group = &right_rows[group_start..ri];

					// Process all left rows with the same key
					while li < left_rows.len() && left_rows[li].0 == match_key {
						match kind {
							JoinKind::Semi => {
								output.push(left_rows[li].1.clone());
							}
							JoinKind::Anti => {
								// Has match -- skip
							}
							_ => {
								for (gi, (_, rv)) in right_group.iter().enumerate() {
									if matches!(kind, JoinKind::Right) {
										right_matched[group_start + gi] = true;
									}
									output.push(merge_records(
										&left_rows[li].1, rv, &left_alias, &right_alias,
									));
								}
							}
						}
						li += 1;
					}
				}

				if output.len() >= 1000 {
					let batch = std::mem::take(&mut output);
					yield ValueBatch { values: batch };
				}
			}

			// Emit remaining unmatched left rows
			while li < left_rows.len() {
				match kind {
					JoinKind::Left => {
						output.push(merge_left_null(&left_rows[li].1, &left_alias, &right_alias));
					}
					JoinKind::Anti => {
						output.push(left_rows[li].1.clone());
					}
					_ => {}
				}
				li += 1;
			}

			// Emit unmatched right rows for RIGHT JOIN
			if matches!(kind, JoinKind::Right) {
				for (i, matched) in right_matched.iter().enumerate() {
					if !matched {
						output.push(merge_right_null(&right_rows[i].1, &left_alias, &right_alias));
					}
				}
			}

			if !output.is_empty() {
				yield ValueBatch { values: output };
			}
		};

		Ok(monitor_stream(Box::pin(stream), "SortMergeJoin", &self.metrics))
	}
}
