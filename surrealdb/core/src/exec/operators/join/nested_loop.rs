use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use super::{merge_left_null, merge_records, merge_right_null};
use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};
use crate::expr::join::JoinKind;
use crate::val::Value;

#[derive(Debug)]
pub struct NestedLoopJoin {
	pub(crate) left: Arc<dyn ExecOperator>,
	pub(crate) right: Arc<dyn ExecOperator>,
	pub(crate) kind: JoinKind,
	pub(crate) cond: Option<Arc<dyn PhysicalExpr>>,
	pub(crate) left_alias: String,
	pub(crate) right_alias: String,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl NestedLoopJoin {
	pub(crate) fn new(
		left: Arc<dyn ExecOperator>,
		right: Arc<dyn ExecOperator>,
		kind: JoinKind,
		cond: Option<Arc<dyn PhysicalExpr>>,
		left_alias: String,
		right_alias: String,
	) -> Self {
		Self {
			left,
			right,
			kind,
			cond,
			left_alias,
			right_alias,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for NestedLoopJoin {
	fn name(&self) -> &'static str {
		"NestedLoopJoin"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("join_type".to_string(), format!("{:?}", self.kind))];
		if let Some(ref c) = self.cond {
			attrs.push(("on".to_string(), c.to_sql()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		let base = self.left.required_context().max(self.right.required_context());
		if let Some(ref c) = self.cond {
			base.max(c.required_context())
		} else {
			base
		}
	}

	fn access_mode(&self) -> AccessMode {
		let modes: Vec<AccessMode> = std::iter::once(self.left.access_mode())
			.chain(std::iter::once(self.right.access_mode()))
			.chain(self.cond.iter().map(|c| c.access_mode()))
			.collect();
		modes.into_iter().combine_all()
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.left, &self.right]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn expressions(&self) -> Vec<(&str, &Arc<dyn PhysicalExpr>)> {
		match &self.cond {
			Some(c) => vec![("on", c)],
			None => vec![],
		}
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let right_stream = buffer_stream(
			self.right.execute(ctx)?,
			self.right.access_mode(),
			self.right.cardinality_hint(),
		);
		let left_stream = buffer_stream(
			self.left.execute(ctx)?,
			self.left.access_mode(),
			self.left.cardinality_hint(),
		);

		let cond = self.cond.clone();
		let kind = self.kind.clone();
		let left_alias = self.left_alias.clone();
		let right_alias = self.right_alias.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			let mut right_rows: Vec<Value> = Vec::new();
			{
				futures::pin_mut!(right_stream);
				while let Some(batch) = right_stream.next().await {
					let batch = batch?;
					right_rows.extend(batch.values);
				}
			}

			let mut right_matched = match kind {
				JoinKind::Right => vec![false; right_rows.len()],
				_ => Vec::new(),
			};

			futures::pin_mut!(left_stream);
			while let Some(left_batch) = left_stream.next().await {
				let left_batch = left_batch?;
				let mut output = Vec::new();

				for left_val in &left_batch.values {
					let mut had_match = false;

					for (ri, right_val) in right_rows.iter().enumerate() {
						let merged = merge_records(left_val, right_val, &left_alias, &right_alias);

						let passes = match &cond {
							Some(predicate) => {
								let eval_ctx = EvalContext::from_exec_ctx(&ctx).with_value_and_doc(&merged);
								predicate.evaluate(eval_ctx).await?.is_truthy()
							}
							None => true,
						};

						if passes {
							had_match = true;
							match kind {
								JoinKind::Semi => {
									output.push(left_val.clone());
									break;
								}
								JoinKind::Anti => {
									break;
								}
								JoinKind::Right => {
									if let Some(m) = right_matched.get_mut(ri) {
										*m = true;
									}
									output.push(merged);
								}
								_ => {
									output.push(merged);
								}
							}
						}
					}

					if !had_match {
						match kind {
							JoinKind::Left => {
								output.push(merge_left_null(left_val, &left_alias, &right_alias));
							}
							JoinKind::Anti => {
								output.push(left_val.clone());
							}
							_ => {}
						}
					}
				}

				if !output.is_empty() {
					yield ValueBatch { values: output };
				}
			}

			if let JoinKind::Right = kind {
				let mut output = Vec::new();
				for (ri, matched) in right_matched.iter().enumerate() {
					if !matched {
						output.push(merge_right_null(&right_rows[ri], &left_alias, &right_alias));
					}
				}
				if !output.is_empty() {
					yield ValueBatch { values: output };
				}
			}
		};

		Ok(monitor_stream(Box::pin(stream), "NestedLoopJoin", &self.metrics))
	}
}
