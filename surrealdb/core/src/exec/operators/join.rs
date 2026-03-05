use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;

use crate::exec::{
	AccessMode, CombineAccessModes, ContextLevel, EvalContext, ExecOperator, ExecutionContext,
	FlowResult, OperatorMetrics, PhysicalExpr, ValueBatch, ValueBatchStream, buffer_stream,
	monitor_stream,
};
use crate::expr::join::JoinKind;
use crate::val::{Object, Value};

fn merge_records(left: &Value, right: &Value, left_alias: &str, right_alias: &str) -> Value {
	let mut map = BTreeMap::new();
	if !left_alias.is_empty() {
		map.insert(left_alias.to_string(), left.clone());
	}
	if !right_alias.is_empty() {
		map.insert(right_alias.to_string(), right.clone());
	}
	if let Value::Object(obj) = left {
		for (k, v) in obj.iter() {
			map.entry(k.clone()).or_insert_with(|| v.clone());
		}
	}
	if let Value::Object(obj) = right {
		for (k, v) in obj.iter() {
			map.entry(k.clone()).or_insert_with(|| v.clone());
		}
	}
	Value::Object(Object(map))
}

fn merge_left_null(left: &Value, left_alias: &str, right_alias: &str) -> Value {
	let mut map = BTreeMap::new();
	map.insert(left_alias.to_string(), left.clone());
	map.insert(right_alias.to_string(), Value::Null);
	if let Value::Object(obj) = left {
		for (k, v) in obj.iter() {
			map.entry(k.clone()).or_insert_with(|| v.clone());
		}
	}
	Value::Object(Object(map))
}

fn merge_right_null(right: &Value, left_alias: &str, right_alias: &str) -> Value {
	let mut map = BTreeMap::new();
	map.insert(left_alias.to_string(), Value::Null);
	map.insert(right_alias.to_string(), right.clone());
	if let Value::Object(obj) = right {
		for (k, v) in obj.iter() {
			map.entry(k.clone()).or_insert_with(|| v.clone());
		}
	}
	Value::Object(Object(map))
}

// ============================================================================
// NestedLoopJoin
// ============================================================================

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
							if let JoinKind::Right = kind {
								if let Some(m) = right_matched.get_mut(ri) {
									*m = true;
								}
							}
							output.push(merged);
						}
					}

					if !had_match {
						match kind {
							JoinKind::Left | JoinKind::Cross => {
								if matches!(kind, JoinKind::Left) {
									output.push(merge_left_null(left_val, &left_alias, &right_alias));
								}
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

// ============================================================================
// HashJoin
// ============================================================================

#[derive(Debug)]
pub struct HashJoin {
	pub(crate) left: Arc<dyn ExecOperator>,
	pub(crate) right: Arc<dyn ExecOperator>,
	pub(crate) kind: JoinKind,
	pub(crate) left_key: Arc<dyn PhysicalExpr>,
	pub(crate) right_key: Arc<dyn PhysicalExpr>,
	pub(crate) left_alias: String,
	pub(crate) right_alias: String,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl HashJoin {
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
impl ExecOperator for HashJoin {
	fn name(&self) -> &'static str {
		"HashJoin"
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

		let left_key = Arc::clone(&self.left_key);
		let right_key = Arc::clone(&self.right_key);
		let kind = self.kind.clone();
		let left_alias = self.left_alias.clone();
		let right_alias = self.right_alias.clone();
		let ctx = ctx.clone();

		let stream = async_stream::try_stream! {
			// Build phase: hash the right side
			let mut hash_table: HashMap<String, Vec<Value>> = HashMap::new();
			let mut right_all: Vec<(String, Value)> = Vec::new();
			{
				futures::pin_mut!(right_stream);
				while let Some(batch) = right_stream.next().await {
					let batch = batch?;
					for val in batch.values {
						let key_val = right_key.evaluate(
							EvalContext::from_exec_ctx(&ctx).with_value_and_doc(&val)
						).await?;
						let key_str = key_val.to_raw_string();
						if matches!(kind, JoinKind::Right) {
							right_all.push((key_str.clone(), val.clone()));
						}
						hash_table.entry(key_str).or_default().push(val);
					}
				}
			}

			let mut right_matched: HashMap<String, Vec<bool>> = if matches!(kind, JoinKind::Right) {
				hash_table.iter().map(|(k, v)| (k.clone(), vec![false; v.len()])).collect()
			} else {
				HashMap::new()
			};

			// Probe phase: stream the left side
			futures::pin_mut!(left_stream);
			while let Some(left_batch) = left_stream.next().await {
				let left_batch = left_batch?;
				let mut output = Vec::new();

				for left_val in &left_batch.values {
					let key_val = left_key.evaluate(
						EvalContext::from_exec_ctx(&ctx).with_value_and_doc(left_val)
					).await?;
					let key_str = key_val.to_raw_string();

					let mut had_match = false;
					if let Some(right_vals) = hash_table.get(&key_str) {
						for (ri, right_val) in right_vals.iter().enumerate() {
							had_match = true;
							if let JoinKind::Right = kind {
								if let Some(flags) = right_matched.get_mut(&key_str) {
									if let Some(m) = flags.get_mut(ri) {
										*m = true;
									}
								}
							}
							output.push(merge_records(left_val, right_val, &left_alias, &right_alias));
						}
					}

					if !had_match && matches!(kind, JoinKind::Left) {
						output.push(merge_left_null(left_val, &left_alias, &right_alias));
					}
				}

				if !output.is_empty() {
					yield ValueBatch { values: output };
				}
			}

			// Emit unmatched right rows for RIGHT JOIN
			if let JoinKind::Right = kind {
				let mut output = Vec::new();
				for (key, vals) in &hash_table {
					if let Some(flags) = right_matched.get(key) {
						for (ri, matched) in flags.iter().enumerate() {
							if !matched {
								output.push(merge_right_null(&vals[ri], &left_alias, &right_alias));
							}
						}
					}
				}
				if !output.is_empty() {
					yield ValueBatch { values: output };
				}
			}
		};

		Ok(monitor_stream(Box::pin(stream), "HashJoin", &self.metrics))
	}
}
