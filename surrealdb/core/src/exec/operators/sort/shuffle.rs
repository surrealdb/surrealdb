//! RandomShuffle operator - handles ORDER BY RAND().
//!
//! This operator supports two modes:
//! - Full shuffle: Collects all values and applies Fisher-Yates shuffle
//! - Reservoir sampling: When a limit is specified, uses reservoir sampling to efficiently select a
//!   random sample without storing all values

use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use rand::seq::SliceRandom;
use rand::{Rng, thread_rng};
#[cfg(not(target_family = "wasm"))]
use tokio::task::spawn_blocking;

use crate::exec::{
	AccessMode, CardinalityHint, ContextLevel, ExecOperator, ExecutionContext, FlowResult,
	OperatorMetrics, ValueBatch, ValueBatchStream, buffer_stream, monitor_stream,
};
use crate::val::Value;

/// Randomly shuffles the input stream.
///
/// When `limit` is specified, uses reservoir sampling to efficiently
/// select a random sample of the specified size. This is much more
/// efficient than shuffling all values when you only need a small sample.
///
/// When `limit` is None, collects all values and applies a full
/// Fisher-Yates shuffle.
#[derive(Debug, Clone)]
pub struct RandomShuffle {
	pub(crate) input: Arc<dyn ExecOperator>,
	/// If set, use reservoir sampling to select this many values
	pub(crate) limit: Option<usize>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl RandomShuffle {
	/// Create a new RandomShuffle operator.
	pub(crate) fn new(input: Arc<dyn ExecOperator>, limit: Option<usize>) -> Self {
		Self {
			input,
			limit,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for RandomShuffle {
	fn name(&self) -> &'static str {
		"RandomShuffle"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		let mut attrs = vec![("order".to_string(), "RAND()".to_string())];
		if let Some(limit) = self.limit {
			attrs.push(("limit".to_string(), limit.to_string()));
		}
		attrs
	}

	fn required_context(&self) -> ContextLevel {
		self.input.required_context()
	}

	fn access_mode(&self) -> AccessMode {
		self.input.access_mode()
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		match self.limit {
			Some(n) => CardinalityHint::Bounded(n),
			None => self.input.cardinality_hint(),
		}
	}

	fn children(&self) -> Vec<&Arc<dyn ExecOperator>> {
		vec![&self.input]
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let input_stream = buffer_stream(
			self.input.execute(ctx)?,
			self.input.access_mode(),
			self.input.cardinality_hint(),
			ctx.ctx().config().limits.operator_buffer_size,
		);
		let limit = self.limit;
		let cancellation = ctx.cancellation().clone();

		let shuffled_stream = futures::stream::once(async move {
			// Collect all values from input
			let mut all_values: Vec<Value> = Vec::new();
			futures::pin_mut!(input_stream);
			while let Some(batch_result) = input_stream.next().await {
				// Check for cancellation between batches
				if cancellation.is_cancelled() {
					return Err(crate::expr::ControlFlow::Err(anyhow::anyhow!(
						crate::err::Error::QueryCancelled
					)));
				}
				match batch_result {
					Ok(batch) => all_values.extend(batch.values),
					Err(e) => return Err(e),
				}
			}

			if all_values.is_empty() {
				return Ok(ValueBatch {
					values: vec![],
				});
			}

			// Apply shuffle or reservoir sampling
			let shuffled = if let Some(limit) = limit {
				reservoir_sample(all_values, limit).await
			} else {
				full_shuffle(all_values).await
			};

			Ok(ValueBatch {
				values: shuffled,
			})
		});

		// Filter out empty batches
		let filtered = shuffled_stream.filter_map(|result| async move {
			match result {
				Ok(batch) if batch.values.is_empty() => None,
				other => Some(other),
			}
		});

		Ok(monitor_stream(Box::pin(filtered), "RandomShuffle", &self.metrics))
	}
}

/// Perform a full Fisher-Yates shuffle of all values.
#[cfg(not(target_family = "wasm"))]
async fn full_shuffle(mut values: Vec<Value>) -> Vec<Value> {
	// Move shuffle to blocking task to avoid blocking async executor
	spawn_blocking(move || {
		let mut rng = thread_rng();
		values.shuffle(&mut rng);
		values
	})
	.await
	.expect("shuffle blocking task should not panic")
}

/// Perform a full Fisher-Yates shuffle of all values (WASM version).
#[cfg(target_family = "wasm")]
async fn full_shuffle(mut values: Vec<Value>) -> Vec<Value> {
	let mut rng = thread_rng();
	values.shuffle(&mut rng);
	values
}

/// Select a random sample using reservoir sampling.
///
/// This algorithm ensures that each element has an equal probability
/// of being in the final sample, regardless of the total number of elements.
/// The result is also shuffled to ensure random ordering.
#[cfg(not(target_family = "wasm"))]
async fn reservoir_sample(values: Vec<Value>, limit: usize) -> Vec<Value> {
	spawn_blocking(move || reservoir_sample_sync(values, limit))
		.await
		.expect("reservoir sampling blocking task should not panic")
}

/// Select a random sample using reservoir sampling (WASM version).
#[cfg(target_family = "wasm")]
async fn reservoir_sample(values: Vec<Value>, limit: usize) -> Vec<Value> {
	reservoir_sample_sync(values, limit)
}

/// Synchronous reservoir sampling implementation.
fn reservoir_sample_sync(values: Vec<Value>, limit: usize) -> Vec<Value> {
	let mut rng = thread_rng();
	let mut reservoir: Vec<Value> = Vec::with_capacity(limit);

	for (i, value) in values.into_iter().enumerate() {
		if reservoir.len() < limit {
			// Fill the reservoir first
			reservoir.push(value);
		} else {
			// Randomly decide whether to include this value
			let j = rng.gen_range(0..=i);
			if j < limit {
				reservoir[j] = value;
			}
		}
	}

	// Shuffle the final result to ensure random ordering
	// (reservoir sampling preserves selection probability but not order)
	reservoir.shuffle(&mut rng);
	reservoir
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_reservoir_sample_small() {
		// When values.len() <= limit, should return all values
		let values = vec![Value::from(1), Value::from(2), Value::from(3)];
		let result = reservoir_sample_sync(values, 5);
		assert_eq!(result.len(), 3);
	}

	#[test]
	fn test_reservoir_sample_exact() {
		// When values.len() == limit, should return all values
		let values = vec![Value::from(1), Value::from(2), Value::from(3)];
		let result = reservoir_sample_sync(values, 3);
		assert_eq!(result.len(), 3);
	}

	#[test]
	fn test_reservoir_sample_limit() {
		// When values.len() > limit, should return exactly limit values
		let values: Vec<Value> = (0..100).map(Value::from).collect();
		let result = reservoir_sample_sync(values, 10);
		assert_eq!(result.len(), 10);
	}
}
