//! Sleep operator - pauses execution for a specified duration.
//!
//! This operator implements the SLEEP statement, which pauses query execution
//! for a given duration. It returns Value::None after the sleep completes.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{
	AccessMode, CardinalityHint, ExecOperator, FlowResult, OperatorMetrics, ValueBatch,
	ValueBatchStream,
};
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::{Duration, Value};

/// Sleep operator - pauses execution for a specified duration.
///
/// The SLEEP statement requires root-level edit permissions. It will pause
/// execution for the specified duration, respecting any query timeout that
/// may be configured.
#[derive(Debug, Clone)]
pub struct SleepPlan {
	/// The duration to sleep for
	pub duration: Duration,
	/// Metrics for EXPLAIN ANALYZE
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl SleepPlan {
	pub(crate) fn new(duration: Duration) -> Self {
		Self {
			duration,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for SleepPlan {
	fn name(&self) -> &'static str {
		"Sleep"
	}

	fn attrs(&self) -> Vec<(String, String)> {
		vec![("duration".to_string(), self.duration.to_sql())]
	}

	fn required_context(&self) -> ContextLevel {
		// Sleep requires root-level permissions, but doesn't need ns/db context
		ContextLevel::Root
	}

	fn access_mode(&self) -> AccessMode {
		// Sleep has a side effect (pausing execution) and requires edit permissions
		AccessMode::ReadWrite
	}

	fn cardinality_hint(&self) -> CardinalityHint {
		CardinalityHint::AtMostOne
	}

	fn metrics(&self) -> Option<&OperatorMetrics> {
		Some(&self.metrics)
	}

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let duration = self.duration;
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			ctx.is_allowed(Action::Edit, ResourceKind::Table, &Base::Root)?;

			// Cap the sleep duration to the context timeout (if any),
			// matching the legacy SleepStatement::compute timeout behavior.
			let effective_duration = match ctx.ctx().timeout() {
				Some(remaining) if remaining < duration.0 => remaining,
				_ => duration.0,
			};

			// Sleep with cancellation support.
			// On WASM we use wasmtimer (no CancellationToken support).
			// On native we race against the CancellationToken so that
			// client disconnects and query cancellations stop the sleep
			// promptly.
			#[cfg(target_family = "wasm")]
			wasmtimer::tokio::sleep(effective_duration).await;

			#[cfg(not(target_family = "wasm"))]
			tokio::select! {
				_ = tokio::time::sleep(effective_duration) => {},
				_ = ctx.cancellation().cancelled() => {},
			}

			// Return Value::None as the result
			Ok(ValueBatch {
				values: vec![Value::None],
			})
		})))
	}

	fn is_scalar(&self) -> bool {
		true
	}
}

impl ToSql for SleepPlan {
	fn fmt_sql(&self, f: &mut String, _fmt: surrealdb_types::SqlFormat) {
		f.push_str("SLEEP ");
		f.push_str(&self.duration.to_sql());
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn test_sleep_plan(duration: Duration) -> SleepPlan {
		SleepPlan::new(duration)
	}

	#[test]
	fn test_sleep_plan_name() {
		let plan = test_sleep_plan(Duration(std::time::Duration::from_millis(100)));
		assert_eq!(plan.name(), "Sleep");
	}

	#[test]
	fn test_sleep_plan_attrs() {
		let plan = test_sleep_plan(Duration(std::time::Duration::from_secs(1)));
		let attrs = plan.attrs();
		assert_eq!(attrs.len(), 1);
		assert_eq!(attrs[0].0, "duration");
	}

	#[test]
	fn test_sleep_plan_is_scalar() {
		let plan = test_sleep_plan(Duration(std::time::Duration::from_millis(100)));
		assert!(plan.is_scalar());
	}

	#[test]
	fn test_sleep_plan_access_mode() {
		let plan = test_sleep_plan(Duration(std::time::Duration::from_millis(100)));
		assert_eq!(plan.access_mode(), AccessMode::ReadWrite);
	}
}
