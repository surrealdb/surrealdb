//! Sleep operator - pauses execution for a specified duration.
//!
//! This operator implements the SLEEP statement, which pauses query execution
//! for a given duration. It returns Value::None after the sleep completes.

use async_trait::async_trait;
use futures::stream;
use surrealdb_types::ToSql;

use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::{AccessMode, ExecOperator, FlowResult, ValueBatch, ValueBatchStream};
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
}

#[async_trait]
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

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let duration = self.duration;
		let ctx = ctx.clone();

		Ok(Box::pin(stream::once(async move {
			ctx.is_allowed(Action::Edit, ResourceKind::Table, &Base::Root)?;

			// Perform the sleep, respecting any query timeout
			// The timeout operator wrapping this plan will handle timeout enforcement
			sleep_for_duration(&duration).await;

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

/// Sleep for the specified duration.
///
/// Uses the appropriate sleep implementation based on the target platform.
async fn sleep_for_duration(duration: &Duration) {
	#[cfg(target_family = "wasm")]
	wasmtimer::tokio::sleep(duration.0).await;
	#[cfg(not(target_family = "wasm"))]
	tokio::time::sleep(duration.0).await;
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

	#[test]
	fn test_sleep_plan_name() {
		let plan = SleepPlan {
			duration: Duration(std::time::Duration::from_millis(100)),
		};
		assert_eq!(plan.name(), "Sleep");
	}

	#[test]
	fn test_sleep_plan_attrs() {
		let plan = SleepPlan {
			duration: Duration(std::time::Duration::from_secs(1)),
		};
		let attrs = plan.attrs();
		assert_eq!(attrs.len(), 1);
		assert_eq!(attrs[0].0, "duration");
	}

	#[test]
	fn test_sleep_plan_is_scalar() {
		let plan = SleepPlan {
			duration: Duration(std::time::Duration::from_millis(100)),
		};
		assert!(plan.is_scalar());
	}

	#[test]
	fn test_sleep_plan_access_mode() {
		let plan = SleepPlan {
			duration: Duration(std::time::Duration::from_millis(100)),
		};
		assert_eq!(plan.access_mode(), AccessMode::ReadWrite);
	}
}
