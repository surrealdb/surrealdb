//! Sleep function for the streaming executor.
//!
//! This provides an async sleep function that respects the context cancellation.

use anyhow::Result;

use crate::exec::function::FunctionRegistry;
use crate::exec::physical_expr::EvalContext;
use crate::val::Value;
use crate::{define_async_function, register_functions};

// =========================================================================
// Implementation function
// =========================================================================

async fn sleep_impl(ctx: &EvalContext<'_>, args: Vec<Value>) -> Result<Value> {
	// Extract the duration argument
	let duration = match args.into_iter().next() {
		Some(Value::Duration(d)) => d,
		Some(v) => {
			return Err(anyhow::anyhow!(
				"Function 'sleep' expects a duration argument, got: {}",
				v.kind_of()
			));
		}
		None => return Err(anyhow::anyhow!("Function 'sleep' expects a duration argument")),
	};

	// Get the cancellation token from context
	let cancellation = ctx.exec_ctx.cancellation();

	// Sleep with cancellation support
	tokio::select! {
		_ = tokio::time::sleep(duration.0) => Ok(Value::None),
		_ = cancellation.cancelled() => {
			// Cancelled, return None without error
			Ok(Value::None)
		}
	}
}

// =========================================================================
// Function definition using the macro
// =========================================================================

define_async_function!(Sleep, "sleep", (duration: Duration) -> None, sleep_impl);

// =========================================================================
// Registration
// =========================================================================

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, Sleep,);
}
