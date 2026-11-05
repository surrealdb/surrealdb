//! Sleep function for the engine.
//!
//! This provides an async sleep function that respects the context cancellation.

use anyhow::Result;

use crate::define_async_function;
use crate::exec::EvalContext;
use crate::exec::function::ScalarFunction;
use crate::val::Value;

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
		_ = common::time::sleep(duration.0) => Ok(Value::None),
		_ = cancellation.cancelled() => {
			// Cancelled, return None without error
			Ok(Value::None)
		}
	}
}

// =========================================================================
// Function definition using the macro
// =========================================================================

define_async_function!(Sleep, "sleep", effects: EffectSet::BLOCKING, fallibility: Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA), (duration: Duration) -> None, sleep_impl);

// =========================================================================
// Registration
// =========================================================================

// The delay is the effect of this call, not the NONE it returns.
// `EffectSet::BLOCKING` declares that, so a rewrite rule may neither drop
// the call nor repeat it.
/// The `sleep` scalar builtin.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&Sleep];
