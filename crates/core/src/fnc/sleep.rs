use anyhow::Result;

use crate::ctx::Context;
use crate::val::{Duration, Value};

/// Sleep during the provided duration parameter.
pub async fn sleep(ctx: &Context, (dur,): (Duration,)) -> Result<Value> {
	// Calculate the sleep duration
	let dur = match (ctx.timeout(), dur.0) {
		(Some(t), d) if t < d => t,
		(_, d) => d,
	};
	// Sleep for the specified time
	#[cfg(target_family = "wasm")]
	wasmtimer::tokio::sleep(dur).await;
	#[cfg(not(target_family = "wasm"))]
	tokio::time::sleep(dur).await;
	// Ok all good
	Ok(Value::None)
}
