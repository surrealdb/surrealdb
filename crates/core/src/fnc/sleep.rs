use crate::ctx::Context;
use crate::err::Error;
use crate::sql::Duration;
use crate::sql::Value;

/// Sleep during the provided duration parameter.
pub async fn sleep(ctx: &Context, (dur,): (Duration,)) -> Result<Value, Error> {
	// Calculate the sleep duration
	let dur = match (ctx.timeout(), dur.0) {
		(Some(t), d) if t < d => t,
		(_, d) => d,
	};
	// Sleep for the specified time
	#[cfg(target_arch = "wasm32")]
	wasmtimer::tokio::sleep(dur).await;
	#[cfg(not(target_arch = "wasm32"))]
	tokio::time::sleep(dur).await;
	// Ok all good
	Ok(Value::None)
}
