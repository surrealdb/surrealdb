use crate::ctx::Context;
use crate::err::Error;
use crate::sql::Duration;
use crate::sql::Value;

/// Sleep during the provided duration parameter.
pub async fn sleep(ctx: &Context, (dur,): (Duration,)) -> Result<Value, Error> {
	// Calculate the sleep duration
	let dur = dur.to_std().map_err(|_| Error::InvalidTimeout(dur.to_string()))?;
	let dur = match (ctx.timeout(), dur) {
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
