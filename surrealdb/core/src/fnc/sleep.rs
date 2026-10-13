use anyhow::Result;

use crate::ctx::FrozenContext;
use crate::val::{Duration, Value};

/// Sleep during the provided duration parameter.
///
/// Races a timer against the context's awaitable cancellation token
/// (when installed by the RPC layer) so a client disconnect mid-sleep
/// wakes us up immediately instead of blocking the executor's drain
/// for the full duration. After the select
/// returns, the executor's next `ctx.done(true)` check at the
/// statement boundary observes the cancel flag and bails with
/// `EngineError::QueryCancelled` on the normal error path.
pub async fn sleep(ctx: &FrozenContext, (dur,): (Duration,)) -> Result<Value> {
	// Calculate the sleep duration
	let dur = match (ctx.timeout(), dur.0) {
		(Some(t), d) if t < d => t,
		(_, d) => d,
	};
	// Sleep for the specified time, racing against any installed
	// awaitable cancellation token.
	let sleep_fut = common::time::sleep(dur);
	match ctx.cancel_token() {
		Some(token) => {
			tokio::select! {
				_ = sleep_fut => {}
				_ = token.cancelled() => {}
			}
		}
		None => sleep_fut.await,
	}
	// Ok all good
	Ok(Value::None)
}
