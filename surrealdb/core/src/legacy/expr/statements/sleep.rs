use anyhow::Result;
use tokio::time::timeout;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::sleep::SleepStatement;
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "SleepStatement::compute", skip_all)]
pub(crate) async fn sleep_statement_compute(
	this: &SleepStatement,
	ctx: &FrozenContext,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Table, Base::Root)?;
	// Is there a timeout?
	if let Some(t) = ctx.timeout() {
		timeout(t, crate::legacy::sleep_statement_sleep(this, ctx)).await?;
	} else {
		crate::legacy::sleep_statement_sleep(this, ctx).await;
	}
	// Ok all good
	Ok(Value::None)
}

/// Sleep for the specified time, racing against any awaitable
/// cancellation token installed on the context. Without the
/// `select!`, a `SLEEP 60s` on a closing WebSocket would block the
/// connection's disconnect drain for the full 60 seconds before
/// the executor's next `ctx.done` check could observe the cancel.
/// After the select returns the outer compute path falls through
/// to the executor's normal yield, which sees the cancel flag and
/// bails with `EngineError::QueryCancelled`.
pub(crate) async fn sleep_statement_sleep(this: &SleepStatement, ctx: &FrozenContext) {
	#[cfg(target_family = "wasm")]
	let sleep_fut = wasmtimer::tokio::sleep(this.duration.0);
	#[cfg(not(target_family = "wasm"))]
	let sleep_fut = tokio::time::sleep(this.duration.0);
	match ctx.cancel_token() {
		Some(token) => {
			tokio::select! {
				_ = sleep_fut => {}
				_ = token.cancelled() => {}
			}
		}
		None => sleep_fut.await,
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::time;

	use web_time::SystemTime;

	use crate::dbs::test::mock;
	use crate::expr::statements::*;
	use crate::val::{Duration, Value};

	#[tokio::test]
	async fn test_sleep_compute() {
		let time = SystemTime::now();
		let (ctx, opt) = mock().await;
		let stm = SleepStatement {
			duration: Duration(time::Duration::from_micros(500)),
		};
		let value = crate::legacy::sleep_statement_compute(&stm, &ctx, &opt, None).await.unwrap();
		assert!(time.elapsed().unwrap() >= time::Duration::from_micros(500));
		assert_eq!(value, Value::None);
	}
}
