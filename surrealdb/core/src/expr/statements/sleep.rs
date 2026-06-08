use anyhow::Result;
use tokio::time::timeout;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::{Duration, Value};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct SleepStatement {
	pub(crate) duration: Duration,
}

impl SleepStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "SleepStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Table, Base::Root)?;
		// Is there a timeout?
		if let Some(t) = ctx.timeout() {
			timeout(t, self.sleep(ctx)).await?;
		} else {
			self.sleep(ctx).await;
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
	/// bails with `Error::QueryCancelled`.
	async fn sleep(&self, ctx: &FrozenContext) {
		#[cfg(target_family = "wasm")]
		let sleep_fut = wasmtimer::tokio::sleep(self.duration.0);
		#[cfg(not(target_family = "wasm"))]
		let sleep_fut = tokio::time::sleep(self.duration.0);
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
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::time;

	use web_time::SystemTime;

	use super::*;
	use crate::dbs::test::mock;

	#[tokio::test]
	async fn test_sleep_compute() {
		let time = SystemTime::now();
		let (ctx, opt) = mock().await;
		let stm = SleepStatement {
			duration: Duration(time::Duration::from_micros(500)),
		};
		let value = stm.compute(&ctx, &opt, None).await.unwrap();
		assert!(time.elapsed().unwrap() >= time::Duration::from_micros(500));
		assert_eq!(value, Value::None);
	}
}
