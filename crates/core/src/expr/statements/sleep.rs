use anyhow::Result;
use tokio::time::timeout;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::iam::{Action, ResourceKind};
use crate::val::{Duration, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct SleepStatement {
	pub(crate) duration: Duration,
}

impl SleepStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Root)?;
		// Is there a timeout?
		if let Some(t) = ctx.timeout() {
			println!("{t:?} - {:?}", self.duration.0);
			timeout(t, self.sleep()).await?;
		} else {
			self.sleep().await;
		}
		// Ok all good
		Ok(Value::None)
	}

	// Sleep for the specified time
	async fn sleep(&self) {
		#[cfg(target_family = "wasm")]
		wasmtimer::tokio::sleep(dur).await;
		#[cfg(not(target_family = "wasm"))]
		tokio::time::sleep(self.duration.0).await;
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use std::time::{self, SystemTime};

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
