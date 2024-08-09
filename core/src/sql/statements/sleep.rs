use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Duration, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[non_exhaustive]
pub struct SleepStatement {
	pub(crate) duration: Duration,
}

impl SleepStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Root)?;
		// Calculate the sleep duration
		let dur = match (ctx.timeout(), self.duration.0) {
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
}

impl fmt::Display for SleepStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "SLEEP {}", self.duration)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::dbs::test::mock;
	use std::time::{self, SystemTime};

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
