use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Duration, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct SleepStatement {
	pub(crate) duration: Duration,
}

impl SleepStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
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
