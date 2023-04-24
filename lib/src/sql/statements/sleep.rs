use crate::ctx::Context;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::duration::duration;
use crate::sql::error::IResult;
use crate::sql::{Duration, Value};
use derive::Store;
use nom::bytes::complete::tag_no_case;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct SleepStatement {
	duration: Duration,
}

impl SleepStatement {
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_txn: &Transaction,
		_doc: Option<&Value>,
	) -> Result<Value, Error> {
		// No need for NS/DB
		opt.needs(Level::Kv)?;
		// Allowed to run?
		opt.check(Level::Kv)?;
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

pub fn sleep(i: &str) -> IResult<&str, SleepStatement> {
	let (i, _) = tag_no_case("SLEEP")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = duration(i)?;
	Ok((
		i,
		SleepStatement {
			duration: v,
		},
	))
}

#[cfg(test)]
mod tests {

	use super::*;
	use crate::dbs::test::mock;
	use crate::dbs::Auth;
	use std::time::SystemTime;

	#[test]
	fn test_sleep_statement_sec() {
		let sql = "SLEEP 2s";
		let res = sleep(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("SLEEP 2s", format!("{}", out))
	}

	#[test]
	fn test_sleep_statement_ms() {
		let sql = "SLEEP 500ms";
		let res = sleep(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		assert_eq!("SLEEP 500ms", format!("{}", out))
	}

	#[tokio::test]
	async fn test_sleep_compute() {
		let sql = "SLEEP 500ms";
		let time = SystemTime::now();
		let opt = Options::new(Auth::Kv);
		let (ctx, _, txn) = mock().await;
		let (_, stm) = sleep(sql).unwrap();
		let value = stm.compute(&ctx, &opt, &txn, None).await.unwrap();
		assert!(time.elapsed().unwrap() >= time::Duration::microseconds(500));
		assert_eq!(value, Value::None);
	}
}
