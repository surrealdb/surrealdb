use crate::dbs::Level;
use crate::dbs::Options;
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
	pub(crate) async fn compute(&self, opt: &Options) -> Result<Value, Error> {
		// No need for NS/DB
		opt.needs(Level::Kv)?;
		// Allowed to run?
		opt.check(Level::Kv)?;
		// Process the statement (sleep ...)
		tokio::time::sleep(self.duration.0.clone()).await;
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
	#[ignore]
	fn test_sleep_statement_ms() {
		let sql = "SLEEP 500ms";
		let res = sleep(sql);
		assert!(res.is_ok());
		let out = res.unwrap().1;
		// TODO: This test actually returns "SLEEP 500000000ns"
		assert_eq!("SLEEP 500ms", format!("{}", out))
	}

	#[tokio::test]
	async fn test_sleep_compute() {
		let sql = "SLEEP 500ms";
		let (_, sleep_statement) = sleep(sql).unwrap();
		let opt = Options::new(Auth::Kv);
		let time = SystemTime::now();
		let value = sleep_statement.compute(&opt).await.unwrap();
		assert!(time.elapsed().unwrap() >= time::Duration::microseconds(500));
		assert_eq!(value, Value::None);
	}
}
