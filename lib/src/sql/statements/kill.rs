use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::param::param;
use crate::sql::uuid::uuid;
use crate::sql::value::Value;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::into;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Value,
}

impl KillStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Resolve live query id
		let live_query_id = match &self.id {
			Value::Uuid(id) => id.clone(),
			Value::Param(param) => match param.compute(ctx, opt, txn, None).await? {
				Value::Uuid(id) => id,
				_ => {
					return Err(Error::KillStatement {
						value: self.id.to_string(),
					})
				}
			},
			_ => {
				return Err(Error::KillStatement {
					value: self.id.to_string(),
				})
			}
		};
		// Claim transaction
		let mut run = txn.lock().await;
		// Fetch the live query key
		let key = crate::key::node::lq::new(opt.id()?, live_query_id.0, opt.ns(), opt.db());
		// Fetch the live query key if it exists
		match run.get(key).await? {
			Some(val) => match std::str::from_utf8(&val) {
				Ok(tb) => {
					// Delete the node live query
					let key =
						crate::key::node::lq::new(opt.id()?, live_query_id.0, opt.ns(), opt.db());
					run.del(key).await?;
					// Delete the table live query
					let key = crate::key::table::lq::new(opt.ns(), opt.db(), tb, live_query_id.0);
					run.del(key).await?;
				}
				_ => {
					return Err(Error::KillStatement {
						value: self.id.to_string(),
					})
				}
			},
			None => {
				return Err(Error::KillStatement {
					value: self.id.to_string(),
				})
			}
		}
		// Return the query id
		Ok(Value::None)
	}
}

impl fmt::Display for KillStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "KILL {}", self.id)
	}
}

pub fn kill(i: &str) -> IResult<&str, KillStatement> {
	let (i, _) = tag_no_case("KILL")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, v) = alt((into(uuid), into(param)))(i)?;
	Ok((
		i,
		KillStatement {
			id: v,
		},
	))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::sql::{Ident, Param, Uuid};

	#[test]
	fn kill_uuid() {
		let uuid_str = "c005b8da-63a4-48bc-a371-07e95b39d58e";
		let uuid_str_wrapped = format!("'{}'", uuid_str);
		let sql = format!("kill {}", uuid_str_wrapped);
		let res = kill(&sql);
		assert!(res.is_ok(), "{:?}", res);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			KillStatement {
				id: Value::Uuid(Uuid::from(uuid::Uuid::parse_str(uuid_str).unwrap()))
			}
		);
		assert_eq!("KILL 'c005b8da-63a4-48bc-a371-07e95b39d58e'", format!("{}", out));
	}

	#[test]
	fn kill_param() {
		let sql = "kill $id";
		let res = kill(sql);
		let out = res.unwrap().1;
		assert_eq!(
			out,
			KillStatement {
				id: Value::Param(Param(Ident("id".to_string()))),
			}
		);
		assert_eq!("KILL $id", format!("{}", out));
	}
}
