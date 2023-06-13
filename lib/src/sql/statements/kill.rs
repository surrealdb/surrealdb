use crate::ctx::Context;
use crate::dbs::Level;
use crate::dbs::Options;
use crate::err::Error;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::uuid::{uuid, Uuid};
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Store, Hash)]
pub struct KillStatement {
	pub id: Uuid,
}

impl KillStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		// Allowed to run?
		opt.realtime()?;
		// Selected DB?
		opt.needs(Level::Db)?;
		// Allowed to run?
		opt.check(Level::No)?;
		// Clone transaction
		let txn = ctx.clone_transaction()?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Create the live query key
		let key = crate::key::lq::new(opt.ns(), opt.db(), &self.id);
		// Fetch the live query key if it exists
		match run.get(key).await? {
			Some(val) => match std::str::from_utf8(&val) {
				Ok(tb) => {
					// Delete the live query
					let key = crate::key::lq::new(opt.ns(), opt.db(), &self.id);
					run.del(key).await?;
					// Delete the table live query
					let key = crate::key::lv::new(opt.ns(), opt.db(), tb, &self.id);
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
	let (i, v) = uuid(i)?;
	Ok((
		i,
		KillStatement {
			id: v,
		},
	))
}
