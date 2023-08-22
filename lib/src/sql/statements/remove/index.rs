use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::base::Base;
use crate::sql::comment::shouldbespace;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::opt;
use nom::sequence::tuple;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RemoveIndexStatement {
	pub name: Ident,
	pub what: Ident,
}

impl RemoveIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Get ids
		let chk = run.check_ns_db_tb(opt.ns(), opt.db(), &self.what, true).await;
		let (ns, db, tb) = match chk {
			Err(Error::DbNotFound {
				..
			})
			| Err(Error::TbNotFound {
				..
			})
			| Err(Error::NsNotFound {
				..
			}) => return Ok(Value::None),
			Err(e) => return Err(e),
			Ok(Some((ns, db, tb))) => (ns, db, tb),
			Ok(None) => return Ok(Value::None),
		};
		// Delete the definition
		let key = crate::key::table::ix::new(ns, db, tb, &self.name);
		run.del(key).await?;
		// Remove the index data
		let key = crate::key::index::all::new(ns, db, tb, &self.name);
		run.delp(key, u32::MAX).await?;
		// Clear the cache
		let key = crate::key::table::ix::prefix(ns, db, tb);
		run.clr(key).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveIndexStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE INDEX {} ON {}", self.name, self.what)
	}
}

pub fn index(i: &str) -> IResult<&str, RemoveIndexStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("INDEX")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ON")(i)?;
	let (i, _) = opt(tuple((shouldbespace, tag_no_case("TABLE"))))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, what) = ident(i)?;
	Ok((
		i,
		RemoveIndexStatement {
			name,
			what,
		},
	))
}
