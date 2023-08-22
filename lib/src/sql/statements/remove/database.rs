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
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RemoveDatabaseStatement {
	pub name: Ident,
}

impl RemoveDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Get ids
		let (ns, db) = run.get_ns_db_ids(opt.ns(), &self.name).await?;
		// Delete the alias
		let key = crate::key::namespace::db::new(ns, &self.name);
		run.del(key).await?;
		// Delete the definition
		let key = crate::key::database::db::new(ns, db);
		run.del(key).await?;
		// Delete the resource data
		let key = crate::key::database::all::new(ns, db);
		run.delp(key, u32::MAX).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveDatabaseStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE DATABASE {}", self.name)
	}
}

pub fn database(i: &str) -> IResult<&str, RemoveDatabaseStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = alt((tag_no_case("DB"), tag_no_case("DATABASE")))(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident(i)?;
	Ok((
		i,
		RemoveDatabaseStatement {
			name,
		},
	))
}
