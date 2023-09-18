use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::base::{base, Base};
use crate::sql::comment::shouldbespace;
use crate::sql::error::expect_tag_no_case;
use crate::sql::error::IResult;
use crate::sql::ident::{ident, Ident};
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RemoveUserStatement {
	pub name: Ident,
	pub base: Base,
}

impl RemoveUserStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match self.base {
			Base::Root => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::root::us::new(&self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Delete the definition
				let key = crate::key::namespace::us::new(opt.ns(), &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Delete the definition
				let key = crate::key::database::us::new(opt.ns(), opt.db(), &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for RemoveUserStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE USER {} ON {}", self.name, self.base)
	}
}

pub fn user(i: &str) -> IResult<&str, RemoveUserStatement> {
	let (i, _) = tag_no_case("USER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = cut(ident)(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = expect_tag_no_case("ON")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, base) = cut(base)(i)?;
	Ok((
		i,
		RemoveUserStatement {
			name,
			base,
		},
	))
}
