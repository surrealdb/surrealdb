use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::base::Base;
use crate::sql::comment::{mightbespace, shouldbespace};
use crate::sql::error::IResult;
use crate::sql::ident;
use crate::sql::ident::Ident;
use crate::sql::value::Value;
use derive::Store;
use nom::bytes::complete::tag;
use nom::bytes::complete::tag_no_case;
use nom::character::complete::char;
use nom::combinator::opt;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RemoveFunctionStatement {
	pub name: Ident,
}

impl RemoveFunctionStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Delete the definition
		let key = crate::key::database::fc::new(opt.ns(), opt.db(), &self.name);
		run.del(key).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveFunctionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "REMOVE FUNCTION fn::{}", self.name)
	}
}

pub fn function(i: &str) -> IResult<&str, RemoveFunctionStatement> {
	let (i, _) = tag_no_case("FUNCTION")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag("fn::")(i)?;
	let (i, name) = ident::plain(i)?;
	let (i, _) = opt(|i| {
		let (i, _) = mightbespace(i)?;
		let (i, _) = char('(')(i)?;
		let (i, _) = mightbespace(i)?;
		let (i, _) = char(')')(i)?;
		Ok((i, ()))
	})(i)?;
	Ok((
		i,
		RemoveFunctionStatement {
			name,
		},
	))
}
