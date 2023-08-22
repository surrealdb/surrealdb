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
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 1)]
pub struct RemoveAnalyzerStatement {
	pub name: Ident,
}

impl RemoveAnalyzerStatement {
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Delete the definition
		let (ns, db) = run.get_ns_db_ids(opt.ns(), opt.db()).await?;
		let key = crate::key::database::az::new(ns, db, &self.name);
		run.del(key).await?;
		// TODO Check that the analyzer is not used in any schema
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveAnalyzerStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE ANALYZER {}", self.name)
	}
}

pub fn analyzer(i: &str) -> IResult<&str, RemoveAnalyzerStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ANALYZER")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident(i)?;
	Ok((
		i,
		RemoveAnalyzerStatement {
			name,
		},
	))
}
