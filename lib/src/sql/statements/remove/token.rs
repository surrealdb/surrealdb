use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::base::{base_or_scope, Base};
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
pub struct RemoveTokenStatement {
	pub name: Ident,
	pub base: Base,
}

impl RemoveTokenStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match &self.base {
			Base::Ns => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Delete the definition
				let ns = run.get_ns(opt.ns()).await?;
				let ns = ns.id.unwrap();
				let key = crate::key::namespace::tk::new(ns, &self.name);
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
				let ns = run.get_ns(opt.ns()).await?;
				let ns = ns.id.unwrap();
				let db = run.get_db(opt.ns(), opt.db()).await?;
				let db = db.id.unwrap();
				let key = crate::key::database::tk::new(ns, db, &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Sc(sc) => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Delete the definition
				let key = crate::key::scope::tk::new(opt.ns(), opt.db(), sc, &self.name);
				run.del(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for RemoveTokenStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TOKEN {} ON {}", self.name, self.base)
	}
}

pub fn token(i: &str) -> IResult<&str, RemoveTokenStatement> {
	let (i, _) = tag_no_case("REMOVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("TOKEN")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, name) = ident(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("ON")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, base) = base_or_scope(i)?;
	Ok((
		i,
		RemoveTokenStatement {
			name,
			base,
		},
	))
}
