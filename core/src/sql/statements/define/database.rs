use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{changefeed::ChangeFeed, Base, Ident, Strand, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
#[non_exhaustive]
pub struct DefineDatabaseStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
	pub changefeed: Option<ChangeFeed>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if database already exists
		if self.if_not_exists && run.get_db(opt.ns(), &self.name).await.is_ok() {
			return Err(Error::DbAlreadyExists {
				value: self.name.to_string(),
			});
		}
		// Process the statement
		let key = crate::key::namespace::db::new(opt.ns(), &self.name);
		let ns = run.add_ns(opt.ns(), opt.strict).await?;
		// Set the id
		if self.id.is_none() && ns.id.is_some() {
			// Set the id
			let db = DefineDatabaseStatement {
				id: Some(run.get_next_db_id(ns.id.unwrap()).await?),
				if_not_exists: false,
				..self.clone()
			};

			run.set(key, db).await?;
		} else {
			run.set(
				key,
				DefineDatabaseStatement {
					if_not_exists: false,
					..self.clone()
				},
			)
			.await?;
		}
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineDatabaseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE DATABASE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {}", self.name)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		Ok(())
	}
}
