use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{changefeed::ChangeFeed, Base, Ident, Strand, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
		ctx: &Context<'_>,
		opt: &Options,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_db(opt.ns()?, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::DbAlreadyExists {
					value: self.name.to_string(),
				});
			}
		}
		// Process the statement
		let key = crate::key::namespace::db::new(opt.ns()?, &self.name);
		let ns = txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
		txn.set(
			key,
			DefineDatabaseStatement {
				id: if self.id.is_none() && ns.id.is_some() {
					Some(txn.lock().await.get_next_db_id(ns.id.unwrap()).await?)
				} else {
					None
				},
				// Don't persist the `IF NOT EXISTS` clause to schema
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;
		// Clear the cache
		txn.clear();
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

impl InfoStructure for DefineDatabaseStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
