use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{changefeed::ChangeFeed, Base, Ident, Strand, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineDatabaseStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub comment: Option<Strand>,
	pub changefeed: Option<ChangeFeed>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
}

impl DefineDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
		// Get the NS
		let ns = opt.ns()?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_db(ns, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::DbAlreadyExists {
					name: self.name.to_string(),
				});
			}
		}
		// Process the statement
		let key = crate::key::namespace::db::new(ns, &self.name);
		let nsv = txn.get_or_add_ns(ns, opt.strict).await?;
		txn.set(
			key,
			revision::to_vec(&DefineDatabaseStatement {
				id: if let Some(id) = nsv.id {
					if self.id.is_none() {
						Some(txn.lock().await.get_next_db_id(id).await?)
					} else {
						None
					}
				} else {
					None
				},
				// Don't persist the `IF NOT EXISTS` clause to schema
				if_not_exists: false,
				overwrite: false,
				..self.clone()
			})?,
			None,
		)
		.await?;
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear();
		}
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
		if self.overwrite {
			write!(f, " OVERWRITE")?
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
