use std::fmt::{self, Display};

use anyhow::{Result, bail};

use super::DefineKind;
use crate::catalog::DatabaseDefinition;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::Base;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::statements::info::InfoStructure;
use crate::fmt::{EscapeIdent, QuoteStr};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: String,
	pub comment: Option<String>,
	pub changefeed: Option<ChangeFeed>,
}

impl DefineDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;

		// Get the NS
		let ns = opt.ns()?;

		// Fetch the transaction
		let txn = ctx.tx();
		let nsv = txn.get_or_add_ns(ns, opt.strict).await?;

		// Check if the definition exists
		let database_id = if let Some(db) = txn.get_db_by_name(ns, &self.name).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::DbAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}

			db.database_id
		} else {
			txn.lock().await.get_next_db_id(nsv.namespace_id).await?
		};

		let name: String = self.name.clone();

		// Set the database definition, keyed by namespace name and database name.
		let db_def = DatabaseDefinition {
			namespace_id: nsv.namespace_id,
			database_id,
			name: name.clone(),
			comment: self.comment.clone(),
			changefeed: self.changefeed,
		};
		txn.put_db(&nsv.name, db_def).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear();
		}

		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineDatabaseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE DATABASE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", EscapeIdent(&self.name))?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", QuoteStr(v))?
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
			"name".to_string() => self.name.into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
