use std::fmt::{self, Display};
use std::ops::Deref;

use anyhow::Result;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::catalog::providers::TableProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Value};
use crate::fmt::{EscapeKwFreeIdent, EscapeKwIdent, QuoteStr};
use crate::iam::{Action, ResourceKind};

/// Represents an `ALTER INDEX` statement.
///
/// Currently supports decommissioning indexes as a safe preparation step before removal.
/// Decommissioning an index:
/// - Cancels any ongoing concurrent index builds
/// - Prevents the query planner from using the index
/// - Stops updating the index on record changes
///
/// This allows administrators to verify query performance before permanently removing an index.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterIndexStatement {
	pub name: String,
	pub table: String,
	pub if_exists: bool,
	/// If true, marks the index as decommissioned
	pub prepare_remove: bool,
	pub comment: AlterKind<String>,
}

impl AlterIndexStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the index definition
		let mut ix = match txn.get_tb_index(ns, db, &self.table, &self.name).await? {
			Some(tb) => tb.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::IxNotFound {
						name: self.name.clone(),
					}
					.into());
				}
			}
		};

		match self.comment {
			AlterKind::Set(ref k) => ix.comment = Some(k.clone()),
			AlterKind::Drop => ix.comment = None,
			AlterKind::None => {}
		}

		if self.prepare_remove && !ix.prepare_remove {
			ix.prepare_remove = true;
		}

		// Set the index definition
		txn.put_tb_index(ns, db, &self.table, &ix).await?;

		// Refresh the table cache for indexes
		let tb = txn.expect_tb(ns, db, &self.table).await?;
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.table);
		}
		// Clear the cache
		txn.clear_cache();

		// Ok all good
		Ok(Value::None)
	}
}

impl Display for AlterIndexStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", EscapeKwIdent(&self.name, &["IF"]), EscapeKwFreeIdent(&self.table))?;

		if self.prepare_remove {
			write!(f, " PREPARE REMOVE")?;
		}
		match self.comment {
			AlterKind::Set(ref x) => write!(f, " COMMENT {}", QuoteStr(x))?,
			AlterKind::Drop => write!(f, " DROP COMMENT")?,
			AlterKind::None => {}
		}
		Ok(())
	}
}
