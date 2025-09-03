use std::fmt::{self, Display};

use anyhow::{Result, bail};
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::{EventDefinition, TableDefinition};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Expr, Ident};
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::Fmt;
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineEventStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub target_table: Ident,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Option<Strand>,
}

impl DefineEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_tb_event(ns, db, &self.target_table, &self.name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::EvAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}

		// Ensure the table exists
		let tb = {
			let (ns, db) = opt.ns_db()?;
			txn.get_or_add_tb(ns, db, &self.target_table, opt.strict).await?
		};

		// Process the statement
		let key = crate::key::table::ev::new(ns, db, &self.target_table, &self.name);
		txn.set(
			&key,
			&EventDefinition {
				name: self.name.to_raw_string(),
				target_table: self.target_table.to_raw_string(),
				when: self.when.clone(),
				then: self.then.clone(),
				comment: self.comment.clone().map(|x| x.to_raw_string()),
			},
			None,
		)
		.await?;

		// Refresh the table cache
		let tb = TableDefinition {
			cache_events_ts: Uuid::now_v7(),
			..tb.as_ref().clone()
		};

		txn.put_tb(ns_name, db_name, &tb).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.target_table);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE EVENT",)?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(
			f,
			" {} ON {} WHEN {} THEN {}",
			self.name,
			self.target_table,
			self.when,
			Fmt::comma_separated(self.then.iter())
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}
