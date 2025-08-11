use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::statements::DefineTableStatement;
use crate::expr::{Base, ChangeFeed, Ident, Kind, Permissions, TableType};
use crate::iam::{Action, ResourceKind};
use crate::val::{Strand, Value};
use anyhow::Result;

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::ops::Deref;

use super::AlterKind;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AlterTableStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub schemafull: AlterKind<()>,
	pub permissions: Option<Permissions>,
	pub changefeed: AlterKind<ChangeFeed>,
	pub comment: AlterKind<Strand>,
	pub kind: Option<TableType>,
}

impl AlterTableStatement {
	pub(crate) async fn compute(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = opt.ns_db()?;
		// Fetch the transaction
		let txn = ctx.tx();

		// Get the table definition
		let mut dt = match txn.get_tb(ns, db, &self.name).await {
			Ok(tb) => tb.deref().clone(),
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::TbNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Process the statement
		let key = crate::key::database::tb::new(ns, db, &self.name);
		match self.schemafull {
			AlterKind::Set(_) => dt.full = true,
			AlterKind::Drop => dt.full = false,
			AlterKind::None => {}
		}

		if let Some(permissions) = &self.permissions {
			dt.permissions = permissions.clone();
		}

		let mut changefeed_replaced = false;
		match self.changefeed {
			AlterKind::Set(x) => {
				changefeed_replaced = dt.changefeed.is_some();
				dt.changefeed = Some(x)
			}
			AlterKind::Drop => dt.changefeed = None,
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref x) => dt.comment = Some(x.clone()),

			AlterKind::Drop => dt.comment = None,
			AlterKind::None => {}
		}

		if let Some(kind) = &self.kind {
			dt.table_type = kind.clone();
		}

		// Add table relational fields
		if matches!(self.kind, Some(TableType::Relation(_))) {
			DefineTableStatement::add_in_out_fields(&txn, ns, db, &mut dt).await?;
		}
		// Set the table definition
		txn.set(&key, &dt, None).await?;
		// Record definition change
		if changefeed_replaced {
			txn.lock().await.record_table_change(ns, db, &self.name, &dt);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for AlterTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "ALTER TABLE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		if let Some(kind) = &self.kind {
			write!(f, " TYPE")?;
			match &kind {
				TableType::Normal => {
					f.write_str(" NORMAL")?;
				}
				TableType::Relation(rel) => {
					f.write_str(" RELATION")?;
					if let Some(Kind::Record(kind)) = &rel.from {
						write!(f, " IN ",)?;
						for (idx, k) in kind.iter().enumerate() {
							if idx != 0 {
								" | ".fmt(f)?
							}
							k.fmt(f)?
						}
					}
					if let Some(Kind::Record(kind)) = &rel.to {
						write!(f, " OUT ",)?;
						for (idx, k) in kind.iter().enumerate() {
							if idx != 0 {
								" | ".fmt(f)?
							}
							k.fmt(f)?
						}
					}
				}
				TableType::Any => {
					f.write_str(" ANY")?;
				}
			}
		}

		match self.schemafull {
			AlterKind::Set(_) => writeln!(f, " SCHEMAFULL")?,
			AlterKind::Drop => writeln!(f, " SCHEMALESS")?,
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref x) => writeln!(f, " COMMENT {x}")?,
			AlterKind::Drop => writeln!(f, " DROP COMMENT")?,
			AlterKind::None => {}
		}

		match self.changefeed {
			AlterKind::Set(ref x) => writeln!(f, " CHANGEFEED {x}")?,
			AlterKind::Drop => writeln!(f, " DROP CHANGEFEED")?,
			AlterKind::None => {}
		}

		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		if let Some(permissions) = &self.permissions {
			write!(f, "{permissions}")?;
		}
		Ok(())
	}
}
