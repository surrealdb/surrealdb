use crate::catalog::TableKind;
use crate::expr::Kind;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::statements::DefineTableStatement;
use crate::expr::{Base, ChangeFeed, Ident, Permissions, Strand, Value};
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::ops::Deref;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AlterTableStatement {
	pub name: Ident,
	pub if_exists: bool,
	#[revision(end = 2, convert_fn = "convert_drop")]
	pub _drop: Option<bool>,
	pub schemafull: Option<bool>,
	pub permissions: Option<Permissions>,
	pub changefeed: Option<Option<ChangeFeed>>,
	pub comment: Option<Option<String>>,
	pub kind: Option<TableKind>,
}

impl AlterTableStatement {
	fn convert_drop(
		&mut self,
		_revision: u16,
		_value: Option<bool>,
	) -> Result<(), revision::Error> {
		Ok(())
	}

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
		let (ns, db) = ctx.get_ns_db_ids(opt)?;
		// Fetch the transaction
		let txn = ctx.tx();

		// Get the table definition
		let mut dt = match txn.get_tb(ns, db, &self.name).await? {
			Some(tb) => tb.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::TbNotFound {
						name: self.name.to_string(),
					}.into());
				}
			}
		};
		// Process the statement
		let key = crate::key::database::tb::new(ns, db, &self.name);
		if let Some(schemafull) = &self.schemafull {
			dt.schemafull = *schemafull;
		}
		if let Some(permissions) = &self.permissions {
			dt.permissions = permissions.clone();
		}
		if let Some(changefeed) = &self.changefeed {
			dt.changefeed = *changefeed;
		}
		if let Some(comment) = &self.comment {
			dt.comment.clone_from(comment);
		}
		if let Some(kind) = &self.kind {
			dt.kind = kind.clone();
		}

		// Add table relational fields
		if matches!(self.kind, Some(TableKind::Relation(_))) {
			DefineTableStatement::add_in_out_fields(&txn, ns, db, &mut dt).await?;
		}
		// Set the table definition
		txn.set(&key, &dt, None).await?;
		// Record definition change
		if self.changefeed.is_some() && dt.changefeed.is_some() {
			txn.lock().await.record_table_change(ns, db, &self.name, &dt);
		}
		// Clear the cache
		txn.clear();
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
				TableKind::Normal => {
					f.write_str(" NORMAL")?;
				}
				TableKind::Relation(rel) => {
					f.write_str(" RELATION")?;
					if let Some(Kind::Record(kind)) = &rel.from {
						write!(
							f,
							" IN {}",
							kind.iter().map(|t| t.0.as_str()).collect::<Vec<_>>().join(" | ")
						)?;
					}
					if let Some(Kind::Record(kind)) = &rel.to {
						write!(
							f,
							" OUT {}",
							kind.iter().map(|t| t.0.as_str()).collect::<Vec<_>>().join(" | ")
						)?;
					}
				}
				TableKind::Any => {
					f.write_str(" ANY")?;
				}
			}
		}
		if let Some(full) = self.schemafull {
			f.write_str(if full {
				" SCHEMAFULL"
			} else {
				" SCHEMALESS"
			})?;
		}
		if let Some(comment) = &self.comment {
			if let Some(comment) = comment {
				write!(f, " COMMENT {}", comment.clone())?;
			} else {
				write!(f, " DROP COMMENT")?;
			}
		}
		if let Some(changefeed) = &self.changefeed {
			if let Some(changefeed) = changefeed {
				write!(f, " CHANGEFEED {}", changefeed.clone())?;
			} else {
				write!(f, " DROP CHANGEFEED")?;
			}
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
