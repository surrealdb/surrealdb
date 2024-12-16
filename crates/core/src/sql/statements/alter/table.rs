use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::statements::DefineTableStatement;
use crate::sql::{changefeed::ChangeFeed, Base, Ident, Permissions, Strand, Value};
use crate::sql::{Kind, TableType};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::ops::Deref;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct AlterTableStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub drop: Option<bool>,
	pub full: Option<bool>,
	pub permissions: Option<Permissions>,
	pub changefeed: Option<Option<ChangeFeed>>,
	pub comment: Option<Option<Strand>>,
	pub kind: Option<TableType>,
}

impl AlterTableStatement {
	pub(crate) async fn compute(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the table definition
		let mut dt = match txn.get_tb(opt.ns()?, opt.db()?, &self.name).await {
			Ok(tb) => tb.deref().clone(),
			Err(Error::TbNotFound {
				..
			}) if self.if_exists => return Ok(Value::None),
			Err(v) => return Err(v),
		};
		// Process the statement
		let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &self.name);
		if let Some(ref drop) = &self.drop {
			dt.drop = *drop;
		}
		if let Some(ref full) = &self.full {
			dt.full = *full;
		}
		if let Some(ref permissions) = &self.permissions {
			dt.permissions = permissions.clone();
		}
		if let Some(ref changefeed) = &self.changefeed {
			dt.changefeed = *changefeed;
		}
		if let Some(ref comment) = &self.comment {
			dt.comment.clone_from(comment);
		}
		if let Some(ref kind) = &self.kind {
			dt.kind = kind.clone();
		}

		// Add table relational fields
		if matches!(self.kind, Some(TableType::Relation(_))) {
			DefineTableStatement::add_in_out_fields(&txn, &mut dt, opt).await?;
		}
		// Set the table definition
		txn.set(key, &dt, None).await?;
		// Record definition change
		if self.changefeed.is_some() && dt.changefeed.is_some() {
			txn.lock().await.record_table_change(opt.ns()?, opt.db()?, &self.name, &dt);
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
				TableType::Normal => {
					f.write_str(" NORMAL")?;
				}
				TableType::Relation(rel) => {
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
				TableType::Any => {
					f.write_str(" ANY")?;
				}
			}
		}
		if let Some(drop) = self.drop {
			write!(f, " DROP {drop}")?;
		}
		if let Some(full) = self.full {
			f.write_str(if full {
				" SCHEMAFULL"
			} else {
				" SCHEMALESS"
			})?;
		}
		if let Some(comment) = &self.comment {
			write!(f, " COMMENT {}", comment.clone().unwrap_or("NONE".into()))?
		}
		if let Some(changefeed) = &self.changefeed {
			write!(f, " CHANGEFEED {}", changefeed.map_or("NONE".into(), |v| v.to_string()))?
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
