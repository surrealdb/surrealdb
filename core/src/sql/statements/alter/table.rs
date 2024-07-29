use super::super::define::DefineFieldStatement;
use crate::ctx::Context;
use crate::dbs::{Force, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::paths::{IN, OUT};
use crate::sql::{
	changefeed::ChangeFeed, statements::UpdateStatement, Base, Ident, Output, Permissions, Strand,
	Value, Values,
};
use crate::sql::{Idiom, Kind, TableType};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::ops::Deref;
use std::sync::Arc;

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
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
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
			dt.comment = comment.clone();
		}
		if let Some(ref kind) = &self.kind {
			dt.kind = kind.clone();
		}

		txn.set(key, &dt).await?;
		// Add table relational fields
		if let TableType::Relation(rel) = &dt.kind {
			// Set the `in` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(opt.ns()?, opt.db()?, &self.name, "in");
				let val = rel.from.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					key,
					DefineFieldStatement {
						name: Idiom::from(IN.to_vec()),
						what: self.name.to_owned(),
						kind: Some(val),
						..Default::default()
					},
				)
				.await?;
			}
			// Set the `out` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(opt.ns()?, opt.db()?, &self.name, "out");
				let val = rel.to.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					key,
					DefineFieldStatement {
						name: Idiom::from(OUT.to_vec()),
						what: self.name.to_owned(),
						kind: Some(val),
						..Default::default()
					},
				)
				.await?;
			}
		}
		// Clear the cache
		txn.clear();
		// Record definition change
		if dt.changefeed.is_some() {
			txn.lock().await.record_table_change(opt.ns()?, opt.db()?, &self.name, &dt);
		}
		// Check if table is a view
		if let Some(view) = &dt.view {
			// Remove the table data
			let key = crate::key::table::all::new(opt.ns()?, opt.db()?, &self.name);
			txn.delp(key).await?;
			// Process each foreign table
			for v in view.what.0.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(opt.ns()?, opt.db()?, v, &self.name);
				txn.set(key, self).await?;
			}
			// Force queries to run
			let opt = &opt.new_with_force(Force::Table(Arc::new([dt.clone()])));
			// Process each foreign table
			for v in view.what.0.iter() {
				// Process the view data
				let stm = UpdateStatement {
					what: Values(vec![Value::Table(v.clone())]),
					output: Some(Output::None),
					..UpdateStatement::default()
				};
				stm.compute(stk, ctx, opt, doc).await?;
			}
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
