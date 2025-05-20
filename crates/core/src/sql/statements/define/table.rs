use super::DefineFieldStatement;
use crate::ctx::Context;
use crate::dbs::{Force, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::paths::{IN, OUT};

use crate::iam::{Action, ResourceKind};
use crate::kvs::Transaction;
use crate::sql::{
	Base, Ident, Output, Permissions, SqlValue, Strand, SqlValues, View, changefeed::ChangeFeed,
	statements::UpdateStatement,
};
use crate::sql::{Idiom, Kind, TableType};
use anyhow::{Result, bail};

use reblessive::tree::Stk;
use revision::Error as RevisionError;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::sync::Arc;
use uuid::Uuid;

#[revisioned(revision = 6)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineTableStatement {
	pub id: Option<u32>,
	pub name: Ident,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub kind: TableType,
	/// Should we overwrite the field definition if it already exists
	#[revision(start = 4)]
	pub overwrite: bool,
	/// The last time that a DEFINE FIELD was added to this table
	#[revision(start = 5)]
	pub cache_fields_ts: Uuid,
	/// The last time that a DEFINE EVENT was added to this table
	#[revision(start = 5)]
	pub cache_events_ts: Uuid,
	/// The last time that a DEFINE TABLE was added to this table
	#[revision(start = 5)]
	pub cache_tables_ts: Uuid,
	/// The last time that a DEFINE INDEX was added to this table
	#[revision(start = 5)]
	pub cache_indexes_ts: Uuid,
	/// The last time that a LIVE query was added to this table
	#[revision(start = 5, end = 6, convert_fn = "convert_cache_ts")]
	pub cache_lives_ts: Uuid,
}

impl DefineTableStatement {
	fn convert_cache_ts(&self, _revision: u16, _value: Uuid) -> Result<(), RevisionError> {
		Ok(())
	}
}

impl DefineTableStatement {
	/// Checks if this is a TYPE RELATION table
	pub fn is_relation(&self) -> bool {
		matches!(self.kind, TableType::Relation(_))
	}
	/// Checks if this table allows graph edges / relations
	pub fn allows_relation(&self) -> bool {
		matches!(self.kind, TableType::Relation(_) | TableType::Any)
	}
	/// Checks if this table allows normal records / documents
	pub fn allows_normal(&self) -> bool {
		matches!(self.kind, TableType::Normal | TableType::Any)
	}
	/// Used to add relational fields to existing table records
	pub async fn add_in_out_fields(
		txn: &Transaction,
		ns: &str,
		db: &str,
		tb: &mut DefineTableStatement,
	) -> Result<()> {
		// Add table relational fields
		if let TableType::Relation(rel) = &tb.kind {
			// Set the `in` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(ns, db, &tb.name, "in");
				let val = rel.from.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					key,
					revision::to_vec(&DefineFieldStatement {
						name: Idiom::from(IN.to_vec()),
						what: tb.name.clone(),
						kind: Some(val),
						..Default::default()
					})?,
					None,
				)
				.await?;
			}
			// Set the `out` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(ns, db, &tb.name, "out");
				let val = rel.to.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					key,
					revision::to_vec(&DefineFieldStatement {
						name: Idiom::from(OUT.to_vec()),
						what: tb.name.clone(),
						kind: Some(val),
						..Default::default()
					})?,
					None,
				)
				.await?;
			}
			// Refresh the table cache for the fields
			tb.cache_fields_ts = Uuid::now_v7();
		}
		Ok(())
	}
}

impl Display for DefineTableStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE TABLE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {}", self.name)?;
		write!(f, " TYPE")?;
		match &self.kind {
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
				if rel.enforced {
					write!(f, " ENFORCED")?;
				}
			}
			TableType::Any => {
				f.write_str(" ANY")?;
			}
		}
		if self.drop {
			f.write_str(" DROP")?;
		}
		f.write_str(if self.full {
			" SCHEMAFULL"
		} else {
			" SCHEMALESS"
		})?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if let Some(ref v) = self.view {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		let _indent = if is_pretty() {
			Some(pretty_indent())
		} else {
			f.write_char(' ')?;
			None
		};
		write!(f, "{}", self.permissions)?;
		Ok(())
	}
}

impl From<DefineTableStatement> for crate::expr::statements::DefineTableStatement {
	fn from(v: DefineTableStatement) -> Self {
		crate::expr::statements::DefineTableStatement {
			id: v.id,
			name: v.name.into(),
			drop: v.drop,
			full: v.full,
			view: v.view.map(Into::into),
			permissions: v.permissions.into(),
			changefeed: v.changefeed.map(Into::into),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			kind: v.kind.into(),
			overwrite: v.overwrite,
			cache_fields_ts: v.cache_fields_ts,
			cache_events_ts: v.cache_events_ts,
			cache_tables_ts: v.cache_tables_ts,
			cache_indexes_ts: v.cache_indexes_ts,
		}
	}
}

impl From<crate::expr::statements::DefineTableStatement> for DefineTableStatement {
	fn from(v: crate::expr::statements::DefineTableStatement) -> Self {
		DefineTableStatement {
			id: v.id,
			name: v.name.into(),
			drop: v.drop,
			full: v.full,
			view: v.view.map(Into::into),
			permissions: v.permissions.into(),
			changefeed: v.changefeed.map(Into::into),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			kind: v.kind.into(),
			overwrite: v.overwrite,
			cache_fields_ts: v.cache_fields_ts,
			cache_events_ts: v.cache_events_ts,
			cache_tables_ts: v.cache_tables_ts,
			cache_indexes_ts: v.cache_indexes_ts,
		}
	}
}
