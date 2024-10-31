use super::DefineFieldStatement;
use crate::ctx::Context;
use crate::dbs::{Force, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::kvs::Transaction;
use crate::sql::fmt::{is_pretty, pretty_indent};
use crate::sql::paths::{IN, OUT};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{
	changefeed::ChangeFeed, statements::UpdateStatement, Base, Ident, Output, Permissions, Strand,
	Value, Values, View,
};
use crate::sql::{Idiom, Kind, TableType};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::sync::Arc;
use uuid::Uuid;

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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
	#[revision(start = 5)]
	pub cache_lives_ts: Uuid,
}

impl DefineTableStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_tb(opt.ns()?, opt.db()?, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::TbAlreadyExists {
					value: self.name.to_string(),
				});
			}
		}
		// Process the statement
		let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &self.name);
		let ns = txn.get_or_add_ns(opt.ns()?, opt.strict).await?;
		let db = txn.get_or_add_db(opt.ns()?, opt.db()?, opt.strict).await?;
		let dt = DefineTableStatement {
			id: if self.id.is_none() && ns.id.is_some() && db.id.is_some() {
				Some(txn.lock().await.get_next_tb_id(ns.id.unwrap(), db.id.unwrap()).await?)
			} else {
				None
			},
			// Don't persist the `IF NOT EXISTS` clause to schema
			if_not_exists: false,
			overwrite: false,
			..self.clone()
		};
		txn.set(key, &dt, None).await?;
		// Add table relational fields
		self.add_in_out_fields(&txn, opt).await?;
		// Clear the cache
		txn.clear();
		// Record definition change
		if dt.changefeed.is_some() {
			txn.lock().await.record_table_change(opt.ns()?, opt.db()?, &self.name, &dt);
		}
		// Check if table is a view
		if let Some(view) = &self.view {
			// Remove the table data
			let key = crate::key::table::all::new(opt.ns()?, opt.db()?, &self.name);
			txn.delp(key).await?;
			// Process each foreign table
			for v in view.what.0.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(opt.ns()?, opt.db()?, v, &self.name);
				txn.set(key, self, None).await?;
			}
			// Force queries to run
			let opt = &opt.new_with_force(Force::Table(Arc::new([dt])));
			// Process each foreign table
			for v in view.what.0.iter() {
				// Process the view data
				let stm = UpdateStatement {
					what: Values(vec![Value::Table(v.clone())]),
					output: Some(Output::None),
					..UpdateStatement::default()
				};
				stm.compute(stk, ctx, opt, doc).await?;
				// Refresh the cache id
				let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, v);
				let tb = txn.get_tb(opt.ns()?, opt.db()?, v).await?;
				txn.set(
					key,
					DefineTableStatement {
						cache_tables_ts: Uuid::now_v7(),
						..tb.as_ref().clone()
					},
					None,
				)
				.await?;
			}
		}
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
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
	pub async fn add_in_out_fields(&self, txn: &Transaction, opt: &Options) -> Result<(), Error> {
		// Add table relational fields
		if let TableType::Relation(rel) = &self.kind {
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
					None,
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
					None,
				)
				.await?;
			}
			// Refresh the cache id for the fields
			{
				let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &self.name);
				let tb = txn.get_tb(opt.ns()?, opt.db()?, &self.name).await?;
				txn.set(
					key,
					DefineTableStatement {
						cache_fields_ts: Uuid::now_v7(),
						..tb.as_ref().clone()
					},
					None,
				)
				.await?;
			}
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

impl InfoStructure for DefineTableStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"drop".to_string() => self.drop.into(),
			"full".to_string() => self.full.into(),
			"kind".to_string() => self.kind.structure(),
			"view".to_string(), if let Some(v) = self.view => v.structure(),
			"changefeed".to_string(), if let Some(v) = self.changefeed => v.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
