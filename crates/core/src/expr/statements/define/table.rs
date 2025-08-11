use super::{DefineFieldStatement, DefineKind};
use crate::ctx::Context;
use crate::dbs::{Force, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::paths::{IN, OUT};
use crate::expr::statements::UpdateStatement;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Expr, Ident, Idiom, Kind, Output, Permissions, TableType, View};
use crate::iam::{Action, ResourceKind};
use crate::kvs::{Transaction, impl_kv_value_revisioned};
use crate::val::{Strand, Value};
use anyhow::{Result, bail};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Write};
use std::sync::Arc;
use uuid::Uuid;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DefineTableStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Ident,
	pub drop: bool,
	pub full: bool,
	pub view: Option<View>,
	pub permissions: Permissions,
	pub changefeed: Option<ChangeFeed>,
	pub comment: Option<Strand>,
	pub table_type: TableType,
	/// The last time that a DEFINE FIELD was added to this table
	pub cache_fields_ts: Uuid,
	/// The last time that a DEFINE EVENT was added to this table
	pub cache_events_ts: Uuid,
	/// The last time that a DEFINE TABLE was added to this table
	pub cache_tables_ts: Uuid,
	/// The last time that a DEFINE INDEX was added to this table
	pub cache_indexes_ts: Uuid,
}

impl_kv_value_revisioned!(DefineTableStatement);

impl DefineTableStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = opt.ns_db()?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_tb(ns, db, &self.name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::TbAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}
		// Process the statement
		let key = crate::key::database::tb::new(ns, db, &self.name);
		let nsv = txn.get_or_add_ns(ns, opt.strict).await?;
		let dbv = txn.get_or_add_db(ns, db, opt.strict).await?;
		let mut dt = DefineTableStatement {
			id: match (self.id, nsv.id, dbv.id) {
				(Some(id), _, _) => Some(id),
				(None, Some(nsv_id), Some(dbv_id)) => {
					Some(txn.lock().await.get_next_tb_id(nsv_id, dbv_id).await?)
				}
				_ => None,
			},
			// Don't persist the `IF NOT EXISTS` clause to the schema
			kind: DefineKind::Default,
			..self.clone()
		};
		// Make sure we are refreshing the caches
		dt.cache_fields_ts = Uuid::now_v7();
		dt.cache_events_ts = Uuid::now_v7();
		dt.cache_indexes_ts = Uuid::now_v7();
		dt.cache_tables_ts = Uuid::now_v7();
		// Add table relational fields
		Self::add_in_out_fields(&txn, ns, db, &mut dt).await?;
		// Set the table definition
		txn.set(&key, &dt, None).await?;
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.name);
		}
		// Clear the cache
		txn.clear_cache();
		// Record definition change
		if dt.changefeed.is_some() {
			txn.lock().await.record_table_change(ns, db, &self.name, &dt);
		}
		// Check if table is a view
		if let Some(view) = &self.view {
			// Force queries to run
			let opt = &opt.new_with_force(Force::Table(Arc::new([dt])));
			// Remove the table data
			let key = crate::key::table::all::new(ns, db, &self.name);
			txn.delp(&key).await?;
			// Process each foreign table
			for ft in view.what.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(ns, db, ft, &self.name);
				txn.set(&key, self, None).await?;
				// Refresh the table cache
				let key = crate::key::database::tb::new(ns, db, ft);
				let tb = txn.get_tb(ns, db, ft).await?;
				txn.set(
					&key,
					&DefineTableStatement {
						cache_tables_ts: Uuid::now_v7(),
						..tb.as_ref().clone()
					},
					None,
				)
				.await?;
				// Clear the cache
				if let Some(cache) = ctx.get_cache() {
					cache.clear_tb(ns, db, ft);
				}
				// Clear the cache
				txn.clear_cache();
				// Process the view data
				let stm = UpdateStatement {
					what: vec![Expr::Table(ft.clone())],
					output: Some(Output::None),
					..UpdateStatement::default()
				};
				stm.compute(stk, ctx, opt, doc).await?;
			}
		}
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.name);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl DefineTableStatement {
	/// Checks if this is a TYPE RELATION table
	pub fn is_relation(&self) -> bool {
		matches!(self.table_type, TableType::Relation(_))
	}
	/// Checks if this table allows graph edges / relations
	pub fn allows_relation(&self) -> bool {
		matches!(self.table_type, TableType::Relation(_) | TableType::Any)
	}
	/// Checks if this table allows normal records / documents
	pub fn allows_normal(&self) -> bool {
		matches!(self.table_type, TableType::Normal | TableType::Any)
	}
	/// Used to add relational fields to existing table records
	pub async fn add_in_out_fields(
		txn: &Transaction,
		ns: &str,
		db: &str,
		tb: &mut DefineTableStatement,
	) -> Result<()> {
		// Add table relational fields
		if let TableType::Relation(rel) = &tb.table_type {
			// Set the `in` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(ns, db, &tb.name, "in");
				let val = rel.from.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					&key,
					&DefineFieldStatement {
						name: Idiom::from(IN.to_vec()),
						what: tb.name.clone(),
						field_kind: Some(val),
						..Default::default()
					},
					None,
				)
				.await?;
			}
			// Set the `out` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(ns, db, &tb.name, "out");
				let val = rel.to.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					&key,
					&DefineFieldStatement {
						name: Idiom::from(OUT.to_vec()),
						what: tb.name.clone(),
						field_kind: Some(val),
						..Default::default()
					},
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
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;
		write!(f, " TYPE")?;
		match &self.table_type {
			TableType::Normal => {
				f.write_str(" NORMAL")?;
			}
			TableType::Relation(rel) => {
				f.write_str(" RELATION")?;
				if let Some(Kind::Record(kind)) = &rel.from {
					write!(f, " IN ",)?;
					for (idx, k) in kind.iter().enumerate() {
						if idx != 0 {
							write!(f, " | ")?;
						}
						k.fmt(f)?;
					}
				}
				if let Some(Kind::Record(kind)) = &rel.to {
					write!(f, " OUT ",)?;
					for (idx, k) in kind.iter().enumerate() {
						if idx != 0 {
							write!(f, " | ")?;
						}
						k.fmt(f)?;
					}
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
			"kind".to_string() => self.table_type.structure(),
			"view".to_string(), if let Some(v) = self.view => v.structure(),
			"changefeed".to_string(), if let Some(v) = self.changefeed => v.structure(),
			"permissions".to_string() => self.permissions.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
