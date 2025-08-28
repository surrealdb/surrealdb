use std::fmt::{self, Display, Write};
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::{
	DatabaseId, FieldDefinition, NamespaceId, Permissions, TableDefinition, TableType,
};
use crate::ctx::Context;
use crate::dbs::{Force, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::fmt::{is_pretty, pretty_indent};
use crate::expr::paths::{IN, OUT};
use crate::expr::statements::UpdateStatement;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Expr, Ident, Idiom, Kind, Output, View};
use crate::iam::{Action, ResourceKind};
use crate::kvs::Transaction;
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
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
}

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
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let table_id = if let Some(tb) = txn.get_tb(ns, db, &self.name).await? {
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

			tb.table_id
		} else {
			txn.lock().await.get_next_tb_id(ns, db).await?
		};

		// Process the statement
		let cache_ts = Uuid::now_v7();
		let mut tb_def = TableDefinition {
			namespace_id: ns,
			database_id: db,
			table_id,
			name: self.name.to_raw_string(),
			drop: self.drop,
			schemafull: self.full,
			table_type: self.table_type.clone(),
			view: self.view.clone().map(|v| v.to_definition()),
			permissions: self.permissions.clone(),
			comment: self.comment.clone().map(|c| c.to_raw_string()),
			changefeed: self.changefeed,

			cache_fields_ts: cache_ts,
			cache_events_ts: cache_ts,
			cache_indexes_ts: cache_ts,
			cache_tables_ts: cache_ts,
		};

		// Add table relational fields
		Self::add_in_out_fields(&txn, ns, db, &mut tb_def).await?;

		// Record definition change
		if self.changefeed.is_some() {
			txn.lock().await.record_table_change(ns, db, &self.name, &tb_def);
		}

		// Update the catalog
		txn.put_tb(ns_name, db_name, &tb_def).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.name);
		}
		// Clear the cache
		txn.clear_cache();
		// Check if table is a view
		if let Some(view) = &self.view {
			// Force queries to run
			let opt = &opt.new_with_force(Force::Table(Arc::new([tb_def.clone()])));
			// Remove the table data
			let key = crate::key::table::all::new(ns, db, &self.name);
			txn.delp(&key).await?;
			// Process each foreign table
			for ft in view.what.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(ns, db, ft, &self.name);
				txn.set(&key, &tb_def, None).await?;
				// Refresh the table cache
				let Some(foreign_tb) = txn.get_tb(ns, db, ft).await? else {
					bail!(Error::TbNotFound {
						name: ft.to_string(),
					});
				};

				txn.put_tb(
					ns_name,
					db_name,
					&TableDefinition {
						cache_tables_ts: Uuid::now_v7(),
						..foreign_tb.as_ref().clone()
					},
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
	///
	/// Returns the cache key ts.
	pub async fn add_in_out_fields(
		txn: &Transaction,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &mut TableDefinition,
	) -> Result<()> {
		// Add table relational fields
		if let TableType::Relation(rel) = &tb.table_type {
			// Set the `in` field as a DEFINE FIELD definition
			{
				let key = crate::key::table::fd::new(ns, db, &tb.name, "in");
				let val = rel.from.clone().unwrap_or(Kind::Record(vec![]));
				txn.set(
					&key,
					&FieldDefinition {
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
					&FieldDefinition {
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
		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {comment}")?
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
