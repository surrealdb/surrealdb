use std::ops::Deref;

use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::providers::TableProvider;
use crate::catalog::{Permissions, TableType};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::statements::DefineTableStatement;
use crate::expr::{Base, ChangeFeed};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
/// Executes `ALTER TABLE` operations against an existing table.
///
/// Supported operations include:
/// - toggle `SCHEMAFULL`/`SCHEMALESS`
/// - update `PERMISSIONS`
/// - set/drop `CHANGEFEED`
/// - set/drop table `COMMENT`
/// - change table `TYPE` (`NORMAL`/`RELATION`/`ANY`)
/// - request a table-level storage `COMPACT`
///
/// Notes:
/// - When switching to a `RELATION` table type, in/out fields are created as needed via
///   `DefineTableStatement::add_in_out_fields`.
/// - When `compact` is true, underlying storage for this table is compacted.
pub(crate) struct AlterTableStatement {
	/// Table name.
	pub name: String,
	/// If true, do nothing (and succeed) when the table does not exist.
	pub if_exists: bool,
	/// Switch `SCHEMAFULL` on (`Set`) or switch to `SCHEMALESS` (`Drop`).
	pub(crate) schemafull: AlterKind<()>,
	/// New table permissions, if provided.
	pub permissions: Option<Permissions>,
	/// Set/drop changefeed definition.
	pub(crate) changefeed: AlterKind<ChangeFeed>,
	/// Set/drop human‑readable comment.
	pub(crate) comment: AlterKind<String>,
	/// Request a compaction of the table’s keyspace.
	pub(crate) compact: bool,
	/// Change the table type (`NORMAL` / `RELATION` / `ANY`).
	pub kind: Option<TableType>,
}

impl AlterTableStatement {
	/// Computes the effect of the `ALTER TABLE` statement.
	///
	/// Permissions: requires `Action::Edit` on `ResourceKind::Table`.
	///
	/// Side effects:
	/// - May write table definition metadata
	/// - May compact the underlying storage if `compact` is true
	/// - May create relation helper fields when switching to `RELATION`
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
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
						name: self.name.clone(),
					}
					.into());
				}
			}
		};
		// Process the statement
		match self.schemafull {
			AlterKind::Set(_) => dt.schemafull = true,
			AlterKind::Drop => dt.schemafull = false,
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

		// Record definition change
		if changefeed_replaced {
			txn.changefeed_buffer_table_change(ns, db, &self.name, &dt);
		}

		if self.compact {
			let key = crate::key::table::all::new(ns, db, &self.name);
			txn.compact(Some(key)).await?;
		}

		// Set the table definition
		txn.put_tb(ns_name, db_name, &dt).await?;

		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for AlterTableStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterTableStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
