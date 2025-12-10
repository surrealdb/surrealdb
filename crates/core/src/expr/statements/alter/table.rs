use std::ops::Deref;

use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::catalog::providers::TableProvider;
use crate::catalog::{Permissions, TableType};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::statements::DefineTableStatement;
use crate::expr::{Base, ChangeFeed};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterTableStatement {
	pub name: String,
	pub if_exists: bool,
	pub(crate) schemafull: AlterKind<()>,
	pub permissions: Option<Permissions>,
	pub(crate) changefeed: AlterKind<ChangeFeed>,
	pub(crate) comment: AlterKind<String>,
	pub kind: Option<TableType>,
}

impl AlterTableStatement {
	#[instrument(level = "trace", name = "AlterTableStatement::compute", skip_all)]
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
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
