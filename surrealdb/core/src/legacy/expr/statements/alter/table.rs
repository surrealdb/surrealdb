use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Error, TableType};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::table::AlterTableStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::TblRoot;
use crate::legacy::expr_to_ident;
use crate::val::{TableName, Value};

/// Computes the effect of the `ALTER TABLE` statement.
///
/// Permissions: requires `Action::Edit` on `ResourceKind::Table`.
///
/// Side effects:
/// - May write table definition metadata
/// - May compact the underlying storage if `compact` is true
/// - May create relation helper fields when switching to `RELATION`
#[instrument(level = "trace", name = "AlterTableStatement::compute", skip_all)]
pub(crate) async fn alter_table_statement_compute(
	this: &AlterTableStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Table, Base::Db)?;
	let name = TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.name, "table name").await?);
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Fetch the transaction
	let txn = ctx.tx();

	// Get the table definition
	let mut dt = match txn.get_tb(ns, db, &name, None).await? {
		Some(tb) => tb.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::TbNotFound {
					name: name.clone(),
				}
				.into());
			}
		}
	};
	// Process the statement
	match this.schemafull {
		AlterKind::Set(_) => dt.schemafull = true,
		AlterKind::Drop => dt.schemafull = false,
		AlterKind::None => {}
	}

	if let Some(permissions) = &this.permissions {
		dt.permissions = permissions.clone();
	}

	let mut changefeed_replaced = false;
	match this.changefeed {
		AlterKind::Set(x) => {
			changefeed_replaced = dt.changefeed.is_some();
			dt.changefeed = Some(x)
		}
		AlterKind::Drop => dt.changefeed = None,
		AlterKind::None => {}
	}

	match this.comment {
		AlterKind::Set(ref x) => dt.comment = Some(x.clone()),

		AlterKind::Drop => dt.comment = None,
		AlterKind::None => {}
	}

	if let Some(kind) = &this.kind {
		dt.table_type = kind.clone();
	}

	// Add table relational fields
	if matches!(this.kind, Some(TableType::Relation(_))) {
		crate::legacy::define_table_statement_add_in_out_fields(&txn, ns, db, &mut dt).await?;
	}

	// Record definition change
	if changefeed_replaced {
		txn.changefeed_buffer_table_change(ns, db, &name, &dt.to_stored());
	}

	if this.compact {
		let key = TblRoot {
			ns,
			db,
			tb: Cow::Borrowed(&name),
		};
		txn.compact(&key).await?;
	}

	// Set the table definition
	txn.put_tb(ns_name, db_name, &dt).await?;

	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
