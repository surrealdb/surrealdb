use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};
use tracing::instrument;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::catalog::providers::TableProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};
use crate::val::TableName;

/// Represents an `ALTER INDEX` statement.
///
/// Currently supports decommissioning indexes as a safe preparation step before removal.
/// Decommissioning an index:
/// - Cancels any ongoing concurrent index builds
/// - Prevents the query planner from using the index
/// - Stops updating the index on record changes
///
/// This allows administrators to verify query performance before permanently removing an index.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterIndexStatement {
	pub name: Expr,
	pub table: Expr,
	pub if_exists: bool,
	/// If true, marks the index as decommissioned
	pub prepare_remove: bool,
	pub comment: AlterKind<String>,
}

impl Default for AlterIndexStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			table: Expr::Literal(Literal::None),
			if_exists: false,
			prepare_remove: false,
			comment: AlterKind::None,
		}
	}
}

impl AlterIndexStatement {
	#[instrument(level = "trace", name = "AlterIndexStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Index, Base::Db)?;
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "index name").await?;
		let table =
			TableName::new(expr_to_ident(stk, ctx, opt, doc, &self.table, "table name").await?);
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the index definition
		let mut ix = match txn.get_tb_index(ns, db, &table, &name, None).await? {
			Some(tb) => tb.deref().clone(),
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::IxNotFound {
						name,
					}
					.into());
				}
			}
		};

		match self.comment {
			AlterKind::Set(ref k) => ix.comment = Some(k.clone()),
			AlterKind::Drop => ix.comment = None,
			AlterKind::None => {}
		}

		if self.prepare_remove && !ix.prepare_remove {
			ix.prepare_remove = true;
		}

		// Set the index definition
		txn.put_tb_index(ns, db, &table, &ix).await?;

		// Refresh the table cache for indexes
		let tb = txn.expect_tb(ns, db, &table).await?;
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for AlterIndexStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterIndexStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
