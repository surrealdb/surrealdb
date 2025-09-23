use std::fmt::{self, Display};
#[cfg(target_family = "wasm")]
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, TableProvider};
use crate::catalog::{Index, IndexDefinition, TableDefinition};
use crate::ctx::Context;
#[cfg(target_family = "wasm")]
use crate::dbs::Force;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
#[cfg(target_family = "wasm")]
use crate::expr::Idiom;
#[cfg(target_family = "wasm")]
use crate::expr::Output;
use crate::expr::parameterize::{expr_to_ident, exprs_to_fields};
#[cfg(target_family = "wasm")]
use crate::expr::statements::{RemoveIndexStatement, UpdateStatement};
use crate::expr::{Base, Expr, Literal, Part};
use crate::fmt::Fmt;
use crate::iam::{Action, ResourceKind};
use crate::sql::ToSql;
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub cols: Vec<Expr>,
	pub index: Index,
	pub comment: Option<Expr>,
	pub concurrently: bool,
}

impl Default for DefineIndexStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			cols: Vec::new(),
			index: Index::Idx,
			comment: None,
			concurrently: false,
		}
	}
}

impl DefineIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();

		// Compute name and what
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "index name").await?;
		let what = expr_to_ident(stk, ctx, opt, doc, &self.what, "index table").await?;

		let (ns, db) = opt.ns_db()?;
		let tb = txn.ensure_ns_db_tb(ns, db, &what, opt.strict).await?;

		// Check if the definition exists
		let index_id = if let Some(ix) =
			txn.get_tb_index(tb.namespace_id, tb.database_id, &tb.name, &name).await?
		{
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::IxAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
			// Clear the index store cache
			#[cfg(not(target_family = "wasm"))]
			ctx.get_index_stores()
				.index_removed(
					ctx.get_index_builder(),
					&txn,
					tb.namespace_id,
					tb.database_id,
					&tb.name,
					&name,
				)
				.await?;
			#[cfg(target_family = "wasm")]
			ctx.get_index_stores()
				.index_removed(&txn, tb.namespace_id, tb.database_id, &tb.name, &name)
				.await?;

			ix.index_id
		} else {
			txn.lock().await.get_next_ix_id(tb.namespace_id, tb.database_id).await?
		};

		// Compute columns
		let cols = exprs_to_fields(stk, ctx, opt, doc, self.cols.as_slice()).await?;

		// If the table is schemafull, ensure that the fields exist.
		if tb.schemafull {
			// Check that the fields exist
			for idiom in cols.iter() {
				// TODO: Was this correct? Can users not index data on sub-fields?
				let Some(Part::Field(first)) = idiom.0.first() else {
					continue;
				};

				//
				if txn
					.get_tb_field(tb.namespace_id, tb.database_id, &tb.name, first)
					.await?
					.is_none()
				{
					bail!(Error::FdNotFound {
						name: idiom.to_raw_string(),
					});
				}
			}
		}

		// Process the statement
		let index_def = IndexDefinition {
			index_id,
			name,
			table_name: what,
			cols: cols.clone(),
			index: self.index.clone(),
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
		};
		txn.put_tb_index(tb.namespace_id, tb.database_id, &tb.name, &index_def).await?;

		// Refresh the table cache

		txn.put_tb(
			ns,
			db,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(tb.namespace_id, tb.database_id, &tb.name);
		}
		// Clear the cache
		txn.clear_cache();
		// Process the index
		run_indexing(stk, ctx, opt, doc, &index_def, !self.concurrently).await?;

		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineIndexStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE INDEX")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if !self.cols.is_empty() {
			write!(f, " FIELDS {}", Fmt::comma_separated(self.cols.iter()))?;
		}
		if Index::Idx != self.index {
			write!(f, " {}", self.index.to_sql())?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if self.concurrently {
			write!(f, " CONCURRENTLY")?
		}
		Ok(())
	}
}

pub(in crate::expr::statements) async fn run_indexing(
	#[cfg_attr(not(target_family = "wasm"), expect(unused_variables))] stk: &mut Stk,
	ctx: &Context,
	opt: &Options,
	#[cfg_attr(not(target_family = "wasm"), expect(unused_variables))] doc: Option<&CursorDoc>,
	index: &IndexDefinition,
	_blocking: bool,
) -> Result<()> {
	#[cfg(target_family = "wasm")]
	{
		{
			// Create the remove statement
			let stm = RemoveIndexStatement {
				name: Expr::Idiom(Idiom::field(index.name.clone())),
				what: Expr::Idiom(Idiom::field(index.table_name.clone())),
				if_exists: false,
			};
			// Execute the delete statement
			stm.compute(stk, ctx, opt, doc).await?;
		}
		{
			// Force queries to run
			let opt = &opt.new_with_force(Force::Index(Arc::new([index.clone()])));
			// Update the index data
			let stm = crate::expr::UpdateStatement {
				what: vec![crate::expr::Expr::Table(index.table_name.clone())],
				output: Some(Output::None),
				..UpdateStatement::default()
			};
			stm.compute(stk, ctx, opt, doc).await?;
			Ok(())
		}
	}

	#[cfg(not(target_family = "wasm"))]
	{
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let rcv = ctx
			.get_index_builder()
			.ok_or_else(|| Error::unreachable("No Index Builder"))?
			.build(ctx, opt.clone(), ns, db, index.clone().into(), _blocking)
			.await?;
		if let Some(rcv) = rcv {
			rcv.await.map_err(|_| Error::IndexingBuildingCancelled)?
		} else {
			Ok(())
		}
	}
}
