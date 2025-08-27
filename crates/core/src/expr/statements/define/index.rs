use std::fmt::{self, Display};
#[cfg(target_family = "wasm")]
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::{Index, IndexDefinition, TableDefinition};
use crate::ctx::Context;
#[cfg(target_family = "wasm")]
use crate::dbs::Force;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
#[cfg(target_family = "wasm")]
use crate::expr::Output;
#[cfg(target_family = "wasm")]
use crate::expr::statements::{RemoveIndexStatement, UpdateStatement};
use crate::expr::{Base, Ident, Idiom, Part};
use crate::iam::{Action, ResourceKind};
use crate::sql::ToSql;
use crate::sql::fmt::Fmt;
use crate::val::{Strand, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub what: Ident,
	pub cols: Vec<Idiom>,
	pub index: Index,
	pub comment: Option<Strand>,
	pub concurrently: bool,
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

		let (ns, db) = opt.ns_db()?;
		let tb = txn.ensure_ns_db_tb(ns, db, &self.what, opt.strict).await?;

		// Check if the definition exists
		if txn.get_tb_index(tb.namespace_id, tb.database_id, &tb.name, &self.name).await.is_ok() {
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
					&self.name,
				)
				.await?;
			#[cfg(target_family = "wasm")]
			ctx.get_index_stores()
				.index_removed(&txn, tb.namespace_id, tb.database_id, &tb.name, &self.name)
				.await?;
		}

		// If the table is schemafull, ensure that the fields exist.
		if tb.schemafull {
			// Check that the fields exist
			for idiom in self.cols.iter() {
				// TODO: Was this correct? Can users not index data on sub-fields?
				let Some(Part::Field(first)) = idiom.0.first() else {
					continue;
				};

				//
				if txn
					.get_tb_field(tb.namespace_id, tb.database_id, &tb.name, &first.to_raw_string())
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
		let key = crate::key::table::ix::new(tb.namespace_id, tb.database_id, &tb.name, &self.name);
		let index_def = IndexDefinition {
			name: self.name.to_raw_string(),
			what: self.what.to_raw_string(),
			cols: self.cols.clone(),
			index: self.index.clone(),
			comment: self.comment.clone().map(|x| x.to_raw_string()),
		};
		txn.set(&key, &index_def, None).await?;

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
		write!(
			f,
			" {} ON {} FIELDS {}",
			self.name,
			self.what,
			Fmt::comma_separated(self.cols.iter())
		)?;
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
				name: Ident::new(index.name.clone()).unwrap(),
				what: Ident::new(index.what.clone()).unwrap(),
				if_exists: false,
			};
			// Execute the delete statement
			stm.compute(ctx, opt).await?;
		}
		{
			// Force queries to run
			let opt = &opt.new_with_force(Force::Index(Arc::new([index.clone()])));
			// Update the index data
			let stm = crate::expr::UpdateStatement {
				what: vec![crate::expr::Expr::Table(Ident::new(index.what.clone()).unwrap())],
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
			.build(ctx, opt.clone(), ns, db, index.clone().into(), _blocking)?;
		if let Some(rcv) = rcv {
			rcv.await.map_err(|_| Error::IndexingBuildingCancelled)?
		} else {
			Ok(())
		}
	}
}
