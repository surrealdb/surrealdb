use std::fmt::{self, Display};
#[cfg(target_family = "wasm")]
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::TableDefinition;
use crate::ctx::Context;
#[cfg(target_family = "wasm")]
use crate::dbs::Force;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
#[cfg(target_family = "wasm")]
use crate::expr::Output;
use crate::expr::statements::info::InfoStructure;
#[cfg(target_family = "wasm")]
use crate::expr::statements::{RemoveIndexStatement, UpdateStatement};
use crate::expr::{Base, Ident, Idiom, Index, Part};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use crate::sql::fmt::Fmt;
use crate::val::{Array, Strand, Value};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub what: Ident,
	pub cols: Vec<Idiom>,
	pub index: Index,
	pub comment: Option<Strand>,
	pub concurrently: bool,
}

impl_kv_value_revisioned!(DefineIndexStatement);

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
				let Some(Part::Field(first)) = idiom.0.first() else {
					continue;
				};
				if txn
					.get_tb_field(tb.namespace_id, tb.database_id, &tb.name, &first.as_raw_string())
					.await?
					.is_none()
				{
					bail!(Error::FdNotFound {
						name: first.to_string(),
					});
				}
			}
		}

		// Process the statement
		let key = crate::key::table::ix::new(tb.namespace_id, tb.database_id, &tb.name, &self.name);
		txn.set(
			&key,
			&DefineIndexStatement {
				// Don't persist the `IF NOT EXISTS`, `OVERWRITE` and `CONCURRENTLY` clause to
				// schema
				kind: DefineKind::Default,
				concurrently: false,
				..self.clone()
			},
			None,
		)
		.await?;

		// Refresh the table cache

		let key = crate::key::database::tb::new(tb.namespace_id, tb.database_id, &tb.name);
		txn.set(
			&key,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
			None,
		)
		.await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(tb.namespace_id, tb.database_id, &tb.name);
		}
		// Clear the cache
		txn.clear_cache();
		// Process the index
		#[cfg(not(target_family = "wasm"))]
		self.async_index(stk, ctx, opt, doc, !self.concurrently).await?;
		#[cfg(target_family = "wasm")]
		self.sync_index(stk, ctx, opt, doc).await?;
		// Ok all good
		Ok(Value::None)
	}

	#[cfg(target_family = "wasm")]
	async fn sync_index(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<()> {
		use crate::expr::Expr;

		{
			// Create the remove statement
			let stm = RemoveIndexStatement {
				name: self.name.clone(),
				what: self.what.clone(),
				if_exists: false,
			};
			// Execute the delete statement
			stm.compute(ctx, opt).await?;
		}
		{
			// Force queries to run
			let opt = &opt.new_with_force(Force::Index(Arc::new([self.clone()])));
			// Update the index data
			let stm = UpdateStatement {
				what: vec![Expr::Table(self.what.clone().into())],
				output: Some(Output::None),
				..UpdateStatement::default()
			};
			stm.compute(stk, ctx, opt, doc).await?;
		}
		Ok(())
	}

	#[cfg(not(target_family = "wasm"))]
	async fn async_index(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
		blocking: bool,
	) -> Result<()> {
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let rcv = ctx
			.get_index_builder()
			.ok_or_else(|| Error::unreachable("No Index Builder"))?
			.build(ctx, opt.clone(), ns, db, self.clone().into(), blocking)?;
		if let Some(rcv) = rcv {
			rcv.await.map_err(|_| Error::IndexingBuildingCancelled)?
		} else {
			Ok(())
		}
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
			write!(f, " {}", self.index)?;
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

impl InfoStructure for DefineIndexStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"what".to_string() => self.what.structure(),
			"cols".to_string() => Value::Array(Array(self.cols.into_iter().map(|x| x.structure()).collect())),
			"index".to_string() => self.index.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
