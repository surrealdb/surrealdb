use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineTableStatement;
use crate::sql::{Base, Ident, Idioms, Index, Part, Strand, Value};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use uuid::Uuid;

#[revisioned(revision = 5)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[non_exhaustive]
pub struct DefineIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub cols: Idioms,
	pub index: Index,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
	#[revision(start = 4)]
	pub concurrently: bool,
	#[revision(start = 5)]
	/// Whether to defer the index creation and keep it updated in the background
	pub defer: bool,
}

impl DefineIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = opt.ns_db()?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_tb_index(ns, db, &self.what, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::IxAlreadyExists {
					name: self.name.to_string(),
				});
			}
			// Clear the index store cache
			ctx.get_index_stores()
				.index_removed(ctx.get_index_builder(), &txn, ns, db, &self.what, &self.name)
				.await?;
		}
		// Does the table exists?
		match txn.get_tb(ns, db, &self.what).await {
			Ok(tb) => {
				// Are we SchemaFull?
				if tb.full {
					// Check that the fields exists
					for idiom in self.cols.iter() {
						let Some(Part::Field(first)) = idiom.0.first() else {
							continue;
						};
						txn.get_tb_field(ns, db, &self.what, &first.to_string()).await?;
					}
				}
			}
			// If the TB was not found, we're fine
			Err(Error::TbNotFound {
				..
			}) => {}
			// Any other error should be returned
			Err(e) => return Err(e),
		}
		// Process the statement
		let key = crate::key::table::ix::new(ns, db, &self.what, &self.name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		txn.get_or_add_tb(ns, db, &self.what, opt.strict).await?;
		txn.set(
			key,
			revision::to_vec(&DefineIndexStatement {
				// Don't persist the `IF NOT EXISTS`, `OVERWRITE` and `CONCURRENTLY` clause to schema
				if_not_exists: false,
				overwrite: false,
				concurrently: false,
				..self.clone()
			})?,
			None,
		)
		.await?;
		// Refresh the table cache
		let key = crate::key::database::tb::new(ns, db, &self.what);
		let tb = txn.get_tb(ns, db, &self.what).await?;
		txn.set(
			key,
			revision::to_vec(&DefineTableStatement {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			})?,
			None,
		)
		.await?;
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.what);
		}
		// Clear the cache
		txn.clear();
		// Process the index
		self.run_indexing(ctx, opt, !self.concurrently).await?;
		// Ok all good
		Ok(Value::None)
	}

	pub(crate) async fn run_indexing(
		&self,
		ctx: &Context,
		opt: &Options,
		blocking: bool,
	) -> Result<(), Error> {
		let rcv = ctx
			.get_index_builder()
			.ok_or_else(|| Error::unreachable("No Index Builder"))?
			.build(ctx, opt.clone(), self.clone().into(), blocking)
			.await?;
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
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		if !self.cols.is_empty() {
			write!(f, " FIELDS {}", self.cols)?;
		}
		if Index::Idx != self.index {
			write!(f, " {}", self.index)?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		if self.concurrently {
			write!(f, " CONCURRENTLY")?
		}
		if self.defer {
			write!(f, " DEFER")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineIndexStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"what".to_string() => self.what.structure(),
			"cols".to_string() => self.cols.structure(),
			"index".to_string() => self.index.structure(),
			"defer".to_string() => self.defer.into(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
