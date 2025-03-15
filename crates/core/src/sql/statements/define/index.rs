use crate::ctx::Context;
use crate::dbs::{Force, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::statements::DefineTableStatement;
use crate::sql::statements::UpdateStatement;
use crate::sql::{Base, Ident, Idioms, Index, Output, Part, Strand, Value, Values};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::sync::Arc;
use uuid::Uuid;

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
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
}

impl DefineIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
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
			#[cfg(not(target_family = "wasm"))]
			ctx.get_index_stores()
				.index_removed(ctx.get_index_builder(), &txn, ns, db, &self.what, &self.name)
				.await?;
			#[cfg(target_family = "wasm")]
			ctx.get_index_stores().index_removed(&txn, ns, db, &self.what, &self.name).await?;
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
		#[cfg(not(target_family = "wasm"))]
		if self.concurrently {
			self.async_index(ctx, opt)?;
		} else {
			self.sync_index(stk, ctx, opt, doc).await?;
		}
		#[cfg(target_family = "wasm")]
		self.sync_index(stk, ctx, opt, doc).await?;
		// Ok all good
		Ok(Value::None)
	}

	async fn sync_index(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<(), Error> {
		// Force queries to run
		let opt = &opt.new_with_force(Force::Index(Arc::new([self.clone()])));
		// Update the index data
		let stm = UpdateStatement {
			what: Values(vec![Value::Table(self.what.clone().into())]),
			output: Some(Output::None),
			..UpdateStatement::default()
		};
		stm.compute(stk, ctx, opt, doc).await?;
		Ok(())
	}

	#[cfg(not(target_family = "wasm"))]
	fn async_index(&self, ctx: &Context, opt: &Options) -> Result<(), Error> {
		ctx.get_index_builder().ok_or_else(|| fail!("No Index Builder"))?.build(
			ctx,
			opt.clone(),
			self.clone().into(),
		)
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
		write!(f, " {} ON {} FIELDS {}", self.name, self.what, self.cols)?;
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
			"cols".to_string() => self.cols.structure(),
			"index".to_string() => self.index.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.into(),
		})
	}
}
