use crate::ctx::Context;
use crate::dbs::{Force, Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{statements::UpdateStatement, Base, Ident, Idioms, Index, Strand, Value, Values};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::sync::Arc;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
#[non_exhaustive]
pub struct DefineIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub cols: Idioms,
	pub index: Index,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
}

impl DefineIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		// Check if index already exists
		if self.if_not_exists
			&& run.get_tb_index(opt.ns(), opt.db(), &self.what, &self.name).await.is_ok()
		{
			return Err(Error::IxAlreadyExists {
				value: self.name.to_string(),
			});
		}
		// Process the statement
		let key = crate::key::table::ix::new(opt.ns(), opt.db(), &self.what, &self.name);
		run.add_ns(opt.ns(), opt.strict).await?;
		run.add_db(opt.ns(), opt.db(), opt.strict).await?;
		run.add_tb(opt.ns(), opt.db(), &self.what, opt.strict).await?;
		run.set(
			key,
			DefineIndexStatement {
				// Don't persist the "IF NOT EXISTS" clause to schema
				if_not_exists: false,
				..self.clone()
			},
		)
		.await?;
		// Remove the index data
		let key = crate::key::index::all::new(opt.ns(), opt.db(), &self.what, &self.name);
		run.delp(key, u32::MAX).await?;
		// Clear the cache
		let key = crate::key::table::ix::prefix(opt.ns(), opt.db(), &self.what);
		run.clr(key).await?;
		// Release the transaction
		drop(run);
		// Force queries to run
		let opt = &opt.new_with_force(Force::Index(Arc::new([self.clone()])));
		// Update the index data
		let stm = UpdateStatement {
			what: Values(vec![Value::Table(self.what.clone().into())]),
			..UpdateStatement::default()
		};
		stm.compute(ctx, opt, txn, doc).await?;
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineIndexStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE INDEX")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		write!(f, " {} ON {} FIELDS {}", self.name, self.what, self.cols)?;
		if Index::Idx != self.index {
			write!(f, " {}", self.index)?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}
