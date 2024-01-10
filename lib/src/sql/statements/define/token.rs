use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{escape::quote_str, Algorithm, Base, Ident, Strand, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
pub struct DefineTokenStatement {
	pub name: Ident,
	pub base: Base,
	pub kind: Algorithm,
	pub code: String,
	pub comment: Option<Strand>,
}

impl DefineTokenStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match &self.base {
			Base::Ns => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::namespace::tk::new(opt.ns(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::database::tk::new(opt.ns(), opt.db(), &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.add_db(opt.ns(), opt.db(), opt.strict).await?;
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			Base::Sc(sc) => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				// Process the statement
				let key = crate::key::scope::tk::new(opt.ns(), opt.db(), sc, &self.name);
				run.add_ns(opt.ns(), opt.strict).await?;
				run.add_db(opt.ns(), opt.db(), opt.strict).await?;
				run.add_sc(opt.ns(), opt.db(), sc, opt.strict).await?;
				run.set(key, self).await?;
				// Ok all good
				Ok(Value::None)
			}
			// Other levels are not supported
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for DefineTokenStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(
			f,
			"DEFINE TOKEN {} ON {} TYPE {} VALUE {}",
			self.name,
			self.base,
			self.kind,
			quote_str(&self.code)
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}
