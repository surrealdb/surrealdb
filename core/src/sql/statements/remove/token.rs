use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
#[non_exhaustive]
pub struct RemoveTokenStatement {
	pub name: Ident,
	pub base: Base,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveTokenStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

			match &self.base {
				Base::Ns => {
					// Claim transaction
					let mut run = txn.lock().await;
					// Clear the cache
					run.clear_cache();
					// Get the definition
					let tk = run.get_ns_token(opt.ns(), &self.name).await?;
					// Delete the definition
					let key = crate::key::namespace::tk::new(opt.ns(), &tk.name);
					run.del(key).await?;
					// Ok all good
					Ok(Value::None)
				}
				Base::Db => {
					// Claim transaction
					let mut run = txn.lock().await;
					// Clear the cache
					run.clear_cache();
					// Get the definition
					let tk = run.get_db_token(opt.ns(), opt.db(), &self.name).await?;
					// Delete the definition
					let key = crate::key::database::tk::new(opt.ns(), opt.db(), &tk.name);
					run.del(key).await?;
					// Ok all good
					Ok(Value::None)
				}
				Base::Sc(sc) => {
					// Claim transaction
					let mut run = txn.lock().await;
					// Clear the cache
					run.clear_cache();
					// Get the definition
					let tk = run.get_sc_token(opt.ns(), opt.db(), sc, &self.name).await?;
					// Delete the definition
					let key = crate::key::scope::tk::new(opt.ns(), opt.db(), sc, &tk.name);
					run.del(key).await?;
					// Ok all good
					Ok(Value::None)
				}
				_ => Err(Error::InvalidLevel(self.base.to_string())),
			}
		}
		.await;
		match future {
			Err(e) if self.if_exists => match e {
				Error::NtNotFound {
					..
				} => Ok(Value::None),
				Error::DtNotFound {
					..
				} => Ok(Value::None),
				Error::StNotFound {
					..
				} => Ok(Value::None),
				e => Err(e),
			},
			v => v,
		}
	}
}

impl Display for RemoveTokenStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TOKEN")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.base)?;
		Ok(())
	}
}
