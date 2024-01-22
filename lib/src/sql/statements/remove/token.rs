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
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

		match &self.base {
			Base::Ns => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				match run.get_ns_token(opt.ns(), &self.name).await {
					Ok(nt) => {
						// Delete the definition
						let key = crate::key::namespace::tk::new(opt.ns(), &nt.name);
						run.del(key).await?;
						// Ok all good
						Ok(Value::None)
					}
					Err(err) => {
						if matches!(err, Error::NtNotFound { .. }) && self.if_exists {
							Ok(Value::None)
						} else {
							Err(err)
						}
					}
				}
			}
			Base::Db => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				match run.get_db_token(opt.ns(), opt.db(), &self.name).await {
					Ok(dt) => {
						// Delete the definition
						let key = crate::key::database::tk::new(opt.ns(), opt.db(), &dt.name);
						run.del(key).await?;
						// Ok all good
						Ok(Value::None)
					}
					Err(err) => {
						if matches!(err, Error::DtNotFound { .. }) && self.if_exists {
							Ok(Value::None)
						} else {
							Err(err)
						}
					}
				}
			}
			Base::Sc(sc) => {
				// Claim transaction
				let mut run = txn.lock().await;
				// Clear the cache
				run.clear_cache();
				match run.get_sc_token(opt.ns(), opt.db(), &sc, &self.name).await {
					Ok(st) => {
						// Delete the definition
						let key = crate::key::scope::tk::new(opt.ns(), opt.db(), sc, &st.name);
						run.del(key).await?;
						// Ok all good
						Ok(Value::None)
					}
					Err(err) => {
						if matches!(err, Error::StNotFound { .. }) && self.if_exists {
							Ok(Value::None)
						} else {
							Err(err)
						}
					}
				}
			}
			_ => Err(Error::InvalidLevel(self.base.to_string())),
		}
	}
}

impl Display for RemoveTokenStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TOKEN {} ON {}", self.name, self.base)?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		Ok(())
	}
}
