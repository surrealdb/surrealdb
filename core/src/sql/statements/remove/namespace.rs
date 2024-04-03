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
pub struct RemoveNamespaceStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveNamespaceStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
			// Claim transaction
			let mut run = txn.lock().await;
			ctx.get_index_stores().namespace_removed(opt, &mut run).await?;
			// Clear the cache
			run.clear_cache();
			// Get the definition
			let ns = run.get_ns(&self.name).await?;
			// Delete the definition
			let key = crate::key::root::ns::new(&ns.name);
			run.del(key).await?;
			// Delete the resource data
			let key = crate::key::namespace::all::new(&ns.name);
			run.delp(key, u32::MAX).await?;
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::NsNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveNamespaceStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE NAMESPACE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
