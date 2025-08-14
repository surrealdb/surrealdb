use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RemoveNamespaceStatement {
	pub name: Ident,
	pub if_exists: bool,
	pub expunge: bool,
}

impl RemoveNamespaceStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
		// Get the transaction
		let txn = ctx.tx();
		// Remove the index stores
		#[cfg(not(target_family = "wasm"))]
		ctx.get_index_stores().namespace_removed(ctx.get_index_builder(), &txn, &self.name).await?;
		#[cfg(target_family = "wasm")]
		ctx.get_index_stores().namespace_removed(&txn, &self.name).await?;
		// Remove the sequences
		if let Some(seq) = ctx.get_sequences() {
			seq.namespace_removed(&txn, &self.name).await?;
		}
		// Get the definition
		let ns = match txn.get_ns(&self.name).await {
			Ok(x) => x,
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::NsNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Delete the definition
		let key = crate::key::root::ns::new(&ns.name);
		if self.expunge {
			txn.clr(&key).await?
		} else {
			txn.del(&key).await?
		};
		// Delete the resource data
		let key = crate::key::namespace::all::new(&ns.name);
		if self.expunge {
			txn.clrp(&key).await?
		} else {
			txn.delp(&key).await?
		};
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear();
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
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
