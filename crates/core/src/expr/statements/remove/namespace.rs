use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::NamespaceProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveNamespaceStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Default for RemoveNamespaceStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			expunge: false,
		}
	}
}

impl RemoveNamespaceStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Namespace, &Base::Root)?;
		// Get the transaction
		let txn = ctx.tx();
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "namespace name").await?;
		let ns = match txn.get_ns_by_name(&name).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}

				return Err(Error::NsNotFound {
					name,
				}
				.into());
			}
		};

		// Remove the index stores
		ctx.get_index_stores().namespace_removed(ctx, &txn, ns.namespace_id).await?;
		// Remove the sequences
		if let Some(seq) = ctx.get_sequences() {
			seq.namespace_removed(&txn, ns.namespace_id).await?;
		}

		// Delete the definition
		let key = crate::key::root::ns::new(&ns.name);
		let namespace_root = crate::key::namespace::all::new(ns.namespace_id);
		if self.expunge {
			txn.clr(&key).await?;
			txn.clrp(&namespace_root).await?;
		} else {
			txn.del(&key).await?;
			txn.delp(&namespace_root).await?;
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
