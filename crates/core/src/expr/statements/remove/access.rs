use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::AuthorisationProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveAccessStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
}

impl Default for RemoveAccessStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::default(),
			if_exists: false,
		}
	}
}

impl RemoveAccessStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "access name").await?;
		// Check the statement type
		match &self.base {
			Base::Root => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let Some(ac) = txn.get_root_access(&name).await? else {
					if self.if_exists {
						return Ok(Value::None);
					} else {
						return Err(anyhow::Error::new(Error::AccessRootNotFound {
							ac: name,
						}));
					}
				};

				// Delete the definition
				txn.del_root_access(&ac.name).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let ns = ctx.get_ns_id(opt).await?;
				let Some(ac) = txn.get_ns_access(ns, &name).await? else {
					if self.if_exists {
						return Ok(Value::None);
					} else {
						let ns = opt.ns()?;
						return Err(anyhow::Error::new(Error::AccessNsNotFound {
							ac: name,
							ns: ns.to_string(),
						}));
					}
				};

				// Delete the definition
				txn.del_ns_access(ns, &ac.name).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				let Some(ac) = txn.get_db_access(ns, db, &name).await? else {
					if self.if_exists {
						return Ok(Value::None);
					} else {
						let (ns, db) = opt.ns_db()?;
						return Err(anyhow::Error::new(Error::AccessDbNotFound {
							ac: name,
							ns: ns.to_string(),
							db: db.to_string(),
						}));
					}
				};
				// Delete the definition
				txn.del_db_access(ns, db, &ac.name).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
		}
	}
}
