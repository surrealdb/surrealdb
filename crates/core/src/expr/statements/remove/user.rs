use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::UserProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveUserStatement {
	pub name: Expr,
	pub base: Base,
	pub if_exists: bool,
}

impl Default for RemoveUserStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			base: Base::default(),
			if_exists: false,
		}
	}
}

impl RemoveUserStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "user name").await?;
		// Check the statement type
		match self.base {
			Base::Root => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let us = match txn.get_root_user(&name).await? {
					Some(x) => x,
					None => {
						if self.if_exists {
							return Ok(Value::None);
						}

						return Err(Error::UserRootNotFound {
							name,
						}
						.into());
					}
				};

				// Process the statement
				let key = crate::key::root::us::new(&us.name);
				txn.del(&key).await?;
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
				let us = match txn.get_ns_user(ns, &name).await? {
					Some(x) => x,
					None => {
						if self.if_exists {
							return Ok(Value::None);
						}

						return Err(Error::UserNsNotFound {
							ns: opt.ns()?.to_string(),
							name,
						}
						.into());
					}
				};
				// Delete the definition
				let key = crate::key::namespace::us::new(ns, &us.name);
				txn.del(&key).await?;
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
				let us = match txn.get_db_user(ns, db, &name).await? {
					Some(x) => x,
					None => {
						if self.if_exists {
							return Ok(Value::None);
						}

						return Err(Error::UserDbNotFound {
							ns: opt.ns()?.to_string(),
							db: opt.db()?.to_string(),
							name,
						}
						.into());
					}
				};
				// Delete the definition
				let key = crate::key::database::us::new(ns, db, &us.name);
				txn.del(&key).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
		}
	}
}
