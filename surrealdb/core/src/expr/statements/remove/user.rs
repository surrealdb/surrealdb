use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::Error;
use crate::catalog::providers::UserProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::subscriptions::{
	kill_namespace_principal_subscriptions, kill_principal_subscriptions,
	kill_root_principal_subscriptions,
};
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
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Actor, self.base)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "user name").await?;
		// Check the statement type
		match self.base {
			Base::Root => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let us = match txn.get_root_user(&name, None).await? {
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
				// A subscription replays the `Auth` it captured at LIVE time on
				// every notification, so revoking the principal does not stop
				// delivery on its own.
				kill_root_principal_subscriptions(ctx, &txn, &us.name).await?;
				let key = crate::key::root::us::Us {
					user: std::borrow::Cow::Borrowed(&us.name),
				};
				txn.del_key(&key).await?;
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
				let us = match txn.get_ns_user(ns, &name, None).await? {
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
				// See the root arm.
				kill_namespace_principal_subscriptions(ctx, &txn, ns, &us.name).await?;
				let key = crate::key::namespace::us::Us {
					ns,
					user: std::borrow::Cow::Borrowed(&us.name),
				};
				txn.del_key(&key).await?;
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
				let us = match txn.get_db_user(ns, db, &name, None).await? {
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
				// See the root arm.
				kill_principal_subscriptions(ctx, &txn, ns, db, &us.name).await?;
				let key = crate::key::database::us::UserKey {
					prefix: crate::key::database::all::DatabaseRoot {
						ns,
						db,
					},
					user: std::borrow::Cow::Borrowed(&us.name),
				};
				txn.del_key(&key).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
		}
	}
}
