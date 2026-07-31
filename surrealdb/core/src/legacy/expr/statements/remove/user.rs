use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::Error;
use crate::catalog::providers::UserProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::remove::user::RemoveUserStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::{DbUserKey, NsUserKey, RootUserKey};
use crate::legacy::{
	expr_to_ident, kill_namespace_principal_subscriptions, kill_principal_subscriptions,
	kill_root_principal_subscriptions,
};
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_user_statement_compute(
	this: &RemoveUserStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Actor, this.base)?;
	// Compute the name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "user name").await?;
	// Check the statement type
	match this.base {
		Base::Root => {
			// Get the transaction
			let txn = ctx.tx();
			// Get the definition
			let us = match txn.get_root_user(&name, None).await? {
				Some(x) => x,
				None => {
					if this.if_exists {
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
			let key = RootUserKey {
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
					if this.if_exists {
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
			let key = NsUserKey {
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
					if this.if_exists {
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
			let key = DbUserKey {
				ns,
				db,
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
