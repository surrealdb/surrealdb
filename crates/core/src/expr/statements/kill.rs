use std::fmt;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::{Action, Notification, Options};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Expr, FlowResultExt as _};
use crate::val::{Uuid, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: Expr,
}

impl KillStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Resolve live query id
		let lid = match stk
			.run(|stk| self.id.compute(stk, ctx, opt, None))
			.await
			.catch_return()?
			.cast_to::<Uuid>()
		{
			Err(_) => {
				bail!(Error::KillStatement {
					value: self.id.to_string(),
				})
			}
			Ok(id) => id,
		};
		// Get the Node ID
		let nid = opt.id()?;
		// Get the LIVE ID
		let lid = lid.0;
		// Get the transaction
		let txn = ctx.tx();
		// Fetch the live query key
		let key = crate::key::node::lq::new(nid, lid);
		// Fetch the live query key if it exists
		match txn.get(&key, None).await? {
			Some(live) => {
				// Delete the node live query
				let key = crate::key::node::lq::new(nid, lid);
				txn.clr(&key).await?;
				// Delete the table live query
				let key = crate::key::table::lq::new(live.ns, live.db, &live.tb, lid);
				txn.clr(&key).await?;
				// Refresh the table cache for lives
				if let Some(cache) = ctx.get_cache() {
					cache.new_live_queries_version(live.ns, live.db, &live.tb);
				}
				// Clear the cache
				txn.clear_cache();
			}
			None => {
				bail!(Error::KillStatement {
					value: self.id.to_string(),
				});
			}
		}
		if let Some(chn) = opt.sender.as_ref() {
			chn.send(Notification {
				id: lid.into(),
				action: Action::Killed,
				record: Value::None,
				result: Value::None,
			})
			.await?;
		}
		// Return the query id
		Ok(Value::None)
	}
}

impl fmt::Display for KillStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "KILL {}", self.id)
	}
}
