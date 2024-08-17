use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Auth;
use crate::kvs::Live;
use crate::sql::paths::{AC, RD, TK};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Cond, Fetchs, Fields, Uuid, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct LiveStatement {
	pub id: Uuid,
	pub node: Uuid,
	pub expr: Fields,
	pub what: Value,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) auth: Option<Auth>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) session: Option<Value>,
}

impl LiveStatement {
	#[doc(hidden)]
	pub fn new(expr: Fields) -> Self {
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr,
			..Default::default()
		}
	}

	/// Creates a live statement from parts that can be set during a query.
	pub(crate) fn from_source_parts(
		expr: Fields,
		what: Value,
		cond: Option<Cond>,
		fetch: Option<Fetchs>,
	) -> Self {
		LiveStatement {
			id: Uuid::new_v4(),
			node: Uuid::new_v4(),
			expr,
			what,
			cond,
			fetch,
			..Default::default()
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		// Is realtime enabled?
		opt.realtime()?;
		// Valid options?
		opt.valid_for_db()?;
		// Get the Node ID
		let nid = opt.id()?;
		// Check that auth has been set
		let mut stm = LiveStatement {
			// Use the current session authentication
			// for when we store the LIVE Statement
			auth: Some(opt.auth.as_ref().clone()),
			// Use the current session authentication
			// for when we store the LIVE Statement
			session: ctx.value("session").cloned(),
			// Clone the rest of the original fields
			// from the LIVE statement to the new one
			..self.clone()
		};
		// Get the id
		let id = stm.id.0;
		// Process the live query table
		match stm.what.compute(stk, ctx, opt, doc).await? {
			Value::Table(tb) => {
				// Store the current Node ID
				stm.node = nid.into();
				// Get the NS and DB
				let ns = opt.ns()?;
				let db = opt.db()?;
				// Store the live info
				let lq = Live {
					ns: ns.to_string(),
					db: db.to_string(),
					tb: tb.to_string(),
				};
				// Get the transaction
				let txn = ctx.tx();
				// Lock the transaction
				let mut txn = txn.lock().await;
				// Insert the node live query
				let key = crate::key::node::lq::new(nid, id);
				txn.put(key, lq).await?;
				// Insert the table live query
				let key = crate::key::table::lq::new(ns, db, &tb, id);
				txn.put(key, stm).await?;
			}
			v => {
				return Err(Error::LiveStatement {
					value: v.to_string(),
				});
			}
		};
		// Return the query id
		Ok(id.into())
	}

	// We need to create a new context which we will
	// use for processing this LIVE query statement.
	// This ensures that we are using the session
	// of the user who created the LIVE query.
	pub(crate) fn context(&self, ctx: &Context) -> Option<MutableContext> {
		// Ensure that a session exists on the LIVE query
		let sess = match self.session.as_ref() {
			Some(v) => v,
			None => return None,
		};

		let mut lqctx = MutableContext::background();
		// Set the current transaction on the new LIVE
		// query context to prevent unreachable behaviour
		// and ensure that queries can be executed.
		lqctx.set_transaction(ctx.tx());
		// Add the session params to this LIVE query, so
		// that queries can use these within field
		// projections and WHERE clauses.
		lqctx.add_value("access", sess.pick(AC.as_ref()).into());
		lqctx.add_value("auth", sess.pick(RD.as_ref()).into());
		lqctx.add_value("token", sess.pick(TK.as_ref()).into());
		lqctx.add_value("session", sess.clone().into());

		Some(lqctx)
	}

	// We need to create a new options which we will
	// use for processing this LIVE query statement.
	// This ensures that we are using the auth data
	// of the user who created the LIVE query.
	pub(crate) fn options(&self, opt: &Options) -> Option<Options> {
		// Ensure that auth info exists on the LIVE query
		let auth = match self.auth.clone() {
			Some(v) => v,
			None => return None,
		};

		Some(opt.new_with_perms(true).with_auth(Arc::from(auth)))
	}
}

impl fmt::Display for LiveStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LIVE SELECT {} FROM {}", self.expr, self.what)?;
		if let Some(ref v) = self.cond {
			write!(f, " {v}")?
		}
		if let Some(ref v) = self.fetch {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for LiveStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"expr".to_string() => self.expr.structure(),
			"what".to_string() => self.what.structure(),
			"cond".to_string(), if let Some(v) = self.cond => v.structure(),
			"fetch".to_string(), if let Some(v) = self.fetch => v.structure(),
		})
	}
}
