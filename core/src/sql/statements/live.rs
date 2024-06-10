use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Auth;
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Cond, Fetchs, Fields, Object, Uuid, Value};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 2)]
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
	// When a live query is marked for archiving, this will
	// be set to the node ID that archived the query. This
	// is an internal property, set by the database runtime.
	// This is optional, and is only set when archived.
	pub(crate) archived: Option<Uuid>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	#[revision(start = 2)]
	pub(crate) session: Option<Value>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can check it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) auth: Option<Auth>,
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
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
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
			session: ctx.value("session").cloned(),
			// Use the current session authentication
			// for when we store the LIVE Statement
			auth: Some(opt.auth.as_ref().clone()),
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
				// Get the transaction
				let txn = ctx.tx();
				// Lock the transaction
				let mut txn = txn.lock().await;
				// Insert the node live query
				txn.putc_ndlq(nid, id, opt.ns()?, opt.db()?, tb.as_str(), None).await?;
				// Insert the table live query
				txn.putc_tblq(opt.ns()?, opt.db()?, &tb, stm, None).await?;
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

	pub(crate) fn archive(mut self, node_id: Uuid) -> LiveStatement {
		self.archived = Some(node_id);
		self
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
		let Self {
			expr,
			what,
			cond,
			fetch,
			..
		} = self;

		let mut acc = Object::default();

		acc.insert("expr".to_string(), expr.structure());

		acc.insert("what".to_string(), what.structure());

		if let Some(cond) = cond {
			acc.insert("cond".to_string(), cond.structure());
		}

		if let Some(fetch) = fetch {
			acc.insert("fetch".to_string(), fetch.structure());
		}
		Value::Object(acc)
	}
}
