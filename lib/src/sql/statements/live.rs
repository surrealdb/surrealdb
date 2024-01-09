use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Auth;
use crate::sql::{Cond, Fetchs, Fields, Uuid, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
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
	// This is optional, and os only set when archived.
	pub(crate) archived: Option<Uuid>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can chack it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	#[revision(start = 2)]
	pub(crate) session: Option<Value>,
	// When a live query is created, we must also store the
	// authenticated session of the user who made the query,
	// so we can chack it later when sending notifications.
	// This is optional as it is only set by the database
	// runtime when storing the live query to storage.
	pub(crate) auth: Option<Auth>,
}

impl LiveStatement {
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
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
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
		let id = stm.id.0;
		// Claim transaction
		let mut run = txn.lock().await;
		// Process the live query table
		match stm.what.compute(ctx, opt, txn, doc).await? {
			Value::Table(tb) => {
				// Store the current Node ID
				stm.node = nid.into();
				// Insert the node live query
				run.putc_ndlq(nid, id, opt.ns(), opt.db(), tb.as_str(), None).await?;
				// Insert the table live query
				run.putc_tblq(opt.ns(), opt.db(), &tb, stm, None).await?;
			}
			v => {
				return Err(Error::LiveStatement {
					value: v.to_string(),
				})
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
