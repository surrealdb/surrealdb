use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Auth;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
use crate::sql::error::expect_tag_no_case;
use crate::sql::error::IResult;
use crate::sql::fetch::{fetch, Fetchs};
use crate::sql::field::{fields, Fields};
use crate::sql::param::param;
use crate::sql::table::table;
use crate::sql::value::Value;
use crate::sql::Uuid;
use derive::Store;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::cut;
use nom::combinator::into;
use nom::combinator::map;
use nom::combinator::opt;
use nom::sequence::preceded;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
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

pub fn live(i: &str) -> IResult<&str, LiveStatement> {
	let (i, _) = tag_no_case("LIVE")(i)?;
	let (i, _) = shouldbespace(i)?;
	let (i, _) = tag_no_case("SELECT")(i)?;
	let (i, _) = shouldbespace(i)?;
	cut(|i| {
		let (i, expr) = alt((map(tag_no_case("DIFF"), |_| Fields::default()), fields))(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, _) = expect_tag_no_case("FROM")(i)?;
		let (i, _) = shouldbespace(i)?;
		let (i, what) = alt((into(param), into(table)))(i)?;
		let (i, cond) = opt(preceded(shouldbespace, cond))(i)?;
		let (i, fetch) = opt(preceded(shouldbespace, fetch))(i)?;
		Ok((
			i,
			LiveStatement {
				id: Uuid::new_v4(),
				node: Uuid::new_v4(),
				expr,
				what,
				cond,
				fetch,
				..Default::default()
			},
		))
	})(i)
}
