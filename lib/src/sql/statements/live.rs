use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Transaction;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::iam::Auth;
use crate::sql::comment::shouldbespace;
use crate::sql::cond::{cond, Cond};
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
#[revisioned(revision = 1)]
pub struct LiveStatement {
	pub id: Uuid,
	pub node: Uuid,
	pub expr: Fields,
	pub what: Value,
	pub cond: Option<Cond>,
	pub fetch: Option<Fetchs>,

	// Non-query properties that are necessary for storage or otherwise carrying information

	// When a live query is archived, this should be the node ID that archived the query.
	pub archived: Option<Uuid>,
	// A live query is run with permissions, and we must validate that during the run.
	// It is optional, because the live query may be constructed without it being set.
	// It is populated during compute.
	pub auth: Option<Auth>,
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
		// Check that auth has been set
		let self_override = LiveStatement {
			auth: match self.auth {
				Some(ref auth) => Some(auth.clone()),
				None => Some(opt.auth.as_ref().clone()),
			},
			..self.clone()
		};
		trace!("Evaluated live query auth to {:?}", self_override.auth);
		// Claim transaction
		let mut run = txn.lock().await;
		// Process the live query table
		match self_override.what.compute(ctx, opt, txn, doc).await? {
			Value::Table(tb) => {
				// Clone the current statement
				let mut stm = self_override.clone();
				// Store the current Node ID
				if let Err(e) = opt.id() {
					trace!("No ID for live query {:?}, error={:?}", stm, e)
				}
				stm.node = Uuid(opt.id()?);
				// Insert the node live query
				let key =
					crate::key::node::lq::new(opt.id()?, self_override.id.0, opt.ns(), opt.db());
				run.putc(key, tb.as_str(), None).await?;
				// Insert the table live query
				let key = crate::key::table::lq::new(opt.ns(), opt.db(), &tb, self_override.id.0);
				run.putc(key, stm, None).await?;
			}
			v => {
				return Err(Error::LiveStatement {
					value: v.to_string(),
				})
			}
		};
		// Return the query id
		trace!("Live query after processing: {:?}", self_override);
		Ok(self_override.id.clone().into())
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
		let (i, _) = tag_no_case("FROM")(i)?;
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
